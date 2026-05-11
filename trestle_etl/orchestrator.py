"""Top-level run loop for the Trestle ETL pipeline.

The orchestrator is the single component that knows which mode the
pipeline is running in and the sole writer of the State_Store. It threads
together an extractor generator, the transformer, a loader strategy
(bulk or upsert), and the state store, enforcing the one rule that makes
Requirement 15 (crash-recovery invariant) hold: **state is saved only
after the batch that produced it has committed**.

Three entry points are exposed:

* :func:`run_full_sync`   — invoked by ``--full-sync``. On startup it
  decides between three branches per Requirement 3.8:

    1. Resume the replication stream from the saved ``@odata.nextLink``
       iff ``replication_in_progress`` is set AND the link was persisted
       less than 4 minutes ago (the Trestle replication link lifetime is
       5 minutes of inactivity; 4 minutes gives a safety margin).
    2. Pivot to an incremental run starting from
       ``last_modification_timestamp`` if the link is stale.
    3. Start a fresh full sync otherwise.

* :func:`run_incremental` — invoked by ``--incremental``. Streams the
  ``/Property`` endpoint filtered by ``ModificationTimestamp gt <since>``
  and commits via the upsert loader.

* :func:`run_since`       — invoked by ``--since``. A thin wrapper over
  :func:`run_incremental` that uses the CLI-supplied timestamp as the
  lower bound, overriding whatever the State_Store holds.

Requirements validated:

* 3.6, 3.7   — per-batch ``replication_in_progress`` / ``next_link``
                updates; cleared when the stream terminates.
* 3.8        — resume-vs-pivot decision based on the 4-minute freshness
                window of ``replication_next_link_persisted_at``.
* 4.5        — ``last_modification_timestamp`` persisted to the State_Store
                equals the running max ``ModificationTimestamp`` across
                every committed batch.
* 9.3, 9.4, 9.5 — state updated only after successful commit; the
                replication link is written on every save that touches it.
* 12.2, 12.3, 12.4 — per-batch progress INFO log plus run-start / run-end
                INFO entries.
* 15.1, 15.2, 15.3, 15.4 — save happens after commit (Req 15.1); the
                state's ``last_modification_timestamp`` equals the max
                committed ModTs on both success and failure paths; strict
                greater-than semantics live in the extractor and the
                running-max tracking here respects it.

This module defines a module-level ``_sigint_received`` flag plus the
:mod:`signal` handlers that flip it (Task 9.2). The handlers are
installed on entry to :func:`_run_batches` and uninstalled in its
``finally``, so SIGINT handling is active for the full duration of a
run but the orchestrator does not leave state-modifying handlers
installed on a process that is merely importing the module.
"""

from __future__ import annotations

import logging
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator, Optional

from trestle_etl import transformer
from trestle_etl.config import Settings
from trestle_etl.extractor import incremental_stream, replication_stream
from trestle_etl.http_client import TrestleClient
from trestle_etl.loader import BatchResult, Row
from trestle_etl.loader.bulk import BulkLoader
from trestle_etl.loader.upsert import UpsertLoader
from trestle_etl.logging_setup import log_run_end, log_run_start
from trestle_etl.state import StateStore, SyncState

logger = logging.getLogger(__name__)

# Requirement 3.8: the Trestle replication link expires after 5 minutes
# of inactivity, and cannot be skipped. We use a 4-minute cutoff so a
# resume attempt is still inside the window even after a small amount of
# startup overhead between the state load and the first HTTP request.
_REPLICATION_LINK_FRESH_WINDOW: timedelta = timedelta(minutes=4)


# ---------------------------------------------------------------------------
# SIGINT flag and handlers
# ---------------------------------------------------------------------------
#
# Requirement 10 in total:
#
# * 10.1  First SIGINT: finish the current batch, commit, update state,
#         then exit cleanly.
# * 10.2  After a SIGINT is observed, no additional pages are fetched
#         from Trestle.
# * 10.3  On graceful shutdown the pipeline exits 0 and logs the last
#         committed ``ModificationTimestamp``.
# * 10.4  A second SIGINT terminates the process immediately with exit
#         code 130 (POSIX convention: 128 + SIGINT) and explicitly does
#         NOT commit the in-flight batch.
#
# Keeping the flag at module scope (rather than inside a class) means the
# handler — which must be a free function — can flip it without threading
# extra references through the orchestrator plumbing. The per-batch loop
# polls the flag via :func:`_is_sigint_received` immediately AFTER a
# batch commit; when set, the loop breaks out before pulling the next
# page from the generator, which is how Requirement 10.2 is enforced.

_sigint_received: bool = False


def _is_sigint_received() -> bool:
    """Return ``True`` if a SIGINT has been observed since process start.

    Polled from the per-batch loop AFTER each commit so that an interrupt
    delivered at any point during page fetch / transform / write results
    in a clean exit at the next iteration boundary, with state already
    durably persisted for the completed batch.
    """
    return _sigint_received


def _first_sigint_handler(signum: int, frame: Any) -> None:
    """Handle the first SIGINT: set the flag and install the escalation.

    Flips :data:`_sigint_received` so the batch loop will exit after the
    in-flight batch commits (Requirement 10.1). Before returning,
    installs :func:`_second_sigint_handler` so a second Ctrl+C escalates
    to immediate exit (Requirement 10.4). Emits a WARNING so operators
    watching the log stream see the shutdown decision.
    """
    global _sigint_received
    _sigint_received = True
    logger.warning(
        "sigint_received action=graceful_shutdown "
        "detail=finishing_current_batch_then_exiting"
    )
    # Escalate on a second SIGINT. ``signal.signal`` is idempotent: if
    # the installed handler is already ``_second_sigint_handler`` (e.g.
    # because a signal arrived while this function was running), the
    # reinstall is a no-op.
    signal.signal(signal.SIGINT, _second_sigint_handler)


def _second_sigint_handler(signum: int, frame: Any) -> None:
    """Handle the second SIGINT: abort immediately without commit.

    Requirement 10.4: the in-flight batch MUST NOT commit. Raising via
    :func:`sys.exit` bypasses ``finally`` blocks at the Python level
    only insofar as they are outside the current call stack; loader
    transactions that have not yet issued ``COMMIT`` will be rolled
    back by the database driver on connection teardown, which is the
    behavior required here.
    """
    logger.warning(
        "sigint_received action=immediate_exit "
        "detail=second_signal_no_commit exit_code=130"
    )
    sys.exit(130)


def install_sigint_handler() -> None:
    """Wire :func:`_first_sigint_handler` into :data:`signal.SIGINT`.

    Safe to call from the main thread only (as with every
    :func:`signal.signal` call). The run loop invokes this on entry and
    pairs it with :func:`uninstall_sigint_handler` in its ``finally``,
    so a library consumer importing :mod:`orchestrator` without running
    a sync does not end up with a process-wide handler change.
    """
    signal.signal(signal.SIGINT, _first_sigint_handler)


def uninstall_sigint_handler() -> None:
    """Restore the default SIGINT disposition and clear the flag.

    Called from the batch loop's ``finally`` so a subsequent run within
    the same process starts from a clean slate. Also exposed for tests
    that want to exercise the handler installation path without leaking
    a handler into unrelated tests.
    """
    global _sigint_received
    _sigint_received = False
    signal.signal(signal.SIGINT, signal.SIG_DFL)


def reset_sigint_state() -> None:
    """Test-only helper: clear the flag and restore the default handler.

    Equivalent to :func:`uninstall_sigint_handler`, provided under a
    name that makes intent obvious at the test-site call and that
    decouples tests from the handler-lifecycle naming.
    """
    uninstall_sigint_handler()


# ---------------------------------------------------------------------------
# Dependency container and run result
# ---------------------------------------------------------------------------


def _utc_now() -> datetime:
    """Default clock implementation: timezone-aware UTC wall-clock."""
    return datetime.now(timezone.utc)


@dataclass
class Deps:
    """Dependency bundle passed to every orchestrator entry point.

    Both loader fields are ``Optional`` so the same ``Deps`` shape can
    serve every run mode:

    * ``--full-sync`` requires ``bulk_loader``; ``upsert_loader`` is only
      consulted if the pivot-to-incremental branch runs.
    * ``--incremental`` and ``--since`` require ``upsert_loader`` only.

    The CLI wiring in Task 11.1 constructs whichever loaders a given
    invocation could possibly need (full-sync always constructs both so
    that a stale-link pivot is possible without re-entering the CLI).

    ``clock`` is injectable so property tests can drive the 4-minute
    freshness check deterministically without :mod:`freezegun`.
    """

    state_store: StateStore
    client: TrestleClient
    settings: Settings
    bulk_loader: Optional[BulkLoader] = None
    upsert_loader: Optional[UpsertLoader] = None
    # ``field(default=...)`` rather than a bare ``_utc_now`` so the
    # default is captured when the dataclass is constructed, not at
    # class-definition time — matters for tests that monkeypatch
    # ``_utc_now``.
    clock: Callable[[], datetime] = field(default=_utc_now)


@dataclass
class RunResult:
    """Summary returned to the CLI at the end of a run.

    Attributes:
        total_records: Cumulative number of rows committed across every
            batch that completed successfully.
        final_state: The last ``SyncState`` persisted to disk. Reflects
            the exact bytes the next run will read on startup.
        elapsed_seconds: Wall-clock duration of the run, measured via
            :func:`time.monotonic` so it is unaffected by wall-clock
            jumps during the run.
        final_max_modification_timestamp: Convenience accessor equal to
            ``final_state.last_modification_timestamp``. Exposed
            separately so that callers interested only in the watermark
            do not have to dig into the state.
    """

    total_records: int
    final_state: SyncState
    elapsed_seconds: float
    final_max_modification_timestamp: Optional[datetime]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _link_is_fresh(state: SyncState, now: datetime) -> bool:
    """Decide whether the persisted replication link is still resumable.

    Returns ``False`` if the link was never persisted (``None``) or if
    more than :data:`_REPLICATION_LINK_FRESH_WINDOW` has elapsed since
    it was written. Naive timestamps are treated as UTC so a state file
    produced by an older codebase cannot slip through with an ambiguous
    timezone.
    """
    persisted_at = state.replication_next_link_persisted_at
    if persisted_at is None:
        return False
    if persisted_at.tzinfo is None:
        persisted_at = persisted_at.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return (now - persisted_at) < _REPLICATION_LINK_FRESH_WINDOW


def _max_ts(*candidates: Optional[datetime]) -> Optional[datetime]:
    """Return the maximum of any non-None datetimes, or ``None`` if all None.

    Used to fold a batch's ``max_modification_timestamp`` into the
    running max kept across the run. Must tolerate ``None`` from either
    side because (a) a fresh pipeline has no prior watermark and (b) a
    batch that filters every record (all missing ``ListingKey``) yields
    no ModTs.
    """
    real = [c for c in candidates if c is not None]
    if not real:
        return None
    return max(real)


def _transform_page(page_records: list[dict]) -> list[Row]:
    """Apply :func:`transformer.to_row_safe` to every record in a page.

    ``None`` results (records skipped because ``ListingKey`` was missing;
    Requirement 5.6) are filtered out so the loader never sees them. The
    transformer itself logs the skip WARNING.
    """
    rows: list[Row] = []
    for record in page_records:
        row = transformer.to_row_safe(record)
        if row is not None:
            rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def run_full_sync(deps: Deps) -> RunResult:
    """Run a full sync via the Trestle replication endpoint.

    Performs the startup decision required by Requirement 3.8:

    1. If ``replication_in_progress`` is True and the saved link was
       persisted inside the 4-minute window, resume from that link. The
       bulk loader is told to recreate any secondary index that a
       previous crash may have left missing (Requirement 8.8) before
       extraction begins.
    2. If ``replication_in_progress`` is True but the link is stale, the
       bulk loader repairs missing indexes and then is closed so the
       full set of secondary indexes is present before the pipeline
       pivots to :func:`run_incremental`. Requires
       ``deps.upsert_loader`` for the pivot.
    3. Otherwise, a fresh full sync drops the seven secondary indexes
       (Requirement 8.7) before streaming replication pages.

    The bulk loader's :meth:`~BulkLoader.close` is always invoked in a
    ``finally`` block so the secondary indexes end up recreated
    regardless of success, failure, or SIGINT-driven early exit. Because
    ``BulkLoader.close`` only creates indexes that are missing, a
    duplicate call (for instance after a pivot that already closed the
    loader) is a no-op.
    """
    if deps.bulk_loader is None:
        raise ValueError(
            "run_full_sync requires deps.bulk_loader; received None"
        )

    state = deps.state_store.load()
    now = deps.clock()

    mode_label: str
    stream: Iterator[tuple[list[dict], Optional[str]]]

    if state.replication_in_progress and _link_is_fresh(state, now):
        # ---- Resume branch (Requirement 3.8 first clause) ----
        # A previous run was mid-replication and the link is still
        # inside its inactivity window. Ensure every required index is
        # present (a prior crash may have left some dropped) and then
        # continue from the persisted nextLink verbatim.
        logger.info(
            "resume_full_sync replication_next_link_age_seconds=%.3f",
            (now - state.replication_next_link_persisted_at).total_seconds(),
        )
        deps.bulk_loader.ensure_indexes_if_resuming(state)
        stream = replication_stream(
            deps.client,
            deps.settings,
            resume_from=state.replication_next_link,
        )
        mode_label = "full-sync-resume"

    elif state.replication_in_progress:
        # ---- Pivot branch (Requirement 3.8 second clause) ----
        # The link has aged past the safety window. Rather than risk a
        # 410-Gone on a stale cursor, repair any missing indexes, close
        # the bulk loader (putting the table back into a shape that
        # supports efficient range scans), and hand off to the upsert
        # path starting from the highest committed timestamp. Because
        # Req 15.1 guarantees ``last_modification_timestamp`` reflects
        # only committed batches, the strict ``>`` filter in
        # ``incremental_stream`` prevents duplicates.
        logger.warning(
            "pivot_to_incremental reason=stale_replication_link "
            "link_age_seconds=%.3f",
            (now - state.replication_next_link_persisted_at).total_seconds()
            if state.replication_next_link_persisted_at is not None
            else float("nan"),
        )
        if deps.upsert_loader is None:
            raise ValueError(
                "Cannot pivot from stale full-sync to incremental: "
                "deps.upsert_loader is None"
            )
        if state.last_modification_timestamp is None:
            # A full sync that never committed even one batch has no
            # watermark to pivot from. Surface a clear error rather than
            # silently starting from the epoch.
            raise RuntimeError(
                "Cannot pivot to incremental: state has no "
                "last_modification_timestamp. Rerun with --full-sync "
                "from scratch or --since <ts>."
            )
        # Repair any missing indexes and then close the bulk loader to
        # restore the full secondary-index set before the upsert path
        # starts issuing range queries. ``close`` only creates missing
        # indexes, so this is safe even if nothing was actually dropped.
        deps.bulk_loader.ensure_indexes_if_resuming(state)
        deps.bulk_loader.close()
        return run_incremental(deps, since=state.last_modification_timestamp)

    else:
        # ---- Fresh full sync (Requirement 3.8 third clause) ----
        # No mid-flight replication: drop the seven secondary indexes
        # per Requirement 8.7 and start the stream from the initial URL.
        deps.bulk_loader.drop_secondary_indexes_if_fresh_full_sync(state)
        stream = replication_stream(deps.client, deps.settings)
        mode_label = "full-sync"

    # Common post-decision path: drive the stream through the batch
    # loop. ``_run_batches`` handles the run-start / run-end INFO logs
    # (Req 12.3, 12.4) and the per-batch progress log (Req 12.2). The
    # ``finally`` guarantees indexes are restored even on exception.
    try:
        return _run_batches(deps, state, stream, deps.bulk_loader, mode_label)
    finally:
        deps.bulk_loader.close()


def run_incremental(deps: Deps, since: datetime) -> RunResult:
    """Run an incremental sync starting strictly after ``since``.

    Used both directly for the ``--incremental`` CLI path and as the
    pivot target when :func:`run_full_sync` finds a stale replication
    link. The extractor's strict ``>`` filter (Requirement 15.4) plus
    the fact that ``since`` is always a ``ModificationTimestamp`` of a
    *committed* record mean no boundary record is ever re-processed and
    no record is ever skipped.

    The upsert loader is stateless between calls in the sense that its
    ``close`` is a no-op, so there is no ``finally`` cleanup required
    here.
    """
    if deps.upsert_loader is None:
        raise ValueError(
            "run_incremental requires deps.upsert_loader; received None"
        )

    state = deps.state_store.load()
    stream = incremental_stream(deps.client, deps.settings, since=since)
    return _run_batches(deps, state, stream, deps.upsert_loader, "incremental")


def run_since(deps: Deps, ts: datetime) -> RunResult:
    """Run an incremental sync from the CLI-supplied ``--since`` timestamp.

    Semantically equivalent to :func:`run_incremental` with ``since=ts``
    but uses a distinct mode label in the run-start / run-end logs so
    operators can tell from the log stream which CLI flag produced the
    run. The override bypasses the State_Store's
    ``last_modification_timestamp`` (Requirement 4.3) — note that the
    state is still written at every commit, so an interrupted ``--since``
    run can be resumed normally via ``--incremental`` on the next
    invocation.
    """
    if deps.upsert_loader is None:
        raise ValueError(
            "run_since requires deps.upsert_loader; received None"
        )

    state = deps.state_store.load()
    stream = incremental_stream(deps.client, deps.settings, since=ts)
    return _run_batches(deps, state, stream, deps.upsert_loader, "since")


# ---------------------------------------------------------------------------
# Core batch loop
# ---------------------------------------------------------------------------


def _run_batches(
    deps: Deps,
    state: SyncState,
    stream: Iterator[tuple[list[dict], Optional[str]]],
    loader,
    mode_label: str,
) -> RunResult:
    """Drive ``stream`` page-by-page through transform → load → save.

    Invariants maintained per iteration (these together give
    Requirements 3.6, 3.7, 9.3, 9.5, 15.1, 15.2, 15.3):

    1. :meth:`Loader.write_batch` is called **before** ``state_store.save``.
       If the loader raises, the exception propagates out of this
       function without touching the state file, leaving the prior
       ``last_modification_timestamp`` intact for the next run.
    2. The new ``SyncState`` is built from the running max
       ``ModificationTimestamp`` (folded across every successful batch
       so far) and the just-observed ``next_link``. It is then
       ``save()``d atomically.
    3. The stream is only advanced by the ``for`` loop pulling another
       ``(page, next_link)`` tuple, which happens strictly AFTER the
       save above. That guarantees Requirement 3.4: the ``@odata.nextLink``
       persisted on disk matches the one the next iteration will
       actually follow, and no fetch happens before the prior batch is
       durably committed.

    The progress log (Req 12.2) is emitted once per committed batch with
    cumulative count, running max ModTs, elapsed seconds, and a
    per-minute batch/request rate.

    SIGINT handling (Requirement 10):

    * :func:`install_sigint_handler` is called on entry so the first
      Ctrl+C sets :data:`_sigint_received` and the second escalates to
      an immediate :func:`sys.exit(130)`.
    * The flag is polled AFTER each commit. A set flag breaks the loop
      BEFORE the ``for`` pulls the next ``(page, next_link)`` from the
      generator, which prevents any further HTTP fetch from Trestle
      (Requirement 10.2). It is deliberately NOT polled before the
      commit: a page that has already been yielded by the extractor is
      the "in-flight batch" per Requirement 10.1 and MUST commit before
      the graceful shutdown completes.
    * On exit (graceful or otherwise), a WARNING with the last-committed
      ``ModificationTimestamp`` is logged so the shutdown boundary is
      visible in the log stream (Requirement 10.3).
    * :func:`uninstall_sigint_handler` is called unconditionally in the
      ``finally`` so the process is not left with a handler pointing at
      this module after the run returns.
    """
    log_run_start(
        logger,
        mode_label,
        deps.settings.trestle_base_url,
        state.last_modification_timestamp,
    )

    # Wire SIGINT into this run. ``install_sigint_handler`` is a no-op
    # if the handler is already pointed at ``_first_sigint_handler``; we
    # still call it unconditionally so a fresh run clears any residual
    # state from an earlier one in the same process.
    install_sigint_handler()

    # ``time.monotonic()`` is immune to wall-clock jumps (e.g. NTP
    # adjustments mid-run), which matters for the elapsed/req-rate
    # fields in the progress log. Actual timestamps persisted to state
    # still come from the caller-supplied ``deps.clock``.
    start_monotonic = time.monotonic()
    total_count = 0
    batch_count = 0
    running_max_ts: Optional[datetime] = state.last_modification_timestamp
    sigint_triggered_exit = False

    try:
        for page_records, next_link in stream:
            # NOTE: the SIGINT flag is *not* polled here. A page that
            # has already been yielded by the extractor generator is the
            # "in-flight batch" per Requirement 10.1 and must commit
            # before a graceful shutdown completes. Breaking at this
            # point would discard committed-ready work and leave the
            # replication cursor one page behind the records that the
            # server has already delivered.

            rows = _transform_page(page_records)

            # Empty pages (either zero records from the server or every
            # record filtered for missing ListingKey) must still result
            # in a state save so the new ``next_link`` is persisted.
            # Otherwise a restart would replay the same page and lose
            # ground on the replication stream.
            batch_result: Optional[BatchResult]
            if rows:
                batch_result = loader.write_batch(rows)
            else:
                batch_result = None

            # Fold the batch max into the running max. Requirement 4.5:
            # the persisted watermark is the max across every record
            # observed in the run, not wall-clock time.
            batch_max_ts = (
                batch_result.max_modification_timestamp
                if batch_result is not None
                else None
            )
            running_max_ts = _max_ts(running_max_ts, batch_max_ts)

            # Build the next on-disk state. Requirement 3.6 / 3.7:
            # ``replication_in_progress`` tracks whether there is more
            # to fetch, and is cleared by the terminal page's ``None``
            # nextLink. ``replication_next_link_persisted_at`` is
            # written on every save that touches the link; that is what
            # the 4-minute freshness check at the next startup will
            # consult.
            now = deps.clock()
            new_state = SyncState(
                last_modification_timestamp=running_max_ts,
                replication_in_progress=(next_link is not None),
                replication_next_link=next_link,
                replication_next_link_persisted_at=(
                    now if next_link is not None else None
                ),
            )

            # Save AFTER the commit. This single ordering is what makes
            # Requirement 15.1 hold: the on-disk watermark never points
            # past data that has not been committed to MySQL.
            deps.state_store.save(new_state)
            state = new_state

            committed = batch_result.count if batch_result is not None else 0
            total_count += committed
            batch_count += 1

            # Progress log (Req 12.2). Requests per minute is computed
            # as batches/minute since start: replication pages map 1:1
            # to HTTP requests, so the two rates are equivalent. Using
            # "since start" rather than "last interval" keeps the log
            # line stateless; the orchestrator still has a monotonic
            # cumulative view, which is what long-running operators want.
            elapsed = max(time.monotonic() - start_monotonic, 1e-9)
            req_per_min = (batch_count / elapsed) * 60.0
            logger.info(
                "batch_committed mode=%s batches=%d cumulative=%d "
                "max_mod_ts=%s elapsed_seconds=%.3f "
                "requests_per_minute=%.2f",
                mode_label,
                batch_count,
                total_count,
                running_max_ts.isoformat() if running_max_ts else "<none>",
                elapsed,
                req_per_min,
            )

            # Post-commit SIGINT poll (Requirement 10.1, 10.2):
            # - 10.1: the in-flight batch has just committed and state
            #   is durable, so a graceful exit at this point preserves
            #   the crash-recovery invariant.
            # - 10.2: breaking out of the ``for`` loop prevents the
            #   generator from issuing the next HTTP GET.
            if _is_sigint_received():
                sigint_triggered_exit = True
                logger.warning(
                    "graceful_shutdown_complete mode=%s cumulative=%d "
                    "last_committed_mod_ts=%s",
                    mode_label,
                    total_count,
                    running_max_ts.isoformat()
                    if running_max_ts is not None
                    else "<none>",
                )
                break
    finally:
        elapsed_total = time.monotonic() - start_monotonic
        # Req 12.4: run-end log reports final watermark, total records,
        # and elapsed. Emitted unconditionally so aborted runs still
        # produce a matched start/end pair in the log stream.
        log_run_end(
            logger,
            total_count,
            elapsed_total,
            state.last_modification_timestamp,
        )
        # Always restore the default SIGINT disposition and clear the
        # flag. This is load-bearing for tests (which re-enter the run
        # loop in a single process) and keeps the orchestrator from
        # leaving a handler installed on a module that merely executed
        # one run.
        uninstall_sigint_handler()

    # Suppress the unused-variable warning while documenting that the
    # flag is observed: sigint_triggered_exit is reserved for future
    # callers that want to distinguish graceful shutdown from natural
    # stream termination without re-polling the flag (which has been
    # cleared by ``uninstall_sigint_handler`` above).
    del sigint_triggered_exit

    return RunResult(
        total_records=total_count,
        final_state=state,
        elapsed_seconds=elapsed_total,
        final_max_modification_timestamp=state.last_modification_timestamp,
    )


__all__ = [
    "Deps",
    "RunResult",
    "install_sigint_handler",
    "uninstall_sigint_handler",
    "reset_sigint_state",
    "run_full_sync",
    "run_incremental",
    "run_since",
]
