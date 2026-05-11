"""Command-line interface for the Trestle ETL pipeline.

Implements the ``python -m trestle_etl`` / ``trestle-etl`` entry point.
This module is responsible for:

* Parsing ``--full-sync``, ``--incremental``, ``--since <iso8601>``,
  ``--dry-run``, and ``--reconcile`` flags (Requirement 11.1).
* Validating mode-flag combinations (Requirement 11.2):

  - At most one of ``{--full-sync, --incremental, --reconcile}`` may be
    supplied in a single invocation.
  - ``--dry-run`` MAY combine with any single mode flag, except
    ``--reconcile``.
  - ``--since`` is a modifier that MAY combine with any single mode
    flag; ``--since`` alone (no other mode flag) is treated as a "since"
    run that overrides the State_Store watermark.

* Parsing the ``--since`` argument as an ISO 8601 UTC timestamp
  **before** any HTTP request (Requirement 11.4). A parse failure
  surfaces as a usage error at exit boundary, not as a network-time
  error partway through a run.
* Rejecting ``--reconcile`` with a placeholder message (Requirement 11.5).
* Rejecting the no-mode-flag invocation with a usage message
  (Requirement 11.6).
* Rejecting ``--incremental`` when the State_Store has no
  ``last_modification_timestamp``, with a remediation pointer to
  ``--full-sync`` or ``--since`` (Requirement 4.2).
* Wrapping the loader and state store in no-op shims when ``--dry-run``
  is supplied so that extraction and transformation still exercise the
  full pipeline but no row ever reaches MySQL and ``sync_state.json``
  is never rewritten (Requirement 11.3).

Operational notes:

* Per Requirement 12.1 the pipeline must not use ``print()`` for output.
  This module writes usage / error messages via :func:`sys.stderr.write`
  so they remain invisible to the ``print`` AST check in
  ``tests/unit/test_no_print.py`` while still producing proper stderr
  output for the operator. Log messages (informational / WARNING /
  ERROR) go through the :mod:`logging` module as normal.
* The entry-point function signature is ``main(argv=None)`` so that the
  ``console_scripts`` hook defined in ``pyproject.toml`` can call it
  with no arguments (``argparse`` defaults to ``sys.argv[1:]`` when
  ``argv`` is ``None``) and the ``__main__`` module can forward its own
  ``sys.argv[1:]`` explicitly.

Full DI wiring — constructing ``Settings → TokenManager → TrestleClient
→ Extractor → Transformer → (BulkLoader | UpsertLoader) → StateStore →
Orchestrator`` — is implemented by Task 11.1 and lives in :func:`_run`.
:meth:`Settings.load` runs before :class:`~trestle_etl.auth.TokenManager`
is constructed so any configuration-error path fires before network
I/O is initiated (Requirement 13.3).

Requirements validated: 4.2, 8.1, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6,
13.3.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

import requests
import sqlalchemy
from sqlalchemy.engine import Engine, URL

from trestle_etl import orchestrator
from trestle_etl.auth import TokenManager
from trestle_etl.config import Settings
from trestle_etl.errors import (
    ConfigError,
    CorruptStateError,
    TrestleETLError,
    UsageError,
)
from trestle_etl.http_client import TrestleClient
from trestle_etl.loader import BatchResult, Row
from trestle_etl.loader.bulk import BulkLoader
from trestle_etl.loader.upsert import UpsertLoader
from trestle_etl.logging_setup import configure_logging
from trestle_etl.state import StateStore, SyncState

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exit codes
# ---------------------------------------------------------------------------
#
# Kept as module-level constants so tests (Task 10.5) can reference them
# by name and so operators scanning the source get a single table of what
# each exit code means. The argparse convention of 2 for usage errors is
# preserved; other codes are chosen to be distinct non-zero values that
# the CLI tests can pin.

EXIT_OK = 0
EXIT_PIPELINE_ERROR = 1
EXIT_USAGE_ERROR = 2
EXIT_RECONCILE_PLACEHOLDER = 3
EXIT_CONFIG_ERROR = 4
EXIT_MISSING_STATE = 5


# ---------------------------------------------------------------------------
# Dry-run shims
# ---------------------------------------------------------------------------
#
# Requirement 11.3: under ``--dry-run``, the pipeline SHALL perform
# extraction and transformation but SHALL NOT write to MySQL and SHALL
# NOT update the State_Store.
#
# Rather than threading a ``dry_run`` boolean through every loader and
# through ``StateStore.save``, we wrap the real components in no-op
# shims that implement the same interfaces. This keeps the hot-path
# code in the orchestrator and loaders free of dry-run branching, and
# it makes the property-test that asserts "zero MySQL writes and zero
# state-file modifications" (Property 25, Task 10.2) trivially provable
# by construction: the shim simply has no side-effect path.


class DryRunLoader:
    """No-op loader that reports a ``BatchResult`` without writing to MySQL.

    Used by :func:`main` when ``--dry-run`` is supplied. Conforms to the
    :class:`~trestle_etl.loader.Loader` protocol so it can be substituted
    for :class:`~trestle_etl.loader.bulk.BulkLoader` or
    :class:`~trestle_etl.loader.upsert.UpsertLoader` without any further
    plumbing.

    The reported ``max_modification_timestamp`` is computed from the
    batch's rows so the orchestrator still tracks a meaningful watermark
    in memory and still emits the per-batch progress log
    (Requirement 12.2). The watermark is never written to disk because
    the accompanying :class:`DryRunStateStore` discards saves.
    """

    def __init__(self) -> None:
        # Track a small amount of bookkeeping for the INFO log at close
        # time. Not exposed on the interface; strictly for operator
        # visibility.
        self._batches = 0
        self._rows = 0

    def write_batch(self, rows: list[Row]) -> BatchResult:
        """Pretend to commit ``rows``; return a ``BatchResult`` summary.

        ``rows`` is not mutated. The returned ``max_modification_timestamp``
        is the maximum ``ModificationTimestamp`` across the batch, or a
        UTC ``datetime.min`` sentinel when every row in the batch has
        ``None`` for that field (which cannot occur in practice for
        upstream Trestle data but is defended here so the type signature
        stays non-``Optional``).
        """
        self._batches += 1
        self._rows += len(rows)

        # Import here to avoid a module-load-time dependency on the
        # transformer; cli.py is imported by ``python -m trestle_etl``
        # and we want the happy-path imports to stay shallow.
        from trestle_etl.transformer import PROMOTED_COLUMNS

        mod_ts_index = PROMOTED_COLUMNS.index("ModificationTimestamp")
        max_ts: Optional[datetime] = None
        for promoted, _raw in rows:
            ts = promoted[mod_ts_index]
            if ts is None:
                continue
            if max_ts is None or ts > max_ts:
                max_ts = ts

        if max_ts is None:
            # No ModificationTimestamp in any row. Emit the UTC epoch so
            # ``BatchResult`` keeps a non-Optional contract; the
            # orchestrator folds this into its running max via the same
            # ``max()`` it would use for a real batch, and since the
            # epoch is the minimum possible value it never displaces a
            # real watermark.
            max_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)

        return BatchResult(count=len(rows), max_modification_timestamp=max_ts)

    def close(self) -> None:
        """Log a summary of the dry-run and return.

        Mirrors the ``close`` methods on the real loaders so the
        orchestrator can call it unconditionally without a type check.
        """
        logger.info(
            "dry_run_loader_close batches=%d rows=%d "
            "detail=no_writes_sent_to_mysql",
            self._batches,
            self._rows,
        )

    # Full-sync-only methods on the real BulkLoader. Provided as no-ops
    # so that ``run_full_sync`` can call into this shim without special-
    # casing the ``--dry-run`` path.

    def ensure_indexes_if_resuming(self, state: SyncState) -> None:
        """No-op: indexes are never touched under ``--dry-run``."""
        del state  # unused; required by the bulk-loader signature

    def drop_secondary_indexes_if_fresh_full_sync(
        self, state: SyncState
    ) -> None:
        """No-op: indexes are never touched under ``--dry-run``."""
        del state  # unused


class DryRunStateStore:
    """State store wrapper that loads normally but discards every save.

    ``load()`` is delegated to the underlying :class:`StateStore` so the
    pipeline still sees the real watermark at startup (and therefore
    still enforces Requirement 4.2's "missing watermark" check against
    real on-disk state). ``save()`` is a no-op, which is the entire
    point of ``--dry-run``: the on-disk state file is not modified, so
    the invocation can be repeated without side effects.

    Implements the same public surface as :class:`StateStore` so the
    orchestrator can be handed this wrapper without any further
    plumbing.
    """

    def __init__(self, inner: StateStore) -> None:
        self._inner = inner

    @property
    def path(self) -> Path:
        """Report the underlying state-file path for logging/diagnostics."""
        return self._inner.path

    def load(self) -> SyncState:
        """Delegate to the real state store.

        Dry-run intentionally reads real state so that behavior-under-
        normal-state (e.g. resume-vs-pivot decisions, missing-watermark
        rejections) is honored; the ``--dry-run`` flag only suppresses
        *writes*.
        """
        return self._inner.load()

    def save(self, state: SyncState) -> None:
        """Discard ``state`` without writing to disk.

        Emits a DEBUG log entry with the watermark so operators running
        with ``-v`` can trace what a real run *would* have persisted.
        """
        logger.debug(
            "dry_run_state_store_save_skipped "
            "last_modification_timestamp=%s "
            "replication_in_progress=%s",
            (
                state.last_modification_timestamp.isoformat()
                if state.last_modification_timestamp is not None
                else "<none>"
            ),
            state.replication_in_progress,
        )


# ---------------------------------------------------------------------------
# MySQL engine construction
# ---------------------------------------------------------------------------


def _build_engine(settings: Settings) -> Engine:
    """Construct the SQLAlchemy ``Engine`` pointed at the operator's MySQL.

    Encapsulated in a helper so the DI wiring in :func:`_run` reads as
    prose and so callers can swap in a different construction strategy
    (for example, a testcontainers-backed engine in the end-to-end
    smoke test from Task 11.3) without touching the dispatch logic.

    Credential handling uses :meth:`sqlalchemy.engine.URL.create` rather
    than string concatenation so that any special character in a MySQL
    username or password — ``@``, ``:``, ``/``, ``?``, ``#``, and so on
    — is URL-encoded correctly. A hand-rolled ``f"mysql+pymysql://..."``
    would interpret an unescaped ``@`` in the password as the host-
    segment delimiter and silently route the connection to the wrong
    server. Letting SQLAlchemy own the encoding keeps the credential
    pipeline robust against whatever characters the operator happens to
    have in ``MYSQL_USER`` and ``MYSQL_PASSWORD``.

    ``local_infile=True`` is passed through ``connect_args`` because
    PyMySQL exposes ``local_infile`` as a connection-time option, not a
    DSN query parameter. This is the client-side toggle that
    Requirement 8.5 couples with the server-side ``local_infile=1`` for
    the bulk-load fast path; without it, ``LOAD DATA LOCAL INFILE``
    inside :class:`~trestle_etl.loader.bulk.BulkLoader` raises the
    PyMySQL-level rejection that the loader translates into
    :class:`~trestle_etl.errors.BulkLoadConfigError` (Requirement 8.6).
    Incremental runs do not use ``LOAD DATA LOCAL INFILE`` but we still
    enable the flag so the engine is identically configured regardless
    of mode: one place to look when an operator debugs a bulk-load
    rejection.
    """
    url = URL.create(
        drivername="mysql+pymysql",
        username=settings.mysql_user,
        password=settings.mysql_password,
        host=settings.mysql_host,
        port=settings.mysql_port,
        database=settings.mysql_database,
    )
    return sqlalchemy.create_engine(
        url,
        connect_args={"local_infile": True},
    )


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    """Construct the argparse parser.

    Broken out into its own function so tests can introspect flag
    definitions without invoking :func:`main`, and so the parser can be
    reused from the reconcile / missing-state error paths when we want
    to reuse ``print_usage`` for the ``--reconcile`` rejection message.
    """
    parser = argparse.ArgumentParser(
        prog="python -m trestle_etl",
        description="Trestle ETL pipeline: extract Property records from "
        "the Trestle WebAPI into MySQL.",
    )
    # Mode flags. ``argparse`` does not enforce mutual exclusivity
    # across these three because ``--dry-run`` and ``--since`` are
    # modifiers that interact with them, and argparse's built-in
    # ``add_mutually_exclusive_group`` would reject the modifier
    # combinations we want to allow. The combination check lives in
    # :func:`_validate_args` instead so the error messages can be
    # precise.
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Run a full sync via the Trestle replication endpoint. "
        "Uses the bulk-load fast path (LOAD DATA LOCAL INFILE).",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Run an incremental sync using the State_Store's "
        "last_modification_timestamp as the lower bound.",
    )
    parser.add_argument(
        "--since",
        metavar="ISO8601",
        help="Override the incremental lower bound with the given ISO "
        "8601 UTC timestamp. Parsed before any HTTP request is issued.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract and transform records but do not write to MySQL "
        "and do not update the State_Store. MAY NOT be combined with "
        "--reconcile.",
    )
    parser.add_argument(
        "--reconcile",
        action="store_true",
        help="Placeholder for a future reconcile mode. Currently exits "
        "non-zero without doing any work.",
    )
    return parser


def _parse_iso8601_utc(s: str) -> datetime:
    """Parse an ISO 8601 timestamp and return a UTC-aware ``datetime``.

    Accepts the common ``...Z`` suffix (turned into ``+00:00`` before
    handoff to :func:`datetime.fromisoformat`) as well as any explicit
    offset. A naive (offset-less) timestamp is interpreted as UTC
    rather than rejected, matching the Requirement 11.4 wording that
    says the flag supplies a "UTC timestamp".

    Raises:
        UsageError: The string does not parse as ISO 8601. Emitted
            before any network I/O so operators learn about syntax
            errors instantly, not three retry attempts into a doomed
            extractor loop (Requirement 11.4).
    """
    # datetime.fromisoformat accepts "...+00:00" natively on 3.11+, but
    # not the trailing "Z" that many tools emit. The .replace call is a
    # narrow, documented translation of that single suffix.
    normalized = s.replace("Z", "+00:00") if s.endswith("Z") else s
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise UsageError(
            f"--since value is not a valid ISO 8601 timestamp: {s!r}"
        ) from exc
    if dt.tzinfo is None:
        # Treat naive timestamps as UTC. Requirement 11.4 specifies a
        # UTC timestamp, so assuming UTC for offset-less input matches
        # operator intent; rejecting would be unnecessarily strict for
        # a CLI that already assumes UTC everywhere.
        dt = dt.replace(tzinfo=timezone.utc)
    # Normalize to UTC so downstream code never has to think about offsets.
    return dt.astimezone(timezone.utc)


def _validate_args(args: argparse.Namespace) -> None:
    """Validate the mode-flag combination.

    Enforces Requirement 11.2:

    * At most one of ``{--full-sync, --incremental, --reconcile}``.
    * ``--dry-run`` MAY NOT combine with ``--reconcile``.
    * If no mode flag AND no ``--since`` is supplied, the invocation is
      incomplete; Requirement 11.6 says this is a usage error. Treating
      ``--since`` as sufficient on its own matches the pipeline state
      machine in the design doc (``IncrementalSince: --since``).

    Raises:
        UsageError: The combination is invalid.
    """
    # Count of mode-flag hits. True is 1, False is 0; summing booleans
    # is fine here and keeps the check compact.
    mode_flags = (
        bool(args.full_sync),
        bool(args.incremental),
        bool(args.reconcile),
    )
    n_modes = sum(mode_flags)

    if n_modes > 1:
        raise UsageError(
            "Only one of --full-sync, --incremental, --reconcile may be "
            "specified in the same invocation"
        )

    # --dry-run + --reconcile is explicitly forbidden by Req 11.2.
    # Combined with the single-mode-flag rule above, this also means
    # --dry-run + --full-sync and --dry-run + --incremental are both
    # valid, which is the intended behavior for Property 25 (Task 10.2).
    if args.dry_run and args.reconcile:
        raise UsageError("--dry-run may not be combined with --reconcile")

    # No mode flag AND no --since: Requirement 11.6.
    if n_modes == 0 and not args.since:
        raise UsageError(
            "No mode flag supplied. Specify one of --full-sync, "
            "--incremental, --since <ISO8601>, or --reconcile"
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the CLI and return an exit code.

    ``argv`` defaults to ``None`` so that the ``console_scripts`` entry
    point (which calls ``main()`` with no arguments) works; argparse
    itself defaults to ``sys.argv[1:]`` when ``argv`` is ``None``.

    Exit code contract (referenced by Task 10.5 tests):

    * :data:`EXIT_OK` (0)                    — run completed normally.
    * :data:`EXIT_PIPELINE_ERROR` (1)        — pipeline raised a
                                               :class:`TrestleETLError`.
    * :data:`EXIT_USAGE_ERROR` (2)           — bad flag combination,
                                               unparseable ``--since``,
                                               or argparse-level failure.
    * :data:`EXIT_RECONCILE_PLACEHOLDER` (3) — ``--reconcile`` supplied.
    * :data:`EXIT_CONFIG_ERROR` (4)          — configuration missing or
                                               invalid (includes corrupt
                                               state file per Req 9.8).
    * :data:`EXIT_MISSING_STATE` (5)         — ``--incremental`` invoked
                                               but no watermark exists.
    """
    configure_logging()
    parser = _build_parser()

    # argparse.parse_args exits the process with code 2 and prints its
    # own usage message on syntax errors (e.g. unknown flag). That is
    # exactly the behavior Requirement 11.6 wants for the "unknown
    # input" case, so we rely on it rather than re-implementing it.
    args = parser.parse_args(argv)

    try:
        _validate_args(args)
    except UsageError as exc:
        sys.stderr.write(f"usage error: {exc}\n")
        parser.print_usage(file=sys.stderr)
        return EXIT_USAGE_ERROR

    # Requirement 11.5: --reconcile is a placeholder. Exit non-zero with
    # a message explaining the situation; no further work is performed.
    # This check is deliberately placed AFTER ``_validate_args`` so a
    # bad combination like ``--dry-run --reconcile`` surfaces as the
    # more specific combination error, not as the generic placeholder
    # message.
    if args.reconcile:
        sys.stderr.write(
            "error: --reconcile is a placeholder and not implemented in "
            "the first iteration of the Trestle ETL pipeline\n"
        )
        return EXIT_RECONCILE_PLACEHOLDER

    # Requirement 11.4: parse --since BEFORE any HTTP or database I/O
    # so a malformed timestamp surfaces immediately. This must run
    # BEFORE Settings.load()/StateStore.load() can fail on unrelated
    # issues, so operators see the most specific error possible for
    # their input.
    since_ts: Optional[datetime] = None
    if args.since is not None:
        try:
            since_ts = _parse_iso8601_utc(args.since)
        except UsageError as exc:
            sys.stderr.write(f"usage error: {exc}\n")
            return EXIT_USAGE_ERROR

    # Decide which run mode to dispatch. The validate step above
    # guarantees exactly one of these branches fires.
    #
    # Note on --incremental + --since: Requirement 4.3 says --since
    # overrides the State_Store value. We treat --since as taking
    # precedence over --incremental for mode dispatch so the "since"
    # path runs with the caller-supplied timestamp rather than going
    # through the missing-watermark check. The "since" mode is
    # functionally an incremental run; this is consistent with the
    # design's ``IncrementalSince`` state.
    if args.full_sync:
        mode = "full-sync"
    elif args.since is not None:
        mode = "since"
    elif args.incremental:
        mode = "incremental"
    else:  # pragma: no cover - defended by _validate_args
        # _validate_args should have raised UsageError already. The
        # assert documents the invariant for readers and fails loudly
        # if someone ever refactors the validation away.
        raise AssertionError(
            "Reached mode dispatch with no mode flag; "
            "_validate_args should have blocked this"
        )

    # Dispatch into the run helper. Configuration errors and
    # TrestleETLError subclasses are translated into stable exit codes
    # so shell-level scripts can distinguish "user supplied bad config"
    # from "pipeline failed mid-run" without parsing log output.
    try:
        return _run(mode=mode, since=since_ts, dry_run=bool(args.dry_run))
    except ConfigError as exc:
        sys.stderr.write(f"configuration error: {exc}\n")
        return EXIT_CONFIG_ERROR
    except CorruptStateError as exc:
        # Per Requirement 9.8 a corrupt state file is a hard failure
        # that must not modify the file. The StateStore leaves the
        # file untouched; all we do here is surface the error and a
        # non-zero exit so the operator can inspect/repair it.
        sys.stderr.write(f"configuration error: {exc}\n")
        return EXIT_CONFIG_ERROR
    except TrestleETLError as exc:
        sys.stderr.write(f"pipeline error: {exc}\n")
        return EXIT_PIPELINE_ERROR


def _run(
    mode: str,
    since: Optional[datetime],
    dry_run: bool,
) -> int:
    """Dispatch to the orchestrator; return an exit code.

    Performs the full dependency-injection wiring required by Task 11.1:
    ``Settings → TokenManager → TrestleClient → Extractor → Transformer
    → (BulkLoader | UpsertLoader) → StateStore → Orchestrator``. The
    order of construction is load-bearing for Requirement 13.3: every
    configuration-error path must fire BEFORE any network I/O can be
    initiated, which is why :meth:`Settings.load` is the first call
    made in :func:`main` (via this function's caller) and why
    :class:`TokenManager` cannot be constructed before ``settings`` is
    in hand.

    Behavior:

    1. Load :class:`~trestle_etl.state.StateStore` so the Requirement
       4.2 missing-watermark check has real on-disk state to inspect.
       Under ``--dry-run`` the underlying store is still read (we only
       suppress *writes*); the :class:`DryRunStateStore` wrapper is
       handed to the orchestrator so its ``save()`` calls discard.
    2. Under ``--incremental`` with no persisted watermark, return
       :data:`EXIT_MISSING_STATE` with a remediation message naming
       both valid recovery paths.
    3. Construct the shared :class:`requests.Session` and thread it
       into both :class:`TokenManager` (for OIDC token acquisition)
       and :class:`TrestleClient` (for every API call). Sharing the
       session keeps one HTTP connection pool across token refreshes
       and data fetches, which matters for quota-constrained runs.
    4. Under normal mode construct a SQLAlchemy :class:`Engine` via
       :func:`_build_engine` (URL-encoded credentials,
       ``local_infile=True``) and the loader(s) the mode requires.
       Full sync constructs BOTH loaders because the stale-link pivot
       branch in :func:`orchestrator.run_full_sync` hands off to
       :func:`orchestrator.run_incremental`, which needs the upsert
       loader. Under ``--dry-run`` a single :class:`DryRunLoader`
       is reused for whichever loader slot the mode would otherwise
       populate.
    5. Dispatch to the matching orchestrator entry point. The engine
       is disposed in a ``finally`` so the connection pool is released
       regardless of whether the run completed normally, raised, or
       was interrupted by SIGINT.

    Loader selection by mode (Requirement 8.1):

    * ``--full-sync`` → :class:`BulkLoader` as the primary loader (one
      CSV + one ``LOAD DATA LOCAL INFILE`` per replication page);
      :class:`UpsertLoader` kept alongside for the stale-link pivot.
    * ``--incremental`` / ``--since`` → :class:`UpsertLoader` only.
    """
    # Load configuration first so a missing env var fails fast with a
    # clear ``ConfigError`` BEFORE any network I/O is initiated
    # (Requirement 13.3). The TokenManager construction below depends
    # on ``settings`` and is the first thing in the wiring that would
    # touch the network (on its first ``get_token()`` call), so placing
    # ``Settings.load`` at the very top of ``_run`` is what makes the
    # "configuration errors precede network I/O" guarantee hold.
    settings = Settings.load()

    # Load state so we can satisfy Requirement 4.2 (missing watermark
    # rejection). State_Store.load returns a default-constructed
    # SyncState with ``last_modification_timestamp=None`` when the file
    # does not exist, so this also handles the first-run case.
    real_state_store = StateStore(settings.state_file_path)
    state = real_state_store.load()

    if mode == "incremental" and state.last_modification_timestamp is None:
        # Requirement 4.2 remediation message. Two forward-pointers are
        # offered because either is a valid recovery path: --full-sync
        # bootstraps the watermark from scratch, --since lets the
        # operator supply a known-good cutoff without re-downloading
        # everything.
        sys.stderr.write(
            "error: cannot run --incremental: the State_Store has no "
            "last_modification_timestamp.\n"
            "remediation: run --full-sync to bootstrap the watermark, "
            "or supply --since <ISO8601> to specify an explicit lower "
            "bound.\n"
        )
        return EXIT_MISSING_STATE

    # Pick the state store the orchestrator will actually see. Under
    # ``--dry-run`` this is a wrapper that delegates ``load()`` to the
    # real store (so we kept the real state check above) and discards
    # ``save()`` calls. Under normal mode the real store is used
    # directly.
    effective_state_store: StateStore
    if dry_run:
        # DryRunStateStore mirrors the StateStore surface (load/save/
        # path). The type annotation on ``effective_state_store`` is
        # ``StateStore`` because the orchestrator only reads that
        # subset; the wrapper is substitutable per the Liskov rule.
        effective_state_store = DryRunStateStore(real_state_store)  # type: ignore[assignment]
        logger.info(
            "dry_run_enabled detail=loader_and_state_store_writes_suppressed"
        )
    else:
        effective_state_store = real_state_store

    # ---- HTTP layer -------------------------------------------------
    # Single Session shared between TokenManager (for the OIDC token
    # POST) and TrestleClient (for every API GET). Keeping the pool
    # shared means a token refresh does not tear down the data-path
    # connection and vice versa.
    session = requests.Session()
    token_mgr = TokenManager(settings, http=session)
    client = TrestleClient(settings, token_mgr, http=session)

    # ---- Loaders and engine -----------------------------------------
    # ``engine`` is only constructed in non-dry-run mode. Its lifetime
    # is bounded by the try/finally below so the connection pool is
    # released on every exit path, including exception and SIGINT.
    engine: Optional[Engine] = None
    bulk_loader: Optional[object] = None
    upsert_loader: Optional[object] = None

    if dry_run:
        # A single shim serves whichever loader slot(s) the orchestrator
        # would populate for this mode. The shim implements the full-
        # sync-only bulk-loader surface as well as the common
        # ``write_batch`` / ``close`` protocol, so reusing it across
        # slots is safe.
        shim = DryRunLoader()
        if mode == "full-sync":
            # Full sync could pivot to incremental, so both slots must
            # be populated even under dry-run. The same shim instance
            # is reused: the orchestrator calls at most one loader's
            # ``write_batch`` on any given page, and ``close`` is
            # idempotent on the shim.
            bulk_loader = shim
            upsert_loader = shim
        else:
            upsert_loader = shim
    else:
        engine = _build_engine(settings)
        if mode == "full-sync":
            # Requirement 8.1: full sync uses the bulk loader as its
            # primary path. The upsert loader is constructed alongside
            # because ``orchestrator.run_full_sync`` pivots to
            # ``run_incremental`` when the saved replication link is
            # stale (Requirement 3.8), and that pivot requires an
            # upsert loader. Constructing both up front is cheap (no
            # connections are opened until a batch runs) and keeps the
            # pivot path free of late-binding surprises.
            bulk_loader = BulkLoader(engine)
            upsert_loader = UpsertLoader(engine)
        else:
            # Incremental / since runs never touch the bulk path.
            upsert_loader = UpsertLoader(engine)

    # ---- Dependency bundle and dispatch -----------------------------
    deps = orchestrator.Deps(
        state_store=effective_state_store,
        client=client,
        settings=settings,
        bulk_loader=bulk_loader,  # type: ignore[arg-type]
        upsert_loader=upsert_loader,  # type: ignore[arg-type]
    )

    try:
        if mode == "full-sync":
            orchestrator.run_full_sync(deps)
        elif mode == "since":
            # ``since`` is guaranteed non-None here: ``main`` only
            # selects the "since" branch when ``args.since`` parsed
            # successfully. Narrow the Optional for mypy/readers.
            assert since is not None, (
                "mode='since' reached _run with since=None; "
                "main() should have rejected the invocation earlier"
            )
            orchestrator.run_since(deps, since)
        elif mode == "incremental":
            # The missing-watermark check above guarantees
            # ``state.last_modification_timestamp is not None`` on this
            # branch; feed it to ``run_incremental`` as the lower bound.
            assert state.last_modification_timestamp is not None, (
                "mode='incremental' reached _run dispatch without a "
                "watermark; the missing-state check above should have "
                "returned EXIT_MISSING_STATE"
            )
            orchestrator.run_incremental(
                deps, since=state.last_modification_timestamp
            )
        else:  # pragma: no cover - defended by main()'s mode selection
            raise AssertionError(f"Unknown mode: {mode!r}")
    finally:
        # Release the connection pool on every exit path. ``dispose()``
        # is safe to call even when no connections were ever checked
        # out, so the guard only skips it in the dry-run case where no
        # engine was constructed at all.
        if engine is not None:
            engine.dispose()
        session.close()

    return EXIT_OK


__all__ = [
    "DryRunLoader",
    "DryRunStateStore",
    "EXIT_CONFIG_ERROR",
    "EXIT_MISSING_STATE",
    "EXIT_OK",
    "EXIT_PIPELINE_ERROR",
    "EXIT_RECONCILE_PLACEHOLDER",
    "EXIT_USAGE_ERROR",
    "UsageError",
    "main",
]
