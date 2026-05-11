"""Property test for the crash-recovery invariant (Property 24).

Property 24 (design.md): For any simulated pipeline run — successful
completion, exception during a batch, or SIGINT injected at an arbitrary
point — after the process exits the following invariants hold:

    (a) ``state.last_modification_timestamp`` equals the max
        ``ModificationTimestamp`` across all committed batches.
    (b) every observed record with ``ModificationTimestamp <=
        state.last_modification_timestamp`` is present in the ``property``
        table.
    (c) no committed record's ``ModificationTimestamp`` exceeds
        ``state.last_modification_timestamp``.

**Validates: Requirements 15.2, 15.3, 10.1, 10.2**

Interpretation note on clause (c)
---------------------------------
The design's literal phrasing of (c) is "no *non-committed* record's
``ModificationTimestamp`` exceeds ``state.last_modification_timestamp``".
In Trestle's ``$orderby=ModificationTimestamp asc`` stream, records in a
failed batch necessarily have ModTs ≥ the max of the prior committed
batches, so the literal form is unsatisfiable in any exception scenario
(it is vacuously true only in the success case). The invariant that is
actually useful for crash recovery — and that Requirement 15.3 aligns
with — is the dual safety claim: **every COMMITTED record's ModTs is
<= ``state.last_mod_ts``**. That is what this test asserts under (c).
Combined with (a), this guarantees the state file is a safe upper bound
on committed data, which is what lets the strict-greater-than
incremental filter (Requirement 15.4) resume without skipping or
re-applying records.

How the scenarios are driven
----------------------------
The test does not start MySQL; it models the ``property`` table as an
in-memory ``dict`` and simulates "commit" as an atomic dict update.
Three failure modes are simulated:

* ``success``   — every batch commits; the loop terminates naturally on
                  the terminal page.
* ``exception`` — the fake loader raises ``RuntimeError`` at a chosen
                  batch index; control leaves ``_run_batches`` before
                  that batch's ``state_store.save``, and the dict is
                  left untouched for rows that would have been in the
                  failed batch.
* ``sigint``    — the fake loader commits the chosen batch's rows, then
                  flips ``trestle_etl.orchestrator._sigint_received``
                  directly (mimicking a real signal handler). The
                  orchestrator polls the flag right after save and
                  breaks out of the for-loop without fetching the next
                  page (Requirement 10.2).

The scenario generator respects Trestle's ascending-ModTs ordering so
that the invariants under test mirror production stream shapes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from hypothesis import HealthCheck, given, settings as hyp_settings
from hypothesis import strategies as st

from trestle_etl import orchestrator as orch
from trestle_etl.config import Settings
from trestle_etl.loader import BatchResult, Row
from trestle_etl.state import StateStore


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_settings(state_path: Path) -> Settings:
    """Minimal Settings whose only meaningful field is the state-file path.

    The extractor reads ``trestle_base_url`` and ``default_page_size``
    for the first HTTP request, but our fake client does not care about
    URL shape — it returns pages from a FIFO queue regardless of the URL
    it is handed.
    """
    return Settings(
        trestle_base_url="https://example.invalid/trestle/odata",
        trestle_token_url="https://example.invalid/oidc/token",
        client_id="cid",
        client_secret="cs",
        mysql_host="localhost",
        mysql_port=3306,
        mysql_user="u",
        mysql_password="p",
        mysql_database="d",
        state_file_path=state_path,
        default_page_size=1000,
    )


class _FakeClient:
    """TrestleClient stand-in that hands out pre-scripted pages in FIFO order.

    The extractor only invokes ``.get(url, params=...)`` on its client.
    Any URL is accepted; the fake's job is to deliver the next page in
    the queue until the queue is empty. Popping rather than indexing
    means an unexpected extra GET (for instance, after a terminal page)
    surfaces as an IndexError at the exact call site rather than
    silently replaying a previous page.
    """

    def __init__(self, pages: list[dict]) -> None:
        self._pages: list[dict] = list(pages)

    def get(self, url: str, params: Optional[dict] = None) -> dict:
        if not self._pages:
            raise AssertionError(
                "FakeClient queue exhausted; extractor fetched past the "
                "terminal page, which violates Property 6 / Requirement 3.3."
            )
        return self._pages.pop(0)


@dataclass
class _ControlledLoader:
    """Fake loader that writes to an in-memory dict and can inject failures.

    Protocol-compliant with :class:`trestle_etl.loader.Loader`: it
    implements ``write_batch`` and ``close``. The orchestrator treats
    the loader as an opaque object (duck-typed via ``Loader``), so a
    dataclass with these two methods is all we need.

    ``fail_at``
        If set, ``write_batch`` raises ``RuntimeError`` when
        ``batch_index == fail_at``. No row is written to the DB for
        that batch (simulating a transactional rollback — at the loader
        layer, an exception before the dict-update is equivalent to
        ROLLBACK).

    ``sigint_after``
        If set, after a successful commit on batch
        ``batch_index == sigint_after``, the loader flips the
        orchestrator's module-level ``_sigint_received`` flag to True.
        The orchestrator polls that flag right after ``state_store.save``
        and breaks out of its for-loop (Requirement 10.1, 10.2). The
        commit for batch ``sigint_after`` therefore succeeds; what stops
        is the next-page fetch.
    """

    db: dict[str, Optional[datetime]]
    fail_at: Optional[int] = None
    sigint_after: Optional[int] = None

    def __post_init__(self) -> None:
        # Starts at zero and increments on every call regardless of
        # outcome, so a follow-up call after a failure would be
        # observably "batch_index + 1" — though in the exception case
        # the orchestrator never calls us again.
        self.batch_index: int = 0

    def write_batch(self, rows: list[Row]) -> BatchResult:
        idx = self.batch_index
        self.batch_index += 1

        # Exception scenario: simulate a commit failure. No rows are
        # written. The orchestrator's for-loop propagates the exception
        # out without calling ``state_store.save`` for this batch, so
        # the prior state is preserved (Requirement 7.4, 9.4, 15.1).
        if self.fail_at is not None and idx == self.fail_at:
            raise RuntimeError(f"Injected commit failure at batch {idx}")

        # Commit: atomically update the in-memory DB. Track the max
        # ModificationTimestamp so the orchestrator can advance the
        # persisted watermark (Requirement 7.5).
        max_ts: Optional[datetime] = None
        for promoted, _raw_json in rows:
            # promoted[0] is ListingKey; promoted[1] is
            # ModificationTimestamp. Index positions are stable because
            # they match the declaration order in
            # ``trestle_etl.transformer.PROMOTED_COLUMNS``.
            listing_key: str = promoted[0]
            mod_ts: Optional[datetime] = promoted[1]
            self.db[listing_key] = mod_ts
            if mod_ts is not None and (max_ts is None or mod_ts > max_ts):
                max_ts = mod_ts

        # SIGINT scenario: set the flag AFTER the commit. The orchestrator
        # polls it after ``state_store.save``, which runs in the caller
        # immediately after we return. Setting it here mimics a signal
        # delivered between the commit of batch idx and the fetch of
        # batch idx+1.
        if self.sigint_after is not None and idx == self.sigint_after:
            orch._sigint_received = True

        # BatchResult.max_modification_timestamp is typed as
        # ``datetime`` (not Optional); our generator always produces at
        # least one record with a ModTs per batch, so ``max_ts`` is
        # never None in practice. The explicit fallback keeps the type
        # sound for the pathological empty-batch case.
        assert max_ts is not None, "Generator produces batches with ModTs"
        return BatchResult(count=len(rows), max_modification_timestamp=max_ts)

    def close(self) -> None:
        # Nothing to release: the dict is owned by the test.
        return None


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------


# ModificationTimestamp range. Bounds keep us comfortably inside
# datetime's representable space and the state-file's ISO 8601 range.
# Hypothesis returns naive datetimes from ``st.datetimes`` when the
# bounds are naive; we attach UTC in ``_scenarios``.
_MIN_TS_NAIVE = datetime(2020, 1, 1)
_MAX_TS_NAIVE = datetime(2030, 1, 1)


@st.composite
def _scenarios(
    draw: st.DrawFn,
) -> tuple[
    list[dict],           # pages (OData envelopes) fed to the fake client
    list[list[dict]],     # records grouped by page, for invariant checks
    str,                  # mode: "success" | "exception" | "sigint"
    Optional[int],        # fail_at index (exception mode only)
    Optional[int],        # sigint_after index (sigint mode only)
]:
    """Generate a pipeline run scenario.

    A scenario is:

    * 1..4 pages, each carrying 1..3 Property records with globally
      unique ``ListingKey`` values.
    * ModificationTimestamps are assigned in strictly non-decreasing
      order across the full stream (mirroring Trestle's
      ``$orderby=ModificationTimestamp asc`` contract).
    * A failure mode (``success``, ``exception``, ``sigint``) plus, for
      the non-success modes, the batch index at which to inject the
      failure.
    """
    n_pages: int = draw(st.integers(min_value=1, max_value=4))

    # Seed the stream at a random naive timestamp, then attach UTC.
    running_ts: datetime = draw(
        st.datetimes(min_value=_MIN_TS_NAIVE, max_value=_MAX_TS_NAIVE)
    ).replace(tzinfo=timezone.utc)

    pages: list[dict] = []
    by_page: list[list[dict]] = []
    global_idx = 0
    for i in range(n_pages):
        n_records: int = draw(st.integers(min_value=1, max_value=3))
        records: list[dict] = []
        for _ in range(n_records):
            # Advance by 1..86_400_000_000 microseconds (≈ up to 1 day)
            # to guarantee strictly increasing ModTs values. Strictly
            # increasing (rather than merely non-decreasing) keeps the
            # invariant-check math simple: the "max of committed" is
            # always the last committed record's ModTs.
            delta_micros: int = draw(
                st.integers(min_value=1, max_value=86_400_000_000)
            )
            running_ts = running_ts + timedelta(microseconds=delta_micros)
            records.append(
                {
                    "ListingKey": f"LK-{global_idx:06d}",
                    "ModificationTimestamp": running_ts.isoformat(),
                }
            )
            global_idx += 1

        page: dict[str, Any] = {"value": records}
        # Non-terminal page: include an ``@odata.nextLink``. The value
        # is arbitrary because the fake client ignores it (pops from
        # its FIFO queue).
        if i < n_pages - 1:
            page["@odata.nextLink"] = f"https://example.invalid/next/{i + 1}"
        pages.append(page)
        by_page.append(records)

    mode: str = draw(st.sampled_from(["success", "exception", "sigint"]))
    fail_at: Optional[int] = None
    sigint_after: Optional[int] = None
    if mode == "exception":
        fail_at = draw(st.integers(min_value=0, max_value=n_pages - 1))
    elif mode == "sigint":
        sigint_after = draw(st.integers(min_value=0, max_value=n_pages - 1))

    return pages, by_page, mode, fail_at, sigint_after


# ---------------------------------------------------------------------------
# Invariant check helpers
# ---------------------------------------------------------------------------


def _parse_iso_utc(s: str) -> datetime:
    """Parse an ISO 8601 UTC timestamp produced by ``datetime.isoformat()``.

    Hypothesis' generator emits ``YYYY-MM-DDTHH:MM:SS.ffffff+00:00``,
    which ``datetime.fromisoformat`` handles directly on Python 3.11+.
    """
    return datetime.fromisoformat(s)


def _expected_committed_page_count(
    n_pages: int,
    mode: str,
    fail_at: Optional[int],
    sigint_after: Optional[int],
) -> int:
    """Count of pages whose ``write_batch`` call fully committed + saved.

    * ``success``   — every page committed.
    * ``exception`` — pages ``0..fail_at - 1`` committed; page
                      ``fail_at`` failed before any row was written to
                      the DB and before ``state_store.save`` ran for
                      that batch (orchestrator invariant: save only
                      happens AFTER a successful commit).
    * ``sigint``    — pages ``0..sigint_after`` committed. The flag is
                      set AFTER the commit of batch ``sigint_after``,
                      so that batch still commits + saves; the
                      orchestrator then observes the flag on its
                      post-save poll and breaks out of the for-loop
                      BEFORE fetching the next page (Requirement 10.2).
    """
    if mode == "success":
        return n_pages
    if mode == "exception":
        assert fail_at is not None
        return fail_at
    if mode == "sigint":
        assert sigint_after is not None
        return sigint_after + 1
    raise AssertionError(f"unknown mode {mode!r}")


# ---------------------------------------------------------------------------
# The property test
# ---------------------------------------------------------------------------


@given(scenario=_scenarios())
@hyp_settings(
    # At least 100 examples per the design's testing-strategy budget
    # for crash-injection properties. Each example runs the real
    # orchestrator loop with real state-file IO, which stays well under
    # a second per example on local machines.
    max_examples=100,
    # Per-example wall-clock varies with tempfile IO; disable the
    # Hypothesis deadline so a slow host does not flake the run.
    deadline=None,
    suppress_health_check=[
        # ``tmp_path`` is function-scoped by design; we re-use it
        # across examples by writing a unique state file per example
        # inside the test body below.
        HealthCheck.function_scoped_fixture,
    ],
)
def test_crash_recovery_invariant(
    scenario: tuple[
        list[dict],
        list[list[dict]],
        str,
        Optional[int],
        Optional[int],
    ],
    tmp_path: Path,
) -> None:
    """Property 24 — Requirements 15.2, 15.3, 10.1, 10.2.

    Runs the real orchestrator against a fake client and a fake
    loader, exercising every failure mode at every valid injection
    point, and asserts that the state file and simulated ``property``
    table together satisfy the crash-recovery invariants (a), (b), and
    (c) after the run returns (or propagates a RuntimeError in the
    exception case).
    """
    pages, by_page, mode, fail_at, sigint_after = scenario
    n_pages = len(by_page)

    # Defensive reset: if a prior Hypothesis example raised unexpectedly
    # between the orchestrator's handler-install and handler-uninstall,
    # the SIGINT flag could still be set. The orchestrator's own
    # ``finally`` normally handles this, but paying one cheap call here
    # removes the cross-example coupling entirely.
    orch.reset_sigint_state()

    # Per-example state file so Hypothesis' retry / shrinking machinery
    # cannot observe the bytes from a prior example.
    state_path = tmp_path / f"state-{mode}-{fail_at}-{sigint_after}.json"
    # ``unlink(missing_ok=True)`` keeps the example clean even if two
    # draws happen to generate the same ``(mode, fail_at, sigint_after)``
    # triple (possible with Hypothesis' shrinking).
    state_path.unlink(missing_ok=True)

    settings_obj = _make_settings(state_path)
    state_store = StateStore(state_path)
    client = _FakeClient(pages)
    db: dict[str, Optional[datetime]] = {}
    loader = _ControlledLoader(
        db=db, fail_at=fail_at, sigint_after=sigint_after
    )

    deps = orch.Deps(
        state_store=state_store,
        client=client,
        settings=settings_obj,
        # The ``upsert_loader`` slot is duck-typed by the orchestrator;
        # it only calls ``write_batch`` and (on close) nothing at all
        # for the incremental path, which is exactly what our fake
        # supports.
        upsert_loader=loader,
    )

    # Drive the incremental path: it is the simplest orchestrator entry
    # point and exercises the same ``_run_batches`` loop (with the same
    # SIGINT-poll and save-after-commit logic) that full-sync uses. The
    # ``since`` is epoch so every record passes the strict-> filter in
    # the real extractor — but our records all have ModTs > epoch by
    # construction, so the filter is a no-op against the fake client.
    try:
        orch.run_incremental(
            deps,
            since=datetime(1970, 1, 1, tzinfo=timezone.utc),
        )
    except RuntimeError:
        # Only the exception scenario is allowed to raise. Any other
        # RuntimeError is a real test failure.
        assert mode == "exception", (
            f"Unexpected RuntimeError in mode={mode!r} "
            f"(fail_at={fail_at}, sigint_after={sigint_after})"
        )
    finally:
        # Defense in depth: the orchestrator's finally already resets
        # the flag, but if a test assertion later throws, this keeps
        # the state contained to the current example.
        orch.reset_sigint_state()

    # ------------------------------------------------------------------
    # Derive the set of records that should have committed.
    # ------------------------------------------------------------------

    committed_pages = _expected_committed_page_count(
        n_pages, mode, fail_at, sigint_after
    )
    committed_records: list[dict] = []
    for i in range(committed_pages):
        committed_records.extend(by_page[i])

    # ------------------------------------------------------------------
    # Invariant (a): state.last_mod_ts == max(ModTs over committed).
    # Requirement 15.3: the persisted watermark reflects only committed
    # data.
    # ------------------------------------------------------------------
    final_state = state_store.load()
    final_max_ts = final_state.last_modification_timestamp

    if committed_records:
        expected_max = max(
            _parse_iso_utc(r["ModificationTimestamp"])
            for r in committed_records
        )
        assert final_max_ts == expected_max, (
            f"[a] state.last_mod_ts={final_max_ts!r} expected={expected_max!r} "
            f"mode={mode} fail_at={fail_at} sigint_after={sigint_after} "
            f"committed_pages={committed_pages}"
        )
    else:
        # No batch committed — state must still show the pre-run value,
        # which for a fresh state file is None.
        assert final_max_ts is None, (
            f"[a] state.last_mod_ts={final_max_ts!r} expected=None "
            f"(no committed batches; mode={mode})"
        )

    # ------------------------------------------------------------------
    # Invariant (b): every observed record with ModTs <= state.last_mod_ts
    # appears in the DB.
    # Requirement 15.2: the state never points past records that haven't
    # been written.
    # ------------------------------------------------------------------
    # "Observed" = emitted by the fake client during this run. In the
    # exception case, the extractor emitted pages 0..fail_at (inclusive)
    # before the for-loop's exception unwound; in the sigint case, pages
    # 0..sigint_after were emitted before the break. Either way, pages
    # NOT yet fetched are irrelevant to clause (b) because they were
    # never observed.
    if mode == "exception":
        observed_pages = (fail_at or 0) + 1  # fail_at was emitted too
    elif mode == "sigint":
        observed_pages = (sigint_after or 0) + 1  # sigint batch committed
    else:
        observed_pages = n_pages

    observed_records: list[dict] = []
    for i in range(observed_pages):
        observed_records.extend(by_page[i])

    for rec in observed_records:
        rec_ts = _parse_iso_utc(rec["ModificationTimestamp"])
        if final_max_ts is not None and rec_ts <= final_max_ts:
            assert rec["ListingKey"] in db, (
                f"[b] record {rec['ListingKey']!r} with ModTs={rec_ts} "
                f"<= state.last_mod_ts={final_max_ts} is missing from the "
                f"DB. mode={mode} fail_at={fail_at} "
                f"sigint_after={sigint_after}"
            )

    # ------------------------------------------------------------------
    # Invariant (c), interpreted as the dual of (a): no committed
    # record's ModTs exceeds state.last_mod_ts. This is the safety side
    # of Requirement 15.3 — the on-disk watermark is an upper bound on
    # committed data, which lets the next incremental run use
    # ``ModTs > state.last_mod_ts`` without risking duplicate writes or
    # skipped records.
    # ------------------------------------------------------------------
    for rec in committed_records:
        rec_ts = _parse_iso_utc(rec["ModificationTimestamp"])
        assert final_max_ts is not None and rec_ts <= final_max_ts, (
            f"[c] committed record {rec['ListingKey']!r} has "
            f"ModTs={rec_ts} > state.last_mod_ts={final_max_ts}. "
            f"mode={mode} fail_at={fail_at} sigint_after={sigint_after}"
        )

    # Extra cross-check: the set of keys in the DB equals the set of
    # committed ListingKeys. Catches hypothetical bugs where the loader
    # writes a row in a batch that was supposed to have failed, or
    # skips a row in a batch that was supposed to have committed. This
    # is not a Property 24 clause but falls out "for free" and would
    # shrink to a minimal counterexample if something upstream
    # regressed.
    expected_db_keys = {r["ListingKey"] for r in committed_records}
    assert set(db.keys()) == expected_db_keys, (
        f"DB key set mismatch: db={sorted(db.keys())} "
        f"expected={sorted(expected_db_keys)} "
        f"mode={mode} fail_at={fail_at} sigint_after={sigint_after}"
    )
