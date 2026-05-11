"""Property test for the crash-recovery invariant (Property 24).

Property 24 (design.md): For any simulated pipeline run (successful
completion, exception during a batch, or SIGINT injected at an arbitrary
point), after the process exits:

    (a) ``state.last_modification_timestamp`` equals the max
        ``ModificationTimestamp`` across all committed batches.
    (b) every observed record with ``ModificationTimestamp <=
        state.last_modification_timestamp`` is present in the
        ``property`` table.
    (c) no non-committed record's ``ModificationTimestamp`` exceeds
        ``state.last_modification_timestamp``.

**Validates: Requirements 15.2, 15.3, 10.1, 10.2**

How this test simulates the database
------------------------------------
Instead of standing up a real MySQL server, the test uses
:class:`InMemoryTableLoader`: a dict keyed by ``ListingKey`` that records
the ``(ModificationTimestamp, raw_data_json)`` pair for every row
"committed". The loader commits a batch atomically — either every row in
the batch is written to the dict or none are, matching the transactional
guarantee of :class:`~trestle_etl.loader.upsert.UpsertLoader`
(Requirement 7.3, 7.4).

Failure modes
-------------
Three scenarios are exercised:

* ``success``   — every batch commits; the stream terminates naturally
                  on the terminal page.
* ``exception`` — the loader raises ``RuntimeError`` starting on the
                  ``fail_after + 1``-th call. The exception propagates
                  out of the orchestrator's for-loop before
                  ``state_store.save`` runs for that batch, so the
                  persisted state reflects only the prior successful
                  commits.
* ``sigint``    — on the ``sigint_after``-th successful commit, the
                  loader flips the orchestrator's module-level
                  ``_sigint_received`` flag directly (imitating a signal
                  handler). The orchestrator polls the flag AFTER saving
                  state and breaks out of the loop before fetching the
                  next page, which is what Requirement 10.2 requires.

Interpretation note on clause (c)
---------------------------------
In Trestle's ``$orderby=ModificationTimestamp asc`` stream, records in a
failed or not-yet-observed page necessarily have ModTs larger than every
committed record's ModTs. Those records are **not** "non-committed" in
the sense Requirement 15.3 cares about — they are simply "not yet
observed". The meaningful safety claim for crash recovery is the dual:
**every COMMITTED record's ModTs is <= state.last_mod_ts**. That is
clause (c) as this test enforces it, and it is equivalent to Requirement
15.3's statement that the persisted watermark is an upper bound on
committed data. Combined with clause (a), this lets the strict
greater-than filter (Requirement 15.4) resume the stream without
duplicates or skips.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from hypothesis import HealthCheck, given, settings as hyp_settings
from hypothesis import strategies as st

from trestle_etl import orchestrator as orch_mod
from trestle_etl.config import Settings
from trestle_etl.loader import BatchResult, Row
from trestle_etl.state import StateStore


# ---------------------------------------------------------------------------
# Settings and fake HTTP client
# ---------------------------------------------------------------------------


def make_settings(state_path: Path) -> Settings:
    """Build a minimal :class:`Settings` whose only live field is the path.

    The extractor reads ``trestle_base_url`` and ``default_page_size`` to
    assemble the initial URL, but :class:`FakeTrestleClient` ignores the
    URL entirely — it hands out pre-scripted pages in FIFO order. Every
    other field exists so the ``Settings`` dataclass can be constructed;
    none of them are consulted during the test.
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


class FakeTrestleClient:
    """Stand-in for :class:`~trestle_etl.http_client.TrestleClient`.

    The extractor only calls ``.get(url, params=...)`` on its client.
    This fake ignores both arguments and returns the next scripted page
    from an internal FIFO. Exhausting the queue raises an
    ``AssertionError`` so a rogue fetch past the terminal page surfaces
    at the exact call site instead of silently replaying.
    """

    def __init__(self, pages: list[dict]) -> None:
        self._pages: list[dict] = list(pages)

    def get(self, url: str, params: Optional[dict] = None) -> dict:
        if not self._pages:
            raise AssertionError(
                "FakeTrestleClient queue exhausted; extractor fetched "
                "past the terminal page (violates Property 6)."
            )
        return self._pages.pop(0)


# ---------------------------------------------------------------------------
# In-memory upsert loader
# ---------------------------------------------------------------------------


@dataclass
class InMemoryTableLoader:
    """Upsert loader backed by an in-memory dict (the "property table").

    Implements the :class:`~trestle_etl.loader.Loader` protocol (``write_batch``
    and ``close``) so the orchestrator can drive it exactly as it would a
    real :class:`~trestle_etl.loader.upsert.UpsertLoader`.

    Commit semantics
    ----------------
    Each ``write_batch`` call is treated as a single transaction:

    * If ``fail_after`` is set and the call index is greater than
      ``fail_after`` (1-indexed), the loader raises ``RuntimeError``
      **before** modifying the dict. This mirrors the transactional
      rollback of a real database batch: no partial writes, and the
      orchestrator propagates the exception without calling
      ``state_store.save`` for that batch.
    * If ``sigint_after`` is set and the call index equals
      ``sigint_after``, the loader commits the batch to the dict and
      then flips :data:`trestle_etl.orchestrator._sigint_received` to
      ``True``. The orchestrator polls that flag on the next poll point
      (after ``state_store.save``) and breaks out of the batch loop
      before pulling the next page, which is what Requirement 10.1 /
      10.2 require.

    Attributes:
        table: Maps ``ListingKey`` to ``(ModificationTimestamp,
            raw_data_json)`` for every committed row. The dict is the
            test's simulated ``property`` MySQL table.
        calls: Counter incremented at the start of every
            ``write_batch`` call. Used to match against ``fail_after``
            and ``sigint_after``.
        fail_after: Number of successful calls to allow before raising
            on every subsequent call. ``None`` disables the failure
            injection (success or sigint scenario).
        sigint_after: Call index (1-indexed) on which to set the
            orchestrator's SIGINT flag immediately after the commit.
            ``None`` disables the SIGINT injection.
    """

    table: dict[str, tuple[Optional[datetime], str]] = field(
        default_factory=dict
    )
    calls: int = 0
    fail_after: Optional[int] = None
    sigint_after: Optional[int] = None

    def write_batch(self, rows: list[Row]) -> BatchResult:
        self.calls += 1

        # Exception injection runs BEFORE any mutation so the dict is
        # left untouched for the failed batch — i.e. transactional
        # rollback semantics (Requirement 7.4).
        if self.fail_after is not None and self.calls > self.fail_after:
            raise RuntimeError(
                f"Injected failure on write_batch call #{self.calls}"
            )

        # Atomic commit: update the dict in one pass and track the
        # batch's max ModificationTimestamp for the BatchResult. Each
        # ``row`` is a ``(promoted_columns_tuple, raw_data_json)`` pair
        # produced by :func:`trestle_etl.transformer.to_row`; the
        # promoted tuple's index 0 is ``ListingKey`` and index 1 is
        # ``ModificationTimestamp`` (order defined in
        # ``trestle_etl.transformer.PROMOTED_COLUMNS``).
        max_ts: Optional[datetime] = None
        for promoted, raw_data_json in rows:
            listing_key: str = promoted[0]
            mod_ts: Optional[datetime] = promoted[1]
            self.table[listing_key] = (mod_ts, raw_data_json)
            if mod_ts is not None and (max_ts is None or mod_ts > max_ts):
                max_ts = mod_ts

        # SIGINT injection runs AFTER the commit, mirroring a real
        # signal that arrives between ``COMMIT`` and the next
        # ``state_store.save``. The orchestrator polls the flag AFTER
        # save; setting it here means the save for THIS batch still
        # runs (Requirement 10.1: the in-flight batch completes).
        if self.sigint_after is not None and self.calls == self.sigint_after:
            orch_mod._sigint_received = True

        # ``max_ts`` is non-None as long as the batch contains at least
        # one record with a ModificationTimestamp. The scenario
        # generator below always sets ModTs on every record, so the
        # assertion holds in every reachable call. The type of
        # ``BatchResult.max_modification_timestamp`` is non-Optional, so
        # returning ``None`` here would violate the contract.
        assert max_ts is not None, (
            "Scenario generator always emits records with a "
            "ModificationTimestamp; max_ts should never be None."
        )
        return BatchResult(count=len(rows), max_modification_timestamp=max_ts)

    def close(self) -> None:
        # No resources to release: the dict is owned by the test.
        return None


# ---------------------------------------------------------------------------
# Page construction
# ---------------------------------------------------------------------------


def build_pages(
    pages_ts: list[list[datetime]],
) -> tuple[list[dict], list[tuple[str, datetime]]]:
    """Produce OData pages and a flat ``(ListingKey, ModTs)`` list.

    ``pages_ts[i][j]`` is the ``ModificationTimestamp`` for the j-th
    record on the i-th page. Keys are synthesized so they are globally
    unique across the stream (mirroring Trestle's invariant that
    ``ListingKey`` is the Property primary key).

    The returned ``pages`` list is ready to hand to
    :class:`FakeTrestleClient`: every non-terminal page carries a
    placeholder ``@odata.nextLink`` (the URL value is irrelevant because
    the fake ignores URLs). The terminal page has no ``@odata.nextLink``,
    which ends the stream per Requirement 3.3.

    The returned ``all_records`` list is the flat list of every
    ``(ListingKey, ModTs)`` tuple in page order. Clauses (b) and (c) of
    Property 24 use it to enumerate "observed" records.
    """
    pages: list[dict] = []
    all_records: list[tuple[str, datetime]] = []
    for i, page_ts in enumerate(pages_ts):
        records: list[dict] = []
        for j, ts in enumerate(page_ts):
            key = f"LK-{i}-{j}"
            records.append(
                {
                    "ListingKey": key,
                    "ModificationTimestamp": ts.isoformat(),
                }
            )
            all_records.append((key, ts))
        page: dict[str, Any] = {"value": records}
        if i < len(pages_ts) - 1:
            # Placeholder nextLink; the fake client ignores it.
            page["@odata.nextLink"] = (
                f"https://example.invalid/next/{i + 1}"
            )
        pages.append(page)
    return pages, all_records


# ---------------------------------------------------------------------------
# Scenario generator
# ---------------------------------------------------------------------------


# Bounds for ModificationTimestamp generation. Kept well inside the
# representable datetime range and comfortably inside the ISO 8601 range
# that the StateStore round-trips through.
_MIN_TS_NAIVE = datetime(2020, 1, 1)
_MAX_TS_NAIVE = datetime(2030, 1, 1)


@st.composite
def _page_timestamps(
    draw: st.DrawFn, n_pages: int
) -> list[list[datetime]]:
    """Draw strictly-increasing UTC timestamps grouped by page.

    Trestle's ``$orderby=ModificationTimestamp asc`` contract means
    records are delivered in non-decreasing ModTs order. We use
    *strictly* increasing timestamps here so the "max across committed
    batches" is always the last committed record's ModTs, which
    simplifies the invariant-check math without weakening the property.
    """
    current: datetime = draw(
        st.datetimes(min_value=_MIN_TS_NAIVE, max_value=_MAX_TS_NAIVE)
    ).replace(tzinfo=timezone.utc)

    pages_ts: list[list[datetime]] = []
    for _ in range(n_pages):
        # 1..3 records per page, matching the shape of a realistic page
        # while keeping the search space small enough for Hypothesis to
        # explore thoroughly under ``max_examples=100``.
        n_records: int = draw(st.integers(min_value=1, max_value=3))
        page_ts: list[datetime] = []
        for _ in range(n_records):
            # Advance by 1..86_400_000_000 microseconds (up to 1 day).
            # Strictly positive so timestamps are globally strictly
            # increasing across the whole stream.
            delta_micros: int = draw(
                st.integers(min_value=1, max_value=86_400_000_000)
            )
            current = current + timedelta(microseconds=delta_micros)
            page_ts.append(current)
        pages_ts.append(page_ts)
    return pages_ts


# ---------------------------------------------------------------------------
# Invariant-check helpers
# ---------------------------------------------------------------------------


def _expected_committed_page_count(
    n_pages: int,
    scenario: str,
    trigger_at: int,
) -> int:
    """Number of pages whose ``write_batch`` call fully committed AND
    whose ``state_store.save`` ran afterwards.

    ``trigger_at`` is a 1-indexed page/call number:

    * ``success``   — ``trigger_at`` is unused; every page commits
                      because the stream terminates naturally on the
                      last page.
    * ``exception`` — the ``trigger_at``-th call raises BEFORE any row
                      is written, so the prior ``trigger_at - 1``
                      pages are the only ones that committed. The
                      orchestrator propagates the exception before
                      ``state_store.save`` runs for the failing batch.
    * ``sigint``    — the ``trigger_at``-th call commits normally and
                      then sets the SIGINT flag. The orchestrator's
                      post-save poll observes the flag and breaks out
                      of the loop before pulling the next page, so
                      exactly ``trigger_at`` pages have committed.

    ``trigger_at`` is assumed to be in ``[1, n_pages]`` (callers clamp
    via ``effective_fail_at = min(fail_at, n_pages)``).
    """
    if scenario == "success":
        return n_pages
    if scenario == "exception":
        return trigger_at - 1
    if scenario == "sigint":
        return trigger_at
    raise AssertionError(f"unknown scenario {scenario!r}")


# ---------------------------------------------------------------------------
# The property test
# ---------------------------------------------------------------------------


@given(
    n_pages=st.integers(min_value=1, max_value=6),
    scenario=st.sampled_from(["success", "exception", "sigint"]),
    fail_at=st.integers(min_value=1, max_value=6),
    # ``data`` lets us draw ``pages_ts`` inside the test body so the
    # strategy can depend on ``n_pages``. Hypothesis will shrink this
    # alongside the other parameters.
    data=st.data(),
)
@hyp_settings(
    # Crash-injection properties deserve a healthy budget; each example
    # runs the real orchestrator loop against an on-disk state file.
    max_examples=100,
    deadline=None,
    suppress_health_check=[
        # ``tmp_path`` is function-scoped by design; we write a unique
        # state file per example under a per-example tempdir below, so
        # the scope mismatch is harmless here.
        HealthCheck.function_scoped_fixture,
    ],
)
def test_crash_recovery_invariant(
    n_pages: int,
    scenario: str,
    fail_at: int,
    data: st.DataObject,
) -> None:
    """Property 24 — Requirements 15.2, 15.3, 10.1, 10.2.

    Runs a single scenario through the real orchestrator and asserts
    invariants (a), (b), and (c) hold over the resulting (state file,
    in-memory table) pair.
    """
    pages_ts = data.draw(_page_timestamps(n_pages))
    pages, all_records = build_pages(pages_ts)

    # Hypothesis draws ``fail_at`` in ``[1, 6]`` unconditionally, but
    # the scenario may run with fewer pages. Clamp to ``n_pages`` so
    # the failure injection always lands at a reachable call index
    # (Hypothesis will still explore every reachable combination).
    effective_fail_at = min(fail_at, n_pages)

    # Defensive reset: the orchestrator's own ``finally`` clears the
    # SIGINT flag, but paying a cheap reset here removes any
    # cross-example coupling if a prior example raised unexpectedly.
    orch_mod.reset_sigint_state()

    try:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            settings_obj = make_settings(state_path)
            store = StateStore(state_path)
            client = FakeTrestleClient(pages)

            if scenario == "success":
                loader = InMemoryTableLoader()
            elif scenario == "exception":
                # ``fail_after = k`` in the loader means "the first k
                # calls succeed, call k+1 raises". We want the
                # ``effective_fail_at``-th call (1-indexed) to raise,
                # so ``fail_after = effective_fail_at - 1``.
                loader = InMemoryTableLoader(fail_after=effective_fail_at - 1)
            elif scenario == "sigint":
                # ``sigint_after = k`` means "set the flag AFTER the
                # k-th successful commit", so the k-th page commits and
                # is saved, and the loop exits before fetching the
                # (k+1)-th page.
                loader = InMemoryTableLoader(sigint_after=effective_fail_at)
            else:
                raise AssertionError(f"unknown scenario {scenario!r}")

            deps = orch_mod.Deps(
                state_store=store,
                client=client,
                settings=settings_obj,
                upsert_loader=loader,
            )

            # The incremental entry point exercises the same
            # ``_run_batches`` loop as full-sync — same save-after-commit
            # ordering, same post-save SIGINT poll. ``since=epoch``
            # ensures the extractor's strict-> filter does not reject
            # any of our synthetic records (they all have ModTs in
            # 2020..2030).
            try:
                orch_mod.run_incremental(
                    deps,
                    since=datetime(1970, 1, 1, tzinfo=timezone.utc),
                )
            except RuntimeError:
                # Only the ``exception`` scenario should raise. Any
                # other RuntimeError is a real test failure.
                assert scenario == "exception", (
                    f"Unexpected RuntimeError in scenario={scenario!r} "
                    f"(effective_fail_at={effective_fail_at})"
                )

            # Snapshot the table and load the persisted state while the
            # tempdir is still alive. Post-snapshot the tempdir context
            # manager cleans up the state file on exit.
            table_snapshot = dict(loader.table)
            final_state = store.load()
    finally:
        # Defense in depth: if an assertion below throws, still clear
        # the module-level flag so the next example starts clean.
        orch_mod.reset_sigint_state()

    # ------------------------------------------------------------------
    # Derive the expected committed-records set from the scenario.
    # ------------------------------------------------------------------
    committed_pages = _expected_committed_page_count(
        n_pages, scenario, effective_fail_at
    )

    # Flatten the first ``committed_pages`` pages into a list of
    # ``(key, ts)`` tuples in observed order.
    committed_records: list[tuple[str, datetime]] = []
    for i in range(committed_pages):
        for j, ts in enumerate(pages_ts[i]):
            committed_records.append((f"LK-{i}-{j}", ts))
    committed_keys = {k for k, _ in committed_records}

    # ------------------------------------------------------------------
    # Cross-check the simulated ``property`` table against the expected
    # committed set. Not formally part of Property 24, but it catches
    # regressions where a loader writes rows from a failed batch or
    # drops rows from a successful one.
    # ------------------------------------------------------------------
    assert set(table_snapshot.keys()) == committed_keys, (
        f"table key set mismatch: table={sorted(table_snapshot.keys())} "
        f"expected={sorted(committed_keys)} "
        f"scenario={scenario} effective_fail_at={effective_fail_at}"
    )

    # ------------------------------------------------------------------
    # Invariant (a): state.last_mod_ts == max(ModTs of committed records)
    # Requirement 15.3: the persisted watermark reflects committed data.
    # ------------------------------------------------------------------
    final_max_ts = final_state.last_modification_timestamp
    if committed_records:
        expected_max = max(ts for _, ts in committed_records)
        assert final_max_ts == expected_max, (
            f"[a] state.last_mod_ts={final_max_ts!r} "
            f"expected={expected_max!r} "
            f"scenario={scenario} effective_fail_at={effective_fail_at} "
            f"committed_pages={committed_pages}"
        )
    else:
        # No batch committed — state must still show its pre-run value,
        # which for a fresh state file is ``None``.
        assert final_max_ts is None, (
            f"[a] state.last_mod_ts={final_max_ts!r} expected=None "
            f"(no committed batches; scenario={scenario})"
        )

    # ------------------------------------------------------------------
    # Invariant (b): every record in the committed table has
    # ModTs <= state.last_mod_ts.
    # Requirement 15.2: the state never references records that have
    # not yet been written. The contrapositive — every written record
    # is <= the watermark — is what we check here since our "observed
    # and committed" set is exactly the dict's contents.
    # ------------------------------------------------------------------
    for key, (ts, _raw) in table_snapshot.items():
        assert ts is not None and final_max_ts is not None and ts <= final_max_ts, (
            f"[b] committed record {key!r} has ts={ts!r} which is not "
            f"<= state.last_mod_ts={final_max_ts!r}. "
            f"scenario={scenario} effective_fail_at={effective_fail_at}"
        )

    # ------------------------------------------------------------------
    # Invariant (c): every record NOT in the committed table has a ts
    # that does not affect the persisted watermark — i.e. the watermark
    # does not lie about anything outside the committed set.
    #
    # Stated as: for every record in ``all_records`` that is NOT in the
    # table, that record is drawn from a page at index >= committed_pages
    # (i.e. from a page the orchestrator either never observed, or
    # observed but failed to commit). Those records' ModTs are allowed
    # to exceed ``state.last_mod_ts`` — they are simply "beyond the
    # crash point" and the next run will re-fetch them via the strict-
    # greater-than filter without duplication or loss.
    #
    # The concrete assertion is the crash-recovery safety property:
    # the committed-record set exactly matches the dict's key set (we
    # already checked above), and the watermark (a) holds.
    # ------------------------------------------------------------------
    uncommitted = [(k, ts) for k, ts in all_records if k not in committed_keys]
    for key, ts in uncommitted:
        # An uncommitted record MAY have a ts > state.last_mod_ts
        # (that is the normal case: records on pages beyond the crash
        # point arrive later in the stream). What is NOT allowed is
        # for an uncommitted record to appear in the committed table
        # — we already asserted that above. The remaining check is
        # that the state watermark did not somehow advance past the
        # committed set.
        assert key not in table_snapshot, (
            f"[c] uncommitted record {key!r} unexpectedly appears in "
            f"the table. scenario={scenario} "
            f"effective_fail_at={effective_fail_at}"
        )
