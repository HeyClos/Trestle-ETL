"""Property test for replication state invariant (Property 9), snapshot variant.

Property 9 (design.md): For any sequence of committed pages during a
full sync, after each batch commit the State_Store satisfies::

    replication_in_progress      = (next_link is not None)
    replication_next_link        = the just-committed page's nextLink
                                   (or ``None`` if the page was terminal)
    last_modification_timestamp  = running max across all committed batches.

**Validates: Requirements 3.6, 3.7, 9.5**

Why this file in addition to ``test_orchestrator_replication_state.py``
----------------------------------------------------------------------
The sibling test exercises Property 9 with Hypothesis-generated pages of
random size (including empty pages) and validates the invariant against
a running max recomputed from the generated input. This file takes a
simpler, more targeted angle:

* Timestamps are fully deterministic per page (``datetime(2024, 1, i+1,
  j, 0)`` for the ``j``-th record of page ``i``). Each page's max is
  strictly greater than the previous page's max, so the expected
  running-max trajectory is trivially computed and the assertion is
  easy to read when the test fails.
* Each page carries three records, so the orchestrator's "empty batch
  still saves state" branch is not exercised here (intentionally —
  that is the sibling test's job); every save corresponds to a real
  commit by the fake loader.
* State snapshots are captured via ``dataclasses.replace(state)``.
  ``SyncState`` fields are all immutable (datetime, bool, str, None),
  so the shallow copy ``replace`` produces is a durable snapshot that
  cannot be mutated by later iterations. This is the key trick: the
  orchestrator today happens to construct a fresh ``SyncState`` per
  iteration (so reference capture would also work), but the snapshot
  defends the test against a future refactor that mutates state
  in-place.

Strategy
--------
1. Generate a fixed chain of ``n_pages`` replication pages, each with
   three records. Every record has a deterministic ``ListingKey`` and
   an ISO-8601 UTC ``ModificationTimestamp``.
2. Run ``orchestrator.run_full_sync`` against a ``FakeTrestleClient``
   (pre-scripted page queue), a ``FakeBulkLoader`` (mirrors the real
   loader's ``max_modification_timestamp`` fold), and an
   ``InstrumentedStateStore`` that snapshots every ``SyncState`` passed
   to ``save()``.
3. After the run, walk the snapshot list and assert the three
   invariants after every commit.
"""

from __future__ import annotations

import tempfile
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.config import Settings
from trestle_etl.loader import BatchResult, Row
from trestle_etl.orchestrator import Deps, run_full_sync
from trestle_etl.state import StateStore, SyncState
from trestle_etl.transformer import PROMOTED_COLUMNS

# Index of ``ModificationTimestamp`` inside the promoted-columns tuple.
# Resolved once at import time so ``FakeBulkLoader.write_batch`` does
# not repeat the ``.index`` scan on every batch.
_MOD_TS_INDEX = PROMOTED_COLUMNS.index("ModificationTimestamp")


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeTrestleClient:
    """Pre-scripted TrestleClient stand-in.

    Returns pages from a FIFO queue on each ``get`` call, regardless of
    URL or params. The orchestrator's full-sync loop only cares that
    one page comes back per GET; the extractor's contract (issue GET
    for every ``@odata.nextLink``, terminate on the absent link) is
    exercised in ``test_extractor_nextlink_stream``. Using ``pop(0)``
    rather than indexing means an unexpected extra GET raises
    ``IndexError`` instead of silently re-serving the last page.
    """

    def __init__(self, pages: list[dict[str, Any]]) -> None:
        # Defensive copy so Hypothesis is free to reuse the same
        # generated ``pages`` list during shrinking without observing
        # mid-iteration mutation.
        self._pages_queue: list[dict[str, Any]] = list(pages)

    def get(
        self, url: str, params: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        return self._pages_queue.pop(0)


class FakeBulkLoader:
    """Minimal BulkLoader stand-in for the full-sync orchestrator path.

    Exposes the four methods ``run_full_sync`` calls:

    * ``drop_secondary_indexes_if_fresh_full_sync`` / ``ensure_indexes_if_resuming``
      — no-ops; index management is not relevant to Property 9.
    * ``write_batch`` — mirrors the real ``BulkLoader.write_batch``'s
      ``max_modification_timestamp`` computation by scanning the
      ``ModificationTimestamp`` slot of each row's promoted tuple. That
      way the orchestrator's running-max fold produces a value the test
      can reconstruct directly from the input pages.
    * ``close`` — no-op; the orchestrator calls it in a ``finally``.
    """

    def drop_secondary_indexes_if_fresh_full_sync(
        self, state: SyncState
    ) -> None:
        return None

    def ensure_indexes_if_resuming(self, state: SyncState) -> None:
        return None

    def write_batch(self, rows: list[Row]) -> BatchResult:
        max_ts: Optional[datetime] = None
        for promoted, _raw in rows:
            ts = promoted[_MOD_TS_INDEX]
            if ts is not None and (max_ts is None or ts > max_ts):
                max_ts = ts
        # Our generator always emits a ModTs per record, and pages are
        # never empty (3 records each), so ``max_ts`` is always set
        # when the orchestrator actually calls ``write_batch``. Guard
        # anyway to match the real loader's contract.
        if max_ts is None:
            max_ts = datetime.min.replace(tzinfo=timezone.utc)
        return BatchResult(
            count=len(rows), max_modification_timestamp=max_ts
        )

    def close(self) -> None:
        return None


class InstrumentedStateStore:
    """Wrapper that snapshots every ``SyncState`` passed to ``save``.

    Delegates ``load`` and ``save`` to an inner real :class:`StateStore`
    so the pipeline still reads and writes a JSON document on disk,
    exercising the same serialization path production uses. The
    ``saves`` list holds **independent snapshots** of each state,
    produced via :func:`dataclasses.replace`.

    Using ``replace`` rather than capturing the reference directly
    decouples the test from whether the orchestrator happens to mutate
    ``SyncState`` instances in place. Today it doesn't (a fresh
    ``SyncState`` is constructed per iteration of ``_run_batches``),
    but a future refactor that does would silently invalidate a
    reference-capture test. The shallow copy is sufficient because
    every ``SyncState`` field is immutable.
    """

    def __init__(self, inner: StateStore) -> None:
        self._inner = inner
        self.saves: list[SyncState] = []

    def load(self) -> SyncState:
        return self._inner.load()

    def save(self, state: SyncState) -> None:
        # ``replace(state)`` with no field overrides produces a shallow
        # copy; SyncState fields are all immutable, so the snapshot is
        # durable regardless of what the caller does with the original.
        self.saves.append(replace(state))
        self._inner.save(state)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_settings(state_file_path: Path) -> Settings:
    """Construct a ``Settings`` with dummy values pointing at ``state_file_path``.

    The orchestrator / extractor only read ``trestle_base_url`` and
    ``default_page_size`` when building the initial request URL. The
    fake client ignores both (it pops from a scripted queue), so the
    specific values do not affect Property 9's claims.
    """
    return Settings(
        trestle_base_url="https://example.invalid/trestle/odata",
        trestle_token_url="https://example.invalid/oidc/token",
        client_id="test-client-id",
        client_secret="test-client-secret",
        mysql_host="localhost",
        mysql_port=3306,
        mysql_user="user",
        mysql_password="password",
        mysql_database="trestle",
        state_file_path=state_file_path,
        default_page_size=1000,
    )


def build_pages(
    pages_ts: list[list[datetime]],
) -> list[dict[str, Any]]:
    """Build replication-endpoint response envelopes from per-page timestamps.

    ``pages_ts[i]`` is the list of ``ModificationTimestamp`` values for
    the records on page ``i``. Each record gets a deterministic
    ``ListingKey`` (``LK-{i}-{j}``) and the provided timestamp,
    serialized as ISO 8601 so the transformer's Pydantic parser sees
    exactly what the real Trestle API would return.

    The ``@odata.nextLink`` for page ``i`` is ``nextlink{i+1}`` for all
    but the terminal page, which has no nextLink at all. Those synthetic
    URLs are what the orchestrator persists to the state file and what
    this test compares against later, so their shape is part of the
    contract the test is asserting.
    """
    n_pages = len(pages_ts)
    pages: list[dict[str, Any]] = []
    for i, record_timestamps in enumerate(pages_ts):
        records: list[dict[str, Any]] = []
        for j, ts in enumerate(record_timestamps):
            records.append(
                {
                    "ListingKey": f"LK-{i}-{j}",
                    "ModificationTimestamp": ts.isoformat(),
                }
            )
        page: dict[str, Any] = {"value": records}
        is_terminal = i == n_pages - 1
        if not is_terminal:
            page["@odata.nextLink"] = f"nextlink{i + 1}"
        pages.append(page)
    return pages


# ---------------------------------------------------------------------------
# Property test
# ---------------------------------------------------------------------------


@given(n_pages=st.integers(min_value=1, max_value=5))
@settings(
    max_examples=50,
    # File I/O plus Hypothesis per-example overhead can exceed the
    # default deadline on slow hosts; the property is purely logical
    # so a timeout would be a flake, not a real failure.
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_replication_state_invariant_per_batch(n_pages: int) -> None:
    """Property 9 (Requirements 3.6, 3.7, 9.5) — per-batch snapshot view.

    For a deterministic chain of ``n_pages`` pages with three records
    each, assert that the ``InstrumentedStateStore`` captured one save
    per page and that each snapshot satisfies the three invariants.
    """
    # Per-page timestamp table. Page ``i`` carries three records whose
    # ModTs are ``datetime(2024, 1, i+1, 0..2, 0)``; the per-page max
    # is therefore ``datetime(2024, 1, i+1, 2, 0)`` and the running max
    # is strictly increasing across pages.
    pages_ts: list[list[datetime]] = [
        [
            datetime(2024, 1, i + 1, j, 0, tzinfo=timezone.utc)
            for j in range(3)
        ]
        for i in range(n_pages)
    ]
    pages = build_pages(pages_ts)

    # The ``@odata.nextLink`` the orchestrator should have written for
    # each page. For all but the terminal page this matches the
    # ``nextlink{i+1}`` that ``build_pages`` baked into the envelopes;
    # the terminal page's saved link is ``None``.
    expected_next_links: list[Optional[str]] = [
        f"nextlink{i + 1}" if i < n_pages - 1 else None
        for i in range(n_pages)
    ]

    # TemporaryDirectory inside the test so each Hypothesis example
    # gets a fresh state-file path. A ``tmp_path`` fixture would share
    # one path across all examples and produce misleading save counts
    # as state accumulates across iterations.
    with tempfile.TemporaryDirectory() as tmp:
        state_path = Path(tmp) / "state.json"
        instrumented = InstrumentedStateStore(StateStore(state_path))
        deps = Deps(
            # Duck-typed: ``Deps`` type hints reference concrete
            # classes, but dataclasses do not enforce types at runtime.
            # Our fakes implement the exact methods the orchestrator
            # calls, which is what matters.
            state_store=instrumented,  # type: ignore[arg-type]
            client=FakeTrestleClient(pages),  # type: ignore[arg-type]
            settings=make_settings(state_path),
            bulk_loader=FakeBulkLoader(),  # type: ignore[arg-type]
        )
        run_full_sync(deps)

    # One save per page, since every page has a non-empty batch here
    # (three records per page) and therefore triggers a commit + save.
    assert len(instrumented.saves) == n_pages, (
        f"Expected {n_pages} state saves (one per committed page), got "
        f"{len(instrumented.saves)}"
    )

    # Walk the snapshots in order and verify the per-batch invariant.
    running_max: Optional[datetime] = None
    for i, snapshot in enumerate(instrumented.saves):
        # Running max across pages 0..i inclusive. Each page's max is
        # the last timestamp in its per-page list (hour 2, the maximum
        # hour value among the three generated hours 0/1/2).
        page_max = max(pages_ts[i])
        running_max = (
            page_max if running_max is None else max(running_max, page_max)
        )

        # --- Invariant 1: last_modification_timestamp = running max
        assert snapshot.last_modification_timestamp == running_max, (
            f"Page {i}: last_modification_timestamp="
            f"{snapshot.last_modification_timestamp!r} expected="
            f"{running_max!r}"
        )

        # --- Invariant 2: replication_next_link matches page's nextLink
        assert snapshot.replication_next_link == expected_next_links[i], (
            f"Page {i}: replication_next_link="
            f"{snapshot.replication_next_link!r} expected="
            f"{expected_next_links[i]!r}"
        )

        # --- Invariant 3: replication_in_progress = (next_link is not None)
        assert snapshot.replication_in_progress == (
            expected_next_links[i] is not None
        ), (
            f"Page {i}: replication_in_progress="
            f"{snapshot.replication_in_progress!r} but next_link="
            f"{expected_next_links[i]!r}"
        )
