"""Property test for replication state invariant (Property 9).

Property 9 (design.md): For any sequence of committed pages during a
full sync, after each batch commit the State_Store satisfies::

    replication_in_progress      = (next_link is not None)
    replication_next_link        = the just-committed page's nextLink
                                   (or ``None`` if the page was terminal)
    last_modification_timestamp  = max(ModificationTimestamp) across
                                   every batch committed so far.

**Validates: Requirements 3.6, 3.7, 9.5**

Implementation notes
--------------------
* The orchestrator's full-sync path pipes the extractor through
  ``BulkLoader.write_batch`` and saves state AFTER the commit. This test
  exercises that rhythm with fakes so the assertion can inspect the
  *sequence* of saves, not just the terminal state -- the invariant must
  hold after every batch, not only after the run completes.

* A :class:`FakeTrestleClient` returns pre-scripted replication pages so
  the orchestrator's per-page loop is driven without the HTTP stack.

* A :class:`FakeBulkLoader` exposes the minimal surface the full-sync
  code path calls (``drop_secondary_indexes_if_fresh_full_sync``,
  ``ensure_indexes_if_resuming``, ``write_batch``, ``close``). The real
  loader's CSV and ``LOAD DATA LOCAL INFILE`` path is irrelevant to the
  invariants this property asserts -- we only need ``write_batch`` to
  compute the batch's max ``ModificationTimestamp`` the same way the
  real loader does so the running-max folded into state is comparable.

* A :class:`RecordingStateStore` wraps a real :class:`StateStore` and
  appends every ``SyncState`` passed to ``save()`` to an in-memory list.
  The orchestrator constructs a fresh ``SyncState`` per iteration so
  reference-capture is safe -- there is no shared mutable state to copy.
"""

from __future__ import annotations

import tempfile
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.config import Settings
from trestle_etl.loader import BatchResult, Row
from trestle_etl.orchestrator import Deps, run_full_sync
from trestle_etl.state import StateStore, SyncState
from trestle_etl.transformer import PROMOTED_COLUMNS

# Index of ``ModificationTimestamp`` inside the promoted-columns tuple.
# Resolved once at import time so ``FakeBulkLoader.write_batch`` does not
# repeat the ``.index`` scan on every batch.
_MOD_TS_INDEX = PROMOTED_COLUMNS.index("ModificationTimestamp")


def _make_settings() -> Settings:
    """Construct a ``Settings`` with dummy values.

    The orchestrator / extractor only read ``trestle_base_url`` and
    ``default_page_size`` for the initial request URL. Every subsequent
    page follows the server-supplied ``@odata.nextLink`` verbatim, so
    the specific values do not affect Property 9's claims about state
    contents after each commit.
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
        state_file_path=Path("sync_state.json"),
        default_page_size=1000,
    )


class FakeTrestleClient:
    """Pre-scripted TrestleClient stand-in.

    Returns pages from a FIFO queue on each ``get`` call, regardless of
    URL or params. That is sufficient for the orchestrator's
    full-sync loop: the extractor's only behavioral contract is
    "issue one GET per yielded page", and we test that separately
    in ``test_extractor_nextlink_stream``.
    """

    def __init__(self, pages: list[dict[str, Any]]) -> None:
        # Defensive copy so the fixture owner can safely re-use the
        # generator-produced ``pages`` list without observing
        # mid-iteration mutation.
        self._pages_queue: list[dict[str, Any]] = list(pages)

    def get(
        self, url: str, params: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        # ``pop(0)`` rather than index so an unexpected extra GET raises
        # IndexError instead of silently re-returning the last page.
        return self._pages_queue.pop(0)


class FakeBulkLoader:
    """Minimal BulkLoader stand-in for the full-sync orchestrator path.

    Implements the four methods the orchestrator actually invokes:

    * ``drop_secondary_indexes_if_fresh_full_sync(state)`` -- called on
      the fresh full-sync branch; a no-op here because index management
      is out of scope for Property 9.
    * ``ensure_indexes_if_resuming(state)`` -- called on the resume
      branch (not exercised in this test, but stubbed for completeness
      so the fake keeps working if the orchestrator's decision logic
      changes).
    * ``write_batch(rows)`` -- computes ``max_modification_timestamp``
      the same way the real loader does (scan promoted[mod_ts_index])
      so the orchestrator's running-max fold produces a value the test
      can reconstruct from the input pages.
    * ``close()`` -- called unconditionally in the orchestrator's
      ``finally`` block; a no-op here.
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
        # Mirror ``BulkLoader.write_batch``'s sentinel for "no records
        # carried a ModificationTimestamp". Our generator always sets
        # one, so the sentinel path is unreachable in this test; keep
        # the branch for parity with the real loader's contract.
        if max_ts is None:
            max_ts = datetime.min.replace(tzinfo=UTC)
        return BatchResult(
            count=len(rows), max_modification_timestamp=max_ts
        )

    def close(self) -> None:
        return None


class RecordingStateStore:
    """Wrapper that records every ``SyncState`` passed to ``save``.

    Delegates ``load`` and ``save`` to an inner real :class:`StateStore`
    so the pipeline still reads and writes a JSON document on disk
    (which exercises the same code path the production pipeline uses).
    The in-memory ``saves`` list is what the test inspects.

    Reference-capture is safe because the orchestrator constructs a
    fresh ``SyncState`` per iteration (see the ``new_state = SyncState(...)``
    line in ``_run_batches``), so the saved instances do not share
    mutable state with subsequent iterations.
    """

    def __init__(self, inner: StateStore) -> None:
        self._inner = inner
        self.saves: list[SyncState] = []

    def load(self) -> SyncState:
        return self._inner.load()

    def save(self, state: SyncState) -> None:
        self.saves.append(state)
        self._inner.save(state)


def _parse_mod_ts(value: str) -> datetime:
    """Mirror the Pydantic model validator: ISO 8601 -> UTC datetime.

    We keep this equivalent to ``Property._normalize_modification_timestamp``
    so the running-max computed here matches what the orchestrator folds
    into state (via the loader, via the promoted-columns tuple).
    """
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Hypothesis strategy
# ---------------------------------------------------------------------------


@st.composite
def _scenarios(draw: st.DrawFn) -> list[dict[str, Any]]:
    """Generate a full-sync scenario: ``n_pages`` OData response envelopes.

    Each envelope carries zero to four records and, when not the
    terminal page, an ``@odata.nextLink`` pointing at a synthetic URL.
    Zero-record pages exercise the orchestrator's
    "empty batch still saves state" path (the state must still advance
    to the new nextLink even when the loader is not invoked).

    Every record carries a deterministic ``ListingKey`` and a
    UTC-aware ``ModificationTimestamp`` formatted as ISO 8601. The
    transformer parses the string back to a UTC-aware datetime; our
    assertion recomputes the running max from the same input, so the
    expected and observed max values are directly comparable.
    """
    n_pages = draw(st.integers(min_value=1, max_value=6))
    pages: list[dict[str, Any]] = []
    for i in range(n_pages):
        n_records = draw(st.integers(min_value=0, max_value=4))
        records: list[dict[str, Any]] = []
        for j in range(n_records):
            ts = draw(
                st.datetimes(
                    min_value=datetime(2020, 1, 1),
                    max_value=datetime(2025, 1, 1),
                    timezones=st.just(timezone.utc),
                )
            )
            records.append(
                {
                    # Deterministic, globally unique keys avoid any
                    # cross-batch collision concerns for loaders that
                    # might care; the fake loader in this test does not.
                    "ListingKey": f"LK-{i}-{j}",
                    "ModificationTimestamp": ts.isoformat(),
                }
            )
        page: dict[str, Any] = {"value": records}
        is_terminal = i == n_pages - 1
        if not is_terminal:
            # Synthetic nextLink that the fake client will not actually
            # follow as a URL; it only needs to be a non-None string so
            # the extractor signals "more pages to come".
            page["@odata.nextLink"] = (
                f"https://example.invalid/trestle/odata/next/{i + 1}"
            )
        pages.append(page)
    return pages


# ---------------------------------------------------------------------------
# Property test
# ---------------------------------------------------------------------------


@given(pages=_scenarios())
@settings(
    max_examples=100,
    # The orchestrator's file I/O plus Hypothesis's per-example overhead
    # can exceed the default per-example deadline on slow hosts; the
    # property is purely logical so timing out would be a flake, not a
    # real failure.
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_replication_state_invariant(pages: list[dict[str, Any]]) -> None:
    """Property 9 (Requirements 3.6, 3.7, 9.5).

    Run the orchestrator's full-sync path against a fake client, fake
    bulk loader, and a state store that records every save. After the
    run, verify -- for each saved state in order -- that:

    1. ``replication_in_progress`` is ``True`` iff that page carried a
       non-null ``@odata.nextLink`` (Requirement 3.6, 3.7).
    2. ``replication_next_link`` equals the value of that page's
       ``@odata.nextLink`` (``None`` if the page was terminal).
    3. ``last_modification_timestamp`` equals the max
       ``ModificationTimestamp`` across every record observed in pages
       0..i (Requirement 9.5, 4.5).
    """
    settings_obj = _make_settings()
    client = FakeTrestleClient(pages)
    loader = FakeBulkLoader()

    # TemporaryDirectory inside the test so each Hypothesis example
    # gets a fresh state-file path. Using the pytest ``tmp_path``
    # fixture would share one path across all examples, which would
    # produce misleading save-count assertions as state accumulates
    # across iterations.
    with tempfile.TemporaryDirectory() as tmp_dir:
        state_path = Path(tmp_dir) / "sync_state.json"
        state_store = RecordingStateStore(StateStore(state_path))
        deps = Deps(
            # Duck-typed: ``Deps`` type hints reference the concrete
            # classes, but Python dataclasses do not enforce types at
            # runtime. Our fakes implement the exact methods the
            # orchestrator calls, which is what matters.
            state_store=state_store,  # type: ignore[arg-type]
            client=client,  # type: ignore[arg-type]
            settings=settings_obj,
            bulk_loader=loader,  # type: ignore[arg-type]
        )

        run_full_sync(deps)

    # One state save per page, including the terminal page (whose
    # save clears ``replication_in_progress`` and ``replication_next_link``).
    assert len(state_store.saves) == len(pages), (
        f"Expected {len(pages)} state saves (one per page), got "
        f"{len(state_store.saves)}"
    )

    # Walk the saved states in order, reconstructing the orchestrator's
    # running max from the same input the orchestrator saw.
    running_max: Optional[datetime] = None
    for i, saved_state in enumerate(state_store.saves):
        page = pages[i]
        expected_next_link: Optional[str] = page.get("@odata.nextLink")

        # Update the running max from this page's records BEFORE
        # asserting, because the orchestrator saves state AFTER the
        # batch for page ``i`` commits (the save reflects page ``i``'s
        # contribution to the watermark).
        for rec in page.get("value", []):
            ts_str = rec.get("ModificationTimestamp")
            if ts_str is not None:
                ts = _parse_mod_ts(ts_str)
                if running_max is None or ts > running_max:
                    running_max = ts

        # --- Invariant 1: replication_in_progress = (next_link is not None)
        assert saved_state.replication_in_progress == (
            expected_next_link is not None
        ), (
            f"Page {i}: replication_in_progress="
            f"{saved_state.replication_in_progress!r} but next_link="
            f"{expected_next_link!r}"
        )

        # --- Invariant 2: replication_next_link equals page's nextLink
        assert saved_state.replication_next_link == expected_next_link, (
            f"Page {i}: replication_next_link="
            f"{saved_state.replication_next_link!r} expected="
            f"{expected_next_link!r}"
        )

        # --- Invariant 3: last_modification_timestamp = running max
        assert saved_state.last_modification_timestamp == running_max, (
            f"Page {i}: last_modification_timestamp="
            f"{saved_state.last_modification_timestamp!r} expected="
            f"{running_max!r}"
        )
