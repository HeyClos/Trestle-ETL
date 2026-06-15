"""Property test for max-ModificationTimestamp reporting (Property 8).

Property 8 (design.md): For any sequence of records yielded by an
incremental run, the ``last_modification_timestamp`` reported to the
State_Store equals ``max(record.ModificationTimestamp)`` over every
record observed in the run.

**Validates: Requirements 4.5**

Implementation notes
--------------------
Drives :func:`run_incremental` end-to-end with:

* A :class:`FakeTrestleClient` that pops pre-scripted OData pages from
  a FIFO so the extractor's nextLink-walking loop is exercised without
  the real HTTP stack.
* A :class:`FakeUpsertLoader` that records every ``write_batch`` call
  and computes ``max_modification_timestamp`` by scanning the
  ``ModificationTimestamp`` slot (index 1) of each row's
  promoted-columns tuple, mirroring the real ``UpsertLoader`` contract.
* A real :class:`StateStore` against a per-example ``tempfile``
  directory, so the equality assertion exercises the full
  encode/decode round-trip the state file actually goes through.

The Hypothesis strategy emits 1..5 pages, each carrying 1..10 records
with a tz-aware UTC ``ModificationTimestamp``. Every record carries a
non-empty ``ListingKey`` so the transformer accepts it (Requirement
5.6); the running max folded into state is therefore the max across
every generated record, which the test reconstructs locally and
compares directly.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.config import Settings
from trestle_etl.loader import BatchResult, Row
from trestle_etl.orchestrator import Deps, reset_sigint_state, run_incremental
from trestle_etl.state import StateStore
from trestle_etl.transformer import PROMOTED_COLUMNS

# Index of ``ModificationTimestamp`` inside the promoted-columns tuple,
# resolved once so the fake loader tracks column-order changes in
# ``PROMOTED_COLUMNS`` automatically.
_MOD_TS_INDEX = PROMOTED_COLUMNS.index("ModificationTimestamp")


class FakeUpsertLoader:
    """Records ``write_batch`` calls and reports the batch's max ModTs.

    The orchestrator folds ``BatchResult.max_modification_timestamp``
    into its running max (Requirement 4.5). Scanning the promoted-
    columns tuple at ``_MOD_TS_INDEX`` (``ModificationTimestamp``)
    matches the real ``UpsertLoader``'s behavior so the value the
    orchestrator sees is the same one the test reconstructs from the
    generated input.
    """

    def __init__(self) -> None:
        self.batches: list[list[Row]] = []

    def write_batch(self, rows: list[Row]) -> BatchResult:
        self.batches.append(list(rows))
        max_ts: Optional[datetime] = None
        for promoted, _raw in rows:
            # ModificationTimestamp lives at ``_MOD_TS_INDEX``. The
            # transformer coerces it to a tz-aware UTC datetime via the
            # Pydantic model, so direct comparison with ``>`` is safe.
            ts = promoted[_MOD_TS_INDEX]
            if ts is not None and (max_ts is None or ts > max_ts):
                max_ts = ts
        return BatchResult(count=len(rows), max_modification_timestamp=max_ts)

    def close(self) -> None:
        return None


class FakeTrestleClient:
    """Pops pre-scripted OData pages from a FIFO on each GET.

    URL and params are ignored: the orchestrator's incremental path
    only relies on the generator's "issue one GET per yielded page"
    contract, which is tested separately in the extractor properties.
    """

    def __init__(self, pages: list[dict[str, Any]]) -> None:
        # Defensive copy so Hypothesis shrinking does not see a
        # mutated input if it replays the same example.
        self.pages: list[dict[str, Any]] = list(pages)

    def get(
        self, url: str, params: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        return self.pages.pop(0)


def build_pages(records_per_page: list[list[datetime]]) -> list[dict[str, Any]]:
    """Assemble OData pages from per-page timestamp lists.

    Every record carries a deterministic ``ListingKey`` so the
    transformer accepts it (Requirement 5.6). ``@odata.nextLink`` is
    populated on every non-terminal page so the extractor's streaming
    traversal keeps iterating until the final page.
    """
    pages: list[dict[str, Any]] = []
    for i, page_timestamps in enumerate(records_per_page):
        records: list[dict[str, Any]] = []
        for j, ts in enumerate(page_timestamps):
            records.append(
                {
                    "ListingKey": f"LK-{i}-{j}",
                    # ``Z`` suffix matches the form Trestle emits and
                    # the Pydantic model's timestamp parser accepts.
                    "ModificationTimestamp": ts.isoformat().replace(
                        "+00:00", "Z"
                    ),
                }
            )
        page: dict[str, Any] = {"value": records}
        if i < len(records_per_page) - 1:
            page["@odata.nextLink"] = f"nextlink{i + 1}"
        pages.append(page)
    return pages


def make_settings(state_path: Path) -> Settings:
    """Construct a Settings instance with harmless dummy values.

    Only ``trestle_base_url``, ``default_page_size``, and
    ``state_file_path`` are read by anything on the orchestrator
    incremental path. The fake client ignores URL / params so the base
    URL's specific value does not affect the property.
    """
    return Settings(
        trestle_base_url="https://example.invalid/",
        trestle_token_url="https://example.invalid/token",
        client_id="cid",
        client_secret="csec",
        mysql_host="h",
        mysql_port=3306,
        mysql_user="u",
        mysql_password="p",
        mysql_database="d",
        state_file_path=state_path,
        default_page_size=1000,
    )


@st.composite
def page_record_lists(draw: st.DrawFn) -> list[list[datetime]]:
    """Generate 1..5 non-empty pages of UTC-aware ModificationTimestamps.

    Each page carries between 1 and 10 timestamps. Every timestamp is
    tz-aware UTC, which is the form the real Trestle API returns and
    the form the Pydantic model normalizes to.
    """
    n_pages = draw(st.integers(min_value=1, max_value=5))
    pages: list[list[datetime]] = []
    for _ in range(n_pages):
        n = draw(st.integers(min_value=1, max_value=10))
        timestamps = draw(
            st.lists(
                st.datetimes(
                    min_value=datetime(2020, 1, 1),
                    max_value=datetime(2030, 1, 1),
                    timezones=st.just(timezone.utc),
                ),
                min_size=n,
                max_size=n,
            )
        )
        pages.append(timestamps)
    return pages


@given(pages_ts=page_record_lists())
@settings(
    max_examples=100,
    # Orchestrator runs exercise file I/O on every example; the default
    # deadline causes flakes on slow hosts without saying anything
    # useful about the property under test.
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_max_modts_reported(pages_ts: list[list[datetime]]) -> None:
    """Property 8 (Requirements 4.5).

    Drive ``run_incremental`` over a scripted page chain and assert
    that both the :class:`RunResult` and the persisted
    :class:`SyncState` carry a ``last_modification_timestamp`` equal
    to the max across every generated record.
    """
    # Clear any residual module-level SIGINT flag from an earlier test
    # in the same process. The orchestrator polls that flag after each
    # page commit, and if it's set the loop breaks out early, missing
    # later pages and causing a spurious mismatch.
    reset_sigint_state()
    with tempfile.TemporaryDirectory() as tmp:
        state_path = Path(tmp) / "state.json"
        settings_obj = make_settings(state_path)
        pages = build_pages(pages_ts)
        client = FakeTrestleClient(pages)
        loader = FakeUpsertLoader()
        store = StateStore(state_path)
        deps = Deps(
            state_store=store,
            client=client,  # type: ignore[arg-type]
            settings=settings_obj,
            upsert_loader=loader,  # type: ignore[arg-type]
        )

        # ``since`` is arbitrary: the fake client ignores the URL /
        # params, and the orchestrator's running max is folded from
        # the loader's BatchResult (not from ``since``).
        since = datetime(2019, 1, 1, tzinfo=timezone.utc)
        result = run_incremental(deps, since=since)

        # Expected max = max of all timestamps across all pages, with
        # no mutation -- the Pydantic model normalizes to UTC but
        # every generated timestamp is already UTC, so the instants
        # are identical.
        all_ts = [ts for page in pages_ts for ts in page]
        expected_max = max(all_ts)

        # --- Assertion 1: RunResult reports the expected max.
        assert result.final_max_modification_timestamp == expected_max, (
            f"RunResult.final_max_modification_timestamp="
            f"{result.final_max_modification_timestamp!r} "
            f"expected {expected_max!r}"
        )

        # --- Assertion 2: the persisted state matches too. Reload
        # via the existing store (and also a fresh one for paranoia)
        # so the full save -> encode -> decode -> load path is
        # exercised end-to-end.
        loaded = store.load()
        assert loaded.last_modification_timestamp == expected_max, (
            f"SyncState.last_modification_timestamp="
            f"{loaded.last_modification_timestamp!r} "
            f"expected {expected_max!r}"
        )
