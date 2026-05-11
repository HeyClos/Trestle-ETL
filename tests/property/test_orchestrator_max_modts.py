"""Property test for max-ModificationTimestamp reporting (Property 8).

Property 8 (design.md): For any sequence of records yielded by an
incremental run, the ``last_modification_timestamp`` reported to the
State_Store equals ``max(record.ModificationTimestamp)`` over every
record observed in the run.

**Validates: Requirements 4.5**

Implementation notes
--------------------
The orchestrator's incremental path is driven end-to-end by
:func:`run_incremental`, which pipes the extractor generator through
``UpsertLoader.write_batch`` and saves state AFTER the batch commits.
We exercise that rhythm with fakes so the property is a statement about
the terminal state written to disk AND the ``RunResult`` returned to the
caller, which is what the CLI / operator ultimately observes.

* A :class:`FakeTrestleClient` returns pre-scripted OData pages so the
  extractor's per-page loop is driven without the HTTP stack. The
  first GET's URL / params are ignored: the client always pops from
  its FIFO, so the generator's nextLink traversal is exercised
  transparently (we don't need the nextLink URLs to be real, only
  non-null for non-terminal pages).
* A :class:`FakeLoader` implements the ``UpsertLoader`` surface the
  orchestrator touches (``write_batch`` only; ``close`` is a no-op).
  It computes ``max_modification_timestamp`` by scanning the promoted
  columns tuple the same way the real upsert loader does, so the
  orchestrator's running-max fold produces a value the test can
  reconstruct from the generated input pages.
* A real :class:`StateStore` against a tmp-path JSON file is used
  (rather than a mock) so the round-trip through
  ``_encode_datetime`` / ``_decode_datetime`` is exercised -- the
  state store normalizes to UTC on both save and load, which is what
  makes the equality assertion against the generated ``expected_max``
  hold byte-for-byte.

The generated records always carry a non-empty ``ListingKey`` and a
UTC-aware ``ModificationTimestamp``. The transformer's
:func:`to_row_safe` therefore accepts every record, so the loader sees
every generated timestamp and the running max folded into state equals
the max across the full generated input.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.config import Settings
from trestle_etl.loader import BatchResult, Row
from trestle_etl.orchestrator import Deps, run_incremental
from trestle_etl.state import StateStore
from trestle_etl.transformer import PROMOTED_COLUMNS

# Index of ``ModificationTimestamp`` inside the promoted-columns tuple.
# Resolved once at import time so ``FakeLoader.write_batch`` does not
# repeat the ``.index`` scan on every batch.
_MOD_TS_INDEX = PROMOTED_COLUMNS.index("ModificationTimestamp")


def _make_settings() -> Settings:
    """Construct a ``Settings`` with dummy values.

    The orchestrator / extractor only read ``trestle_base_url`` and
    ``default_page_size`` for the initial request URL. The fake client
    ignores both (it pops from a scripted queue), so the specific
    values do not affect Property 8's claim about the running max.
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

    Returns pages from a FIFO queue on each ``get`` call regardless of
    URL or params. Sufficient for the orchestrator's incremental loop:
    the extractor's only behavioral contract the orchestrator relies on
    is "issue one GET per yielded page", which is tested separately in
    ``test_extractor_nextlink_stream``.
    """

    def __init__(self, pages: list[dict[str, Any]]) -> None:
        # Defensive copy so the Hypothesis-provided strategy output is
        # not mutated as the client consumes it (important because
        # Hypothesis may shrink by re-running with the same input).
        self._pages_queue: list[dict[str, Any]] = list(pages)
        self.calls: list[tuple[str, Optional[dict[str, Any]]]] = []

    def get(
        self, url: str, params: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        self.calls.append((url, params))
        # ``pop(0)`` rather than index so an unexpected extra GET raises
        # IndexError instead of silently re-returning the last page.
        return self._pages_queue.pop(0)


class FakeLoader:
    """Minimal UpsertLoader stand-in for the incremental orchestrator path.

    Implements the two methods the orchestrator actually invokes:

    * ``write_batch(rows)`` -- computes ``max_modification_timestamp``
      by scanning the ``ModificationTimestamp`` slot of each row's
      promoted-columns tuple, mirroring the real upsert loader's
      contract. The returned ``BatchResult`` is what the orchestrator
      folds into its running max (Requirement 4.5).
    * ``close()`` -- the orchestrator does not call ``close`` on the
      upsert path today, but it is defined here for protocol symmetry.

    The ``batches`` list exposes every commit for debugging; the test
    itself asserts against the terminal state, not per-batch contents.
    """

    def __init__(self) -> None:
        self.batches: list[list[Row]] = []

    def write_batch(self, rows: list[Row]) -> BatchResult:
        self.batches.append(list(rows))
        # Scan the promoted-columns tuple for the batch max. Matches
        # the real UpsertLoader's behavior so the orchestrator's fold
        # produces a value the test can reconstruct from the generated
        # input pages.
        tss = [r[0][_MOD_TS_INDEX] for r in rows if r[0][_MOD_TS_INDEX] is not None]
        return BatchResult(
            count=len(rows),
            # The generator below always emits a ModTs per record, so
            # ``tss`` is non-empty whenever ``rows`` is non-empty and
            # the orchestrator calls ``write_batch`` only on non-empty
            # batches -- ``max(tss)`` is always safe here.
            max_modification_timestamp=max(tss),
        )

    def close(self) -> None:
        return None


@st.composite
def record_batches(draw: st.DrawFn) -> list[dict[str, Any]]:
    """Generate an incremental-run scenario: a chain of OData pages.

    Each page carries between 1 and 5 records and, when not the terminal
    page, an ``@odata.nextLink`` pointing at a synthetic URL. Every
    record carries a deterministic ``ListingKey`` (so the transformer
    accepts it; Requirement 5.6) and a UTC-aware
    ``ModificationTimestamp`` formatted as ISO 8601. The transformer
    parses the string back to a UTC-aware datetime; the test
    recomputes the running max from the same input, so the expected
    and observed max values are directly comparable.
    """
    n_pages = draw(st.integers(min_value=1, max_value=5))
    pages: list[dict[str, Any]] = []
    for p in range(n_pages):
        n_records = draw(st.integers(min_value=1, max_value=5))
        records: list[dict[str, Any]] = []
        for i in range(n_records):
            # ``datetimes`` with a UTC ``timezones`` strategy guarantees
            # the generated value is already tz-aware, matching the
            # format the Trestle API would return.
            dt = draw(
                st.datetimes(
                    min_value=datetime(2020, 1, 1),
                    max_value=datetime(2030, 1, 1),
                    timezones=st.just(timezone.utc),
                )
            )
            records.append(
                {
                    "ListingKey": f"LK-{p}-{i}",
                    "ModificationTimestamp": dt.isoformat(),
                }
            )
        page: dict[str, Any] = {"value": records}
        is_last = p == n_pages - 1
        if not is_last:
            # Synthetic nextLink; the fake client does not actually
            # follow it as a URL, it only needs to be non-None so the
            # extractor signals "more pages to come".
            page["@odata.nextLink"] = f"https://example.invalid/next/{p + 1}"
        pages.append(page)
    return pages


def _parse_mod_ts(value: str) -> datetime:
    """Mirror the Pydantic model validator: ISO 8601 -> UTC datetime.

    Kept equivalent to ``Property._normalize_modification_timestamp``
    so the running max reconstructed here matches what the orchestrator
    folds into state (via the loader, via the promoted-columns tuple).
    """
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@given(pages=record_batches())
@settings(
    max_examples=100,
    # The orchestrator's file I/O plus Hypothesis's per-example overhead
    # can exceed the default per-example deadline on slow hosts; the
    # property is purely logical so timing out would be a flake, not a
    # real failure.
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_max_modification_timestamp_reported(
    pages: list[dict[str, Any]],
) -> None:
    """Property 8 (Requirements 4.5).

    Drive ``run_incremental`` with a scripted page chain, then assert:

    1. The ``SyncState`` persisted on disk has
       ``last_modification_timestamp`` equal to the max
       ``ModificationTimestamp`` across every generated record.
    2. The ``RunResult`` returned by the orchestrator reports the same
       value via ``final_max_modification_timestamp``.

    The state-store round-trip goes through UTC-normalizing encode /
    decode on both sides, and the Pydantic model normalizes inbound
    timestamps to UTC-aware ``datetime`` values, so the comparison
    against the locally-computed ``expected_max`` is exact.
    """
    with tempfile.TemporaryDirectory() as d:
        state_path = Path(d) / "sync_state.json"

        # Compute the expected max across every generated record.
        # This mirrors the orchestrator's fold: start at None, take
        # the max over every record's parsed (UTC) timestamp.
        all_mod_ts: list[datetime] = []
        for page in pages:
            for rec in page["value"]:
                all_mod_ts.append(_parse_mod_ts(rec["ModificationTimestamp"]))
        expected_max = max(all_mod_ts)

        # Wire the orchestrator with the fakes plus a real state store
        # at a fresh per-example path. Using a real StateStore
        # exercises the same encode/decode path production uses, which
        # is what makes the equality assertion byte-exact across the
        # UTC normalization.
        state_store = StateStore(state_path)
        client = FakeTrestleClient(pages)
        loader = FakeLoader()
        deps = Deps(
            state_store=state_store,
            client=client,  # type: ignore[arg-type]
            settings=_make_settings(),
            upsert_loader=loader,  # type: ignore[arg-type]
        )

        # ``since`` is arbitrary: the fake client ignores URL and
        # params, and the orchestrator's running max is folded from
        # the loader's BatchResult values (not from ``since``). A
        # timestamp comfortably earlier than every generated record
        # makes the intent clear.
        since = datetime(2000, 1, 1, tzinfo=timezone.utc)
        result = run_incremental(deps, since=since)

        # --- Assertion 1: on-disk state matches the generated max.
        # Reload via a fresh StateStore to exercise the save ->
        # encode -> decode -> load path end-to-end.
        final_state = StateStore(state_path).load()
        assert final_state.last_modification_timestamp == expected_max, (
            f"State last_modification_timestamp="
            f"{final_state.last_modification_timestamp!r} "
            f"expected {expected_max!r}"
        )

        # --- Assertion 2: RunResult reports the same value.
        assert result.final_max_modification_timestamp == expected_max, (
            f"RunResult.final_max_modification_timestamp="
            f"{result.final_max_modification_timestamp!r} "
            f"expected {expected_max!r}"
        )
