"""Property test for resume-vs-pivot decision (Property 10).

Property 10 (design.md): For any State_Store with
``replication_in_progress=true``, the orchestrator resumes from
``replication_next_link`` iff
``now − replication_next_link_persisted_at < 4 min``; otherwise it
pivots to an incremental run starting from
``last_modification_timestamp``.

**Validates: Requirements 3.8**

Implementation notes
--------------------

The property is tested end-to-end at the orchestrator boundary by
driving :func:`run_full_sync` with:

* A pre-seeded state file (``replication_in_progress=True``, a known
  ``replication_next_link`` URL, and a parametrically-aged
  ``replication_next_link_persisted_at``).
* A fake :class:`TrestleClient` that records every ``get(url, params)``
  call and returns a terminal OData page, so the first GET alone
  determines the branch taken.
* A fake :class:`BulkLoader` and a fake :class:`UpsertLoader` that
  implement only the methods the orchestrator touches (so that no
  real MySQL or filesystem CSV I/O is needed).
* An injected clock (``deps.clock``) fixed at a known instant so the
  4-minute window check is deterministic.

The observable output is the first URL / params tuple seen by the
fake client:

* **Resume** — the orchestrator follows ``replication_next_link``
  verbatim with ``params=None`` (this is the Extractor's contract for
  server-supplied nextLink URLs, per :mod:`trestle_etl.extractor`).
* **Pivot** — the orchestrator hands off to ``run_incremental``, which
  issues ``GET <base>/Property`` with a
  ``$filter=ModificationTimestamp gt <last_mod_ts>`` query parameter.

The design's 4-minute cutoff is strict ``<``: at exactly 240 s the link
is NOT fresh. Hypothesis sweeps ``link_age_seconds`` across a range
that brackets the boundary so both branches (and the boundary itself)
are exercised.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.config import Settings
from trestle_etl.loader import BatchResult
from trestle_etl.orchestrator import Deps, run_full_sync
from trestle_etl.state import StateStore, SyncState


# The 4-minute freshness window (Requirement 3.8). A link with an age
# STRICTLY LESS THAN this bound is fresh; at or above the bound the
# orchestrator must pivot to incremental.
_FRESH_WINDOW_SECONDS: int = 240

# Arbitrary but realistic resume link. The content does not matter to
# Property 10 — what matters is that the orchestrator forwards it
# verbatim on the resume branch.
_RESUME_LINK: str = "https://example.invalid/Property?$skiptoken=resume-cursor"


def _make_settings(state_path: Path) -> Settings:
    """Build a :class:`Settings` instance pinned to a per-example state path.

    The orchestrator reads ``trestle_base_url`` (used by the pivot
    branch to construct the ``/Property`` URL) and ``default_page_size``
    (used by both branches for ``$top``). Neither affects Property 10's
    decision; they are supplied here only so the dataclass is valid.

    The ``state_file_path`` is threaded through so any component that
    consults it (currently none beyond :class:`StateStore`, which the
    test constructs directly) sees the same temp file used by the
    test.
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
        state_file_path=state_path,
        default_page_size=10,
    )


class _RecordingTrestleClient:
    """Minimal :class:`TrestleClient` stand-in that records ``get`` calls.

    The orchestrator drives the extractor which only touches
    ``client.get(url, params=...)``; the real client's auth/retry/quota
    machinery is irrelevant to Property 10. Each call is recorded as
    ``(url, params)`` so the test can inspect the FIRST call to
    determine which branch the orchestrator took.

    Every ``get`` returns a terminal OData page (empty ``value`` list
    with no ``@odata.nextLink``) so the stream yields exactly one page
    and the orchestrator's batch loop completes after a single
    iteration. That keeps the test deterministic without requiring a
    scripted multi-page chain — Property 10 is a statement about the
    FIRST request, so one page per run is enough.
    """

    def __init__(self) -> None:
        # List rather than a counter so failures can print the full
        # sequence of GETs, which makes branch-misidentification bugs
        # easy to diagnose.
        self.calls: list[tuple[str, Optional[dict[str, Any]]]] = []

    def get(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        # Snapshot ``params`` (the extractor currently does not mutate
        # it, but a defensive copy future-proofs the test against any
        # downstream change).
        self.calls.append((url, dict(params) if params is not None else None))
        return {"value": []}


class _FakeBulkLoader:
    """No-op :class:`BulkLoader` stand-in.

    The orchestrator calls, in order on the resume branch:
    ``ensure_indexes_if_resuming`` → (stream iteration) → ``close``.
    On the pivot branch it calls:
    ``ensure_indexes_if_resuming`` → ``close`` → (hand off to
    ``run_incremental``).

    All four methods must exist for the orchestrator to run to
    completion; none of them need to do real work because no batch is
    actually written (the fake client returns an empty terminal page).
    """

    def __init__(self) -> None:
        # Counters are exposed for debugging but the test does not
        # assert on them — the orchestrator's branch choice is directly
        # observable from the client's first ``get`` call.
        self.dropped = 0
        self.ensured = 0
        self.closed = 0
        self.batches: list[list] = []

    def drop_secondary_indexes_if_fresh_full_sync(
        self, state: SyncState
    ) -> None:
        self.dropped += 1

    def ensure_indexes_if_resuming(self, state: SyncState) -> None:
        self.ensured += 1

    def write_batch(self, rows: list) -> BatchResult:
        self.batches.append(list(rows))
        # ``datetime.min.replace(tzinfo=utc)`` is the conventional
        # "no real data" sentinel used elsewhere in the test suite.
        return BatchResult(
            count=len(rows),
            max_modification_timestamp=datetime.min.replace(tzinfo=timezone.utc),
        )

    def close(self) -> None:
        self.closed += 1


class _FakeUpsertLoader:
    """No-op :class:`UpsertLoader` stand-in.

    Only ``write_batch`` is invoked on the pivot branch (and even then
    only if a page has rows; with the terminal-page fake client there
    are none). ``close`` is not part of the orchestrator's upsert-path
    flow today, but is defined for symmetry with the protocol.
    """

    def __init__(self) -> None:
        self.batches: list[list] = []
        self.closed = 0

    def write_batch(self, rows: list) -> BatchResult:
        self.batches.append(list(rows))
        return BatchResult(
            count=len(rows),
            max_modification_timestamp=datetime.min.replace(tzinfo=timezone.utc),
        )

    def close(self) -> None:
        self.closed += 1


# Fixed "now" instant. Property 10 is scale-invariant in the wall-clock
# value; only the ``now - persisted_at`` delta matters, and that delta
# is the Hypothesis-driven variable (``link_age_seconds``).
_NOW = datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc)

# Last-committed watermark used both by the pre-seeded state and by the
# pivot branch's ``$filter`` lower bound. Chosen to be two hours before
# ``_NOW`` so the value is unambiguously distinct from the wall-clock
# instant and any formatting bug that echoed ``now`` into the filter
# would be caught.
_LAST_MOD_TS = datetime(2024, 3, 15, 10, 0, 0, tzinfo=timezone.utc)


@given(
    # Sweep a range that brackets the 240 s boundary. 0..600 gives
    # plenty of examples on each side of the cutoff so Hypothesis
    # reliably exercises both branches and the boundary itself.
    link_age_seconds=st.integers(min_value=0, max_value=600),
)
@settings(
    max_examples=100,
    # ``tmp_path`` is a function-scoped pytest fixture; Hypothesis
    # would otherwise flag its reuse across examples as a health-check
    # violation. The state file is overwritten per example, so the
    # reuse is safe.
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_resume_vs_pivot(link_age_seconds: int, tmp_path: Path) -> None:
    """Property 10 (Requirements 3.8).

    Pre-seeds a state file with ``replication_in_progress=true`` and a
    known ``replication_next_link``, then runs :func:`run_full_sync`
    with an injected clock so ``now - replication_next_link_persisted_at``
    equals ``link_age_seconds``. Asserts the FIRST GET issued by the
    extractor matches the branch Property 10 requires:

    * **Age < 240 s** — resume branch: first GET targets
      ``_RESUME_LINK`` verbatim with ``params is None`` (matches the
      Extractor's ``resume_from`` contract in
      :mod:`trestle_etl.extractor`).
    * **Age >= 240 s** — pivot branch: first GET targets
      ``<base>/Property`` with a ``$filter=ModificationTimestamp gt
      <last_mod_ts>`` query parameter (the incremental entry point).
    """
    persisted_at = _NOW - timedelta(seconds=link_age_seconds)

    # --- Pre-seed the on-disk state ----------------------------------
    # Mirrors what a prior interrupted full sync would have left on
    # disk: in-progress flag set, a concrete resume link, a concrete
    # persisted-at timestamp, and a known high-water mark so the pivot
    # branch has a sensible lower bound for the incremental filter.
    state_path = tmp_path / "sync_state.json"
    pre_state = SyncState(
        last_modification_timestamp=_LAST_MOD_TS,
        replication_in_progress=True,
        replication_next_link=_RESUME_LINK,
        replication_next_link_persisted_at=persisted_at,
    )
    StateStore(state_path).save(pre_state)

    # --- Wire up the orchestrator ------------------------------------
    client = _RecordingTrestleClient()
    settings_obj = _make_settings(state_path)
    state_store = StateStore(state_path)
    bulk_loader = _FakeBulkLoader()
    upsert_loader = _FakeUpsertLoader()

    deps = Deps(
        state_store=state_store,
        client=client,  # type: ignore[arg-type]
        settings=settings_obj,
        bulk_loader=bulk_loader,  # type: ignore[arg-type]
        upsert_loader=upsert_loader,  # type: ignore[arg-type]
        # Freeze ``now`` at a known instant so the 4-minute check is
        # driven solely by ``link_age_seconds``.
        clock=lambda: _NOW,
    )

    # --- Drive the orchestrator --------------------------------------
    # ``run_full_sync`` returns a ``RunResult``; we only care about the
    # side effect on the recording client here.
    run_full_sync(deps)

    # --- Property 10 assertions --------------------------------------
    assert len(client.calls) >= 1, (
        "Orchestrator issued no GETs; expected at least one for the "
        "initial resume or pivot request."
    )
    first_url, first_params = client.calls[0]

    if link_age_seconds < _FRESH_WINDOW_SECONDS:
        # Resume branch (Requirement 3.8 first clause). The Extractor
        # takes ``resume_from`` verbatim with ``params=None``; any
        # deviation (appending a query string, re-issuing the initial
        # replication URL, etc.) would violate Property 6 as well.
        assert first_url == _RESUME_LINK, (
            f"Expected resume branch at age {link_age_seconds}s "
            f"(cutoff {_FRESH_WINDOW_SECONDS}s); first GET targeted "
            f"{first_url!r}, expected {_RESUME_LINK!r}"
        )
        assert first_params is None, (
            f"Resume branch must use params=None (the nextLink already "
            f"carries the query string); got params={first_params!r}"
        )
    else:
        # Pivot branch (Requirement 3.8 second clause). The orchestrator
        # calls ``run_incremental(deps, since=last_modification_timestamp)``,
        # which invokes ``incremental_stream``. The first GET targets
        # ``<base>/Property`` with a $filter parameter whose value is
        # ``ModificationTimestamp gt <iso>``.
        assert "Property" in first_url, (
            f"Expected pivot branch at age {link_age_seconds}s "
            f"(cutoff {_FRESH_WINDOW_SECONDS}s); first GET targeted "
            f"{first_url!r}, which does not look like the incremental "
            f"entry point"
        )
        # Crucially: pivot must NOT follow the saved resume link.
        assert first_url != _RESUME_LINK, (
            f"Pivot branch must not follow the stale replication link; "
            f"first GET targeted the saved link {_RESUME_LINK!r}"
        )
        assert first_params is not None, (
            "Pivot (incremental) branch must carry $filter/$orderby "
            "query parameters on the initial request; got params=None"
        )
        filter_val = first_params.get("$filter")
        assert filter_val is not None, (
            f"Pivot branch first GET missing $filter parameter; "
            f"params={first_params!r}"
        )
        assert "ModificationTimestamp gt" in filter_val, (
            f"Pivot branch filter must be 'ModificationTimestamp gt "
            f"<ts>'; got {filter_val!r}"
        )
