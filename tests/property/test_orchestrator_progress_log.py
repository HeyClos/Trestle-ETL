"""Property test for per-batch progress log structure (Property 28).

Property 28 (design.md): For any committed batch, the pipeline emits
exactly one INFO log entry containing cumulative record count, highest
committed ``ModificationTimestamp``, elapsed wall-clock time since run
start, and requests-per-minute rate over the last interval.

**Validates: Requirements 12.2**

Implementation notes
--------------------

The orchestrator emits per-batch progress by logging an INFO message whose
text starts with ``batch_committed``. The exact key-value fields included
in that log line are fixed by the ``_run_batches`` implementation; this
test asserts their presence and count per committed batch without binding
to any specific downstream formatting.

The test:

* Drives :func:`run_incremental` through a :class:`FakeTrestleClient` that
  serves a generated chain of ``n_pages`` replication pages, each carrying
  three records.
* Uses a fake :class:`Loader` so the test is independent of a live MySQL
  instance; the fake reports three committed rows per call with a fixed
  max ``ModificationTimestamp`` so the orchestrator is guaranteed to
  treat every call as a committed batch.
* Captures ``trestle_etl.orchestrator`` log records at INFO via pytest's
  ``caplog`` fixture.
* Asserts (a) exactly one ``batch_committed`` entry per page processed
  and (b) that each such entry carries every required field from
  Property 28 / Requirement 12.2: ``cumulative=``, ``max_mod_ts=``,
  ``elapsed_seconds=``, and ``requests_per_minute=``.

``reset_sigint_state`` is invoked at the start of each example to clear
any residual module-level SIGINT flag from a prior run in the same
process. The orchestrator polls that flag after every batch; leaving it
set from a neighboring test would cause the loop to exit after the first
batch and drop progress entries for the remainder, invalidating the
per-page count assertion.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.config import Settings
from trestle_etl.loader import BatchResult, Row
from trestle_etl.orchestrator import Deps, reset_sigint_state, run_incremental
from trestle_etl.state import StateStore


# Fixed batch-result timestamp. Only required to be tz-aware UTC so the
# orchestrator's ``_max_ts`` fold and state serialization both accept it.
# The actual instant does not affect the per-batch log count, which is
# what Property 28 constrains.
_BATCH_MAX_TS: datetime = datetime(2024, 3, 14, 12, 0, 0, tzinfo=timezone.utc)

# Incremental ``since`` value: anything strictly older than
# ``_BATCH_MAX_TS`` so the orchestrator treats each batch as advancing
# the watermark. Using epoch keeps the test deterministic.
_SINCE: datetime = datetime(2020, 1, 1, tzinfo=timezone.utc)


def _make_settings(tmp_dir: Path) -> Settings:
    """Build a ``Settings`` with a state-file path inside ``tmp_dir``.

    The fake client bypasses all HTTP concerns so only
    ``trestle_base_url`` (consumed when the extractor builds the initial
    request URL) and ``state_file_path`` (passed through to the
    orchestrator via ``StateStore``) have to be populated sensibly.
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
        state_file_path=tmp_dir / "sync_state.json",
        default_page_size=1000,
    )


class _FakeTrestleClient:
    """Pre-scripted TrestleClient stand-in.

    The orchestrator only invokes ``client.get(url, params)`` through the
    extractor. Pop-from-front semantics ensure any unexpected extra GET
    raises ``IndexError`` rather than silently re-returning a page.
    """

    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self._pages: list[dict[str, Any]] = list(pages)

    def get(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        return self._pages.pop(0)


class _FakeUpsertLoader:
    """Minimal :class:`Loader` stand-in that records every call.

    ``write_batch`` returns a :class:`BatchResult` reporting the exact
    row count handed in and a fixed max ``ModificationTimestamp`` that
    is newer than ``_SINCE``, so the orchestrator always treats the
    batch as committed. No I/O, no state.
    """

    def __init__(self) -> None:
        self.batches: list[list[Row]] = []

    def write_batch(self, rows: list[Row]) -> BatchResult:
        self.batches.append(rows)
        return BatchResult(
            count=len(rows),
            max_modification_timestamp=_BATCH_MAX_TS,
        )

    def close(self) -> None:
        # Upsert path's ``close`` is a no-op in production; mirror that
        # here so any future caller that invokes ``close`` does not trip.
        return None


def _build_pages(n_pages: int) -> list[dict[str, Any]]:
    """Construct ``n_pages`` OData response envelopes with 3 records each.

    Each record carries a unique ``ListingKey`` and a
    ``ModificationTimestamp`` matching ``_BATCH_MAX_TS`` so the
    orchestrator's running-max fold returns a stable value and the log
    line's ``max_mod_ts=`` field is populated non-trivially. Non-terminal
    pages carry an ``@odata.nextLink`` pointing at the next page's
    server-supplied URL; the terminal page omits it, signaling end of
    stream to the extractor.
    """
    pages: list[dict[str, Any]] = []
    for i in range(n_pages):
        records = [
            {
                "ListingKey": f"p{i}r{j}",
                "ModificationTimestamp": _BATCH_MAX_TS.isoformat().replace(
                    "+00:00", "Z"
                ),
            }
            for j in range(3)
        ]
        page: dict[str, Any] = {"value": records}
        if i < n_pages - 1:
            page["@odata.nextLink"] = f"link{i + 1}"
        pages.append(page)
    return pages


def _batch_committed_records(
    caplog_records: list[logging.LogRecord],
) -> list[logging.LogRecord]:
    """Filter captured log records to orchestrator progress entries.

    Selects records that (a) originated from the orchestrator logger,
    (b) were emitted at INFO level, and (c) carry the
    ``batch_committed`` prefix the orchestrator stamps on every
    per-batch progress line. Filtering on all three axes avoids false
    positives from the run-start / run-end INFO lines (which do not
    carry ``batch_committed``) and from any other subsystem logger
    that happens to be installed.
    """
    return [
        record
        for record in caplog_records
        if record.name == "trestle_etl.orchestrator"
        and record.levelno == logging.INFO
        and "batch_committed" in record.getMessage()
    ]


@given(n_pages=st.integers(min_value=1, max_value=5))
@settings(
    max_examples=30,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_progress_log_per_batch(n_pages: int, caplog: Any) -> None:
    """Property 28 (Requirement 12.2).

    For an incremental run over ``n_pages`` non-empty pages the
    orchestrator must:

    * Emit exactly one ``batch_committed`` INFO entry per committed
      page — so ``n_pages`` total.
    * Include, in each such entry, all four of the fields enumerated by
      Requirement 12.2: cumulative record count, highest committed
      ``ModificationTimestamp``, elapsed wall-clock seconds, and the
      requests-per-minute rate.

    Neither the specific numeric values nor the ordering of fields is
    asserted: Property 28 only constrains presence and per-batch count,
    and binding to a format would make the test brittle to cosmetic
    changes.
    """
    # Clear residual module-level SIGINT state. The orchestrator's
    # post-commit poll would otherwise break out of the loop early,
    # dropping progress entries and failing the per-page count assertion.
    reset_sigint_state()

    # Scope the caplog capture to the orchestrator logger at INFO.
    # Leaving the root logger's level untouched avoids pulling in INFO
    # lines from unrelated modules (e.g. the transformer's skip
    # warnings, which are below INFO anyway, but defensively).
    caplog.clear()
    caplog.set_level(logging.INFO, logger="trestle_etl.orchestrator")

    with tempfile.TemporaryDirectory() as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)

        pages = _build_pages(n_pages)
        client = _FakeTrestleClient(pages)
        settings_obj = _make_settings(tmp_dir)
        state_store = StateStore(settings_obj.state_file_path)
        loader = _FakeUpsertLoader()

        deps = Deps(
            state_store=state_store,
            client=client,
            settings=settings_obj,
            upsert_loader=loader,
        )

        run_incremental(deps, since=_SINCE)

    progress = _batch_committed_records(caplog.records)

    # One progress entry per committed batch. Every page carries three
    # records with valid ListingKeys, so every page commits as a batch
    # and therefore produces exactly one log line.
    assert len(progress) == n_pages, (
        f"Expected exactly {n_pages} 'batch_committed' progress entries, "
        f"got {len(progress)}. Captured messages: "
        f"{[r.getMessage() for r in progress]}"
    )

    # Each progress entry must carry every field required by
    # Requirement 12.2. The orchestrator formats them as
    # ``key=value`` pairs so substring checks are sufficient.
    required_fields = (
        "cumulative=",
        "max_mod_ts=",
        "elapsed_seconds=",
        "requests_per_minute=",
    )
    for entry in progress:
        message = entry.getMessage()
        for field in required_fields:
            assert field in message, (
                f"Progress log entry missing {field!r}: {message!r}"
            )
