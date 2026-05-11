"""Property test for the BulkLoader's CSV file lifecycle (Property 20).

Property 20 (design.md): For any replication page processed by the
Bulk_Load_Path, the loader creates exactly one temporary CSV file,
invokes ``LOAD DATA LOCAL INFILE`` exactly once against that file, and
deletes the file after a successful load.

**Validates: Requirements 3.9, 8.2, 8.3, 8.4**

Implementation notes:

* Requirement 3.9 prohibits aggregating rows across replication pages:
  every :meth:`BulkLoader.write_batch` call is a single page, and the
  loader must commit it via exactly one CSV + one LOAD DATA invocation.
  Requirements 8.2, 8.3, and 8.4 cover the CSV-in-tempdir, the LOAD DATA
  call itself, and the post-load cleanup respectively. This test asserts
  all four in a single pass per generated batch size.

* Running this property test against a real MySQL would be overkill and
  slow: the file-lifecycle invariant is entirely on the Python side. We
  mock the SQLAlchemy :class:`Engine` so that ``engine.begin()`` returns
  a context manager whose :meth:`execute` records the statement text
  without actually talking to a database. That is enough to verify "one
  LOAD DATA" and simultaneously lets the BulkLoader proceed past the
  ingest step so the ``finally``-block cleanup (Requirement 8.4) runs on
  the success path rather than the failure path.

* :func:`tempfile.mkdtemp` is wrapped via :func:`unittest.mock.patch` on
  the ``trestle_etl.loader.bulk.tempfile`` reference (not on the global
  ``tempfile`` module) so the wrapper is scoped to the loader's imports
  and cannot leak into unrelated code paths during test collection. The
  wrapper delegates to the real :func:`tempfile.mkdtemp` so actual
  filesystem behavior is exercised (the file is really written, really
  fsynced, really unlinked), while the list of returned directories
  gives us an exact count of "how many CSVs were created".

* The batch size is the only free variable: the property holds for any
  non-empty page, so a Hypothesis integer strategy over a modest range
  (1..50) is sufficient. Larger sizes would only stress CSV formatting,
  which is Property 14's concern (raw_data preservation) rather than
  Property 20's. Hypothesis runs 100 examples per the project's
  property-testing policy; deadline is disabled because real filesystem
  fsync can occasionally push a single example past Hypothesis' default
  200 ms budget on busy machines.

* Rows are constructed with a unique ``ListingKey`` per position and
  ``None`` for every other Promoted_Column. ``None`` is a legal value
  (all non-key columns are nullable per Requirement 5.2) and it keeps
  the CSV cheap to serialize while still exercising the formatter's
  NULL-as-backslash-N path. The ``raw_data`` payload is an empty JSON
  object; preservation semantics are out of scope for this test.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from hypothesis import given, settings, strategies as st

from trestle_etl.loader.bulk import BulkLoader
from trestle_etl.transformer import PROMOTED_COLUMNS


def _make_rows(n: int) -> list[tuple[tuple, str]]:
    """Build ``n`` synthetic rows in the shape the loader expects.

    Each row is a ``(promoted_columns_tuple, raw_data_json_str)`` pair.
    The first element of the promoted tuple is ``ListingKey`` (by the
    :data:`PROMOTED_COLUMNS` ordering contract); every other column is
    ``None``. Keys are made unique per position so the generated CSV has
    distinct primary keys, mirroring a real replication page where
    ``ListingKey`` is unique.
    """
    rows: list[tuple[tuple, str]] = []
    for i in range(n):
        promoted = tuple([f"K{i}"] + [None] * (len(PROMOTED_COLUMNS) - 1))
        rows.append((promoted, "{}"))
    return rows


class _RecordingConnection:
    """Stand-in for a SQLAlchemy :class:`Connection` that records SQL text.

    Only :meth:`execute` is needed by :meth:`BulkLoader._load_csv`; any
    other attribute access is a bug in the test surface, and letting it
    raise :class:`AttributeError` keeps the harness honest.
    """

    def __init__(self, executed: list[str]) -> None:
        self._executed = executed

    def execute(self, statement, *args, **kwargs):  # type: ignore[no-untyped-def]
        # ``str(text("..."))`` returns the raw SQL, which is what we
        # need to match on "LOAD DATA LOCAL INFILE".
        self._executed.append(str(statement))
        return MagicMock()


class _RecordingBeginContext:
    """Context manager returned by the mock engine's ``begin()`` method.

    Each ``with engine.begin() as conn:`` block that the loader opens
    yields a fresh :class:`_RecordingConnection` bound to the shared
    ``executed`` list, so execution order across nested context entries
    is preserved in call order.
    """

    def __init__(self, executed: list[str]) -> None:
        self._executed = executed

    def __enter__(self) -> _RecordingConnection:
        return _RecordingConnection(self._executed)

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
        # Propagate exceptions raised inside the block; returning False
        # (or None) means "do not suppress".
        return False


@given(n_rows=st.integers(min_value=1, max_value=50))
@settings(max_examples=100, deadline=None)
def test_bulk_load_file_lifecycle(n_rows: int) -> None:
    """Property 20 (Requirements 3.9, 8.2, 8.3, 8.4).

    For any non-empty replication page, :meth:`BulkLoader.write_batch`
    MUST:

    1. Call :func:`tempfile.mkdtemp` exactly once, producing exactly one
       CSV directory for the page (Requirement 3.9 + 8.2: one CSV per
       page, no aggregation).
    2. Execute exactly one ``LOAD DATA LOCAL INFILE`` statement against
       that CSV (Requirement 8.3: one LOAD DATA per CSV).
    3. Remove the temp directory (and therefore its CSV) after the load
       returns successfully (Requirement 8.4: post-load cleanup).

    The :class:`BatchResult` row count is also checked to guard against
    a regression where the loader silently drops rows.
    """
    created_dirs: list[str] = []
    real_mkdtemp = tempfile.mkdtemp

    def tracking_mkdtemp(*args, **kwargs):  # type: ignore[no-untyped-def]
        """Wrap :func:`tempfile.mkdtemp`, record every call, delegate."""
        path = real_mkdtemp(*args, **kwargs)
        created_dirs.append(path)
        return path

    executed_statements: list[str] = []
    begin_context = _RecordingBeginContext(executed_statements)

    mock_engine = MagicMock()
    # ``return_value`` re-uses the same context manager across calls to
    # ``engine.begin()``. Each ``__enter__`` still returns a fresh
    # connection, so the semantics stay correct for the loader's one
    # ``with engine.begin()`` block per batch.
    mock_engine.begin.return_value = begin_context

    rows = _make_rows(n_rows)

    # Patch the module-scoped reference so the wrapper only applies
    # inside the loader under test.
    with patch(
        "trestle_etl.loader.bulk.tempfile.mkdtemp",
        side_effect=tracking_mkdtemp,
    ):
        loader = BulkLoader(mock_engine)
        result = loader.write_batch(rows)

    # --- 1. Exactly one CSV directory was created for this page. -------
    assert len(created_dirs) == 1, (
        f"Expected exactly one tempfile.mkdtemp() call per page "
        f"(Req 3.9, 8.2); got {len(created_dirs)}"
    )
    tmpdir = Path(created_dirs[0])

    # --- 2. Exactly one LOAD DATA LOCAL INFILE was executed. -----------
    load_statements = [
        stmt
        for stmt in executed_statements
        if "LOAD DATA LOCAL INFILE" in stmt.upper()
    ]
    assert len(load_statements) == 1, (
        f"Expected exactly one LOAD DATA LOCAL INFILE per page "
        f"(Req 8.3); got {len(load_statements)}. "
        f"All executed statements: {executed_statements}"
    )
    # The LOAD DATA statement must reference the path that was just
    # created; otherwise "one mkdtemp + one LOAD DATA" could both be true
    # while still violating Property 20 (e.g. loading a different file).
    assert str(tmpdir) in load_statements[0], (
        f"LOAD DATA statement does not reference the mkdtemp() path. "
        f"tmpdir={tmpdir}, statement={load_statements[0]!r}"
    )

    # --- 3. The temp directory (and its CSV) were removed after load. --
    assert not tmpdir.exists(), (
        f"Temp directory {tmpdir} was not cleaned up after successful "
        f"load (Req 8.4)"
    )

    # Sanity check: BatchResult row count matches the input page size.
    assert result.count == n_rows, (
        f"Expected BatchResult.count == {n_rows}, got {result.count}"
    )
