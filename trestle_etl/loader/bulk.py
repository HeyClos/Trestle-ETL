"""Bulk load path: one CSV per page, ``LOAD DATA LOCAL INFILE`` per page.

This module implements the fast path the pipeline uses during a full sync
(``--full-sync``). It is the mirror image of the transactional
``INSERT … ON DUPLICATE KEY UPDATE`` upsert loader: instead of rowwise SQL
it serializes each replication page to a single CSV file and hands that
file to MySQL's native ``LOAD DATA LOCAL INFILE`` bulk-ingest path. That
path is measurably faster for the 1.6 M-record initial backfill and is the
only way to make full sync finish in a practical window on a single
machine.

Requirements covered:

* 3.9 — one CSV + one ``LOAD DATA LOCAL INFILE`` per replication page;
  batches are never aggregated across pages.
* 6.7 — ``loaded_at`` is set by the loader at batch-start wall-clock
  time, not by a MySQL ``DEFAULT CURRENT_TIMESTAMP``.
* 8.1, 8.2, 8.3, 8.4 — CSV written to ``tempfile.mkdtemp()``,
  ``LOAD DATA LOCAL INFILE`` executed against it, temporary file and
  directory removed after a successful load.
* 8.6 — rejection of ``LOAD DATA LOCAL INFILE`` by either the server or
  the client surfaces as :class:`BulkLoadConfigError` whose message
  explicitly names both ``local_infile=1`` (server) and
  ``local_infile=True`` (client).
* 8.7 — the seven secondary indexes listed in Req 6.5 are dropped at
  the start of a fresh full sync and recreated on :meth:`close`; the
  ``ListingKey`` primary key is never dropped.
* 8.8 — on startup with ``replication_in_progress=true``, every missing
  secondary index is recreated before extraction resumes.

The public surface intentionally mirrors the orchestrator's pseudocode in
design.md: the orchestrator calls
:meth:`drop_secondary_indexes_if_fresh_full_sync` and
:meth:`ensure_indexes_if_resuming` at startup and :meth:`close` in a
``finally`` block to guarantee indexes are restored even if the run
aborts.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from datetime import UTC, date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, Sequence

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from trestle_etl.errors import BulkLoadConfigError
from trestle_etl.loader import BatchResult, Row
from trestle_etl.state import SyncState
from trestle_etl.transformer import PROMOTED_COLUMNS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

# The tables receiving the bulk load. Kept as named constants to keep the
# DDL and DML statements below textually aligned with the schema file.
_TABLE = "property"
_TABLE_RAW = "property_raw"

# CSV column order for each table. We always write the columns in a fixed
# order and pass a matching column list to LOAD DATA (which is positional).
# Keeping these tuples as the single source of truth means a schema change
# in one place propagates to CSV generation and the LOAD DATA statements.
#   * `property`     gets the promoted columns plus loaded_at (no raw_data).
#   * `property_raw` gets the shared PK, the JSON payload, and loaded_at.
_PROPERTY_CSV_COLUMNS: Final[tuple[str, ...]] = PROMOTED_COLUMNS + ("loaded_at",)
_RAW_CSV_COLUMNS: Final[tuple[str, ...]] = ("ListingKey", "raw_data", "loaded_at")

# Prefix lengths for the VARCHAR(512) multi-select columns, matching the
# prefix indexes declared in schema.sql. Every other column is indexed in
# full (prefix ``None``).
_INDEX_PREFIX_LENGTHS: Final[dict[str, int]] = {
    "AssociationAmenities": 255,
    "HorseAmenities": 255,
}

# One secondary index per non-PK Promoted_Column (Requirement 6.5), derived
# from PROMOTED_COLUMNS so the drop/recreate set can never drift from the
# schema. Each entry is (index_name, column_name, prefix_len_or_None). The
# PK index on ListingKey is deliberately excluded (Requirement 8.7 preserves
# the PK). ``property_raw`` carries only its PK, so it has no entries here.
_SECONDARY_INDEXES: Final[tuple[tuple[str, str, "int | None"], ...]] = tuple(
    (f"idx_property_{col}", col, _INDEX_PREFIX_LENGTHS.get(col))
    for col in PROMOTED_COLUMNS
    if col != "ListingKey"
)

# MySQL error codes raised when LOAD DATA LOCAL INFILE is rejected. These
# are the values we translate to BulkLoadConfigError per Requirement 8.6.
#
# 1148 (ER_NOT_ALLOWED_COMMAND)
#     Returned by MySQL 5.x and early 8.x when ``local_infile`` is
#     disabled on the server OR the client did not opt in.
# 3948 (ER_LOAD_DATA_LOCAL_INFILE_DISABLED)
#     Returned by MySQL 8 when ``local_infile`` is disabled on the server.
# 2068 (PyMySQL's CR_LOAD_DATA_LOCAL_INFILE_REJECTED)
#     Raised client-side by PyMySQL when the server asks for a file but
#     the client connection was opened without ``local_infile=True``.
_LOCAL_INFILE_REJECTED_CODES: Final[frozenset[int]] = frozenset({1148, 3948, 2068})

# Human-readable remediation text appended to every BulkLoadConfigError.
# Centralizing the text (a) guarantees every code path uses an identical
# message and (b) keeps the required phrasing of "local_infile=1" and
# "local_infile=True" in one searchable place for Requirement 8.6.
_LOCAL_INFILE_REMEDIATION: Final[str] = (
    "MySQL rejected LOAD DATA LOCAL INFILE. This path requires BOTH "
    "local_infile=1 on the MySQL server (my.cnf or "
    "SET GLOBAL local_infile=1) AND local_infile=True on the client "
    "connection (passed through connect_args when constructing the "
    "SQLAlchemy engine)."
)


# ---------------------------------------------------------------------------
# CSV field formatting
# ---------------------------------------------------------------------------
#
# We generate CSV bytes by hand rather than using the ``csv`` module. The
# reason is subtle but important: MySQL's ``LOAD DATA LOCAL INFILE`` with
# ``FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\'`` has
# escaping semantics that don't line up cleanly with ``csv.writer``:
#
#   * NULL must be written as the literal two characters ``\N`` OUTSIDE any
#     enclosing quotes. ``csv.writer`` has no mode that emits this.
#   * Inside an enclosed field, the enclosure character must be escaped by
#     the ESCAPED BY character (``\"``), not doubled (``""``). ``csv.writer``
#     offers ``doublequote=False`` + ``escapechar``, but the combination
#     also re-escapes the escape char in ways MySQL doesn't expect when we
#     want ``\N`` passthrough.
#
# A 40-line hand-written formatter is safer than fighting ``csv.writer``'s
# dialect knobs, and keeps the loader code fully self-documenting.


def _format_scalar(value: Any) -> str:
    """Return the MySQL textual representation of a scalar value.

    Datetimes are normalized to UTC and stripped of their tzinfo because
    MySQL's ``DATETIME`` columns do not store timezone; all data committed
    through this loader is UTC by construction (Requirement 4.6, 6.7).
    ``Decimal`` is converted via ``str`` so we don't pass through a float
    representation that could lose trailing zeroes. ``date`` is rendered as
    ``YYYY-MM-DD`` and ``bool`` as ``1``/``0`` for completeness; everything
    else falls through to ``str``.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # Treat naive datetimes as UTC; all pipeline-internal
            # datetimes are tz-aware (per Requirement 5.7), but guard
            # against accidental drift.
            normalized = value.replace(tzinfo=timezone.utc)
        else:
            normalized = value.astimezone(timezone.utc)
        # MySQL accepts 'YYYY-MM-DD HH:MM:SS[.ffffff]' with a space
        # separator. isoformat(sep=' ') gives exactly that format.
        return normalized.replace(tzinfo=None).isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bool):
        # Check bool BEFORE int (bool is a subclass of int in Python).
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    return str(value)


def _format_field(value: Any) -> str:
    """Encode a single field for the CSV row.

    ``None`` becomes the literal two-character sequence ``\\N``, written
    UNENCLOSED so MySQL interprets it as SQL NULL (per its ``LOAD DATA``
    docs, NULL is recognized only outside of ``ENCLOSED BY`` chars). All
    other values are serialized via :func:`_format_scalar` and wrapped in
    double quotes; the two characters that can disrupt a quoted field —
    backslash and the quote char itself — are escaped with a preceding
    backslash (matching ``ESCAPED BY '\\\\'``).
    """
    if value is None:
        # The two literal characters backslash + N, not a newline, not a
        # unicode escape. MySQL recognizes this as NULL.
        return "\\N"
    text_value = _format_scalar(value)
    # Order matters: escape the backslash FIRST so that the escapes we
    # emit for the quote char (``\"``) don't get doubled. Then escape the
    # quote char. Line terminators and commas are safe inside an enclosed
    # field and need no special handling.
    escaped = text_value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _format_row(values: Sequence[Any]) -> str:
    """Join formatted fields with commas and terminate with ``\\n``."""
    return ",".join(_format_field(v) for v in values) + "\n"


# ---------------------------------------------------------------------------
# Index management
# ---------------------------------------------------------------------------


def _existing_index_names(engine: Engine) -> set[str]:
    """Return the set of index names currently present on the property table.

    Uses SQLAlchemy's inspector which queries ``information_schema`` under
    the hood. The PK is excluded from ``get_indexes`` output (it's reported
    via ``get_pk_constraint``), so every name returned here refers to a
    secondary index and can safely be compared against
    :data:`_SECONDARY_INDEXES`.
    """
    inspector = inspect(engine)
    return {idx["name"] for idx in inspector.get_indexes(_TABLE) if idx.get("name")}


def _drop_index(engine: Engine, index_name: str) -> None:
    """Drop a single secondary index by name if it exists.

    A direct ``DROP INDEX`` is used rather than ``DROP INDEX IF EXISTS``
    because MySQL only added ``IF EXISTS`` for index drops in 8.0.29 and
    we want the loader to work on any MySQL 8 patch level. Presence is
    therefore checked via the inspector before issuing the DROP.
    """
    with engine.begin() as conn:
        conn.execute(text(f"DROP INDEX {index_name} ON {_TABLE}"))


def _create_index(
    engine: Engine, index_name: str, column: str, prefix: "int | None" = None
) -> None:
    """Create a single secondary index (idempotent guard done by caller).

    ``prefix`` mirrors the prefix length used in schema.sql for the long
    VARCHAR(512) multi-select columns; ``None`` indexes the full column.
    """
    col_expr = f"{column}({prefix})" if prefix else column
    with engine.begin() as conn:
        conn.execute(
            text(f"CREATE INDEX {index_name} ON {_TABLE}({col_expr})")
        )


# ---------------------------------------------------------------------------
# Error translation
# ---------------------------------------------------------------------------


def _is_local_infile_rejection(err: DBAPIError) -> bool:
    """Return ``True`` when ``err`` was raised because LOCAL INFILE is off.

    MySQL surfaces this failure mode through a small set of numeric error
    codes, listed in :data:`_LOCAL_INFILE_REJECTED_CODES`. We inspect the
    underlying DBAPI exception (``err.orig``) and match the code.
    """
    orig = getattr(err, "orig", None)
    if orig is None:
        return False
    # PyMySQL exceptions carry (code, message) as ``args``.
    args = getattr(orig, "args", None)
    if not args:
        return False
    first = args[0]
    try:
        code = int(first)
    except (TypeError, ValueError):
        return False
    return code in _LOCAL_INFILE_REJECTED_CODES


def _raise_bulk_load_config_error(cause: Exception) -> None:
    """Raise :class:`BulkLoadConfigError` with the canonical remediation text.

    Centralizing construction guarantees every raise site uses identical
    wording, which is how we satisfy Requirement 8.6's "name both
    settings" clause unambiguously.
    """
    raise BulkLoadConfigError(
        f"{_LOCAL_INFILE_REMEDIATION} Underlying driver error: {cause}"
    ) from cause


# ---------------------------------------------------------------------------
# BulkLoader
# ---------------------------------------------------------------------------


class BulkLoader:
    """Loader strategy for the full-sync fast path.

    The orchestrator drives the loader lifecycle:

    1. Construct ``BulkLoader(engine)`` — no DDL runs yet, so construction
       is safe in dry-run mode and in tests that want to inspect the
       object before touching MySQL.
    2. Call exactly one of:
         * :meth:`drop_secondary_indexes_if_fresh_full_sync` for a fresh
           full sync (``replication_in_progress`` was ``False`` on load),
           which drops the 7 secondary indexes so the bulk load writes
           unindexed pages at full speed (Requirement 8.7).
         * :meth:`ensure_indexes_if_resuming` when resuming a mid-flight
           replication (``replication_in_progress`` was ``True``), which
           recreates any secondary index that a prior crash left missing
           (Requirement 8.8).
    3. Call :meth:`write_batch` once per replication page.
    4. Call :meth:`close` in a ``finally`` block — this recreates every
       secondary index, regardless of which startup branch ran, so a
       post-run full-sync database is in the same schema shape as a
       pre-run one (Requirement 8.7).

    The loader holds the SQLAlchemy :class:`Engine` by reference but does
    not own it: callers construct the engine (with
    ``connect_args={"local_infile": True}``) and remain responsible for
    disposing it.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        # Track what the caller did at startup so close() knows whether
        # it is "restoring" previously-dropped indexes or merely
        # idempotently ensuring they exist. Functionally the close-time
        # behavior is the same (ensure every required index exists); this
        # flag is kept for diagnostic logging only.
        self._fresh_full_sync: bool = False

    # ------------------------------------------------------------------ DDL

    def drop_secondary_indexes_if_fresh_full_sync(
        self, state: SyncState
    ) -> None:
        """Drop the 7 secondary indexes if this is a fresh full sync.

        A "fresh" full sync is one where ``replication_in_progress`` is
        ``False`` on the loaded state — i.e. the pipeline is starting
        from scratch rather than resuming a previous interrupted run.
        For resume runs, the indexes were already dropped on the prior
        startup and we must not re-drop them.

        The method is a no-op when ``state.replication_in_progress`` is
        ``True``; that case is handled by
        :meth:`ensure_indexes_if_resuming` instead.
        """
        if state.replication_in_progress:
            logger.info(
                "BulkLoader: replication already in progress; skipping "
                "secondary-index drop (resume path will ensure indexes)"
            )
            return
        self._fresh_full_sync = True
        existing = _existing_index_names(self._engine)
        for name, _column, _prefix in _SECONDARY_INDEXES:
            if name in existing:
                logger.info("BulkLoader: dropping secondary index %s", name)
                _drop_index(self._engine, name)
            else:
                # Missing at the start of a fresh full sync is unusual
                # (the schema just got applied) but not fatal; close()
                # will create it. Log so operators can spot schema drift.
                logger.warning(
                    "BulkLoader: secondary index %s already missing at "
                    "start of fresh full sync",
                    name,
                )

    def ensure_indexes_if_resuming(self, state: SyncState) -> None:
        """Recreate any of the 7 required indexes that are missing on resume.

        Called at startup when ``state.replication_in_progress`` is
        ``True``. A prior run that crashed between "drop indexes" and
        "recreate indexes" leaves an arbitrary subset of the 7 indexes
        absent from the table; this method brings the table back to a
        known-good shape before extraction resumes (Requirement 8.8).

        When ``state.replication_in_progress`` is ``False`` the method is
        a no-op; fresh-sync callers use
        :meth:`drop_secondary_indexes_if_fresh_full_sync` instead.
        """
        if not state.replication_in_progress:
            return
        existing = _existing_index_names(self._engine)
        for name, column, prefix in _SECONDARY_INDEXES:
            if name not in existing:
                logger.info(
                    "BulkLoader: resume path recreating missing index %s "
                    "on column %s",
                    name,
                    column,
                )
                _create_index(self._engine, name, column, prefix)

    # --------------------------------------------------------------- batches

    def write_batch(self, rows: list[Row]) -> BatchResult:
        """Write ``rows`` to a CSV and ingest it via ``LOAD DATA LOCAL INFILE``.

        Per Requirement 3.9, each replication page is committed as a
        single batch: one CSV file, one ``LOAD DATA LOCAL INFILE``. The
        loader does not aggregate across pages. ``REPLACE INTO`` rather
        than ``INSERT`` lets a retry of a previously-partially-loaded
        page remain idempotent on ``ListingKey`` (Requirement 7.6's
        idempotence property applies to the bulk path as well).

        ``loaded_at`` is computed once per batch from
        ``datetime.now(UTC)`` and applied to every row in the CSV. Taking
        the reading once keeps all rows in a batch textually identical in
        that column and matches Requirement 6.7's "batch-commit time"
        semantics.

        On success, returns a :class:`BatchResult` carrying the row count
        and the max ``ModificationTimestamp`` seen in the batch. On
        failure, the temporary directory is cleaned up and the error is
        re-raised; ``LOAD DATA LOCAL INFILE`` rejection specifically is
        translated to :class:`BulkLoadConfigError`.

        An empty ``rows`` list short-circuits without touching MySQL. That
        case is defensive — the orchestrator never yields empty pages —
        but it avoids a spurious CSV and keeps the caller's loop simple.
        """
        if not rows:
            return BatchResult(
                count=0,
                max_modification_timestamp=datetime.min.replace(tzinfo=UTC),
            )

        # Single wall-clock reading per batch (Requirement 6.7). Taken
        # BEFORE any disk I/O so the value is as close to "batch start"
        # as practical and every row in the batch shares it.
        loaded_at = datetime.now(UTC)

        tmpdir = Path(tempfile.mkdtemp(prefix="trestle-bulk-"))
        property_csv = tmpdir / "property.csv"
        raw_csv = tmpdir / "property_raw.csv"
        try:
            max_mod_ts = self._write_csvs(
                rows, property_csv, raw_csv, loaded_at
            )
            self._load_csvs(property_csv, raw_csv)
        finally:
            # Remove the CSVs and their parent directory whether or not the
            # load succeeded (Requirement 8.4). On the failure path this
            # prevents accumulation of orphaned temp files under /tmp on
            # long-running systems.
            shutil.rmtree(tmpdir, ignore_errors=True)

        return BatchResult(
            count=len(rows),
            max_modification_timestamp=max_mod_ts,
        )

    def _write_csvs(
        self,
        rows: list[Row],
        property_csv: Path,
        raw_csv: Path,
        loaded_at: datetime,
    ) -> datetime:
        """Serialize ``rows`` to two CSV files and return the max ModTs.

        Writes one CSV for the `property` table (promoted columns +
        ``loaded_at``) and one for `property_raw` (``ListingKey`` +
        ``raw_data`` + ``loaded_at``) in a single pass over ``rows``. The
        promoted-columns tuple carries ``ModificationTimestamp`` at a
        known index; we scan it here to produce the
        :class:`BatchResult`'s ``max_modification_timestamp`` without a
        second pass.
        """
        mod_ts_index = PROMOTED_COLUMNS.index("ModificationTimestamp")
        listing_key_index = PROMOTED_COLUMNS.index("ListingKey")
        max_mod_ts: datetime | None = None
        # utf-8 bytes, newline='' so the OS doesn't translate our \n into
        # \r\n on any platform (our LINES TERMINATED BY is exactly \n).
        with open(property_csv, "w", encoding="utf-8", newline="") as pfh, open(
            raw_csv, "w", encoding="utf-8", newline=""
        ) as rfh:
            for promoted, raw_data_json in rows:
                ts = promoted[mod_ts_index]
                if ts is not None and (max_mod_ts is None or ts > max_mod_ts):
                    max_mod_ts = ts
                # property CSV: promoted columns + loaded_at.
                pfh.write(_format_row((*promoted, loaded_at)))
                # property_raw CSV: ListingKey, raw_data, loaded_at.
                rfh.write(
                    _format_row(
                        (promoted[listing_key_index], raw_data_json, loaded_at)
                    )
                )
            pfh.flush()
            os.fsync(pfh.fileno())
            rfh.flush()
            os.fsync(rfh.fileno())
        if max_mod_ts is None:
            # Defensive: a page where every row lacks ModificationTimestamp
            # would leave the state unchanged. Use UTC epoch as the
            # smallest possible sentinel; callers compare via ``max(...)``
            # against the running state value so this is safe.
            max_mod_ts = datetime.min.replace(tzinfo=UTC)
        return max_mod_ts

    def _load_csvs(self, property_csv: Path, raw_csv: Path) -> None:
        """Bulk-load both CSV files inside a single transaction.

        Each ``LOAD DATA LOCAL INFILE`` path is interpolated into the SQL
        text because MySQL does not accept a placeholder in the
        ``LOCAL INFILE`` position. We control the paths completely (they
        come from :func:`tempfile.mkdtemp`), so interpolation is safe;
        defensive escaping of backslash and single-quote guards against
        any future path sources. Both loads run in one ``engine.begin()``
        block so a batch is all-or-nothing across `property` and
        `property_raw` (Requirement 3.9, 7.6).
        """
        property_stmt = self._build_load_stmt(
            property_csv, _TABLE, _PROPERTY_CSV_COLUMNS
        )
        raw_stmt = self._build_load_stmt(raw_csv, _TABLE_RAW, _RAW_CSV_COLUMNS)
        try:
            with self._engine.begin() as conn:
                conn.execute(text(property_stmt))
                conn.execute(text(raw_stmt))
        except DBAPIError as exc:
            if _is_local_infile_rejection(exc):
                _raise_bulk_load_config_error(exc)
            raise

    @staticmethod
    def _build_load_stmt(
        csv_path: Path, table: str, columns: tuple[str, ...]
    ) -> str:
        """Build one ``LOAD DATA LOCAL INFILE`` statement for ``table``."""
        escaped_path = str(csv_path).replace("\\", "\\\\").replace("'", "\\'")
        column_list = ", ".join(columns)
        return (
            f"LOAD DATA LOCAL INFILE '{escaped_path}' "
            f"REPLACE INTO TABLE {table} "
            f"CHARACTER SET utf8mb4 "
            f"FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\\\\' "
            f"LINES TERMINATED BY '\\n' "
            f"({column_list})"
        )

    # ----------------------------------------------------------------- close

    def close(self) -> None:
        """Recreate every required secondary index that is missing.

        Called from the orchestrator's ``finally`` block so that the
        table is always restored to schema-shape equivalence with its
        pre-run state, regardless of whether the run succeeded, raised,
        or was interrupted by SIGINT. The call is idempotent: indexes
        already present are left alone.

        The loader does NOT dispose of ``self._engine``; engine ownership
        belongs to the caller that constructed it.
        """
        existing = _existing_index_names(self._engine)
        for name, column, prefix in _SECONDARY_INDEXES:
            if name not in existing:
                logger.info(
                    "BulkLoader.close: recreating index %s on column %s",
                    name,
                    column,
                )
                _create_index(self._engine, name, column, prefix)


__all__ = ["BulkLoader"]
