"""Upsert loader: transactional batched ``INSERT ... ON DUPLICATE KEY UPDATE``.

Used by the incremental sync path (and the ``--since`` override). Each call
to :meth:`UpsertLoader.write_batch` executes a single SQL statement inside a
single transaction; on failure the transaction rolls back and the exception
propagates so that the orchestrator does not advance the State_Store
(Requirements 7.4 and 9.4).

Requirements validated:
    - 6.7: ``loaded_at`` is set to ``datetime.now(UTC)`` at commit time by
      the loader itself, never by a MySQL ``DEFAULT CURRENT_TIMESTAMP``
      clause.
    - 7.1: Uses ``INSERT ... ON DUPLICATE KEY UPDATE``.
    - 7.2: Configurable batch size, default 1,000, capped at 5,000. The
      loader enforces the cap by refusing batches larger than 5,000 rows.
    - 7.3: Each batch wrapped in exactly one transaction.
    - 7.4: On failure, the transaction rolls back and the error is re-raised.
    - 7.5: Returns ``BatchResult(count, max_modification_timestamp)``.
    - 7.7: SQLAlchemy Core plus ``pymysql``; no ORM involvement.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Final

from sqlalchemy import text
from sqlalchemy.engine import Engine

from trestle_etl.loader import BatchResult, Row
from trestle_etl.transformer import PROMOTED_COLUMNS

logger = logging.getLogger(__name__)

# Hard ceiling from Requirement 7.2. Callers that ask for more than this
# are rejected at construction rather than at write time so misconfiguration
# surfaces during wiring, not in the middle of a long-running sync.
_MAX_BATCH_SIZE: Final[int] = 5_000

# Default batch size from Requirement 7.2.
_DEFAULT_BATCH_SIZE: Final[int] = 1_000

# Columns that participate in the INSERT: every promoted column, plus the
# ``raw_data`` JSON payload and the loader-supplied ``loaded_at`` stamp.
# Built once at import time so the SQL statement itself can also be
# constructed once and reused across batches.
_ALL_COLUMNS: Final[tuple[str, ...]] = (
    *PROMOTED_COLUMNS,
    "raw_data",
    "loaded_at",
)

# Index of the ModificationTimestamp field inside the promoted-columns
# tuple. Used by :meth:`UpsertLoader.write_batch` to compute the batch's
# max_modification_timestamp without re-scanning the tuple layout on every
# call.
_MOD_TS_INDEX: Final[int] = PROMOTED_COLUMNS.index("ModificationTimestamp")


def _build_upsert_sql() -> str:
    """Construct the parameterized upsert statement once at import time.

    The statement is dynamically built from :data:`PROMOTED_COLUMNS` so that
    adding a new promoted field is a one-place schema change
    (Requirement 7.1). Column identifiers are interpolated from a trusted
    constant; row values are bound through SQLAlchemy parameters and are
    never concatenated into the SQL string.

    The ON DUPLICATE KEY UPDATE clause covers every column except the
    primary key (``ListingKey``): rewriting the PK to its own value would
    be a no-op, and MySQL 8's optimizer is slightly cleaner when it is
    omitted. ``VALUES(col)`` is used rather than the newer row-alias
    syntax so the statement stays compatible with MySQL 5.7 as well as 8.x.
    """
    columns_sql = ", ".join(_ALL_COLUMNS)
    placeholders_sql = ", ".join(f":{col}" for col in _ALL_COLUMNS)
    update_targets = [col for col in _ALL_COLUMNS if col != "ListingKey"]
    update_sql = ", ".join(f"{col} = VALUES({col})" for col in update_targets)
    return (
        f"INSERT INTO property ({columns_sql}) "
        f"VALUES ({placeholders_sql}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )


# Pre-built once; SQLAlchemy will re-compile it per engine but the Python
# string assembly (non-trivial with 30+ columns) happens only at import.
_UPSERT_SQL: Final[str] = _build_upsert_sql()


class UpsertLoader:
    """Transactional batched upsert loader against the ``property`` table.

    The loader does not own its ``Engine``: the caller (typically
    :mod:`trestle_etl.cli`) constructs the engine so that connection-pool
    lifetime is tied to the overall CLI invocation and the engine can be
    shared across loader instances if the pipeline ever needs that.
    """

    def __init__(self, engine: Engine, *, batch_size: int = _DEFAULT_BATCH_SIZE) -> None:
        if batch_size <= 0:
            # A non-positive batch size would either deadlock the caller
            # (batch_size=0 → never flush) or be logically meaningless
            # (negative). Reject at construction rather than surfacing a
            # confusing ValueError deep inside ``write_batch``.
            raise ValueError(
                f"batch_size must be positive, got {batch_size}"
            )
        if batch_size > _MAX_BATCH_SIZE:
            # Requirement 7.2 caps upsert batches at 5,000 rows.
            raise ValueError(
                f"batch_size must be <= {_MAX_BATCH_SIZE}, got {batch_size}"
            )

        self._engine = engine
        self._batch_size = batch_size

    @property
    def batch_size(self) -> int:
        """Maximum number of rows accepted in a single ``write_batch`` call."""
        return self._batch_size

    def write_batch(self, rows: list[Row]) -> BatchResult:
        """Upsert ``rows`` in a single transaction.

        The orchestrator calls this once per extractor page. The loader
        wraps the entire batch in one ``engine.begin()`` block so that
        either every row is committed together (advancing
        ``last_modification_timestamp``) or none are (leaving state
        untouched for a clean retry). The batch must not exceed
        :attr:`batch_size`; larger inputs indicate an orchestrator bug and
        are rejected up front rather than silently chunked, which would
        break the one-transaction-per-page contract.
        """
        if not rows:
            # An empty page is harmless: return a zero-count result rather
            # than issuing a no-op transaction. The orchestrator recognizes
            # count==0 and skips the State_Store update for this batch.
            return BatchResult(count=0, max_modification_timestamp=None)  # type: ignore[arg-type]

        if len(rows) > self._batch_size:
            # Hard cap: if the caller hands us more than the loader was
            # configured to handle, fail loudly. Silent chunking would
            # split one orchestrator page across multiple transactions,
            # which contradicts Requirement 7.3.
            raise ValueError(
                f"batch contains {len(rows)} rows; exceeds configured "
                f"batch_size {self._batch_size}"
            )

        # Compute ``loaded_at`` once per batch and stamp every row with the
        # same value (Requirement 6.7). Using ``datetime.now(timezone.utc)``
        # produces a tz-aware UTC timestamp that ``pymysql`` serializes to
        # a MySQL DATETIME(6) with microsecond precision.
        loaded_at = datetime.now(timezone.utc)

        params: list[dict[str, object]] = []
        max_mod_ts: datetime | None = None
        for promoted, raw_data_json in rows:
            # Pair the promoted-column names with the tuple values the
            # transformer produced; append ``raw_data`` and ``loaded_at``
            # so every bind parameter in the SQL is supplied.
            row_params: dict[str, object] = dict(zip(PROMOTED_COLUMNS, promoted))
            row_params["raw_data"] = raw_data_json
            row_params["loaded_at"] = loaded_at
            params.append(row_params)

            # Track the running max ModificationTimestamp so the caller
            # can advance ``last_modification_timestamp`` in the state
            # file once the transaction commits (Requirement 7.5).
            row_mod_ts = promoted[_MOD_TS_INDEX]
            if row_mod_ts is not None and (
                max_mod_ts is None or row_mod_ts > max_mod_ts
            ):
                max_mod_ts = row_mod_ts

        statement = text(_UPSERT_SQL)

        # ``engine.begin()`` is the canonical SQLAlchemy 2.0 transaction
        # scope: it issues BEGIN on entry, COMMIT on clean exit, and
        # ROLLBACK on any exception before re-raising. That gives us
        # Requirements 7.3 and 7.4 for free, without hand-rolling a
        # try/except/rollback block.
        with self._engine.begin() as connection:
            connection.execute(statement, params)

        logger.info(
            "Upserted batch of %d rows (max ModificationTimestamp=%s)",
            len(rows),
            max_mod_ts,
        )

        return BatchResult(
            count=len(rows),
            # ``max_mod_ts`` is only ``None`` if every row in the batch
            # lacked a ModificationTimestamp, which the Trestle API does
            # not produce in practice. We still type-narrow here so the
            # orchestrator can surface the unusual case rather than
            # silently reporting ``None`` where a datetime is expected.
            max_modification_timestamp=max_mod_ts,  # type: ignore[arg-type]
        )

    def close(self) -> None:
        """No-op: the engine is owned by the caller and disposed there."""
        # Intentionally empty. The caller that built the Engine is also
        # responsible for ``engine.dispose()`` once the pipeline is done.
        # Keeping ``close`` as a no-op satisfies the :class:`Loader`
        # protocol without coupling this loader's lifetime to the
        # engine's.


__all__ = ["UpsertLoader"]
