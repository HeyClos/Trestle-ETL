"""Loader subpackage: upsert and bulk load paths.

Defines the :class:`Loader` protocol shared by :mod:`trestle_etl.loader.upsert`
and :mod:`trestle_etl.loader.bulk`, along with the :class:`BatchResult` value
type returned from a successful batch commit and the :data:`Row` type alias
shared by the transformer and both loader implementations.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable

# A Row is the transformer's output: a tuple of typed Promoted_Column values
# paired with the JSON-serialized original raw record (Requirement 5.5, 14.4).
# Keeping this as a top-level alias lets the transformer and both loaders share
# a single definition without circular imports.
Row = tuple[tuple, str]


@dataclass
class BatchResult:
    """Result of a successful :meth:`Loader.write_batch` call.

    Attributes:
        count: Number of rows committed in the batch.
        max_modification_timestamp: The highest ``ModificationTimestamp`` seen
            across the committed rows. The orchestrator uses this to advance
            the persisted ``last_modification_timestamp`` (Requirement 4.5).
    """

    count: int
    max_modification_timestamp: datetime


@runtime_checkable
class Loader(Protocol):
    """Common interface implemented by the upsert and bulk loader strategies.

    Implementations commit each batch atomically and report the committed
    count plus the highest observed ``ModificationTimestamp`` back to the
    orchestrator (Requirement 7.5). On failure, implementations roll back and
    re-raise so the orchestrator does not advance state (Requirement 7.4).
    """

    def write_batch(self, rows: list[Row]) -> BatchResult:
        """Commit ``rows`` in a single transaction and return the result."""
        ...

    def close(self) -> None:
        """Release resources held by the loader (connections, indexes, etc)."""
        ...


__all__ = ["BatchResult", "Loader", "Row"]
