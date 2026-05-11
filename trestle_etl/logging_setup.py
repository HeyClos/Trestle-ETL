"""Structured logging configuration for the Trestle ETL pipeline.

This module centralizes log formatting so that every operational message
flows through the Python ``logging`` module (Requirement 12.1) and carries
a consistent ISO 8601 UTC timestamp, level, logger name, and message. It
also exposes small helpers for the run-start and run-end INFO entries
required by Requirements 12.3 and 12.4.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

# Sentinel attribute used to mark handlers we have installed, so that
# ``configure_logging()`` can be called more than once without stacking
# duplicate handlers on the root logger.
_HANDLER_MARKER = "_trestle_etl_configured"

# Format string applied to our structured handler. The asctime field is
# produced by ``_UtcIsoFormatter.formatTime`` below so that timestamps are
# ISO 8601 with explicit UTC offset and microsecond precision.
_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


class _UtcIsoFormatter(logging.Formatter):
    """Formatter that emits ISO 8601 UTC timestamps with microsecond precision."""

    def formatTime(  # noqa: N802 - required override signature
        self, record: logging.LogRecord, datefmt: Optional[str] = None
    ) -> str:
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        if datefmt:
            return dt.strftime(datefmt)
        # e.g. "2024-03-14T17:32:00.123456+00:00"
        return dt.isoformat()


def configure_logging(level: int = logging.INFO) -> None:
    """Install a structured handler on the root logger.

    The handler writes to stderr with an ISO 8601 UTC timestamp, the level
    name, the logger name, and the message. The function is idempotent:
    calling it twice leaves exactly one Trestle-installed handler on the
    root logger, and subsequent calls only refresh the level.
    """

    root = logging.getLogger()

    # Idempotence: if we have already installed a handler, just update the
    # level on both the root logger and that handler, and return.
    for existing in root.handlers:
        if getattr(existing, _HANDLER_MARKER, False):
            existing.setLevel(level)
            root.setLevel(level)
            return

    handler = logging.StreamHandler()
    handler.setFormatter(_UtcIsoFormatter(_LOG_FORMAT))
    handler.setLevel(level)
    # Tag so later calls can identify our handler.
    setattr(handler, _HANDLER_MARKER, True)

    root.addHandler(handler)
    root.setLevel(level)


def _format_timestamp(ts: Optional[datetime]) -> str:
    """Render a timestamp for human-readable log messages, or ``<none>``."""
    if ts is None:
        return "<none>"
    if ts.tzinfo is None:
        # Treat naive datetimes as UTC rather than silently dropping tz info.
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat()


def log_run_start(
    logger: logging.Logger,
    mode: str,
    base_url: str,
    last_modification_timestamp: Optional[datetime],
) -> None:
    """Emit the INFO run-start log entry required by Requirement 12.3.

    Includes the invoked mode, the resolved Trestle base URL, and the
    ``last_modification_timestamp`` read from the State_Store.
    """
    logger.info(
        "run_start mode=%s base_url=%s last_modification_timestamp=%s",
        mode,
        base_url,
        _format_timestamp(last_modification_timestamp),
    )


def log_run_end(
    logger: logging.Logger,
    total_records: int,
    elapsed_seconds: float,
    last_modification_timestamp: Optional[datetime],
) -> None:
    """Emit the INFO run-end log entry required by Requirement 12.4.

    Includes the total records loaded, total elapsed wall-clock time, and
    the final ``last_modification_timestamp`` persisted to the State_Store.
    """
    logger.info(
        "run_end total_records=%d elapsed_seconds=%.3f last_modification_timestamp=%s",
        total_records,
        elapsed_seconds,
        _format_timestamp(last_modification_timestamp),
    )
