"""Exceptions raised by the Trestle ETL pipeline.

Centralizing error types here keeps the public surface discoverable and lets
callers import by a stable path regardless of which internal module raises
the error.
"""

from __future__ import annotations


class TrestleETLError(Exception):
    """Base class for all errors raised by the Trestle ETL pipeline."""


class ConfigError(TrestleETLError):
    """Raised when required configuration is missing or invalid.

    The message SHALL name the offending environment variable so that the
    operator can remediate without inspecting a stack trace
    (Requirement 13.3).
    """


class CorruptStateError(TrestleETLError):
    """Raised when the state file is present on disk but cannot be parsed.

    Per Requirement 9.8, encountering a malformed state file is a hard
    failure: the pipeline exits non-zero without modifying the file so the
    operator can inspect or repair it. The message includes the offending
    path to make remediation obvious.
    """


class AuthError(TrestleETLError):
    """Raised when OAuth2 token acquisition fails.

    Covers transport-level failures against the Trestle OIDC token endpoint
    (connection errors, non-2xx status codes) and malformed token responses
    that lack the expected ``access_token`` / ``expires_in`` fields. Also
    raised by the Trestle_Client when a post-401 re-authentication attempt
    fails (Requirement 1.7).
    """


class BulkLoadConfigError(TrestleETLError):
    """Raised when MySQL rejects ``LOAD DATA LOCAL INFILE`` (Requirement 8.6).

    The pipeline's bulk-load fast path depends on ``LOAD DATA LOCAL INFILE``,
    which requires two independent configuration toggles:

    * ``local_infile=1`` on the MySQL **server** (e.g. in ``my.cnf`` or set
      via ``SET GLOBAL local_infile=1``).
    * ``local_infile=True`` on the **client** connection (passed through
      PyMySQL via SQLAlchemy's ``connect_args``).

    When MySQL rejects the statement because either toggle is missing, the
    driver surfaces error code 1148 (``ER_NOT_ALLOWED_COMMAND``) or, on
    MySQL 8, 3948 (``ER_LOAD_DATA_LOCAL_INFILE_DISABLED``). The
    :class:`~trestle_etl.loader.bulk.BulkLoader` catches both, translates
    them into this exception, and ensures the error message explicitly
    names **both** settings so the operator knows to check server and
    client side, not just one.
    """


class UsageError(TrestleETLError):
    """Raised when the CLI flag combination is invalid (Requirement 11.2).

    Distinct from argparse's own parse-time usage errors (which it handles
    internally by calling ``sys.exit(2)``). ``UsageError`` covers the
    semantic combination rules that argparse cannot express directly — for
    example, "only one of ``{--full-sync, --incremental, --reconcile}``
    may be supplied" or "``--dry-run`` may not combine with ``--reconcile``".

    Lives in :mod:`trestle_etl.errors` rather than :mod:`trestle_etl.cli`
    so it remains a subclass of :class:`TrestleETLError` and shares the
    standard exception hierarchy with every other pipeline error. The CLI
    catches this at the top level and translates it into exit code 2 plus
    a stderr message that names the violated rule.
    """


class TrestleHTTPError(TrestleETLError):
    """Raised when the Trestle WebAPI returns an unrecoverable HTTP error.

    Covers two distinct cases, both carrying the same tuple of diagnostic
    attributes so that callers (and logs) can distinguish them by status
    code alone:

    1. A 4xx response other than 401 — these are not retried, per the
       Requirement 2 design table.
    2. A 429, 504, or other 5xx response that persisted across the full
       shared retry budget of 6 attempts (Requirement 2.6).

    The ``body_excerpt`` is capped at ~200 characters by the Trestle_Client
    before construction so that exception messages remain actionable and
    log lines do not balloon when the server returns a large HTML error
    page.
    """

    def __init__(self, status: int, body_excerpt: str, url: str) -> None:
        # Store the triple as attributes so callers (orchestrator, tests)
        # can match on status without parsing the message string.
        self.status = status
        self.body_excerpt = body_excerpt
        self.url = url
        super().__init__(
            f"Trestle API returned HTTP {status} for {url}: {body_excerpt}"
        )


class PipelineLockError(TrestleETLError):
    """Raised when another pipeline instance already holds the run lock.

    The pipeline serializes itself with an advisory file lock so that two
    concurrent invocations cannot race on the atomic State_Store replace
    (which previously corrupted progress tracking when two ``--full-sync``
    runs overlapped). When the lock is already held, the second invocation
    raises this error rather than proceeding. The message names the lock
    path and, when known, the PID recorded in the lock file so an operator
    can identify the running process.
    """


__all__ = [
    "TrestleETLError",
    "ConfigError",
    "CorruptStateError",
    "AuthError",
    "BulkLoadConfigError",
    "TrestleHTTPError",
    "PipelineLockError",
    "UsageError",
]
