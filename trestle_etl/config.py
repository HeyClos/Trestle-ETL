"""Configuration loading for the Trestle ETL pipeline.

All runtime configuration is sourced from environment variables, optionally
populated from a local ``.env`` file via ``python-dotenv``. No configuration
value is read from anywhere else: command-line flags override behavior but
not identity or credentials.

Implements Requirements 13.1, 13.2, 13.3, and 13.5.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from .errors import ConfigError

# Default values for variables that have sensible fallbacks per the design.
_DEFAULT_MYSQL_PORT = 3306
_DEFAULT_STATE_FILE_PATH = Path("sync_state.json")
_DEFAULT_PAGE_SIZE = 1000

# Environment variable names. Centralized so that error messages, tests,
# and .env.example stay aligned with one source of truth.
_ENV_TRESTLE_BASE_URL = "TRESTLE_BASE_URL"
_ENV_TRESTLE_TOKEN_URL = "TRESTLE_TOKEN_URL"
_ENV_CLIENT_ID = "TRESTLE_CLIENT_ID"
_ENV_CLIENT_SECRET = "TRESTLE_CLIENT_SECRET"
_ENV_MYSQL_HOST = "MYSQL_HOST"
_ENV_MYSQL_PORT = "MYSQL_PORT"
_ENV_MYSQL_USER = "MYSQL_USER"
_ENV_MYSQL_PASSWORD = "MYSQL_PASSWORD"
_ENV_MYSQL_DATABASE = "MYSQL_DATABASE"
_ENV_STATE_FILE_PATH = "STATE_FILE_PATH"
_ENV_PAGE_SIZE = "PAGE_SIZE"


@dataclass(frozen=True)
class Settings:
    """Immutable snapshot of runtime configuration.

    Every field corresponds to one environment variable enumerated in
    Requirement 13.2. The dataclass is frozen so that configuration cannot
    be mutated after load, which simplifies reasoning about the rest of the
    pipeline.
    """

    trestle_base_url: str
    trestle_token_url: str
    client_id: str
    client_secret: str
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_database: str
    state_file_path: Path
    default_page_size: int

    @classmethod
    def load(cls) -> "Settings":
        """Load configuration from a ``.env`` file and the process environment.

        Values set in the real environment take precedence over ``.env``
        entries (this is the default ``python-dotenv`` behavior and matches
        operator expectations: shell exports win).

        Raises:
            ConfigError: If any required variable is missing, or if an
                optional variable that must be numeric cannot be parsed.
        """
        # Populate os.environ from .env if present; no-op if the file is
        # absent. Downstream reads go through os.environ so tests can stub
        # values with monkeypatch.setenv without touching the filesystem.
        load_dotenv()

        return cls(
            trestle_base_url=_require_str(_ENV_TRESTLE_BASE_URL),
            trestle_token_url=_require_str(_ENV_TRESTLE_TOKEN_URL),
            client_id=_require_str(_ENV_CLIENT_ID),
            client_secret=_require_str(_ENV_CLIENT_SECRET),
            mysql_host=_require_str(_ENV_MYSQL_HOST),
            mysql_port=_optional_int(_ENV_MYSQL_PORT, _DEFAULT_MYSQL_PORT),
            mysql_user=_require_str(_ENV_MYSQL_USER),
            mysql_password=_require_str(_ENV_MYSQL_PASSWORD),
            mysql_database=_require_str(_ENV_MYSQL_DATABASE),
            state_file_path=_optional_path(
                _ENV_STATE_FILE_PATH, _DEFAULT_STATE_FILE_PATH
            ),
            default_page_size=_optional_int(
                _ENV_PAGE_SIZE, _DEFAULT_PAGE_SIZE
            ),
        )


def _require_str(name: str) -> str:
    """Return the environment variable, or raise ConfigError naming it.

    An empty string is treated as missing: a blank secret in ``.env`` is
    almost always an operator mistake, and silently accepting it would
    surface as an opaque authentication failure far later in the pipeline.
    """
    value = os.environ.get(name)
    if value is None or value == "":
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _optional_int(name: str, default: int) -> int:
    """Return the env var parsed as int, falling back to default when unset.

    A present-but-unparseable value is a configuration error (not a silent
    fallback) because the operator clearly intended to override the default.
    """
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(
            f"Environment variable {name} must be an integer, got {raw!r}"
        ) from exc


def _optional_path(name: str, default: Path) -> Path:
    """Return the env var as a Path, falling back to default when unset."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return Path(raw)


__all__ = ["Settings", "ConfigError"]
