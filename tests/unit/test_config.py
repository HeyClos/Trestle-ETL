"""Unit tests for ``trestle_etl.config.Settings.load``.

Validates Requirement 13.3: when a required environment variable is missing,
``Settings.load()`` raises ``ConfigError`` whose message names the missing
variable. Also covers the happy path, including default fall-through for
``MYSQL_PORT``, ``STATE_FILE_PATH``, and ``PAGE_SIZE``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from trestle_etl.config import Settings
from trestle_etl.errors import ConfigError


# Every environment variable read by ``config.py``. Centralized so the
# isolation fixture always wipes the same surface, keeping tests
# independent of whatever is exported in the developer's shell or
# present in a local ``.env`` file.
ALL_ENV_VARS = (
    "TRESTLE_BASE_URL",
    "TRESTLE_TOKEN_URL",
    "TRESTLE_CLIENT_ID",
    "TRESTLE_CLIENT_SECRET",
    "MYSQL_HOST",
    "MYSQL_PORT",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
    "MYSQL_DATABASE",
    "STATE_FILE_PATH",
    "PAGE_SIZE",
)


# Baseline values used to populate the environment for the happy path
# and for missing-variable tests (which remove exactly one name before
# calling ``load()``).
_VALID_ENV = {
    "TRESTLE_BASE_URL": "https://api.example.com/trestle/odata/",
    "TRESTLE_TOKEN_URL": "https://api.example.com/trestle/oidc/connect/token",
    "TRESTLE_CLIENT_ID": "client-abc",
    "TRESTLE_CLIENT_SECRET": "secret-xyz",
    "MYSQL_HOST": "db.internal",
    "MYSQL_USER": "etl",
    "MYSQL_PASSWORD": "hunter2",
    "MYSQL_DATABASE": "trestle",
}


REQUIRED_VARS = (
    "TRESTLE_BASE_URL",
    "TRESTLE_TOKEN_URL",
    "TRESTLE_CLIENT_ID",
    "TRESTLE_CLIENT_SECRET",
    "MYSQL_HOST",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
    "MYSQL_DATABASE",
)


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch):
    """Ensure tests see a clean, reproducible environment.

    Two independent sources of leakage need to be closed:

    1. Variables exported in the developer's shell. ``monkeypatch.delenv``
       with ``raising=False`` handles this by unsetting each name for the
       duration of the test.
    2. A local ``.env`` file. ``Settings.load()`` calls ``load_dotenv()``
       as its first action, so stubbing that symbol to a no-op prevents
       any real ``.env`` from silently reintroducing values that would
       defeat the missing-variable tests.
    """
    for name in ALL_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("trestle_etl.config.load_dotenv", lambda *a, **k: None)


def _set_required(monkeypatch, overrides=None):
    """Populate every required env var; ``overrides`` may remove or adjust."""
    env = dict(_VALID_ENV)
    if overrides:
        env.update(overrides)
    for name, value in env.items():
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)


def test_load_happy_path_returns_frozen_settings_with_defaults(monkeypatch):
    """All required vars set, optionals unset -> defaults apply, instance frozen."""
    _set_required(monkeypatch)

    settings = Settings.load()

    assert settings.trestle_base_url == _VALID_ENV["TRESTLE_BASE_URL"]
    assert settings.trestle_token_url == _VALID_ENV["TRESTLE_TOKEN_URL"]
    assert settings.client_id == _VALID_ENV["TRESTLE_CLIENT_ID"]
    assert settings.client_secret == _VALID_ENV["TRESTLE_CLIENT_SECRET"]
    assert settings.mysql_host == _VALID_ENV["MYSQL_HOST"]
    assert settings.mysql_user == _VALID_ENV["MYSQL_USER"]
    assert settings.mysql_password == _VALID_ENV["MYSQL_PASSWORD"]
    assert settings.mysql_database == _VALID_ENV["MYSQL_DATABASE"]

    # Defaults must fall through when the optional variables are unset.
    assert settings.mysql_port == 3306
    assert settings.state_file_path == Path("sync_state.json")
    assert settings.default_page_size == 1000

    # The dataclass is frozen per the design, so mutation must fail.
    with pytest.raises(Exception):
        settings.mysql_host = "other"  # type: ignore[misc]


def test_load_honors_optional_overrides(monkeypatch):
    """Optional vars override their defaults when provided."""
    _set_required(monkeypatch)
    monkeypatch.setenv("MYSQL_PORT", "3307")
    monkeypatch.setenv("STATE_FILE_PATH", "/var/lib/trestle/state.json")
    monkeypatch.setenv("PAGE_SIZE", "2500")

    settings = Settings.load()

    assert settings.mysql_port == 3307
    assert settings.state_file_path == Path("/var/lib/trestle/state.json")
    assert settings.default_page_size == 2500


@pytest.mark.parametrize("missing", REQUIRED_VARS)
def test_load_raises_config_error_naming_missing_required_var(monkeypatch, missing):
    """Each required variable, when absent, triggers a named ConfigError.

    Validates Requirement 13.3: the error message SHALL name the missing
    variable so operators can remediate without reading a stack trace.
    """
    # Remove exactly one required variable; leave every other required
    # variable set so we isolate which name the error references.
    _set_required(monkeypatch, overrides={missing: None})

    with pytest.raises(ConfigError) as excinfo:
        Settings.load()

    assert missing in str(excinfo.value)


@pytest.mark.parametrize("missing", REQUIRED_VARS)
def test_load_treats_empty_required_var_as_missing(monkeypatch, missing):
    """An empty string is treated as missing, same as unset.

    This is part of the same Req 13.3 guarantee: a blank value in ``.env``
    is an operator mistake, not a valid credential, and must be reported
    with the same named-variable error.
    """
    _set_required(monkeypatch, overrides={missing: ""})

    with pytest.raises(ConfigError) as excinfo:
        Settings.load()

    assert missing in str(excinfo.value)
