"""Unit tests for CLI error paths and their documented exit codes.

Validates three distinct error paths in :mod:`trestle_etl.cli`, each tied
to an acceptance criterion in the requirements document:

* **Requirement 11.5** — ``--reconcile`` is a placeholder and must exit
  non-zero with a message stating that reconcile is not implemented in
  the first iteration.
* **Requirement 11.6** — Invoking the CLI with no mode flag must exit
  non-zero with usage information.
* **Requirement 4.2** — Invoking ``--incremental`` when the State_Store
  has no ``last_modification_timestamp`` must exit non-zero with a
  remediation message directing the operator to ``--full-sync`` or
  ``--since``.

Each test pins both the numeric exit code (via the symbolic constant
exported from ``cli``) and a substring of the stderr output. Pinning the
numeric code protects shell-level callers that branch on exit status;
pinning a substring of the message protects operators who only see the
terminal output.

The fixture below wipes every environment variable the pipeline reads
(so a developer's shell exports cannot leak in), installs a known-good
set of config values so ``Settings.load()`` succeeds, and stubs
``trestle_etl.config.load_dotenv`` to a no-op so a local ``.env`` file
cannot reintroduce values silently. ``STATE_FILE_PATH`` is pointed at a
pytest ``tmp_path`` entry that does **not** exist, which is precisely
the "uninitialized pipeline" condition the Req 4.2 test needs.
"""

from __future__ import annotations

import pytest

from trestle_etl.cli import (
    EXIT_MISSING_STATE,
    EXIT_RECONCILE_PLACEHOLDER,
    EXIT_USAGE_ERROR,
    main,
)


# Known-good env values for every required variable read by
# ``Settings.load``. Chosen to be obviously synthetic so a test failure
# involving one of these values is visibly unrelated to any real
# credential that might leak from a developer's shell.
_VALID_ENV = {
    "TRESTLE_BASE_URL": "https://example.invalid/",
    "TRESTLE_TOKEN_URL": "https://example.invalid/token",
    "TRESTLE_CLIENT_ID": "cid",
    "TRESTLE_CLIENT_SECRET": "csec",
    "MYSQL_HOST": "h",
    "MYSQL_USER": "u",
    "MYSQL_PASSWORD": "p",
    "MYSQL_DATABASE": "d",
}


# Every environment variable the pipeline reads. Kept in sync with
# ``tests/unit/test_config.py`` so both test modules wipe the same
# surface; diverging this set would let shell exports leak into the
# missing-state test only.
_ALL_ENV_VARS = (
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


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch, tmp_path):
    """Make every test see a clean, reproducible config + state surface.

    Three sources of leakage need to be closed so these tests behave
    identically in CI and on a developer's laptop:

    1. Variables exported in the shell. Each name in ``_ALL_ENV_VARS``
       is unset (``raising=False`` so missing names are a no-op).
    2. A local ``.env`` file. ``Settings.load()`` calls
       ``load_dotenv()`` as its first action; stubbing that symbol to
       a no-op prevents any real ``.env`` from silently reintroducing
       values.
    3. Any pre-existing state file. ``STATE_FILE_PATH`` is pointed at
       a path inside pytest's ``tmp_path`` that does NOT yet exist,
       which is exactly the "uninitialized pipeline" precondition
       Req 4.2's missing-watermark test requires.

    The ``autouse=True`` marker applies the fixture to every test in
    the module so individual tests don't have to repeat the setup.
    """
    for name in _ALL_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    for key, value in _VALID_ENV.items():
        monkeypatch.setenv(key, value)
    # Point STATE_FILE_PATH at a path inside tmp_path that does not
    # exist. StateStore.load() treats a missing file as an
    # uninitialized pipeline (Req 9.7), which yields a SyncState whose
    # ``last_modification_timestamp`` is None — the precondition for
    # the Req 4.2 check.
    monkeypatch.setenv("STATE_FILE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setattr("trestle_etl.config.load_dotenv", lambda *a, **k: None)


# ---------------------------------------------------------------------------
# Requirement 11.5: --reconcile placeholder
# ---------------------------------------------------------------------------


def test_reconcile_exits_with_placeholder_exit_code_and_message(capsys):
    """``--reconcile`` SHALL exit non-zero with a placeholder message.

    Validates Requirement 11.5. Pins both the numeric exit code
    (``EXIT_RECONCILE_PLACEHOLDER`` / 3) and a substring of the stderr
    message so the contract is protected against unrelated copy
    changes; the substring check allows either "placeholder" or "not
    implemented" since the design wording uses both interchangeably.
    """
    exit_code = main(["--reconcile"])

    assert exit_code == EXIT_RECONCILE_PLACEHOLDER

    err = capsys.readouterr().err
    lowered = err.lower()
    # The message must make the situation explicit. Accept either
    # phrasing so a minor copy edit doesn't flake this assertion.
    assert "placeholder" in lowered or "not implemented" in lowered, (
        f"stderr did not mention placeholder / not implemented: {err!r}"
    )


# ---------------------------------------------------------------------------
# Requirement 11.6: no mode flag
# ---------------------------------------------------------------------------


def test_no_mode_flag_exits_with_usage_error(capsys):
    """No mode flag SHALL exit non-zero with usage information.

    Validates Requirement 11.6. Argparse-level parsing succeeds because
    every flag is optional, so the check lives in ``_validate_args``
    and the CLI surfaces it as ``EXIT_USAGE_ERROR`` (2). The stderr
    output is required to carry either "usage" (from ``print_usage``)
    or the explicit "no mode flag" phrasing emitted by
    ``_validate_args``; matching either one keeps the contract loose
    enough to survive a copy edit but tight enough to protect the
    operator-visible signal.
    """
    exit_code = main([])

    assert exit_code == EXIT_USAGE_ERROR

    err = capsys.readouterr().err
    lowered = err.lower()
    assert "usage" in lowered or "mode flag" in lowered, (
        f"stderr did not mention usage / mode flag: {err!r}"
    )


# ---------------------------------------------------------------------------
# Requirement 4.2: --incremental with no watermark in state
# ---------------------------------------------------------------------------


def test_incremental_without_state_exits_with_missing_state_and_remediation(
    capsys, tmp_path
):
    """``--incremental`` with uninitialized state SHALL exit non-zero.

    Validates Requirement 4.2. The error message SHALL direct the
    operator toward either ``--full-sync`` or ``--since`` as recovery
    paths, so we require both literal flag tokens to appear in stderr.
    Either alone would leave the operator guessing; the spec says the
    message must point at both.

    The state file is absent by construction: the ``_isolated_env``
    fixture sets ``STATE_FILE_PATH`` to ``tmp_path / "state.json"``
    and does not create the file, so ``StateStore.load()`` returns a
    default-constructed ``SyncState`` with
    ``last_modification_timestamp=None``.
    """
    # Sanity-check the precondition: the state file truly does not
    # exist. If this assertion ever fires, the fixture has drifted
    # and the rest of the test would be meaningless.
    state_path = tmp_path / "state.json"
    assert not state_path.exists(), (
        f"precondition violated: state file {state_path} already exists"
    )

    exit_code = main(["--incremental"])

    assert exit_code == EXIT_MISSING_STATE

    err = capsys.readouterr().err
    # Both remediation pointers must appear literally (the flags are
    # the actionable thing the operator needs to see). Matching on the
    # literal flag token is intentionally strict: a rewording that
    # dropped either flag would silently degrade the remediation
    # message.
    assert "--full-sync" in err, (
        f"stderr missing --full-sync remediation pointer: {err!r}"
    )
    assert "--since" in err, (
        f"stderr missing --since remediation pointer: {err!r}"
    )
