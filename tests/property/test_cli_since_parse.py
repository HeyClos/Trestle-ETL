"""Property test for ``--since`` ISO 8601 parsing (Property 27).

Property 27 (design.md): For any string ``s``: if ``s`` is a valid ISO
8601 UTC timestamp, ``--since s`` SHALL produce a UTC-aware ``datetime``
equal to the same instant; otherwise the CLI SHALL exit non-zero with a
usage error before any HTTP request is issued.

**Validates: Requirements 11.4**

Implementation notes
--------------------

The property is split across three Hypothesis-driven invariants plus
one example-based end-to-end check:

1. **UTC round-trip**. Any ``datetime`` generated in UTC, serialized
   via ``isoformat()`` with the common ``Z`` suffix, must parse back
   through :func:`trestle_etl.cli._parse_iso8601_utc` to the same
   instant. The returned value must be tz-aware (the property is
   explicit about "UTC-aware ``datetime``").
2. **Any-timezone round-trip**. Any ``datetime`` generated in an
   arbitrary IANA timezone, serialized via ``isoformat()`` (which
   emits an explicit offset), must parse to a tz-aware value equal to
   the same UTC instant. This covers operators who supply a local-
   offset timestamp: per the design, ``--since`` is "UTC" in the
   sense of "normalized to UTC before use", not "rejected unless
   already UTC".
3. **Invalid strings raise :class:`UsageError`**. Any string that is
   not a valid ISO 8601 timestamp (as determined by a local oracle
   that mirrors the parser's acceptance rules) must raise
   :class:`~trestle_etl.errors.UsageError`, not any other exception.
4. **End-to-end: the error surfaces before any HTTP request**.
   Invoking ``main(["--since", <bad>])`` must return
   :data:`~trestle_etl.cli.EXIT_USAGE_ERROR` and must never call into
   ``requests.Session.{get,post}``. This is the "before any HTTP
   request is issued" clause of the property — tested by spying on
   both verbs on :class:`requests.Session` and asserting the spy was
   never hit.

The helper :func:`_is_iso8601` is a local oracle that mirrors the
acceptance rules of :func:`_parse_iso8601_utc` exactly (``Z`` suffix
translation plus :func:`datetime.fromisoformat`). Keeping the oracle
local — rather than importing from the module under test — means a
regression that expands or narrows accepted input on the production
side would be caught here: the filter and the parser would disagree
and Hypothesis would find an example that drives the contradiction.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.cli import EXIT_USAGE_ERROR, _parse_iso8601_utc, main
from trestle_etl.errors import UsageError


# ---------------------------------------------------------------------------
# Oracles and helpers
# ---------------------------------------------------------------------------


def _is_iso8601(s: str) -> bool:
    """Return True iff ``s`` would be accepted by ``_parse_iso8601_utc``.

    Mirrors the production parser's acceptance rule exactly: the common
    ``Z`` suffix is translated to ``+00:00`` and the result is handed to
    :func:`datetime.fromisoformat`. Used as a filter for the
    invalid-string strategy so Hypothesis only feeds strings the parser
    is expected to reject.
    """
    try:
        normalized = s.replace("Z", "+00:00") if s.endswith("Z") else s
        datetime.fromisoformat(normalized)
        return True
    except (ValueError, TypeError):
        return False


def _setup_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Populate env vars so ``Settings.load()`` would succeed if reached.

    The end-to-end invalid-``--since`` check needs the *parse* step to
    fail, not configuration loading. Setting every required variable
    isolates the failure to the parser: if parsing were skipped and
    control somehow reached :meth:`Settings.load`, that call would now
    succeed and the test would still detect a missing parse failure
    via the return-code assertion. Also stubs ``load_dotenv`` so the
    developer's local ``.env`` cannot reintroduce values the fixture
    cleared.
    """
    monkeypatch.setenv("TRESTLE_BASE_URL", "https://example.invalid/odata/")
    monkeypatch.setenv(
        "TRESTLE_TOKEN_URL", "https://example.invalid/oidc/token"
    )
    monkeypatch.setenv("TRESTLE_CLIENT_ID", "test-client")
    monkeypatch.setenv("TRESTLE_CLIENT_SECRET", "test-secret")
    monkeypatch.setenv("MYSQL_HOST", "localhost")
    monkeypatch.setenv("MYSQL_USER", "test")
    monkeypatch.setenv("MYSQL_PASSWORD", "test")
    monkeypatch.setenv("MYSQL_DATABASE", "test")
    monkeypatch.setenv(
        "STATE_FILE_PATH", str(tmp_path / "sync_state.json")
    )
    monkeypatch.setattr(
        "trestle_etl.config.load_dotenv", lambda *a, **k: None
    )


# ---------------------------------------------------------------------------
# Property 27 — valid ISO 8601 UTC round-trip
# ---------------------------------------------------------------------------


# Bounded to the same 2000–2100 window as the rest of the property-test
# suite (see ``test_extractor_since_override.py``). MLS data does not
# exist outside this range in practice, and bounding keeps Hypothesis's
# exploration focused on realistic inputs rather than year-1 dates
# whose ISO serializations trigger irrelevant edge cases.
_UTC_DATETIMES = st.datetimes(
    min_value=datetime(2000, 1, 1),
    max_value=datetime(2100, 1, 1),
    timezones=st.just(timezone.utc),
)

_ANY_TZ_DATETIMES = st.datetimes(
    min_value=datetime(2000, 1, 1),
    max_value=datetime(2100, 1, 1),
    timezones=st.timezones(),
)


@given(dt=_UTC_DATETIMES)
@settings(max_examples=100)
def test_valid_iso8601_utc_z_suffix_roundtrips(dt: datetime) -> None:
    """UTC datetime serialized with the ``Z`` suffix round-trips exactly.

    Emits the wire form ``...Z`` that most operators type by hand.
    The parser must accept it, return a tz-aware value, and the
    resulting instant must equal the input.
    """
    # Serialize with the "Z" suffix: the conventional way humans type
    # a UTC timestamp, and the form the extractor emits onto the
    # OData query string.
    s = dt.isoformat().replace("+00:00", "Z")

    parsed = _parse_iso8601_utc(s)

    # "UTC-aware datetime" clause of Property 27.
    assert parsed.tzinfo is not None, (
        f"Parsed datetime must be tz-aware; got naive datetime for input "
        f"{s!r}"
    )
    # "Equal to the same instant" clause of Property 27. Python's
    # datetime equality compares instants across timezones, so this
    # assertion does not require the timezone representations to match
    # — only the underlying UTC instant.
    assert parsed == dt, (
        f"Round-trip changed the instant: input {dt!r} → serialized "
        f"{s!r} → parsed {parsed!r}"
    )


@given(dt=_ANY_TZ_DATETIMES)
@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
def test_valid_iso8601_any_timezone_roundtrips_to_utc(dt: datetime) -> None:
    """Any-timezone datetime normalizes to the same UTC instant.

    Operators might supply ``--since 2024-01-01T12:00:00-05:00``. Per
    the design, ``--since`` is "UTC" in the sense of "normalized to
    UTC", not "rejected unless the offset is zero". The parser must
    accept any valid offset and return the same instant expressed in
    UTC.
    """
    # ``dt.isoformat()`` on a tz-aware datetime emits an explicit
    # offset suffix (``+HH:MM`` or ``-HH:MM``). No "Z" translation
    # needed here: the parser's "Z" branch is covered by the previous
    # test.
    s = dt.isoformat()

    parsed = _parse_iso8601_utc(s)

    assert parsed.tzinfo is not None, (
        f"Parsed datetime must be tz-aware; got naive datetime for input "
        f"{s!r}"
    )
    # Compare instants. ``==`` across tz-aware datetimes compares
    # UTC instants, so this also catches any bug where the parser
    # silently drops the offset and treats the string as UTC.
    assert parsed == dt.astimezone(timezone.utc), (
        f"Any-timezone round-trip shifted the instant: input {dt!r} → "
        f"serialized {s!r} → parsed {parsed!r}, expected "
        f"{dt.astimezone(timezone.utc)!r}"
    )


# ---------------------------------------------------------------------------
# Property 27 — invalid strings raise UsageError
# ---------------------------------------------------------------------------


@given(
    s=st.text(max_size=50).filter(lambda x: not _is_iso8601(x))
)
@settings(
    max_examples=100,
    suppress_health_check=[HealthCheck.filter_too_much],
)
def test_invalid_iso8601_strings_raise_usage_error(s: str) -> None:
    """Non-ISO 8601 strings raise ``UsageError``, not ``ValueError``.

    The parser's contract is that parse failures surface as
    :class:`UsageError`, which the CLI catches and translates into a
    usage error exit. Leaking the underlying :class:`ValueError` from
    :func:`datetime.fromisoformat` would violate Requirement 11.4 by
    coupling operators to an implementation detail.
    """
    with pytest.raises(UsageError):
        _parse_iso8601_utc(s)


# ---------------------------------------------------------------------------
# Property 27 — invalid --since exits non-zero BEFORE any HTTP request
# ---------------------------------------------------------------------------


def test_invalid_since_exits_with_usage_error_before_http(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """End-to-end: ``main(["--since", <bad>])`` returns the usage exit code
    without issuing any HTTP request.

    Wires in a spy on :meth:`requests.Session.get` and
    :meth:`requests.Session.post` so any regression that moved the
    parse step after HTTP initialization would be detected by the
    ``assert not called`` line.

    Env is populated via :func:`_setup_env` so that a hypothetical
    regression that bypassed the parser but somehow still reached
    ``Settings.load()`` would NOT fail for an unrelated "missing env
    var" reason. The only pass-condition is that the parser failed
    early and returned :data:`EXIT_USAGE_ERROR`.
    """
    _setup_env(monkeypatch, tmp_path)

    # Spy on both verbs. A regression that moved the parse step AFTER
    # HTTP initialization could call either (TokenManager calls POST
    # against the OIDC endpoint; every other client call is GET), and
    # covering both catches either drift.
    import requests

    called: list[str] = []

    def spy_get(*_args, **_kwargs):  # pragma: no cover - spy never invoked
        called.append("get")
        raise AssertionError(
            "requests.Session.get was called during an invalid-since run; "
            "Property 27 requires the usage error to surface before any "
            "HTTP request"
        )

    def spy_post(*_args, **_kwargs):  # pragma: no cover - spy never invoked
        called.append("post")
        raise AssertionError(
            "requests.Session.post was called during an invalid-since run; "
            "Property 27 requires the usage error to surface before any "
            "HTTP request"
        )

    monkeypatch.setattr(requests.Session, "get", spy_get)
    monkeypatch.setattr(requests.Session, "post", spy_post)

    code = main(["--since", "not-a-date"])

    assert code == EXIT_USAGE_ERROR, (
        f"Expected main() to return EXIT_USAGE_ERROR ({EXIT_USAGE_ERROR}) "
        f"for an invalid --since value; got {code}"
    )
    assert not called, (
        f"No HTTP request should be issued when --since fails to parse; "
        f"spy captured calls: {called!r}"
    )
