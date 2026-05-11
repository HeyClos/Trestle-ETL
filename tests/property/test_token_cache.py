"""Property test for TokenManager cache reuse window (Property 5).

Property 5: For any ``(issued_at, expires_in, current_time)`` clock state,
``TokenManager.get_token()`` returns the cached token without contacting the
token endpoint iff ``(issued_at + expires_in) - current_time > 60 s``;
otherwise it fetches a fresh token.

**Validates: Requirements 1.4, 1.5**
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional

from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.auth import TokenManager
from trestle_etl.config import Settings


def _make_settings() -> Settings:
    """Build a Settings instance with dummy credentials.

    The TokenManager only reads ``client_id``, ``client_secret``, and
    ``trestle_token_url``; everything else is irrelevant to Property 5 but
    is required by the frozen dataclass constructor.
    """
    return Settings(
        trestle_base_url="https://example.invalid/trestle/odata/",
        trestle_token_url="https://example.invalid/oidc/token",
        client_id="test-client-id",
        client_secret="test-client-secret",
        mysql_host="localhost",
        mysql_port=3306,
        mysql_user="user",
        mysql_password="password",
        mysql_database="trestle",
        state_file_path=Path("sync_state.json"),
        default_page_size=1000,
    )


class _FakeResponse:
    """Minimal stand-in for requests.Response used by TokenManager.

    Only the attributes/methods the TokenManager actually touches are
    implemented: ``ok``, ``status_code``, ``text``, and ``json()``.
    """

    def __init__(self, expires_in: int, token: str = "tok") -> None:
        self.ok = True
        self.status_code = 200
        self.text = ""
        self._payload = {"access_token": token, "expires_in": expires_in}

    def json(self) -> dict[str, Any]:
        return dict(self._payload)


class _FakeSession:
    """Fake requests.Session that counts POSTs and returns a fixed token.

    Counting POSTs is what lets the test distinguish "cache hit" from
    "cache miss + refetch" without inspecting TokenManager internals.
    """

    def __init__(self, expires_in: int) -> None:
        self._expires_in = expires_in
        self.call_count = 0
        self.last_url: Optional[str] = None
        self.last_data: Optional[dict[str, Any]] = None

    def post(
        self,
        url: str,
        data: Optional[dict[str, Any]] = None,
        **_: Any,
    ) -> _FakeResponse:
        self.call_count += 1
        self.last_url = url
        self.last_data = data
        return _FakeResponse(self._expires_in)


@given(
    expires_in=st.integers(min_value=61, max_value=86400),
    current_offset=st.integers(min_value=0, max_value=100_000),
)
@settings(
    max_examples=100,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_cache_reuse_window(expires_in: int, current_offset: int) -> None:
    """Property 5 (Requirements 1.4, 1.5).

    Drive a TokenManager with a controllable monotonic clock. After the
    initial fetch (which always contacts the endpoint because the cache is
    empty), advance the clock to ``current_offset`` and call ``get_token``
    again. The cached token must be reused iff ``expires_in -
    current_offset > 60``; otherwise a second POST must be issued.
    """
    # Controllable clock: each call reads the current value without
    # advancing it, so issued_at is deterministically 0.0 on the first
    # fetch and the cache deadline becomes ``expires_in - 60``.
    clock: List[float] = [0.0]

    def now() -> float:
        return clock[0]

    session = _FakeSession(expires_in=expires_in)
    tm = TokenManager(_make_settings(), session, time_func=now)

    # First call: cache is empty, so a POST must happen regardless of
    # ``current_offset``.
    clock[0] = 0.0
    first_token = tm.get_token()
    assert first_token == "tok"
    assert session.call_count == 1

    # Advance the clock to the test offset and re-request.
    clock[0] = float(current_offset)
    second_token = tm.get_token()
    assert second_token == "tok"

    remaining = expires_in - current_offset
    if remaining > 60:
        # Strictly more than 60 s of validity: cache hit, no new POST.
        assert session.call_count == 1
    else:
        # At or below the 60 s safety margin: a fresh token was fetched.
        assert session.call_count == 2
