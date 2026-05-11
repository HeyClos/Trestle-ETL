"""Property test for ``Retry-After`` honored exactly (Property 1).

Property 1: For any 429 response carrying ``Retry-After: n``, the
Trestle_Client waits at least ``n`` seconds before issuing the retry.

**Validates: Requirements 2.1**
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.config import Settings
from trestle_etl.http_client import TrestleClient


def _make_settings() -> Settings:
    """Build a Settings instance with dummy values.

    The TrestleClient only needs settings threaded through for logging;
    it accepts any absolute URL in :meth:`get`, so the base URL is a
    placeholder.
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
    """Minimal stand-in for ``requests.Response`` used by TrestleClient.

    Only the attributes/methods the client actually touches are
    implemented: ``status_code``, ``ok``, ``headers``, ``text``, and
    ``json()``.
    """

    def __init__(
        self,
        status: int,
        body: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        self.status_code = status
        self.ok = 200 <= status < 300
        self.headers = dict(headers) if headers else {}
        self.text = "body"
        self._body: dict[str, Any] = body if body is not None else {"value": []}

    def json(self) -> dict[str, Any]:
        return dict(self._body)


class _FakeSession:
    """Fake ``requests.Session`` that returns a queue of responses.

    The client calls ``session.get(url, params=params, headers=headers)``
    once per attempt; each call pops the next queued response. The list of
    calls is retained so tests can inspect the request sequence.
    """

    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, Optional[dict], Optional[dict]]] = []

    def get(
        self,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
    ) -> _FakeResponse:
        self.calls.append((url, params, headers))
        return self._responses.pop(0)


class _FakeTokenManager:
    """Token manager stub that returns a fixed token.

    ``invalidate()`` is tracked so we can verify the 401 path is not
    triggered by a 429 retry.
    """

    def __init__(self) -> None:
        self.invalidate_count = 0

    def get_token(self) -> str:
        return "TOK"

    def invalidate(self) -> None:
        self.invalidate_count += 1


@given(retry_after=st.integers(min_value=0, max_value=60))
@settings(
    max_examples=100,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_retry_after_honored(retry_after: int) -> None:
    """Property 1 (Requirements 2.1).

    For any ``Retry-After: n`` on a 429 response, the client must sleep at
    least ``n`` seconds before retrying, and the retried request must
    return the final 200 payload. Bounding the generator to ``[0, 60]``
    keeps the test fast while covering the realistic Trestle quota-reset
    range.
    """
    sleeps: list[float] = []

    session = _FakeSession(
        [
            _FakeResponse(429, headers={"Retry-After": str(retry_after)}),
            _FakeResponse(200, body={"ok": True}),
        ]
    )
    token_mgr = _FakeTokenManager()
    client = TrestleClient(
        _make_settings(),
        token_mgr,
        http=session,
        sleep_func=lambda s: sleeps.append(s),
    )

    result = client.get("https://example.invalid/Property")

    # Final 200 payload is returned unchanged.
    assert result == {"ok": True}

    # Exactly one 429 triggered exactly one sleep before the retry.
    assert len(sleeps) == 1, f"Expected one sleep, got {sleeps}"

    # The first (and only) sleep value is exactly the Retry-After value:
    # TrestleClient parses the header verbatim as a float.
    assert sleeps[0] == float(retry_after)

    # "At least n seconds" — Property 1's exact statement.
    assert any(
        s >= retry_after for s in sleeps
    ), f"Expected a sleep >= {retry_after}, got {sleeps}"

    # The retry went out (two session calls) and the 401 re-auth path was
    # never triggered (guards against accidental status-code mishandling).
    assert len(session.calls) == 2
    assert token_mgr.invalidate_count == 0
