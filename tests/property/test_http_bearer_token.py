"""Property test for bearer token on every request (Property 4).

Property 4: For any GET issued by the Trestle_Client, the outgoing request
includes ``Authorization: Bearer <token>`` whose value equals the current
TokenManager-cached token.

**Validates: Requirements 1.8**
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional

from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.config import Settings
from trestle_etl.http_client import TrestleClient


def _make_settings() -> Settings:
    """Build a Settings instance with dummy values.

    The TrestleClient threads ``settings`` into log fields only; none of
    its retry or auth logic reads from it, so the exact values here do not
    influence Property 4.
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
    implemented: ``status_code``, ``headers``, ``text``, and ``json()``.
    """

    def __init__(
        self,
        status_code: int,
        body: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        self.status_code = status_code
        # Empty headers for every response: Property 4 is orthogonal to
        # Retry-After, so we want every transient failure to take the
        # exponential-backoff branch rather than the Retry-After branch.
        self.headers: dict[str, str] = headers if headers is not None else {}
        self._body = body if body is not None else {}
        self.text = ""

    def json(self) -> dict[str, Any]:
        return dict(self._body)


class _FakeSession:
    """Fake ``requests.Session`` that replays a pre-scripted response list.

    Each GET records ``(url, params, headers)`` so the test can inspect
    the full history of outbound request headers. The headers dict is
    copied on capture so that any post-call mutation by the client
    (currently none, but a future refactor could introduce one) cannot
    retroactively alter the recorded snapshot.
    """

    def __init__(self, responses: List[_FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, Optional[dict], Optional[dict]]] = []

    def get(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> _FakeResponse:
        self.calls.append(
            (
                url,
                dict(params) if params else None,
                dict(headers) if headers else None,
            )
        )
        return self._responses.pop(0)


class _RotatingTokenManager:
    """TokenManager fake that hands out a distinct token per call.

    Property 4 claims the Authorization header carries the CURRENT
    TokenManager-cached token. Returning a fresh, distinguishable token
    on every ``get_token()`` invocation lets the test verify that the
    client attached the token that was current at the exact moment of
    each GET — not a stale value from an earlier attempt or a value that
    had not yet been issued at that point in the loop.

    ``invalidate()`` is a no-op here: since every ``get_token()`` already
    returns a new token, the 401 re-auth path naturally rotates to a new
    token on the next iteration without any extra bookkeeping.
    """

    def __init__(self) -> None:
        # Tokens are recorded in the exact order get_token() handed them
        # out, which is the same order in which the client used them on
        # the wire.
        self.tokens: list[str] = []

    def get_token(self) -> str:
        tok = f"TOK{len(self.tokens) + 1}"
        self.tokens.append(tok)
        return tok

    def invalidate(self) -> None:
        # Intentional no-op; rotation happens in get_token().
        return None


@st.composite
def _success_sequences(draw: st.DrawFn) -> list[int]:
    """Generate a status sequence that culminates in a 200.

    Scenario shape: 0-3 transient 5xx failures, optionally one 401 re-auth,
    then a terminating 200. This exercises three distinct paths through
    the client loop — transient retry, 401 re-auth (separate retry
    budget), and final success — all within Property 4's domain: every
    GET, regardless of outcome, must carry the current token.

    The 5xx set spans both the 504 case and the "other 5xx" case called
    out in the design retry table, and the 401 is placed adjacent to the
    200 to avoid the "two consecutive 401s" terminal-auth-failure branch
    (see TrestleClient._previous_was_401 logic / Requirement 1.6).
    """
    n_transient = draw(st.integers(min_value=0, max_value=3))
    include_401 = draw(st.booleans())
    seq = [
        draw(st.sampled_from([500, 502, 503, 504]))
        for _ in range(n_transient)
    ]
    if include_401:
        seq.append(401)
    seq.append(200)
    return seq


@given(status_seq=_success_sequences())
@settings(
    max_examples=100,
    suppress_health_check=[
        HealthCheck.function_scoped_fixture,
        HealthCheck.too_slow,
    ],
)
def test_every_request_carries_bearer_token(status_seq: list[int]) -> None:
    """Property 4 (Requirements 1.8).

    For any GET issued during a ``TrestleClient.get()`` call — the initial
    attempt, any transient retries, and the post-401 re-auth retry — the
    outgoing request's ``Authorization`` header must equal ``Bearer <tok>``
    where ``<tok>`` is the token the TokenManager handed out for that
    specific attempt. The final assertion checks the FULL sequence of
    tokens on the wire against the sequence ``get_token()`` returned,
    which pins down "current" as "most-recently-issued at the moment of
    the call".
    """
    responses = [_FakeResponse(s, body={"ok": True}) for s in status_seq]
    session = _FakeSession(responses)
    token_mgr = _RotatingTokenManager()
    client = TrestleClient(
        _make_settings(),
        token_mgr,  # type: ignore[arg-type]
        http=session,  # type: ignore[arg-type]
        # No-op sleep; Property 4 is orthogonal to delay semantics.
        sleep_func=lambda _: None,
    )

    result = client.get("https://example.invalid/Property")

    # Sanity check: the scripted scenario terminates in 200, and the
    # client returned the parsed body.
    assert result == {"ok": True}

    # One GET per scripted status: the client must not issue extra
    # lookahead requests or swallow attempts.
    assert len(session.calls) == len(status_seq)

    # Per-request: Authorization header is present and well-formed.
    used_tokens: list[str] = []
    for url, _params, headers in session.calls:
        assert headers is not None, (
            f"Client sent no headers with request to {url}"
        )
        auth = headers.get("Authorization")
        assert auth is not None, (
            f"Missing Authorization header on request to {url}"
        )
        assert auth.startswith("Bearer "), (
            f"Authorization header missing 'Bearer ' prefix: {auth!r}"
        )
        tok = auth[len("Bearer ") :]
        assert tok, f"Bearer token value is empty on request to {url}"
        used_tokens.append(tok)

    # Core invariant of Property 4: the header sequence on the wire is
    # exactly the sequence of tokens the TokenManager handed out, in
    # order. This rules out caching a stale token across retries AND
    # rules out any look-ahead where the client might attach a token
    # issued AFTER the request it was attached to.
    assert used_tokens == token_mgr.tokens
