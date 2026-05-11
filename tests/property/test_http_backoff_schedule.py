"""Property test for the Trestle_Client exponential backoff schedule (Property 2).

Property 2: For any transient failure response (HTTP 429 without
``Retry-After``, HTTP 504, or any other 5xx) at retry attempt ``k``
(0-indexed), the Trestle_Client SHALL delay by exactly ``2^k`` seconds
before the next attempt, for ``k ∈ {0, 1, 2, 3, 4, 5}``.

**Validates: Requirements 2.2, 2.3, 2.5**
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional

from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.config import Settings
from trestle_etl.http_client import TrestleClient


# Transient-failure statuses that all must fall back to the exponential
# backoff schedule. 429 is included because the FakeResponse produced for
# 429 below carries NO ``Retry-After`` header, which per the design table
# forces the client onto the ``2^k`` schedule.
TRANSIENT_STATUSES = [429, 500, 502, 503, 504]


def _make_settings() -> Settings:
    """Build a Settings instance with dummy values.

    TrestleClient only forwards ``settings`` into log fields; it never
    reads any value from it when computing retry delays, so the specific
    values here do not influence Property 2.
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

    Exposes the exact surface the client touches: ``status_code``,
    ``headers`` (dict-like with ``.get``), ``text`` (for body-excerpt
    formatting in error paths), and ``json()`` on the success response.
    """

    def __init__(
        self,
        status_code: int,
        body: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        self.status_code = status_code
        # Property 2 is specifically about 429-without-Retry-After, 504, and
        # other 5xx. We default to an empty headers dict so that every
        # transient response we synthesize below takes the backoff branch
        # rather than the Retry-After branch (which is Property 1's scope).
        self.headers: dict[str, str] = headers if headers is not None else {}
        self._body = body if body is not None else {}
        # ``text`` is read only by the error-excerpt path and only when the
        # retry budget has already been exhausted; a plain string is
        # sufficient here.
        self.text = ""

    def json(self) -> dict[str, Any]:
        return dict(self._body)


class _FakeSession:
    """Fake ``requests.Session`` that replays a pre-scripted response list.

    Each GET call pops the next response from the queue. The client is
    expected to stop calling once it observes a 200 (success) or exhausts
    its retry budget, so the queue size should match the scripted scenario
    exactly.
    """

    def __init__(self, responses: List[_FakeResponse]) -> None:
        self._responses = list(responses)
        self.call_count = 0

    def get(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> _FakeResponse:
        self.call_count += 1
        # IndexError here would indicate the client made MORE requests than
        # the test anticipated, which is itself a meaningful failure signal
        # — surface it rather than hiding it behind a default response.
        return self._responses.pop(0)


class _FakeTokenManager:
    """Stand-in for TokenManager; always returns a constant token.

    Property 2 does not interact with token lifecycle, so ``invalidate``
    is a no-op and ``get_token`` always returns the same string. The
    Trestle_Client is expected NOT to call ``invalidate`` on this fake
    because no 401 responses appear in any scripted scenario.
    """

    def __init__(self) -> None:
        self.get_token_calls = 0
        self.invalidate_calls = 0

    def get_token(self) -> str:
        self.get_token_calls += 1
        return "fake-token"

    def invalidate(self) -> None:
        self.invalidate_calls += 1


@given(
    statuses=st.lists(
        st.sampled_from(TRANSIENT_STATUSES),
        min_size=0,
        # Upper bound matches the shared retry budget: 6 transient failures
        # followed by a 200 exhausts the schedule exactly and still
        # succeeds (the budget check is ``attempt >= 6`` evaluated BEFORE
        # the status of the current response is inspected for success).
        max_size=6,
    )
)
@settings(
    max_examples=100,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_exponential_backoff_schedule(statuses: List[int]) -> None:
    """Property 2 (Requirements 2.2, 2.3, 2.5).

    For a scenario of ``len(statuses)`` transient failures followed by a
    200, the recorded sleep durations must equal ``[2^0, 2^1, ..., 2^(k-1)]``
    where ``k = len(statuses)``. This covers:

    - 429 without ``Retry-After`` → backoff (Requirement 2.2)
    - 504 → backoff (Requirement 2.3)
    - Other 5xx (500, 502, 503) → backoff (Requirement 2.5)

    by drawing each transient status from a set that spans all three
    categories. The client must not distinguish between these statuses
    when computing the delay; the only relevant input is the retry attempt
    index ``k``.
    """
    # Record every sleep the client requests. A list (not a counter) so the
    # test can assert on ORDER as well as count — Property 2 claims the
    # delays match the schedule at each attempt, not just in aggregate.
    sleeps: List[float] = []

    def record_sleep(duration: float) -> None:
        sleeps.append(duration)

    # 429 responses here deliberately carry no ``Retry-After`` header, so
    # the client takes the backoff branch even for status 429 (per the
    # design retry table and Requirement 2.2). All other transient statuses
    # use the backoff branch unconditionally.
    responses: List[_FakeResponse] = [_FakeResponse(s) for s in statuses]
    responses.append(_FakeResponse(200, body={"ok": True}))

    session = _FakeSession(responses)
    token_mgr = _FakeTokenManager()
    client = TrestleClient(
        _make_settings(),
        token_mgr,  # type: ignore[arg-type]
        http=session,  # type: ignore[arg-type]
        sleep_func=record_sleep,
    )

    result = client.get("https://example.invalid/Property")

    # Success response was returned by the client.
    assert result == {"ok": True}

    # The client must have issued exactly one GET per scripted response:
    # one per transient failure, plus one for the final 200.
    assert session.call_count == len(statuses) + 1

    # Core claim: at retry attempt k (0-indexed), delay is exactly 2^k.
    expected = [float(2 ** k) for k in range(len(statuses))]
    assert sleeps == expected
