"""Property test for shared 6-retry budget with 401 re-auth separation (Property 3).

Property 3: For any interleaving of 429/504/5xx responses, the
Trestle_Client raises ``TrestleHTTPError`` after exactly 6 retries;
inserting any number of HTTP 401 responses into the sequence does NOT
change the retry count before the error is raised (the one-shot 401
re-auth retry is not counted against the 6-retry budget).

**Validates: Requirements 2.4, 2.8**
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional, Tuple

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.config import Settings
from trestle_etl.errors import TrestleHTTPError
from trestle_etl.http_client import TrestleClient


# Transient statuses that count against the shared 6-retry budget. 429 is
# excluded here so every synthesized response takes the backoff branch
# unconditionally and we do not have to thread ``Retry-After`` headers
# through the generator (``Retry-After`` semantics are Property 1's
# scope; Property 3 is about counting, not delay magnitude).
TRANSIENT_STATUSES = [500, 502, 503, 504]

# Shared transient-retry budget per the design retry table. Once this
# many transient responses have been observed (and slept on), the 7th
# transient response exhausts the budget and triggers TrestleHTTPError.
_RETRY_BUDGET = 6


def _make_settings() -> Settings:
    """Build a Settings instance with dummy values.

    TrestleClient forwards ``settings`` only into log fields and never
    reads any value from it when counting retries, so the specific values
    here do not influence Property 3.
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

    Only the attributes/methods the client touches on the transient / 401
    paths are implemented: ``status_code``, ``headers`` (dict-like with
    ``.get``), and ``text`` (read by the body-excerpt helper when the
    budget is exhausted).
    """

    def __init__(
        self,
        status_code: int,
        body: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        self.status_code = status_code
        # Empty headers dict means 429 would take the backoff branch, but
        # we never synthesize a 429 here anyway (see TRANSIENT_STATUSES).
        self.headers: dict[str, str] = headers if headers is not None else {}
        self._body = body if body is not None else {}
        self.text = ""

    def json(self) -> dict[str, Any]:  # pragma: no cover - not hit on failure path
        return dict(self._body)


class _FakeSession:
    """Fake ``requests.Session`` that replays a pre-scripted response list.

    Each ``get`` call pops the next response from the queue. If the client
    issues more requests than scripted, the IndexError surfaces as a test
    failure rather than being hidden behind a default response — this is
    the signal we want when Property 3 is violated (e.g. the client
    miscounts and keeps retrying past the budget).
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
        return self._responses.pop(0)


class _FakeTokenManager:
    """Stand-in for TokenManager.

    Counts ``invalidate`` calls so the test can (optionally) check that
    every 401 triggered a cache invalidation. ``get_token`` always returns
    the same token; Property 3 does not care about token identity, only
    that the 401 path does not consume a transient-retry slot.
    """

    def __init__(self) -> None:
        self.get_token_calls = 0
        self.invalidate_calls = 0

    def get_token(self) -> str:
        self.get_token_calls += 1
        return "fake-token"

    def invalidate(self) -> None:
        self.invalidate_calls += 1


@st.composite
def _interleaved_sequences(
    draw: st.DrawFn,
) -> Tuple[List[int], List[int]]:
    """Generate (response_sequence, transient_statuses_only).

    Shape of the returned sequence:

    - Exactly 7 transient statuses drawn from :data:`TRANSIENT_STATUSES`;
      the 7th response is what exhausts the budget (after 6 retries).
    - Zero or more HTTP 401 responses interleaved anywhere in the
      sequence, each one separated from the next 401 by at least one
      transient response.

    Why the separation guard: :class:`TrestleClient` treats two
    *consecutive* 401s as a terminal auth failure (re-authenticated
    request was still rejected) and raises :class:`AuthError` rather than
    continuing to retry. That behavior is Requirement 1.6's concern, not
    Property 3's. To keep the test focused on the shared retry budget, we
    construct sequences where every 401 is immediately followed by a
    non-401 (a transient status). The simplest realization is: for each
    transient slot, optionally *prepend* a single 401.
    """
    n_401s = draw(st.integers(min_value=0, max_value=5))
    transient_statuses = draw(
        st.lists(
            st.sampled_from(TRANSIENT_STATUSES),
            min_size=7,
            max_size=7,
        )
    )
    # Pick the transient-slot indices (0..6) that each get a 401 prepended.
    # ``unique=True`` guarantees at most one 401 is inserted per slot, which
    # together with "always prepended to a non-401" guarantees no two 401s
    # end up consecutive in the final sequence.
    positions_401 = draw(
        st.lists(
            st.integers(min_value=0, max_value=6),
            min_size=n_401s,
            max_size=n_401s,
            unique=True,
        )
    )
    positions_401_set = set(positions_401)

    sequence: List[int] = []
    for i, status in enumerate(transient_statuses):
        if i in positions_401_set:
            sequence.append(401)
        sequence.append(status)

    return sequence, transient_statuses


@given(seq_and_transient=_interleaved_sequences())
@settings(
    max_examples=100,
    suppress_health_check=[
        HealthCheck.function_scoped_fixture,
        HealthCheck.too_slow,
    ],
)
def test_retry_budget_with_401_interleavings(
    seq_and_transient: Tuple[List[int], List[int]],
) -> None:
    """Property 3 (Requirements 2.4, 2.8).

    Drive :class:`TrestleClient` with a scripted sequence of 7 transient
    failures interleaved with an arbitrary (0-5) number of 401 responses
    and assert:

    1. The call raises :class:`TrestleHTTPError` — never
       :class:`AuthError` — because every 401 is separated from the next
       401 by at least one transient response (see
       :func:`_interleaved_sequences`).
    2. The ``TrestleHTTPError.status`` equals the status of the 7th
       transient response. That is the response which exhausts the
       budget: the client has already consumed 6 sleep/retry slots on
       transients T0..T5, so the check ``attempt >= 6`` fires before T6
       is retried.
    3. Exactly 6 sleeps were recorded. 401s MUST NOT add sleeps (the 401
       branch invalidates and continues without calling ``sleep_func``),
       so the sleep count is independent of how many 401s were injected.
    """
    sequence, transients = seq_and_transient
    assert len(transients) == 7  # generator invariant
    assert sum(1 for s in sequence if s != 401) == 7  # every transient present

    # Record every delay the client requests. Order and count both matter:
    # the count is the Property 3 claim; the ordering (implicitly the
    # 2^k schedule from Property 2) is a useful secondary invariant that
    # falls out of this scripting for free.
    sleeps: List[float] = []

    responses = [_FakeResponse(s) for s in sequence]
    session = _FakeSession(responses)
    token_mgr = _FakeTokenManager()
    client = TrestleClient(
        _make_settings(),
        token_mgr,  # type: ignore[arg-type]
        http=session,  # type: ignore[arg-type]
        sleep_func=lambda s: sleeps.append(s),
    )

    with pytest.raises(TrestleHTTPError) as exc_info:
        client.get("https://example.invalid/Property")

    # Claim 2: the exception carries the 7th transient response's status.
    # ``transients[6]`` is the status that exhausted the budget.
    assert exc_info.value.status == transients[6], (
        f"Expected error status {transients[6]} (the 7th transient), "
        f"got {exc_info.value.status}"
    )

    # Claim 3 (the headline claim): exactly 6 transient-retry sleeps,
    # independent of the number of 401s injected.
    assert len(sleeps) == _RETRY_BUDGET, (
        f"Expected exactly {_RETRY_BUDGET} sleeps, got {len(sleeps)}: "
        f"{sleeps}"
    )

    # Secondary invariant: the client consumed every scripted response —
    # no response was left unread (would indicate an early raise) and no
    # extra request went out (would indicate overrun).
    assert session.call_count == len(sequence), (
        f"Expected {len(sequence)} GETs, got {session.call_count}"
    )

    # Secondary invariant: every 401 in the sequence triggered exactly one
    # token invalidation. This is not part of Property 3's claim but it
    # guards against a regression where the client silently treats a 401
    # as a transient failure (which would also pass the sleep-count
    # assertion, masking the bug).
    expected_invalidations = sum(1 for s in sequence if s == 401)
    assert token_mgr.invalidate_calls == expected_invalidations, (
        f"Expected {expected_invalidations} token invalidations, "
        f"got {token_mgr.invalidate_calls}"
    )
