"""Property test for retry WARNING log structure (Property 29).

Property 29: For any retry triggered inside the Trestle_Client, exactly
one WARNING log entry is emitted carrying HTTP status, retry delay
(seconds), and retry attempt number.

**Validates: Requirements 12.5**
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, List, Optional

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.config import Settings
from trestle_etl.http_client import TrestleClient


# Transient-failure statuses that fall through to the shared retry budget
# and thus produce a per-retry WARNING log entry. 429 is included without
# a ``Retry-After`` header so the client still treats it as a transient
# retry the same as 504 / other 5xx; Property 29 doesn't care which delay
# schedule was chosen, only that each retry emits exactly one WARNING.
TRANSIENT_STATUSES = [429, 500, 502, 503, 504]


def _make_settings() -> Settings:
    """Build a Settings instance with dummy values.

    TrestleClient forwards ``settings`` into log fields but never reads
    from it when deciding what to log; the specific values here don't
    affect Property 29.
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

    Only the attributes the client actually touches: ``status_code``,
    ``headers`` (dict with ``.get``), ``text`` (for error-excerpt
    formatting), and ``json()``.
    """

    def __init__(
        self,
        status_code: int,
        body: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        self.status_code = status_code
        # Deliberately empty by default: we want transient 429s to take
        # the backoff branch rather than the Retry-After branch, matching
        # the other retry property tests.
        self.headers: dict[str, str] = headers if headers is not None else {}
        self._body = body if body is not None else {}
        self.text = ""

    def json(self) -> dict[str, Any]:
        return dict(self._body)


class _FakeSession:
    """Fake ``requests.Session`` that replays a pre-scripted response list.

    Each GET pops the next response from the queue. The client should
    exit the retry loop as soon as it observes a 200, so the queue is
    sized to match the scripted scenario exactly.
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
    """Stand-in for TokenManager; always returns a constant token.

    Property 29 doesn't interact with token lifecycle (no 401s in any
    scripted scenario), so ``invalidate`` is a simple counter we can
    assert stays at zero.
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
        # Upper bound matches the shared retry budget: 6 transient
        # failures followed by a 200 still succeeds (attempt >= 6 is
        # checked BEFORE the current status is evaluated for success).
        max_size=6,
    )
)
@settings(
    max_examples=100,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_retry_warning_log(
    statuses: List[int], caplog: pytest.LogCaptureFixture
) -> None:
    """Property 29 (Requirements 12.5).

    For a scenario of ``k = len(statuses)`` transient failures followed
    by a 200, the Trestle_Client must emit exactly ``k`` retry-WARNING
    log entries, and each entry's formatted message must contain:

    - the HTTP status code of the response that triggered the retry
    - the retry delay, formatted as ``delay=...``
    - the retry attempt number, formatted as ``attempt=...``

    We filter on the ``"Retrying after HTTP"`` prefix so any unrelated
    WARNING (for example a future Hour-Quota-Available warning) would
    not contaminate the count.
    """
    # Reset both the caplog buffer and the level binding on every
    # Hypothesis example; without this, records from earlier examples
    # would accumulate and break the per-example count assertion.
    caplog.clear()
    caplog.set_level(logging.WARNING, logger="trestle_etl.http_client")

    responses: List[_FakeResponse] = [
        _FakeResponse(s, body={"ok": True}) for s in statuses
    ]
    responses.append(_FakeResponse(200, body={"ok": True}))

    session = _FakeSession(responses)
    client = TrestleClient(
        _make_settings(),
        _FakeTokenManager(),  # type: ignore[arg-type]
        http=session,  # type: ignore[arg-type]
        # Silence actual sleeping so the test is fast; Property 29 is
        # about log structure, not timing.
        sleep_func=lambda _s: None,
    )

    result = client.get("https://x.invalid/")
    assert result == {"ok": True}

    # The http_client can also emit WARNINGs for 401 re-auth ("Retrying
    # after HTTP 401 ..."), but no 401s appear in this scenario, so
    # filtering on the common "Retrying after HTTP" prefix cleanly
    # captures only the retry warnings we care about.
    retry_warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING
        and "Retrying after HTTP" in r.getMessage()
    ]

    # Exactly one WARNING per transient failure — never zero, never two.
    assert len(retry_warnings) == len(statuses), (
        f"expected {len(statuses)} retry warnings, got {len(retry_warnings)}:"
        f" {[r.getMessage() for r in retry_warnings]}"
    )

    # Each warning records the status that triggered it, the delay (in
    # seconds), and the attempt index.
    for i, (record, status) in enumerate(zip(retry_warnings, statuses)):
        msg = record.getMessage()
        assert str(status) in msg, (
            f"warning {i}: status {status} not in message: {msg!r}"
        )
        assert "delay=" in msg, (
            f"warning {i}: 'delay=' missing from message: {msg!r}"
        )
        assert "attempt=" in msg, (
            f"warning {i}: 'attempt=' missing from message: {msg!r}"
        )
