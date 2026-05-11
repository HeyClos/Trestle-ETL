"""HTTP client for the Trestle WebAPI.

The :class:`TrestleClient` centralizes every rule that applies to outbound
requests to the Trestle API: authentication-header injection, 401
re-authentication, the shared 6-retry budget for transient failures, the
``Retry-After`` and exponential-backoff schedules, the quota-header log
entry, and the final :class:`~trestle_etl.errors.TrestleHTTPError` shape.

Keeping all of this logic in one place means the Extractor can remain a
pure generator over ``@odata.nextLink`` URLs and never needs to know about
status codes, sleep schedules, or token lifecycle.

The sleep function is injected so that property-based tests (Properties 1,
2, 3, 29) can drive the retry loop deterministically without a real wall
clock.
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Optional

import requests

from .auth import TokenManager
from .config import Settings
from .errors import AuthError, TrestleHTTPError

logger = logging.getLogger(__name__)

# Exponential backoff schedule used when ``Retry-After`` is absent or the
# response is a 504 / other 5xx. The list is literal (rather than
# ``2 ** k``) so that the code matches the design spec line-for-line and
# so that Property 2's "exactly 2^k seconds" claim can be checked against
# this constant directly.
_BACKOFF_SCHEDULE: tuple[int, ...] = (1, 2, 4, 8, 16, 32)

# Shared retry budget across 429 / 504 / other 5xx (Requirement 2.4, 2.8).
# The one-shot 401 re-authentication retry is explicitly NOT counted.
_MAX_TRANSIENT_RETRIES: int = len(_BACKOFF_SCHEDULE)

# Bound the excerpt of the response body that we attach to errors and log
# lines. 200 characters is enough to identify an HTML error page or a
# structured JSON error payload without ballooning log output when the
# server returns a large stack trace or captive-portal page.
_BODY_EXCERPT_LIMIT: int = 200

# Trestle-specific response headers.
_HOUR_QUOTA_HEADER: str = "Hour-Quota-Available"
_RETRY_AFTER_HEADER: str = "Retry-After"


class TrestleClient:
    """HTTP client for the Trestle WebAPI with auth, retry, and quota handling.

    Args:
        settings: Loaded :class:`Settings`. Currently only used to thread
            configuration into log messages; base URL construction is the
            Extractor's responsibility so the client stays URL-agnostic
            (any absolute URL including ``@odata.nextLink`` can be passed
            to :meth:`get`).
        token_mgr: The :class:`TokenManager` whose cached token is attached
            as the ``Authorization: Bearer`` header on every request. Its
            :meth:`~TokenManager.invalidate` method is called on HTTP 401
            so that the next attempt re-authenticates.
        http: A :class:`requests.Session`. Defaults to a freshly
            constructed session if not provided. Injecting lets the caller
            share a connection pool between the TokenManager and this
            client.
        sleep_func: Function used to implement retry delays. Defaults to
            :func:`time.sleep`. Property-based tests (Properties 1, 2, 3)
            inject a recording fake so they can assert the exact delay
            schedule without a real wall clock.
    """

    def __init__(
        self,
        settings: Settings,
        token_mgr: TokenManager,
        http: Optional[requests.Session] = None,
        sleep_func: Callable[[float], None] = time.sleep,
    ) -> None:
        self._settings = settings
        self._token_mgr = token_mgr
        self._http = http if http is not None else requests.Session()
        self._sleep = sleep_func

    def get(self, url: str, params: Optional[dict] = None) -> dict:
        """GET a JSON resource from Trestle and return the parsed body.

        Applies the full retry / re-auth / quota policy described in
        design.md. The caller receives either a ``dict`` on success or one
        of two exceptions on failure:

        - :class:`AuthError` — token acquisition failed (either on the
          initial request or during post-401 re-authentication); the
          Trestle OIDC endpoint is down, credentials are wrong, or the
          response was malformed. Not retried (Requirement 1.7).
        - :class:`TrestleHTTPError` — either a non-401 4xx response
          (raised immediately) or a 429 / 504 / other 5xx that persisted
          across the full 6-retry budget.

        Args:
            url: Absolute URL to GET. For replication pages this is the
                verbatim ``@odata.nextLink`` returned by the previous
                page; for incremental pages it is constructed by the
                Extractor.
            params: Optional query parameters dict merged into ``url`` by
                requests. Callers following an ``@odata.nextLink`` pass
                ``None`` because the link already carries all required
                query parameters.

        Returns:
            The parsed JSON body of the 200 response.
        """
        # Counts only transient retries (429 / 504 / other 5xx). The 401
        # re-auth path does NOT advance this counter (Requirement 2.8 /
        # Property 3). Starts at 0 so that the first retry uses
        # ``_BACKOFF_SCHEDULE[0] == 1`` second, matching Property 2's
        # 0-indexed ``2^k`` schedule.
        attempt = 0

        # Tracks whether the immediately preceding response was HTTP 401.
        # Requirement 1.6 says we retry the original request "exactly
        # once" after a 401, so a second consecutive 401 (i.e. the
        # re-authenticated retry was also rejected) is a terminal auth
        # failure rather than a signal to loop forever fetching tokens.
        # The flag is cleared as soon as we observe any non-401 response,
        # which preserves Property 3: multiple 401s can appear across a
        # sequence as long as each is separated by a non-401 response.
        previous_was_401 = False

        while True:
            # get_token() raises AuthError on initial-auth failure or
            # post-401 re-auth failure; in both cases we propagate without
            # consuming a transient-retry slot (Requirement 1.7).
            token = self._token_mgr.get_token()
            headers = {"Authorization": f"Bearer {token}"}

            response = self._http.get(url, params=params, headers=headers)
            status = response.status_code

            if status == 200:
                # Log Hour-Quota-Available on every successful response so
                # operators watching the log can see the remaining quota
                # trend without enabling debug-level tracing
                # (Requirement 2.7).
                self._log_quota(response)
                return response.json()

            if status == 401:
                if previous_was_401:
                    # The re-authenticated retry was ALSO rejected with
                    # 401. Per Requirement 1.6 we only retry once after
                    # a 401; this is the terminal state. Surface as
                    # AuthError so the caller distinguishes "credentials
                    # / authorization problem" from "Trestle API is
                    # transiently unhealthy" (TrestleHTTPError).
                    raise AuthError(
                        f"Re-authenticated request still returned HTTP 401 "
                        f"for {url}: {self._body_excerpt(response)}"
                    )
                # Invalidate the cached token so the next loop iteration
                # triggers a fresh token fetch. The 401 retry is separate
                # from the 6-retry budget (Requirement 2.8).
                logger.warning(
                    "Retrying after HTTP 401 (re-authenticating) "
                    "url=%s attempt=%d delay=0",
                    url,
                    attempt,
                )
                self._token_mgr.invalidate()
                previous_was_401 = True
                continue

            # Any non-401 response (success, transient, or hard 4xx)
            # resets the consecutive-401 guard so that a later 401
            # further along the retry loop can again trigger one re-auth.
            previous_was_401 = False

            # Non-401 4xx responses are not retryable: these are client
            # errors (malformed request, forbidden resource, not found)
            # and retrying would not change the outcome. Note that 429 is
            # explicitly excluded from this branch even though it is a
            # 4xx status — the Trestle API documents 429 as a quota signal
            # with a retry protocol (Requirement 2.1, 2.2).
            if 400 <= status < 500 and status != 429:
                raise TrestleHTTPError(
                    status, self._body_excerpt(response), url
                )

            # Transient failure path: 429, 504, or any other 5xx.
            # Exhausted budget → surface the error to the caller
            # (Requirement 2.6).
            if attempt >= _MAX_TRANSIENT_RETRIES:
                raise TrestleHTTPError(
                    status, self._body_excerpt(response), url
                )

            delay = self._compute_transient_delay(status, response, attempt)

            # One WARNING per retry (Requirement 12.5 / Property 29). The
            # attempt number is 0-indexed so it aligns with the ``k`` in
            # Property 2's ``2^k`` schedule; a test asserting "exactly one
            # WARNING per retry" can key on this record.
            logger.warning(
                "Retrying after HTTP %d delay=%s attempt=%d url=%s",
                status,
                delay,
                attempt,
                url,
            )
            self._sleep(delay)
            attempt += 1

    def _compute_transient_delay(
        self,
        status: int,
        response: requests.Response,
        attempt: int,
    ) -> float:
        """Return the sleep duration in seconds for a transient failure.

        Precedence (design.md retry table):

        - HTTP 429 with a parseable ``Retry-After`` header → honor the
          header value verbatim (Requirement 2.1 / Property 1).
        - HTTP 429 without ``Retry-After``, HTTP 504, or other 5xx →
          ``_BACKOFF_SCHEDULE[attempt]`` (Requirement 2.2, 2.3, 2.5 /
          Property 2).

        If ``Retry-After`` is present but not a valid number (e.g. an
        HTTP-date form, which Trestle does not document but which is
        permitted by RFC 7231), fall back to the backoff schedule rather
        than sleeping for an undefined duration.
        """
        if status == 429:
            retry_after = response.headers.get(_RETRY_AFTER_HEADER)
            if retry_after is not None:
                try:
                    return float(retry_after)
                except ValueError:
                    # Unparseable Retry-After; fall through to backoff.
                    pass
        return float(_BACKOFF_SCHEDULE[attempt])

    def _log_quota(self, response: requests.Response) -> None:
        """Emit INFO log for Hour-Quota-Available on a successful response.

        Silent when the header is absent; Trestle may omit it on some
        endpoints or accounts and the log stream should not carry noise
        for those cases.
        """
        quota = response.headers.get(_HOUR_QUOTA_HEADER)
        if quota is not None:
            logger.info("%s=%s", _HOUR_QUOTA_HEADER, quota)

    def _body_excerpt(self, response: requests.Response) -> str:
        """Return the first ``_BODY_EXCERPT_LIMIT`` chars of the response body.

        Used for exception messages and log entries. ``response.text`` can
        raise if the response body stream was already consumed or the
        server returned a body in an unsupported encoding; we guard so
        that a secondary failure in the excerpt path does not mask the
        primary HTTP error.
        """
        try:
            text = response.text
        except Exception:  # pragma: no cover - defensive
            return "<unreadable body>"
        if len(text) > _BODY_EXCERPT_LIMIT:
            return text[:_BODY_EXCERPT_LIMIT] + "..."
        return text


__all__ = ["TrestleClient"]
