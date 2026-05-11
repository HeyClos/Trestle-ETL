"""OAuth2 token acquisition and caching for the Trestle WebAPI.

Implements Requirement 1: the TokenManager fetches an access token from the
Trestle OIDC token endpoint using ``grant_type=client_credentials`` and
``scope=api``, caches it against a monotonic clock, and returns the cached
token while it still has more than 60 seconds of remaining validity. The
Trestle_Client owns the 401-retry logic in Requirement 1.6/1.7; this module
only provides :meth:`TokenManager.invalidate` so that the client can drop
the cached token before retrying.

The monotonic clock is injected to keep the cache-window property (Property
5) testable without ``freezegun`` or monkeypatching ``time``.
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Optional

import requests

from .config import Settings
from .errors import AuthError, ConfigError

logger = logging.getLogger(__name__)

# Per Requirement 1.5: reuse the cached token only while it has strictly
# more than 60 seconds of remaining validity. The margin absorbs clock skew
# between this process and the Trestle OIDC endpoint and avoids handing out
# a token that could expire mid-request.
_REFRESH_SAFETY_MARGIN_SECONDS: float = 60.0


class TokenManager:
    """Fetches and caches an OAuth2 bearer token for the Trestle_API.

    Thread-safety: this class is intentionally NOT thread-safe. The pipeline
    is single-threaded by design (see design.md "Process Model"), and adding
    a lock would obscure the cache-window semantics that Property 5 tests.

    Args:
        settings: Loaded :class:`Settings`. ``client_id`` and
            ``client_secret`` must be non-empty; otherwise construction
            raises :class:`ConfigError` (Requirement 1.3). The token URL is
            read from ``settings.trestle_token_url``.
        http: A :class:`requests.Session` used to POST the token request.
            Injected rather than constructed internally so that
            :class:`~trestle_etl.http_client.TrestleClient` can share a
            single session (and its connection pool) across token refreshes
            and API calls.
        time_func: Monotonic clock used to measure token lifetime. Defaults
            to :func:`time.monotonic`. Injected for testability so
            Property 5 can drive the cache-window boundary without patching
            module-level ``time``.
    """

    def __init__(
        self,
        settings: Settings,
        http: requests.Session,
        time_func: Callable[[], float] = time.monotonic,
    ) -> None:
        # Requirement 1.3: fail fast during construction so that a
        # misconfigured deployment never issues an outbound HTTP request.
        # config.Settings.load() already enforces this, but we re-check
        # defensively because Settings can be constructed directly (for
        # example in tests or future programmatic entry points).
        if not settings.client_id:
            raise ConfigError(
                "Missing required configuration: client_id is empty"
            )
        if not settings.client_secret:
            raise ConfigError(
                "Missing required configuration: client_secret is empty"
            )

        self._settings = settings
        self._http = http
        self._now = time_func

        # Cache state. ``_cached_token`` and ``_cached_deadline`` are always
        # set together: either both are None (no cached token) or both are
        # populated. The deadline is a monotonic timestamp already adjusted
        # for the 60-second safety margin, so the reuse check reduces to
        # ``self._now() < self._cached_deadline``.
        self._cached_token: Optional[str] = None
        self._cached_deadline: Optional[float] = None

    def get_token(self) -> str:
        """Return a valid bearer token, fetching a new one if needed.

        Reuses the cached token iff it has strictly more than 60 seconds of
        remaining validity (Requirement 1.5). Otherwise, posts to the
        Trestle OIDC token endpoint to obtain a fresh token and updates the
        cache.

        Raises:
            AuthError: If the HTTP request to the token endpoint fails, the
                response is not 2xx, or the response body lacks the
                expected ``access_token`` / ``expires_in`` fields.
        """
        if self._cached_token is not None and self._cached_deadline is not None:
            if self._now() < self._cached_deadline:
                return self._cached_token

        return self._fetch_new_token()

    def invalidate(self) -> None:
        """Drop the cached token.

        Called by :class:`~trestle_etl.http_client.TrestleClient` after an
        HTTP 401 response so that the next :meth:`get_token` call goes to
        the token endpoint (Requirement 1.6).
        """
        self._cached_token = None
        self._cached_deadline = None

    def _fetch_new_token(self) -> str:
        """Post to the token endpoint and update the cache.

        The request body uses ``application/x-www-form-urlencoded`` with
        ``grant_type=client_credentials`` and ``scope=api`` per
        Requirement 1.1. Credentials are sent in the form body rather than
        HTTP Basic so that the Trestle-supported flow is explicit and
        visible in request logs.
        """
        logger.debug(
            "Requesting new OAuth2 token from %s",
            self._settings.trestle_token_url,
        )

        # Snapshot the clock BEFORE the network call. Using the pre-request
        # timestamp for the deadline means any network latency shortens the
        # effective cache window, which is the safe direction: we may
        # refresh slightly earlier than necessary, but we will never hand
        # out a token that outlives the server's ``expires_in`` window.
        issued_at = self._now()

        try:
            response = self._http.post(
                self._settings.trestle_token_url,
                data={
                    "grant_type": "client_credentials",
                    "scope": "api",
                    "client_id": self._settings.client_id,
                    "client_secret": self._settings.client_secret,
                },
            )
        except requests.RequestException as exc:
            raise AuthError(
                f"Failed to reach token endpoint "
                f"{self._settings.trestle_token_url}: {exc}"
            ) from exc

        if not response.ok:
            raise AuthError(
                f"Token endpoint returned HTTP {response.status_code}: "
                f"{_excerpt(response.text)}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise AuthError(
                f"Token endpoint returned non-JSON body: "
                f"{_excerpt(response.text)}"
            ) from exc

        access_token = payload.get("access_token")
        expires_in = payload.get("expires_in")
        if not isinstance(access_token, str) or not access_token:
            raise AuthError(
                "Token endpoint response is missing a non-empty "
                "'access_token' field"
            )
        if not isinstance(expires_in, (int, float)) or expires_in <= 0:
            raise AuthError(
                "Token endpoint response is missing a positive numeric "
                "'expires_in' field"
            )

        # Pre-compute the monotonic deadline with the safety margin already
        # applied so that :meth:`get_token` reduces to a single comparison.
        self._cached_token = access_token
        self._cached_deadline = (
            issued_at + float(expires_in) - _REFRESH_SAFETY_MARGIN_SECONDS
        )
        return access_token


def _excerpt(text: str, limit: int = 200) -> str:
    """Return a short excerpt of a response body for error messages."""
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


__all__ = ["TokenManager"]
