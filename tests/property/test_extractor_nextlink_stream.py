"""Property test for streaming nextLink traversal (Property 6).

Property 6: For any finite chain of replication or incremental pages, the
Extractor issues a GET for every ``@odata.nextLink`` it observes,
terminates immediately when a page has no ``@odata.nextLink``, and yields
each page to the consumer before issuing the GET for the next page (no
buffering of multiple ``@odata.nextLink`` URLs).

**Validates: Requirements 3.2, 3.3, 3.4, 3.5, 4.4**
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.config import Settings
from trestle_etl.extractor import incremental_stream, replication_stream


def _make_settings() -> Settings:
    """Build a Settings instance with dummy values.

    The Extractor reads only ``trestle_base_url`` and ``default_page_size``
    from settings, and only for the INITIAL request URL. Every subsequent
    page uses the server-supplied ``@odata.nextLink`` verbatim, so the
    specific base URL does not affect Property 6's claims about call
    counts, call order, or termination.
    """
    return Settings(
        trestle_base_url="https://example.invalid/trestle/odata",
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


class _FakeTrestleClient:
    """Pre-scripted TrestleClient stand-in for Property 6.

    Records every ``get(url, params)`` call and returns pages from a
    queue in FIFO order. The Extractor only invokes ``.get`` on this
    fake, so no other method of the real TrestleClient needs to be
    reproduced.
    """

    def __init__(self, pages: list[dict[str, Any]]) -> None:
        # ``list(pages)`` to defensively copy: the Extractor should never
        # mutate the input, but sharing state between fakes across
        # generator invocations would mask bugs if it did.
        self._pages_queue: list[dict[str, Any]] = list(pages)
        # ``calls`` is a list of ``(url, params)`` tuples so tests can
        # assert ordering as well as count. A counter alone would hide
        # regressions where the Extractor re-fetched the initial URL
        # instead of following the server-supplied nextLink.
        self.calls: list[tuple[str, Optional[dict[str, Any]]]] = []

    def get(
        self, url: str, params: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        self.calls.append((url, params))
        # Pop rather than index so an unexpected extra GET raises
        # IndexError instead of silently re-returning the last page.
        # That failure mode is exactly what Property 6 forbids (no
        # requests after a terminal page), so letting it surface as an
        # IndexError here is the right signal.
        return self._pages_queue.pop(0)


def _build_pages(n_pages: int) -> list[dict[str, Any]]:
    """Construct ``n_pages`` OData response envelopes.

    Page ``i`` (0-indexed) has one record ``{"ListingKey": f"LK{i}"}``
    and, when not the terminal page, an ``@odata.nextLink`` pointing at
    the URL ``f"link{i+1}"`` that is meant to be used for page ``i+1``.
    The terminal page omits ``@odata.nextLink`` entirely, which is what
    signals the Extractor to stop (Requirement 3.3).
    """
    pages: list[dict[str, Any]] = []
    for i in range(n_pages):
        page: dict[str, Any] = {"value": [{"ListingKey": f"LK{i}"}]}
        if i < n_pages - 1:
            # The link returned in page ``i`` will be used by the
            # Extractor to fetch page ``i+1``. Naming it ``link{i+1}``
            # makes the assertion "page i+1 was fetched via ``link{i+1}``"
            # read naturally.
            page["@odata.nextLink"] = f"link{i + 1}"
        pages.append(page)
    return pages


def _expected_url_sequence(n_pages: int, initial_url: str) -> list[str]:
    """Return the URL sequence the Extractor is expected to GET.

    Page 0 uses ``initial_url`` (the URL the Extractor constructs from
    settings); pages 1..n-1 use ``link1``, ``link2``, ..., ``link{n-1}``
    (the server-supplied ``@odata.nextLink`` values from the previous
    page). This is the verbatim list of URLs the fake's ``calls`` must
    match.
    """
    urls: list[str] = [initial_url]
    for i in range(1, n_pages):
        urls.append(f"link{i}")
    return urls


def _assert_stream_traversal(
    stream: Any,
    client: _FakeTrestleClient,
    n_pages: int,
    expected_urls: list[str],
) -> None:
    """Shared Property 6 assertions for any page-yielding generator.

    Iterates ``stream`` one page at a time, checking after each pull:

    - Exactly one new GET was issued for this page (no lookahead).
    - The GET targeted the URL the previous page's ``@odata.nextLink``
      declared (or the initial URL for page 0).
    - The yielded records match the page that was just GETed.
    - ``next_link`` matches what the fake's page declared: ``link{i+1}``
      for all non-terminal pages, ``None`` for the terminal page.

    After the terminal page, a further ``next(stream)`` must raise
    ``StopIteration`` and MUST NOT issue any additional GET (Property 6
    termination clause and Requirement 3.3).
    """
    for i in range(n_pages):
        # Before pulling page ``i``, the fake has seen exactly ``i``
        # calls. If the Extractor had buffered ahead (violating the
        # no-buffering clause of Property 6 / Requirement 3.5), this
        # count would already be ``i+1`` or higher.
        assert len(client.calls) == i, (
            f"Before pulling page {i}, expected {i} prior GETs "
            f"but saw {len(client.calls)}: {client.calls}"
        )

        records, next_link = next(stream)

        # After pulling page ``i``, exactly one GET has fired for this
        # page — no more, no fewer. Combined with the pre-pull check
        # above, this is the per-page "one GET per yield" invariant.
        assert len(client.calls) == i + 1, (
            f"After pulling page {i}, expected {i + 1} total GETs "
            f"but saw {len(client.calls)}: {client.calls}"
        )

        # The URL the Extractor hit for page ``i`` must match what the
        # previous page's nextLink advertised (or the initial URL for
        # page 0). Requirement 3.2: every @odata.nextLink is followed.
        assert client.calls[i][0] == expected_urls[i], (
            f"Page {i} GET targeted {client.calls[i][0]!r}, "
            f"expected {expected_urls[i]!r}"
        )

        # Page ``i`` was built with a single record ``LK{i}``; verify the
        # envelope decoding round-tripped the value list correctly.
        assert records == [{"ListingKey": f"LK{i}"}]

        # next_link value contract: the link pointing at page ``i+1``
        # for non-terminal pages, None on the terminal page.
        if i < n_pages - 1:
            assert next_link == f"link{i + 1}"
        else:
            assert next_link is None

    # Terminal-page semantics (Requirement 3.3): the generator stops
    # iterating as soon as a page had no ``@odata.nextLink``. Pulling
    # again raises StopIteration, and no extra GET fires — which is what
    # ``pop(0)`` on an exhausted queue would otherwise expose via
    # IndexError.
    with pytest.raises(StopIteration):
        next(stream)

    assert len(client.calls) == n_pages, (
        f"After termination, expected {n_pages} total GETs, "
        f"saw {len(client.calls)}: {client.calls}"
    )


@given(n_pages=st.integers(min_value=1, max_value=10))
@settings(
    max_examples=100,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_replication_stream_nextlink_traversal(n_pages: int) -> None:
    """Property 6 on :func:`replication_stream` (Requirements 3.2-3.5).

    For a pre-scripted chain of ``n_pages`` replication pages, the
    Extractor must:

    - Issue exactly ``n_pages`` GETs total (one per page, no buffering).
    - Visit the initial replication URL for page 0, and each subsequent
      server-supplied ``@odata.nextLink`` for page 1..n-1.
    - Yield each page BEFORE issuing the GET for the following page.
    - Terminate (StopIteration) the first time a page has no
      ``@odata.nextLink`` — and issue no additional GET after that.
    """
    settings_obj = _make_settings()
    pages = _build_pages(n_pages)
    client = _FakeTrestleClient(pages)

    # Initial URL for a fresh full sync. Matches the construction in
    # replication_stream: base URL + /Property with the replication
    # query-string params.
    initial_url = settings_obj.trestle_base_url.rstrip("/") + "/Property"
    expected_urls = _expected_url_sequence(n_pages, initial_url)

    stream = replication_stream(client, settings_obj)
    _assert_stream_traversal(stream, client, n_pages, expected_urls)


@given(n_pages=st.integers(min_value=1, max_value=10))
@settings(
    max_examples=100,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_incremental_stream_nextlink_traversal(n_pages: int) -> None:
    """Property 6 on :func:`incremental_stream` (Requirement 4.4).

    Same traversal / no-buffering / termination invariants as the
    replication case, but driven through ``incremental_stream`` with a
    fixed ``since`` boundary. The initial URL is the ``/Property``
    endpoint (the filter is passed through ``params``, not the URL path),
    and every subsequent request follows the server-supplied nextLink
    verbatim.
    """
    settings_obj = _make_settings()
    pages = _build_pages(n_pages)
    client = _FakeTrestleClient(pages)

    # Fixed UTC boundary. Requirement 4.6 requires UTC-aware timestamps;
    # the actual instant does not influence Property 6 since the
    # Extractor stamps the filter only into the initial request's params.
    since = datetime(2024, 1, 1, tzinfo=timezone.utc)

    initial_url = settings_obj.trestle_base_url.rstrip("/") + "/Property"
    expected_urls = _expected_url_sequence(n_pages, initial_url)

    stream = incremental_stream(client, settings_obj, since=since)
    _assert_stream_traversal(stream, client, n_pages, expected_urls)
