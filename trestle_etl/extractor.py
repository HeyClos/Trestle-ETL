"""Page-by-page extractors over the Trestle WebAPI.

The Extractor exposes lazy generators that yield ``(records, next_link)``
tuples. The orchestrator drives each generator one page at a time, loading
the page and committing state BEFORE pulling the next page. That rhythm is
what keeps the replication ``@odata.nextLink`` inside its 5-minute
inactivity window (Requirements 3.4 / 3.5) and what lets the State_Store
maintain the "never references uncommitted data" invariant (Requirement
15.1).

This module is intentionally thin: all HTTP concerns (auth, retries, quota,
401 re-auth) are the Trestle_Client's responsibility. The Extractor only
knows about URL shape and the ``@odata.nextLink`` traversal protocol.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterator, Optional

from .config import Settings
from .http_client import TrestleClient

# OData v4 response keys. Named as constants so that a hypothetical switch
# to a different envelope (e.g. a mock format used in property tests) only
# touches this file.
_ODATA_VALUE_KEY: str = "value"
_ODATA_NEXT_LINK_KEY: str = "@odata.nextLink"

# Path appended to the configured Trestle base URL for Property queries.
# Trestle-specific; kept as a module constant so ``replication_stream`` and
# (later) ``incremental_stream`` can share the same URL assembly.
_PROPERTY_PATH: str = "/Property"


def replication_stream(
    client: TrestleClient,
    settings: Settings,
    resume_from: Optional[str] = None,
) -> Iterator[tuple[list[dict], Optional[str]]]:
    """Yield replication pages from the Trestle Property endpoint.

    The first request is either:

    - ``resume_from`` verbatim with ``params=None`` — used when the
      orchestrator is resuming a prior full sync from a persisted
      ``@odata.nextLink`` (Requirement 3.8). The saved link already
      carries all required query parameters (``replication``,
      ``$orderby``, ``$top``, and the opaque replication cursor), so
      re-adding them would corrupt the URL.
    - ``GET <base>/Property?replication=true&$orderby=ModificationTimestamp asc&$top=<page_size>``
      — the fresh full-sync entry point (Requirement 3.1). The page size
      is read from ``settings.default_page_size`` so that the operator
      can tune it via the ``PAGE_SIZE`` env var without code changes.

    After each response the generator yields ``(records, next_link)``:

    - ``records`` is the ``value`` list from the OData envelope (empty
      list if the server returned an unexpected shape; the extractor does
      not validate the response — that is the Transformer's job).
    - ``next_link`` is the ``@odata.nextLink`` string or ``None`` if the
      response was terminal.

    Because this is a generator, execution pauses at the ``yield``
    statement. The loop does not advance — and therefore does not GET the
    next link — until the consumer iterates again. That guarantees
    Requirement 3.5: the Extractor never holds more than one
    ``@odata.nextLink`` in scope at a time. When ``next_link`` is ``None``
    the generator falls out of the loop and StopIteration terminates the
    stream (Requirement 3.3).

    Args:
        client: HTTP client used to issue requests. All retry / auth /
            quota handling happens inside the client; this generator only
            cares about URL shape and response envelope.
        settings: Configuration snapshot. Only ``trestle_base_url`` and
            ``default_page_size`` are read; they are used solely for the
            initial URL when ``resume_from`` is not supplied.
        resume_from: Saved ``@odata.nextLink`` from a prior interrupted
            run. When provided, the first request uses this URL verbatim
            with ``params=None``. When ``None``, the generator constructs
            the initial replication URL from ``settings``.

    Yields:
        Tuples of ``(records, next_link)``. ``records`` is a list of raw
        record dicts (may be empty). ``next_link`` is either the next
        ``@odata.nextLink`` URL to be followed on the subsequent
        iteration, or ``None`` when the page was terminal.
    """
    # Decide the first request. Everything past the first iteration uses
    # ``next_link`` verbatim with ``params=None``, because the server-
    # generated link already encodes the query string and any replication
    # cursor state.
    if resume_from is not None:
        url: str = resume_from
        params: Optional[dict] = None
    else:
        url = settings.trestle_base_url.rstrip("/") + _PROPERTY_PATH
        # Query parameters for the initial replication request
        # (Requirement 3.1). ``$top`` is passed as a string because
        # ``requests`` will urlencode it either way and keeping it a
        # string here keeps the type of ``params`` homogeneous.
        params = {
            "replication": "true",
            "$orderby": "ModificationTimestamp asc",
            "$top": str(settings.default_page_size),
        }

    while True:
        page = client.get(url, params=params)

        # OData v4 envelope: ``value`` is a list of entities, and
        # ``@odata.nextLink`` — when present — is the URL for the next
        # page. A missing ``value`` is unexpected but not worth raising
        # from the Extractor; the orchestrator / transformer will observe
        # an empty page and proceed, which keeps the failure mode local
        # to the affected page rather than aborting the entire run.
        records: list[dict] = page.get(_ODATA_VALUE_KEY) or []
        next_link: Optional[str] = page.get(_ODATA_NEXT_LINK_KEY)

        yield records, next_link

        # Generator semantics: control only returns here when the
        # consumer pulls again. If ``next_link`` is None the server
        # declared the stream terminal (Requirement 3.3), so we exit the
        # loop without ever buffering a stale link.
        if next_link is None:
            return

        # Subsequent requests follow the server-supplied link verbatim.
        # Do NOT pass ``params`` here: the link already carries them and
        # merging would either duplicate or drop the replication cursor.
        url = next_link
        params = None


def incremental_stream(
    client: TrestleClient,
    settings: Settings,
    since: datetime,
) -> Iterator[tuple[list[dict], Optional[str]]]:
    """Yield incremental-sync pages from the Trestle Property endpoint.

    Drives ``GET <base>/Property`` with an OData v4 filter that restricts
    the result set to records modified strictly after ``since``:

    ``$filter=ModificationTimestamp gt <since>``
    ``$orderby=ModificationTimestamp asc``
    ``$top=<page_size>``

    The filter uses a **strict** greater-than so that the record at the
    exact ``since`` boundary (which we already loaded on the prior run,
    and whose timestamp is what the orchestrator saves as
    ``last_modification_timestamp``) is not re-processed. Combined with
    the ascending sort on ``ModificationTimestamp``, this gives the
    orchestrator a stable, resumable incremental cursor (Requirement 4.1,
    4.5, 15.4).

    All timestamps are UTC (Requirement 4.6). If ``since`` is
    timezone-aware it is normalized to UTC; if it is naive we assume UTC
    rather than raise, because the State_Store is the primary caller and
    it always stores UTC-aware timestamps. The outbound filter uses
    ISO 8601 with a ``Z`` suffix (rather than ``+00:00``) because that is
    the form Trestle and most OData v4 servers accept without
    complaint.

    The caller should not URL-encode ``since``: ``requests`` will
    percent-encode the ``$filter`` query string when it assembles the
    final URL. Passing a pre-encoded value would result in
    double-encoding.

    Pagination follows the same ``@odata.nextLink`` protocol as
    :func:`replication_stream`: after the initial request every
    subsequent GET uses the server-supplied link verbatim with no
    additional params, and the generator yields one page at a time so
    that the orchestrator's commit-then-advance rhythm is preserved
    (Requirement 4.4). Termination occurs the first time a response
    lacks ``@odata.nextLink``.

    Args:
        client: HTTP client used to issue requests. All retry / auth /
            quota handling happens inside the client.
        settings: Configuration snapshot. ``trestle_base_url`` provides
            the endpoint root and ``default_page_size`` sets ``$top`` for
            the initial request.
        since: Lower bound for the ``ModificationTimestamp`` filter. The
            exact boundary is excluded (strict greater-than). Assumed UTC
            if naive.

    Yields:
        Tuples of ``(records, next_link)`` — identical in shape to
        :func:`replication_stream`. ``records`` is the OData ``value``
        list (possibly empty); ``next_link`` is the next
        ``@odata.nextLink`` URL or ``None`` when the page is terminal.
    """
    # Normalize ``since`` to a UTC-aware datetime. A naive datetime is
    # treated as already-UTC because every upstream producer
    # (State_Store, CLI ``--since`` parser, Pydantic timestamp parser)
    # emits UTC; raising here would force callers to redundantly
    # re-tag the value. ``astimezone(timezone.utc)`` on an already-aware
    # datetime is a no-op for UTC instants and a correct conversion
    # otherwise, so the branch below is sufficient.
    if since.tzinfo is None:
        since_utc = since.replace(tzinfo=timezone.utc)
    else:
        since_utc = since.astimezone(timezone.utc)

    # ISO 8601 with a ``Z`` suffix. ``isoformat`` on a UTC datetime
    # produces ``...+00:00``; swapping that for ``Z`` matches the form
    # the Trestle documentation uses in its filter examples and avoids
    # any ambiguity about whether ``+00:00`` should be URL-encoded as
    # ``%2B00%3A00`` inside the filter expression.
    iso: str = since_utc.isoformat().replace("+00:00", "Z")

    # Initial request URL and query parameters. ``requests`` handles
    # percent-encoding of the space and operator inside the filter, so
    # we pass the raw OData expression as a string.
    url: str = settings.trestle_base_url.rstrip("/") + _PROPERTY_PATH
    params: Optional[dict] = {
        "$filter": f"ModificationTimestamp gt {iso}",
        "$orderby": "ModificationTimestamp asc",
        "$top": str(settings.default_page_size),
    }

    while True:
        page = client.get(url, params=params)

        # Same envelope semantics as replication_stream: ``value`` is
        # the page records and ``@odata.nextLink`` — when present — is
        # the URL for the next page. An absent or non-list ``value``
        # degrades to an empty page rather than raising; the orchestrator
        # handles empty pages gracefully and validation is the
        # Transformer's responsibility.
        records: list[dict] = page.get(_ODATA_VALUE_KEY) or []
        next_link: Optional[str] = page.get(_ODATA_NEXT_LINK_KEY)

        yield records, next_link

        # Generator resumes here only when the consumer pulls again.
        # A terminal response (no nextLink) ends the stream without the
        # extractor ever holding a stale link in scope (mirrors
        # Requirement 3.5 for the incremental path).
        if next_link is None:
            return

        # Subsequent pages use the server-supplied link verbatim. The
        # link already carries the filter, orderby, top, and any opaque
        # pagination cursor; re-supplying ``params`` would either
        # duplicate keys or silently drop the cursor.
        url = next_link
        params = None


__all__ = ["replication_stream", "incremental_stream"]
