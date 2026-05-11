"""Property test for ``--since`` override of first request (Property 7).

Property 7 (design.md): For any State_Store value ``s`` and any
CLI-supplied ``--since t``, the first incremental request issued by the
Extractor uses ``t`` (not ``s``) as the ``ModificationTimestamp gt``
filter lower bound.

**Validates: Requirements 4.3**

Implementation notes
--------------------

At the extractor level this property reduces to a very focused claim:
``incremental_stream(client, settings, since=t)`` issues its first GET
with ``$filter=ModificationTimestamp gt <t_iso>``, independent of any
State_Store value. The ``--since`` CLI flag is the orchestrator's
responsibility — the orchestrator decides whether to pass the
State_Store's ``last_modification_timestamp`` or the CLI-supplied
override — but the contract the Extractor owes the rest of the system
is that ``since`` is honored verbatim.

The test therefore:

* Generates two distinct tz-aware UTC datetimes ``s`` (the State_Store
  value that must NOT appear on the wire) and ``t`` (the CLI override).
* Installs a ``FakeTrestleClient`` that captures the first GET's URL and
  params and then returns a terminal page (no ``@odata.nextLink``), so
  the generator finishes after yielding exactly one page.
* Calls ``next(stream)`` to drive the first GET.
* Asserts the filter value equals ``ModificationTimestamp gt <t_iso>``
  where ``t_iso`` is the exact wire form the extractor produces:
  ``t.astimezone(UTC).isoformat().replace("+00:00", "Z")``.
* Asserts the ``s`` value is not present anywhere in the filter
  expression (ruling out a regression where both timestamps somehow
  appear, e.g. through string concatenation).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from hypothesis import assume, given, settings, strategies as st

from trestle_etl.config import Settings
from trestle_etl.extractor import incremental_stream


def _make_settings() -> Settings:
    """Build a ``Settings`` instance with dummy values.

    The extractor only reads ``trestle_base_url`` (for the initial URL)
    and ``default_page_size`` (for ``$top``); neither affects the
    ``$filter`` value, which is what Property 7 constrains. Concrete
    values are supplied purely to satisfy the dataclass.
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


class _FakeTrestleClient:
    """Minimal TrestleClient stand-in that captures the first GET.

    The extractor only calls ``client.get(url, params=...)``. All auth,
    retry, and quota concerns belong to the real client and are
    orthogonal to Property 7 — so replicating that surface here would
    only add noise.

    The fake returns a terminal OData page (no ``@odata.nextLink``) on
    every call. That guarantees the generator yields exactly one page
    and terminates, which keeps the test deterministic regardless of
    how far the consumer iterates.
    """

    def __init__(self) -> None:
        # List of ``(url, params)`` tuples in call order. Property 7 only
        # inspects the first entry, but keeping the full history makes
        # failures easier to diagnose if a regression issues extra GETs
        # before the first yield.
        self.calls: list[tuple[str, Optional[dict[str, Any]]]] = []

    def get(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        # Copy ``params`` on capture so that any post-call mutation by
        # the extractor (currently none, but defensive) cannot
        # retroactively alter the recorded snapshot. ``url`` is a str
        # and is already immutable.
        self.calls.append((url, dict(params) if params is not None else None))
        # Terminal OData page: empty ``value`` list and no
        # ``@odata.nextLink``. The extractor yields ``([], None)`` and
        # exits the loop on the next consumer pull.
        return {"value": []}


# Bounded to 2000-2100 to match the realistic range of MLS modification
# timestamps (see test_timestamp_utc_roundtrip.py for the same bounds).
# Pinning ``timezones=st.just(timezone.utc)`` keeps the draw focused on
# the UTC instant — the timezone branch of the extractor's input
# normalization is covered by Property 15 and is not what Property 7
# constrains.
_UTC_DATETIMES = st.datetimes(
    min_value=datetime(2000, 1, 1),
    max_value=datetime(2100, 1, 1),
    timezones=st.just(timezone.utc),
)


def _to_odata_iso(dt: datetime) -> str:
    """Render ``dt`` in the exact wire form the extractor emits.

    Mirrors ``incremental_stream``'s serialization logic (UTC-normalize,
    ``isoformat()``, replace ``+00:00`` suffix with ``Z``). Duplicating
    the implementation here rather than importing a helper keeps the
    test a true oracle: a regression in the extractor that changes the
    wire form would be caught by this test even if the helper also
    shifted.
    """
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@given(s=_UTC_DATETIMES, t=_UTC_DATETIMES)
@settings(max_examples=100)
def test_since_override_uses_cli_value_not_state(
    s: datetime, t: datetime
) -> None:
    """Property 7 (Requirements 4.3).

    The first GET issued by ``incremental_stream(client, settings,
    since=t)`` must carry ``$filter=ModificationTimestamp gt <t_iso>``,
    not ``<s_iso>`` — independent of ``s`` (the State_Store value the
    CLI override is replacing).

    The ``assume(s != t)`` guard drops draws where the two timestamps
    collide: when they do, there is no observable difference between
    "used ``t``" and "used ``s``" on the wire, so the example carries
    no evidence either way. Hypothesis re-draws these without counting
    them against ``max_examples``.
    """
    assume(s != t)

    client = _FakeTrestleClient()
    settings_obj = _make_settings()

    # Drive the first GET. The extractor is a generator; ``next()``
    # advances it to the first ``yield``, at which point it has already
    # issued exactly one GET (captured by the fake client).
    stream = incremental_stream(client, settings_obj, since=t)  # type: ignore[arg-type]
    records, next_link = next(stream)

    # Sanity: the terminal page fake returns an empty page with no
    # nextLink, so the extractor yielded ``([], None)``. If this
    # assertion fires the fake is misbehaving, not the extractor.
    assert records == []
    assert next_link is None

    # Property 7 only constrains the FIRST request, but the fake records
    # every GET so we can verify no look-ahead request was issued before
    # the first yield.
    assert len(client.calls) == 1, (
        f"Extractor issued {len(client.calls)} GETs before the first yield; "
        f"expected exactly 1"
    )

    url, params = client.calls[0]

    # The initial request must carry query params (the server-supplied
    # ``@odata.nextLink`` path — which uses ``params=None`` — is only
    # taken on subsequent requests).
    assert params is not None, (
        "First incremental GET must carry query parameters; got params=None "
        "(would indicate the extractor treated the initial request as a "
        "nextLink follow-up)"
    )

    filter_val = params.get("$filter")
    assert filter_val is not None, (
        f"First incremental GET missing $filter parameter; params={params!r}"
    )

    # Core Property 7 assertion: the filter uses ``t`` as the lower bound.
    t_iso = _to_odata_iso(t)
    expected_filter = f"ModificationTimestamp gt {t_iso}"
    assert filter_val == expected_filter, (
        f"Expected filter {expected_filter!r}, got {filter_val!r}; "
        f"extractor did not honor the CLI --since override"
    )

    # Negative assertion: the State_Store value ``s`` must not appear in
    # the filter. This rules out a regression where both values are
    # concatenated or where the state value is appended as a secondary
    # constraint. Because ``assume(s != t)`` guarantees the two
    # timestamps represent distinct instants, ``s_iso`` and ``t_iso`` are
    # distinct strings, so a substring check is a sound test.
    s_iso = _to_odata_iso(s)
    assert s_iso not in filter_val, (
        f"State_Store timestamp {s_iso!r} leaked into first-request filter "
        f"{filter_val!r}; extractor must use the CLI --since value verbatim"
    )
