"""Property test for timestamp UTC round-trip (Property 15).

Property 15 (design.md): For any timezone-aware ``datetime``, serializing
through the outbound OData filter and re-parsing the response timestamp
yields a ``datetime`` representing the same UTC instant.

**Validates: Requirements 4.6, 5.7**

Implementation notes:

* The outbound OData filter format is ISO 8601 with an explicit UTC
  offset. Trestle's ``/Property`` incremental endpoint accepts both
  ``Z`` and ``+00:00`` forms; we exercise the ``Z`` form here because
  that is what the Extractor emits (see design.md, Requirement 4.6).
* The inbound parse goes through :class:`trestle_etl.models.Property`,
  whose ``ModificationTimestamp`` field validator
  (``_normalize_modification_timestamp``) normalizes every parsed
  ``datetime`` to UTC (Requirement 5.7). This is exactly the round-trip
  the property is asserting.
* We use :func:`hypothesis.strategies.timezones` rather than a fixed
  list so the property holds across arbitrary IANA zones, including
  fractional-hour offsets (e.g. ``Asia/Kolkata``) and
  negative-UTC-offset zones. Hypothesis's ``datetimes`` strategy skips
  ambiguous/non-existent local times around DST transitions by default,
  which matches the property's precondition that the input is a
  well-defined instant on the timeline.
* ``ListingKey="K"`` is the minimum non-empty key that satisfies the
  model's ``min_length=1`` constraint; this test is about timestamps,
  so the key is held constant to keep shrinking focused on the
  timestamp draw.
"""

from __future__ import annotations

from datetime import datetime, timezone

from hypothesis import given, settings, strategies as st

from trestle_etl.models import Property


# Hypothesis can only generate tz-aware datetimes when we pass a
# timezones strategy. The bounds cover the realistic range of MLS
# modification timestamps without forcing Hypothesis to draw from the
# full 1-9999 AD span (which would waste examples on dates that the
# Trestle API would never return).
_TZ_AWARE_DATETIMES = st.datetimes(
    min_value=datetime(2000, 1, 1),
    max_value=datetime(2100, 1, 1),
    timezones=st.timezones(),
)


def _format_odata_filter_timestamp(dt: datetime) -> str:
    """Render ``dt`` as an OData ``ModificationTimestamp`` filter value.

    Requirement 4.6 mandates UTC ISO 8601. We convert to UTC first and
    then use the ``Z`` suffix form (as opposed to ``+00:00``) to match
    the Extractor's outbound wire format. Pydantic's ``datetime`` parser
    accepts both forms, so the test is not brittle to this choice; the
    ``Z`` form just mirrors what real Trestle traffic carries.
    """
    utc = dt.astimezone(timezone.utc)
    # ``isoformat()`` on a UTC-aware datetime yields ``...+00:00``.
    # Replacing the offset with ``Z`` produces the canonical OData form.
    return utc.isoformat().replace("+00:00", "Z")


@given(dt=_TZ_AWARE_DATETIMES)
@settings(max_examples=100)
def test_timestamp_utc_roundtrip(dt: datetime) -> None:
    """Property 15 (Requirements 4.6, 5.7).

    Serialize ``dt`` through the OData filter format, feed the
    resulting string back through ``Property.model_validate``, and
    assert the parsed timestamp represents the same UTC instant.

    The comparison uses ``==`` on tz-aware datetimes, which Python
    evaluates by comparing UTC instants — so a value parsed as UTC is
    equal to ``dt.astimezone(timezone.utc)`` iff they point to the
    same moment on the timeline, regardless of how many hours apart
    their wall-clock representations were before the round-trip.
    """
    serialized = _format_odata_filter_timestamp(dt)

    model = Property.model_validate(
        {"ListingKey": "K", "ModificationTimestamp": serialized}
    )

    assert model.ModificationTimestamp is not None
    # The model's field validator forces UTC, so the parsed value is
    # tz-aware and in UTC. Comparing against ``dt.astimezone(UTC)``
    # asserts instant equality.
    assert model.ModificationTimestamp == dt.astimezone(timezone.utc)
    # Belt-and-suspenders: the parsed value must itself be UTC (not
    # just equal to the UTC projection of ``dt``). This catches a
    # regression where the validator normalized to a different zone
    # that still happened to represent the right instant.
    assert model.ModificationTimestamp.tzinfo is not None
    assert model.ModificationTimestamp.utcoffset() == timezone.utc.utcoffset(
        model.ModificationTimestamp
    )
