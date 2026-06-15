"""Property-based test for Pydantic model round-trip.

Property 13 (design.md): For any valid raw record,
``parse(serialize(parse(record)))`` produces a model instance equal
(under Pydantic equality) to ``parse(record)``.

**Validates: Requirements 5.8, 14.2, 14.3**

The round-trip exercises the full parse/serialize contract that the rest
of the pipeline relies on:

1. ``Property.model_validate`` coerces ISO 8601 strings into
   tz-aware datetimes / dates, numeric strings into ``Decimal``, etc.
2. ``Property.model_dump_json`` emits JSON using Pydantic's canonical
   serialization (datetimes as ISO 8601, ``Decimal`` as JSON strings).
3. ``Property.model_validate_json`` re-parses that JSON back into a
   model instance.

If every typed field round-trips losslessly, ``m1 == m2`` holds and
Pydantic equality (which compares field values) passes.

Implementation notes:

* The strategy builds raw records by sampling each Promoted_Column
  independently with a "maybe present" flag. This covers both the
  all-fields case and the sparse/empty case that :func:`validate`
  must also handle (Requirement 5.2).
* ``Decimal`` strategies constrain to finite, non-NaN values with fixed
  ``places`` so generated values survive JSON string serialization
  byte-for-byte. Values are emitted as strings (``.map(str)``) because
  Trestle serves numeric fields as JSON strings in practice; Pydantic
  coerces them on the way in.
* Datetimes are always UTC-aware. The model's post-validator normalizes
  any aware datetime to UTC, so generating naive datetimes would cause
  spurious round-trip mismatches (naive input -> UTC-aware output).
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.models import Property


# Bounds used across every decimal strategy. ``allow_nan`` and
# ``allow_infinity`` are disabled because both are non-finite Decimals
# that neither the MySQL schema (DECIMAL columns reject them) nor JSON
# (NaN/Infinity are not valid JSON numbers) can represent.
_MONEY_MIN = Decimal("0")
_MONEY_MAX = Decimal("99999999.99")
_LAT_MIN = Decimal("-90")
_LAT_MAX = Decimal("90")
_LON_MIN = Decimal("-180")
_LON_MAX = Decimal("180")

# Bounded date range keeps ``date.isoformat()`` within the ``YYYY-MM-DD``
# format Pydantic accepts and avoids the year-0/year-9999 edges that
# Hypothesis is fond of shrinking to.
_MIN_DATE = date(2000, 1, 1)
_MAX_DATE = date(2100, 1, 1)
_MIN_DT = datetime(2000, 1, 1, tzinfo=timezone.utc)
_MAX_DT = datetime(2100, 1, 1, tzinfo=timezone.utc)


def _money_strategy() -> st.SearchStrategy[str]:
    """Positive DECIMAL(14,2) values serialized as strings."""
    return st.decimals(
        min_value=_MONEY_MIN,
        max_value=_MONEY_MAX,
        places=2,
        allow_nan=False,
        allow_infinity=False,
    ).map(str)


def _latitude_strategy() -> st.SearchStrategy[str]:
    return st.decimals(
        min_value=_LAT_MIN,
        max_value=_LAT_MAX,
        places=7,
        allow_nan=False,
        allow_infinity=False,
    ).map(str)


def _longitude_strategy() -> st.SearchStrategy[str]:
    return st.decimals(
        min_value=_LON_MIN,
        max_value=_LON_MAX,
        places=7,
        allow_nan=False,
        allow_infinity=False,
    ).map(str)


def _area_strategy() -> st.SearchStrategy[str]:
    """DECIMAL(10,2)/DECIMAL(12,2) area values as strings."""
    return st.decimals(
        min_value=Decimal("0"),
        max_value=Decimal("9999999.99"),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    ).map(str)


def _datetime_strategy() -> st.SearchStrategy[str]:
    """UTC-aware datetimes serialized as ISO 8601 strings."""
    return st.datetimes(
        min_value=_MIN_DT.replace(tzinfo=None),
        max_value=_MAX_DT.replace(tzinfo=None),
        timezones=st.just(timezone.utc),
    ).map(lambda d: d.isoformat())


def _date_strategy() -> st.SearchStrategy[str]:
    return st.dates(min_value=_MIN_DATE, max_value=_MAX_DATE).map(
        lambda d: d.isoformat()
    )


@st.composite
def valid_records(draw: st.DrawFn) -> dict[str, Any]:
    """Generate a raw Trestle record with arbitrary sparse field coverage.

    ``ListingKey`` is always populated (non-empty, within the 128-char
    cap) so the record is valid per Requirement 5.6. Every other
    Promoted_Column is optionally present; when present, its value comes
    from a field-appropriate strategy that stays inside the MySQL column
    bounds declared in ``schema.sql``.
    """
    record: dict[str, Any] = {
        "ListingKey": draw(
            st.text(min_size=1, max_size=128).filter(lambda s: s.strip() != "")
        )
    }

    def maybe(field: str, strategy: st.SearchStrategy[Any]) -> None:
        """Include ``field`` with a drawn value with probability ~0.5."""
        if draw(st.booleans()):
            record[field] = draw(strategy)

    # Identifier and status --------------------------------------------
    maybe("ListingId", st.text(max_size=128))
    maybe("MlsStatus", st.text(max_size=30))

    # Internet display flags -------------------------------------------
    maybe("InternetEntireListingDisplayYN", st.booleans())
    maybe("InternetAddressDisplayYN", st.booleans())
    maybe("InternetAutomatedValuationDisplayYN", st.booleans())
    maybe("InternetConsumerCommentYN", st.booleans())

    # Geospatial -------------------------------------------------------
    maybe("Latitude", _latitude_strategy())
    maybe("Longitude", _longitude_strategy())

    # Address ----------------------------------------------------------
    maybe("ParcelNumber", st.text(max_size=64))
    maybe("StreetNumberNumeric", st.integers(min_value=0, max_value=999999))
    maybe("StreetDirPrefix", st.text(max_size=16))
    maybe("StreetName", st.text(max_size=128))
    maybe("StreetSuffix", st.text(max_size=32))
    maybe("UnitNumber", st.text(max_size=32))
    maybe("City", st.text(max_size=64))
    # Two-letter state code; any printable text of the right length is
    # accepted by the Pydantic model (no pattern constraint).
    maybe("StateOrProvince", st.text(min_size=2, max_size=2))
    maybe("PostalCode", st.text(max_size=16))

    # Monetary values --------------------------------------------------
    maybe("OriginalListPrice", _money_strategy())
    maybe("ListPrice", _money_strategy())
    maybe("ClosePrice", _money_strategy())

    # Timestamps / dates ------------------------------------------------
    maybe("ModificationTimestamp", _datetime_strategy())
    maybe("OriginalEntryTimestamp", _datetime_strategy())
    maybe("PendingTimestamp", _datetime_strategy())
    maybe("StatusChangeTimestamp", _datetime_strategy())
    maybe("WithdrawnDate", _date_strategy())
    maybe("CloseDate", _date_strategy())
    maybe("PhotosChangeTimestamp", _datetime_strategy())

    # Media counts -----------------------------------------------------
    maybe("PhotosCount", st.integers(min_value=0, max_value=1000))
    maybe("VideosCount", st.integers(min_value=0, max_value=1000))

    # Property classification ------------------------------------------
    maybe("PropertyType", st.text(max_size=30))
    maybe("PropertySubType", st.text(max_size=30))
    maybe("PropertySubTypeAdditional", st.text(max_size=128))
    maybe("StructureType", st.text(max_size=128))
    maybe("YearBuiltDetails", st.text(max_size=128))
    maybe("ArchitecturalStyle", st.text(max_size=128))
    maybe("PropertyAttachedYN", st.booleans())
    maybe("Stories", st.integers(min_value=0, max_value=200))

    # Size and rooms ---------------------------------------------------
    maybe("LivingArea", _area_strategy())
    maybe("LotSizeSquareFeet", _area_strategy())
    maybe("BedroomsTotal", st.integers(min_value=0, max_value=50))
    maybe("BathroomsFull", st.integers(min_value=0, max_value=50))
    maybe("BathroomsHalf", st.integers(min_value=0, max_value=50))
    maybe("BathroomsThreeQuarter", st.integers(min_value=0, max_value=50))
    maybe(
        "GarageSpaces",
        st.decimals(
            min_value=Decimal("0"),
            max_value=Decimal("9999.99"),
            places=2,
            allow_nan=False,
            allow_infinity=False,
        ).map(str),
    )
    maybe("YearBuilt", st.integers(min_value=1700, max_value=2100))
    maybe("YearBuiltEffective", st.integers(min_value=1700, max_value=2100))

    # Features ---------------------------------------------------------
    maybe("PoolPrivateYN", st.booleans())
    maybe("SpaYN", st.booleans())
    maybe("DirectionFaces", st.text(max_size=32))
    maybe("SeniorCommunityYN", st.booleans())
    maybe("AssociationYN", st.booleans())
    maybe("AssociationAmenities", st.text(max_size=512))
    maybe("HorseAmenities", st.text(max_size=512))
    maybe("PetsAllowedYN", st.booleans())
    maybe("Furnished", st.text(max_size=32))

    # Agents, offices, teams -------------------------------------------
    maybe("ListAgentKey", st.text(max_size=128))
    maybe("ListOfficeKey", st.text(max_size=128))
    maybe("ListTeamKey", st.text(max_size=128))
    maybe("BuyerAgentKey", st.text(max_size=128))
    maybe("BuyerOfficeKey", st.text(max_size=128))
    maybe("BuyerTeamKey", st.text(max_size=128))

    return record


@given(raw=valid_records())
@settings(
    max_examples=100,
    suppress_health_check=[
        HealthCheck.function_scoped_fixture,
        HealthCheck.too_slow,
    ],
)
def test_pydantic_round_trip(raw: dict[str, Any]) -> None:
    """Property 13 (Requirements 5.8, 14.2, 14.3).

    ``parse -> serialize -> parse`` must fixpoint: the twice-parsed model
    equals the once-parsed model under Pydantic equality. Any lossy
    serialization (datetime precision, Decimal formatting, tz
    normalization, unknown-field stripping) would break this equality
    and Hypothesis would shrink to a minimal counterexample.
    """
    m1 = Property.model_validate(raw)
    json_str = m1.model_dump_json()
    m2 = Property.model_validate_json(json_str)

    assert m1 == m2
