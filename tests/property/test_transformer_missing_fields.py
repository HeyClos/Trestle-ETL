"""Property-based test for transformer tolerance of missing RESO fields.

Property 12 (design.md): For any raw record formed by removing any subset
of non-``ListingKey`` fields from a valid record, validation succeeds and
the resulting model has ``None`` for every removed field.

**Validates: Requirements 5.2**

Implementation notes:

* The design's acceptance criterion 5.2 promises that absent RESO fields
  are treated as null rather than raising. ``ListingKey`` is the one
  exception (Requirement 5.6) and is therefore held constant across every
  generated subset so ``validate`` never hits the skip path.
* We start from a fully-populated reference record covering every
  ``PROMOTED_COLUMNS`` entry. Hypothesis samples an arbitrary subset of
  the non-key column names to remove, and the body asserts both that
  validation succeeds and that each removed field surfaces as ``None`` on
  the returned model. Any field whose model default was not ``None``, or
  any validator that raised on absence, would fail this property.
* The field values in ``FULL_RECORD`` are expressed as strings where the
  typed column uses ``Decimal``/``date``/``datetime``. Pydantic coerces
  them on validation, exercising the same path production records take
  from Trestle JSON responses.
"""

from __future__ import annotations

from hypothesis import given, settings, strategies as st

from trestle_etl.transformer import PROMOTED_COLUMNS, validate


# A fully populated reference record used as the starting point for every
# generated example. Keeping ``ListingKey`` in every subset means the
# transformer never takes the Requirement 5.6 skip path while Hypothesis
# explores Requirement 5.2's "absent field" space.
FULL_RECORD: dict = {
    "ListingKey": "LK-ABC-123",
    "ListingId": "MLS-001",
    "MlsStatus": "Active",
    "InternetEntireListingDisplayYN": True,
    "InternetAddressDisplayYN": True,
    "InternetAutomatedValuationDisplayYN": False,
    "InternetConsumerCommentYN": False,
    "Latitude": "47.6062",
    "Longitude": "-122.3321",
    "ParcelNumber": "1234-567-890",
    "StreetNumberNumeric": 123,
    "StreetDirPrefix": "N",
    "StreetName": "Main",
    "StreetSuffix": "St",
    "UnitNumber": "4B",
    "City": "Seattle",
    "StateOrProvince": "WA",
    "PostalCode": "98101",
    "OriginalListPrice": "525000.00",
    "ListPrice": "500000.00",
    "ClosePrice": "495000.00",
    "ModificationTimestamp": "2024-03-14T17:32:00Z",
    "OriginalEntryTimestamp": "2024-01-15T09:00:00Z",
    "PendingTimestamp": "2024-02-20T12:00:00Z",
    "StatusChangeTimestamp": "2024-03-14T17:32:00Z",
    "WithdrawnDate": "2024-04-01",
    "CloseDate": "2024-03-14",
    "PhotosChangeTimestamp": "2024-01-16T10:00:00Z",
    "PhotosCount": 20,
    "VideosCount": 1,
    "PropertyType": "Residential",
    "PropertySubType": "SingleFamilyResidence",
    "PropertySubTypeAdditional": "Detached",
    "StructureType": "House",
    "YearBuiltDetails": "Approximate",
    "ArchitecturalStyle": "Craftsman",
    "PropertyAttachedYN": False,
    "Stories": 2,
    "LivingArea": "1800.5",
    "LotSizeSquareFeet": "5200.0",
    "BedroomsTotal": 3,
    "BathroomsFull": 2,
    "BathroomsHalf": 1,
    "BathroomsThreeQuarter": 0,
    "GarageSpaces": "2.0",
    "YearBuilt": 1998,
    "YearBuiltEffective": 2010,
    "PoolPrivateYN": False,
    "SpaYN": False,
    "DirectionFaces": "East",
    "SeniorCommunityYN": False,
    "AssociationYN": True,
    "AssociationAmenities": "Pool,Clubhouse",
    "HorseAmenities": "None",
    "PetsAllowedYN": True,
    "Furnished": "Unfurnished",
    "ListAgentKey": "AK-1",
    "ListOfficeKey": "OK-1",
    "ListTeamKey": "TK-1",
    "BuyerAgentKey": "BAK-1",
    "BuyerOfficeKey": "BOK-1",
    "BuyerTeamKey": "BTK-1",
}

# Every Promoted_Column other than the primary key. ``st.sampled_from``
# needs a non-empty sequence, which is guaranteed because
# ``PROMOTED_COLUMNS`` has 60+ entries in the design's data-model table.
NON_KEY_FIELDS: tuple[str, ...] = tuple(
    field for field in PROMOTED_COLUMNS if field != "ListingKey"
)


@given(to_remove=st.sets(st.sampled_from(NON_KEY_FIELDS)))
@settings(max_examples=100)
def test_missing_field_tolerance(to_remove: set[str]) -> None:
    """``validate`` tolerates any subset of absent non-key fields.

    For each generated subset ``to_remove``:

    1. Build a record that omits those keys from the reference record.
    2. Validate; the result must be a ``Property`` instance, never
       ``None`` (which would indicate the ``ListingKey`` skip path) and
       never an exception (which would violate Requirement 5.2).
    3. Every removed field must surface as ``None`` on the model, proving
       the Pydantic defaults line up with the "absent = null" contract.
    """
    record = {k: v for k, v in FULL_RECORD.items() if k not in to_remove}

    model = validate(record)

    assert model is not None
    for field in to_remove:
        value = getattr(model, field)
        assert value is None, (
            f"Field {field} should be None when removed from the raw "
            f"record but got {value!r}"
        )
