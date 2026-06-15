"""Pydantic v2 models for Trestle Property records.

The :class:`Property` model mirrors the Promoted_Columns set defined in the
feature design. Unknown RESO fields are retained on the model via
``extra="allow"`` so that the Transformer can later emit them into the
``raw_data`` JSON column alongside the typed Promoted_Columns.

Requirements:
    - 5.1: Validate each incoming record against a Pydantic v2 model.
    - 5.4: Store standard RESO enumeration values in their canonical form
      (no reliance on Trestle's ``PrettyEnums`` query parameter).
    - 5.7: Timestamp fields are parsed as UTC ``datetime`` values.
    - 14.1: Model supports parse/serialize round-trips via
      :meth:`Property.model_validate` and :meth:`Property.model_dump` /
      :meth:`Property.model_dump_json`.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _coerce_utc(value: datetime) -> datetime:
    """Return ``value`` as a UTC-aware ``datetime``.

    Naive datetimes are assumed to already express a UTC instant and are
    tagged with :data:`datetime.timezone.utc`. Aware datetimes are converted
    to UTC so downstream code only ever sees one canonical timezone
    (Requirement 5.7).
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class Property(BaseModel):
    """Validated representation of a single Trestle Property record.

    Only Promoted_Columns are declared as typed fields. Any additional RESO
    fields present on the raw record are retained on the model instance
    thanks to ``extra="allow"``, which keeps the Pydantic model aligned with
    the canonical copy preserved in the ``raw_data`` JSON column
    (Requirement 14.4).
    """

    model_config = ConfigDict(extra="allow")

    # Primary key: a non-empty RESO ListingKey, capped at 128 characters to
    # match the MySQL column definition (Requirement 6.2, 5.6).
    ListingKey: str = Field(min_length=1, max_length=128)

    # MLS-facing identifier and status ----------------------------------
    ListingId: str | None = None
    MlsStatus: str | None = None

    # Internet display flags --------------------------------------------
    InternetEntireListingDisplayYN: bool | None = None
    InternetAddressDisplayYN: bool | None = None
    InternetAutomatedValuationDisplayYN: bool | None = None
    InternetConsumerCommentYN: bool | None = None

    # Geospatial --------------------------------------------------------
    Latitude: Decimal | None = None
    Longitude: Decimal | None = None

    # Address -----------------------------------------------------------
    ParcelNumber: str | None = None
    StreetNumberNumeric: int | None = None
    StreetDirPrefix: str | None = None
    StreetName: str | None = None
    StreetSuffix: str | None = None
    UnitNumber: str | None = None
    City: str | None = None
    StateOrProvince: str | None = None
    PostalCode: str | None = None

    # Monetary values ---------------------------------------------------
    OriginalListPrice: Decimal | None = None
    ListPrice: Decimal | None = None
    ClosePrice: Decimal | None = None

    # Timestamps and dates ----------------------------------------------
    ModificationTimestamp: datetime | None = None
    OriginalEntryTimestamp: datetime | None = None
    PendingTimestamp: datetime | None = None
    StatusChangeTimestamp: datetime | None = None
    WithdrawnDate: date | None = None
    CloseDate: date | None = None
    PhotosChangeTimestamp: datetime | None = None

    # Media counts ------------------------------------------------------
    PhotosCount: int | None = None
    VideosCount: int | None = None

    # Property classification -------------------------------------------
    PropertyType: str | None = None
    PropertySubType: str | None = None
    PropertySubTypeAdditional: str | None = None
    StructureType: str | None = None
    YearBuiltDetails: str | None = None
    ArchitecturalStyle: str | None = None
    PropertyAttachedYN: bool | None = None
    Stories: int | None = None

    # Size and rooms ----------------------------------------------------
    LivingArea: Decimal | None = None
    LotSizeSquareFeet: Decimal | None = None
    BedroomsTotal: int | None = None
    BathroomsFull: int | None = None
    BathroomsHalf: int | None = None
    BathroomsThreeQuarter: int | None = None
    GarageSpaces: Decimal | None = None
    YearBuilt: int | None = None
    YearBuiltEffective: int | None = None

    # Features ----------------------------------------------------------
    PoolPrivateYN: bool | None = None
    SpaYN: bool | None = None
    DirectionFaces: str | None = None
    SeniorCommunityYN: bool | None = None
    AssociationYN: bool | None = None
    AssociationAmenities: str | None = None
    HorseAmenities: str | None = None
    PetsAllowedYN: bool | None = None
    Furnished: str | None = None

    # Agents, offices, teams --------------------------------------------
    ListAgentKey: str | None = None
    ListOfficeKey: str | None = None
    ListTeamKey: str | None = None
    BuyerAgentKey: str | None = None
    BuyerOfficeKey: str | None = None
    BuyerTeamKey: str | None = None

    @field_validator(
        "ModificationTimestamp",
        "OriginalEntryTimestamp",
        "PendingTimestamp",
        "StatusChangeTimestamp",
        "PhotosChangeTimestamp",
        mode="after",
    )
    @classmethod
    def _normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        """Force timestamp fields to a UTC-aware ``datetime``.

        Pydantic parses ISO 8601 strings (with or without offset) into
        ``datetime`` instances; this validator runs afterwards so it can
        normalize both already-parsed values and naive datetimes supplied
        directly by callers. Naive values are treated as UTC because the
        Trestle API documents all timestamps as UTC (Requirement 4.6, 5.7).
        """
        if value is None:
            return None
        return _coerce_utc(value)


__all__ = ["Property"]
