"""Transformer: validates raw Trestle records and builds database rows.

Pure functions, no I/O. The transformer is the sole component that pairs a
validated :class:`Property` model with its original JSON payload, producing
the :data:`Row` tuple that both loader strategies consume.

Requirements validated here:
    - 5.1: Records are validated through :class:`Property` (Pydantic v2).
    - 5.2: Absent RESO fields become ``None`` without raising; all fields
      other than ``ListingKey`` are declared ``Optional`` on the model.
    - 5.3: Multi-select enumeration strings survive untouched in the raw
      JSON because the raw dict is serialized verbatim (Property 14).
    - 5.5: Every validated record yields a Promoted_Columns tuple plus a
      full ``raw_data`` JSON payload.
    - 5.6: Records lacking ``ListingKey`` are skipped with a WARNING log and
      the pipeline continues processing subsequent records.
    - 5.7: Timestamp fields are parsed as UTC ``datetime`` by the model.
    - 14.1: :func:`validate` and :func:`to_row` are the parse / serialize
      entry points used by the rest of the pipeline.
    - 14.4: Unknown RESO fields are preserved end-to-end because
      ``raw_data_json_str`` is produced from the ORIGINAL raw dict, not
      from :meth:`Property.model_dump`.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Final

from trestle_etl.loader import Row
from trestle_etl.models import Property

logger = logging.getLogger(__name__)

# PROMOTED_COLUMNS lists the typed column names in the exact order declared
# by the MySQL ``property`` table in ``trestle_etl/sql/schema.sql``. Both
# loader strategies rely on this order when building INSERT statements or
# CSV rows, so the single source of truth lives here (Requirement 5.5, 6.4).
PROMOTED_COLUMNS: Final[tuple[str, ...]] = (
    "ListingKey",
    "ModificationTimestamp",
    "StandardStatus",
    "MlsStatus",
    "PropertyType",
    "PropertySubType",
    "ListPrice",
    "ClosePrice",
    "OriginalListPrice",
    "ListingContractDate",
    "CloseDate",
    "StreetNumber",
    "StreetName",
    "UnitNumber",
    "City",
    "StateOrProvince",
    "PostalCode",
    "County",
    "Country",
    "Latitude",
    "Longitude",
    "BedroomsTotal",
    "BathroomsTotalInteger",
    "LivingArea",
    "LotSizeSquareFeet",
    "YearBuilt",
    "DaysOnMarket",
    "ListAgentKey",
    "ListOfficeKey",
    "PhotosCount",
    "PublicRemarks",
)

# Best-effort set of fields surfaced in the warning log line when
# ``ListingKey`` is missing, so an operator can trace the skipped record
# back to its source without digging through raw payloads. Only fields
# actually present on the record are included.
_IDENTIFYING_FIELDS: Final[tuple[str, ...]] = (
    "ListingId",
    "MlsId",
    "ListAgentKey",
    "ListOfficeKey",
    "ModificationTimestamp",
)


def _has_listing_key(raw: dict) -> bool:
    """Return ``True`` iff ``raw`` carries a non-empty ``ListingKey``.

    We check the raw dict before calling :meth:`Property.model_validate` so
    that the skip case (Requirement 5.6) is a clean ``return None`` rather
    than catching a Pydantic ``ValidationError``. Strings that are empty or
    whitespace-only are treated as missing, mirroring the model's
    ``min_length=1`` constraint.
    """
    value = raw.get("ListingKey")
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def _identifying_context(raw: dict) -> dict[str, Any]:
    """Pick a small set of identifying fields for the skip warning."""
    return {
        field: raw[field]
        for field in _IDENTIFYING_FIELDS
        if field in raw and raw[field] is not None
    }


def validate(raw: dict) -> Property | None:
    """Validate a raw Trestle Property dict into a :class:`Property` model.

    Returns ``None`` (and emits a WARNING carrying whatever identifying
    fields the record carries) when ``ListingKey`` is missing or empty so
    the orchestrator can simply filter out the skipped rows
    (Requirement 5.6). All other Pydantic validation errors propagate to
    the caller.
    """
    if not _has_listing_key(raw):
        logger.warning(
            "Skipping record without ListingKey: identifying_fields=%s",
            _identifying_context(raw),
        )
        return None
    return Property.model_validate(raw)


def to_row(raw: dict, model: Property) -> Row:
    """Build a ``(promoted_columns_tuple, raw_data_json_str)`` pair.

    The promoted-columns tuple is drawn from the validated ``model`` so the
    typed column values honor Pydantic's coercion (decimals, UTC
    datetimes, etc.). The ``raw_data_json_str`` is produced from the
    ORIGINAL ``raw`` dict, not from :meth:`Property.model_dump`. That is
    what guarantees Requirement 14.4: unknown RESO fields, and multi-select
    comma-separated enumeration strings (Requirement 5.3), survive the
    round-trip through the loader into the ``raw_data`` column byte-for-
    byte. ``default=str`` is a defensive fallback for any non-primitive
    values a caller might have injected into the dict; Trestle JSON
    responses parsed by :mod:`requests` are already primitive-typed.
    """
    promoted = tuple(getattr(model, name) for name in PROMOTED_COLUMNS)
    raw_data_json = json.dumps(raw, default=str, ensure_ascii=False)
    return promoted, raw_data_json


def to_row_safe(raw: dict) -> Row | None:
    """Validate and build a row in one step; ``None`` when the record skips.

    Convenience wrapper for orchestrator callers that only want a row if
    the record is valid. When :func:`validate` returns ``None`` because the
    record lacks a ``ListingKey``, this function also returns ``None``.
    """
    model = validate(raw)
    if model is None:
        return None
    return to_row(raw, model)


__all__ = ["PROMOTED_COLUMNS", "validate", "to_row", "to_row_safe"]
