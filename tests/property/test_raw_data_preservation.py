"""Property test for the Transformer's ``raw_data`` preservation (Property 14).

Property 14 (design.md): For any raw record dict (including arbitrary
unknown fields, comma-separated multi-select values), the JSON payload
produced by :func:`trestle_etl.transformer.to_row` round-trips to an
equal dict, i.e. ``json.loads(raw_data_json) == record``.

**Validates: Requirements 5.3, 5.5, 14.4**

Implementation notes:

* The ``to_row`` contract is ``raw_data_json = json.dumps(raw, default=str,
  ensure_ascii=False)``. The property therefore holds by construction
  provided the raw dict contains only JSON-safe values (types that
  ``json.dumps`` serializes natively and ``json.loads`` restores to an
  equal Python value). The generator is deliberately constrained to that
  subspace so the test exercises the preservation invariant itself rather
  than the ``default=str`` defensive fallback (which would convert a
  ``datetime`` into a string and break round-trip equality by design).
* Every generated record carries a non-empty, non-whitespace ``ListingKey``
  so :func:`validate` succeeds and :func:`to_row` can be called.
  Property 11 already covers the skip branch; here we focus on the
  post-validation preservation invariant.
* Extras exclude Promoted_Column names other than ``ListingKey``. Typed
  fields like ``ListPrice`` or ``ModificationTimestamp`` would otherwise
  receive arbitrary JSON values and trip Pydantic validation. Property 14
  is about preservation of unknown fields and multi-select strings, so
  constraining the keyspace to "anything but the typed columns" cleanly
  isolates what we're asserting.
* A subset of records carries an explicit comma-separated multi-select
  enumeration string (e.g. ``"CentralAir,Electric,Zoned"``) in an unknown
  field. Requirement 5.3 calls this case out by name; sampling it
  deliberately ensures Hypothesis hits it rather than relying on random
  commas landing in generated text.
* Floats are drawn with ``allow_nan=False`` and ``allow_infinity=False``
  because ``json.dumps`` rejects those values by default, and the
  production code does not pass ``allow_nan=True``. Finite floats round-
  trip exactly through Python's ``json`` module (``repr``-based
  serialization preserves the bit pattern), so equality holds.
* Integers are capped at ``±2**53`` - Python's ``json`` module handles
  arbitrary-precision ints fine, but staying inside the IEEE 754
  safe-integer range keeps generated payloads small without changing
  what the property asserts.
"""

from __future__ import annotations

import json
from typing import Any

from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.transformer import PROMOTED_COLUMNS, to_row, validate


# Keys that map to typed Pydantic fields on ``Property``. Extras avoid
# these names so arbitrary JSON values can't trip model validation; the
# property under test is about the unknown-field keyspace per Req 14.4.
_TYPED_COLUMN_NAMES = frozenset(PROMOTED_COLUMNS)


# Alphabet used for the guaranteed-valid ``ListingKey``. Excluding
# whitespace guarantees ``_has_listing_key`` returns True (the model also
# enforces ``min_length=1``/``max_length=128``).
_LISTING_KEY_ALPHABET = (
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789-_"
)


# JSON-safe atoms: types whose ``json.dumps`` -> ``json.loads`` round-trip
# yields an equal Python value. ``st.text()`` already excludes surrogate
# code points, which would otherwise break UTF-8 encoding.
_json_atoms = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**53), max_value=(2**53)),
    st.floats(allow_nan=False, allow_infinity=False, width=64),
    st.text(max_size=40),
)


# Recursive JSON-safe values: atoms plus lists and string-keyed dicts of
# the same. ``max_leaves`` keeps example size reasonable for a 100+
# iteration budget.
_json_values = st.recursive(
    _json_atoms,
    lambda children: st.one_of(
        st.lists(children, max_size=5),
        st.dictionaries(st.text(max_size=20), children, max_size=5),
    ),
    max_leaves=15,
)


# Multi-select enumeration string: Requirement 5.3 calls out comma-
# separated values explicitly. Non-empty tokens without commas inside
# them produce a realistic RESO-style payload like "CentralAir,Electric".
_multi_select_token = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    min_size=1,
    max_size=20,
)
_multi_select_string = st.lists(
    _multi_select_token, min_size=0, max_size=4
).map(",".join)


@st.composite
def raw_records(draw: st.DrawFn) -> dict[str, Any]:
    """Generate a raw record dict that :func:`validate` will accept.

    Shape:

    * Always contains a valid ``ListingKey``.
    * Optionally contains an ``_MultiSelectExample`` key with a comma-
      separated enumeration string, exercising Requirement 5.3.
    * Contains zero or more arbitrary unknown fields with JSON-safe
      values (atoms, nested lists, nested dicts). Key names avoid the
      typed Promoted_Column set so Pydantic validation can't fail on
      unrelated type mismatches.
    """
    record: dict[str, Any] = {
        "ListingKey": draw(
            st.text(
                alphabet=_LISTING_KEY_ALPHABET, min_size=1, max_size=128
            )
        ),
    }

    # Multi-select enumeration string, covered as an unknown field so
    # the comma-separated shape is preserved verbatim in ``raw_data``
    # (Requirement 5.3).
    if draw(st.booleans()):
        record["_MultiSelectExample"] = draw(_multi_select_string)

    # Arbitrary extras. Keys are constrained to avoid clashing with the
    # typed Promoted_Column set; values span the full JSON-safe space.
    extras = draw(
        st.dictionaries(
            st.text(min_size=1, max_size=20).filter(
                lambda k: k not in _TYPED_COLUMN_NAMES
                and k != "_MultiSelectExample"
            ),
            _json_values,
            max_size=8,
        )
    )
    record.update(extras)
    return record


@given(raw=raw_records())
@settings(max_examples=150, suppress_health_check=[HealthCheck.too_slow])
def test_raw_data_preserves_original_dict(raw: dict[str, Any]) -> None:
    """Property 14 (Requirements 5.3, 5.5, 14.4).

    ``to_row`` must emit a ``raw_data`` JSON payload whose
    ``json.loads`` value equals the original raw dict. This guarantees
    that unknown RESO fields and multi-select enumeration strings
    survive end-to-end into the MySQL ``raw_data`` column without any
    lossy round-trip through the Pydantic model.
    """
    model = validate(raw)
    # The generator guarantees a non-empty ListingKey, so validate()
    # must return a model; anything else would indicate a regression in
    # the skip logic exercised by Property 11.
    assert model is not None

    _promoted, raw_data_json = to_row(raw, model)

    # The core preservation invariant: a byte-for-byte-equivalent dict
    # can be reconstructed from the persisted JSON payload.
    assert json.loads(raw_data_json) == raw
