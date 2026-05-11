"""Property test for the Transformer's missing-``ListingKey`` skip (Property 11).

Property 11 (design.md): For any raw record dict, the Transformer produces
a Row iff the record contains a non-empty ``ListingKey`` field; records
without a ``ListingKey`` produce no Row and do not raise.

**Validates: Requirements 5.6**

Implementation notes:

* The Pydantic ``Property`` model declares ``ListingKey: str`` with
  ``min_length=1`` and ``max_length=128``. The Transformer's
  :func:`_has_listing_key` helper additionally treats whitespace-only
  strings as missing so the skip path is a clean ``return None`` rather
  than a caught ``ValidationError``. The generator here covers all four
  shape buckets explicitly so Hypothesis shrinks to the canonical minimal
  counterexample in each bucket on failure (``"A"`` for present,
  ``""`` for empty, ``" "`` for whitespace, and ``{}`` for missing).
* We don't rely on ``st.text().filter(bool)`` for the "present" case
  because that filter is sensitive to whitespace-only draws; using
  ``alphabet=`` with non-whitespace characters and ``min_size=1``
  guarantees a stripped-non-empty key without Hypothesis throwing
  ``Unsatisfiable`` errors from filter pressure.
* Extras beyond ``ListingKey`` are optional; when present they exercise
  that the Transformer tolerates arbitrary RESO fields on either branch
  of the iff (Requirements 5.2, 14.4 remain covered by separate tests).
"""

from __future__ import annotations

from typing import Any

from hypothesis import given, settings, strategies as st

from trestle_etl.transformer import to_row_safe, validate


# Alphabet that cannot produce whitespace-only keys. Using ASCII letters
# plus digits keeps generated keys short, readable, and well within the
# model's 128-character cap without any filter overhead.
_NON_WS_ALPHABET = (
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
)

# Pure-whitespace alphabet for the "whitespace" bucket. The Transformer's
# ``_has_listing_key`` calls ``.strip()`` on the value, so any combination
# of these characters must be treated as missing.
_WS_ALPHABET = " \t\n\r\f\v"


@st.composite
def raw_records(draw: st.DrawFn) -> tuple[dict[str, Any], str]:
    """Generate a raw record paired with its ``ListingKey`` shape bucket.

    Returns a ``(record, kind)`` tuple where ``kind`` is one of
    ``"present"``, ``"empty"``, ``"whitespace"``, or ``"missing"``.
    Covering the four buckets with equal sampling weight keeps Hypothesis
    from over-indexing on the trivially-missing case (``{}``), which
    would otherwise dominate the draw distribution because the default
    ``st.dictionaries`` strategy skews toward small dicts.
    """
    record: dict[str, Any] = {}
    kind = draw(
        st.sampled_from(["present", "empty", "whitespace", "missing"])
    )

    if kind == "present":
        # A non-empty key whose ``.strip()`` is also non-empty, capped at
        # the model's 128-character limit (Requirement 6.2).
        record["ListingKey"] = draw(
            st.text(alphabet=_NON_WS_ALPHABET, min_size=1, max_size=128)
        )
    elif kind == "empty":
        record["ListingKey"] = ""
    elif kind == "whitespace":
        record["ListingKey"] = draw(
            st.text(alphabet=_WS_ALPHABET, min_size=1, max_size=16)
        )
    # kind == "missing": leave the key absent from ``record`` entirely.

    # Optionally add a handful of unrelated fields so both branches of
    # the iff are exercised against realistic-shaped records rather than
    # against a single-key dict. ``City`` is typed; arbitrary ``extra_*``
    # fields exercise Pydantic's ``extra="allow"`` path on the present
    # branch (and are simply ignored on the skip branch).
    if draw(st.booleans()):
        record["City"] = draw(st.text(max_size=30))
    if draw(st.booleans()):
        record["ListAgentKey"] = draw(
            st.text(alphabet=_NON_WS_ALPHABET, min_size=1, max_size=64)
        )
    if draw(st.booleans()):
        record["extra_unknown_field"] = draw(
            st.one_of(st.integers(), st.text(max_size=20), st.none())
        )

    return record, kind


@given(raw_and_kind=raw_records())
@settings(max_examples=200)
def test_listing_key_skip_property(
    raw_and_kind: tuple[dict[str, Any], str],
) -> None:
    """Property 11 (Requirements 5.6).

    Runs both Transformer entry points on every generated record and
    asserts the iff across the full behavior surface:

    * :func:`to_row_safe` returns a Row for "present" records and ``None``
      for every other bucket.
    * :func:`validate` returns a :class:`Property` instance for "present"
      records and ``None`` for every other bucket (direct coverage of
      Requirement 5.6 as called out in the design).
    * Neither entry point raises on any bucket: the skip path is a quiet
      ``return None`` plus a WARNING log, never an exception.
    """
    raw, kind = raw_and_kind

    # Neither call is allowed to raise; the test itself would fail with a
    # traceback rather than an assertion, but spelling this out keeps the
    # "does not raise" clause of Property 11 explicit in the test body.
    row = to_row_safe(raw)
    model = validate(raw)

    if kind == "present":
        # A non-empty, non-whitespace ``ListingKey`` must produce a Row.
        assert row is not None, (
            f"Expected a Row for present ListingKey={raw.get('ListingKey')!r}"
        )
        # Row is a ``(promoted_tuple, raw_data_json)`` pair; the first
        # element of the promoted tuple is the ``ListingKey`` column by
        # virtue of ``PROMOTED_COLUMNS`` ordering in the transformer.
        promoted, _raw_json = row
        assert promoted[0] == raw["ListingKey"]

        assert model is not None
        assert model.ListingKey == raw["ListingKey"]
    else:
        # Missing, empty, or whitespace-only ``ListingKey`` must skip.
        assert row is None, (
            f"Expected no Row for kind={kind!r} "
            f"ListingKey={raw.get('ListingKey')!r}"
        )
        assert model is None
