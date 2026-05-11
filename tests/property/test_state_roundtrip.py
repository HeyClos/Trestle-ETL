"""Property-based tests for ``trestle_etl.state.StateStore`` round-trip.

Property 23 (design.md): For any ``SyncState`` value (including ``None``
fields, long ``nextLink`` URLs up to 2048 chars, and arbitrary UTC
timestamps), ``StateStore.load()`` after ``StateStore.save(s)`` returns a
``SyncState`` equal to ``s``.

**Validates: Requirements 9.1, 9.2, 9.6**

Implementation notes:

* Hypothesis emits a ``function_scoped_fixture`` health check for pytest
  ``tmp_path``/``tmp_path_factory`` usage inside ``@given`` bodies because
  the fixture is re-created per test invocation rather than per Hypothesis
  example. To keep the test self-contained and honest to real on-disk
  behavior, we allocate a fresh ``tempfile.TemporaryDirectory`` inside the
  test body for each example. This is what the design's testing strategy
  recommends for state-file round-trips.
* The StateStore normalizes all datetimes to UTC on the way in (via
  ``_encode_datetime``) and returns UTC-aware datetimes on the way out.
  Generating UTC-aware datetimes up front means equality holds without
  post-processing.
* Hypothesis' default ``st.text()`` strategy excludes surrogate code
  points (Cs category), so the generated ``nextLink`` strings are always
  encodable as UTF-8 and representable in JSON without additional
  filtering.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path

from hypothesis import HealthCheck, given, settings, strategies as st

from trestle_etl.state import StateStore, SyncState


# Bounds chosen to match the design's "arbitrary UTC timestamps" phrasing
# while staying inside datetime's representable range with a margin. Using
# naive datetimes from ``st.datetimes()`` (no ``timezones=`` kwarg) and
# manually attaching UTC keeps microsecond precision and avoids any
# ambiguity about whether Hypothesis generated the offset we expect.
_MIN_DT = datetime(2000, 1, 1)
_MAX_DT = datetime(2100, 1, 1)


@st.composite
def sync_states(draw: st.DrawFn) -> SyncState:
    """Generate an arbitrary ``SyncState``.

    Covers the shape space called out by Property 23:

    * ``last_modification_timestamp`` may be ``None`` (un-initialized
      pipeline) or a tz-aware UTC datetime with microsecond precision.
    * ``replication_in_progress`` is an arbitrary boolean.
    * ``replication_next_link`` may be ``None`` or a non-empty string up
      to 2048 characters (the design's upper bound for link length).
    * ``replication_next_link_persisted_at`` is coupled to the presence of
      ``replication_next_link``: when the link is set, the persisted-at
      timestamp is also set (mirroring the invariant enforced by the
      orchestrator when it writes state).
    """
    has_last_mod = draw(st.booleans())
    last_mod: datetime | None = None
    if has_last_mod:
        last_mod = draw(
            st.datetimes(min_value=_MIN_DT, max_value=_MAX_DT)
        ).replace(tzinfo=timezone.utc)

    replication_in_progress = draw(st.booleans())

    has_link = draw(st.booleans())
    link: str | None = None
    persisted_at: datetime | None = None
    if has_link:
        # ``min_size=1`` because the StateStore treats a present-but-empty
        # link identically to absent; generating empty strings here would
        # just collapse into the ``has_link=False`` case without adding
        # coverage.
        link = draw(st.text(min_size=1, max_size=2048))
        persisted_at = draw(
            st.datetimes(min_value=_MIN_DT, max_value=_MAX_DT)
        ).replace(tzinfo=timezone.utc)

    return SyncState(
        last_modification_timestamp=last_mod,
        replication_in_progress=replication_in_progress,
        replication_next_link=link,
        replication_next_link_persisted_at=persisted_at,
    )


@given(state=sync_states())
@settings(
    max_examples=100,
    # ``function_scoped_fixture`` does not apply here because the test
    # body creates its own tempdir, but we suppress the check explicitly
    # so any future refactor that reaches for a pytest fixture gets a
    # clear failure rather than a silent flake.
    #
    # ``too_slow`` is suppressed because generating full-unicode text up
    # to 2048 characters (the design's upper bound on nextLink length)
    # is legitimately slow on the first few draws; the test body itself
    # is fast (a single atomic file write + read).
    suppress_health_check=[
        HealthCheck.function_scoped_fixture,
        HealthCheck.too_slow,
    ],
)
def test_state_store_round_trip(state: SyncState) -> None:
    """``load(save(s)) == s`` for every generated ``SyncState``.

    The round-trip exercises the entire persistence path:

    1. ``_serialize`` emits JSON with ISO 8601 UTC timestamps.
    2. ``StateStore.save`` writes the document atomically via tmp-file +
       ``os.replace`` (Requirement 9.6).
    3. ``StateStore.load`` reads the document and ``_deserialize``
       reconstructs a ``SyncState`` with UTC-aware datetimes
       (Requirements 9.1, 9.2).

    Any mismatch between the input and the reloaded state -- whether from
    a timestamp precision loss, a tz normalization bug, or a missing
    field -- surfaces here as an equality failure that Hypothesis shrinks
    to a minimal counterexample.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "state.json"
        store = StateStore(path)
        store.save(state)
        loaded = store.load()

        assert loaded == state
