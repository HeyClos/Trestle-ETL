"""Property test for Upsert_Path batch-size bounds (Property 18).

Property 18 (design.md): For any input record stream processed by the
Upsert_Path, every committed batch except possibly the final batch SHALL
contain between 1,000 and 5,000 records (inclusive), and the final batch
SHALL contain between 1 and 5,000 records.

**Validates: Requirements 7.2**

Implementation notes
--------------------

Requirement 7.2 has two structural halves:

1. The ``UpsertLoader`` refuses any configuration that would permit a
   batch outside ``[1, 5000]``. Concretely: ``batch_size`` must be in
   ``(0, 5000]`` at construction time, and ``write_batch`` must refuse a
   batch larger than that configured ceiling at call time. Any orchestrator
   that tries to violate the Property-18 bounds therefore crashes at the
   loader boundary instead of silently committing an oversized batch.

2. The orchestrator chunks its input stream so that every non-final batch
   is at least 1,000 rows. The orchestrator is Task 9.1 (not yet built),
   so this file covers only the loader-side contract. Property 18's
   "every non-final batch has at least 1,000 rows" half will be added to
   the orchestrator's own property tests once that module exists; what
   matters for Property 18 under the current codebase is that the loader
   cannot be talked into committing a batch outside its configured range.

The loader's validation is pure Python: it runs before any SQL is issued,
so the test does not need a real MySQL. We construct a throwaway
SQLAlchemy engine (sqlite in-memory) purely so the ``Engine`` type check
and attribute access inside ``UpsertLoader.__init__`` succeed. The test
never calls ``write_batch`` on a row count that would hit the database.

Hypothesis strategy
-------------------

* ``batch_size`` generator: draws integers in a window that straddles
  every boundary of interest: negatives, zero, one, the default (1,000),
  the cap (5,000), and values above the cap. That gives Hypothesis
  enough room to shrink counterexamples to the minimal failing boundary.
* ``n_rows`` generator: draws integers in ``[0, 10000]`` so the test
  exercises empty batches (the no-op short-circuit), in-bounds batches
  (rejected only when they would exceed the configured ceiling), and
  over-cap batches (always rejected). We never call ``write_batch``
  with a non-empty in-bounds batch because that would reach the SQL
  layer, which a sqlite engine cannot satisfy.
"""

from __future__ import annotations

from typing import Final

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from trestle_etl.loader.upsert import UpsertLoader


# Requirement 7.2 fixes these two numbers. Repeating them here (rather
# than importing the private module-level constants) keeps the test a
# specification: a regression that relaxes the cap in the implementation
# must also edit the numbers here, which is exactly the "second look"
# a property-level test is supposed to force.
_DEFAULT_BATCH_SIZE: Final[int] = 1_000
_MAX_BATCH_SIZE: Final[int] = 5_000


def _throwaway_engine() -> Engine:
    """Return a SQLAlchemy engine suitable for constructor-only use.

    ``UpsertLoader.__init__`` performs no I/O against the engine; it just
    stores the reference. An in-memory sqlite engine is the cheapest
    object that satisfies the ``Engine`` type and avoids pulling in a
    real MySQL or pymysql driver for tests that never execute SQL.
    """
    return create_engine("sqlite:///:memory:")


# ---------------------------------------------------------------------------
# Constructor-side bound: batch_size must be in (0, 5000].
# ---------------------------------------------------------------------------


@given(
    batch_size=st.integers(min_value=-50, max_value=_MAX_BATCH_SIZE + 100),
)
@settings(
    max_examples=150,
    # The test body only constructs engines and loaders; both are fast
    # enough that ``too_slow`` shouldn't fire, but Hypothesis can flag
    # the first few iterations while JITting. Suppress so CI is stable.
    suppress_health_check=[HealthCheck.too_slow],
)
def test_constructor_enforces_batch_size_window(batch_size: int) -> None:
    """Property 18, construction half (Requirement 7.2).

    ``UpsertLoader(engine, batch_size=n)`` raises ``ValueError`` iff
    ``n`` is outside ``(0, 5000]``. Inside the window, the loader is
    constructed successfully and exposes the requested ``batch_size``
    unchanged: no silent clamping, because a silent clamp would cause
    the orchestrator and the loader to disagree about batch boundaries
    and make Property-18 compliance unverifiable from the orchestrator
    side.
    """
    engine = _throwaway_engine()

    if batch_size <= 0 or batch_size > _MAX_BATCH_SIZE:
        with pytest.raises(ValueError):
            UpsertLoader(engine, batch_size=batch_size)
    else:
        loader = UpsertLoader(engine, batch_size=batch_size)
        # The loader must echo the requested value verbatim; Property 18's
        # upper-bound half depends on the orchestrator being able to
        # trust ``loader.batch_size`` as the true per-batch ceiling.
        assert loader.batch_size == batch_size


def test_default_batch_size_is_one_thousand() -> None:
    """Requirement 7.2 fixes the default at 1,000.

    The default is what a vanilla orchestrator will chunk against when
    no override is supplied, so it needs to match Property 18's lower
    bound for non-final batches. Asserting the concrete number here is
    deliberate: it fails loudly if someone moves the default to, say,
    500 without re-reading the requirement.
    """
    loader = UpsertLoader(_throwaway_engine())
    assert loader.batch_size == _DEFAULT_BATCH_SIZE


# ---------------------------------------------------------------------------
# Call-site bound: write_batch refuses len(rows) > batch_size.
# ---------------------------------------------------------------------------


@given(
    n_rows=st.integers(min_value=0, max_value=_MAX_BATCH_SIZE + 1_000),
    batch_size=st.integers(min_value=1, max_value=_MAX_BATCH_SIZE),
)
@settings(
    max_examples=150,
    suppress_health_check=[HealthCheck.too_slow],
)
def test_write_batch_rejects_oversized_batches(
    n_rows: int, batch_size: int
) -> None:
    """Property 18, call-site half (Requirement 7.2).

    ``write_batch`` must raise ``ValueError`` whenever a batch exceeds
    the loader's configured ceiling. The empty-batch case is the only
    input that bypasses SQL entirely (the loader short-circuits with a
    zero-count ``BatchResult``), so it is the only non-oversized size
    we actually commit to calling ``write_batch`` with here.

    For ``0 < n_rows <= batch_size`` the loader would attempt a real
    upsert, which requires a MySQL engine we deliberately do not have
    in a property test. We rely on the constructor-side test above and
    on the integration tests in Task 7.11 to cover that path; what
    matters for Property 18 is the boundary behavior at the ceiling,
    and that is what this assertion pins down.
    """
    loader = UpsertLoader(_throwaway_engine(), batch_size=batch_size)

    # Build placeholder rows cheaply. The loader's size check runs
    # before any row-level work, so the row contents are irrelevant;
    # any object that supplies ``len()`` downstream is fine. We use
    # simple tuples to stay faithful to the ``Row`` type alias without
    # paying for PROMOTED_COLUMNS-wide tuples we'll never iterate.
    rows = [((f"K{i}",), "{}") for i in range(n_rows)]

    if n_rows > batch_size:
        # Over-ceiling: must be rejected before any SQL is issued. If
        # the loader ever silently chunked this into multiple commits,
        # Property 18's "one transaction per batch" sibling invariant
        # (Requirement 7.3) would also break.
        with pytest.raises(ValueError):
            loader.write_batch(rows)  # type: ignore[arg-type]
    elif n_rows == 0:
        # Empty batch: the loader must short-circuit with a zero-count
        # result rather than open a useless transaction. This is the
        # "final batch contains between 1 and 5,000" clause at its
        # lower edge: zero is NOT a committed batch, so the loader
        # explicitly flags it as count=0 for the orchestrator to skip.
        result = loader.write_batch(rows)  # type: ignore[arg-type]
        assert result.count == 0
    # 0 < n_rows <= batch_size: the validation passes, but executing the
    # upsert requires MySQL. Covered by integration tests (Task 7.11).
