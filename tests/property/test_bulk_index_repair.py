"""Property-based test for ``BulkLoader.ensure_indexes_if_resuming``.

Property 22 (design.md): For any state with ``replication_in_progress=true``
and any subset of the 7 required secondary indexes missing from the
``property`` table, the pipeline startup check recreates every missing
index so that all 7 are present before extraction resumes.

**Validates: Requirements 8.8**

Implementation notes:

* The test requires a real MySQL instance. We spin one up via
  ``testcontainers`` so the test exercises MySQL's actual DDL behavior
  rather than a mocked subset. Environments without Docker (CI sandboxes,
  minimal dev boxes) must still be able to run the rest of the property
  suite without failures, so the module guards with
  ``pytest.importorskip`` followed by a live ``docker.ping()`` — the first
  catches a missing Python dependency, the second catches "daemon not
  running".
* ``schema.sql`` begins with several SQL line comments (``-- ...``). A
  naïve ``split(";")`` + ``startswith("--")`` filter collapses the
  comment block and the first ``CREATE TABLE`` statement into a single
  chunk whose stripped form still begins with ``--``; applying that
  filter silently drops the table definition. The helper strips comment
  lines up front to dodge that trap.
* Pytest + Hypothesis interaction: a function-scoped fixture combined
  with ``@given`` runs its setup/teardown exactly once for the entire
  test (around all generated examples), not once per example. That means
  we cannot rely on fixture teardown to restore indexes between
  examples. Instead, each example restores the table to the
  "all-indexes-present" starting state at the top of its body.
* ``mysql_engine`` is intentionally module-scoped so the (expensive)
  MySQL container start-up and schema application run once per module.
  Module-scoped fixtures are compatible with Hypothesis without the
  ``function_scoped_fixture`` health check warning.
"""

from __future__ import annotations

import pytest

# Skip cleanly when the dev dependency is not installed (e.g. on a
# production box that only has the runtime deps). ``importorskip`` emits
# a pytest ``SKIPPED`` outcome with a clear reason rather than a hard
# ImportError.
pytest.importorskip("testcontainers.mysql")

import docker

try:
    # Actually reach the Docker daemon. ``docker.from_env()`` succeeds
    # lazily on bad configuration; ``.ping()`` is what forces the socket
    # connection and surfaces "daemon not running" as an exception we can
    # turn into a module-level skip.
    docker.from_env().ping()
except Exception:
    pytest.skip("Docker unavailable", allow_module_level=True)

from pathlib import Path

from hypothesis import HealthCheck, given, settings, strategies as st
from sqlalchemy import create_engine, inspect
from testcontainers.mysql import MySqlContainer

from trestle_etl.loader.bulk import BulkLoader
from trestle_etl.state import SyncState


# Mapping from required secondary-index name to the column it indexes.
# Mirrors Requirement 6.5 / the CREATE INDEX block at the bottom of
# trestle_etl/sql/schema.sql exactly. Keeping this local (rather than
# importing the private tuple from the loader module) makes the test an
# independent second source of truth: a typo in either side surfaces as
# a test failure rather than a silent drift.
_INDEX_DEFINITIONS: dict[str, str] = {
    "idx_property_modts": "ModificationTimestamp",
    "idx_property_status": "StandardStatus",
    "idx_property_type": "PropertyType",
    "idx_property_city": "City",
    "idx_property_postal": "PostalCode",
    "idx_property_price": "ListPrice",
    "idx_property_state": "StateOrProvince",
}

REQUIRED_INDEXES: tuple[str, ...] = tuple(_INDEX_DEFINITIONS.keys())


def _apply_schema(engine, schema_sql: str) -> None:
    """Execute every non-comment statement in ``schema_sql`` against ``engine``.

    Two subtle points worth documenting:

    * Comment-only lines (``-- ...``) are stripped BEFORE the split on
      ``;``. Otherwise the long leading block of comment lines at the
      top of ``schema.sql`` ends up in the same split chunk as
      ``CREATE TABLE property (…)``, the chunk's ``.strip()`` still
      starts with ``--``, and a ``startswith("--")`` filter silently
      discards the whole table definition.
    * Each statement runs through ``exec_driver_sql`` rather than
      ``text()`` so that SQLAlchemy does not try to parse ``:foo`` bind
      parameter placeholders out of the DDL (the schema's comment text
      could contain colon-prefixed tokens).
    """
    no_comments = "\n".join(
        line
        for line in schema_sql.splitlines()
        if not line.strip().startswith("--")
    )
    with engine.begin() as conn:
        for statement in no_comments.split(";"):
            if statement.strip():
                conn.exec_driver_sql(statement)


def _existing_indexes(engine) -> set[str]:
    """Return the set of secondary-index names on the ``property`` table.

    Uses SQLAlchemy's inspector, which queries ``information_schema``.
    The PK on ``ListingKey`` is reported separately by
    ``get_pk_constraint`` and does not appear in this set, so every
    member here is a candidate for comparison against
    :data:`REQUIRED_INDEXES`.
    """
    return {
        idx["name"]
        for idx in inspect(engine).get_indexes("property")
        if idx.get("name")
    }


def _restore_all_indexes(engine) -> None:
    """Re-create any missing required indexes, leaving existing ones alone.

    Idempotent by construction: checks presence first and only issues a
    ``CREATE INDEX`` for names that are absent. Used at the top of each
    Hypothesis example to normalize the starting state so examples do
    not leak side effects into one another.
    """
    existing = _existing_indexes(engine)
    with engine.begin() as conn:
        for name, column in _INDEX_DEFINITIONS.items():
            if name not in existing:
                conn.exec_driver_sql(
                    f"CREATE INDEX {name} ON property({column})"
                )


@pytest.fixture(scope="module")
def mysql_engine():
    """Module-scoped MySQL 8 container + engine with schema applied.

    Module scope keeps the (multi-second) container startup out of every
    Hypothesis example. The schema is applied exactly once; individual
    examples manipulate indexes on the existing table and restore them
    before each run.
    """
    with MySqlContainer("mysql:8.0") as mysql:
        engine = create_engine(mysql.get_connection_url())
        schema_sql = Path("trestle_etl/sql/schema.sql").read_text()
        _apply_schema(engine, schema_sql)
        try:
            yield engine
        finally:
            engine.dispose()


@given(to_drop=st.sets(st.sampled_from(REQUIRED_INDEXES)))
@settings(
    # A small example count because each example runs real MySQL DDL
    # against a real container. The property's state space is the power
    # set of 7 index names (128 total subsets), so 15 examples plus
    # Hypothesis' shrinking heuristics give ample coverage of the
    # "empty subset", "full subset", and intermediate-subset cases that
    # Requirement 8.8 universally quantifies over.
    max_examples=15,
    deadline=None,
    # The ``function_scoped_fixture`` warning would fire if we attached a
    # function-scoped fixture to this test. The only fixture here is
    # module-scoped, so the suppression is defensive against future
    # refactors that reach for a per-example fixture.
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_startup_index_repair(mysql_engine, to_drop: frozenset[str]) -> None:
    """``ensure_indexes_if_resuming`` restores every missing required index.

    Validates Property 22 / Requirement 8.8 directly: given
    ``replication_in_progress=True`` and an arbitrary subset of the 7
    required indexes missing from ``property``, the startup check must
    leave all 7 present.
    """
    # Normalize the starting state. Hypothesis runs many examples inside
    # a single fixture setup, so a prior example that dropped indexes
    # would make the next DROP fail with "index does not exist". Restore
    # every missing required index before each draw.
    _restore_all_indexes(mysql_engine)

    # Drop exactly the Hypothesis-selected subset. The full-subset case
    # (all 7 dropped) and the empty-subset case (nothing dropped) are
    # both legal inputs to the property; the empty case degenerates to
    # "ensure_indexes_if_resuming is a no-op when all indexes already
    # exist", which is still a valid behavior to assert.
    with mysql_engine.begin() as conn:
        for index_name in to_drop:
            conn.exec_driver_sql(
                f"DROP INDEX {index_name} ON property"
            )

    # Sanity-check the setup: the table must be missing precisely the
    # subset we dropped. If this assertion ever fires, the test bug is
    # in _restore_all_indexes / _existing_indexes, not in the code under
    # test.
    before = _existing_indexes(mysql_engine)
    assert to_drop.isdisjoint(before), (
        "Test setup failure: indexes expected absent are still present: "
        f"{sorted(to_drop & before)}"
    )

    # Invoke the method under test on a state that matches Requirement
    # 8.8's precondition (``replication_in_progress=true``).
    loader = BulkLoader(mysql_engine)
    loader.ensure_indexes_if_resuming(
        SyncState(replication_in_progress=True)
    )

    # Property 22 assertion: every one of the 7 required indexes must be
    # present. We check each explicitly rather than comparing sets so
    # the failure message names the specific missing index on a
    # regression.
    after = _existing_indexes(mysql_engine)
    for required in REQUIRED_INDEXES:
        assert required in after, (
            f"Index {required!r} missing after "
            f"ensure_indexes_if_resuming(replication_in_progress=True); "
            f"dropped this example: {sorted(to_drop)}; "
            f"present after repair: {sorted(after)}"
        )
