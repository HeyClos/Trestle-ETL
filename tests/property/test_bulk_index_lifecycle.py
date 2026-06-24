"""Property-based test for ``BulkLoader`` secondary-index lifecycle.

Property 21 (design.md): For any full-sync run that completes
successfully, the set of secondary indexes on the ``property`` table
before the run equals the set after the run; during the run (between
construction and close of the :class:`~trestle_etl.loader.bulk.BulkLoader`)
none of the 7 required secondary indexes exist.

**Validates: Requirements 8.7**

Why this reads more like an integration check than a Hypothesis
property-based test:

    Property 21 is *universally and deterministically* quantified over
    "any full-sync run that completes successfully". Once the
    pre-conditions hold (schema applied, fresh sync, close called), the
    post-condition is strictly determined by the loader's DDL behavior.
    There is no meaningful input space to sample over — only a single
    observable run. The design's testing strategy treats a property as
    "a universal invariant we verify against real execution", which is
    what this file does: we observe the invariant holding against real
    MySQL DDL operations via testcontainers. Property 22 (Task 7.10)
    will cover the parameterized case where subsets of the 7 indexes
    are missing at startup, which is the genuinely multi-input space.

Docker / testcontainers skip semantics:

    The three upfront guards skip the module cleanly when any of the
    following is true:

    1. ``testcontainers.mysql`` is not importable (the test environment
       doesn't have the optional dev dependency installed).
    2. The ``docker`` Python SDK is not importable (same story).
    3. A Docker daemon is not reachable from this process — either the
       socket doesn't exist, the user isn't in the ``docker`` group, or
       the daemon is stopped. ``docker.from_env()`` itself can raise in
       this case (not just ``ping()``), so we wrap the entire probe.

    Skipping at import time via ``allow_module_level=True`` keeps CI
    matrices that lack Docker from failing the property-test phase; the
    integration tests covered by Task 7.11 make the same trade-off.
"""

from __future__ import annotations

import pytest

# Skip the whole module cleanly if the testcontainers dev extra is not
# installed. ``importorskip`` is preferable to a bare ``import`` + skip:
# it reports the missing dependency in the pytest summary.
pytest.importorskip("testcontainers.mysql")

# Importing the ``docker`` SDK is also optional — it's a transitive
# dependency of ``testcontainers`` in practice, but we guard anyway so
# that any future packaging change that decouples them doesn't break
# this file.
docker = pytest.importorskip("docker")

# Probe the Docker daemon. ``docker.from_env()`` raises
# ``DockerException`` when the socket is missing (the common laptop
# case), so the broad ``Exception`` catch is intentional: we want to
# skip under any connectivity failure mode, not just the one ``ping()``
# reports.
try:
    docker.from_env().ping()
except Exception:  # pragma: no cover - environment-dependent skip path
    pytest.skip("Docker daemon not reachable", allow_module_level=True)

from pathlib import Path

from sqlalchemy import create_engine, inspect
from testcontainers.mysql import MySqlContainer

from trestle_etl.loader.bulk import BulkLoader
from trestle_etl.state import SyncState
from trestle_etl.transformer import PROMOTED_COLUMNS


# The secondary indexes enumerated by Requirement 6.5: one per non-PK
# Promoted_Column. Derived from PROMOTED_COLUMNS with the same
# ``idx_property_<Column>`` convention as schema.sql and the loader, so a
# regression that renamed or dropped an index surfaces here as a failure.
REQUIRED_INDEXES = frozenset(
    f"idx_property_{col}" for col in PROMOTED_COLUMNS if col != "ListingKey"
)


def _current_index_names(engine) -> set[str]:
    """Return the names of every secondary index currently on ``property``.

    A fresh ``inspect(engine)`` call is issued on every invocation so
    the result reflects the live ``information_schema`` state rather
    than any cached reflection snapshot. This matches how
    :func:`trestle_etl.loader.bulk._existing_index_names` probes the
    table, which means any caching bug in that path would surface here
    as an equality failure rather than a silent pass.
    """
    return {
        idx["name"]
        for idx in inspect(engine).get_indexes("property")
        if idx.get("name")
    }


@pytest.fixture(scope="module")
def mysql_engine():
    """Spin up a MySQL 8 container with the project schema applied.

    Scoped ``module`` because container startup is expensive (tens of
    seconds) and the single test in this file only needs a clean table
    once. ``local_infile=1`` on the container and ``local_infile=True``
    on the client engine satisfy Requirement 8.5 so that any future
    test in this module that exercises ``write_batch`` doesn't have to
    reconfigure the container.
    """
    # Enabling ``local_infile`` on the server via a command-line flag is
    # the simplest way to match the project's documented runtime
    # requirement (see README / Requirement 8.5). It also keeps the
    # container config in one place rather than scattered across a
    # separate my.cnf file.
    with MySqlContainer("mysql:8.0").with_command(
        "--local-infile=1"
    ) as mysql:
        engine = create_engine(
            mysql.get_connection_url(),
            connect_args={"local_infile": True},
        )
        schema_sql = (
            Path(__file__).resolve().parents[2]
            / "trestle_etl"
            / "sql"
            / "schema.sql"
        ).read_text()
        # ``schema.sql`` is a multi-statement file; MySQL's DBAPI
        # driver (pymysql) rejects multiple statements in a single
        # ``execute()`` call, so we split on ``;`` and issue each
        # non-comment, non-empty fragment individually. A proper SQL
        # parser would be overkill: our schema uses plain ASCII and
        # no embedded semicolons.
        with engine.begin() as conn:
            for stmt in schema_sql.split(";"):
                cleaned = stmt.strip()
                if not cleaned:
                    continue
                # Drop pure-comment fragments. Comments mid-statement
                # are tolerated by MySQL; only whole-fragment comments
                # need to be filtered here.
                if all(
                    line.strip().startswith("--") or not line.strip()
                    for line in cleaned.splitlines()
                ):
                    continue
                conn.exec_driver_sql(cleaned)
        try:
            yield engine
        finally:
            engine.dispose()


def test_bulk_index_lifecycle_fresh_full_sync(mysql_engine) -> None:
    """Drop-then-recreate cycle restores the original secondary-index set.

    The property under test breaks into three observable moments:

    1. **Before**: the schema applied above established every required
       index. We assert the 7-name subset is present rather than set
       equality because MySQL may add implicit indexes (e.g. for the
       PK) whose presence is implementation-detail noise.
    2. **During**: immediately after
       ``drop_secondary_indexes_if_fresh_full_sync`` returns, none of
       the 7 names may be present. ``replication_in_progress=False``
       makes the call a real drop rather than the no-op branch used
       for resume runs.
    3. **After**: ``close()`` must recreate every one of the 7 indexes.

    The final assertion anchors Property 21's "set equality" clause:
    restricting each snapshot to ``REQUIRED_INDEXES`` before comparison
    excludes the implicit-index noise mentioned above while still
    enforcing that the full 7-index contract round-trips.
    """
    before = _current_index_names(mysql_engine)
    assert REQUIRED_INDEXES.issubset(before), (
        f"Schema setup did not create every required index; "
        f"missing: {REQUIRED_INDEXES - before}"
    )

    loader = BulkLoader(mysql_engine)

    # Simulate a fresh full-sync startup: ``replication_in_progress``
    # is False, so the loader must drop the 7 secondary indexes.
    loader.drop_secondary_indexes_if_fresh_full_sync(
        SyncState(replication_in_progress=False)
    )

    during = _current_index_names(mysql_engine)
    leaked = during & REQUIRED_INDEXES
    assert not leaked, (
        f"BulkLoader left required indexes in place during the run: {leaked}"
    )

    # ``close()`` is the orchestrator's finally-block contract: whether
    # the run succeeded or raised, every dropped index must be back.
    loader.close()

    after = _current_index_names(mysql_engine)
    assert REQUIRED_INDEXES.issubset(after), (
        f"BulkLoader.close() did not recreate every required index; "
        f"missing: {REQUIRED_INDEXES - after}"
    )

    # Set equality on the required-index slice: before == after when
    # both are projected onto the 7-name subset enumerated by Req 6.5.
    # Projection avoids false failures from MySQL-internal indexes
    # while still proving the full-sync lifecycle preserved the
    # required-index set exactly.
    assert (before & REQUIRED_INDEXES) == (after & REQUIRED_INDEXES)
