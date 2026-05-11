"""Integration tests for schema application and bulk-load config error.

Covers Task 7.11 and validates Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6,
8.5, and 8.6 against a real MySQL instance brought up via testcontainers.

Why integration rather than unit:

* 6.1-6.5 are properties of the DDL emitted by ``schema.sql``. Proving them
  requires a live MySQL that actually parses the statements and populates
  ``information_schema`` — stubs cannot verify the InnoDB/utf8mb4/index
  shape the design document commits to.
* 8.6 is a behavioural property of ``LOAD DATA LOCAL INFILE`` rejection.
  The error code translation inside :class:`BulkLoader` only fires when the
  MySQL server issues a real 3948/1148 response, which again cannot be
  faked at the SQLAlchemy layer without losing fidelity.

The module skips cleanly when Docker or the ``testcontainers`` MySQL
package is unavailable, so the suite stays runnable in CI environments
that intentionally disable Docker access. Cold-start time for the MySQL
container is ~30-60 s per test; these are intentionally marked as
integration rather than unit tests and are not part of the fast-feedback
loop.
"""

from __future__ import annotations

import pytest

# Skip the entire module if the testcontainers MySQL extra is not installed.
# ``importorskip`` is the idiomatic pytest pattern for optional dependencies
# and emits a clear skip reason at collection time.
pytest.importorskip("testcontainers.mysql")

# Ping the Docker daemon before we even try to import the container class.
# ``testcontainers`` swallows Docker-unavailable errors only when a container
# is actually started, which would fail mid-test with a confusing message;
# skipping at module scope keeps CI logs clean.
try:
    import docker  # type: ignore[import-not-found]

    docker.from_env().ping()
except Exception:  # pragma: no cover - exercised only when Docker is absent
    pytest.skip("Docker unavailable", allow_module_level=True)

from pathlib import Path

from sqlalchemy import create_engine, inspect
from testcontainers.mysql import MySqlContainer

import trestle_etl
from trestle_etl.errors import BulkLoadConfigError
from trestle_etl.loader.bulk import BulkLoader
from trestle_etl.transformer import PROMOTED_COLUMNS

# The seven secondary indexes mandated by Requirement 6.5. Kept here as a
# module-level set (rather than imported from the loader) so the test acts
# as an independent check on the schema file: if a future refactor of
# ``_SECONDARY_INDEXES`` accidentally drops an entry, this test will still
# fail against the schema, preserving the contract.
EXPECTED_INDEXES = frozenset(
    {
        "idx_property_modts",
        "idx_property_status",
        "idx_property_type",
        "idx_property_city",
        "idx_property_postal",
        "idx_property_price",
        "idx_property_state",
    }
)

# Resolve schema.sql via the installed package so the test works regardless
# of pytest's working directory; relying on a relative ``Path("trestle_etl/
# sql/...")`` would break when pytest is invoked from a subdirectory.
_SCHEMA_PATH = Path(trestle_etl.__file__).parent / "sql" / "schema.sql"


def _apply_schema(engine) -> None:
    """Apply ``schema.sql`` statement-by-statement against ``engine``.

    MySQL's ``LOAD DATA`` driver path accepts multi-statement SQL only when
    ``client_flag=MULTI_STATEMENTS`` is enabled on the connection. Rather
    than wire that up, we split on ``;`` and execute each non-empty,
    non-comment statement individually, which is sufficient for this
    DDL-only file.
    """
    schema_sql = _SCHEMA_PATH.read_text()
    with engine.begin() as conn:
        for stmt in schema_sql.split(";"):
            s = stmt.strip()
            if s and not s.startswith("--"):
                conn.exec_driver_sql(s)


def test_schema_applies_and_columns_present():
    """Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6.

    Applies ``schema.sql`` against a fresh MySQL 8 container and
    introspects ``information_schema`` to assert that every
    Promoted_Column, every required secondary index, the primary key, the
    InnoDB engine, and the utf8mb4 collation are all present as the design
    document specifies. Running against a real server is what makes this
    a contract test rather than a textual check on the SQL file.
    """
    with MySqlContainer("mysql:8.0") as mysql:
        engine = create_engine(mysql.get_connection_url())
        try:
            _apply_schema(engine)
            inspector = inspect(engine)

            # Req 6.3: every Promoted_Column present as a typed column.
            cols = {c["name"] for c in inspector.get_columns("property")}
            missing = [c for c in PROMOTED_COLUMNS if c not in cols]
            assert not missing, f"Promoted columns missing: {missing}"

            # Req 6.4 and 6.6: raw_data (JSON) and loaded_at (DATETIME)
            # are present alongside the Promoted_Columns.
            assert "raw_data" in cols, "raw_data column missing"
            assert "loaded_at" in cols, "loaded_at column missing"

            # Req 6.5: all seven secondary indexes are present. We use
            # subset rather than equality because MySQL reports the
            # PRIMARY index separately (via get_pk_constraint), so
            # get_indexes returns exactly the seven secondaries we expect.
            index_names = {
                i["name"] for i in inspector.get_indexes("property") if i.get("name")
            }
            assert EXPECTED_INDEXES.issubset(index_names), (
                f"Missing indexes: {EXPECTED_INDEXES - index_names}"
            )

            # Req 6.2: ListingKey is the sole primary-key column.
            pk = inspector.get_pk_constraint("property")
            assert pk["constrained_columns"] == ["ListingKey"], (
                f"Expected PK on ListingKey, got {pk['constrained_columns']}"
            )

            # Req 6.1: InnoDB engine and utf8mb4 collation. We read
            # straight from information_schema.TABLES because SQLAlchemy's
            # inspector doesn't surface engine/collation uniformly across
            # dialects.
            with engine.connect() as conn:
                row = conn.exec_driver_sql(
                    "SELECT ENGINE, TABLE_COLLATION "
                    "FROM information_schema.TABLES "
                    "WHERE TABLE_NAME='property'"
                ).fetchone()
            assert row is not None, "property table not registered"
            assert row[0] == "InnoDB", f"Expected InnoDB engine, got {row[0]}"
            assert "utf8mb4" in row[1], (
                f"Expected utf8mb4 collation, got {row[1]}"
            )
        finally:
            engine.dispose()


def test_bulk_load_config_error_when_local_infile_disabled():
    """Validates: Requirements 8.5, 8.6.

    Brings up MySQL with ``--local-infile=OFF`` on the server and a client
    engine that does NOT pass ``local_infile=True`` in ``connect_args``.
    Both sides of the toggle are disabled, guaranteeing that any attempt
    to run ``LOAD DATA LOCAL INFILE`` is rejected. The loader must
    translate the driver-level rejection into a
    :class:`BulkLoadConfigError` whose message names both
    ``local_infile=1`` (server) and ``local_infile=True`` (client) so the
    operator sees both remediation steps in one place.
    """
    # ``with_command`` replaces the image's CMD. The mysql:8.0 entrypoint
    # treats the first argument as the program name, so passing
    # ``mysqld --local-infile=OFF`` starts the server with local-infile
    # explicitly disabled. (MySQL 8 defaults to OFF, but being explicit
    # documents the test's intent.)
    with MySqlContainer("mysql:8.0").with_command(
        "mysqld --local-infile=OFF"
    ) as mysql:
        # Deliberately omit ``connect_args={"local_infile": True}`` so
        # both sides of the toggle are off. The loader's error translator
        # catches the resulting 1148/3948/2068 driver error regardless of
        # which side actually rejected the load.
        engine = create_engine(mysql.get_connection_url())
        try:
            _apply_schema(engine)

            # Build a minimal but schema-valid row: ListingKey filled,
            # every other promoted column null, empty JSON for raw_data.
            # The BulkLoader needs the tuple to have len(PROMOTED_COLUMNS)
            # entries (it indexes into it by position when building the
            # CSV) so we pad with None.
            promoted = tuple(["K1"] + [None] * (len(PROMOTED_COLUMNS) - 1))
            rows = [(promoted, "{}")]

            loader = BulkLoader(engine)
            with pytest.raises(BulkLoadConfigError) as excinfo:
                loader.write_batch(rows)

            msg = str(excinfo.value)
            # Req 8.6: both the server setting and the client setting
            # must appear verbatim in the message. If either is missing
            # the operator would have to guess where to look for the fix.
            assert "local_infile=1" in msg, (
                f"Error message missing server setting (local_infile=1): {msg}"
            )
            assert "local_infile=True" in msg, (
                f"Error message missing client setting (local_infile=True): {msg}"
            )
        finally:
            engine.dispose()
