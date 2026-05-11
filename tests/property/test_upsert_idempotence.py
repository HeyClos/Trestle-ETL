"""Property test for Upsert_Path idempotence (Property 17).

Property 17 (design.md): For any batch of Rows, applying the batch via the
Upsert_Path twice produces the same final database state as applying it
once.

**Validates: Requirements 7.6**

Interpretation of "same final database state"
---------------------------------------------
The ``property`` table carries a ``loaded_at`` column that the loader sets
to ``datetime.now(UTC)`` on every commit (Requirement 6.7). Applying the
same batch twice therefore writes two different ``loaded_at`` values for
every row, which makes the rows byte-for-byte unequal even though the
business-data state is unchanged. We follow the design's interpretation
(Requirement 7.6): idempotence is about the logical state under
``INSERT ... ON DUPLICATE KEY UPDATE`` with identical input values. The
test compares row contents excluding ``loaded_at``.

Why a real MySQL container
--------------------------
``INSERT ... ON DUPLICATE KEY UPDATE`` is MySQL-specific. SQLite and
in-memory fakes cannot exercise the actual upsert semantics the loader
depends on (Requirement 7.1, 7.7). Using
:class:`testcontainers.mysql.MySqlContainer` gives the test a real MySQL
8.0 server with the production schema applied.

Skip behavior when Docker is unavailable
----------------------------------------
CI and developer machines without Docker running cannot execute this
test. We detect that up front and skip the module cleanly rather than
failing with an opaque connection error.

Hypothesis settings
-------------------
* ``max_examples=20`` - each example starts a transaction against the
  containerized MySQL, so keeping the example budget small keeps the
  test bounded in wall-clock time. Twenty examples is sufficient to
  exercise the idempotence invariant across varying batch shapes.
* ``deadline=None`` - MySQL round-trips over the Docker network vary
  too much for Hypothesis's default per-example deadline.
* ``suppress_health_check=[HealthCheck.function_scoped_fixture]`` -
  the MySQL engine fixture is module-scoped, but the table-cleaning
  fixture is function-scoped; Hypothesis emits a health-check warning
  because reusing a function-scoped fixture across examples can mask
  state leaks. We truncate the table at the top of every example so
  the warning is spurious in this case.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

# testcontainers is an optional dev dependency; skip the module cleanly
# when it (or docker-py) is not installed.
pytest.importorskip("testcontainers.mysql")
pytest.importorskip("docker")

import docker  # noqa: E402

# Verify Docker daemon is reachable before attempting to pull images or
# start containers. ``docker.from_env().ping()`` is the canonical health
# check and returns True on success or raises on any connectivity error.
try:
    docker.from_env().ping()
except Exception:  # pragma: no cover - environment-dependent
    pytest.skip("Docker is not available", allow_module_level=True)

from hypothesis import HealthCheck, given, settings as hyp_settings, strategies as st  # noqa: E402
from sqlalchemy import create_engine, text  # noqa: E402
from testcontainers.mysql import MySqlContainer  # noqa: E402

from trestle_etl.loader import Row  # noqa: E402
from trestle_etl.loader.upsert import UpsertLoader  # noqa: E402
from trestle_etl.transformer import PROMOTED_COLUMNS  # noqa: E402


# Alphabet for generated ListingKey values. Stays inside ASCII so the
# primary-key comparison is not tripped by MySQL collation surprises.
_LISTING_KEY_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mysql_engine():
    """Start a MySQL 8.0 container and apply the production schema.

    Module-scoped so the (slow) image pull and server startup happen once
    per test run. The engine is configured with ``local_infile=1`` so it
    can be shared with any future bulk-load tests without changing the
    fixture contract.
    """
    with MySqlContainer("mysql:8.0") as mysql:
        engine = create_engine(
            mysql.get_connection_url(),
            connect_args={"local_infile": 1},
        )

        # ``schema.sql`` is a single DDL script containing multiple
        # statements separated by ``;``. PyMySQL's default cursor does
        # not execute multi-statement scripts, so we split on ``;`` and
        # dispatch each statement individually. Comment lines are
        # skipped so the splitter does not emit empty statements.
        schema_sql = Path("trestle_etl/sql/schema.sql").read_text()
        with engine.begin() as conn:
            for stmt in schema_sql.split(";"):
                cleaned = "\n".join(
                    line
                    for line in stmt.splitlines()
                    if not line.strip().startswith("--")
                ).strip()
                if cleaned:
                    conn.exec_driver_sql(cleaned)

        yield engine
        engine.dispose()


# ---------------------------------------------------------------------------
# Row generator
# ---------------------------------------------------------------------------


@st.composite
def row_batches(
    draw: st.DrawFn, min_size: int = 1, max_size: int = 10
) -> list[Row]:
    """Generate a batch of :data:`Row` tuples with unique ListingKeys.

    Each row carries a ListingKey (required) and leaves all other
    promoted columns as ``None``. That keeps the generator small and
    focuses the property on the upsert state-equivalence invariant
    itself rather than on the typed-column serialization. The
    ``raw_data`` field is a minimal JSON document that includes the
    ListingKey so the MySQL JSON column can validate the payload.

    ListingKeys are drawn with ``unique=True`` because the ``property``
    table's primary-key constraint would otherwise reject duplicates
    within a single batch, masking the property under test.
    """
    n = draw(st.integers(min_value=min_size, max_value=max_size))
    keys = draw(
        st.lists(
            st.text(alphabet=_LISTING_KEY_ALPHABET, min_size=1, max_size=32),
            min_size=n,
            max_size=n,
            unique=True,
        )
    )
    rows: list[Row] = []
    for key in keys:
        # PROMOTED_COLUMNS[0] is ListingKey; everything else is None.
        promoted: tuple[Any, ...] = (key,) + (None,) * (len(PROMOTED_COLUMNS) - 1)
        raw_data = json.dumps({"ListingKey": key})
        rows.append((promoted, raw_data))
    return rows


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


# Columns to compare when asserting state equivalence. We exclude
# ``loaded_at`` because the loader intentionally updates it on every
# commit (Requirement 6.7); its divergence across two applies is
# expected and does not violate logical idempotence (Requirement 7.6).
_COMPARE_COLUMNS = ", ".join((*PROMOTED_COLUMNS, "raw_data"))


@given(rows=row_batches())
@hyp_settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_upsert_idempotence(rows: list[Row], mysql_engine) -> None:
    """Property 17 (Requirement 7.6).

    Applying the same batch through :class:`UpsertLoader` twice must
    leave the ``property`` table in the same logical state as a single
    apply. Because the loader stamps ``loaded_at`` at commit time, that
    single column is expected to change between the two applies and is
    excluded from the comparison. Every other column - including the
    primary key set, every promoted column, and the ``raw_data`` JSON
    payload - must be byte-for-byte identical after the second apply.
    """
    # Fresh table for every Hypothesis example. ``TRUNCATE`` is faster
    # than ``DELETE FROM`` and resets any secondary-index bookkeeping.
    with mysql_engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE property")

    loader = UpsertLoader(mysql_engine, batch_size=len(rows))

    # First apply.
    loader.write_batch(rows)
    with mysql_engine.connect() as conn:
        query = text(
            f"SELECT {_COMPARE_COLUMNS} FROM property ORDER BY ListingKey"
        )
        result1 = [tuple(row) for row in conn.execute(query).fetchall()]

    # Second apply of the exact same batch.
    loader.write_batch(rows)
    with mysql_engine.connect() as conn:
        result2 = [tuple(row) for row in conn.execute(query).fetchall()]

    # Row count stable: the upsert did not insert duplicates on the
    # primary key.
    assert len(result1) == len(result2) == len(rows)

    # Primary-key set stable.
    keys1 = [r[0] for r in result1]
    keys2 = [r[0] for r in result2]
    assert keys1 == keys2

    # Full logical-state equivalence (all promoted columns + raw_data).
    # This is the core idempotence assertion: the second apply leaves
    # every business-data column in exactly the same state as the first.
    assert result1 == result2
