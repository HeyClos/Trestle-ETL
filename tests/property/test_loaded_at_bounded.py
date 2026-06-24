"""Property-based test for ``loaded_at`` bounded by commit wall-clock (Property 16).

Property 16 (design.md): For any batch committed at wall-clock
``t_commit``, every row has ``|loaded_at − t_commit| ≤ δ`` for a small
``δ`` (milliseconds), and no row uses a MySQL ``DEFAULT CURRENT_TIMESTAMP``
value.

**Validates: Requirements 6.7**

The property targets :class:`trestle_etl.loader.upsert.UpsertLoader`,
which per Requirement 6.7 stamps every row with a Python-computed
``datetime.now(UTC)`` at batch-commit time rather than relying on a MySQL
``DEFAULT CURRENT_TIMESTAMP`` clause. The test therefore runs against a
real MySQL instance (started via ``testcontainers``) and asserts two
things:

1. **Wall-clock bounds.** ``t_before = datetime.now(UTC)`` is captured
   immediately before ``write_batch``; ``t_after`` is captured
   immediately after. Every ``loaded_at`` value written into the
   ``property`` table must fall in the closed interval
   ``[t_before, t_after]``.
2. **Single wall-clock per batch.** All rows in a single batch share
   exactly one ``loaded_at`` value. This follows from the loader
   computing ``loaded_at`` once per batch (``loaded_at =
   datetime.now(timezone.utc)`` at the top of ``write_batch``) and is
   the operational signature that the loader — not MySQL — is the
   source of the timestamp.

A separate schema-introspection test asserts that the ``loaded_at``
column has no ``DEFAULT`` clause and no ``on update`` ``EXTRA``
attribute, which is the "no ``DEFAULT CURRENT_TIMESTAMP`` value" clause
of Property 16 enforced at the DDL level.

Implementation notes:

* The module is skipped cleanly when Docker is unavailable so the
  full test suite still runs on machines without a Docker daemon. The
  skip probe is a lightweight ``docker info`` subprocess call to avoid
  pulling the MySQL image on a machine that can't run it anyway.
* The ``mysql_engine`` fixture is module-scoped so a single container
  is reused across all Hypothesis examples. The ``property`` table is
  truncated before each example (inside the test body, after the
  Hypothesis draw) so one example's rows can't leak into the next.
* Hypothesis' ``function_scoped_fixture`` health check is suppressed:
  the fixture is actually module-scoped, but Hypothesis still flags
  pytest fixtures used inside ``@given`` bodies by default and the
  suppression is the standard escape hatch.
* ``max_examples=20`` is a deliberate trade-off: each example does a
  full INSERT round-trip against MySQL, so running the Hypothesis
  default (100) would push the test past a reasonable wall-clock
  budget with no additional coverage — the property under test doesn't
  depend on the shape of the generated rows, only on the fact that a
  batch got written.
* MySQL's ``DATETIME(6)`` returns naive Python ``datetime`` values
  because PyMySQL does not synthesize a ``tzinfo``; the test attaches
  ``UTC`` after the fact so the comparison against tz-aware
  ``t_before`` / ``t_after`` is well-defined (comparing tz-aware to
  naive raises ``TypeError``).
"""

from __future__ import annotations

import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from trestle_etl.loader import Row
from trestle_etl.loader.upsert import UpsertLoader
from trestle_etl.transformer import PROMOTED_COLUMNS


def _docker_available() -> bool:
    """Return ``True`` iff a Docker daemon responds to ``docker info``.

    The property test needs a Docker-hosted MySQL 8 container. Probing
    with a lightweight ``docker info`` subprocess keeps the skip path
    cheap on machines without Docker and avoids dragging in a
    testcontainers ``DockerException`` at collection time. The five
    second timeout is generous for a local-socket call but still bounds
    the worst case when the Docker CLI is present but the daemon is
    hanging.
    """
    if shutil.which("docker") is None:
        return False
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


# Mark every test in the module as skipped (rather than failing at
# collection) when Docker is unavailable. Using ``pytestmark`` keeps
# the tests collectible and reports them as skipped with a clear reason,
# so a developer running the suite without Docker sees ``2 skipped``
# rather than an abrupt module-level error.
pytestmark = pytest.mark.skipif(
    not _docker_available(),
    reason="Docker is not available; skipping MySQL testcontainers tests",
)


# Absolute path to the schema DDL used by the pipeline in production.
# Applying the exact file keeps the test honest: any divergence between
# the ``loaded_at`` column declared in ``schema.sql`` and what the
# loader relies on would surface as an integration-level failure here.
_SCHEMA_SQL_PATH = (
    Path(__file__).resolve().parents[2]
    / "trestle_etl"
    / "sql"
    / "schema.sql"
)


@pytest.fixture(scope="module")
def mysql_engine() -> Any:
    """Start a disposable MySQL 8 container and yield a SQLAlchemy ``Engine``.

    The container is booted once per test module and torn down at
    module teardown; the ``property`` table is truncated per-example
    inside the test body rather than rebuilt per-example so we don't
    pay schema-apply cost 20+ times.
    """
    # Imported lazily so the module still collects cleanly on machines
    # where ``docker`` is present but the daemon is down (and the
    # testcontainers package is technically importable but unusable).
    from testcontainers.mysql import MySqlContainer

    with MySqlContainer("mysql:8.0") as container:
        url = container.get_connection_url()
        # testcontainers returns a ``mysql+pymysql://`` URL when
        # ``pymysql`` is installed; fall through unchanged if it's
        # already the pymysql form.
        if url.startswith("mysql://"):
            url = "mysql+pymysql://" + url[len("mysql://") :]

        engine = create_engine(url, future=True)

        # Apply the production schema. The file contains multiple
        # statements (CREATE TABLE + 7 CREATE INDEX) separated by
        # semicolons; SQLAlchemy Core's ``exec_driver_sql`` forwards
        # one statement at a time to the driver, so split on `;` and
        # skip empty tails.
        schema_sql = _SCHEMA_SQL_PATH.read_text(encoding="utf-8")
        with engine.begin() as conn:
            for statement in schema_sql.split(";"):
                stripped = statement.strip()
                if stripped:
                    conn.exec_driver_sql(stripped)

        try:
            yield engine
        finally:
            engine.dispose()


# ---------------------------------------------------------------------------
# Row generation
# ---------------------------------------------------------------------------
#
# The property under test is about ``loaded_at`` timing, not about the
# shape of the promoted-column values. The generator therefore emits
# minimal rows: every typed column is ``None`` except ``ListingKey``
# (required) and ``ModificationTimestamp`` (tracked by the loader's
# ``BatchResult``). This keeps Hypothesis examples small and focuses
# shrinking on the batch size, which is the only interesting axis for
# a timing property.

_LISTING_KEY_ALPHABET = (
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
)


@st.composite
def rows_batches(draw: st.DrawFn) -> list[Row]:
    """Generate a non-empty batch of :data:`Row` tuples with unique keys.

    Uniqueness on ``ListingKey`` matters because the loader uses
    ``INSERT ... ON DUPLICATE KEY UPDATE``: two rows sharing a key in
    the same batch would still collapse to a single row on commit,
    which would make the "all ``loaded_at`` equal" assertion pass
    trivially for the wrong reason. Enforcing uniqueness in the
    generator keeps the test honest without complicating the
    assertion.
    """
    # Keep batch size small (1..10). The property has nothing to do
    # with batch-size bounds (that's Property 18); we just want a few
    # rows per example to assert "all loaded_at equal" meaningfully.
    n = draw(st.integers(min_value=1, max_value=10))
    keys = draw(
        st.lists(
            st.text(
                alphabet=_LISTING_KEY_ALPHABET, min_size=1, max_size=32
            ),
            min_size=n,
            max_size=n,
            unique=True,
        )
    )

    # Arbitrary but fixed ModificationTimestamp per row. The loader
    # advances ``max_modification_timestamp`` off this value; generating
    # a tz-aware UTC datetime keeps pydantic-style equality downstream.
    mod_ts = draw(
        st.datetimes(
            min_value=datetime(2020, 1, 1),
            max_value=datetime(2030, 1, 1),
        )
    ).replace(tzinfo=timezone.utc)

    rows: list[Row] = []
    for key in keys:
        # Build the promoted-columns tuple in ``PROMOTED_COLUMNS``
        # order with everything-but-ListingKey/ModTs set to ``None``.
        # The tuple layout must match :data:`PROMOTED_COLUMNS` exactly;
        # the loader relies on that ordering when it zips values into
        # bind parameters.
        promoted_values: list[Any] = []
        for col in PROMOTED_COLUMNS:
            if col == "ListingKey":
                promoted_values.append(key)
            elif col == "ModificationTimestamp":
                promoted_values.append(mod_ts)
            else:
                promoted_values.append(None)
        # Minimal valid JSON payload for the NOT NULL ``raw_data``
        # column. The test doesn't care about the payload contents.
        raw_data_json = '{"ListingKey": "%s"}' % key
        rows.append((tuple(promoted_values), raw_data_json))
    return rows


def _truncate_property_table(engine: Engine) -> None:
    """Remove all rows from both data tables between examples.

    ``TRUNCATE TABLE`` is faster than ``DELETE FROM`` on MySQL and
    returns each table to its post-schema-apply state. Both ``property``
    and ``property_raw`` are cleared so the upsert loader (which writes
    both) starts each example from empty.
    """
    with engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE property")
        conn.exec_driver_sql("TRUNCATE TABLE property_raw")


# ---------------------------------------------------------------------------
# Schema-level assertion: no DEFAULT CURRENT_TIMESTAMP on ``loaded_at``
# ---------------------------------------------------------------------------


def test_loaded_at_column_has_no_default(mysql_engine: Engine) -> None:
    """Requirement 6.6/6.7 enforced at the DDL level.

    A MySQL ``DATETIME DEFAULT CURRENT_TIMESTAMP`` column would satisfy
    the wall-clock bound from Property 16 accidentally (the server
    would stamp the row at commit time), which is exactly the
    anti-pattern Requirement 6.7 prohibits. This test introspects
    ``information_schema.COLUMNS`` to confirm the ``loaded_at`` column
    is declared without a ``DEFAULT`` clause and without the
    ``on update CURRENT_TIMESTAMP`` ``EXTRA`` attribute, so the only
    source of the value is the Python-side
    ``datetime.now(UTC)`` in the loader.
    """
    with mysql_engine.connect() as conn:
        result = conn.exec_driver_sql(
            "SELECT COLUMN_DEFAULT, EXTRA "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_NAME='property' AND COLUMN_NAME='loaded_at'"
        ).fetchone()

    assert result is not None, "loaded_at column not found in schema"
    col_default, extra = result

    # ``COLUMN_DEFAULT`` is ``NULL`` when no ``DEFAULT`` clause exists.
    # Some MySQL versions return an empty string for ``NO DEFAULT``; we
    # accept either so the test is not brittle to server version.
    assert col_default is None or col_default == "", (
        f"loaded_at has a DEFAULT clause: {col_default!r}; Requirement 6.7 "
        "forbids this so the Loader is the sole writer of loaded_at."
    )

    # ``EXTRA`` carries flags like ``DEFAULT_GENERATED`` and
    # ``on update CURRENT_TIMESTAMP``. Neither is allowed: both would
    # let MySQL silently overwrite the Python-computed value.
    extra_upper = (extra or "").upper()
    assert "CURRENT_TIMESTAMP" not in extra_upper, (
        f"loaded_at has a CURRENT_TIMESTAMP EXTRA attribute: {extra!r}; "
        "Requirement 6.7 forbids this."
    )
    assert "DEFAULT_GENERATED" not in extra_upper, (
        f"loaded_at is flagged DEFAULT_GENERATED: {extra!r}; "
        "Requirement 6.7 forbids server-side default generation."
    )


# ---------------------------------------------------------------------------
# Property 16: loaded_at bounded by commit wall-clock
# ---------------------------------------------------------------------------


@given(rows=rows_batches())
@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_loaded_at_within_wall_clock_bounds(
    rows: list[Row], mysql_engine: Engine
) -> None:
    """Property 16 (Requirement 6.7).

    For a batch committed between wall-clock ``t_before`` and
    ``t_after``, every ``loaded_at`` value in the committed rows must
    fall in the closed interval ``[t_before, t_after]``, and all
    ``loaded_at`` values in the batch must be equal (single wall-clock
    per batch).
    """
    _truncate_property_table(mysql_engine)

    loader = UpsertLoader(mysql_engine)

    # ``t_before`` and ``t_after`` bracket the entire ``write_batch``
    # call. ``datetime.now(timezone.utc)`` matches what the loader
    # itself calls internally, so the three timestamps are sampled from
    # the same clock and comparison is meaningful.
    t_before = datetime.now(timezone.utc)
    loader.write_batch(rows)
    t_after = datetime.now(timezone.utc)

    with mysql_engine.connect() as conn:
        loaded_at_values = [
            r[0]
            for r in conn.exec_driver_sql(
                "SELECT loaded_at FROM property"
            ).fetchall()
        ]

    # Sanity: we should have exactly ``len(rows)`` rows back, because
    # the generator enforces unique ``ListingKey`` values per batch and
    # the table was truncated above.
    assert len(loaded_at_values) == len(rows), (
        f"expected {len(rows)} rows committed, got {len(loaded_at_values)}"
    )

    # MySQL DATETIME(6) returns naive Python datetimes under pymysql.
    # The column stores UTC because the loader writes UTC; attach
    # ``tzinfo=UTC`` so the interval comparison against tz-aware
    # ``t_before`` / ``t_after`` doesn't raise ``TypeError``.
    loaded_at_values = [
        v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v
        for v in loaded_at_values
    ]

    # Wall-clock bound: every row's ``loaded_at`` falls inside the
    # ``[t_before, t_after]`` window that brackets the write call.
    # Requirement 6.7 permits a small tolerance ``δ``, but because the
    # loader computes ``loaded_at`` *inside* the ``write_batch`` call,
    # the value is always within the bracket; no extra slack needed.
    for la in loaded_at_values:
        assert t_before <= la <= t_after, (
            f"loaded_at {la.isoformat()} not in bracket "
            f"[{t_before.isoformat()}, {t_after.isoformat()}]"
        )

    # Single wall-clock per batch: Requirement 6.7 says the loader
    # stamps every row in the batch with the same ``datetime.now(UTC)``
    # captured at batch-commit time. If MySQL were silently filling
    # ``loaded_at`` via ``DEFAULT CURRENT_TIMESTAMP`` on each row, the
    # values would differ by microseconds across rows; observing a
    # single distinct value confirms the Python-side stamp.
    distinct = set(loaded_at_values)
    assert len(distinct) == 1, (
        f"loaded_at values differ across a single batch: {sorted(distinct)}; "
        "Requirement 6.7 requires a single commit-time wall-clock per batch."
    )
