"""Property test for transactional rollback preservation (Property 19).

Property 19 (design.md): For any batch whose commit fails (simulated via
an injected failure), both the ``property`` table and ``sync_state.json``
are byte-for-byte identical to their pre-batch state.

**Validates: Requirements 7.4, 9.4, 15.1**

Implementation notes:

* Requires a real MySQL instance via ``testcontainers``. When Docker
  (or testcontainers) is unavailable, the module-scoped fixture calls
  ``pytest.skip`` so the suite stays green on dev machines without a
  running daemon. The skip is proactive: we try to start the container
  and translate any exception into a skip rather than letting the test
  error out.
* Part 1 — database rollback (Requirement 7.4). The ``UpsertLoader``
  wraps its single ``INSERT ... ON DUPLICATE KEY UPDATE`` in
  ``engine.begin()``; that context manager rolls back and re-raises on
  any exception before commit. We inject a failure via a SQLAlchemy
  ``before_cursor_execute`` event listener that raises whenever the
  loader's ``INSERT INTO property`` statement is about to be sent to
  the server. The listener fires after ``BEGIN`` but before any row
  hits the table, so the transaction is purely a rollback.
* Part 2 — state-file preservation (Requirements 9.4, 15.1). The
  orchestrator is the ONLY writer of ``sync_state.json`` and only
  calls ``state_store.save()`` AFTER a batch commits successfully
  (design.md, "Per-batch loop"). At the loader layer this means
  ``UpsertLoader.write_batch`` must not touch the state file at all:
  a failed batch therefore leaves ``sync_state.json`` byte-for-byte
  identical. We assert that invariant directly by writing a realistic
  ``SyncState`` to disk, snapshotting the raw bytes, running the
  failing ``write_batch``, and re-reading. Any new IO against the
  state file -- even a no-op rewrite -- would cause a byte-level
  diff (timestamp formatting, trailing newline, etc.) and fail the
  assertion.
* The ``property`` table is pre-seeded with one fixed row at module
  scope so the pre-batch snapshot is non-empty. If the generated batch
  happens to collide with the seed's ListingKey, a successful
  ``ON DUPLICATE KEY UPDATE`` would mutate the seed row's
  ``loaded_at``; the injected failure must block that too, which this
  test confirms by snapshot comparison.
"""

from __future__ import annotations

import string
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

try:
    from testcontainers.mysql import MySqlContainer
except ImportError:  # pragma: no cover - handled by the fixture skip.
    MySqlContainer = None  # type: ignore[assignment]

from hypothesis import HealthCheck, given, settings, strategies as st
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine

from trestle_etl.loader import Row
from trestle_etl.loader.upsert import UpsertLoader
from trestle_etl.state import StateStore, SyncState
from trestle_etl.transformer import PROMOTED_COLUMNS, to_row, validate


# Path to the canonical schema.sql shipped with the package. The test
# applies it verbatim against the testcontainer so the on-disk schema
# is the thing under test rather than a duplicated DDL literal.
_SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "trestle_etl"
    / "sql"
    / "schema.sql"
)

# Alphabet used for the guaranteed-valid ``ListingKey`` values: letters,
# digits, and the two punctuation characters RESO keys typically use.
_LISTING_KEY_ALPHABET = string.ascii_letters + string.digits + "-_"


def _execute_schema(engine: Engine) -> None:
    """Apply ``schema.sql`` statement by statement against ``engine``.

    The schema file is a sequence of semicolon-terminated statements
    interspersed with ``--`` comment lines. PyMySQL's single-statement
    cursor rejects a chunk that contains ONLY comment lines with
    ``"Query was empty"``; we strip those lines before dispatching.
    Comments embedded in a real statement are left untouched because
    MySQL parses them fine.
    """
    schema_sql = _SCHEMA_PATH.read_text(encoding="utf-8")
    with engine.begin() as conn:
        for raw_stmt in schema_sql.split(";"):
            effective = "\n".join(
                line
                for line in raw_stmt.splitlines()
                if not line.lstrip().startswith("--")
            ).strip()
            if not effective:
                continue
            conn.exec_driver_sql(effective)


def _seed_row(engine: Engine) -> None:
    """Insert one fixed row so pre-batch snapshots are non-empty."""
    seed_raw = {"ListingKey": "SEED-ROW"}
    seed_model = validate(seed_raw)
    assert seed_model is not None, "seed record must validate"
    promoted, raw_data_json = to_row(seed_raw, seed_model)

    columns = (*PROMOTED_COLUMNS, "raw_data", "loaded_at")
    params: dict[str, Any] = dict(zip(PROMOTED_COLUMNS, promoted))
    params["raw_data"] = raw_data_json
    params["loaded_at"] = datetime(2024, 1, 1, tzinfo=timezone.utc)

    insert_sql = (
        f"INSERT INTO property ({', '.join(columns)}) "
        f"VALUES ({', '.join(':' + c for c in columns)})"
    )
    with engine.begin() as conn:
        conn.execute(text(insert_sql), params)


@pytest.fixture(scope="module")
def mysql_engine() -> Engine:
    """Module-scoped MySQL engine backed by ``testcontainers``.

    Yields a live engine with the canonical schema applied and one
    seed row present. Skips the entire module -- cleanly, without
    raising -- when Docker or the testcontainers package is
    unavailable. Every failure mode from the container (daemon down,
    image pull blocked, port bind conflict) is translated into a
    ``pytest.skip`` so CI or dev runs on Docker-less machines stay
    green.
    """
    if MySqlContainer is None:
        pytest.skip("testcontainers.mysql is not installed")

    # Construct AND start inside the try: on Docker-less machines
    # ``MySqlContainer(...)`` itself can reach for the Docker daemon
    # (resolving image metadata, for example). Wrapping both steps
    # translates any such failure into a clean skip rather than a
    # fixture ERROR.
    try:
        container = MySqlContainer("mysql:8.0")
        container.start()
    except Exception as exc:
        pytest.skip(f"Docker / MySQL container unavailable: {exc}")

    engine: Engine | None = None
    try:
        url = container.get_connection_url()
        engine = create_engine(url, future=True)
        _execute_schema(engine)
        _seed_row(engine)
        yield engine
    finally:
        if engine is not None:
            engine.dispose()
        try:
            container.stop()
        except Exception:
            # Best-effort teardown; a failure here must not mask the
            # real test outcome.
            pass


@st.composite
def row_batches(
    draw: st.DrawFn,
    *,
    min_size: int = 1,
    max_size: int = 5,
) -> list[Row]:
    """Generate a non-empty batch of ``Row`` values via the transformer.

    Each row is built from a minimal raw record (``{"ListingKey": ...}``)
    by running it through :func:`validate` / :func:`to_row`, so the
    tuple layout matches exactly what the orchestrator produces in
    production. Keys are unique inside a batch: the ``ON DUPLICATE KEY
    UPDATE`` path collapses duplicates silently, which would weaken the
    test by reducing the effective row count.
    """
    keys = draw(
        st.lists(
            st.text(
                alphabet=_LISTING_KEY_ALPHABET,
                min_size=4,
                max_size=32,
            ),
            min_size=min_size,
            max_size=max_size,
            unique=True,
        )
    )
    rows: list[Row] = []
    for key in keys:
        raw = {"ListingKey": key}
        model = validate(raw)
        # The generator guarantees a valid, non-empty ListingKey, so
        # ``validate`` must succeed.
        assert model is not None
        rows.append(to_row(raw, model))
    return rows


def _snapshot_property_table(engine: Engine) -> list[tuple[Any, ...]]:
    """Return a deterministic snapshot of the ``property`` table.

    Columns chosen to cover every row component the loader touches:
    the primary key, the raw-data JSON payload, and the loader-supplied
    ``loaded_at`` stamp. Results are sorted by ``ListingKey`` so two
    snapshots with the same logical contents compare equal regardless
    of MySQL's row ordering.
    """
    with engine.connect() as conn:
        result = conn.exec_driver_sql(
            "SELECT ListingKey, raw_data, loaded_at FROM property "
            "ORDER BY ListingKey"
        )
        return [tuple(row) for row in result.fetchall()]


@given(rows=row_batches())
@settings(
    # 25 examples is enough to exercise the rollback path across a
    # variety of batch shapes without paying testcontainer startup cost
    # per example (the container is module-scoped).
    max_examples=25,
    # Per-example timing varies with MySQL round-trip cost; disable the
    # deadline so a slow Docker host doesn't flake the suite.
    deadline=None,
    suppress_health_check=[
        # The ``mysql_engine`` fixture is module-scoped, so this check
        # won't actually trigger, but suppressing it up front means a
        # future refactor to a function-scoped fixture won't silently
        # start failing Hypothesis examples.
        HealthCheck.function_scoped_fixture,
        HealthCheck.too_slow,
    ],
)
def test_rollback_preserves_database_and_state(
    rows: list[Row],
    mysql_engine: Engine,
) -> None:
    """Property 19 (Requirements 7.4, 9.4, 15.1).

    1. Snapshot the ``property`` table and a real ``sync_state.json``
       written by :class:`StateStore`.
    2. Attach a SQLAlchemy ``before_cursor_execute`` listener that
       raises ``RuntimeError`` as soon as the loader issues
       ``INSERT INTO property``. The error escapes the loader's
       ``engine.begin()`` block, triggering rollback and re-raising.
    3. Assert ``write_batch`` raised.
    4. Assert ``SELECT * FROM property`` is byte-identical to the
       pre-batch snapshot (Requirement 7.4).
    5. Assert ``sync_state.json`` is byte-identical to the pre-batch
       contents (Requirement 9.4 / 15.1). ``UpsertLoader`` must not
       touch the state file even transiently.
    """
    # --- Snapshot the DB pre-batch ------------------------------------
    pre_db_rows = _snapshot_property_table(mysql_engine)

    with tempfile.TemporaryDirectory() as tmp_dir:
        state_path = Path(tmp_dir) / "sync_state.json"
        store = StateStore(state_path)

        # Write a realistic ``SyncState`` so the byte-level comparison
        # below has non-trivial content. Any stray write by the loader
        # -- even a rewrite of the same values -- would change the
        # bytes and fail the assertion (for instance, a new
        # ``replication_next_link_persisted_at`` would shift the JSON).
        initial_state = SyncState(
            last_modification_timestamp=datetime(
                2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc
            ),
            replication_in_progress=True,
            replication_next_link=(
                "https://example.invalid/Property?$skiptoken=abc123"
            ),
            replication_next_link_persisted_at=datetime(
                2024, 1, 2, 3, 4, 0, tzinfo=timezone.utc
            ),
        )
        store.save(initial_state)
        pre_state_bytes = state_path.read_bytes()

        # --- Inject failure into the engine --------------------------
        def _raise_on_property_insert(
            conn: Any,
            cursor: Any,
            statement: str,
            parameters: Any,
            context: Any,
            executemany: bool,
        ) -> None:
            # Match only the loader's upsert; other statements (the
            # snapshot SELECT, for example) must still succeed so we
            # can read the post-batch state.
            if "INSERT INTO property" in statement:
                raise RuntimeError("Injected commit failure")

        event.listen(
            mysql_engine,
            "before_cursor_execute",
            _raise_on_property_insert,
        )
        try:
            loader = UpsertLoader(mysql_engine)
            # The loader re-raises after the engine.begin() context
            # rolls back; any exception type is acceptable here -- the
            # point is that control does not reach a successful return.
            with pytest.raises(Exception):
                loader.write_batch(rows)
        finally:
            # Always detach the listener so the next Hypothesis example
            # (and any unrelated test) sees a clean engine.
            event.remove(
                mysql_engine,
                "before_cursor_execute",
                _raise_on_property_insert,
            )

        # --- Assert the DB is byte-for-byte unchanged -----------------
        post_db_rows = _snapshot_property_table(mysql_engine)
        assert post_db_rows == pre_db_rows, (
            "Rollback failed: property table diverged from pre-batch "
            "snapshot after a failing write_batch."
        )

        # --- Assert the state file is byte-for-byte unchanged ---------
        # The UpsertLoader must not write to the state store. A byte
        # diff here would imply an unexpected IO path that violates
        # Requirements 9.4 / 15.1.
        assert state_path.read_bytes() == pre_state_bytes, (
            "Rollback failed: sync_state.json diverged from its "
            "pre-batch contents."
        )
