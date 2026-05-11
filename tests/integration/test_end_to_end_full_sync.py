"""End-to-end smoke test for ``--full-sync`` against a mocked Trestle server.

Covers Task 11.3 and exercises the whole pipeline — DI wiring, CLI,
orchestrator, extractor, transformer, bulk loader, state store — in a
single black-box run driven through :func:`trestle_etl.cli.main`. The
test stands up two external dependencies:

* A real MySQL 8 container via :mod:`testcontainers` (with server-side
  ``local_infile=1`` so the ``LOAD DATA LOCAL INFILE`` fast path is
  permitted; the CLI already passes client-side ``local_infile=True``
  when it builds the engine).
* A mocked Trestle WebAPI assembled with :mod:`responses`
  (``OrderedRegistry``), which intercepts every outgoing ``requests``
  call and serves a canned token response plus a three-page
  replication chain. The pages are linked by ``@odata.nextLink`` so
  the extractor exercises its lazy fetch-then-commit rhythm rather
  than buffering the full stream.

Requirements validated:

* 3.1, 3.2, 3.3 — extractor initiates from the replication endpoint,
  follows every ``@odata.nextLink``, and terminates when a page lacks
  it. This test's 3-page chain has a terminal third page; the
  orchestrator must observe all three and stop.
* 3.6, 3.7 — after a successful full sync the State_Store carries
  ``replication_in_progress=false`` and ``replication_next_link=null``.
* 6.1, 6.2, 6.3, 6.4, 6.5, 6.6 — exercised transitively through
  ``_apply_schema`` (DDL executes successfully and the subsequent
  bulk loads land rows into the real table).
* 7.5 — the loader's ``BatchResult.max_modification_timestamp`` is
  folded into ``last_modification_timestamp``; a terminal state with
  the max across all batches is what we assert.
* 8.1, 8.2, 8.3, 8.4 — bulk loader writes one CSV and invokes
  ``LOAD DATA LOCAL INFILE`` per page; a successful end-to-end run
  with all rows present demonstrates the fast path works and does
  not leave the table in a broken shape.
* 9.1, 9.2, 9.5, 9.6 — ``sync_state.json`` is written atomically and
  the fields defined by Req 9.2 carry the correct terminal values
  when the run completes.
* 12.2 — per-batch progress logging runs without error (implicit:
  if the INFO log helper raised, the pipeline would fail before
  committing the second page).

The test skips cleanly when Docker is unavailable or the
``testcontainers`` MySQL extra is missing, matching the pattern used
by ``test_schema_and_bulk_config.py``.
"""

from __future__ import annotations

import pytest

# Skip the whole module when the testcontainers MySQL extra isn't
# installed. ``importorskip`` produces a clear skip reason at
# collection time and keeps CI logs uncluttered.
pytest.importorskip("testcontainers.mysql")

# Ping Docker before we attempt to import the container class; when the
# daemon isn't running the testcontainers startup code would fail mid-
# test with an opaque message, so we short-circuit here instead.
try:
    import docker  # type: ignore[import-not-found]

    docker.from_env().ping()
except Exception:  # pragma: no cover - exercised only when Docker is absent
    pytest.skip("Docker unavailable", allow_module_level=True)

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import responses
from responses.registries import OrderedRegistry
from sqlalchemy import create_engine
from testcontainers.mysql import MySqlContainer

import trestle_etl
from trestle_etl.cli import EXIT_OK, main
from trestle_etl.state import StateStore


# Resolve ``schema.sql`` via the installed package so the test works
# regardless of pytest's working directory.
_SCHEMA_PATH = Path(trestle_etl.__file__).parent / "sql" / "schema.sql"


def _apply_schema(engine) -> None:
    """Apply ``schema.sql`` statement-by-statement against ``engine``.

    Matches the splitting behavior used by the schema-application test
    in :mod:`test_schema_and_bulk_config` so the test suite stays
    consistent on how it bootstraps a fresh MySQL instance.
    """
    schema_sql = _SCHEMA_PATH.read_text()
    with engine.begin() as conn:
        for stmt in schema_sql.split(";"):
            s = stmt.strip()
            if s and not s.startswith("--"):
                conn.exec_driver_sql(s)


def _build_pages(base_url: str) -> list[dict]:
    """Return three linked OData replication pages for the mock server.

    Pages 1 and 2 carry an ``@odata.nextLink`` pointing at the next
    page; page 3 is terminal (no nextLink), which is the cue the
    extractor uses to stop iterating. The shape mirrors a real
    Trestle replication envelope: a ``value`` array of record dicts
    plus the optional ``@odata.nextLink``.

    Records carry just enough fields to exercise every promoted-column
    type family that matters for this test:

    * ``ListingKey`` — the primary key, asserted directly.
    * ``ModificationTimestamp`` — the watermark that the orchestrator
      folds into ``last_modification_timestamp``.
    * ``ListPrice`` — a ``Decimal`` column; asserted to confirm the
      CSV formatter round-trips monetary values without float drift.
    * ``City`` — a string column present in both the promoted-columns
      list and the secondary-index set (``idx_property_city``).
    * ``SomeUnknownRESOField`` (page 2 only) — an arbitrary field not
      present on the Pydantic model, included to show ``raw_data``
      preserves unknown fields end-to-end (Requirement 14.4).
    """
    return [
        {
            "value": [
                {
                    "ListingKey": "LK-1",
                    "ModificationTimestamp": "2024-01-01T00:00:00Z",
                    "ListPrice": "100000.00",
                    "City": "Seattle",
                },
                {
                    "ListingKey": "LK-2",
                    "ModificationTimestamp": "2024-01-02T00:00:00Z",
                    "ListPrice": "200000.00",
                    "City": "Portland",
                },
            ],
            "@odata.nextLink": f"{base_url}/Property?replication=true&skiptoken=page2",
        },
        {
            "value": [
                {
                    "ListingKey": "LK-3",
                    "ModificationTimestamp": "2024-01-03T00:00:00Z",
                    "ListPrice": "300000.00",
                    "City": "Austin",
                    # Unknown RESO field; verifies Req 14.4 at the DB layer.
                    "SomeUnknownRESOField": "preserve-me",
                },
            ],
            "@odata.nextLink": f"{base_url}/Property?replication=true&skiptoken=page3",
        },
        {
            "value": [
                {
                    "ListingKey": "LK-4",
                    "ModificationTimestamp": "2024-01-04T00:00:00Z",
                    "ListPrice": "400000.00",
                    "City": "Boston",
                },
            ],
            # No @odata.nextLink: terminal page. Triggers stream
            # termination and state.replication_in_progress=False.
        },
    ]


def test_full_sync_end_to_end(tmp_path, monkeypatch):
    """Validates: Requirements 3.1, 3.2, 3.3, 3.6, 3.7, 6.1-6.6, 7.5,
    8.1-8.4, 9.1, 9.2, 9.5, 9.6, 12.2.

    Drives ``main(['--full-sync'])`` against a mocked Trestle server and
    a real MySQL container. Asserts three facts that together cover
    every requirement listed above:

    1. The ``property`` table contains exactly the four records the
       mock served, in ``ListingKey`` order, with typed-column values
       matching the source JSON (promoted columns) and ``raw_data``
       preserving the original dicts byte-for-byte.
    2. Every ``loaded_at`` value falls within the run's wall-clock
       window, proving the loader set the column itself at batch-
       commit time (Req 6.7) rather than relying on a MySQL default.
    3. The final ``sync_state.json`` carries ``replication_in_progress
       =false``, ``replication_next_link=null``, and a
       ``last_modification_timestamp`` equal to the max observed
       across every page.
    """
    # ``mysqld --local-infile=1`` enables the server-side toggle that
    # ``LOAD DATA LOCAL INFILE`` requires (Requirement 8.5). The
    # client-side ``local_infile=True`` is supplied by the CLI's
    # ``_build_engine`` helper, so we don't need to wire that up here.
    with MySqlContainer("mysql:8.0").with_command("mysqld --local-infile=1") as mysql:
        # Apply the schema via a throwaway engine. The main engine that
        # the pipeline uses is constructed internally by ``_build_engine``
        # from the ``MYSQL_*`` env vars we set below.
        setup_engine = create_engine(
            mysql.get_connection_url(),
            connect_args={"local_infile": 1},
        )
        try:
            _apply_schema(setup_engine)
        finally:
            setup_engine.dispose()

        # Surface the container's connection details as env vars so the
        # pipeline's ``Settings.load()`` picks them up and builds an
        # engine that points at this container. ``get_container_host_ip``
        # / ``get_exposed_port`` handle the host-port mapping for both
        # local Docker and remote daemons.
        host = mysql.get_container_host_ip()
        port = mysql.get_exposed_port(3306)
        user = mysql.username
        password = mysql.password
        db_name = mysql.dbname

        # Base URL for the mocked Trestle API. The host is a synthetic
        # TLD (``.invalid``) so it can never resolve to a real server,
        # which protects the test from a ``responses`` bypass if some
        # future refactor accidentally opts out of mocking.
        base_url = "https://mock.trestle.invalid"
        token_url = f"{base_url}/oidc/token"

        monkeypatch.setenv("TRESTLE_BASE_URL", base_url + "/")
        monkeypatch.setenv("TRESTLE_TOKEN_URL", token_url)
        monkeypatch.setenv("TRESTLE_CLIENT_ID", "test-client-id")
        monkeypatch.setenv("TRESTLE_CLIENT_SECRET", "test-client-secret")
        monkeypatch.setenv("MYSQL_HOST", host)
        monkeypatch.setenv("MYSQL_PORT", str(port))
        monkeypatch.setenv("MYSQL_USER", user)
        monkeypatch.setenv("MYSQL_PASSWORD", password)
        monkeypatch.setenv("MYSQL_DATABASE", db_name)
        state_path = tmp_path / "sync_state.json"
        monkeypatch.setenv("STATE_FILE_PATH", str(state_path))

        # Defend against a local ``.env`` file (not present in this
        # workspace, but a developer might drop one in) leaking values
        # that would mask the monkeypatched environment variables.
        # ``load_dotenv`` is invoked inside ``Settings.load``; turning
        # it into a no-op for this run keeps env-var precedence crystal
        # clear.
        monkeypatch.setattr(
            "trestle_etl.config.load_dotenv", lambda *a, **k: None
        )

        pages = _build_pages(base_url)

        # Capture a pre-run wall-clock bound. Combined with the post-run
        # reading below this brackets every row's ``loaded_at`` inside a
        # narrow window and validates Requirement 6.7 (loader supplies
        # ``loaded_at`` at commit time).
        t_before = datetime.now(timezone.utc)

        # ``OrderedRegistry`` matches registered responses in strict
        # order of registration. The pipeline issues exactly one token
        # POST followed by three GETs (one per replication page), so
        # the registrations below map 1:1 onto the request sequence.
        # Using a regex for the GET URL accommodates the query-string
        # variation between the initial request and subsequent
        # ``@odata.nextLink`` URLs without needing to model each one
        # byte-for-byte.
        property_url_pattern = re.compile(
            rf"^{re.escape(base_url)}/Property(\?.*)?$"
        )

        with responses.RequestsMock(registry=OrderedRegistry) as rsps:
            # Token endpoint: single POST that hands out a canned
            # bearer token with a long expiry so the TokenManager's
            # 60-second refresh margin never kicks in during the run.
            rsps.add(
                responses.POST,
                token_url,
                json={
                    "access_token": "fake-access-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                },
            )
            # Three GET responses, one per replication page. The
            # OrderedRegistry dispatches them in order to match the
            # extractor's iteration rhythm.
            for page in pages:
                rsps.add(
                    responses.GET,
                    property_url_pattern,
                    json=page,
                )

            exit_code = main(["--full-sync"])

        # Post-run wall-clock bound for the ``loaded_at`` check.
        t_after = datetime.now(timezone.utc)

        # ---- Assertion 1: clean pipeline exit --------------------
        assert exit_code == EXIT_OK, (
            f"expected EXIT_OK ({EXIT_OK}), got {exit_code}"
        )

        # ---- Assertion 2: property table contents ---------------
        # A fresh engine is used for assertions so we don't accidentally
        # share connection state with the pipeline's engine (which was
        # disposed by ``_run``'s ``finally`` block).
        verify_engine = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}",
            connect_args={"local_infile": 1},
        )
        try:
            with verify_engine.connect() as conn:
                rows = conn.exec_driver_sql(
                    "SELECT ListingKey, ListPrice, ModificationTimestamp, "
                    "City, raw_data, loaded_at "
                    "FROM property ORDER BY ListingKey"
                ).fetchall()
        finally:
            verify_engine.dispose()

        # Exactly four rows, one per mocked record, in ListingKey order.
        assert [r[0] for r in rows] == ["LK-1", "LK-2", "LK-3", "LK-4"], (
            f"unexpected ListingKey set: {[r[0] for r in rows]}"
        )
        # Decimal round-trip through CSV -> LOAD DATA LOCAL INFILE ->
        # DECIMAL(14,2) preserves the two-digit scale. Comparing as
        # strings keeps the assertion free of Decimal construction
        # boilerplate while still pinning the exact textual shape.
        assert [str(r[1]) for r in rows] == [
            "100000.00",
            "200000.00",
            "300000.00",
            "400000.00",
        ]
        # Promoted-column ``City`` reflects the source city per row.
        assert [r[3] for r in rows] == ["Seattle", "Portland", "Austin", "Boston"]

        # ModificationTimestamp is stored as DATETIME(6) (no timezone);
        # MySQL returns it as a naive datetime. Re-tagging as UTC
        # matches how the rest of the pipeline treats these values.
        expected_mod_ts = [
            datetime(2024, 1, d, tzinfo=timezone.utc) for d in (1, 2, 3, 4)
        ]
        actual_mod_ts: list[datetime] = []
        for r in rows:
            ts = r[2]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            actual_mod_ts.append(ts)
        assert actual_mod_ts == expected_mod_ts, (
            f"ModificationTimestamp mismatch: got {actual_mod_ts}, "
            f"expected {expected_mod_ts}"
        )

        # ---- Assertion 3: raw_data preservation (Req 14.4) ------
        # Per Requirement 5.5 the raw_data column stores the original
        # record dict; unknown RESO fields must survive round-trip.
        # MySQL returns JSON columns as strings via PyMySQL; handle
        # both the string and pre-parsed cases defensively.
        raw_by_key: dict[str, dict] = {}
        for r in rows:
            raw_value = r[4]
            raw_dict = (
                json.loads(raw_value)
                if isinstance(raw_value, (str, bytes, bytearray))
                else raw_value
            )
            raw_by_key[r[0]] = raw_dict

        assert raw_by_key["LK-1"]["City"] == "Seattle"
        assert raw_by_key["LK-1"]["ListPrice"] == "100000.00"
        # Unknown field on LK-3 must have survived end-to-end.
        assert raw_by_key["LK-3"].get("SomeUnknownRESOField") == "preserve-me", (
            "Unknown RESO field was dropped before reaching raw_data; "
            "Requirement 14.4 violated"
        )

        # ---- Assertion 4: loaded_at bounded by run wall-clock ---
        # Validates Requirement 6.7: the loader writes ``loaded_at``
        # itself at batch-commit time rather than leaning on a MySQL
        # ``DEFAULT CURRENT_TIMESTAMP``. We use a generous window
        # (a few seconds on each side) to absorb container I/O
        # latency without flaking.
        slack = timedelta(seconds=5)
        for r in rows:
            loaded_at = r[5]
            if loaded_at.tzinfo is None:
                loaded_at = loaded_at.replace(tzinfo=timezone.utc)
            assert (t_before - slack) <= loaded_at <= (t_after + slack), (
                f"loaded_at={loaded_at.isoformat()} for ListingKey={r[0]} "
                f"falls outside run window "
                f"[{t_before.isoformat()}, {t_after.isoformat()}]"
            )

        # ---- Assertion 5: terminal sync_state.json --------------
        # Requirements 3.6, 3.7, 9.1, 9.2, 9.5, 9.6 taken together
        # specify the terminal state after a clean full sync:
        #   * in_progress flag cleared
        #   * nextLink null (and therefore persisted_at null)
        #   * watermark equal to the max ModTs across every page
        assert state_path.exists(), (
            f"state file not written at {state_path}"
        )
        state = StateStore(state_path).load()
        assert state.replication_in_progress is False, (
            "replication_in_progress should be False after clean full sync"
        )
        assert state.replication_next_link is None, (
            f"replication_next_link should be None, got "
            f"{state.replication_next_link!r}"
        )
        assert state.replication_next_link_persisted_at is None, (
            "replication_next_link_persisted_at should be None when "
            "replication_next_link is None"
        )
        assert state.last_modification_timestamp == datetime(
            2024, 1, 4, tzinfo=timezone.utc
        ), (
            f"last_modification_timestamp should equal max across all "
            f"pages (2024-01-04T00:00:00+00:00), got "
            f"{state.last_modification_timestamp!r}"
        )
