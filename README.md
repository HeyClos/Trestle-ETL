# Trestle ETL Pipeline

## Overview

Trestle ETL is a Python pipeline that extracts `Property` records from Cotality's
Trestle WebAPI (OData v4) and loads them into MySQL. It provides two ingest
paths — a full-sync backfill via the Trestle replication endpoint plus
`LOAD DATA LOCAL INFILE`, and an incremental delta sync via
`$filter=ModificationTimestamp gt <since>` plus
`INSERT ... ON DUPLICATE KEY UPDATE` — and it persists progress to a JSON state
file after every committed batch so that crashes resume cleanly. A single CLI
entry point (`python -m trestle_etl` / `trestle-etl`) drives both operator
runs and scheduled jobs.

## Prerequisites

- **Python 3.11+** (the codebase relies on 3.11-only `datetime.fromisoformat`
  behavior and other 3.11 features).
- **MySQL 8.0+**, InnoDB storage engine, `utf8mb4` character set. The schema
  in `trestle_etl/sql/schema.sql` assumes both.
- **Trestle API credentials** — an OAuth2 `client_id` and `client_secret`
  scoped to `api`, issued by Cotality.
- **Docker** (optional) — only required for the integration tests, which spin
  up MySQL via `testcontainers`.

## Installation

Clone the repo and install into a virtual environment with dev extras:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

This installs runtime dependencies (`requests`, `pydantic>=2`, `sqlalchemy`,
`pymysql`, `python-dotenv`), dev dependencies (`pytest`, `hypothesis`,
`responses`, `freezegun`, `testcontainers`), and registers the `trestle-etl`
console script.

## Configuration

All runtime configuration is read from environment variables. The repository
ships a [`.env.example`](./.env.example) that enumerates every variable the
pipeline reads; copy it and fill in the secret values:

```bash
cp .env.example .env
# edit .env: set TRESTLE_CLIENT_ID, TRESTLE_CLIENT_SECRET, MYSQL_PASSWORD, etc.
```

Do not commit `.env` — it is listed in `.gitignore` and contains secrets.

| Variable | Purpose |
| --- | --- |
| `TRESTLE_BASE_URL` | Trestle WebAPI base URL (e.g. `https://api.cotality.com/trestle/odata/`). |
| `TRESTLE_TOKEN_URL` | OAuth2 token endpoint (e.g. `https://api.cotality.com/trestle/oidc/connect/token`). |
| `TRESTLE_CLIENT_ID` | OAuth2 client ID issued by Cotality. |
| `TRESTLE_CLIENT_SECRET` | OAuth2 client secret issued by Cotality. |
| `MYSQL_HOST` | MySQL hostname or IP. |
| `MYSQL_PORT` | MySQL port (typically `3306`). |
| `MYSQL_USER` | MySQL user with `SELECT`/`INSERT`/`UPDATE`/`ALTER` on the target database. |
| `MYSQL_PASSWORD` | Password for `MYSQL_USER`. |
| `MYSQL_DATABASE` | Target database name (must already exist; the pipeline does not create it). |
| `STATE_FILE_PATH` | Path to the JSON state file (default `sync_state.json`). |
| `PAGE_SIZE` | OData `$top` page size for incremental pulls (default `1000`). |

Missing required values cause the pipeline to exit with a configuration error
that names the missing variable, before any network or database I/O.

## MySQL setup

### 1. Apply the schema

The schema at [`trestle_etl/sql/schema.sql`](./trestle_etl/sql/schema.sql)
creates the `property` table (InnoDB, `utf8mb4`), every RESO Promoted_Column
as a typed column, a `raw_data JSON NOT NULL` column for full-record
preservation, a `loaded_at DATETIME(6) NOT NULL` column, and the seven
secondary indexes (`ModificationTimestamp`, `MlsStatus`, `PropertyType`,
`City`, `PostalCode`, `ListPrice`, `StateOrProvince`).

Apply it once against your target database:

```bash
mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p "$MYSQL_DATABASE" \
  < trestle_etl/sql/schema.sql
```

Alternatively, use the idempotent helper, which reads the same `.env` as the
pipeline:

```bash
python scripts/apply_schema.py
```

To pick up a breaking column change on a database whose data is disposable,
pass `--recreate` to drop and rebuild the `property` table (this destroys all
existing rows):

```bash
python scripts/apply_schema.py --recreate
```

### 2. Enable `LOAD DATA LOCAL INFILE` (required for `--full-sync`)

The full-sync fast path uses `LOAD DATA LOCAL INFILE` to ingest each
replication page as a CSV. MySQL disables this feature by default on both
sides of the connection, so you must configure it in two places:

1. **Server-side:** set `local_infile=1` in the MySQL server configuration
   (e.g. `/etc/mysql/my.cnf` under `[mysqld]`), then restart the server:

   ```ini
   [mysqld]
   local_infile=1
   ```

   Verify with `SHOW GLOBAL VARIABLES LIKE 'local_infile';` — the value must
   be `ON`.

2. **Client-side:** the pipeline already passes `local_infile=True` in its
   `pymysql` connection arguments. If you connect manually with the `mysql`
   CLI for debugging, pass `--local-infile=1`.

If either side is misconfigured, the `BulkLoader` raises `BulkLoadConfigError`
with a message that names both `local_infile=1` (server) and
`local_infile=True` (client), so the failing setting is obvious from the
error alone.

The incremental path (`--incremental` / `--since`) uses plain
`INSERT ... ON DUPLICATE KEY UPDATE` and does not require `local_infile`.

## CLI usage

Invoke the pipeline as a module or via the installed console script. All
examples below assume a populated `.env` in the working directory.

```bash
python -m trestle_etl <flags>
# or, equivalently
trestle-etl <flags>
```

Exactly one of `--full-sync`, `--incremental`, `--since <ISO8601>`, and
`--reconcile` selects the run mode. `--dry-run` is a modifier that may be
combined with any mode except `--reconcile`.

**Initial backfill (~1.6M records)** — full sync via the replication
endpoint, CSV + `LOAD DATA LOCAL INFILE`, with secondary indexes dropped for
ingest and recreated at the end:

```bash
python -m trestle_etl --full-sync
```

**Delta sync** — incremental pull bounded by the state file's
`last_modification_timestamp`:

```bash
python -m trestle_etl --incremental
```

Exits non-zero with a remediation message pointing to `--full-sync` or
`--since` when the state file has no `last_modification_timestamp` yet.

**Custom start timestamp** — incremental run from a supplied UTC timestamp,
overriding whatever the state file holds:

```bash
python -m trestle_etl --since 2024-01-15T00:00:00Z
```

Accepts any ISO 8601 UTC string; trailing `Z` and explicit offsets are both
accepted. Parse failures surface before any HTTP request.

**No-writes preview** — extract and transform but do not touch MySQL or the
state file; combinable with any mode flag except `--reconcile`:

```bash
python -m trestle_etl --dry-run --incremental
```

**Reconcile (placeholder, not implemented)** — reserved for a future audit
mode; exits non-zero with a placeholder message and performs no work:

```bash
python -m trestle_etl --reconcile
```

## Operational notes

**Graceful shutdown.** Ctrl+C (SIGINT) triggers a graceful stop: the in-flight
batch commits, the state file is updated, and the process exits 0 logging the
final `ModificationTimestamp`. A second Ctrl+C exits immediately with code
130 and does not commit the in-flight batch.

**State file.** Progress is written to the JSON file at `STATE_FILE_PATH`
(default `./sync_state.json`) after every batch commit. Writes are atomic
(`<path>.tmp` + `fsync` + `rename`), so a crash never leaves a half-written
file. The document records `last_modification_timestamp` (drives the next
incremental's lower bound), `replication_in_progress`, `replication_next_link`
(the verbatim `@odata.nextLink` of the last committed full-sync page), and
`replication_next_link_persisted_at`.

**Recovery.** On the next start:

- If `replication_in_progress=true` and the saved nextLink was persisted less
  than four minutes ago, the pipeline **resumes** full sync from that link.
- If `replication_in_progress=true` but the link is stale (> 4 minutes), the
  pipeline **pivots** to an incremental run from
  `last_modification_timestamp`.
- Otherwise the run proceeds according to the CLI flag.

If the state file is present but malformed, the pipeline exits non-zero and
does not modify it; back it up, inspect, then repair or delete before
re-running.

## Testing

```bash
pytest -q
```

The suite is split into three tiers under `tests/`:

- `tests/unit/` — fast, no external dependencies.
- `tests/property/` — Hypothesis-driven; each test cites its property number
  and the requirement it validates, with at least 100 examples per property.
- `tests/integration/` — Docker-gated; spins up a real MySQL via
  `testcontainers`. Skipped automatically when Docker is unavailable.

Run only the fast tiers (no Docker required):

```bash
pytest -q tests/unit tests/property
```

Run the integration tier on its own (Docker must be running):

```bash
pytest -q tests/integration
```
