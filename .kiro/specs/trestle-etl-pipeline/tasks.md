# Implementation Plan: Trestle ETL Pipeline

## Overview

This plan builds the Trestle ETL Pipeline incrementally in Python, beginning with project scaffolding and the foundational contracts (config, schema, models), then layering in authentication, HTTP, extraction, transformation, state persistence, loaders (upsert and bulk), orchestration, and the CLI, and finally wiring everything together with an end-to-end smoke test. Property-based tests live alongside the component they validate, are annotated with the property number and requirements from the design, and use Hypothesis with at least 100 examples per property.

## Tasks

- [x] 1. Project scaffolding and core contracts
  - [x] 1.1 Create Python package structure, pyproject.toml, and `.env.example`
    - Create `trestle_etl/` with empty submodules (`__init__.py`, `loader/__init__.py`, `sql/`)
    - Create `tests/` with `unit/`, `property/`, `integration/` folders
    - Add `pyproject.toml` pinning runtime deps (`requests`, `pydantic>=2`, `sqlalchemy`, `pymysql`, `python-dotenv`) and dev deps (`pytest`, `hypothesis`, `responses`, `freezegun`, `testcontainers`)
    - Add `.env.example` listing every env var read by `config.py` (`TRESTLE_BASE_URL`, `TRESTLE_TOKEN_URL`, `TRESTLE_CLIENT_ID`, `TRESTLE_CLIENT_SECRET`, `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`, `STATE_FILE_PATH`, `PAGE_SIZE`) with placeholder values
    - _Requirements: 13.4_

  - [x] 1.2 Implement `config.Settings` and `Settings.load()`
    - Frozen dataclass exposing all constants required by Requirement 13.2
    - `load()` loads `.env` via `python-dotenv`, then reads from `os.environ`
    - Raise `ConfigError` naming the missing variable on any missing required value
    - _Requirements: 13.1, 13.2, 13.3, 13.5_

  - [x] 1.3 Write unit tests for `Settings.load()`
    - Happy path plus one test per required missing variable
    - _Requirements: 13.3_

  - [x] 1.4 Implement `logging_setup.configure_logging()`
    - Structured format with timestamp, level, logger name, message
    - Provide helpers for the run-start and run-end INFO entries required by Req 12.3 / 12.4
    - _Requirements: 12.1, 12.3, 12.4_

  - [x] 1.5 Write smoke test asserting the `trestle_etl` package contains no `print(` calls
    - Walk package source tree and grep for `print(` outside comments/strings
    - _Requirements: 12.1_

  - [x] 1.6 Author MySQL schema at `trestle_etl/sql/schema.sql`
    - `property` table with `ListingKey VARCHAR(128) NOT NULL PRIMARY KEY`, all Promoted_Columns as typed columns, `raw_data JSON NOT NULL`, `loaded_at DATETIME(6) NOT NULL` (no `DEFAULT CURRENT_TIMESTAMP`)
    - InnoDB engine, `utf8mb4` charset, `utf8mb4_unicode_ci` collation
    - 7 secondary indexes per Req 6.5
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_

- [x] 2. Domain model and transformer
  - [x] 2.1 Implement `models.Property` (Pydantic v2)
    - `model_config = ConfigDict(extra="allow")` to retain unknown RESO fields on the model
    - Typed fields for every Promoted_Column per the design data-model table
    - Timestamp fields parsed as UTC `datetime`; `Decimal` types for monetary/geospatial fields
    - _Requirements: 5.1, 5.4, 5.7, 14.1_

  - [x] 2.2 Implement `transformer.validate()` and `transformer.to_row()`
    - `validate()` returns `Property | None`; returns `None` and logs a WARNING (with available identifying fields) when `ListingKey` is missing or empty
    - Absent RESO fields become `None` without raising
    - `to_row()` returns `(promoted_columns_tuple, raw_data_json_str)` where `raw_data_json_str` is built from the ORIGINAL raw dict (not `model.model_dump()`), guaranteeing unknown-field preservation
    - _Requirements: 5.2, 5.5, 5.6, 5.7, 14.1, 14.4_

  - [x] 2.3 Write property test for missing-`ListingKey` skip
    - **Property 11: For any raw record dict, the Transformer produces a Row iff the record contains a non-empty `ListingKey`; records without a `ListingKey` produce no Row and do not raise**
    - **Validates: Requirements 5.6**

  - [x] 2.4 Write property test for missing-field tolerance
    - **Property 12: For any raw record formed by removing any subset of non-`ListingKey` fields from a valid record, validation succeeds and the resulting model has `None` for every removed field**
    - **Validates: Requirements 5.2**

  - [x] 2.5 Write property test for Pydantic round-trip
    - **Property 13: For any valid raw record, `parse(serialize(parse(record)))` produces a model equal (under Pydantic equality) to `parse(record)`**
    - **Validates: Requirements 5.8, 14.2, 14.3**

  - [x] 2.6 Write property test for `raw_data` preservation
    - **Property 14: For any raw record dict (including arbitrary unknown fields, comma-separated multi-select values), `json.loads(to_row(record).raw_data) == record`**
    - **Validates: Requirements 5.3, 5.5, 14.4**

  - [x] 2.7 Write property test for timestamp UTC round-trip
    - **Property 15: For any timezone-aware `datetime`, serializing through the outbound OData filter and re-parsing the response timestamp yields a `datetime` representing the same UTC instant**
    - **Validates: Requirements 4.6, 5.7**

- [x] 3. Authentication and HTTP client
  - [x] 3.1 Implement `auth.TokenManager`
    - `get_token()` requests via `grant_type=client_credentials` and `scope=api`, caches against `time.monotonic()` with a 60-second safety margin
    - `invalidate()` drops the cached token
    - Raise `ConfigError` during construction if `client_id` or `client_secret` is missing
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

  - [x] 3.2 Write property test for token-cache reuse window
    - **Property 5: For any `(issued_at, expires_in, current_time)` clock state, `get_token()` returns the cached token without contacting the token endpoint iff `(issued_at + expires_in) − current_time > 60 s`; otherwise it fetches a fresh token**
    - **Validates: Requirements 1.4, 1.5**

  - [x] 3.3 Implement `http_client.TrestleClient`
    - `get(url, params=None)` parses JSON responses; attaches `Authorization: Bearer <token>`
    - On 401: invalidate token, re-auth, retry exactly once (outside the shared budget); re-auth failure raises `AuthError`
    - On 429 with `Retry-After`: sleep the indicated seconds
    - On 429 without `Retry-After`, 504, and other 5xx: backoff `[1, 2, 4, 8, 16, 32]` indexed by attempt
    - Shared retry budget of 6 across transient failures; exhausted → `TrestleHTTPError(status, body_excerpt, url)`
    - Log `Hour-Quota-Available` value at INFO when present; log WARNING with status, delay, attempt on every retry
    - Non-504 4xx failures raise immediately
    - Injectable sleep function for testability
    - _Requirements: 1.6, 1.7, 1.8, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 12.5_

  - [x] 3.4 Write property test for `Retry-After` honored exactly
    - **Property 1: For any 429 response carrying `Retry-After: n`, the Trestle_Client waits at least `n` seconds before issuing the retry**
    - **Validates: Requirements 2.1**

  - [x] 3.5 Write property test for exponential backoff schedule
    - **Property 2: For any transient failure (429-without-Retry-After, 504, other 5xx) at retry attempt `k` (0-indexed), the Trestle_Client delays by exactly `2^k` seconds for `k ∈ {0..5}`**
    - **Validates: Requirements 2.2, 2.3, 2.5**

  - [x] 3.6 Write property test for shared 6-retry budget with 401 re-auth separation
    - **Property 3: For any interleaving of 429/504/5xx responses, the Trestle_Client raises `TrestleHTTPError` after exactly 6 retries; inserting any number of 401 responses into the sequence does not change the retry count before the error is raised**
    - **Validates: Requirements 2.4, 2.8**

  - [x] 3.7 Write property test for bearer token on every request
    - **Property 4: For any GET issued by the Trestle_Client, the outgoing request includes `Authorization: Bearer <token>` whose value equals the current TokenManager-cached token**
    - **Validates: Requirements 1.8**

  - [x] 3.8 Write property test for retry WARNING log structure
    - **Property 29: For any retry triggered inside the Trestle_Client, exactly one WARNING log entry is emitted carrying HTTP status, retry delay (seconds), and retry attempt number**
    - **Validates: Requirements 12.5**

- [x] 4. Checkpoint - Foundation and client
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Extractor
  - [x] 5.1 Implement `extractor.replication_stream()`
    - Lazy generator yielding `(records, next_link)` page-by-page
    - Initial URL: `GET /Property?replication=true&$orderby=ModificationTimestamp asc&$top=<page_size>`
    - Accepts `resume_from` to start at a saved `@odata.nextLink` verbatim
    - Terminates when a response lacks `@odata.nextLink`; never buffers multiple nextLinks
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

  - [x] 5.2 Implement `extractor.incremental_stream()`
    - Lazy generator using `$filter=ModificationTimestamp gt <since>`, `$orderby=ModificationTimestamp asc`, `$top=<page_size>`
    - Formats `since` as UTC ISO 8601; follows `@odata.nextLink` until absent
    - Uses strict greater-than to avoid re-processing the boundary record
    - _Requirements: 4.1, 4.4, 4.6, 15.4_

  - [x] 5.3 Write property test for streaming nextLink traversal
    - **Property 6: For any finite chain of replication or incremental pages, the Extractor issues a GET for every `@odata.nextLink` it observes, terminates immediately when a page has no `@odata.nextLink`, and yields each page to the consumer before issuing the GET for the next page (no buffering)**
    - **Validates: Requirements 3.2, 3.3, 3.4, 3.5, 4.4**

  - [x] 5.4 Write property test for `--since` override of first request
    - **Property 7: For any State_Store value `s` and any CLI-supplied `--since t`, the first incremental request issued by the Extractor uses `t` (not `s`) as the `ModificationTimestamp gt` filter lower bound**
    - **Validates: Requirements 4.3**

- [x] 6. State store
  - [x] 6.1 Implement `state.SyncState` dataclass and `state.StateStore`
    - Fields: `last_modification_timestamp`, `replication_in_progress`, `replication_next_link`, `replication_next_link_persisted_at`
    - `load()` returns default (uninitialized) state when the file is missing; raises `CorruptStateError` without modifying the file when present-but-unparseable
    - `save()` writes `<path>.tmp`, `fsync`s, then renames atomically over `<path>`
    - _Requirements: 9.1, 9.2, 9.6, 9.7, 9.8_

  - [x] 6.2 Write property test for state-file round-trip
    - **Property 23: For any `SyncState` value (including `None` fields, long `nextLink` URLs up to 2048 chars, and arbitrary UTC timestamps), `StateStore.load()` after `StateStore.save(s)` returns a `SyncState` equal to `s`**
    - **Validates: Requirements 9.1, 9.2, 9.6**

  - [x] 6.3 Write unit test for corrupt state-file behavior
    - Write malformed JSON and assert `CorruptStateError` is raised and the file is not modified
    - _Requirements: 9.8_

- [x] 7. Loaders
  - [x] 7.1 Define `loader.Loader` protocol and `BatchResult` dataclass
    - Protocol: `write_batch(rows) -> BatchResult`, `close()`
    - `BatchResult(count: int, max_modification_timestamp: datetime)`
    - _Requirements: 7.5_

  - [x] 7.2 Implement `loader.upsert.UpsertLoader`
    - SQLAlchemy Core + `pymysql` engine (no ORM)
    - Batched `INSERT ... ON DUPLICATE KEY UPDATE` built dynamically from the Promoted_Columns list
    - Each batch wrapped in one transaction; rollback and re-raise on failure so state is not advanced
    - Configurable batch size, default 1,000, capped at 5,000
    - Sets `loaded_at` column to `datetime.now(UTC)` at commit-time for every row
    - _Requirements: 6.7, 7.1, 7.2, 7.3, 7.4, 7.5, 7.7_

  - [x] 7.3 Write property test for upsert idempotence
    - **Property 17: For any batch of Rows, applying the batch via the Upsert_Path twice produces the same final database state as applying it once**
    - **Validates: Requirements 7.6**

  - [x] 7.4 Write property test for upsert batch-size bounds
    - **Property 18: For any input record stream processed by the Upsert_Path, every committed batch except possibly the final batch contains between 1,000 and 5,000 records (inclusive); the final batch contains between 1 and 5,000**
    - **Validates: Requirements 7.2**

  - [x] 7.5 Write property test for `loaded_at` bounded by commit wall-clock
    - **Property 16: For any batch committed at wall-clock `t_commit`, every row has `|loaded_at − t_commit| ≤ δ` for small `δ` (ms), and no row uses a MySQL `DEFAULT CURRENT_TIMESTAMP` value**
    - **Validates: Requirements 6.7**

  - [x] 7.6 Write property test for transactional rollback preserving state
    - **Property 19: For any batch whose commit fails (simulated via an injected failure), both the `property` table and `sync_state.json` are byte-for-byte identical to their pre-batch state**
    - **Validates: Requirements 7.4, 9.4, 15.1**

  - [x] 7.7 Implement `loader.bulk.BulkLoader`
    - On fresh full-sync construction: drop the 7 secondary indexes listed in Req 6.5 (preserving the PK)
    - On startup when `replication_in_progress=true`: verify all 7 required secondary indexes exist, recreate any missing, before extraction resumes
    - Per page: write exactly one CSV to `tempfile.mkdtemp()`, then `LOAD DATA LOCAL INFILE '<path>' REPLACE INTO TABLE property CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\' LINES TERMINATED BY '\n' (<column_list>)`; do not aggregate across pages
    - Set `loaded_at` column in the CSV to `datetime.now(UTC)` at batch-start time
    - Delete the CSV after a successful load
    - `close()` recreates the dropped indexes
    - Raise `BulkLoadConfigError` with a message naming both `local_infile=1` (server) and `local_infile=True` (client) on local_infile rejection
    - _Requirements: 3.9, 6.7, 8.1, 8.2, 8.3, 8.4, 8.6, 8.7, 8.8_

  - [x] 7.8 Write property test for bulk-load file lifecycle
    - **Property 20: For any replication page processed by the Bulk_Load_Path, the loader creates exactly one temporary CSV file, invokes `LOAD DATA LOCAL INFILE` exactly once against that file, and deletes the file after a successful load**
    - **Validates: Requirements 3.9, 8.2, 8.3, 8.4**

  - [x] 7.9 Write property test for bulk-load index lifecycle
    - **Property 21: For any full-sync run that completes successfully, the set of secondary indexes on the `property` table before the run equals the set after the run; during the run (between construction and close of the BulkLoader) none of the 7 required secondary indexes exist**
    - **Validates: Requirements 8.7**

  - [x] 7.10 Write property test for startup index repair
    - **Property 22: For any state with `replication_in_progress=true` and any subset of the 7 required secondary indexes missing from the `property` table, the pipeline startup check recreates every missing index so that all 7 are present before extraction resumes**
    - **Validates: Requirements 8.8**

  - [x] 7.11 Write integration tests for schema application and bulk-load config error
    - Apply `schema.sql` against a real MySQL (testcontainers); introspect `information_schema` and assert every Promoted_Column and every required index is present
    - Start MySQL without `local_infile` and assert `BulkLoadConfigError` whose message names both `local_infile=1` (server) and `local_infile=True` (client)
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 8.5, 8.6_

- [x] 8. Checkpoint - Extract, state, load complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Orchestrator and SIGINT handling
  - [x] 9.1 Implement `orchestrator` module with full-sync, incremental, and `--since` flows
    - `run_full_sync(deps)`, `run_incremental(deps, since)`, `run_since(deps, ts)`
    - Full-sync startup: resume from `replication_next_link` iff `replication_in_progress=true` AND `now − replication_next_link_persisted_at < 4 min`; otherwise pivot to incremental starting from `last_modification_timestamp`
    - Per-batch loop: transform → `loader.write_batch` → build new `SyncState` → `state_store.save()` → advance to next page (never before commit)
    - Track running max `ModificationTimestamp`; persist `replication_in_progress = (next_link is not None)` and `replication_next_link_persisted_at = now()` on every save that touches the link
    - Emit per-batch INFO progress log (cumulative count, max committed ModTs, elapsed, requests/min) and the run-start / run-end INFO entries
    - _Requirements: 3.6, 3.7, 3.8, 4.5, 9.3, 9.4, 9.5, 12.2, 12.3, 12.4, 15.1, 15.2, 15.3, 15.4_

  - [x] 9.2 Add SIGINT handling to the orchestrator
    - First SIGINT: set a module-level flag; finish the in-flight batch, commit, update state, then exit 0 logging the last-committed `ModificationTimestamp`
    - Second SIGINT: install a handler that `sys.exit(130)`s immediately without committing the in-flight batch
    - While the flag is set: do not fetch additional pages from Trestle
    - _Requirements: 10.1, 10.2, 10.3, 10.4_

  - [x] 9.3 Write property test for max-ModificationTimestamp reporting
    - **Property 8: For any sequence of records yielded by an incremental run, the `last_modification_timestamp` reported to the State_Store equals `max(record.ModificationTimestamp)` over every record observed in the run**
    - **Validates: Requirements 4.5**

  - [x] 9.4 Write property test for replication state invariant
    - **Property 9: For any sequence of committed pages during a full sync, after each batch commit the State_Store satisfies `replication_in_progress = (next_link is not None)`, `replication_next_link` equals the just-committed page's nextLink (or null if terminal), and `last_modification_timestamp` equals the running max across all committed batches**
    - **Validates: Requirements 3.6, 3.7, 9.5**

  - [x] 9.5 Write property test for resume-vs-pivot decision
    - **Property 10: For any State_Store with `replication_in_progress=true`, the orchestrator resumes from `replication_next_link` iff `now − replication_next_link_persisted_at < 4 min`; otherwise it pivots to an incremental run starting from `last_modification_timestamp`**
    - **Validates: Requirements 3.8**

  - [x] 9.6 Write property test for crash-recovery invariant
    - **Property 24: For any simulated run (successful completion, exception during a batch, or SIGINT injected at an arbitrary point), after the process exits: (a) `state.last_modification_timestamp` equals the max `ModificationTimestamp` across all committed batches, (b) every observed record with `ModificationTimestamp ≤ state.last_modification_timestamp` is present in the `property` table, (c) no non-committed record's `ModificationTimestamp` exceeds `state.last_modification_timestamp`**
    - **Validates: Requirements 15.2, 15.3, 10.1, 10.2**

  - [x] 9.7 Write property test for per-batch progress log structure
    - **Property 28: For any committed batch, the pipeline emits exactly one INFO log entry containing cumulative record count, highest committed `ModificationTimestamp`, elapsed wall-clock time since run start, and requests-per-minute rate over the last interval**
    - **Validates: Requirements 12.2**

- [x] 10. Command-line interface
  - [x] 10.1 Implement `cli.py` and `__main__.py`
    - `argparse` flags: `--full-sync`, `--incremental`, `--since <iso8601>`, `--dry-run`, `--reconcile`
    - Validate mode-flag combinations: more than one of `{--full-sync, --incremental, --reconcile}` → `UsageError`; `--dry-run` MAY combine with any mode except `--reconcile`
    - `--since` parses ISO 8601 UTC; parse failure → `UsageError` BEFORE any HTTP request
    - `--reconcile` → exit non-zero with placeholder message
    - No mode flag → exit non-zero with usage
    - `--incremental` with no `last_modification_timestamp` in state → exit non-zero with remediation pointing to `--full-sync` or `--since`
    - `--dry-run` wiring: suppress loader writes and state-store saves
    - _Requirements: 4.2, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6_

  - [x] 10.2 Write property test for `--dry-run` no-side-effects
    - **Property 25: For any mode flag combined with `--dry-run` and any input stream, the run issues zero writes to MySQL and does not modify the State_File on disk**
    - **Validates: Requirements 11.3**

  - [x] 10.3 Write property test for CLI flag combination validation
    - **Property 26: For any subset of CLI flags drawn from `{--full-sync, --incremental, --reconcile, --dry-run, --since=<ts>}`, the CLI exits non-zero with a usage error iff the combination violates the rules (more than one of the three mode flags, or `--dry-run` with `--reconcile`); all other combinations are accepted**
    - **Validates: Requirements 11.2**

  - [x] 10.4 Write property test for `--since` ISO 8601 parsing
    - **Property 27: For any string `s`: if `s` is a valid ISO 8601 UTC timestamp, `--since s` produces a UTC-aware `datetime` equal to the same instant; otherwise the CLI exits non-zero with a usage error before any HTTP request is issued**
    - **Validates: Requirements 11.4**

  - [x] 10.5 Write unit tests for `--reconcile`, missing-mode-flag, and missing-state messages
    - Assert each documented exit code and message is produced
    - _Requirements: 4.2, 11.5, 11.6_

- [x] 11. Wiring and end-to-end smoke test
  - [x] 11.1 Build dependency-injection wiring in `cli.py`
    - Construct `Settings → TokenManager → TrestleClient → Extractor → Transformer → (BulkLoader | UpsertLoader) → StateStore → Orchestrator`
    - Ensure `Settings.load()` runs before `TokenManager` is constructed so configuration errors precede any network I/O
    - Select loader by mode: `--full-sync` → `BulkLoader`; `--incremental` / `--since` → `UpsertLoader`
    - _Requirements: 8.1, 13.3_

  - [x] 11.2 Write `README.md` covering operator setup
    - Document `local_infile=1` server and `local_infile=True` client requirements
    - Enumerate required env vars (linking to `.env.example`)
    - Document CLI usage for full sync, incremental, `--since`, `--dry-run`, `--reconcile`
    - _Requirements: 8.5, 13.4_

  - [x] 11.3 Write end-to-end smoke test with mock Trestle server
    - Use `pytest-httpserver` (or `responses`) to simulate a 3-page replication chain
    - Run `python -m trestle_etl --full-sync` against a testcontainers MySQL
    - Assert expected rows in `property`, expected `loaded_at` values, and expected final `sync_state.json`
    - _Requirements: 3.1, 3.2, 3.3, 3.6, 3.7, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 7.5, 8.1, 8.2, 8.3, 8.4, 9.1, 9.2, 9.5, 9.6, 12.2_

- [x] 12. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Sub-tasks marked with `*` are optional (tests and the README) and can be skipped for a faster MVP build, but every correctness property from the design has a dedicated test sub-task here.
- Each implementation sub-task references specific acceptance criteria. Each property-test sub-task cites its property number, the property statement, and the validated requirement clauses.
- Property tests use Hypothesis with `max_examples ≥ 100` per the design testing strategy; crash-injection properties (19 and 24) use Hypothesis-driven failure hooks so the library can shrink to minimal failure points.
- The State_Store is the only writer of `sync_state.json`, and every save happens AFTER the corresponding batch commit, which is what makes the crash-recovery invariant hold.
- Bulk loading stays one-CSV-per-page (Req 3.9) so the replication-link 5-minute freshness window is never exceeded.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "1.4", "1.6", "7.1"] },
    { "id": 2, "tasks": ["1.3", "1.5", "2.1", "3.1", "6.1"] },
    { "id": 3, "tasks": ["2.2", "3.2", "3.3", "6.2", "6.3"] },
    { "id": 4, "tasks": ["2.3", "2.4", "2.5", "2.6", "2.7", "3.4", "3.5", "3.6", "3.7", "3.8", "5.1"] },
    { "id": 5, "tasks": ["5.2"] },
    { "id": 6, "tasks": ["5.3", "5.4", "7.2", "7.7"] },
    { "id": 7, "tasks": ["7.3", "7.4", "7.5", "7.6", "7.8", "7.9", "7.10", "7.11"] },
    { "id": 8, "tasks": ["9.1"] },
    { "id": 9, "tasks": ["9.2"] },
    { "id": 10, "tasks": ["9.3", "9.4", "9.5", "9.6", "9.7", "10.1", "11.2"] },
    { "id": 11, "tasks": ["10.2", "10.3", "10.4", "10.5"] },
    { "id": 12, "tasks": ["11.1"] },
    { "id": 13, "tasks": ["11.3"] }
  ]
}
```
