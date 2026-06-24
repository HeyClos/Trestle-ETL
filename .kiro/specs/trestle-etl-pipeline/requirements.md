# Requirements Document

## Introduction

The Trestle ETL Pipeline is a Python-based data synchronization system that extracts real estate property data from Cotality's Trestle WebAPI (an OData v4 interface over the RESO Data Dictionary) and loads it into a local MySQL database. The pipeline handles initial backfills of approximately 1.6 million Property records (which exceeds the 1,000,000 record limit of standard `$top/$skip` pagination and therefore requires the Trestle replication endpoint), ongoing incremental synchronization based on `ModificationTimestamp`, and crash recovery via durable state persistence. The system is designed for a single-developer, single-machine deployment and prioritizes correctness, restartability, and operational observability over horizontal scalability.

## Glossary

- **Trestle_API**: Cotality's Trestle WebAPI, an OData v4 endpoint exposing RESO Data Dictionary resources. Base URL is configurable (e.g., `https://api.cotality.com/trestle/odata/` or the legacy `https://api-prod.corelogic.com/trestle/odata/`).
- **RESO_Data_Dictionary**: Real Estate Standards Organization data model defining canonical field names and enumerations for real estate data.
- **Property_Resource**: The `/Property` OData entity set in Trestle, keyed by `ListingKey`.
- **ListingKey**: The primary key of a Property record, a string of up to 128 characters.
- **ModificationTimestamp**: UTC timestamp field on Property indicating the last modification time; used to drive incremental sync.
- **Replication_Endpoint**: The Trestle endpoint activated via `?replication=true` that returns records in pages via `@odata.nextLink`, used for bulk exports exceeding 1,000,000 records.
- **Replication_Link**: A `@odata.nextLink` URL returned by the replication endpoint. Expires after 5 minutes of inactivity and cannot be skipped.
- **Incremental_Endpoint**: The standard `/Property` endpoint filtered by `ModificationTimestamp gt <timestamp>` used for delta syncs.
- **Token_Manager**: Component responsible for obtaining, caching, and refreshing OAuth2 access tokens from the Trestle OIDC token endpoint.
- **Trestle_Client**: HTTP client component that wraps authentication, retries, and paginated fetch against the Trestle API.
- **Extractor**: Component exposing iterators (`replication_stream()` and `incremental_stream()`) that yield pages of Property records.
- **Transformer**: Component that validates and normalizes raw JSON records into a canonical form using Pydantic v2 models.
- **Loader**: Component that persists transformed records to MySQL using either batched upserts or the bulk-load fast path.
- **Bulk_Load_Path**: A loader mode that writes CSV files to a temporary directory and invokes MySQL `LOAD DATA LOCAL INFILE`, used during full sync.
- **Upsert_Path**: A loader mode that uses `INSERT ... ON DUPLICATE KEY UPDATE` in transactional batches, used during incremental sync.
- **State_Store**: A local JSON file (`sync_state.json`) persisting the highest committed `ModificationTimestamp`, replication progress, and current `@odata.nextLink` when mid-replication.
- **Promoted_Columns**: The subset of RESO fields materialized as typed MySQL columns on the `property` table in addition to being present in `raw_data`. The set is: `ListingKey` (primary key), `ListingId`, `MlsStatus`, `InternetEntireListingDisplayYN`, `InternetAddressDisplayYN`, `InternetAutomatedValuationDisplayYN`, `InternetConsumerCommentYN`, `Latitude`, `Longitude`, `ParcelNumber`, `StreetNumberNumeric`, `StreetDirPrefix`, `StreetName`, `StreetSuffix`, `UnitNumber`, `City`, `StateOrProvince`, `PostalCode`, `OriginalListPrice`, `ListPrice`, `ClosePrice`, `ModificationTimestamp`, `OriginalEntryTimestamp`, `PendingTimestamp`, `StatusChangeTimestamp`, `WithdrawnDate`, `CloseDate`, `PhotosChangeTimestamp`, `PhotosCount`, `VideosCount`, `PropertyType`, `PropertySubType`, `PropertySubTypeAdditional`, `StructureType`, `YearBuiltDetails`, `ArchitecturalStyle`, `PropertyAttachedYN`, `Stories`, `LivingArea`, `LotSizeSquareFeet`, `BedroomsTotal`, `BathroomsFull`, `BathroomsHalf`, `BathroomsThreeQuarter`, `GarageSpaces`, `YearBuilt`, `YearBuiltEffective`, `PoolPrivateYN`, `SpaYN`, `DirectionFaces`, `SeniorCommunityYN`, `AssociationYN`, `AssociationAmenities`, `HorseAmenities`, `PetsAllowedYN`, `Furnished`, `ListAgentKey`, `ListOfficeKey`, `ListTeamKey`, `BuyerAgentKey`, `BuyerOfficeKey`, `BuyerTeamKey`.
- **Raw_Data_Column**: A MySQL native `JSON` column named `raw_data` on the `property_raw` table that stores the full unmodified RESO record, including fields that are also promoted to typed columns on `property`. It is kept in a separate table (1:1 by `ListingKey`) so searches over the typed `property` columns never read the large JSON payload.
- **CLI**: The command-line interface exposed via `python -m trestle_etl` with subcommands/flags for full sync, incremental sync, since-override, dry-run, and reconcile.
- **Quota_Error**: An HTTP 429 response from Trestle indicating the request quota has been exceeded; may include a `Retry-After` header and an `Hour-Quota-Available` header.
- **Gateway_Timeout**: An HTTP 504 response from Trestle, retried with the same request.
- **State_File**: The on-disk JSON document maintained by the State_Store.
- **SIGINT**: The POSIX interrupt signal (Ctrl+C) that the pipeline handles for graceful shutdown.

## Requirements

### Requirement 1: OAuth2 Authentication and Token Caching

**User Story:** As an operator of the ETL pipeline, I want the system to authenticate to Trestle using client credentials and cache the resulting access token, so that API calls succeed without re-authenticating on every request.

#### Acceptance Criteria

1. WHEN the Trestle_Client is first invoked, THE Token_Manager SHALL request an access token from the Trestle OIDC token endpoint using `grant_type=client_credentials` and `scope=api`.
2. THE Token_Manager SHALL read `client_id` and `client_secret` exclusively from environment variables loaded via the `.env` file.
3. IF `client_id` or `client_secret` is missing from the environment, THEN THE Token_Manager SHALL raise a configuration error before making any HTTP request.
4. WHEN a valid access token has been obtained, THE Token_Manager SHALL cache the token in memory along with its expiration time.
5. WHILE a cached access token has more than 60 seconds of remaining validity, THE Token_Manager SHALL reuse the cached token without contacting the token endpoint.
6. WHEN a request returns HTTP 401, THE Token_Manager SHALL discard the cached token, obtain a new token, and retry the original request exactly once.
7. IF obtaining a new token after a 401 response fails, THEN THE Trestle_Client SHALL raise an authentication error and SHALL NOT retry further.
8. THE Trestle_Client SHALL include the current access token as a `Bearer` token in the `Authorization` header of every request to the Trestle API.

### Requirement 2: Paginated Fetch with Retry and Quota Handling

**User Story:** As an operator, I want the HTTP client to transparently handle transient failures, gateway timeouts, and quota errors, so that the pipeline completes reliably without manual intervention.

#### Acceptance Criteria

1. WHEN the Trestle_Client issues a request and receives HTTP 429, THE Trestle_Client SHALL wait for the duration specified in the `Retry-After` response header before retrying.
2. IF no `Retry-After` header is present on a 429 response, THEN THE Trestle_Client SHALL apply exponential backoff with delays of 1, 2, 4, 8, 16, and 32 seconds across successive retries.
3. WHEN the Trestle_Client issues a request and receives HTTP 504, THE Trestle_Client SHALL retry the same request using exponential backoff with delays of 1, 2, 4, 8, 16, and 32 seconds.
4. THE Trestle_Client SHALL perform at most 6 retries per request before raising an error to the caller.
5. IF a request fails with HTTP status 5xx other than 504, THEN THE Trestle_Client SHALL apply exponential backoff and retry up to 6 times.
6. WHEN a request fails after 6 retries, THE Trestle_Client SHALL raise an error that includes the final HTTP status code, response body excerpt, and request URL.
7. WHEN the Trestle_Client receives an `Hour-Quota-Available` response header, THE Trestle_Client SHALL log the remaining quota value at INFO level.
8. THE Trestle_Client SHALL enforce a single shared retry budget of 6 retries per request across HTTP 429, 504, and other 5xx responses; the one-shot 401 re-authentication retry defined in Requirement 1 is separate and SHALL NOT count against this budget.

### Requirement 3: Full Sync via Replication Endpoint

**User Story:** As an operator performing an initial backfill, I want the pipeline to use the Trestle replication endpoint, so that I can load more than 1,000,000 Property records without hitting the `$top/$skip` pagination cap.

#### Acceptance Criteria

1. WHEN the CLI is invoked with `--full-sync`, THE Extractor SHALL issue the initial request `GET /Property?replication=true&$orderby=ModificationTimestamp asc` with `$top=1000`.
2. WHILE the most recent replication response contains an `@odata.nextLink` field, THE Extractor SHALL follow that link to fetch the next page.
3. WHEN a replication response does not contain an `@odata.nextLink` field, THE Extractor SHALL terminate the replication stream.
4. THE Extractor SHALL yield each replication page to the downstream Loader before fetching the next page, so that replication links are followed within the 5-minute inactivity expiration window.
5. THE Extractor SHALL NOT collect or buffer multiple replication `@odata.nextLink` URLs before processing.
6. WHILE a full sync is in progress, THE State_Store SHALL record `replication_in_progress=true`, the current `@odata.nextLink`, and the highest `ModificationTimestamp` observed so far.
7. WHEN the replication stream completes without error, THE State_Store SHALL record `replication_in_progress=false` and retain the highest `ModificationTimestamp` committed.
8. IF a full sync fails partway through, THEN on the next startup THE pipeline SHALL attempt to resume from `replication_next_link` when `replication_in_progress=true` AND the link was persisted less than 4 minutes prior; OTHERWISE the pipeline SHALL pivot to an incremental sync starting from `last_modification_timestamp`, which is safe because Requirement 15 guarantees that timestamp reflects only committed records and the strict-greater-than filter prevents duplication.
9. WHILE operating in Bulk_Load_Path during a full sync, THE Loader SHALL treat each replication page as a single batch (one CSV file, one `LOAD DATA LOCAL INFILE` invocation) and SHALL NOT aggregate records across replication pages before committing.

### Requirement 4: Incremental Sync via ModificationTimestamp

**User Story:** As an operator running ongoing syncs after the initial backfill, I want the pipeline to fetch only records modified since the last successful run, so that syncs complete quickly and do not re-transfer unchanged data.

#### Acceptance Criteria

1. WHEN the CLI is invoked with `--incremental`, THE Extractor SHALL issue `GET /Property?$filter=ModificationTimestamp gt <timestamp>&$orderby=ModificationTimestamp asc&$top=1000`, where `<timestamp>` is the `last_modification_timestamp` value from the State_Store.
2. IF the State_Store contains no `last_modification_timestamp` when `--incremental` is invoked, THEN THE CLI SHALL exit with a non-zero status and an error message directing the operator to run `--full-sync` or `--since` first.
3. WHEN the CLI is invoked with `--since <iso8601_timestamp>`, THE Extractor SHALL use the provided timestamp as the `ModificationTimestamp` filter lower bound, overriding the State_Store value for that run.
4. WHILE additional pages remain in the incremental response, THE Extractor SHALL follow `@odata.nextLink` until no further link is returned.
5. THE Extractor SHALL track the highest `ModificationTimestamp` observed across the incremental run and SHALL use that value (not wall-clock time) to update the State_Store.
6. THE Extractor SHALL send and interpret all `ModificationTimestamp` values as UTC.

### Requirement 5: Data Transformation and Validation

**User Story:** As an operator, I want raw Trestle JSON records to be validated and normalized before database load, so that malformed records are caught early and downstream queries can rely on typed columns.

#### Acceptance Criteria

1. THE Transformer SHALL validate each incoming record against a Pydantic v2 model for the Property_Resource.
2. WHEN a field documented in the RESO_Data_Dictionary is absent from an incoming record, THE Transformer SHALL treat the field as null and SHALL NOT raise an error.
3. WHEN a multi-select enumeration field is received as a comma-separated string, THE Transformer SHALL preserve the raw string in the Raw_Data_Column.
4. THE Transformer SHALL store standard RESO enumeration values in their canonical PascalCase form and SHALL NOT rely on the Trestle `PrettyEnums=true` query parameter.
5. THE Transformer SHALL produce, for each validated record, a tuple of Promoted_Columns values and a complete `raw_data` JSON payload representing the unmodified record as received.
6. IF a record lacks a `ListingKey`, THEN THE Transformer SHALL skip the record, log a warning with the record's other identifying fields, and continue processing subsequent records.
7. THE Transformer SHALL parse timestamp fields as UTC `datetime` values for all Promoted_Columns of timestamp type.
8. FOR ALL records that pass Transformer validation, the round-trip property SHALL hold: serializing the Transformer output back to JSON and re-validating through the Pydantic model SHALL produce an equivalent model instance.

### Requirement 6: MySQL Schema and Raw Data Storage

**User Story:** As an operator querying the loaded data, I want a MySQL schema with commonly queried RESO fields promoted to typed columns plus the full raw record preserved as JSON, so that I can query key fields efficiently without schema migrations when new fields are needed.

#### Acceptance Criteria

1. THE schema.sql file SHALL define a `property` table and a `property_raw` table, both using the InnoDB storage engine and the `utf8mb4` character set.
2. THE `property` and `property_raw` tables SHALL each declare `ListingKey` as `VARCHAR(128) NOT NULL PRIMARY KEY`, with a 1:1 correspondence on `ListingKey`.
3. THE `property_raw` table SHALL declare a column named `raw_data` of MySQL native `JSON` type that stores the full unmodified RESO record. THE `property` table SHALL NOT carry the `raw_data` payload, so that searches over the typed columns never read the large JSON.
4. THE `property` table SHALL include, as typed columns, every field listed in the Promoted_Columns set defined in the Glossary.
5. THE `property` table SHALL include a secondary index on every non-`ListingKey` Promoted_Column listed in the Glossary.
6. THE `property` and `property_raw` tables SHALL each include a `loaded_at` column of type `DATETIME` that records the time at which each row was written or updated by the Loader.
7. THE Loader SHALL set `loaded_at` to the current UTC wall-clock time at batch-commit time for every row inserted or updated in that batch, and SHALL NOT rely on a MySQL `DEFAULT CURRENT_TIMESTAMP` or `ON UPDATE CURRENT_TIMESTAMP` clause for this value.
8. THE Loader SHALL write the `property` row and its corresponding `property_raw` row within the same transaction so the two tables never diverge for a committed batch.

### Requirement 7: Upsert Load Path

**User Story:** As an operator running incremental syncs, I want records to be upserted into MySQL in transactional batches, so that re-running a sync is idempotent and partial failures do not leave the database in an inconsistent state.

#### Acceptance Criteria

1. WHEN the Loader is operating in Upsert_Path mode, THE Loader SHALL use `INSERT ... ON DUPLICATE KEY UPDATE` statements against the `property` table.
2. THE Loader SHALL commit upserts in batches of between 1,000 and 5,000 records.
3. THE Loader SHALL wrap each batch in a single database transaction.
4. IF a batch transaction fails, THEN THE Loader SHALL roll back the transaction, SHALL NOT update the State_Store for that batch, and SHALL raise the error to the caller.
5. WHEN a batch transaction commits successfully, THE Loader SHALL report the batch size and the highest `ModificationTimestamp` in the batch to the caller for State_Store updates.
6. FOR ALL records processed by the Upsert_Path, applying the same batch twice SHALL produce the same final database state as applying it once (idempotence property).
7. THE Loader SHALL use `pymysql` as the DBAPI driver via SQLAlchemy Core and SHALL NOT use the SQLAlchemy ORM.

### Requirement 8: Bulk Load Path

**User Story:** As an operator performing a full-sync backfill, I want a bulk-load fast path that uses MySQL `LOAD DATA LOCAL INFILE`, so that 1.6 million records can be loaded in a practical timeframe.

#### Acceptance Criteria

1. WHEN the CLI is invoked with `--full-sync`, THE Loader SHALL operate in Bulk_Load_Path mode.
2. THE Bulk_Load_Path SHALL write each batch to a temporary CSV file in the system temporary directory.
3. THE Bulk_Load_Path SHALL execute MySQL `LOAD DATA LOCAL INFILE` to ingest each CSV file into the `property` table.
4. WHEN a CSV file has been successfully loaded into MySQL, THE Bulk_Load_Path SHALL delete the temporary CSV file.
5. THE README SHALL document that `LOAD DATA LOCAL INFILE` requires both `local_infile=1` on the MySQL server and `local_infile=True` in the client connection.
6. IF the MySQL server rejects `LOAD DATA LOCAL INFILE` due to server configuration, THEN THE Loader SHALL raise an error that names the required server and client settings.
7. WHEN the Bulk_Load_Path begins a full sync, THE Loader SHALL drop all secondary indexes enumerated in Requirement 6.5 (one per non-`ListingKey` Promoted_Column) from the `property` table before loading and SHALL recreate those indexes after the full sync completes. The `ListingKey` primary key SHALL NOT be dropped.
8. WHEN the pipeline starts AND `replication_in_progress=true` in the State_Store, THE Loader SHALL verify the presence of the secondary indexes enumerated in Requirement 6.5 and SHALL recreate any that are missing before proceeding with either resumption or incremental fallback.

### Requirement 9: State Persistence and Crash Recovery

**User Story:** As an operator whose pipeline might be interrupted mid-run, I want progress persisted to a state file after each committed batch, so that a crashed run can resume or pivot to incremental sync without losing work or duplicating effort.

#### Acceptance Criteria

1. THE State_Store SHALL persist its state as a JSON document at a configurable file path (default `sync_state.json`).
2. THE State_Store JSON document SHALL include the fields `last_modification_timestamp`, `replication_in_progress`, and `replication_next_link`.
3. WHEN a batch commits successfully, THE State_Store SHALL update `last_modification_timestamp` to the highest `ModificationTimestamp` in that batch before the pipeline acknowledges the batch.
4. THE State_Store SHALL NOT update `last_modification_timestamp` for batches that fail to commit.
5. WHEN the pipeline is mid-replication, THE State_Store SHALL update `replication_next_link` to the current `@odata.nextLink` after each successful batch commit.
6. THE State_Store SHALL write its JSON document atomically by writing to a temporary file in the same directory and renaming it over the target file.
7. IF the State_File is missing on startup, THEN THE State_Store SHALL treat the pipeline as un-initialized and SHALL return null for `last_modification_timestamp`.
8. IF the State_File is present but malformed, THEN the pipeline SHALL exit with a non-zero status and an error message identifying the corrupt file, without modifying it.

### Requirement 10: Graceful Shutdown on SIGINT

**User Story:** As an operator, I want Ctrl+C to stop the pipeline cleanly, so that in-flight work is either completed or discarded without corrupting the state file.

#### Acceptance Criteria

1. WHEN the pipeline receives SIGINT, THE pipeline SHALL complete the current batch, commit it, update the State_Store, and then exit.
2. WHILE a SIGINT has been received and a batch is in flight, THE pipeline SHALL NOT fetch additional pages from the Trestle_API.
3. WHEN a graceful shutdown completes, THE pipeline SHALL exit with status code 0 and SHALL log the last committed `ModificationTimestamp`.
4. IF a second SIGINT is received after the first, THEN the pipeline SHALL exit immediately with a non-zero status without committing the in-flight batch.

### Requirement 11: Command-Line Interface

**User Story:** As an operator, I want a command-line interface with distinct modes for full sync, incremental sync, timestamp override, and dry-run, so that I can control the pipeline's behavior from the shell and from scheduled jobs.

#### Acceptance Criteria

1. THE CLI SHALL be invokable as `python -m trestle_etl` with the flags `--full-sync`, `--incremental`, `--since <iso8601_timestamp>`, `--dry-run`, and `--reconcile`.
2. IF more than one of `--full-sync`, `--incremental`, and `--reconcile` is supplied in the same invocation, THEN THE CLI SHALL exit with a non-zero status and a usage error. The `--dry-run` and `--since` flags are modifiers and MAY be combined with any single mode flag, except that `--dry-run` MAY NOT be combined with `--reconcile`.
3. WHEN the CLI is invoked with `--dry-run` in combination with `--full-sync`, `--incremental`, or `--since`, THE pipeline SHALL perform extraction and transformation but SHALL NOT write to MySQL or update the State_Store.
4. WHEN the CLI is invoked with `--since <iso8601_timestamp>`, THE pipeline SHALL parse the argument as an ISO 8601 UTC timestamp and SHALL exit with a usage error if parsing fails.
5. WHEN the CLI is invoked with `--reconcile`, THE CLI SHALL exit with a non-zero status and a message stating that reconcile is a placeholder and not implemented in the first iteration.
6. IF no mode flag is supplied, THEN THE CLI SHALL exit with a non-zero status and print usage information.

### Requirement 12: Structured Logging and Progress Reporting

**User Story:** As an operator monitoring a long-running sync, I want periodic structured log output with counts, timestamps, and throughput, so that I can observe progress and detect stalls.

#### Acceptance Criteria

1. THE pipeline SHALL use the Python `logging` module for all output and SHALL NOT use `print` statements for operational messages.
2. WHEN the Loader has committed every batch, THE pipeline SHALL emit a progress log entry containing the cumulative record count, the highest `ModificationTimestamp` committed, the elapsed wall-clock time since the run started, and the requests-per-minute rate over the last interval.
3. THE pipeline SHALL emit an INFO-level log entry at run start that includes the invoked mode, the resolved base URL, and the `last_modification_timestamp` read from the State_Store.
4. THE pipeline SHALL emit an INFO-level log entry at run end that includes total records loaded, total elapsed time, and the final `last_modification_timestamp`.
5. WHEN any retry is triggered in the Trestle_Client, THE pipeline SHALL emit a WARNING-level log entry including the HTTP status, the retry delay, and the retry attempt number.

### Requirement 13: Configuration Management

**User Story:** As an operator deploying the pipeline on a new machine, I want all runtime configuration to come from environment variables loaded via `.env`, so that secrets are not committed to the repository and the same code runs across environments.

#### Acceptance Criteria

1. THE `config.py` module SHALL load environment variables from a `.env` file using `python-dotenv` when present.
2. THE `config.py` module SHALL expose as constants the Trestle base URL, the Trestle token URL, the `client_id`, the `client_secret`, the MySQL connection parameters (host, port, user, password, database), the state file path, and the default page size.
3. IF a required configuration value is missing at startup, THEN `config.py` SHALL raise an error naming the missing variable and the pipeline SHALL exit with a non-zero status.
4. THE repository SHALL include a `.env.example` file that enumerates every environment variable read by `config.py` with placeholder values and SHALL NOT commit a real `.env` file.
5. THE Trestle base URL SHALL be read from an environment variable so that the account can be pointed at either `https://api.cotality.com/trestle/odata/` or `https://api-prod.corelogic.com/trestle/odata/` without code changes.

### Requirement 14: Parser Round-Trip Correctness

**User Story:** As a developer maintaining the Transformer, I want round-trip tests between raw JSON and the Pydantic model, so that serialization and parsing remain consistent as the model evolves.

#### Acceptance Criteria

1. THE Transformer SHALL expose functions to parse a raw JSON record into a validated model and to serialize a validated model back to JSON.
2. FOR ALL records accepted by the Transformer, parsing a JSON record, serializing the resulting model, and parsing the serialized output SHALL produce a model instance equivalent to the first parsed model (round-trip property).
3. FOR ALL valid model instances, serializing the model to JSON and parsing the JSON back into a model SHALL produce a model instance equivalent to the original (round-trip property).
4. Because Requirement 5.5 stores the complete unmodified record in the Raw_Data_Column, IF a raw JSON record contains a field that is not present in the Pydantic model, THEN that field SHALL be retained in the Raw_Data_Column and SHALL NOT be dropped.

### Requirement 15: Progress File and Batch Boundary Invariants

**User Story:** As an operator reasoning about crash recovery, I want the invariant that the state file never references data that has not committed, so that restarts never skip records.

#### Acceptance Criteria

1. THE pipeline SHALL update the State_Store only after the corresponding batch commit succeeds.
2. FOR ALL successful runs, the invariant SHALL hold: every record with `ModificationTimestamp <= state.last_modification_timestamp` that was observed during the run has been committed to MySQL.
3. FOR ALL failed runs, the invariant SHALL hold: `state.last_modification_timestamp` in the State_Store equals the highest `ModificationTimestamp` of any batch that committed successfully.
4. WHEN the pipeline starts an incremental run, THE Extractor SHALL use a strictly-greater-than filter (`ModificationTimestamp gt state.last_modification_timestamp`) so that the boundary record is not re-processed if it was committed on the prior run.
