-- Trestle ETL Pipeline: MySQL `property` table schema.
--
-- This schema is authored verbatim from the "MySQL `property` Table Schema"
-- section of design.md and satisfies Requirements 6.1 through 6.6:
--   6.1 InnoDB engine with utf8mb4 / utf8mb4_unicode_ci.
--   6.2 `ListingKey VARCHAR(128) NOT NULL PRIMARY KEY`.
--   6.3 All Promoted_Columns present as typed columns.
--   6.4 `raw_data JSON NOT NULL` preserves the full source payload.
--   6.5 Seven secondary indexes on the columns enumerated below.
--   6.6 `loaded_at DATETIME(6) NOT NULL` without `DEFAULT CURRENT_TIMESTAMP`;
--       the Loader supplies this value at commit time (Requirement 6.7).

CREATE TABLE property (
    ListingKey              VARCHAR(128) NOT NULL PRIMARY KEY,
    ModificationTimestamp   DATETIME(6) NULL,
    StandardStatus          VARCHAR(64) NULL,
    MlsStatus               VARCHAR(64) NULL,
    PropertyType            VARCHAR(64) NULL,
    PropertySubType         VARCHAR(64) NULL,
    ListPrice               DECIMAL(14,2) NULL,
    ClosePrice              DECIMAL(14,2) NULL,
    OriginalListPrice       DECIMAL(14,2) NULL,
    ListingContractDate     DATE NULL,
    CloseDate               DATE NULL,
    StreetNumber            VARCHAR(32) NULL,
    StreetName              VARCHAR(128) NULL,
    UnitNumber              VARCHAR(32) NULL,
    City                    VARCHAR(64) NULL,
    StateOrProvince         VARCHAR(2) NULL,
    PostalCode              VARCHAR(16) NULL,
    County                  VARCHAR(64) NULL,
    Country                 VARCHAR(2) NULL,
    Latitude                DECIMAL(10,7) NULL,
    Longitude               DECIMAL(10,7) NULL,
    BedroomsTotal           SMALLINT NULL,
    BathroomsTotalInteger   SMALLINT NULL,
    LivingArea              DECIMAL(10,2) NULL,
    LotSizeSquareFeet       DECIMAL(12,2) NULL,
    YearBuilt               SMALLINT NULL,
    DaysOnMarket            INT NULL,
    ListAgentKey            VARCHAR(128) NULL,
    ListOfficeKey           VARCHAR(128) NULL,
    PhotosCount             INT NULL,
    PublicRemarks           TEXT NULL,
    raw_data                JSON NOT NULL,
    loaded_at               DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_property_modts      ON property(ModificationTimestamp);
CREATE INDEX idx_property_status     ON property(StandardStatus);
CREATE INDEX idx_property_type       ON property(PropertyType);
CREATE INDEX idx_property_city       ON property(City);
CREATE INDEX idx_property_postal     ON property(PostalCode);
CREATE INDEX idx_property_price      ON property(ListPrice);
CREATE INDEX idx_property_state      ON property(StateOrProvince);
