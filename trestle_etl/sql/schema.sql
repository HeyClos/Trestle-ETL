-- Trestle ETL Pipeline: MySQL `property` table schema.
--
-- This schema is authored from the Promoted_Columns set defined in the
-- feature design and satisfies Requirements 6.1 through 6.6:
--   6.1 InnoDB engine with utf8mb4 / utf8mb4_unicode_ci.
--   6.2 `ListingKey VARCHAR(128) NOT NULL PRIMARY KEY`.
--   6.3 All Promoted_Columns present as typed columns.
--   6.4 `raw_data JSON NOT NULL` preserves the full source payload.
--   6.5 Seven secondary indexes on the columns enumerated below.
--   6.6 `loaded_at DATETIME(6) NOT NULL` without `DEFAULT CURRENT_TIMESTAMP`;
--       the Loader supplies this value at commit time (Requirement 6.7).

CREATE TABLE property (
    ListingKey                              VARCHAR(128) NOT NULL PRIMARY KEY,
    ListingId                               VARCHAR(128) NULL,
    MlsStatus                               VARCHAR(64) NULL,

    -- Internet display flags ----------------------------------------
    InternetEntireListingDisplayYN          TINYINT(1) NULL,
    InternetAddressDisplayYN                TINYINT(1) NULL,
    InternetAutomatedValuationDisplayYN     TINYINT(1) NULL,
    InternetConsumerCommentYN               TINYINT(1) NULL,

    -- Geospatial ----------------------------------------------------
    Latitude                                DECIMAL(10,7) NULL,
    Longitude                               DECIMAL(10,7) NULL,

    -- Address -------------------------------------------------------
    ParcelNumber                            VARCHAR(64) NULL,
    StreetNumberNumeric                     INT NULL,
    StreetDirPrefix                         VARCHAR(16) NULL,
    StreetName                              VARCHAR(128) NULL,
    StreetSuffix                            VARCHAR(32) NULL,
    UnitNumber                              VARCHAR(32) NULL,
    City                                    VARCHAR(64) NULL,
    StateOrProvince                         VARCHAR(2) NULL,
    PostalCode                              VARCHAR(16) NULL,

    -- Pricing -------------------------------------------------------
    OriginalListPrice                       DECIMAL(14,2) NULL,
    ListPrice                               DECIMAL(14,2) NULL,
    ClosePrice                              DECIMAL(14,2) NULL,

    -- Timestamps and dates ------------------------------------------
    ModificationTimestamp                   DATETIME(6) NULL,
    OriginalEntryTimestamp                  DATETIME(6) NULL,
    PendingTimestamp                        DATETIME(6) NULL,
    StatusChangeTimestamp                   DATETIME(6) NULL,
    WithdrawnDate                           DATE NULL,
    CloseDate                               DATE NULL,
    PhotosChangeTimestamp                   DATETIME(6) NULL,

    -- Media counts --------------------------------------------------
    PhotosCount                             INT NULL,
    VideosCount                             INT NULL,

    -- Property classification ---------------------------------------
    PropertyType                            VARCHAR(64) NULL,
    PropertySubType                         VARCHAR(64) NULL,
    PropertySubTypeAdditional               VARCHAR(128) NULL,
    StructureType                           VARCHAR(128) NULL,
    YearBuiltDetails                        VARCHAR(128) NULL,
    ArchitecturalStyle                      VARCHAR(128) NULL,
    PropertyAttachedYN                      TINYINT(1) NULL,
    Stories                                 SMALLINT NULL,

    -- Size and rooms ------------------------------------------------
    LivingArea                              DECIMAL(10,2) NULL,
    LotSizeSquareFeet                       DECIMAL(12,2) NULL,
    BedroomsTotal                           SMALLINT NULL,
    BathroomsFull                           SMALLINT NULL,
    BathroomsHalf                           SMALLINT NULL,
    BathroomsThreeQuarter                   SMALLINT NULL,
    GarageSpaces                            DECIMAL(6,2) NULL,
    YearBuilt                               SMALLINT NULL,
    YearBuiltEffective                      SMALLINT NULL,

    -- Features ------------------------------------------------------
    PoolPrivateYN                           TINYINT(1) NULL,
    SpaYN                                   TINYINT(1) NULL,
    DirectionFaces                          VARCHAR(32) NULL,
    SeniorCommunityYN                       TINYINT(1) NULL,
    AssociationYN                           TINYINT(1) NULL,
    AssociationAmenities                    VARCHAR(512) NULL,
    HorseAmenities                          VARCHAR(512) NULL,
    PetsAllowedYN                           TINYINT(1) NULL,
    Furnished                               VARCHAR(32) NULL,

    -- Agents, offices, teams ----------------------------------------
    ListAgentKey                            VARCHAR(128) NULL,
    ListOfficeKey                           VARCHAR(128) NULL,
    ListTeamKey                             VARCHAR(128) NULL,
    BuyerAgentKey                           VARCHAR(128) NULL,
    BuyerOfficeKey                          VARCHAR(128) NULL,
    BuyerTeamKey                            VARCHAR(128) NULL,

    -- Raw payload and load metadata ---------------------------------
    raw_data                                JSON NOT NULL,
    loaded_at                               DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_property_modts      ON property(ModificationTimestamp);
CREATE INDEX idx_property_status     ON property(MlsStatus);
CREATE INDEX idx_property_type       ON property(PropertyType);
CREATE INDEX idx_property_city       ON property(City);
CREATE INDEX idx_property_postal     ON property(PostalCode);
CREATE INDEX idx_property_price      ON property(ListPrice);
CREATE INDEX idx_property_state      ON property(StateOrProvince);
