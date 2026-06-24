-- Trestle ETL Pipeline: MySQL schema.
--
-- The data is split across two tables to keep search/scan queries fast:
--
--   * `property`     — the typed Promoted_Columns plus `loaded_at`. This is
--                      the "hot" table every search hits. It carries NO large
--                      JSON payload, so full scans and index range scans read
--                      only narrow rows. Every Promoted_Column other than the
--                      `ListingKey` primary key has a secondary index.
--   * `property_raw` — the full unmodified RESO payload (`raw_data JSON`)
--                      keyed 1:1 by `ListingKey`. Queries JOIN to this table
--                      only when they actually need the raw document, so the
--                      ~31 KB/row JSON never weighs down a search.
--
-- Requirements:
--   6.1 InnoDB engine with utf8mb4 / utf8mb4_unicode_ci.
--   6.2 `ListingKey VARCHAR(128) NOT NULL PRIMARY KEY` on both tables.
--   6.3 All Promoted_Columns present as typed columns on `property`.
--   6.4 `raw_data JSON NOT NULL` preserves the full source payload
--       (now on `property_raw`).
--   6.5 Secondary indexes on every non-PK Promoted_Column.
--   6.6 `loaded_at DATETIME(6) NOT NULL` without `DEFAULT CURRENT_TIMESTAMP`;
--       the Loader supplies this value at commit time (Requirement 6.7).

CREATE TABLE property (
    ListingKey                              VARCHAR(128) NOT NULL PRIMARY KEY,
    ListingId                               VARCHAR(128) NULL,
    MlsStatus                               VARCHAR(64) NULL,
    InternetEntireListingDisplayYN          TINYINT(1) NULL,
    InternetAddressDisplayYN                TINYINT(1) NULL,
    InternetAutomatedValuationDisplayYN     TINYINT(1) NULL,
    InternetConsumerCommentYN               TINYINT(1) NULL,
    Latitude                                DECIMAL(10,7) NULL,
    Longitude                               DECIMAL(10,7) NULL,
    ParcelNumber                            VARCHAR(64) NULL,
    StreetNumberNumeric                     INT NULL,
    StreetDirPrefix                         VARCHAR(16) NULL,
    StreetName                              VARCHAR(128) NULL,
    StreetSuffix                            VARCHAR(32) NULL,
    UnitNumber                              VARCHAR(32) NULL,
    City                                    VARCHAR(64) NULL,
    StateOrProvince                         VARCHAR(2) NULL,
    PostalCode                              VARCHAR(16) NULL,
    OriginalListPrice                       DECIMAL(14,2) NULL,
    ListPrice                               DECIMAL(14,2) NULL,
    ClosePrice                              DECIMAL(14,2) NULL,
    ModificationTimestamp                   DATETIME(6) NULL,
    OriginalEntryTimestamp                  DATETIME(6) NULL,
    PendingTimestamp                        DATETIME(6) NULL,
    StatusChangeTimestamp                   DATETIME(6) NULL,
    WithdrawnDate                           DATE NULL,
    CloseDate                               DATE NULL,
    PhotosChangeTimestamp                   DATETIME(6) NULL,
    PhotosCount                             INT NULL,
    VideosCount                             INT NULL,
    PropertyType                            VARCHAR(64) NULL,
    PropertySubType                         VARCHAR(64) NULL,
    PropertySubTypeAdditional               VARCHAR(128) NULL,
    StructureType                           VARCHAR(128) NULL,
    YearBuiltDetails                        VARCHAR(128) NULL,
    ArchitecturalStyle                      VARCHAR(128) NULL,
    PropertyAttachedYN                      TINYINT(1) NULL,
    Stories                                 SMALLINT NULL,
    LivingArea                              DECIMAL(10,2) NULL,
    LotSizeSquareFeet                       DECIMAL(12,2) NULL,
    BedroomsTotal                           SMALLINT NULL,
    BathroomsFull                           SMALLINT NULL,
    BathroomsHalf                           SMALLINT NULL,
    BathroomsThreeQuarter                   SMALLINT NULL,
    GarageSpaces                            DECIMAL(6,2) NULL,
    YearBuilt                               SMALLINT NULL,
    YearBuiltEffective                      SMALLINT NULL,
    PoolPrivateYN                           TINYINT(1) NULL,
    SpaYN                                   TINYINT(1) NULL,
    DirectionFaces                          VARCHAR(32) NULL,
    SeniorCommunityYN                       TINYINT(1) NULL,
    AssociationYN                           TINYINT(1) NULL,
    AssociationAmenities                    VARCHAR(512) NULL,
    HorseAmenities                          VARCHAR(512) NULL,
    PetsAllowedYN                           TINYINT(1) NULL,
    Furnished                               VARCHAR(32) NULL,
    ListAgentKey                            VARCHAR(128) NULL,
    ListOfficeKey                           VARCHAR(128) NULL,
    ListTeamKey                             VARCHAR(128) NULL,
    BuyerAgentKey                           VARCHAR(128) NULL,
    BuyerOfficeKey                          VARCHAR(128) NULL,
    BuyerTeamKey                            VARCHAR(128) NULL,
    loaded_at                               DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE property_raw (
    ListingKey      VARCHAR(128) NOT NULL PRIMARY KEY,
    raw_data        JSON NOT NULL,
    loaded_at       DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Secondary indexes: one per non-PK Promoted_Column (Requirement 6.5).
-- The two VARCHAR(512) multi-select columns use a 255-char prefix to keep
-- index entries compact; every other column is indexed in full.
CREATE INDEX idx_property_ListingId                            ON property(ListingId);
CREATE INDEX idx_property_MlsStatus                            ON property(MlsStatus);
CREATE INDEX idx_property_InternetEntireListingDisplayYN       ON property(InternetEntireListingDisplayYN);
CREATE INDEX idx_property_InternetAddressDisplayYN             ON property(InternetAddressDisplayYN);
CREATE INDEX idx_property_InternetAutomatedValuationDisplayYN  ON property(InternetAutomatedValuationDisplayYN);
CREATE INDEX idx_property_InternetConsumerCommentYN            ON property(InternetConsumerCommentYN);
CREATE INDEX idx_property_Latitude                             ON property(Latitude);
CREATE INDEX idx_property_Longitude                            ON property(Longitude);
CREATE INDEX idx_property_ParcelNumber                         ON property(ParcelNumber);
CREATE INDEX idx_property_StreetNumberNumeric                  ON property(StreetNumberNumeric);
CREATE INDEX idx_property_StreetDirPrefix                      ON property(StreetDirPrefix);
CREATE INDEX idx_property_StreetName                           ON property(StreetName);
CREATE INDEX idx_property_StreetSuffix                         ON property(StreetSuffix);
CREATE INDEX idx_property_UnitNumber                           ON property(UnitNumber);
CREATE INDEX idx_property_City                                 ON property(City);
CREATE INDEX idx_property_StateOrProvince                      ON property(StateOrProvince);
CREATE INDEX idx_property_PostalCode                           ON property(PostalCode);
CREATE INDEX idx_property_OriginalListPrice                    ON property(OriginalListPrice);
CREATE INDEX idx_property_ListPrice                            ON property(ListPrice);
CREATE INDEX idx_property_ClosePrice                           ON property(ClosePrice);
CREATE INDEX idx_property_ModificationTimestamp                ON property(ModificationTimestamp);
CREATE INDEX idx_property_OriginalEntryTimestamp               ON property(OriginalEntryTimestamp);
CREATE INDEX idx_property_PendingTimestamp                     ON property(PendingTimestamp);
CREATE INDEX idx_property_StatusChangeTimestamp                ON property(StatusChangeTimestamp);
CREATE INDEX idx_property_WithdrawnDate                        ON property(WithdrawnDate);
CREATE INDEX idx_property_CloseDate                            ON property(CloseDate);
CREATE INDEX idx_property_PhotosChangeTimestamp                ON property(PhotosChangeTimestamp);
CREATE INDEX idx_property_PhotosCount                          ON property(PhotosCount);
CREATE INDEX idx_property_VideosCount                          ON property(VideosCount);
CREATE INDEX idx_property_PropertyType                         ON property(PropertyType);
CREATE INDEX idx_property_PropertySubType                      ON property(PropertySubType);
CREATE INDEX idx_property_PropertySubTypeAdditional            ON property(PropertySubTypeAdditional);
CREATE INDEX idx_property_StructureType                        ON property(StructureType);
CREATE INDEX idx_property_YearBuiltDetails                     ON property(YearBuiltDetails);
CREATE INDEX idx_property_ArchitecturalStyle                   ON property(ArchitecturalStyle);
CREATE INDEX idx_property_PropertyAttachedYN                   ON property(PropertyAttachedYN);
CREATE INDEX idx_property_Stories                              ON property(Stories);
CREATE INDEX idx_property_LivingArea                           ON property(LivingArea);
CREATE INDEX idx_property_LotSizeSquareFeet                    ON property(LotSizeSquareFeet);
CREATE INDEX idx_property_BedroomsTotal                        ON property(BedroomsTotal);
CREATE INDEX idx_property_BathroomsFull                        ON property(BathroomsFull);
CREATE INDEX idx_property_BathroomsHalf                        ON property(BathroomsHalf);
CREATE INDEX idx_property_BathroomsThreeQuarter                ON property(BathroomsThreeQuarter);
CREATE INDEX idx_property_GarageSpaces                         ON property(GarageSpaces);
CREATE INDEX idx_property_YearBuilt                            ON property(YearBuilt);
CREATE INDEX idx_property_YearBuiltEffective                   ON property(YearBuiltEffective);
CREATE INDEX idx_property_PoolPrivateYN                        ON property(PoolPrivateYN);
CREATE INDEX idx_property_SpaYN                                ON property(SpaYN);
CREATE INDEX idx_property_DirectionFaces                       ON property(DirectionFaces);
CREATE INDEX idx_property_SeniorCommunityYN                    ON property(SeniorCommunityYN);
CREATE INDEX idx_property_AssociationYN                        ON property(AssociationYN);
CREATE INDEX idx_property_AssociationAmenities                 ON property(AssociationAmenities(255));
CREATE INDEX idx_property_HorseAmenities                       ON property(HorseAmenities(255));
CREATE INDEX idx_property_PetsAllowedYN                        ON property(PetsAllowedYN);
CREATE INDEX idx_property_Furnished                            ON property(Furnished);
CREATE INDEX idx_property_ListAgentKey                         ON property(ListAgentKey);
CREATE INDEX idx_property_ListOfficeKey                        ON property(ListOfficeKey);
CREATE INDEX idx_property_ListTeamKey                          ON property(ListTeamKey);
CREATE INDEX idx_property_BuyerAgentKey                        ON property(BuyerAgentKey);
CREATE INDEX idx_property_BuyerOfficeKey                       ON property(BuyerOfficeKey);
CREATE INDEX idx_property_BuyerTeamKey                         ON property(BuyerTeamKey);
