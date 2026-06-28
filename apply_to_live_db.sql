-- ============================================================================
-- apply_to_live_db.sql
--
-- Idempotent schema additions for an EXISTING vaugouindb database.
--
-- The canonical schema (01_create_schema.sql) only runs on a fresh database, so
-- a long-lived live DB never picks up newly-added tables. This script brings an
-- existing DB up to date for the SEASON / EPISODE / CHARACTER entity types added
-- to the V2 Wikidata crawler. It is safe to run repeatedly: every statement uses
-- CREATE TABLE IF NOT EXISTS and touches nothing that already exists.
--
-- It is executed automatically at the start of step 108 (load staging) and
-- step 110 (bulk load) by wikidata_crawler.py, so a normal run applies it for
-- free. It can also be run by hand:
--     docker exec -i damp-vaugouin-com-mariadb-1 mariadb -uroot -p \
--         --default-character-set=utf8mb4 vaugouindb < apply_to_live_db.sql
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Target entity tables (mirror T_WC_WIKIDATA_MOVIE in 01_create_schema.sql)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS T_WC_WIKIDATA_SEASON (
    ID_ROW              BIGINT NOT NULL AUTO_INCREMENT,
    ID_WIKIDATA         VARCHAR(50) NOT NULL,
    LABEL_EN            VARCHAR(500) DEFAULT NULL,
    DESCRIPTION_EN      TEXT DEFAULT NULL,
    LABELS_JSON         LONGTEXT DEFAULT NULL,
    DESCRIPTIONS_JSON   LONGTEXT DEFAULT NULL,
    DELETED             TINYINT(1) DEFAULT 0,
    DAT_CREAT           DATETIME DEFAULT CURRENT_TIMESTAMP,
    TIM_UPDATED         DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (ID_ROW),
    UNIQUE KEY UK_T_WC_WIKIDATA_SEASON_ID_WIKIDATA (ID_WIKIDATA),
    KEY IDX_T_WC_WIKIDATA_SEASON_LABEL_EN (LABEL_EN(255)),
    KEY IDX_T_WC_WIKIDATA_SEASON_DELETED (DELETED)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS T_WC_WIKIDATA_EPISODE (
    ID_ROW              BIGINT NOT NULL AUTO_INCREMENT,
    ID_WIKIDATA         VARCHAR(50) NOT NULL,
    LABEL_EN            VARCHAR(500) DEFAULT NULL,
    DESCRIPTION_EN      TEXT DEFAULT NULL,
    LABELS_JSON         LONGTEXT DEFAULT NULL,
    DESCRIPTIONS_JSON   LONGTEXT DEFAULT NULL,
    DELETED             TINYINT(1) DEFAULT 0,
    DAT_CREAT           DATETIME DEFAULT CURRENT_TIMESTAMP,
    TIM_UPDATED         DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (ID_ROW),
    UNIQUE KEY UK_T_WC_WIKIDATA_EPISODE_ID_WIKIDATA (ID_WIKIDATA),
    KEY IDX_T_WC_WIKIDATA_EPISODE_LABEL_EN (LABEL_EN(255)),
    KEY IDX_T_WC_WIKIDATA_EPISODE_DELETED (DELETED)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS T_WC_WIKIDATA_CHARACTER (
    ID_ROW              BIGINT NOT NULL AUTO_INCREMENT,
    ID_WIKIDATA         VARCHAR(50) NOT NULL,
    LABEL_EN            VARCHAR(500) DEFAULT NULL,
    DESCRIPTION_EN      TEXT DEFAULT NULL,
    LABELS_JSON         LONGTEXT DEFAULT NULL,
    DESCRIPTIONS_JSON   LONGTEXT DEFAULT NULL,
    DELETED             TINYINT(1) DEFAULT 0,
    DAT_CREAT           DATETIME DEFAULT CURRENT_TIMESTAMP,
    TIM_UPDATED         DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (ID_ROW),
    UNIQUE KEY UK_T_WC_WIKIDATA_CHARACTER_ID_WIKIDATA (ID_WIKIDATA),
    KEY IDX_T_WC_WIKIDATA_CHARACTER_LABEL_EN (LABEL_EN(255)),
    KEY IDX_T_WC_WIKIDATA_CHARACTER_DELETED (DELETED)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ----------------------------------------------------------------------------
-- Staging tables (mirror STG_T_WC_WIKIDATA_MOVIE in 02_staging_and_triggers.sql)
-- Full definitions (not CREATE ... LIKE) so this script is self-contained.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS STG_T_WC_WIKIDATA_SEASON (
    ID_STG_ROW          BIGINT NOT NULL AUTO_INCREMENT,
    IMPORT_BATCH_ID     VARCHAR(100) DEFAULT NULL,
    SOURCE_FILE         VARCHAR(500) DEFAULT NULL,
    ID_WIKIDATA         VARCHAR(50) NOT NULL,
    LABEL_EN            VARCHAR(500) DEFAULT NULL,
    DESCRIPTION_EN      TEXT DEFAULT NULL,
    LABELS_JSON         LONGTEXT DEFAULT NULL,
    DESCRIPTIONS_JSON   LONGTEXT DEFAULT NULL,
    ROW_STATUS          VARCHAR(20) DEFAULT 'NEW',
    ERROR_MESSAGE       TEXT DEFAULT NULL,
    DAT_IMPORT          DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ID_STG_ROW),
    KEY IDX_STG_SEASON_BATCH (IMPORT_BATCH_ID),
    KEY IDX_STG_SEASON_WIKIDATA (ID_WIKIDATA),
    KEY IDX_STG_SEASON_STATUS (ROW_STATUS)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS STG_T_WC_WIKIDATA_EPISODE (
    ID_STG_ROW          BIGINT NOT NULL AUTO_INCREMENT,
    IMPORT_BATCH_ID     VARCHAR(100) DEFAULT NULL,
    SOURCE_FILE         VARCHAR(500) DEFAULT NULL,
    ID_WIKIDATA         VARCHAR(50) NOT NULL,
    LABEL_EN            VARCHAR(500) DEFAULT NULL,
    DESCRIPTION_EN      TEXT DEFAULT NULL,
    LABELS_JSON         LONGTEXT DEFAULT NULL,
    DESCRIPTIONS_JSON   LONGTEXT DEFAULT NULL,
    ROW_STATUS          VARCHAR(20) DEFAULT 'NEW',
    ERROR_MESSAGE       TEXT DEFAULT NULL,
    DAT_IMPORT          DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ID_STG_ROW),
    KEY IDX_STG_EPISODE_BATCH (IMPORT_BATCH_ID),
    KEY IDX_STG_EPISODE_WIKIDATA (ID_WIKIDATA),
    KEY IDX_STG_EPISODE_STATUS (ROW_STATUS)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS STG_T_WC_WIKIDATA_CHARACTER (
    ID_STG_ROW          BIGINT NOT NULL AUTO_INCREMENT,
    IMPORT_BATCH_ID     VARCHAR(100) DEFAULT NULL,
    SOURCE_FILE         VARCHAR(500) DEFAULT NULL,
    ID_WIKIDATA         VARCHAR(50) NOT NULL,
    LABEL_EN            VARCHAR(500) DEFAULT NULL,
    DESCRIPTION_EN      TEXT DEFAULT NULL,
    LABELS_JSON         LONGTEXT DEFAULT NULL,
    DESCRIPTIONS_JSON   LONGTEXT DEFAULT NULL,
    ROW_STATUS          VARCHAR(20) DEFAULT 'NEW',
    ERROR_MESSAGE       TEXT DEFAULT NULL,
    DAT_IMPORT          DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ID_STG_ROW),
    KEY IDX_STG_CHARACTER_BATCH (IMPORT_BATCH_ID),
    KEY IDX_STG_CHARACTER_WIKIDATA (ID_WIKIDATA),
    KEY IDX_STG_CHARACTER_STATUS (ROW_STATUS)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ----------------------------------------------------------------------------
-- Widen YEAR_VALUE to BIGINT: Wikidata stores astronomical/geological years
-- (e.g. Big Bang +13800000000) that overflow signed INT (max 2147483647),
-- which raised error 1264 "Out of range value for column 'YEAR_VALUE'".
-- ALTER ... MODIFY to the same type is a harmless no-op, so this stays idempotent.
-- ----------------------------------------------------------------------------
ALTER TABLE T_WC_WIKIDATA_TIME_VALUE               MODIFY COLUMN YEAR_VALUE BIGINT DEFAULT NULL;
ALTER TABLE T_WC_WIKIDATA_QUALIFIER_TIME_VALUE     MODIFY COLUMN YEAR_VALUE BIGINT DEFAULT NULL;
ALTER TABLE STG_T_WC_WIKIDATA_TIME_VALUE           MODIFY COLUMN YEAR_VALUE BIGINT DEFAULT NULL;
ALTER TABLE STG_T_WC_WIKIDATA_QUALIFIER_TIME_VALUE MODIFY COLUMN YEAR_VALUE BIGINT DEFAULT NULL;
