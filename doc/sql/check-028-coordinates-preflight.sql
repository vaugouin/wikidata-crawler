SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
-- WIKIDATA-CRAWLER-028, preflight before the first run with globe-coordinate support.
-- READ ONLY. Answers four questions before apply_to_live_db.sql touches the live DB:
--   1. the MariaDB version (appending an ENUM value is instant since 10.3);
--   2. the current definition of the three VALUE_TYPE ENUMs;
--   3. which BEFORE INSERT value triggers exist on the live DB (02 is the only file
--      that creates them, so the live DB may have none, or an old set);
--   4. what V2 knows of P625 today (expected: metadata row, unsupported, no statement).
-- Run: ssh vps-agent run wikidata-crawler/doc/sql/check-028-coordinates-preflight.sql

SELECT VERSION() AS MARIADB_VERSION;

SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND ((TABLE_NAME = 'T_WC_WIKIDATA_STATEMENT' AND COLUMN_NAME = 'VALUE_TYPE')
    OR (TABLE_NAME = 'T_WC_WIKIDATA_STATEMENT_QUALIFIER' AND COLUMN_NAME = 'VALUE_TYPE')
    OR (TABLE_NAME = 'T_WC_WIKIDATA_PROPERTY_METADATA' AND COLUMN_NAME = 'LOCAL_VALUE_TYPE'));

SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION
FROM information_schema.TRIGGERS
WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE LIKE 'T_WC_WIKIDATA%'
ORDER BY EVENT_OBJECT_TABLE;

SELECT TABLE_NAME, TABLE_ROWS
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE '%WIKIDATA%COORDINATE%';

SELECT ID_PROPERTY, PROPERTY_LABEL, WIKIDATA_DATATYPE, LOCAL_VALUE_TYPE, EXPECTED_CHILD_TABLE, IS_SUPPORTED
FROM T_WC_WIKIDATA_PROPERTY_METADATA
WHERE WIKIDATA_DATATYPE = 'globe-coordinate'
ORDER BY ID_PROPERTY
LIMIT 20;

SELECT COUNT(*) AS P625_STATEMENTS FROM T_WC_WIKIDATA_STATEMENT WHERE ID_PROPERTY = 'P625';

-- Scale of the run: cached places that will get coordinates (items with a P17 or a P131).
SELECT COUNT(DISTINCT ID_WIKIDATA) AS CACHED_PLACES_WITH_P17_OR_P131
FROM T_WC_WIKIDATA_STATEMENT WHERE ID_PROPERTY IN ('P17', 'P131');
