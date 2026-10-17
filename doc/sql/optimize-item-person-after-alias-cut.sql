-- ============================================================================
-- Reclaim the space freed by the fr + en alias cut (WIKIDATA-CRAWLER-025)
-- ============================================================================
--
-- MAINTENANCE, run by Philippe (route 2: OPTIMIZE is DDL, the agent account cannot):
--   cd ~/docker/wikidata-crawler && git pull
--   ~/docker/tools/runsqlvaugouindb.sh ~/docker/wikidata-crawler/doc/sql/optimize-item-person-after-alias-cut.sql
--
-- Why. Since 2026-09-27 extract_aliases keeps fr + en only, and step 110 overwrote
-- ALIASES_JSON in place, but InnoDB does not hand freed pages back to the disk
-- (AGENTS.md, ALIASES_JSON paragraph). Run report of 2026-10-16, A3b: ITEM 4.45 GB,
-- PERSON 1.67 GB, against 2 367 MB and 1 404 MB on 2026-09-20 before any alias was
-- collected, while ITEM only grew by about 25 % in rows (backfill of -023).
--
-- When. Between two crawls only: never while run-if-new-dump.sh is running (check
-- section 0 below says SUCCESS and no current process). OPTIMIZE on InnoDB rebuilds
-- the table (online in MariaDB, reads and writes keep working) and needs free disk
-- space about the size of the rebuilt table: check `df -h` on the VPS first, about
-- 5 GB free is enough.
--
-- Expectation, written before the run: ITEM drops to roughly 2.5 to 3 GB, PERSON
-- to roughly 1.4 GB. Nothing changes in the rows (section 2 counts are equal).
-- ============================================================================

SELECT '=== 0 . crawler state (must be SUCCESS, no current pass) ===' AS section;

SELECT VAR_NAME, VAR_VALUE, TIM_UPDATED
FROM T_WC_SERVER_VARIABLE
WHERE VAR_NAME IN ('strwikidatacrawlerstatus', 'strwikidatacrawlercurrentpass',
                   'strwikidatacrawlerenddatetime', 'strwikidatacrawlercurrentprocess');

SELECT '=== 1 . size before ===' AS section;

SELECT TABLE_NAME, TABLE_ROWS AS rows_estimated,
       ROUND(DATA_LENGTH / 1024 / 1024) AS data_mb,
       ROUND(INDEX_LENGTH / 1024 / 1024) AS index_mb,
       ROUND(DATA_FREE / 1024 / 1024) AS free_mb
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('T_WC_WIKIDATA_ITEM', 'T_WC_WIKIDATA_PERSON');

SELECT 'ITEM' AS t, COUNT(*) AS rows_exact FROM T_WC_WIKIDATA_ITEM
UNION ALL SELECT 'PERSON', COUNT(*) FROM T_WC_WIKIDATA_PERSON;

SELECT '=== 2 . OPTIMIZE ===' AS section;

OPTIMIZE TABLE T_WC_WIKIDATA_ITEM;
OPTIMIZE TABLE T_WC_WIKIDATA_PERSON;

SELECT '=== 3 . size after ===' AS section;

SELECT TABLE_NAME, TABLE_ROWS AS rows_estimated,
       ROUND(DATA_LENGTH / 1024 / 1024) AS data_mb,
       ROUND(INDEX_LENGTH / 1024 / 1024) AS index_mb,
       ROUND(DATA_FREE / 1024 / 1024) AS free_mb
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('T_WC_WIKIDATA_ITEM', 'T_WC_WIKIDATA_PERSON');

SELECT 'ITEM' AS t, COUNT(*) AS rows_exact FROM T_WC_WIKIDATA_ITEM
UNION ALL SELECT 'PERSON', COUNT(*) FROM T_WC_WIKIDATA_PERSON;

SELECT '========== FIN ==========' AS section;
