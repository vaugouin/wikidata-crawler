-- ============================================================================
-- Reclaim the space freed by the step-114 entity prune (WIKIDATA-CRAWLER-024)
-- ============================================================================
--
-- MAINTENANCE, run by Philippe (route 2: OPTIMIZE is DDL, the agent account cannot):
--   cd ~/docker/wikidata-crawler && git pull
--   ~/docker/tools/runsqlvaugouindb.sh ~/docker/wikidata-crawler/doc/sql/optimize-item-serie-after-entity-prune.sql
--
-- Why. The first run on image 1.0.1 deletes the entity rows the batch no longer
-- stages: 40 224 in ITEM and 11 942 in SERIE by the 2026-10-17 dry run
-- (check-024-entity-prune-dry-run-20261017.txt), 4.5 % and 6.7 % of the tables.
-- InnoDB keeps the freed pages, so the files do not shrink by themselves.
--
-- When. After that run only, and between two crawls: section 0 must show SUCCESS,
-- no current pass, and a non-empty entity prune detail. OPTIMIZE on InnoDB rebuilds
-- the table (online in MariaDB) and needs free disk about the size of the rebuilt
-- table: check `df -h` on the VPS first, about 4 GB free is enough.
--
-- Expectation, written before the run: ITEM a few percent under its 2 808 MB of
-- 2026-10-17 (after the alias OPTIMIZE), SERIE some 5 to 7 % under its size before;
-- free_mb near 0 after. Nothing changes in the rows (sections 1 and 3 counts equal).
-- ============================================================================

SELECT '=== 0 . crawler state (must be SUCCESS, no current pass, prune done) ===' AS section;

SELECT VAR_NAME, VAR_VALUE, TIM_UPDATED
FROM T_WC_SERVER_VARIABLE
WHERE VAR_NAME IN ('strwikidatacrawlerstatus', 'strwikidatacrawlercurrentpass',
                   'strwikidatacrawlerenddatetime', 'strwikidatacrawlercurrentprocess',
                   'strwikidatacrawlerentityprunerowsdeleted', 'strwikidatacrawlerentityprunedetail');

SELECT '=== 1 . size before ===' AS section;

SELECT TABLE_NAME, TABLE_ROWS AS rows_estimated,
       ROUND(DATA_LENGTH / 1024 / 1024) AS data_mb,
       ROUND(INDEX_LENGTH / 1024 / 1024) AS index_mb,
       ROUND(DATA_FREE / 1024 / 1024) AS free_mb
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('T_WC_WIKIDATA_ITEM', 'T_WC_WIKIDATA_SERIE');

SELECT 'ITEM' AS t, COUNT(*) AS rows_exact FROM T_WC_WIKIDATA_ITEM
UNION ALL SELECT 'SERIE', COUNT(*) FROM T_WC_WIKIDATA_SERIE;

SELECT '=== 2 . OPTIMIZE ===' AS section;

OPTIMIZE TABLE T_WC_WIKIDATA_ITEM;
OPTIMIZE TABLE T_WC_WIKIDATA_SERIE;

SELECT '=== 3 . size after ===' AS section;

SELECT TABLE_NAME, TABLE_ROWS AS rows_estimated,
       ROUND(DATA_LENGTH / 1024 / 1024) AS data_mb,
       ROUND(INDEX_LENGTH / 1024 / 1024) AS index_mb,
       ROUND(DATA_FREE / 1024 / 1024) AS free_mb
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('T_WC_WIKIDATA_ITEM', 'T_WC_WIKIDATA_SERIE');

SELECT 'ITEM' AS t, COUNT(*) AS rows_exact FROM T_WC_WIKIDATA_ITEM
UNION ALL SELECT 'SERIE', COUNT(*) FROM T_WC_WIKIDATA_SERIE;

SELECT '========== FIN ==========' AS section;
