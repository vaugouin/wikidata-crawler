-- ============================================================================
-- 13_cleanup_staging_old_batches.sql
-- Delete every STG_* staging row belonging to a batch OLDER than the current one,
-- so that after a successful run staging holds exactly one batch: the one that
-- just finished.
--
-- WHY
--   Step 108 deletes only the CURRENT IMPORT_BATCH_ID before loading it (that is
--   what makes a --start-step 108 resume safe), and step 114 prunes the TARGET
--   tables only. So without this cleanup every run leaves its predecessor behind
--   in staging: two full batches, well over 100 M rows and several GB that
--   nothing ever reads again.
--
-- RELATION TO THE PIPELINE
--   This is the hand-runnable twin of step 115 (`step_cleanup_staging_batches` in
--   wikidata_crawler.py), which runs at the end of every successful run. The
--   crawler does NOT execute this file: it issues the same DELETEs in committed
--   chunks of 50 000 rows and derives its table list from TABLE_SPECS in
--   load_staging_jsonl.py. Use this file to clean up a database by hand, or to
--   catch up a run that predates step 115.
--
-- RELATION TO 10_clear_staging_batch.sql
--   10 removes ONE named batch (@OLD_BATCH_ID). This file removes ALL batches
--   older than the current one in a single pass. Use 10 when you want to drop a
--   specific batch, this one for the routine "keep only the latest" cleanup.
--
-- CUTOFF SEMANTICS
--   Strictly-older comparison ( < @IMPORT_BATCH_ID ), identical to step 114 on
--   the target tables. The recommended batch id format
--   `wikidata_full_YYYYMMDD_HHMM` sorts lexicographically in chronological order,
--   so a plain string `<` is a valid "is older than" test. The current batch (=)
--   and any hypothetical newer batch (>) are left intact, and so are rows with a
--   NULL IMPORT_BATCH_ID (NULL < x is unknown). A batch whose id does not sort
--   chronologically therefore survives, and needs 10_clear_staging_batch.sql.
--
-- SAFETY
--   Set @IMPORT_BATCH_ID to the CURRENT run's batch id, i.e. the one you want to
--   KEEP, not the one you want to delete. Run the two checks below first: if the
--   current batch is not in staging, an id that sorts above every real batch
--   would empty the staging tables completely. Only run this once the current
--   batch is loaded into T_WC_WIKIDATA_STATEMENT: until then, older staging is
--   the one thing a --start-step 110 resume could fall back on.
--
-- PERFORMANCE
--   Each statement below deletes a whole batch of that table (tens of millions
--   of rows for the statement/value tables) in ONE transaction, and can run for
--   several minutes. There is no wrapping transaction, so each autocommits on
--   its own and the file can be interrupted and re-run. If the undo log or the
--   binlog is a concern on a busy server, delete in chunks instead, repeating this
--   until it reports 0 rows affected, for each table:
--     DELETE FROM STG_T_WC_WIKIDATA_STATEMENT
--     WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID LIMIT 50000;
--   (that is exactly what step 115 does).
--
--   IMPORT_BATCH_ID is indexed on every staging table, so the comparison is kept
--   bare: putting COLLATE on the column would lose the index. All STG_* tables
--   are utf8mb4_unicode_ci, and a string literal is coercible, so no #1267.
-- ============================================================================

SET NAMES utf8mb4;
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- >>> set to the CURRENT batch id, the one to KEEP <<<
SET @IMPORT_BATCH_ID = 'wikidata_full_20260823_0317';

-- ---- Pre-flight checks (run these before the DELETEs) ----------------------
-- 1. What is actually in staging, and how much of it:
--      SELECT IMPORT_BATCH_ID, COUNT(*) AS ROWS_STAGED
--      FROM STG_T_WC_WIKIDATA_STATEMENT
--      GROUP BY IMPORT_BATCH_ID ORDER BY IMPORT_BATCH_ID;
-- 2. Guard 1, the batch you are keeping must be present in staging:
--      SELECT EXISTS(SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT
--                    WHERE IMPORT_BATCH_ID = @IMPORT_BATCH_ID) AS KEEP_IS_STAGED;
-- 3. Guard 2, and already loaded into the target tables:
--      SELECT EXISTS(SELECT 1 FROM T_WC_WIKIDATA_STATEMENT
--                    WHERE IMPORT_BATCH_ID = @IMPORT_BATCH_ID) AS KEEP_IS_LOADED;
--    Both must return 1. If either returns 0, do not run the DELETEs.

SET FOREIGN_KEY_CHECKS = 0;

-- ---- Entity + metadata staging --------------------------------------------
DELETE FROM STG_T_WC_WIKIDATA_MOVIE                       WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_SERIE                       WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_PERSON                      WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_ITEM                        WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_SEASON                      WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_EPISODE                     WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_CHARACTER                   WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_PROPERTY_METADATA           WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_SUBCLASS                    WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;

-- ---- Statement + main typed-value staging ---------------------------------
DELETE FROM STG_T_WC_WIKIDATA_STATEMENT                   WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_ITEM_VALUE                  WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_STRING_VALUE                WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_EXTERNAL_ID_VALUE           WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_MEDIA_VALUE                 WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_TIME_VALUE                  WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_QUANTITY_VALUE              WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;

-- ---- Qualifier + qualifier typed-value staging ----------------------------
DELETE FROM STG_T_WC_WIKIDATA_STATEMENT_QUALIFIER         WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE        WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_QUALIFIER_STRING_VALUE      WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_QUALIFIER_EXTERNAL_ID_VALUE WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_QUALIFIER_MEDIA_VALUE       WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_QUALIFIER_TIME_VALUE        WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_QUALIFIER_QUANTITY_VALUE    WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;

-- ---- Media-resource staging -----------------------------------------------
-- Never written by the pipeline (step 112 fills the target tables directly), so
-- these are normally empty. Kept here for the case where something loaded them.
DELETE FROM STG_T_WC_WIKIDATA_MEDIA_RESOURCE              WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_MEDIA_RESOURCE_URL          WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK        WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID;

SET FOREIGN_KEY_CHECKS = 1;

-- ---- Verification ----------------------------------------------------------
-- Exactly one batch should remain:
--   SELECT IMPORT_BATCH_ID, COUNT(*) AS ROWS_STAGED
--   FROM STG_T_WC_WIKIDATA_STATEMENT
--   GROUP BY IMPORT_BATCH_ID ORDER BY IMPORT_BATCH_ID;
--
-- InnoDB does not return the freed pages to the filesystem. To actually shrink
-- the .ibd files afterwards (locks the table, so pick a quiet moment):
--   OPTIMIZE TABLE STG_T_WC_WIKIDATA_STATEMENT;
