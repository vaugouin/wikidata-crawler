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
-- WHY THE CUTOFF IS DERIVED AND NO LONGER WRITTEN BY HAND
--   Until 2026-09-02 this file carried a literal batch id, and that literal was
--   one run stale. Its failure mode is the dangerous kind, the silent one: a
--   cutoff older than everything in staging makes every DELETE match zero rows,
--   the script reports success, and the previous batch stays. Measured that day:
--   staging held wikidata_full_20260823_0317 (37 243 139 statements) alongside
--   wikidata_full_20260829_0417 (37 437 945), and running the file as written
--   would have removed neither.
--
--   The newest batch present in staging IS by definition the one to keep, so the
--   cutoff is now read from the data instead of being maintained by hand. This
--   also makes guard 1 structurally true rather than a check to remember: a value
--   taken from STG_T_WC_WIKIDATA_STATEMENT cannot be absent from it. If staging
--   is empty, MAX() returns NULL, every comparison is unknown, and nothing is
--   deleted.
--
--   To pin a different cutoff (replaying an old state, a batch id that does not
--   sort chronologically), replace the SELECT with a literal. Everything below
--   keeps working.
--
-- SAFETY
--   Guard 2 is now ENFORCED rather than merely documented: if the batch being
--   kept is not yet present in T_WC_WIKIDATA_STATEMENT, the cutoff is set to
--   NULL, which turns all 25 DELETEs into no-ops. Deliberately not a SIGNAL: this
--   file is run with --force (see COLLATION below), and --force carries on past
--   an error, so an abort would not stop the statements that follow it. A cutoff
--   that matches nothing stops them by construction.
--   The rule this guard encodes: older staging is disposable only once the new
--   data has landed in the target tables. Until then it is the one thing a
--   --start-step 110 resume could fall back on.
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
--   are utf8mb4_unicode_ci, and the cutoff is read from one of those columns, so
--   no #1267. Run with --force anyway, as everywhere in this repo.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---- 1. What is in staging right now ---------------------------------------
SELECT '1. Batches present in staging' AS SECTION;

SELECT IMPORT_BATCH_ID, COUNT(*) AS ROWS_STAGED
FROM STG_T_WC_WIKIDATA_STATEMENT
GROUP BY IMPORT_BATCH_ID
ORDER BY IMPORT_BATCH_ID;

-- ---- 2. The candidate cutoff, derived from the data ------------------------
-- The newest batch present in staging is by definition the one to keep. Nothing
-- to edit here, and guard 1 (the kept batch must be staged) is satisfied by
-- construction. Replace this SELECT with a literal only to pin another cutoff.
SET @CANDIDATE_BATCH_ID = (SELECT MAX(IMPORT_BATCH_ID) FROM STG_T_WC_WIKIDATA_STATEMENT);

-- ---- 3. Guard 2, enforced. A NULL cutoff disarms every DELETE below ---------
-- Older staging is disposable only once the new data has landed in the targets.
SET @KEEP_IS_LOADED = (SELECT EXISTS(
  SELECT 1 FROM T_WC_WIKIDATA_STATEMENT WHERE IMPORT_BATCH_ID = @CANDIDATE_BATCH_ID));

SET @IMPORT_BATCH_ID = IF(@KEEP_IS_LOADED = 1, @CANDIDATE_BATCH_ID, NULL);

SELECT '3. Decision' AS SECTION;

-- STATEMENTS_TO_DELETE walks the IMPORT_BATCH_ID index over a whole batch, so
-- this SELECT can take a minute on its own. It is the number the DELETEs below
-- will remove from the statement table, and the one to compare with section 5.
SELECT @CANDIDATE_BATCH_ID                                     AS NEWEST_BATCH_IN_STAGING,
       @KEEP_IS_LOADED                                         AS KEEP_IS_LOADED_IN_TARGET,
       @IMPORT_BATCH_ID                                        AS CUTOFF_APPLIED,
       (SELECT COUNT(*) FROM STG_T_WC_WIKIDATA_STATEMENT
        WHERE IMPORT_BATCH_ID < @IMPORT_BATCH_ID)              AS STATEMENTS_TO_DELETE,
       CASE WHEN @CANDIDATE_BATCH_ID IS NULL
            THEN 'DISARMED: staging is empty, nothing to do.'
            WHEN @IMPORT_BATCH_ID IS NULL
            THEN 'DISARMED: the newest staged batch is not in T_WC_WIKIDATA_STATEMENT yet. Run the bulk load first (--start-step 110). Nothing will be deleted.'
            ELSE 'ARMED: every staging row strictly older than CUTOFF_APPLIED will be deleted.'
       END                                                     AS VERDICT;

SET FOREIGN_KEY_CHECKS = 0;

-- ---- 4. The deletes ------------------------------------------------------
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

-- ---- 5. Verification -------------------------------------------------------
-- Exactly one batch should remain, the cutoff itself. If two are still listed,
-- read the VERDICT of section 3: a disarmed run deletes nothing and says so.
SELECT '5. Batches remaining in staging' AS SECTION;

SELECT IMPORT_BATCH_ID, COUNT(*) AS ROWS_STAGED
FROM STG_T_WC_WIKIDATA_STATEMENT
GROUP BY IMPORT_BATCH_ID
ORDER BY IMPORT_BATCH_ID;

-- InnoDB does not return the freed pages to the filesystem. To actually shrink
-- the .ibd files afterwards (locks the table, so pick a quiet moment):
--   OPTIMIZE TABLE STG_T_WC_WIKIDATA_STATEMENT;
