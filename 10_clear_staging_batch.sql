-- ============================================================================
-- 10_clear_staging_batch.sql
-- Delete every STG_* staging row belonging to ONE specific import batch.
--
-- WHY
--   The pipeline loads staging (step 108) but never deletes it afterwards, so a
--   staging table can accumulate more than one batch if it is not cleared
--   between runs. Step 114 prunes the TARGET tables only; the old batch's
--   staging rows (tens of millions of rows / several GB) stay behind. This
--   script removes exactly one old batch from every staging table, leaving the
--   current batch intact.
--
-- WHEN TO USE
--   After a successful run that stacked a new batch on top of an old one in
--   staging, to reclaim space and avoid a confusing "two batches in staging"
--   state. It is a surgical alternative to 04_reset_for_full_rerun.sql (which
--   clears BOTH staging and targets for a full rebuild).
--
-- SAFETY
--   Set @OLD_BATCH_ID to the batch you want to REMOVE. It MUST be an old batch,
--   NOT the batch currently loaded into the target tables. Deleting the current
--   batch's staging is harmless to already-loaded target data but would prevent
--   a --start-step 108/110 resume of that batch.
--
--   Faster alternative: once a batch is fully loaded and validated into the
--   target tables, ALL staging is disposable. If no batch needs a staging-level
--   resume, you can TRUNCATE every STG_* table instead of this per-batch delete
--   (much faster, no per-row undo/binlog). This script is the conservative,
--   keep-the-current-batch option.
--
-- NOTE
--   Big DELETEs: on a full Wikidata batch this touches tens of millions of rows
--   and can run for several minutes per large table. Each statement autocommits
--   on its own (no wrapping transaction), so it will not build one giant undo.
-- ============================================================================

-- >>> set to the OLD batch id you want to remove from staging <<<
-- NOTE: the STG_*.IMPORT_BATCH_ID columns are not all the same collation
-- (this DB mixes utf8mb4_general_ci / utf8mb4_unicode_ci), so every comparison
-- below forces utf8mb4_unicode_ci on BOTH sides to avoid error #1267
-- ("Illegal mix of collations"), exactly as the crawler does in step 110.
SET @OLD_BATCH_ID := CONVERT('wikidata_full_20260509_1730' USING utf8mb4) COLLATE utf8mb4_unicode_ci;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ---- Entity + metadata staging --------------------------------------------
DELETE FROM STG_T_WC_WIKIDATA_MOVIE                    WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_SERIE                    WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_PERSON                   WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_ITEM                     WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_SEASON                   WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_EPISODE                  WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_CHARACTER                WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_PROPERTY_METADATA        WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;

-- ---- Statement + main typed-value staging ---------------------------------
DELETE FROM STG_T_WC_WIKIDATA_STATEMENT                WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_ITEM_VALUE               WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_STRING_VALUE             WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_EXTERNAL_ID_VALUE        WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_MEDIA_VALUE              WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_TIME_VALUE               WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_QUANTITY_VALUE           WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;

-- ---- Qualifier + qualifier typed-value staging ----------------------------
DELETE FROM STG_T_WC_WIKIDATA_STATEMENT_QUALIFIER      WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE     WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_QUALIFIER_STRING_VALUE   WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_QUALIFIER_EXTERNAL_ID_VALUE WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_QUALIFIER_MEDIA_VALUE    WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_QUALIFIER_TIME_VALUE     WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_QUALIFIER_QUANTITY_VALUE WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;

-- ---- Media-resource staging -----------------------------------------------
DELETE FROM STG_T_WC_WIKIDATA_MEDIA_RESOURCE           WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_MEDIA_RESOURCE_URL       WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;
DELETE FROM STG_T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK     WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @OLD_BATCH_ID;

SET FOREIGN_KEY_CHECKS = 1;
