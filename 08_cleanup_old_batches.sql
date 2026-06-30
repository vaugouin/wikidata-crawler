-- ============================================================================
-- Old-batch cleanup
-- Removes every target-table row left behind by a PREVIOUS full run, i.e. rows
-- whose IMPORT_BATCH_ID is strictly older than the current batch.
--
-- Why this is needed:
--   The bulk load (03_bulk_load_from_staging_FULL.sql) only INSERTs/UPSERTs the
--   CURRENT batch's staging rows. Statements that were present in an earlier run
--   but are no longer emitted (entity went out of scope, claim deleted/edited)
--   keep their OLD IMPORT_BATCH_ID and are never overwritten or removed. This
--   script prunes those stale "orphan" rows so the live tables match the latest
--   dump.
--
-- Cutoff semantics:
--   Strictly-older comparison ( < @IMPORT_BATCH_ID ). The recommended batch id
--   format is `wikidata_full_YYYYMMDD_HHMM`, which sorts lexicographically in
--   chronological order, so a plain string `<` is a valid "is older than" test.
--   The current batch (=) and any hypothetical newer batch (>) are left intact.
--   Rows with a NULL IMPORT_BATCH_ID are left intact (NULL < x is unknown).
--
-- NOT touched (by design):
--   - Entity tables (MOVIE/SERIE/PERSON/ITEM/SEASON/EPISODE/CHARACTER) and
--     PROPERTY_METADATA have no IMPORT_BATCH_ID column. They are upserted in
--     place (one row per ID_WIKIDATA / ID_PROPERTY) and never accumulate one
--     row per batch, so there is nothing batch-scoped to prune.
--
-- Idempotent: re-running deletes whatever old-batch rows remain (none on a
-- second pass). Safe to run standalone in MariaDB after editing @IMPORT_BATCH_ID,
-- or via wikidata_crawler.py step 114 (which substitutes the batch id for you).
--
-- Important operational note:
--   Set @IMPORT_BATCH_ID to the CURRENT run's batch id before running.
-- ============================================================================

SET NAMES utf8mb4;

SET @IMPORT_BATCH_ID = 'BATCH_20260309_001';

SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------------------------------------------------------
-- 1. MEDIA RESOLUTION LAYER (children of T_WC_WIKIDATA_MEDIA_RESOURCE)
--    Deleted first because MEDIA_RESOURCE references T_WC_WIKIDATA_STATEMENT.
-- ----------------------------------------------------------------------------

DELETE u
FROM T_WC_WIKIDATA_MEDIA_RESOURCE_URL u
JOIN T_WC_WIKIDATA_MEDIA_RESOURCE m ON m.ID_MEDIA_RESOURCE = u.ID_MEDIA_RESOURCE
WHERE m.IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

DELETE c
FROM T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK c
JOIN T_WC_WIKIDATA_MEDIA_RESOURCE m ON m.ID_MEDIA_RESOURCE = c.ID_MEDIA_RESOURCE
WHERE m.IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

DELETE FROM T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK
WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

DELETE FROM T_WC_WIKIDATA_MEDIA_RESOURCE
WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

-- ----------------------------------------------------------------------------
-- 2. QUALIFIER TYPED-VALUE TABLES (children of T_WC_WIKIDATA_STATEMENT_QUALIFIER)
--    A qualifier always shares its parent statement's IMPORT_BATCH_ID, so
--    filtering on the qualifier's own batch id is consistent with step 4.
-- ----------------------------------------------------------------------------

DELETE qv
FROM T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qv
JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q ON q.ID_STATEMENT_QUALIFIER = qv.ID_STATEMENT_QUALIFIER
WHERE q.IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

DELETE qv
FROM T_WC_WIKIDATA_QUALIFIER_STRING_VALUE qv
JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q ON q.ID_STATEMENT_QUALIFIER = qv.ID_STATEMENT_QUALIFIER
WHERE q.IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

DELETE qv
FROM T_WC_WIKIDATA_QUALIFIER_EXTERNAL_ID_VALUE qv
JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q ON q.ID_STATEMENT_QUALIFIER = qv.ID_STATEMENT_QUALIFIER
WHERE q.IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

DELETE qv
FROM T_WC_WIKIDATA_QUALIFIER_MEDIA_VALUE qv
JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q ON q.ID_STATEMENT_QUALIFIER = qv.ID_STATEMENT_QUALIFIER
WHERE q.IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

DELETE qv
FROM T_WC_WIKIDATA_QUALIFIER_TIME_VALUE qv
JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q ON q.ID_STATEMENT_QUALIFIER = qv.ID_STATEMENT_QUALIFIER
WHERE q.IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

DELETE qv
FROM T_WC_WIKIDATA_QUALIFIER_QUANTITY_VALUE qv
JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q ON q.ID_STATEMENT_QUALIFIER = qv.ID_STATEMENT_QUALIFIER
WHERE q.IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

-- ----------------------------------------------------------------------------
-- 3. QUALIFIER PARENT
-- ----------------------------------------------------------------------------

DELETE FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER
WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

-- ----------------------------------------------------------------------------
-- 4. MAIN TYPED-VALUE TABLES (children of T_WC_WIKIDATA_STATEMENT)
-- ----------------------------------------------------------------------------

DELETE v
FROM T_WC_WIKIDATA_ITEM_VALUE v
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_STATEMENT = v.ID_STATEMENT
WHERE s.IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

DELETE v
FROM T_WC_WIKIDATA_STRING_VALUE v
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_STATEMENT = v.ID_STATEMENT
WHERE s.IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

DELETE v
FROM T_WC_WIKIDATA_EXTERNAL_ID_VALUE v
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_STATEMENT = v.ID_STATEMENT
WHERE s.IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

DELETE v
FROM T_WC_WIKIDATA_MEDIA_VALUE v
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_STATEMENT = v.ID_STATEMENT
WHERE s.IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

DELETE v
FROM T_WC_WIKIDATA_TIME_VALUE v
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_STATEMENT = v.ID_STATEMENT
WHERE s.IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

DELETE v
FROM T_WC_WIKIDATA_QUANTITY_VALUE v
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_STATEMENT = v.ID_STATEMENT
WHERE s.IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

-- ----------------------------------------------------------------------------
-- 5. STATEMENT PARENT
-- ----------------------------------------------------------------------------

DELETE FROM T_WC_WIKIDATA_STATEMENT
WHERE IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci < @IMPORT_BATCH_ID;

SET FOREIGN_KEY_CHECKS = 1;
