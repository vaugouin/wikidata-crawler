-- ============================================================================
-- 09_fix_value_type_conflicts.sql
-- Manual repair for trigger error 1644:
--   "<TABLE>: statement already exists in another child table"
--   "<TABLE>: qualifier already exists in another child table"
-- raised during the bulk load (step 110) by the "one statement -> one value
-- table" / "one qualifier -> one value table" BEFORE INSERT triggers in
-- 02_staging_and_triggers.sql.
--
-- CAUSE
--   ID_STATEMENT / ID_STATEMENT_QUALIFIER are deterministic hashes of the
--   statement GUID / qualifier snak, so they are stable across dumps. When a
--   statement's (or qualifier's) VALUE_TYPE changes between two dumps (a
--   property's datatype changed, or the classifier changed), the upsert-only
--   bulk load updates the parent VALUE_TYPE but leaves the EARLIER batch's value
--   row in the now-wrong sibling table. The triggers then abort the new value
--   row.
--
-- FIX
--   Delete, for the batch you are loading (@IMPORT_BATCH_ID), every target value
--   row whose statement/qualifier is classified as a DIFFERENT type in this
--   batch's staging. Only contradicting rows are removed; rows the current batch
--   agrees with are kept, so no current-batch data is lost.
--
-- NOTE
--   The same six deletes are now built into 03_bulk_load_from_staging_FULL.sql
--   (sections "3B" and the qualifier purge), so a fresh run self-heals and this
--   script is only needed to unblock an in-flight load without rebuilding the
--   Docker image. Idempotent; safe to re-run. Run it, then resume:
--       ./wikidata-crawler.sh --start-step 110
-- ============================================================================

-- >>> set to the IMPORT_BATCH_ID in your .env (the batch step 110 loads) <<<
SET @IMPORT_BATCH_ID := 'wikidata_full_20260623_1710';

-- ---- (A) COUNT first: how many contradicting rows exist --------------------
SELECT 'item'        AS value_table, COUNT(*) AS n FROM T_WC_WIKIDATA_ITEM_VALUE v
  WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT s
               WHERE s.ID_STATEMENT=v.ID_STATEMENT AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'item')
UNION ALL
SELECT 'string',      COUNT(*) FROM T_WC_WIKIDATA_STRING_VALUE v
  WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT s
               WHERE s.ID_STATEMENT=v.ID_STATEMENT AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'string')
UNION ALL
SELECT 'external_id', COUNT(*) FROM T_WC_WIKIDATA_EXTERNAL_ID_VALUE v
  WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT s
               WHERE s.ID_STATEMENT=v.ID_STATEMENT AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'external_id')
UNION ALL
SELECT 'media',       COUNT(*) FROM T_WC_WIKIDATA_MEDIA_VALUE v
  WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT s
               WHERE s.ID_STATEMENT=v.ID_STATEMENT AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'media')
UNION ALL
SELECT 'time',        COUNT(*) FROM T_WC_WIKIDATA_TIME_VALUE v
  WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT s
               WHERE s.ID_STATEMENT=v.ID_STATEMENT AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'time')
UNION ALL
SELECT 'quantity',    COUNT(*) FROM T_WC_WIKIDATA_QUANTITY_VALUE v
  WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT s
               WHERE s.ID_STATEMENT=v.ID_STATEMENT AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'quantity');

-- ---- (B) DELETE contradicting STATEMENT value rows -------------------------
SET FOREIGN_KEY_CHECKS = 0;

DELETE v FROM T_WC_WIKIDATA_ITEM_VALUE v
 WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT s
              WHERE s.ID_STATEMENT=v.ID_STATEMENT AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'item');

DELETE v FROM T_WC_WIKIDATA_STRING_VALUE v
 WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT s
              WHERE s.ID_STATEMENT=v.ID_STATEMENT AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'string');

DELETE v FROM T_WC_WIKIDATA_EXTERNAL_ID_VALUE v
 WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT s
              WHERE s.ID_STATEMENT=v.ID_STATEMENT AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'external_id');

DELETE v FROM T_WC_WIKIDATA_MEDIA_VALUE v
 WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT s
              WHERE s.ID_STATEMENT=v.ID_STATEMENT AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'media');

DELETE v FROM T_WC_WIKIDATA_TIME_VALUE v
 WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT s
              WHERE s.ID_STATEMENT=v.ID_STATEMENT AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'time');

DELETE v FROM T_WC_WIKIDATA_QUANTITY_VALUE v
 WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT s
              WHERE s.ID_STATEMENT=v.ID_STATEMENT AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'quantity');

-- ---- (C) DELETE contradicting QUALIFIER value rows ------------------------
DELETE v FROM T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE v
 WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT_QUALIFIER s
              WHERE s.ID_STATEMENT_QUALIFIER=v.ID_STATEMENT_QUALIFIER AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'item');

DELETE v FROM T_WC_WIKIDATA_QUALIFIER_STRING_VALUE v
 WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT_QUALIFIER s
              WHERE s.ID_STATEMENT_QUALIFIER=v.ID_STATEMENT_QUALIFIER AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'string');

DELETE v FROM T_WC_WIKIDATA_QUALIFIER_EXTERNAL_ID_VALUE v
 WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT_QUALIFIER s
              WHERE s.ID_STATEMENT_QUALIFIER=v.ID_STATEMENT_QUALIFIER AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'external_id');

DELETE v FROM T_WC_WIKIDATA_QUALIFIER_MEDIA_VALUE v
 WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT_QUALIFIER s
              WHERE s.ID_STATEMENT_QUALIFIER=v.ID_STATEMENT_QUALIFIER AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'media');

DELETE v FROM T_WC_WIKIDATA_QUALIFIER_TIME_VALUE v
 WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT_QUALIFIER s
              WHERE s.ID_STATEMENT_QUALIFIER=v.ID_STATEMENT_QUALIFIER AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'time');

DELETE v FROM T_WC_WIKIDATA_QUALIFIER_QUANTITY_VALUE v
 WHERE EXISTS (SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT_QUALIFIER s
              WHERE s.ID_STATEMENT_QUALIFIER=v.ID_STATEMENT_QUALIFIER AND s.IMPORT_BATCH_ID=@IMPORT_BATCH_ID AND s.VALUE_TYPE<>'quantity');

SET FOREIGN_KEY_CHECKS = 1;
