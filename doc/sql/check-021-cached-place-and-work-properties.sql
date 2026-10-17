-- ============================================================================
-- WIKIDATA-CRAWLER-021 : P131, P17, P50 and P7937 on cached items
-- ============================================================================
--
-- READ ONLY. Commit 68d896d (2026-10-09) added P131, P17, P50 and P7937 to
-- CACHED_ENTITY_PROPERTIES. The first run carrying it is the 2026-10-13 dump
-- (batch wikidata_full_20261013_0817). T_WC_WIKIDATA_ITEM holds the cached items
-- only (item_cache never writes a core entity there), so a statement whose subject
-- is in ITEM was emitted by item_cache, and for these four properties it is the
-- volume -021 added. The older batches are deleted by step 114, so there is no
-- "before" to subtract: the ITEM column is the measure.
--
-- Expectations, written before the run:
--   A. P131 is the bulk of the ITEM column; P50 and P7937 are much smaller.
--   B. Q90 (Paris) carries a P131 and a P17 (France, Q142).
--   C. Q470937 (The Shining, novel) carries a P50 (Stephen King, Q39829) and a
--      P7937 (novel, Q8261).
--   D. Among cached items typed literary work (Q7725634), a clear share now
--      carries a P7937, which makes the novel / short story form of
--      TMDB-MOVIE-PREPROCESS-055 computable.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT 'A. Statements per property: on cached items (added by -021) and elsewhere' AS SECTION;

SELECT s.ID_PROPERTY,
       COUNT(*) AS STATEMENTS,
       SUM(i.ID_WIKIDATA IS NOT NULL) AS ON_CACHED_ITEMS,
       SUM(i.ID_WIKIDATA IS NULL) AS ON_OTHER_ENTITIES,
       COUNT(DISTINCT CASE WHEN i.ID_WIKIDATA IS NOT NULL THEN s.ID_WIKIDATA END) AS CACHED_ITEMS_CARRYING_IT
FROM T_WC_WIKIDATA_STATEMENT s
LEFT JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = s.ID_WIKIDATA
WHERE s.ID_PROPERTY IN ('P131', 'P17', 'P50', 'P7937')
  AND s.DELETED = 0
GROUP BY s.ID_PROPERTY
ORDER BY ON_CACHED_ITEMS DESC;

SELECT 'B. and C. The witnesses: Q90 Paris, Q470937 The Shining' AS SECTION;

SELECT s.ID_WIKIDATA,
       COALESCE(i.LABEL_EN, '(not in ITEM)') AS SUBJECT_LABEL,
       s.ID_PROPERTY,
       v.ID_ITEM AS VALUE_ID,
       COALESCE(vi.LABEL_EN, vp.LABEL_EN) AS VALUE_LABEL,
       s.RANK,
       s.IMPORT_BATCH_ID
FROM T_WC_WIKIDATA_STATEMENT s
LEFT JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = s.ID_WIKIDATA
LEFT JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM vi ON vi.ID_WIKIDATA = v.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_PERSON vp ON vp.ID_WIKIDATA = v.ID_ITEM
WHERE s.ID_WIKIDATA IN ('Q90', 'Q470937')
  AND s.ID_PROPERTY IN ('P131', 'P17', 'P50', 'P7937')
  AND s.DELETED = 0
ORDER BY s.ID_WIKIDATA, s.ID_PROPERTY, v.ID_ITEM;

SELECT 'D. Cached items typed literary work (P31 = Q7725634): how many carry P7937, P50' AS SECTION;

SELECT COUNT(*) AS LITERARY_WORKS,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT f
                   WHERE f.ID_WIKIDATA = lw.ID_WIKIDATA AND f.ID_PROPERTY = 'P7937' AND f.DELETED = 0)) AS WITH_P7937,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT a
                   WHERE a.ID_WIKIDATA = lw.ID_WIKIDATA AND a.ID_PROPERTY = 'P50' AND a.DELETED = 0)) AS WITH_P50
FROM (SELECT DISTINCT s.ID_WIKIDATA
      FROM T_WC_WIKIDATA_STATEMENT s
      JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
      JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = s.ID_WIKIDATA
      WHERE s.ID_PROPERTY = 'P31' AND v.ID_ITEM = 'Q7725634' AND s.DELETED = 0) lw;

SELECT 'D2. The P7937 forms found on cached items, most frequent first' AS SECTION;

SELECT v.ID_ITEM AS FORM_ID, vi.LABEL_EN AS FORM_LABEL, COUNT(*) AS STATEMENTS
FROM T_WC_WIKIDATA_STATEMENT s
JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = s.ID_WIKIDATA
JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM vi ON vi.ID_WIKIDATA = v.ID_ITEM
WHERE s.ID_PROPERTY = 'P7937' AND s.DELETED = 0
GROUP BY v.ID_ITEM, vi.LABEL_EN
ORDER BY STATEMENTS DESC
LIMIT 15;

SELECT '========== FIN ==========' AS SECTION;
