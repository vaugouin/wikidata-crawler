-- ============================================================================
-- WIKIDATA-CRAWLER-026 : entities with labels in many languages but no English
-- ============================================================================
--
-- READ ONLY. The hypothesis: Wikidata's multilingual default label 'mul' replaced the
-- redundant 'en' (and often 'fr') labels on items whose name is the same across
-- languages, and get_label(doc, "en"), which fills LABEL_EN, does not fall back to it.
-- Seen on 2026-10-09 in tmdb-movie-preprocess (process 73): Jane Eyre (Q182961) with
-- 78 label languages and no 'en', The Last of Us game (Q1986744) named only once
-- 'mul' was read.
--
-- Expectations, written before the run:
--   A. MUL_WITHOUT_EN is large on T_WC_WIKIDATA_ITEM and close to NO_EN_BUT_LABELS.
--      If it is near zero, the hypothesis is wrong and the dump itself must be read.
--   B. Q182961 and Q1986744 show a 'mul' label and no 'en'.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT 'A. Per entity table: labels without en, and how many of them carry mul' AS SECTION;

SELECT 'ITEM' AS ENTITY_TABLE, COUNT(*) AS ENTITIES,
       SUM(JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.mul')) AS WITH_MUL,
       SUM(JSON_LENGTH(LABELS_JSON) > 0 AND NOT JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.en')) AS NO_EN_BUT_LABELS,
       SUM(JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.mul') AND NOT JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.en')) AS MUL_WITHOUT_EN,
       SUM(COALESCE(LABEL_EN, '') = '') AS LABEL_EN_EMPTY
FROM T_WC_WIKIDATA_ITEM
UNION ALL
SELECT 'MOVIE', COUNT(*),
       SUM(JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.mul')),
       SUM(JSON_LENGTH(LABELS_JSON) > 0 AND NOT JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.en')),
       SUM(JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.mul') AND NOT JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.en')),
       SUM(COALESCE(LABEL_EN, '') = '')
FROM T_WC_WIKIDATA_MOVIE
UNION ALL
SELECT 'SERIE', COUNT(*),
       SUM(JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.mul')),
       SUM(JSON_LENGTH(LABELS_JSON) > 0 AND NOT JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.en')),
       SUM(JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.mul') AND NOT JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.en')),
       SUM(COALESCE(LABEL_EN, '') = '')
FROM T_WC_WIKIDATA_SERIE
UNION ALL
SELECT 'PERSON', COUNT(*),
       SUM(JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.mul')),
       SUM(JSON_LENGTH(LABELS_JSON) > 0 AND NOT JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.en')),
       SUM(JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.mul') AND NOT JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.en')),
       SUM(COALESCE(LABEL_EN, '') = '')
FROM T_WC_WIKIDATA_PERSON
UNION ALL
SELECT 'CHARACTER', COUNT(*),
       SUM(JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.mul')),
       SUM(JSON_LENGTH(LABELS_JSON) > 0 AND NOT JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.en')),
       SUM(JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.mul') AND NOT JSON_CONTAINS_PATH(LABELS_JSON, 'one', '$.en')),
       SUM(COALESCE(LABEL_EN, '') = '')
FROM T_WC_WIKIDATA_CHARACTER;

SELECT 'B. The two witnesses' AS SECTION;

SELECT ID_WIKIDATA, LABEL_EN,
       JSON_UNQUOTE(JSON_EXTRACT(LABELS_JSON, '$.mul')) AS LABEL_MUL,
       JSON_UNQUOTE(JSON_EXTRACT(LABELS_JSON, '$.en')) AS LABEL_JSON_EN,
       JSON_UNQUOTE(JSON_EXTRACT(LABELS_JSON, '$.fr')) AS LABEL_JSON_FR,
       JSON_LENGTH(LABELS_JSON) AS LANGUAGES
FROM T_WC_WIKIDATA_ITEM
WHERE ID_WIKIDATA IN ('Q182961', 'Q1986744');

SELECT 'C. Twenty items with mul and no en, the ones most cited by T2S works' AS SECTION;

SELECT wi.ID_WIKIDATA, JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.mul')) AS LABEL_MUL,
       JSON_LENGTH(wi.LABELS_JSON) AS LANGUAGES, COUNT(*) AS CITED_BY_STATEMENTS
FROM T_WC_WIKIDATA_ITEM wi
JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_ITEM = wi.ID_WIKIDATA
WHERE JSON_CONTAINS_PATH(wi.LABELS_JSON, 'one', '$.mul')
  AND NOT JSON_CONTAINS_PATH(wi.LABELS_JSON, 'one', '$.en')
GROUP BY wi.ID_WIKIDATA, LABEL_MUL, LANGUAGES
ORDER BY CITED_BY_STATEMENTS DESC
LIMIT 20;
