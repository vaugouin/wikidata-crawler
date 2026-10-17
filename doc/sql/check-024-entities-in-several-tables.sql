-- ============================================================================
-- WIKIDATA-CRAWLER-024 : entities present in more than one V2 entity table
-- ============================================================================
--
-- READ ONLY. Measured 2026-09-19 (doc/sql/archive/wikidata-v1-backfill-target-set-20260919.txt
-- section 3): 54 204 multiple memberships among the V1 floor alone, SERIE and EPISODE
-- first. The SERIE/EPISODE cleanup of 2026-09-28 and 2026-10-02 (f599c40 and
-- wikidata-serie-episode-cleanup.sql) has run since, so this re-measures on the
-- whole of the seven tables, not on the V1 floor only.
--
-- Expectations, written before the run:
--   A. Far fewer multiple memberships than 54 204, SERIE + EPISODE near zero.
--   B. What remains is mostly ITEM paired with a core table: a row written by
--      item_cache before the entity was classified as core, and never deleted
--      because step 110 upserts and does not remove (the -024 hypothesis).
--   C. The 632 entities typed episode (P31 = Q21191270) found in neither EPISODE
--      nor SERIE on 2026-10-16 sit in ITEM or SEASON, or in no table at all.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

DROP TEMPORARY TABLE IF EXISTS tmp_entity_membership;
CREATE TEMPORARY TABLE tmp_entity_membership (
    ID_WIKIDATA VARCHAR(50) NOT NULL,
    ENTITY_TABLE VARCHAR(10) NOT NULL,
    IS_DELETED TINYINT(1) NOT NULL,
    KEY (ID_WIKIDATA)
);

INSERT INTO tmp_entity_membership
SELECT ID_WIKIDATA, 'ITEM', COALESCE(DELETED, 0) FROM T_WC_WIKIDATA_ITEM
UNION ALL SELECT ID_WIKIDATA, 'MOVIE', COALESCE(DELETED, 0) FROM T_WC_WIKIDATA_MOVIE
UNION ALL SELECT ID_WIKIDATA, 'SERIE', COALESCE(DELETED, 0) FROM T_WC_WIKIDATA_SERIE
UNION ALL SELECT ID_WIKIDATA, 'PERSON', COALESCE(DELETED, 0) FROM T_WC_WIKIDATA_PERSON
UNION ALL SELECT ID_WIKIDATA, 'EPISODE', COALESCE(DELETED, 0) FROM T_WC_WIKIDATA_EPISODE
UNION ALL SELECT ID_WIKIDATA, 'SEASON', COALESCE(DELETED, 0) FROM T_WC_WIKIDATA_SEASON
UNION ALL SELECT ID_WIKIDATA, 'CHARACTER', COALESCE(DELETED, 0) FROM T_WC_WIKIDATA_CHARACTER;

DROP TEMPORARY TABLE IF EXISTS tmp_multi;
CREATE TEMPORARY TABLE tmp_multi (PRIMARY KEY (ID_WIKIDATA)) AS
SELECT ID_WIKIDATA,
       COUNT(*) AS N_TABLES,
       GROUP_CONCAT(ENTITY_TABLE ORDER BY ENTITY_TABLE SEPARATOR '+') AS TABLES
FROM tmp_entity_membership
GROUP BY ID_WIKIDATA
HAVING COUNT(*) > 1;

SELECT 'A. Rows per table, and entities found in more than one table' AS SECTION;

SELECT ENTITY_TABLE, COUNT(*) AS ROWS_TOTAL, SUM(IS_DELETED) AS ROWS_DELETED
FROM tmp_entity_membership
GROUP BY ENTITY_TABLE
ORDER BY ROWS_TOTAL DESC;

SELECT COUNT(*) AS ENTITIES_IN_SEVERAL_TABLES,
       COALESCE(SUM(N_TABLES - 1), 0) AS EXTRA_MEMBERSHIPS,
       (SELECT COUNT(DISTINCT ID_WIKIDATA) FROM tmp_entity_membership) AS DISTINCT_ENTITIES,
       '54 204 extra memberships on the V1 floor alone, 2026-09-19' AS REFERENCE
FROM tmp_multi;

SELECT 'B. The combinations, most frequent first' AS SECTION;

SELECT TABLES, COUNT(*) AS ENTITIES
FROM tmp_multi
GROUP BY TABLES
ORDER BY ENTITIES DESC;

SELECT 'B2. Five examples per combination (labels from the table that comes first)' AS SECTION;

SELECT TABLES, ID_WIKIDATA, LABEL_EN
FROM (SELECT m.TABLES, m.ID_WIKIDATA,
             COALESCE(i.LABEL_EN, mo.LABEL_EN, s.LABEL_EN, p.LABEL_EN, e.LABEL_EN, se.LABEL_EN, c.LABEL_EN) AS LABEL_EN,
             ROW_NUMBER() OVER (PARTITION BY m.TABLES ORDER BY m.ID_WIKIDATA) AS RN
      FROM tmp_multi m
      LEFT JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = m.ID_WIKIDATA
      LEFT JOIN T_WC_WIKIDATA_MOVIE mo ON mo.ID_WIKIDATA = m.ID_WIKIDATA
      LEFT JOIN T_WC_WIKIDATA_SERIE s ON s.ID_WIKIDATA = m.ID_WIKIDATA
      LEFT JOIN T_WC_WIKIDATA_PERSON p ON p.ID_WIKIDATA = m.ID_WIKIDATA
      LEFT JOIN T_WC_WIKIDATA_EPISODE e ON e.ID_WIKIDATA = m.ID_WIKIDATA
      LEFT JOIN T_WC_WIKIDATA_SEASON se ON se.ID_WIKIDATA = m.ID_WIKIDATA
      LEFT JOIN T_WC_WIKIDATA_CHARACTER c ON c.ID_WIKIDATA = m.ID_WIKIDATA) x
WHERE RN <= 5
ORDER BY TABLES, ID_WIKIDATA;

SELECT 'B3. Rows involved in a multiple membership, per table' AS SECTION;

SELECT em.ENTITY_TABLE, COUNT(*) AS ROWS_IN_MULTI, SUM(em.IS_DELETED) AS DELETED
FROM tmp_entity_membership em
JOIN tmp_multi m ON m.ID_WIKIDATA = em.ID_WIKIDATA
GROUP BY em.ENTITY_TABLE
ORDER BY ROWS_IN_MULTI DESC;

SELECT 'C. The entities typed episode found in neither EPISODE nor SERIE: where are they?' AS SECTION;

WITH ep AS (
    SELECT DISTINCT st.ID_WIKIDATA
    FROM T_WC_WIKIDATA_ITEM_VALUE iv
    JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_STATEMENT = iv.ID_STATEMENT
    WHERE iv.ID_ITEM = 'Q21191270' AND st.ID_PROPERTY = 'P31' AND st.DELETED = 0
)
SELECT COALESCE(em.ENTITY_TABLE, '(no entity table)') AS FOUND_IN,
       COUNT(*) AS ENTITIES
FROM ep
LEFT JOIN T_WC_WIKIDATA_EPISODE e ON e.ID_WIKIDATA = ep.ID_WIKIDATA
LEFT JOIN T_WC_WIKIDATA_SERIE s ON s.ID_WIKIDATA = ep.ID_WIKIDATA
LEFT JOIN tmp_entity_membership em ON em.ID_WIKIDATA = ep.ID_WIKIDATA
WHERE e.ID_WIKIDATA IS NULL AND s.ID_WIKIDATA IS NULL
GROUP BY em.ENTITY_TABLE
ORDER BY ENTITIES DESC;

DROP TEMPORARY TABLE IF EXISTS tmp_multi;
DROP TEMPORARY TABLE IF EXISTS tmp_entity_membership;

SELECT '========== FIN ==========' AS SECTION;
