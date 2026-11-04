-- Aliases (ALIASES_JSON, WIKIDATA-CRAWLER-025) per entity table, and what the Wikidata
-- aliases of persons would add to T_WC_TMDB_PERSON_ALSO_KNOWN_AS. Read-only (SELECT and a
-- temporary table).
--
-- Section 2 sizes an idea, it writes nothing: enrich the TMDb person alias table from the
-- Wikidata aliases (fr + en), deduplicated. "Already there" is measured three ways, from the
-- strictest to the loosest: same string under the table collation (utf8mb4_unicode_ci, the
-- one the UNIQUE (ID_PERSON, PERSON_NAME) key enforces), same as the TMDb NAME, same
-- PERSON_NAME_KEY (the generated key of the alias table, rebuilt here with its own expression).

SELECT NOW() AS NOW_;

-- 1. Aliases per entity table: rows, rows carrying at least one alias, rows with fr, with en,
-- and the number of aliases per language.
SELECT 'MOVIE' AS TBL, COUNT(*) AS ROWS_, SUM(ALIASES_JSON IS NOT NULL) AS COL_FILLED,
       SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}') AS WITH_ALIASES,
       SUM(JSON_CONTAINS_PATH(ALIASES_JSON, 'one', '$.fr')) AS WITH_FR,
       SUM(JSON_CONTAINS_PATH(ALIASES_JSON, 'one', '$.en')) AS WITH_EN,
       SUM(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.fr'), 0)) AS ALIASES_FR,
       SUM(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.en'), 0)) AS ALIASES_EN
FROM T_WC_WIKIDATA_MOVIE WHERE DELETED = 0
UNION ALL
SELECT 'SERIE', COUNT(*), SUM(ALIASES_JSON IS NOT NULL),
       SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'),
       SUM(JSON_CONTAINS_PATH(ALIASES_JSON, 'one', '$.fr')),
       SUM(JSON_CONTAINS_PATH(ALIASES_JSON, 'one', '$.en')),
       SUM(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.fr'), 0)),
       SUM(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.en'), 0))
FROM T_WC_WIKIDATA_SERIE WHERE DELETED = 0
UNION ALL
SELECT 'SEASON', COUNT(*), SUM(ALIASES_JSON IS NOT NULL),
       SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'),
       SUM(JSON_CONTAINS_PATH(ALIASES_JSON, 'one', '$.fr')),
       SUM(JSON_CONTAINS_PATH(ALIASES_JSON, 'one', '$.en')),
       SUM(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.fr'), 0)),
       SUM(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.en'), 0))
FROM T_WC_WIKIDATA_SEASON WHERE DELETED = 0
UNION ALL
SELECT 'EPISODE', COUNT(*), SUM(ALIASES_JSON IS NOT NULL),
       SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'),
       SUM(JSON_CONTAINS_PATH(ALIASES_JSON, 'one', '$.fr')),
       SUM(JSON_CONTAINS_PATH(ALIASES_JSON, 'one', '$.en')),
       SUM(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.fr'), 0)),
       SUM(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.en'), 0))
FROM T_WC_WIKIDATA_EPISODE WHERE DELETED = 0
UNION ALL
SELECT 'PERSON', COUNT(*), SUM(ALIASES_JSON IS NOT NULL),
       SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'),
       SUM(JSON_CONTAINS_PATH(ALIASES_JSON, 'one', '$.fr')),
       SUM(JSON_CONTAINS_PATH(ALIASES_JSON, 'one', '$.en')),
       SUM(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.fr'), 0)),
       SUM(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.en'), 0))
FROM T_WC_WIKIDATA_PERSON WHERE DELETED = 0
UNION ALL
SELECT 'CHARACTER', COUNT(*), SUM(ALIASES_JSON IS NOT NULL),
       SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'),
       SUM(JSON_CONTAINS_PATH(ALIASES_JSON, 'one', '$.fr')),
       SUM(JSON_CONTAINS_PATH(ALIASES_JSON, 'one', '$.en')),
       SUM(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.fr'), 0)),
       SUM(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.en'), 0))
FROM T_WC_WIKIDATA_CHARACTER WHERE DELETED = 0
UNION ALL
SELECT 'ITEM', COUNT(*), SUM(ALIASES_JSON IS NOT NULL),
       SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'),
       SUM(JSON_CONTAINS_PATH(ALIASES_JSON, 'one', '$.fr')),
       SUM(JSON_CONTAINS_PATH(ALIASES_JSON, 'one', '$.en')),
       SUM(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.fr'), 0)),
       SUM(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.en'), 0))
FROM T_WC_WIKIDATA_ITEM WHERE DELETED = 0;

-- 2. Persons: the Wikidata aliases of the persons TMDb knows (T_WC_TMDB_PERSON.ID_WIKIDATA),
-- one row per (TMDb person, language, alias), cut to 200 characters like PERSON_NAME.
DROP TEMPORARY TABLE IF EXISTS tmp_wd_person_alias;
CREATE TEMPORARY TABLE tmp_wd_person_alias (
    ID_PERSON   INT NOT NULL,
    LANG        CHAR(2) NOT NULL,
    ALIAS       VARCHAR(200) NOT NULL,
    TMDB_NAME   VARCHAR(500) DEFAULT NULL,
    ALIAS_KEY   VARCHAR(255) DEFAULT NULL,
    KEY IDX_PERSON (ID_PERSON)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO tmp_wd_person_alias (ID_PERSON, LANG, ALIAS, TMDB_NAME)
SELECT tp.ID_PERSON, 'fr', LEFT(j.ALIAS, 200), tp.NAME
FROM T_WC_WIKIDATA_PERSON wp
INNER JOIN T_WC_TMDB_PERSON tp ON tp.ID_WIKIDATA = wp.ID_WIKIDATA AND COALESCE(tp.DELETED, 0) = 0
INNER JOIN JSON_TABLE(wp.ALIASES_JSON, '$.fr[*]' COLUMNS (ALIAS VARCHAR(500) PATH '$')) j
WHERE wp.DELETED = 0 AND wp.ALIASES_JSON LIKE '%"fr"%' AND j.ALIAS IS NOT NULL AND j.ALIAS <> '';

INSERT INTO tmp_wd_person_alias (ID_PERSON, LANG, ALIAS, TMDB_NAME)
SELECT tp.ID_PERSON, 'en', LEFT(j.ALIAS, 200), tp.NAME
FROM T_WC_WIKIDATA_PERSON wp
INNER JOIN T_WC_TMDB_PERSON tp ON tp.ID_WIKIDATA = wp.ID_WIKIDATA AND COALESCE(tp.DELETED, 0) = 0
INNER JOIN JSON_TABLE(wp.ALIASES_JSON, '$.en[*]' COLUMNS (ALIAS VARCHAR(500) PATH '$')) j
WHERE wp.DELETED = 0 AND wp.ALIASES_JSON LIKE '%"en"%' AND j.ALIAS IS NOT NULL AND j.ALIAS <> '';

-- Same expression as the generated PERSON_NAME_KEY of T_WC_TMDB_PERSON_ALSO_KNOWN_AS.
UPDATE tmp_wd_person_alias
SET ALIAS_KEY = REPLACE(LCASE(REGEXP_REPLACE(REGEXP_REPLACE(ALIAS, '[^\\p{L}\\p{M}\\p{N} ]+', ' '), ' +', ' ')), ' ', '');

-- 2a. Volume: alias rows, distinct (person, alias) under the collation, persons.
SELECT COUNT(*) AS ALIAS_ROWS,
       COUNT(DISTINCT ID_PERSON, ALIAS) AS DISTINCT_PERSON_ALIAS,
       COUNT(DISTINCT ID_PERSON) AS PERSONS,
       SUM(LANG = 'fr') AS ROWS_FR, SUM(LANG = 'en') AS ROWS_EN
FROM tmp_wd_person_alias;

-- 2b. Each distinct (person, alias), classified by the first test that finds it already held.
SELECT STATUS_, COUNT(*) AS PERSON_ALIASES, COUNT(DISTINCT ID_PERSON) AS PERSONS
FROM (
    SELECT t.ID_PERSON, t.ALIAS,
           CASE
             WHEN t.ALIAS = t.TMDB_NAME THEN '1 same as TMDb NAME'
             WHEN EXISTS (SELECT 1 FROM T_WC_TMDB_PERSON_ALSO_KNOWN_AS a
                          WHERE a.ID_PERSON = t.ID_PERSON AND a.PERSON_NAME = t.ALIAS) THEN '2 in alias table, same string'
             WHEN EXISTS (SELECT 1 FROM T_WC_TMDB_PERSON_ALSO_KNOWN_AS a
                          WHERE a.ID_PERSON = t.ID_PERSON AND a.PERSON_NAME_KEY = t.ALIAS_KEY) THEN '3 in alias table, same key'
             WHEN t.ALIAS_KEY = REPLACE(LCASE(REGEXP_REPLACE(REGEXP_REPLACE(t.TMDB_NAME, '[^\\p{L}\\p{M}\\p{N} ]+', ' '), ' +', ' ')), ' ', '')
                  THEN '4 same key as TMDb NAME'
             ELSE '5 new'
           END AS STATUS_
    FROM (SELECT ID_PERSON, ALIAS, MIN(TMDB_NAME) AS TMDB_NAME, MIN(ALIAS_KEY) AS ALIAS_KEY
          FROM tmp_wd_person_alias GROUP BY ID_PERSON, ALIAS) t
) s
GROUP BY STATUS_
ORDER BY STATUS_;

-- 2c. Twenty-five new aliases, persons with the most TMDb popularity first, to judge quality.
SELECT t.ID_PERSON, p.NAME, p.POPULARITY, t.LANG, t.ALIAS
FROM tmp_wd_person_alias t
INNER JOIN T_WC_TMDB_PERSON p ON p.ID_PERSON = t.ID_PERSON
WHERE t.ALIAS <> COALESCE(t.TMDB_NAME, '')
  AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_PERSON_ALSO_KNOWN_AS a
                  WHERE a.ID_PERSON = t.ID_PERSON AND (a.PERSON_NAME = t.ALIAS OR a.PERSON_NAME_KEY = t.ALIAS_KEY))
ORDER BY p.POPULARITY DESC, t.ID_PERSON, t.LANG, t.ALIAS
LIMIT 25;

DROP TEMPORARY TABLE IF EXISTS tmp_wd_person_alias;
