-- ============================================================================
-- DATA-CONTRIBUTION-003 : Wikidata films, series and persons with no TMDb entry
-- ============================================================================
--
-- READ ONLY. Candidates for new TMDb entries (the Creator badges, 10 to 1,000
-- new entries) and, as a by-product, Wikidata items that miss a TMDb id TMDb
-- already has (a write-back for Wikidata, not a new TMDb entry).
--
-- A candidate is a V2 entity (T_WC_WIKIDATA_MOVIE / SERIE / PERSON, DELETED = 0)
-- that carries an IMDb id (P345), carries no TMDb id on Wikidata (P4947 movie,
-- P4983 series, P4985 person), and is unknown to the local TMDb copy both by QID
-- (T_WC_TMDB_*.ID_WIKIDATA) and by IMDb id (T_WC_TMDB_*.ID_IMDB). The IMDb id is
-- required: TMDb asks for one on creation, and it is the check that the work or
-- the person really exists outside Wikidata.
--
-- Notability is approximated by the number of label languages
-- (JSON_LENGTH(LABELS_JSON)): the V2 load keeps no sitelink count, and
-- T_WC_WIKIPEDIA_PAGE_LANG only covers entities already in T2S.
--
-- Each candidate table is built once in a temporary table, so the statement
-- lookups (index IDX_T_WC_WIKIDATA_STATEMENT_WIKIDATA_PROPERTY) run once per
-- entity.
--
-- Expectations, written before the first run (2026-11-04):
--   A. Movie candidates in the thousands, far more than series; persons possibly
--      the largest family, since TMDb only knows persons credited somewhere.
--   B. The top of each list is dominated by old or non-English works and persons
--      (silent films, Indian, Soviet, Japanese cinema), labelled in 20+ languages.
--   C. TMDB_HAS_IT (Wikidata lacks the TMDb id, TMDb has the IMDb id) is non-zero:
--      those rows are Wikidata edits, not TMDb creations.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT NOW() AS NOW_;

-- ---------------------------------------------------------------------------
-- Movies
-- ---------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS TMP_DC003_MOVIE;
CREATE TEMPORARY TABLE TMP_DC003_MOVIE AS
SELECT m.ID_WIKIDATA,
       m.LABEL_EN,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(m.LABELS_JSON, '$.fr.value')),
                JSON_UNQUOTE(JSON_EXTRACT(m.LABELS_JSON, '$.fr'))) AS LABEL_FR,
       COALESCE(JSON_LENGTH(m.LABELS_JSON), 0) AS LABEL_LANGS,
       (SELECT MIN(ev.VALUE_EXTERNAL_ID)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P345' AND s.DELETED = 0) AS ID_IMDB,
       (SELECT MIN(tv.YEAR_VALUE)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_TIME_VALUE tv ON tv.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P577' AND s.DELETED = 0) AS YEAR_,
       EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P4947' AND s.DELETED = 0) AS HAS_TMDB_ON_WIKIDATA
FROM T_WC_WIKIDATA_MOVIE m
WHERE m.DELETED = 0;

SELECT 'A. Movies: candidates and Wikidata write-backs' AS SECTION;
SELECT COUNT(*) AS ENTITIES,
       SUM(ID_IMDB IS NOT NULL) AS WITH_IMDB,
       SUM(ID_IMDB IS NOT NULL AND HAS_TMDB_ON_WIKIDATA = 0) AS WITH_IMDB_NO_TMDB_ON_WIKIDATA,
       SUM(ID_IMDB IS NOT NULL AND HAS_TMDB_ON_WIKIDATA = 0
           AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_MOVIE t WHERE t.ID_WIKIDATA = c.ID_WIKIDATA)
           AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_MOVIE t WHERE t.ID_IMDB = c.ID_IMDB)) AS TMDB_CANDIDATES,
       SUM(ID_IMDB IS NOT NULL AND HAS_TMDB_ON_WIKIDATA = 0
           AND EXISTS (SELECT 1 FROM T_WC_TMDB_MOVIE t WHERE t.ID_IMDB = c.ID_IMDB)) AS TMDB_HAS_IT
FROM TMP_DC003_MOVIE c;

SELECT 'B. Movies: top 200 TMDb candidates by label languages' AS SECTION;
SELECT c.ID_WIKIDATA, c.ID_IMDB, c.YEAR_, c.LABEL_LANGS, c.LABEL_EN, c.LABEL_FR
FROM TMP_DC003_MOVIE c
WHERE c.ID_IMDB IS NOT NULL AND c.HAS_TMDB_ON_WIKIDATA = 0
  AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_MOVIE t WHERE t.ID_WIKIDATA = c.ID_WIKIDATA)
  AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_MOVIE t WHERE t.ID_IMDB = c.ID_IMDB)
ORDER BY c.LABEL_LANGS DESC, c.YEAR_
LIMIT 200;

SELECT 'C. Movies: 50 Wikidata items missing the TMDb id TMDb already has (P4947 to add)' AS SECTION;
SELECT c.ID_WIKIDATA, c.ID_IMDB, t.ID_MOVIE AS ID_TMDB, c.LABEL_LANGS, c.LABEL_EN
FROM TMP_DC003_MOVIE c
INNER JOIN T_WC_TMDB_MOVIE t ON t.ID_IMDB = c.ID_IMDB
WHERE c.ID_IMDB IS NOT NULL AND c.HAS_TMDB_ON_WIKIDATA = 0
ORDER BY c.LABEL_LANGS DESC
LIMIT 50;

DROP TEMPORARY TABLE IF EXISTS TMP_DC003_MOVIE;

-- ---------------------------------------------------------------------------
-- Series
-- ---------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS TMP_DC003_SERIE;
CREATE TEMPORARY TABLE TMP_DC003_SERIE AS
SELECT m.ID_WIKIDATA,
       m.LABEL_EN,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(m.LABELS_JSON, '$.fr.value')),
                JSON_UNQUOTE(JSON_EXTRACT(m.LABELS_JSON, '$.fr'))) AS LABEL_FR,
       COALESCE(JSON_LENGTH(m.LABELS_JSON), 0) AS LABEL_LANGS,
       (SELECT MIN(ev.VALUE_EXTERNAL_ID)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P345' AND s.DELETED = 0) AS ID_IMDB,
       (SELECT MIN(tv.YEAR_VALUE)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_TIME_VALUE tv ON tv.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY IN ('P580', 'P577') AND s.DELETED = 0) AS YEAR_,
       EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P4983' AND s.DELETED = 0) AS HAS_TMDB_ON_WIKIDATA
FROM T_WC_WIKIDATA_SERIE m
WHERE m.DELETED = 0;

SELECT 'D. Series: candidates and Wikidata write-backs' AS SECTION;
SELECT COUNT(*) AS ENTITIES,
       SUM(ID_IMDB IS NOT NULL) AS WITH_IMDB,
       SUM(ID_IMDB IS NOT NULL AND HAS_TMDB_ON_WIKIDATA = 0) AS WITH_IMDB_NO_TMDB_ON_WIKIDATA,
       SUM(ID_IMDB IS NOT NULL AND HAS_TMDB_ON_WIKIDATA = 0
           AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_SERIE t WHERE t.ID_WIKIDATA = c.ID_WIKIDATA)
           AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_SERIE t WHERE t.ID_IMDB = c.ID_IMDB)) AS TMDB_CANDIDATES,
       SUM(ID_IMDB IS NOT NULL AND HAS_TMDB_ON_WIKIDATA = 0
           AND EXISTS (SELECT 1 FROM T_WC_TMDB_SERIE t WHERE t.ID_IMDB = c.ID_IMDB)) AS TMDB_HAS_IT
FROM TMP_DC003_SERIE c;

SELECT 'E. Series: top 100 TMDb candidates by label languages' AS SECTION;
SELECT c.ID_WIKIDATA, c.ID_IMDB, c.YEAR_, c.LABEL_LANGS, c.LABEL_EN, c.LABEL_FR
FROM TMP_DC003_SERIE c
WHERE c.ID_IMDB IS NOT NULL AND c.HAS_TMDB_ON_WIKIDATA = 0
  AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_SERIE t WHERE t.ID_WIKIDATA = c.ID_WIKIDATA)
  AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_SERIE t WHERE t.ID_IMDB = c.ID_IMDB)
ORDER BY c.LABEL_LANGS DESC, c.YEAR_
LIMIT 100;

DROP TEMPORARY TABLE IF EXISTS TMP_DC003_SERIE;

-- ---------------------------------------------------------------------------
-- Persons
-- ---------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS TMP_DC003_PERSON;
CREATE TEMPORARY TABLE TMP_DC003_PERSON AS
SELECT m.ID_WIKIDATA,
       m.LABEL_EN,
       COALESCE(JSON_LENGTH(m.LABELS_JSON), 0) AS LABEL_LANGS,
       (SELECT MIN(ev.VALUE_EXTERNAL_ID)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P345' AND s.DELETED = 0) AS ID_IMDB,
       (SELECT MIN(tv.YEAR_VALUE)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_TIME_VALUE tv ON tv.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P569' AND s.DELETED = 0) AS BIRTH_YEAR,
       EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P4985' AND s.DELETED = 0) AS HAS_TMDB_ON_WIKIDATA
FROM T_WC_WIKIDATA_PERSON m
WHERE m.DELETED = 0;

SELECT 'F. Persons: candidates and Wikidata write-backs' AS SECTION;
SELECT COUNT(*) AS ENTITIES,
       SUM(ID_IMDB IS NOT NULL) AS WITH_IMDB,
       SUM(ID_IMDB IS NOT NULL AND HAS_TMDB_ON_WIKIDATA = 0) AS WITH_IMDB_NO_TMDB_ON_WIKIDATA,
       SUM(ID_IMDB IS NOT NULL AND HAS_TMDB_ON_WIKIDATA = 0
           AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_PERSON t WHERE t.ID_WIKIDATA = c.ID_WIKIDATA)
           AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_PERSON t WHERE t.ID_IMDB = c.ID_IMDB)) AS TMDB_CANDIDATES,
       SUM(ID_IMDB IS NOT NULL AND HAS_TMDB_ON_WIKIDATA = 0
           AND EXISTS (SELECT 1 FROM T_WC_TMDB_PERSON t WHERE t.ID_IMDB = c.ID_IMDB)) AS TMDB_HAS_IT
FROM TMP_DC003_PERSON c;

SELECT 'G. Persons: top 100 TMDb candidates by label languages' AS SECTION;
SELECT c.ID_WIKIDATA, c.ID_IMDB, c.BIRTH_YEAR, c.LABEL_LANGS, c.LABEL_EN
FROM TMP_DC003_PERSON c
WHERE c.ID_IMDB IS NOT NULL AND c.HAS_TMDB_ON_WIKIDATA = 0
  AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_PERSON t WHERE t.ID_WIKIDATA = c.ID_WIKIDATA)
  AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_PERSON t WHERE t.ID_IMDB = c.ID_IMDB)
ORDER BY c.LABEL_LANGS DESC, c.BIRTH_YEAR
LIMIT 100;

DROP TEMPORARY TABLE IF EXISTS TMP_DC003_PERSON;

SELECT NOW() AS NOW_;
