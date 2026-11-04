-- ============================================================================
-- DATA-CONTRIBUTION-003 v2 : Wikidata films, series and persons with no TMDb entry
-- ============================================================================
--
-- READ ONLY. Second version of data-contribution-003-wikidata-without-tmdb.sql,
-- written after its first run (result -20261104.txt). Same candidate definition
-- (V2 entity, IMDb id P345, no TMDb id on Wikidata, unknown to the local TMDb copy
-- by QID and by IMDb id), with the noise of the first run filtered out and a
-- better notability ranking.
--
-- What the first run showed, and what v2 changes:
--   1. LABEL_LANGS ranked nothing: bots propagate one label to 400 languages, so a
--      Tatort episode came first with 413. v2 ranks by NB_EXTERNAL_IDS (how many
--      outside databases catalogue the item), then NB_STATEMENTS. The V2 load keeps
--      no sitelink count, so Wikipedia articles cannot be used.
--   2. Wrong IMDb id kinds: ev... (Oscar ceremonies) among series, ch... (a
--      character id on Jesus Christ) among persons. v2 keeps tt... for movies and
--      series, nm... for persons.
--   3. TV episodes and series among movies (Tatort, Polizeiruf 110, anime). v2
--      drops a movie whose P31 is an episode or series class, whose P179 (part of
--      the series) points to a T_WC_WIKIDATA_SERIE entity, or that also sits in
--      T_WC_WIKIDATA_SERIE or T_WC_WIKIDATA_EPISODE. On TMDb these belong inside
--      the series, not as movies.
--   4. Persons outside film (badminton players, heads of state). v2 keeps a person
--      credited on Wikidata (cast, director, writer, crew: NB_CREDITS) or holding a
--      film occupation (P106). Persons are ranked by NB_CREDITS_ON_TMDB: credits on
--      works TMDb already has, so the new person can be linked at once.
--   5. Series: drops award ceremonies (P31 Q4504495) and episodes (P31 Q21191270,
--      Q1983062) that the classifier left in T_WC_WIKIDATA_SERIE.
--
-- Each family is built in two temporary tables: BASE (every entity, cheap columns)
-- then CAND (the v1 candidates only, with the costly columns and the v2 flags), so
-- the extra subqueries run on tens of thousands of rows, not hundreds of
-- thousands, and every statement stays well under the 600 s limit of the agent
-- account. No statement references a temporary table twice (MariaDB error 1137).
--
-- QID lists to check against the result (written from memory, 2026-11-04):
--   TV classes : Q21191270 television series episode, Q1983062 episode,
--                Q5398426 television series, Q63952888 anime television series,
--                Q1259759 miniseries, Q526877 web series, Q581714 animated series.
--   Ceremony   : Q4504495 award ceremony.
--   Occupations: see TMP_DC003_PERSON_CAND below.
--
-- Scheduling: run it when no text2sql-eval campaign is running (2026-11-04: the
-- API 1.1.20 English campaign was in progress, so the run was put off). The first
-- version took 13 minutes on the shared MariaDB.
--
-- Expectations, written before the first run of v2:
--   A. Movies: the episode filter removes thousands of rows (Tatort alone has
--      more than 1,000 episodes); the kept candidates stay well above the 1,000
--      creations of the top Creator badge.
--   B. The top movies are feature films catalogued by many databases (silent,
--      Soviet, Indian, Japanese cinema), no longer TV episodes.
--   F. Persons: the film filter keeps a minority of the 223,000 v1 candidates;
--      the top is made of actors and crew credited on films TMDb already has.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT NOW() AS NOW_;

-- ---------------------------------------------------------------------------
-- Movies
-- ---------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS TMP_DC003_MOVIE_BASE;
CREATE TEMPORARY TABLE TMP_DC003_MOVIE_BASE AS
SELECT m.ID_WIKIDATA,
       m.LABEL_EN,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(m.LABELS_JSON, '$.fr.value')),
                JSON_UNQUOTE(JSON_EXTRACT(m.LABELS_JSON, '$.fr'))) AS LABEL_FR,
       (SELECT MIN(ev.VALUE_EXTERNAL_ID)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P345' AND s.DELETED = 0) AS ID_IMDB,
       EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P4947' AND s.DELETED = 0) AS HAS_TMDB_ON_WIKIDATA
FROM T_WC_WIKIDATA_MOVIE m
WHERE m.DELETED = 0;

SELECT 'A0. Movies: Wikidata write-backs, 50 items missing the TMDb id TMDb already has (P4947 to add), by TMDb popularity' AS SECTION;
SELECT b.ID_WIKIDATA, b.ID_IMDB, t.ID_MOVIE AS ID_TMDB, ROUND(t.POPULARITY, 1) AS POPULARITY, b.LABEL_EN
FROM TMP_DC003_MOVIE_BASE b
INNER JOIN T_WC_TMDB_MOVIE t ON t.ID_IMDB = b.ID_IMDB
WHERE b.ID_IMDB LIKE 'tt%' AND b.HAS_TMDB_ON_WIKIDATA = 0
ORDER BY t.POPULARITY DESC
LIMIT 50;

DROP TEMPORARY TABLE IF EXISTS TMP_DC003_MOVIE_CAND;
CREATE TEMPORARY TABLE TMP_DC003_MOVIE_CAND AS
SELECT b.ID_WIKIDATA, b.ID_IMDB, b.LABEL_EN, b.LABEL_FR,
       (b.ID_IMDB LIKE 'tt%') AS IMDB_OK,
       (SELECT MIN(tv.YEAR_VALUE)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_TIME_VALUE tv ON tv.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = b.ID_WIKIDATA AND s.ID_PROPERTY = 'P577' AND s.DELETED = 0) AS YEAR_,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT s
         WHERE s.ID_WIKIDATA = b.ID_WIKIDATA AND s.DELETED = 0) AS NB_STATEMENTS,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT s
         WHERE s.ID_WIKIDATA = b.ID_WIKIDATA AND s.VALUE_TYPE = 'external_id' AND s.DELETED = 0) AS NB_EXTERNAL_IDS,
       EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
               INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = s.ID_STATEMENT
                WHERE s.ID_WIKIDATA = b.ID_WIKIDATA AND s.ID_PROPERTY = 'P31' AND s.DELETED = 0
                  AND iv.ID_ITEM IN ('Q21191270', 'Q1983062', 'Q5398426', 'Q63952888',
                                     'Q1259759', 'Q526877', 'Q581714')) AS P31_TV,
       EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
               INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = s.ID_STATEMENT
               INNER JOIN T_WC_WIKIDATA_SERIE ws ON ws.ID_WIKIDATA = iv.ID_ITEM AND ws.DELETED = 0
                WHERE s.ID_WIKIDATA = b.ID_WIKIDATA AND s.ID_PROPERTY = 'P179' AND s.DELETED = 0) AS PART_OF_SERIE,
       (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE x WHERE x.ID_WIKIDATA = b.ID_WIKIDATA AND x.DELETED = 0)
        OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE x WHERE x.ID_WIKIDATA = b.ID_WIKIDATA AND x.DELETED = 0)) AS IN_TV_TABLE
FROM TMP_DC003_MOVIE_BASE b
WHERE b.ID_IMDB IS NOT NULL AND b.HAS_TMDB_ON_WIKIDATA = 0
  AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_MOVIE t WHERE t.ID_WIKIDATA = b.ID_WIKIDATA)
  AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_MOVIE t WHERE t.ID_IMDB = b.ID_IMDB);

DROP TEMPORARY TABLE IF EXISTS TMP_DC003_MOVIE_BASE;

SELECT 'A. Movies: filter funnel (V1_CANDIDATES is the TMDB_CANDIDATES of v1)' AS SECTION;
SELECT COUNT(*) AS V1_CANDIDATES,
       SUM(IMDB_OK = 0) AS DROP_IMDB_NOT_TT,
       SUM(IMDB_OK = 1 AND P31_TV = 1) AS DROP_P31_TV,
       SUM(IMDB_OK = 1 AND P31_TV = 0 AND PART_OF_SERIE = 1) AS DROP_PART_OF_SERIE,
       SUM(IMDB_OK = 1 AND P31_TV = 0 AND PART_OF_SERIE = 0 AND IN_TV_TABLE = 1) AS DROP_IN_TV_TABLE,
       SUM(IMDB_OK = 1 AND P31_TV = 0 AND PART_OF_SERIE = 0 AND IN_TV_TABLE = 0) AS V2_CANDIDATES,
       SUM(IMDB_OK = 1 AND P31_TV = 0 AND PART_OF_SERIE = 0 AND IN_TV_TABLE = 0 AND NB_EXTERNAL_IDS >= 10) AS V2_WITH_10_EXTERNAL_IDS
FROM TMP_DC003_MOVIE_CAND;

SELECT 'B. Movies: top 200 v2 candidates by external ids, then statements' AS SECTION;
SELECT ID_WIKIDATA, ID_IMDB, YEAR_, NB_EXTERNAL_IDS, NB_STATEMENTS, LABEL_EN, LABEL_FR
FROM TMP_DC003_MOVIE_CAND
WHERE IMDB_OK = 1 AND P31_TV = 0 AND PART_OF_SERIE = 0 AND IN_TV_TABLE = 0
ORDER BY NB_EXTERNAL_IDS DESC, NB_STATEMENTS DESC, YEAR_
LIMIT 200;

DROP TEMPORARY TABLE IF EXISTS TMP_DC003_MOVIE_CAND;

-- ---------------------------------------------------------------------------
-- Series
-- ---------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS TMP_DC003_SERIE_BASE;
CREATE TEMPORARY TABLE TMP_DC003_SERIE_BASE AS
SELECT m.ID_WIKIDATA,
       m.LABEL_EN,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(m.LABELS_JSON, '$.fr.value')),
                JSON_UNQUOTE(JSON_EXTRACT(m.LABELS_JSON, '$.fr'))) AS LABEL_FR,
       (SELECT MIN(ev.VALUE_EXTERNAL_ID)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P345' AND s.DELETED = 0) AS ID_IMDB,
       EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P4983' AND s.DELETED = 0) AS HAS_TMDB_ON_WIKIDATA
FROM T_WC_WIKIDATA_SERIE m
WHERE m.DELETED = 0;

SELECT 'C0. Series: Wikidata write-backs, 50 items missing the TMDb id TMDb already has (P4983 to add), by TMDb popularity' AS SECTION;
SELECT b.ID_WIKIDATA, b.ID_IMDB, t.ID_SERIE AS ID_TMDB, ROUND(t.POPULARITY, 1) AS POPULARITY, b.LABEL_EN
FROM TMP_DC003_SERIE_BASE b
INNER JOIN T_WC_TMDB_SERIE t ON t.ID_IMDB = b.ID_IMDB
WHERE b.ID_IMDB LIKE 'tt%' AND b.HAS_TMDB_ON_WIKIDATA = 0
ORDER BY t.POPULARITY DESC
LIMIT 50;

DROP TEMPORARY TABLE IF EXISTS TMP_DC003_SERIE_CAND;
CREATE TEMPORARY TABLE TMP_DC003_SERIE_CAND AS
SELECT b.ID_WIKIDATA, b.ID_IMDB, b.LABEL_EN, b.LABEL_FR,
       (b.ID_IMDB LIKE 'tt%') AS IMDB_OK,
       (SELECT MIN(tv.YEAR_VALUE)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_TIME_VALUE tv ON tv.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = b.ID_WIKIDATA AND s.ID_PROPERTY IN ('P580', 'P577') AND s.DELETED = 0) AS YEAR_,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT s
         WHERE s.ID_WIKIDATA = b.ID_WIKIDATA AND s.DELETED = 0) AS NB_STATEMENTS,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT s
         WHERE s.ID_WIKIDATA = b.ID_WIKIDATA AND s.VALUE_TYPE = 'external_id' AND s.DELETED = 0) AS NB_EXTERNAL_IDS,
       EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
               INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = s.ID_STATEMENT
                WHERE s.ID_WIKIDATA = b.ID_WIKIDATA AND s.ID_PROPERTY = 'P31' AND s.DELETED = 0
                  AND iv.ID_ITEM IN ('Q4504495', 'Q21191270', 'Q1983062')) AS P31_NOT_SERIES
FROM TMP_DC003_SERIE_BASE b
WHERE b.ID_IMDB IS NOT NULL AND b.HAS_TMDB_ON_WIKIDATA = 0
  AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_SERIE t WHERE t.ID_WIKIDATA = b.ID_WIKIDATA)
  AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_SERIE t WHERE t.ID_IMDB = b.ID_IMDB);

DROP TEMPORARY TABLE IF EXISTS TMP_DC003_SERIE_BASE;

SELECT 'D. Series: filter funnel (V1_CANDIDATES is the TMDB_CANDIDATES of v1)' AS SECTION;
SELECT COUNT(*) AS V1_CANDIDATES,
       SUM(IMDB_OK = 0) AS DROP_IMDB_NOT_TT,
       SUM(IMDB_OK = 1 AND P31_NOT_SERIES = 1) AS DROP_CEREMONY_OR_EPISODE,
       SUM(IMDB_OK = 1 AND P31_NOT_SERIES = 0) AS V2_CANDIDATES,
       SUM(IMDB_OK = 1 AND P31_NOT_SERIES = 0 AND NB_EXTERNAL_IDS >= 10) AS V2_WITH_10_EXTERNAL_IDS
FROM TMP_DC003_SERIE_CAND;

SELECT 'E. Series: top 100 v2 candidates by external ids, then statements' AS SECTION;
SELECT ID_WIKIDATA, ID_IMDB, YEAR_, NB_EXTERNAL_IDS, NB_STATEMENTS, LABEL_EN, LABEL_FR
FROM TMP_DC003_SERIE_CAND
WHERE IMDB_OK = 1 AND P31_NOT_SERIES = 0
ORDER BY NB_EXTERNAL_IDS DESC, NB_STATEMENTS DESC, YEAR_
LIMIT 100;

DROP TEMPORARY TABLE IF EXISTS TMP_DC003_SERIE_CAND;

-- ---------------------------------------------------------------------------
-- Persons
-- ---------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS TMP_DC003_PERSON_BASE;
CREATE TEMPORARY TABLE TMP_DC003_PERSON_BASE AS
SELECT m.ID_WIKIDATA,
       m.LABEL_EN,
       (SELECT MIN(ev.VALUE_EXTERNAL_ID)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P345' AND s.DELETED = 0) AS ID_IMDB,
       EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                WHERE s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY = 'P4985' AND s.DELETED = 0) AS HAS_TMDB_ON_WIKIDATA
FROM T_WC_WIKIDATA_PERSON m
WHERE m.DELETED = 0;

SELECT 'F0. Persons: Wikidata write-backs, 50 items missing the TMDb id TMDb already has (P4985 to add), by TMDb popularity' AS SECTION;
SELECT b.ID_WIKIDATA, b.ID_IMDB, t.ID_PERSON AS ID_TMDB, ROUND(t.POPULARITY, 1) AS POPULARITY, b.LABEL_EN
FROM TMP_DC003_PERSON_BASE b
INNER JOIN T_WC_TMDB_PERSON t ON t.ID_IMDB = b.ID_IMDB
WHERE b.ID_IMDB LIKE 'nm%' AND b.HAS_TMDB_ON_WIKIDATA = 0
ORDER BY t.POPULARITY DESC
LIMIT 50;

-- Credits: the person is the value of a cast or crew statement on another item.
--   P161 cast member, P725 voice actor, P57 director, P58 screenwriter,
--   P86 composer, P162 producer, P1431 executive producer,
--   P344 director of photography, P1040 film editor,
--   P2554 production designer, P2515 costume designer.
-- Film occupations (P106): Q33999 actor, Q10800557 film actor,
--   Q10798782 television actor, Q2405480 voice actor, Q948329 character actor,
--   Q2526255 film director, Q3455803 director, Q2059704 television director,
--   Q28389 screenwriter, Q3282637 film producer, Q578109 television producer,
--   Q222344 cinematographer, Q7042855 film editor, Q1415090 film score composer,
--   Q266569 animator, Q2962070 production designer, Q1323191 costume designer,
--   Q1049296 casting director, Q465501 stunt performer, Q947873 television presenter.
DROP TEMPORARY TABLE IF EXISTS TMP_DC003_PERSON_CAND;
CREATE TEMPORARY TABLE TMP_DC003_PERSON_CAND AS
SELECT b.ID_WIKIDATA, b.ID_IMDB, b.LABEL_EN,
       (b.ID_IMDB LIKE 'nm%') AS IMDB_OK,
       (SELECT MIN(tv.YEAR_VALUE)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_TIME_VALUE tv ON tv.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = b.ID_WIKIDATA AND s.ID_PROPERTY = 'P569' AND s.DELETED = 0) AS BIRTH_YEAR,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT s
         WHERE s.ID_WIKIDATA = b.ID_WIKIDATA AND s.VALUE_TYPE = 'external_id' AND s.DELETED = 0) AS NB_EXTERNAL_IDS,
       EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
               INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = s.ID_STATEMENT
                WHERE s.ID_WIKIDATA = b.ID_WIKIDATA AND s.ID_PROPERTY = 'P106' AND s.DELETED = 0
                  AND iv.ID_ITEM IN ('Q33999', 'Q10800557', 'Q10798782', 'Q2405480', 'Q948329',
                                     'Q2526255', 'Q3455803', 'Q2059704', 'Q28389', 'Q3282637',
                                     'Q578109', 'Q222344', 'Q7042855', 'Q1415090', 'Q266569',
                                     'Q2962070', 'Q1323191', 'Q1049296', 'Q465501', 'Q947873')) AS FILM_OCCUPATION,
       (SELECT COUNT(DISTINCT s.ID_WIKIDATA)
          FROM T_WC_WIKIDATA_ITEM_VALUE iv
          INNER JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_STATEMENT = iv.ID_STATEMENT
         WHERE iv.ID_ITEM = b.ID_WIKIDATA AND s.DELETED = 0
           AND s.ID_PROPERTY IN ('P161', 'P725', 'P57', 'P58', 'P86', 'P162', 'P1431',
                                 'P344', 'P1040', 'P2554', 'P2515')) AS NB_CREDITS,
       (SELECT COUNT(DISTINCT s.ID_WIKIDATA)
          FROM T_WC_WIKIDATA_ITEM_VALUE iv
          INNER JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_STATEMENT = iv.ID_STATEMENT
         WHERE iv.ID_ITEM = b.ID_WIKIDATA AND s.DELETED = 0
           AND s.ID_PROPERTY IN ('P161', 'P725', 'P57', 'P58', 'P86', 'P162', 'P1431',
                                 'P344', 'P1040', 'P2554', 'P2515')
           AND (EXISTS (SELECT 1 FROM T_WC_TMDB_MOVIE t WHERE t.ID_WIKIDATA = s.ID_WIKIDATA)
                OR EXISTS (SELECT 1 FROM T_WC_TMDB_SERIE t WHERE t.ID_WIKIDATA = s.ID_WIKIDATA))) AS NB_CREDITS_ON_TMDB
FROM TMP_DC003_PERSON_BASE b
WHERE b.ID_IMDB IS NOT NULL AND b.HAS_TMDB_ON_WIKIDATA = 0
  AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_PERSON t WHERE t.ID_WIKIDATA = b.ID_WIKIDATA)
  AND NOT EXISTS (SELECT 1 FROM T_WC_TMDB_PERSON t WHERE t.ID_IMDB = b.ID_IMDB);

DROP TEMPORARY TABLE IF EXISTS TMP_DC003_PERSON_BASE;

SELECT 'G. Persons: filter funnel (V1_CANDIDATES is the TMDB_CANDIDATES of v1)' AS SECTION;
SELECT COUNT(*) AS V1_CANDIDATES,
       SUM(IMDB_OK = 0) AS DROP_IMDB_NOT_NM,
       SUM(IMDB_OK = 1 AND NB_CREDITS = 0 AND FILM_OCCUPATION = 0) AS DROP_NOT_FILM,
       SUM(IMDB_OK = 1 AND (NB_CREDITS > 0 OR FILM_OCCUPATION = 1)) AS V2_CANDIDATES,
       SUM(IMDB_OK = 1 AND NB_CREDITS > 0) AS V2_CREDITED,
       SUM(IMDB_OK = 1 AND NB_CREDITS_ON_TMDB > 0) AS V2_CREDITED_ON_TMDB_WORK
FROM TMP_DC003_PERSON_CAND;

SELECT 'H. Persons: top 200 v2 candidates by credits on TMDb works, then credits, then external ids' AS SECTION;
SELECT ID_WIKIDATA, ID_IMDB, BIRTH_YEAR, NB_CREDITS_ON_TMDB, NB_CREDITS, FILM_OCCUPATION, NB_EXTERNAL_IDS, LABEL_EN
FROM TMP_DC003_PERSON_CAND
WHERE IMDB_OK = 1 AND (NB_CREDITS > 0 OR FILM_OCCUPATION = 1)
ORDER BY NB_CREDITS_ON_TMDB DESC, NB_CREDITS DESC, NB_EXTERNAL_IDS DESC
LIMIT 200;

DROP TEMPORARY TABLE IF EXISTS TMP_DC003_PERSON_CAND;

SELECT NOW() AS NOW_;
