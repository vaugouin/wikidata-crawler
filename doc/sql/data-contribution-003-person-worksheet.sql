-- ============================================================================
-- DATA-CONTRIBUTION-003 : worksheet for creating the top 50 persons on TMDb
-- ============================================================================
--
-- READ ONLY. The 50 persons at the top of section H of
-- data-contribution-003-wikidata-without-tmdb-v2-20261105.txt (Wikidata persons
-- with an nm IMDb id, unknown to the local TMDb copy, ranked by credits on works
-- TMDb already has). The list is frozen here on purpose: recomputing the ranking
-- costs about 20 minutes, and a worksheet must not move under the hand that uses
-- it. RANK_ is the rank in that section.
--
-- Section P gives what the TMDb person form asks for: name, gender, birthday,
-- deathday, place of birth, IMDb id, plus the Wikidata description and the
-- occupations as a start for the biography and the known-for department.
-- Section C gives, person by person, the credits to add once the person exists:
-- the TMDb movie or series, the TMDb department and job matching the Wikidata
-- property, and the character (qualifier P453) for cast members.
--
-- Before creating anyone, search TMDb by name (TMDB_SEARCH column): the local
-- TMDb copy may lack a person that TMDb has, or hold it without its IMDb id.
-- Dates keep the Wikidata precision (year, year-month or full date).
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT NOW() AS NOW_;

SELECT 'P. Persons: data for the TMDb creation form' AS SECTION;
SELECT FIELD(p.ID_WIKIDATA,
             'Q5512207','Q2269625','Q26878090','Q4751271','Q6108101','Q28070985','Q13110599','Q731703','Q2053416','Q2928642',
             'Q7238852','Q16599878','Q5055301','Q26251134','Q3726570','Q5667396','Q16624098','Q21067208','Q6434301','Q19903994',
             'Q7929224','Q2417167','Q6172768','Q6893516','Q8962663','Q2792151','Q4288040','Q6014454','Q4779123','Q55374',
             'Q5865167','Q3707818','Q11689767','Q7108170','Q37994724','Q155202','Q23771900','Q16200798','Q4984538','Q6004557',
             'Q7787584','Q23581929','Q5969631','Q3530914','Q15117297','Q3429974','Q7357181','Q6414811','Q4790711','Q10857806') AS RANK_,
       p.ID_WIKIDATA,
       (SELECT MIN(ev.VALUE_EXTERNAL_ID)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = p.ID_WIKIDATA AND s.ID_PROPERTY = 'P345' AND s.DELETED = 0) AS ID_IMDB,
       p.LABEL_EN AS NAME,
       (SELECT CASE MIN(iv.ID_ITEM) WHEN 'Q6581097' THEN 'Male' WHEN 'Q6581072' THEN 'Female'
                                    ELSE MIN(iv.ID_ITEM) END
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = p.ID_WIKIDATA AND s.ID_PROPERTY = 'P21' AND s.DELETED = 0) AS GENDER,
       (SELECT MIN(CASE WHEN tv.TIME_PRECISION >= 11
                        THEN CONCAT(tv.YEAR_VALUE, '-', LPAD(tv.MONTH_VALUE, 2, '0'), '-', LPAD(tv.DAY_VALUE, 2, '0'))
                        WHEN tv.TIME_PRECISION = 10
                        THEN CONCAT(tv.YEAR_VALUE, '-', LPAD(tv.MONTH_VALUE, 2, '0'))
                        ELSE CAST(tv.YEAR_VALUE AS CHAR) END)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_TIME_VALUE tv ON tv.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = p.ID_WIKIDATA AND s.ID_PROPERTY = 'P569' AND s.DELETED = 0) AS BIRTHDAY,
       (SELECT MIN(CASE WHEN tv.TIME_PRECISION >= 11
                        THEN CONCAT(tv.YEAR_VALUE, '-', LPAD(tv.MONTH_VALUE, 2, '0'), '-', LPAD(tv.DAY_VALUE, 2, '0'))
                        WHEN tv.TIME_PRECISION = 10
                        THEN CONCAT(tv.YEAR_VALUE, '-', LPAD(tv.MONTH_VALUE, 2, '0'))
                        ELSE CAST(tv.YEAR_VALUE AS CHAR) END)
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_TIME_VALUE tv ON tv.ID_STATEMENT = s.ID_STATEMENT
         WHERE s.ID_WIKIDATA = p.ID_WIKIDATA AND s.ID_PROPERTY = 'P570' AND s.DELETED = 0) AS DEATHDAY,
       (SELECT MIN(COALESCE(i.LABEL_EN, iv.ID_ITEM))
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = s.ID_STATEMENT
          LEFT JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = iv.ID_ITEM
         WHERE s.ID_WIKIDATA = p.ID_WIKIDATA AND s.ID_PROPERTY = 'P19' AND s.DELETED = 0) AS PLACE_OF_BIRTH,
       (SELECT GROUP_CONCAT(DISTINCT COALESCE(i.LABEL_EN, iv.ID_ITEM) ORDER BY COALESCE(i.LABEL_EN, iv.ID_ITEM) SEPARATOR ', ')
          FROM T_WC_WIKIDATA_STATEMENT s
          INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = s.ID_STATEMENT
          LEFT JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = iv.ID_ITEM
         WHERE s.ID_WIKIDATA = p.ID_WIKIDATA AND s.ID_PROPERTY = 'P106' AND s.DELETED = 0) AS OCCUPATIONS,
       LEFT(p.DESCRIPTION_EN, 80) AS DESCRIPTION_EN,
       CONCAT('https://www.themoviedb.org/search/person?query=', REPLACE(p.LABEL_EN, ' ', '+')) AS TMDB_SEARCH
FROM T_WC_WIKIDATA_PERSON p
WHERE p.ID_WIKIDATA IN
      ('Q5512207','Q2269625','Q26878090','Q4751271','Q6108101','Q28070985','Q13110599','Q731703','Q2053416','Q2928642',
       'Q7238852','Q16599878','Q5055301','Q26251134','Q3726570','Q5667396','Q16624098','Q21067208','Q6434301','Q19903994',
       'Q7929224','Q2417167','Q6172768','Q6893516','Q8962663','Q2792151','Q4288040','Q6014454','Q4779123','Q55374',
       'Q5865167','Q3707818','Q11689767','Q7108170','Q37994724','Q155202','Q23771900','Q16200798','Q4984538','Q6004557',
       'Q7787584','Q23581929','Q5969631','Q3530914','Q15117297','Q3429974','Q7357181','Q6414811','Q4790711','Q10857806')
  AND p.DELETED = 0
ORDER BY RANK_;

SELECT 'C. Credits to add once the person exists, on works TMDb already has' AS SECTION;
SELECT FIELD(iv.ID_ITEM,
             'Q5512207','Q2269625','Q26878090','Q4751271','Q6108101','Q28070985','Q13110599','Q731703','Q2053416','Q2928642',
             'Q7238852','Q16599878','Q5055301','Q26251134','Q3726570','Q5667396','Q16624098','Q21067208','Q6434301','Q19903994',
             'Q7929224','Q2417167','Q6172768','Q6893516','Q8962663','Q2792151','Q4288040','Q6014454','Q4779123','Q55374',
             'Q5865167','Q3707818','Q11689767','Q7108170','Q37994724','Q155202','Q23771900','Q16200798','Q4984538','Q6004557',
             'Q7787584','Q23581929','Q5969631','Q3530914','Q15117297','Q3429974','Q7357181','Q6414811','Q4790711','Q10857806') AS RANK_,
       iv.ID_ITEM AS PERSON_QID,
       CASE WHEN tm.ID_MOVIE IS NOT NULL THEN 'movie' ELSE 'tv' END AS TMDB_TYPE,
       COALESCE(tm.ID_MOVIE, ts.ID_SERIE) AS ID_TMDB,
       LEFT(COALESCE(tm.TITLE, ts.TITLE), 50) AS TITLE,
       (SELECT MIN(tv.YEAR_VALUE)
          FROM T_WC_WIKIDATA_STATEMENT s2
          INNER JOIN T_WC_WIKIDATA_TIME_VALUE tv ON tv.ID_STATEMENT = s2.ID_STATEMENT
         WHERE s2.ID_WIKIDATA = s.ID_WIKIDATA AND s2.ID_PROPERTY IN ('P577', 'P580') AND s2.DELETED = 0) AS YEAR_,
       CASE s.ID_PROPERTY
            WHEN 'P161'  THEN 'Acting / Actor'
            WHEN 'P725'  THEN 'Acting / Voice'
            WHEN 'P57'   THEN 'Directing / Director'
            WHEN 'P58'   THEN 'Writing / Screenplay'
            WHEN 'P86'   THEN 'Sound / Original Music Composer'
            WHEN 'P162'  THEN 'Production / Producer'
            WHEN 'P1431' THEN 'Production / Executive Producer'
            WHEN 'P344'  THEN 'Camera / Director of Photography'
            WHEN 'P1040' THEN 'Editing / Editor'
            WHEN 'P2554' THEN 'Art / Production Design'
            WHEN 'P2515' THEN 'Costume & Make-Up / Costume Design'
       END AS TMDB_DEPARTMENT_JOB,
       (SELECT GROUP_CONCAT(DISTINCT COALESCE(c.LABEL_EN, i.LABEL_EN, qv.ID_ITEM) SEPARATOR ', ')
          FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER q
          INNER JOIN T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qv ON qv.ID_STATEMENT_QUALIFIER = q.ID_STATEMENT_QUALIFIER
          LEFT JOIN T_WC_WIKIDATA_CHARACTER c ON c.ID_WIKIDATA = qv.ID_ITEM
          LEFT JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = qv.ID_ITEM
         WHERE q.ID_STATEMENT = s.ID_STATEMENT AND q.ID_QUALIFIER_PROPERTY = 'P453' AND q.DELETED = 0) AS CHARACTER_,
       s.ID_WIKIDATA AS WORK_QID
FROM T_WC_WIKIDATA_ITEM_VALUE iv
INNER JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_STATEMENT = iv.ID_STATEMENT AND s.DELETED = 0
LEFT JOIN T_WC_TMDB_MOVIE tm ON tm.ID_WIKIDATA = s.ID_WIKIDATA
LEFT JOIN T_WC_TMDB_SERIE ts ON ts.ID_WIKIDATA = s.ID_WIKIDATA
WHERE iv.ID_ITEM IN
      ('Q5512207','Q2269625','Q26878090','Q4751271','Q6108101','Q28070985','Q13110599','Q731703','Q2053416','Q2928642',
       'Q7238852','Q16599878','Q5055301','Q26251134','Q3726570','Q5667396','Q16624098','Q21067208','Q6434301','Q19903994',
       'Q7929224','Q2417167','Q6172768','Q6893516','Q8962663','Q2792151','Q4288040','Q6014454','Q4779123','Q55374',
       'Q5865167','Q3707818','Q11689767','Q7108170','Q37994724','Q155202','Q23771900','Q16200798','Q4984538','Q6004557',
       'Q7787584','Q23581929','Q5969631','Q3530914','Q15117297','Q3429974','Q7357181','Q6414811','Q4790711','Q10857806')
  AND s.ID_PROPERTY IN ('P161', 'P725', 'P57', 'P58', 'P86', 'P162', 'P1431', 'P344', 'P1040', 'P2554', 'P2515')
  AND (tm.ID_MOVIE IS NOT NULL OR ts.ID_SERIE IS NOT NULL)
ORDER BY RANK_, YEAR_, TITLE;

SELECT NOW() AS NOW_;
