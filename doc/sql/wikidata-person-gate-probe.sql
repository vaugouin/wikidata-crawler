-- ============================================================================
-- Sonde : concevoir le "gate cinema-aware" pour les persons
-- ============================================================================
-- Question : parmi les persons V1 SANS IMDb absentes de V2 PERSON (~48 819),
--   qui sont-elles ? Des gens de cinema (occupation P106 = acteur/realisateur...)
--   captables par un gate occupation, ou du bruit (politiques, sportifs...) ?
--   On mesure avant de coder un gate et de relancer 5-6 jours de re-run.
--
-- LECTURE SEULE. COLLATE / COALESCE(DELETED) : memes conventions que
--   wikidata-v1-v2-comparison.sql.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- P1 · combien des persons sans IMDb manquantes ont AU MOINS une occupation P106
-- ----------------------------------------------------------------------------
SELECT '---------- P1 · persons sans IMDb manquantes : avec / sans occupation P106 ----------' AS section;

SELECT
  SUM(CASE WHEN has_p106 = 1 THEN 1 ELSE 0 END) AS with_any_occupation,
  SUM(CASE WHEN has_p106 = 0 THEN 1 ELSE 0 END) AS without_occupation
FROM (
  SELECT v1.ID_WIKIDATA,
    (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM_PROPERTY p106
             WHERE p106.ID_WIKIDATA = v1.ID_WIKIDATA
               AND p106.ID_PROPERTY = 'P106'
               AND COALESCE(p106.DELETED,0)=0)) AS has_p106
  FROM   T_WC_WIKIDATA_PERSON_V1 v1
  WHERE  COALESCE(v1.DELETED,0)=0
    AND  (v1.ID_IMDB IS NULL OR v1.ID_IMDB = '')
    AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON v2
                     WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t;

-- ----------------------------------------------------------------------------
-- P2 · TOP occupations P106 de ces persons : de qui parle-t-on ?
--   (si acteur/realisateur/scenariste dominent -> gate occupation efficace ;
--    si politique/sportif/scientifique dominent -> ce ne sont pas des gens de cinema)
-- ----------------------------------------------------------------------------
SELECT '---------- P2 · TOP occupations P106 des persons sans IMDb manquantes ----------' AS section;

SELECT p106.ID_ITEM AS occupation_qid, occ.LABEL AS occupation_label,
       COUNT(DISTINCT p106.ID_WIKIDATA) AS nb_persons
FROM   T_WC_WIKIDATA_PERSON_V1 v1
JOIN   T_WC_WIKIDATA_ITEM_PROPERTY p106
       ON p106.ID_WIKIDATA = v1.ID_WIKIDATA AND p106.ID_PROPERTY = 'P106'
       AND COALESCE(p106.DELETED,0)=0
LEFT JOIN T_WC_WIKIDATA_ITEM_V1 occ ON occ.ID_WIKIDATA = p106.ID_ITEM
WHERE  COALESCE(v1.DELETED,0)=0
  AND  (v1.ID_IMDB IS NULL OR v1.ID_IMDB = '')
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON v2
                   WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
GROUP BY p106.ID_ITEM, occ.LABEL
ORDER BY nb_persons DESC
LIMIT 40;

SELECT '========== FIN sonde person ==========' AS section;
