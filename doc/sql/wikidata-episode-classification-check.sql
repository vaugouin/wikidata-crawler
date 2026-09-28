-- ============================================================================
-- Les episodes sont-ils classes comme series ? (hypothese du 2026-09-27)
-- ============================================================================
--
-- CONSTAT. Le run du dump du 2026-09-24 a detecte ZERO episode, en pass1 comme en
-- pass2 (run_summary.json : episodes_detected = 0), alors qu'il detecte 26 776
-- saisons et 124 579 personnages. T_WC_WIKIDATA_EPISODE garde exactement ses
-- 187 463 lignes du 2026-08-16, et c'est la seule table d'entite ou aucun alias
-- n'est arrive (acceptation -025, F2 : 0,0 %).
--
-- HYPOTHESE. Q15416 (television program) est racine de serie depuis le 2026-07-12
-- (parite V1). Si Q21191270 (television series episode) en descend par P279,
-- classify_qids le range en "series", puisqu'il teste la serie AVANT l'episode.
-- Chaque episode partirait alors dans T_WC_WIKIDATA_SERIE.
--
-- CE QUI TRANCHE. E1 dit si la filiation existe dans le graphe P279 charge.
-- E2 dit ou vivent reellement les entites dont P31 = Q21191270. E3 est le temoin :
-- les saisons, detectees normalement, doivent vivre dans SEASON.
-- Si E1 trouve Q15416 parmi les ancetres ET que E2 met la masse des episodes dans
-- SERIE, l'hypothese est confirmee.
--
-- Controle hors base, gratuit, a faire aussi sur le VPS :
--   grep '"Q21191270"' ~/docker/shared_data/wikidata-crawler/pass1/class_roots.jsonl
-- Une ligne "ROOT_TYPE": "series" pour Q21191270 confirme la filiation telle que
-- le crawler l'a vue.
--
-- COUT. E1 parcourt les ancetres d'un seul noeud (quelques centaines de lignes).
-- E2 et E3 partent de l'index ID_ITEM de ITEM_VALUE, puis la cle primaire de
-- STATEMENT : de l'ordre de quelques centaines de milliers de lignes. Leger.
--
-- LECTURE SEULE. Executer avec --force -t (runsqlvaugouindb.sh le fait).
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ----------------------------------------------------------------------------
-- E1 . Q21191270 descend-il d'une racine de serie ?
-- ----------------------------------------------------------------------------
SELECT '=== E1 . ancetres P279 de Q21191270 qui sont des racines de serie ===' AS section;

WITH RECURSIVE anc (ID_NODE, DEPTH) AS (
    SELECT CAST('Q21191270' AS CHAR(50)), 0   -- sans CAST, le type est VARCHAR(9) : erreur 1406 (2026-09-28)
    UNION ALL
    SELECT sc.ID_PARENT, anc.DEPTH + 1
    FROM   anc
    JOIN   T_WC_WIKIDATA_SUBCLASS sc ON sc.ID_CHILD = anc.ID_NODE
    WHERE  anc.DEPTH < 12          -- le graphe P279 a des cycles : borne obligatoire
)
SELECT ID_NODE                  AS racine,
       MIN(DEPTH)               AS profondeur_min,
       CASE ID_NODE WHEN 'Q15416'   THEN 'television program (racine depuis 2026-07-12)'
                    WHEN 'Q5398426' THEN 'television series'
                    WHEN 'Q1259759' THEN 'miniseries'
                    WHEN 'Q526877'  THEN 'web series' END AS nom
FROM   anc
WHERE  ID_NODE IN ('Q15416', 'Q5398426', 'Q1259759', 'Q526877')
GROUP  BY ID_NODE;
-- Aucune ligne : l'hypothese tombe, chercher ailleurs (par exemple, Q21191270
-- absent du graphe charge, ou une racine d'episode qui ne correspond plus au dump).

SELECT '=== E1b . parents directs de Q21191270 (pour lire le chemin) ===' AS section;

SELECT sc.ID_PARENT,
       COALESCE(i.LABEL_EN, '(pas dans ITEM)') AS LABEL_EN
FROM   T_WC_WIKIDATA_SUBCLASS sc
LEFT   JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = sc.ID_PARENT
WHERE  sc.ID_CHILD = 'Q21191270';

-- ----------------------------------------------------------------------------
-- E2 . Ou vivent les entites dont P31 = Q21191270 ?
-- ----------------------------------------------------------------------------
SELECT '=== E2 . entites P31 = episode, par table d entite ===' AS section;

WITH ep AS (
    SELECT DISTINCT st.ID_WIKIDATA
    FROM   T_WC_WIKIDATA_ITEM_VALUE iv
    JOIN   T_WC_WIKIDATA_STATEMENT  st ON st.ID_STATEMENT = iv.ID_STATEMENT
    WHERE  iv.ID_ITEM = 'Q21191270'
      AND  st.ID_PROPERTY = 'P31'
      AND  st.DELETED = 0
)
SELECT COUNT(*)                                                    AS entites_p31_episode,
       SUM(e.ID_WIKIDATA IS NOT NULL)                              AS dans_episode,
       SUM(s.ID_WIKIDATA IS NOT NULL)                              AS dans_serie,
       SUM(e.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA IS NOT NULL) AS dans_les_deux,
       SUM(e.ID_WIKIDATA IS NULL AND s.ID_WIKIDATA IS NULL)         AS dans_aucune,
       SUM(s.ALIASES_JSON IS NOT NULL)                             AS serie_avec_colonne_alias_posee
FROM   ep
LEFT   JOIN T_WC_WIKIDATA_EPISODE e ON e.ID_WIKIDATA = ep.ID_WIKIDATA
LEFT   JOIN T_WC_WIKIDATA_SERIE   s ON s.ID_WIKIDATA = ep.ID_WIKIDATA;
-- Lecture : dans_serie proche de entites_p31_episode = hypothese confirmee.
-- serie_avec_colonne_alias_posee dit que ces lignes SERIE ont ete reecrites par ce
-- run (la colonne n'existait pas avant), donc que la classification est actuelle.

SELECT '=== E2b . dix exemples d episodes presents dans SERIE ===' AS section;

SELECT s.ID_WIKIDATA, s.LABEL_EN, LEFT(s.DESCRIPTION_EN, 80) AS DESCRIPTION_EN
FROM   T_WC_WIKIDATA_ITEM_VALUE iv
JOIN   T_WC_WIKIDATA_STATEMENT  st ON st.ID_STATEMENT = iv.ID_STATEMENT
JOIN   T_WC_WIKIDATA_SERIE      s  ON s.ID_WIKIDATA  = st.ID_WIKIDATA
WHERE  iv.ID_ITEM = 'Q21191270'
  AND  st.ID_PROPERTY = 'P31'
  AND  st.DELETED = 0
LIMIT  10;

-- ----------------------------------------------------------------------------
-- E3 . Temoin : les saisons (P31 = Q3464665), detectees normalement
-- ----------------------------------------------------------------------------
SELECT '=== E3 . temoin : entites P31 = saison, par table d entite ===' AS section;

WITH se AS (
    SELECT DISTINCT st.ID_WIKIDATA
    FROM   T_WC_WIKIDATA_ITEM_VALUE iv
    JOIN   T_WC_WIKIDATA_STATEMENT  st ON st.ID_STATEMENT = iv.ID_STATEMENT
    WHERE  iv.ID_ITEM = 'Q3464665'
      AND  st.ID_PROPERTY = 'P31'
      AND  st.DELETED = 0
)
SELECT COUNT(*)                        AS entites_p31_saison,
       SUM(a.ID_WIKIDATA IS NOT NULL)  AS dans_season,
       SUM(s.ID_WIKIDATA IS NOT NULL)  AS dans_serie
FROM   se
LEFT   JOIN T_WC_WIKIDATA_SEASON a ON a.ID_WIKIDATA = se.ID_WIKIDATA
LEFT   JOIN T_WC_WIKIDATA_SERIE  s ON s.ID_WIKIDATA = se.ID_WIKIDATA;

SELECT '========== FIN ==========' AS section;
