-- ============================================================================
-- Wikidata V1 -> V2 : comparaison entite-par-entite (movie / serie / person)
-- ============================================================================
--
-- BUT : verifier que wikidata-crawler (V2) peut remplacer sparql-crawler +
--       sparql-movies-persons (V1), en vue de decommissionner ces 2 depots.
--   Objectif A : toutes les lignes V1 sont-elles incluses dans V2 ?
--   Objectif B : des valeurs de 'instance of' (P31) manquent-elles en V2 vs V1 ?
--                (cas critique = SERIE, ou V2 < V1 en nombre de lignes)
--
-- Compagnon de : doc/wikidata-v1-v2-gap-analysis.md (qui ne sondait que P2079).
--
-- LECTURE SEULE : que des SELECT, aucun ecrit. Sans danger a executer.
--
-- COMMENT EXECUTER (exemple, depuis l'hote du conteneur MariaDB) :
--   docker exec -i damp-vaugouin-com-mariadb-1 mariadb -uroot -p \
--       --default-character-set=utf8mb4 -t vaugouindb \
--       < doc/sql/wikidata-v1-v2-comparison.sql > resultats.txt
--   (le flag -t donne une sortie en tableau, plus lisible a recoller)
--
-- MODELE DES DEUX GENERATIONS :
--   V1  : *_V1, PK ID_WIKIDATA, colonne INSTANCE_OF (UNE classe P31 denormalisee),
--         DELETED int(5) souvent NULL. P31 complet dans T_WC_WIKIDATA_ITEM_PROPERTY
--         (ID_PROPERTY='P31', ID_ITEM = classe).
--   V2  : tables entite (UK ID_WIKIDATA, DELETED tinyint default 0). P31 dans
--         T_WC_WIKIDATA_STATEMENT (ID_PROPERTY='P31') |X| T_WC_WIKIDATA_ITEM_VALUE
--         (ID_STATEMENT -> ID_ITEM = classe).
--
-- COLLATION : COLLATE utf8mb4_unicode_ci applique sur les comparaisons de
--   chaines inter-generations (le doc gap-analysis signale V1 general_ci vs
--   V2 unicode_ci, erreur #1267). Si la base est deja standardisee unicode_ci
--   partout, ces COLLATE sont des no-op ; on peut les retirer.
--
-- CONVENTION DELETED : V1.DELETED NULL est traite comme actif (COALESCE(...,0)=0).
-- ============================================================================


-- ============================================================================
-- SECTION 0 : DECOMPTES DE CONTROLE (les 3 ensembles d'un coup)
-- Reproduit les >/< observes et montre l'effet de DELETED.
-- ============================================================================
SELECT '========== 0 · decomptes de controle V1 vs V2 ==========' AS section;

SELECT 'movie' AS dataset,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE_V1)                              AS v1_total,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE_V1  WHERE COALESCE(DELETED,0)=0) AS v1_active,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE)                                 AS v2_total,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE     WHERE COALESCE(DELETED,0)=0) AS v2_active
UNION ALL
SELECT 'serie',
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_SERIE_V1),
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_SERIE_V1  WHERE COALESCE(DELETED,0)=0),
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_SERIE),
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_SERIE     WHERE COALESCE(DELETED,0)=0)
UNION ALL
SELECT 'person',
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON_V1),
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON_V1 WHERE COALESCE(DELETED,0)=0),
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON),
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON    WHERE COALESCE(DELETED,0)=0);


-- ############################################################################
-- ###  MOVIE                                                               ###
-- ############################################################################

-- ----------------------------------------------------------------------------
-- 1A · movie · inclusion : QID V1 actifs absents de la table MOVIE V2
-- ----------------------------------------------------------------------------
SELECT '---------- 1A · movie · inclusion (V1 absents de V2 meme type) ----------' AS section;

SELECT COUNT(*)                                                   AS v1_active_total,
       SUM(CASE WHEN v2.ID_WIKIDATA IS NULL THEN 1 ELSE 0 END)    AS missing_in_v2_same_type
FROM   T_WC_WIKIDATA_MOVIE_V1 v1
LEFT JOIN T_WC_WIKIDATA_MOVIE v2
       ON v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
WHERE  COALESCE(v1.DELETED,0)=0;

-- ----------------------------------------------------------------------------
-- 1B · movie · localisation des manquants : ailleurs en V2 vs absents partout
-- ----------------------------------------------------------------------------
SELECT '---------- 1B · movie · localisation des manquants ----------' AS section;

SELECT
  SUM(CASE WHEN present_anywhere_v2 = 1 THEN 1 ELSE 0 END) AS present_other_v2_table,
  SUM(CASE WHEN present_anywhere_v2 = 0 THEN 1 ELSE 0 END) AS absent_everywhere_v2
FROM (
  SELECT v1.ID_WIKIDATA,
    (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE   m  WHERE m.ID_WIKIDATA  = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE   s  WHERE s.ID_WIKIDATA  = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON  p  WHERE p.ID_WIKIDATA  = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON  se WHERE se.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE ep WHERE ep.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM    it WHERE it.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
    ) AS present_anywhere_v2
  FROM T_WC_WIKIDATA_MOVIE_V1 v1
  WHERE COALESCE(v1.DELETED,0)=0
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE v2
                    WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t;

-- ----------------------------------------------------------------------------
-- 1C · movie · classes INSTANCE_OF des manquants, par volume perdu
-- ----------------------------------------------------------------------------
SELECT '---------- 1C · movie · manquants ventiles par INSTANCE_OF ----------' AS section;

SELECT v1.INSTANCE_OF AS class_qid, cls.LABEL AS class_label,
       COUNT(*)       AS nb_v1_missing_in_v2
FROM   T_WC_WIKIDATA_MOVIE_V1 v1
LEFT JOIN T_WC_WIKIDATA_ITEM_V1 cls ON cls.ID_WIKIDATA = v1.INSTANCE_OF
WHERE  COALESCE(v1.DELETED,0)=0
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE v2
                   WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
GROUP BY v1.INSTANCE_OF, cls.LABEL
ORDER BY nb_v1_missing_in_v2 DESC
LIMIT 40;

-- ----------------------------------------------------------------------------
-- 1D · movie · trou d'ingestion P31 : entites communes dont une valeur P31
--       de V1 (ITEM_PROPERTY) est absente des statements V2
-- ----------------------------------------------------------------------------
SELECT '---------- 1D · movie · P31 present V1 absent V2 (entites communes) ----------' AS section;

SELECT p31.ID_ITEM AS class_qid, cls.LABEL AS class_label,
       COUNT(DISTINCT p31.ID_WIKIDATA) AS nb_entities_missing_this_p31
FROM   T_WC_WIKIDATA_MOVIE_V1 v1
JOIN   T_WC_WIKIDATA_MOVIE v2
       ON v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
JOIN   T_WC_WIKIDATA_ITEM_PROPERTY p31
       ON p31.ID_WIKIDATA = v1.ID_WIKIDATA AND p31.ID_PROPERTY = 'P31'
       AND COALESCE(p31.DELETED,0)=0
LEFT JOIN T_WC_WIKIDATA_ITEM_V1 cls ON cls.ID_WIKIDATA = p31.ID_ITEM
WHERE  COALESCE(v1.DELETED,0)=0
  AND  NOT EXISTS (
         SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
         JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
         WHERE  st.ID_WIKIDATA = p31.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
           AND  st.ID_PROPERTY = 'P31'
           AND  iv.ID_ITEM     = p31.ID_ITEM COLLATE utf8mb4_unicode_ci)
GROUP BY p31.ID_ITEM, cls.LABEL
ORDER BY nb_entities_missing_this_p31 DESC
LIMIT 40;


-- ############################################################################
-- ###  SERIE   (cas critique : V2 < V1)                                    ###
-- ############################################################################

-- ----------------------------------------------------------------------------
-- 2A · serie · inclusion : QID V1 actifs absents de la table SERIE V2
-- ----------------------------------------------------------------------------
SELECT '---------- 2A · serie · inclusion (V1 absents de V2 meme type) ----------' AS section;

SELECT COUNT(*)                                                   AS v1_active_total,
       SUM(CASE WHEN v2.ID_WIKIDATA IS NULL THEN 1 ELSE 0 END)    AS missing_in_v2_same_type
FROM   T_WC_WIKIDATA_SERIE_V1 v1
LEFT JOIN T_WC_WIKIDATA_SERIE v2
       ON v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
WHERE  COALESCE(v1.DELETED,0)=0;

-- ----------------------------------------------------------------------------
-- 2B · serie · localisation des manquants : ailleurs en V2 vs absents partout
-- ----------------------------------------------------------------------------
SELECT '---------- 2B · serie · localisation des manquants ----------' AS section;

SELECT
  SUM(CASE WHEN present_anywhere_v2 = 1 THEN 1 ELSE 0 END) AS present_other_v2_table,
  SUM(CASE WHEN present_anywhere_v2 = 0 THEN 1 ELSE 0 END) AS absent_everywhere_v2
FROM (
  SELECT v1.ID_WIKIDATA,
    (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE   m  WHERE m.ID_WIKIDATA  = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE   s  WHERE s.ID_WIKIDATA  = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON  p  WHERE p.ID_WIKIDATA  = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON  se WHERE se.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE ep WHERE ep.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM    it WHERE it.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
    ) AS present_anywhere_v2
  FROM T_WC_WIKIDATA_SERIE_V1 v1
  WHERE COALESCE(v1.DELETED,0)=0
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE v2
                    WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t;

-- ----------------------------------------------------------------------------
-- 2C · serie · LA requete : classes INSTANCE_OF des series V1 absentes de V2,
--       par volume perdu (colonne denormalisee = 1 classe/entite)
-- ----------------------------------------------------------------------------
SELECT '---------- 2C · serie · manquants ventiles par INSTANCE_OF ----------' AS section;

SELECT v1.INSTANCE_OF AS class_qid, cls.LABEL AS class_label,
       COUNT(*)       AS nb_series_v1_missing_in_v2
FROM   T_WC_WIKIDATA_SERIE_V1 v1
LEFT JOIN T_WC_WIKIDATA_ITEM_V1 cls ON cls.ID_WIKIDATA = v1.INSTANCE_OF
WHERE  COALESCE(v1.DELETED,0)=0
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE v2
                   WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
GROUP BY v1.INSTANCE_OF, cls.LABEL
ORDER BY nb_series_v1_missing_in_v2 DESC
LIMIT 40;

-- ----------------------------------------------------------------------------
-- 2C-bis · serie · variante exhaustive (toutes les valeurs P31 via ITEM_PROPERTY,
--       pas seulement la colonne denormalisee)
-- ----------------------------------------------------------------------------
SELECT '---------- 2C-bis · serie · manquants par P31 (ITEM_PROPERTY, multi-valeur) ----------' AS section;

SELECT p31.ID_ITEM AS class_qid, cls.LABEL AS class_label,
       COUNT(DISTINCT p31.ID_WIKIDATA) AS nb_series_v1_missing_in_v2
FROM   T_WC_WIKIDATA_SERIE_V1 v1
JOIN   T_WC_WIKIDATA_ITEM_PROPERTY p31
       ON p31.ID_WIKIDATA = v1.ID_WIKIDATA AND p31.ID_PROPERTY = 'P31'
       AND COALESCE(p31.DELETED,0)=0
LEFT JOIN T_WC_WIKIDATA_ITEM_V1 cls ON cls.ID_WIKIDATA = p31.ID_ITEM
WHERE  COALESCE(v1.DELETED,0)=0
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE v2
                   WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
GROUP BY p31.ID_ITEM, cls.LABEL
ORDER BY nb_series_v1_missing_in_v2 DESC
LIMIT 40;

-- ----------------------------------------------------------------------------
-- 2C-ter · serie · TOUTES les classes vues en V1, couvertes ou non en V2
--       (class_used_as_p31_in_v2 = 0 sur fort volume = classe hors perimetre V2,
--        candidate a ajouter a SERIES_ROOTS / descendants P279)
-- ----------------------------------------------------------------------------
SELECT '---------- 2C-ter · serie · couverture des classes V1 dans les statements V2 ----------' AS section;

SELECT v1.INSTANCE_OF AS class_qid, cls.LABEL AS class_label,
       COUNT(*)       AS nb_series_v1,
       EXISTS (
         SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
         JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
         WHERE  st.ID_PROPERTY = 'P31'
           AND  iv.ID_ITEM = v1.INSTANCE_OF COLLATE utf8mb4_unicode_ci
       ) AS class_used_as_p31_in_v2
FROM   T_WC_WIKIDATA_SERIE_V1 v1
LEFT JOIN T_WC_WIKIDATA_ITEM_V1 cls ON cls.ID_WIKIDATA = v1.INSTANCE_OF
WHERE  COALESCE(v1.DELETED,0)=0
GROUP BY v1.INSTANCE_OF, cls.LABEL
ORDER BY nb_series_v1 DESC
LIMIT 40;

-- ----------------------------------------------------------------------------
-- 2D · serie · trou d'ingestion P31 : entites communes dont une valeur P31
--       de V1 est absente des statements V2
-- ----------------------------------------------------------------------------
SELECT '---------- 2D · serie · P31 present V1 absent V2 (entites communes) ----------' AS section;

SELECT p31.ID_ITEM AS class_qid, cls.LABEL AS class_label,
       COUNT(DISTINCT p31.ID_WIKIDATA) AS nb_entities_missing_this_p31
FROM   T_WC_WIKIDATA_SERIE_V1 v1
JOIN   T_WC_WIKIDATA_SERIE v2
       ON v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
JOIN   T_WC_WIKIDATA_ITEM_PROPERTY p31
       ON p31.ID_WIKIDATA = v1.ID_WIKIDATA AND p31.ID_PROPERTY = 'P31'
       AND COALESCE(p31.DELETED,0)=0
LEFT JOIN T_WC_WIKIDATA_ITEM_V1 cls ON cls.ID_WIKIDATA = p31.ID_ITEM
WHERE  COALESCE(v1.DELETED,0)=0
  AND  NOT EXISTS (
         SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
         JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
         WHERE  st.ID_WIKIDATA = p31.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
           AND  st.ID_PROPERTY = 'P31'
           AND  iv.ID_ITEM     = p31.ID_ITEM COLLATE utf8mb4_unicode_ci)
GROUP BY p31.ID_ITEM, cls.LABEL
ORDER BY nb_entities_missing_this_p31 DESC
LIMIT 40;


-- ############################################################################
-- ###  PERSON   (rappel : V2 applique la regle "person doit avoir un IMDb" ; ###
-- ###  un PERSON_V1 sans ID_IMDB peut legitimement n'etre en V2 que dans ITEM) ###
-- ############################################################################

-- ----------------------------------------------------------------------------
-- 3A · person · inclusion : QID V1 actifs absents de la table PERSON V2
-- ----------------------------------------------------------------------------
SELECT '---------- 3A · person · inclusion (V1 absents de V2 meme type) ----------' AS section;

SELECT COUNT(*)                                                   AS v1_active_total,
       SUM(CASE WHEN v2.ID_WIKIDATA IS NULL THEN 1 ELSE 0 END)    AS missing_in_v2_same_type
FROM   T_WC_WIKIDATA_PERSON_V1 v1
LEFT JOIN T_WC_WIKIDATA_PERSON v2
       ON v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
WHERE  COALESCE(v1.DELETED,0)=0;

-- ----------------------------------------------------------------------------
-- 3B · person · localisation des manquants : ailleurs en V2 vs absents partout
-- ----------------------------------------------------------------------------
SELECT '---------- 3B · person · localisation des manquants ----------' AS section;

SELECT
  SUM(CASE WHEN present_anywhere_v2 = 1 THEN 1 ELSE 0 END) AS present_other_v2_table,
  SUM(CASE WHEN present_anywhere_v2 = 0 THEN 1 ELSE 0 END) AS absent_everywhere_v2
FROM (
  SELECT v1.ID_WIKIDATA,
    (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE   m  WHERE m.ID_WIKIDATA  = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE   s  WHERE s.ID_WIKIDATA  = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON  p  WHERE p.ID_WIKIDATA  = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON  se WHERE se.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE ep WHERE ep.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM    it WHERE it.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
    ) AS present_anywhere_v2
  FROM T_WC_WIKIDATA_PERSON_V1 v1
  WHERE COALESCE(v1.DELETED,0)=0
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON v2
                    WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t;

-- ----------------------------------------------------------------------------
-- 3B-bis · person · les manquants ont-ils un ID_IMDB ? (teste la regle V2)
-- ----------------------------------------------------------------------------
SELECT '---------- 3B-bis · person · manquants avec/sans ID_IMDB ----------' AS section;

SELECT CASE WHEN v1.ID_IMDB IS NULL OR v1.ID_IMDB = '' THEN 'sans_imdb' ELSE 'avec_imdb' END AS imdb_bucket,
       COUNT(*) AS nb_missing
FROM   T_WC_WIKIDATA_PERSON_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON v2
                   WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
GROUP BY imdb_bucket;

-- ----------------------------------------------------------------------------
-- 3C · person · classes INSTANCE_OF des manquants, par volume perdu
-- ----------------------------------------------------------------------------
SELECT '---------- 3C · person · manquants ventiles par INSTANCE_OF ----------' AS section;

SELECT v1.INSTANCE_OF AS class_qid, cls.LABEL AS class_label,
       COUNT(*)       AS nb_v1_missing_in_v2
FROM   T_WC_WIKIDATA_PERSON_V1 v1
LEFT JOIN T_WC_WIKIDATA_ITEM_V1 cls ON cls.ID_WIKIDATA = v1.INSTANCE_OF
WHERE  COALESCE(v1.DELETED,0)=0
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON v2
                   WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
GROUP BY v1.INSTANCE_OF, cls.LABEL
ORDER BY nb_v1_missing_in_v2 DESC
LIMIT 40;

-- ----------------------------------------------------------------------------
-- 3D · person · trou d'ingestion P31 : entites communes dont une valeur P31
--       de V1 est absente des statements V2
-- ----------------------------------------------------------------------------
SELECT '---------- 3D · person · P31 present V1 absent V2 (entites communes) ----------' AS section;

SELECT p31.ID_ITEM AS class_qid, cls.LABEL AS class_label,
       COUNT(DISTINCT p31.ID_WIKIDATA) AS nb_entities_missing_this_p31
FROM   T_WC_WIKIDATA_PERSON_V1 v1
JOIN   T_WC_WIKIDATA_PERSON v2
       ON v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
JOIN   T_WC_WIKIDATA_ITEM_PROPERTY p31
       ON p31.ID_WIKIDATA = v1.ID_WIKIDATA AND p31.ID_PROPERTY = 'P31'
       AND COALESCE(p31.DELETED,0)=0
LEFT JOIN T_WC_WIKIDATA_ITEM_V1 cls ON cls.ID_WIKIDATA = p31.ID_ITEM
WHERE  COALESCE(v1.DELETED,0)=0
  AND  NOT EXISTS (
         SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
         JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
         WHERE  st.ID_WIKIDATA = p31.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
           AND  st.ID_PROPERTY = 'P31'
           AND  iv.ID_ITEM     = p31.ID_ITEM COLLATE utf8mb4_unicode_ci)
GROUP BY p31.ID_ITEM, cls.LABEL
ORDER BY nb_entities_missing_this_p31 DESC
LIMIT 40;

SELECT '========== FIN ==========' AS section;
-- ============================================================================
