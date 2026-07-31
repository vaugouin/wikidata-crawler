-- ============================================================================
-- Wikidata V1 -> V2 : VALIDATION D'APRES RE-RUN (lignes ET colonnes)
-- ============================================================================
--
-- BUT : apres le re-run de wikidata-crawler (pass1 relance avec Q15416 remis
--       dans SERIES_ROOTS), etablir ce qui reste entre V2 et V1, pour decider
--       si on peut decommissionner sparql-crawler + sparql-movies-persons.
--       Ticket WIKIDATA-CRAWLER-015.
--
-- Ce fichier SUCCEDE a doc/sql/wikidata-v1-v2-comparison.sql (qui reste valable
-- pour la version exacte, non echantillonnee, des ventilations P31). Nouveautes :
--   . les 7 couples d'entites, pas seulement movie/serie/person
--   . le volet COLONNES : ce que V1 porte, ce que V2 sait rendre, ce qui est perdu
--   . le volet TEXTE LOCALISE (labels/descriptions FR+EN), angle mort de -015
--
-- LECTURE SEULE : que des SELECT. Aucune ecriture, aucun DDL. Sans danger
-- pendant un crawl.
--
-- COMMENT EXECUTER (VPS, conteneur mariadb) :
--   docker cp doc/sql/wikidata-v1-v2-validation.sql <conteneur>:/bitnami/mariadb/data/
--   docker exec -i <conteneur> mariadb -uroot -p --default-character-set=utf8mb4 \
--       -t --force vaugouindb < /bitnami/mariadb/data/wikidata-v1-v2-validation.sql \
--       > validation.txt 2>&1
--   . -t          : sortie en tableaux, lisible a recoller
--   . --force     : ne pas s'arreter a la premiere erreur (la section 10 depend
--                   d'une colonne ajoutee par wikipedia-crawler, elle peut manquer)
--
-- COUT : chaque section porte une etiquette [rapide] / [moyen] / [lent].
--        Ordre choisi du moins cher au plus cher : meme interrompu, le fichier
--        aura deja repondu a l'essentiel.
--
-- ECHANTILLONNAGE : les sections 6 et 7 travaillent sur un echantillon
--        (LIMIT 20000 sans ORDER BY = ordre de parcours de l'index, donc biaise
--        vers les lignes les plus anciennes). Les taux sont indicatifs, les
--        sections 1 a 5 sont exactes.
--
-- COLLATION : les COLLATE utf8mb4_unicode_ci sont conserves par prudence. Depuis
--        la standardisation, V1 et V2 sont tous deux en unicode_ci : ce sont donc
--        des no-op, et ils ne bloquent pas l'usage des index (la collation porte
--        sur la valeur venant de V1, pas sur la colonne indexee de V2).
--
-- CONVENTION DELETED : V1.DELETED NULL vaut actif, d'ou COALESCE(DELETED,0)=0.
-- ============================================================================

-- La connexion doit parler la meme collation que les tables (toutes en
-- utf8mb4_unicode_ci depuis la standardisation). Sans cette ligne, la collation
-- de connexion reste utf8mb4_general_ci et toute valeur FABRIQUEE par une
-- fonction (CAST, CONVERT, CONCAT sur un nombre) porte general_ci : la comparer
-- a une colonne leve l'erreur 1267 "Illegal mix of collations".
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET SESSION max_statement_time = 0;

SELECT NOW() AS lance_le, DATABASE() AS base, VERSION() AS version_serveur;


-- ############################################################################
-- ### 0 . CONTEXTE : de quel run parle-t-on ?                    [rapide]  ###
-- ############################################################################
-- Sans ca, tous les chiffres qui suivent sont ininterpretables : on ne sait pas
-- si la base contient bien le run termine, ni s'il s'est termine proprement.

SELECT '=== 0A . etat du dernier run wikidata-crawler ===' AS section;

SELECT VAR_NAME, VAR_VALUE, TIM_UPDATED
FROM   T_WC_SERVER_VARIABLE
WHERE  VAR_NAME LIKE 'strwikidatacrawler%'
  AND  VAR_NAME NOT LIKE '%step1%'          -- le detail par etape alourdit sans servir ici
ORDER BY VAR_NAME;

SELECT '=== 0B . detail par etape (101 a 114) ===' AS section;

SELECT VAR_NAME, VAR_VALUE
FROM   T_WC_SERVER_VARIABLE
WHERE  VAR_NAME LIKE 'strwikidatacrawlerstep1%'
ORDER BY VAR_NAME;

SELECT '=== 0C . volumetrie (estimations InnoDB, instantanees) ===' AS section;
-- TABLE_ROWS est une estimation InnoDB, pas un COUNT(*). Suffisant pour situer
-- les ordres de grandeur, notamment sur les tables de statements.

SELECT TABLE_NAME, TABLE_ROWS AS lignes_estimees,
       ROUND((DATA_LENGTH + INDEX_LENGTH)/1024/1024/1024, 2) AS taille_go,
       UPDATE_TIME AS derniere_ecriture
FROM   information_schema.TABLES
WHERE  TABLE_SCHEMA = DATABASE()
  AND  TABLE_NAME LIKE 'T_WC_WIKIDATA%'
ORDER BY TABLE_ROWS DESC;


-- ############################################################################
-- ### 1 . LA QUESTION PRINCIPALE : V1 est-il inclus dans V2 ?               ###
-- ############################################################################

SELECT '=== 1A . inclusion lignes, les 7 couples (exact) ===  [moyen]' AS section;
-- v1_absents_de_v2 = QID actif en V1 qui n'existe pas dans la table V2 de MEME type.
-- C'est la mesure stricte de "V1 inclus dans V2". Le pourcentage est calcule sur
-- les actifs V1. ITEM est compte en QID distincts (V1 y fait une ligne par langue).

SELECT entite, v1_actifs, v2_actifs, v1_absents_de_v2,
       ROUND(100 * v1_absents_de_v2 / NULLIF(v1_actifs,0), 2) AS pct_absents,
       v2_actifs - v1_actifs AS ecart_brut
FROM (
  SELECT 'movie' AS entite,
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE_V1 v WHERE COALESCE(v.DELETED,0)=0) AS v1_actifs,
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE    v WHERE COALESCE(v.DELETED,0)=0) AS v2_actifs,
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_MOVIE_V1 v1 WHERE COALESCE(v1.DELETED,0)=0
       AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE v2
                       WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)) AS v1_absents_de_v2
  UNION ALL
  SELECT 'serie',
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_SERIE_V1 v WHERE COALESCE(v.DELETED,0)=0),
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_SERIE    v WHERE COALESCE(v.DELETED,0)=0),
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_SERIE_V1 v1 WHERE COALESCE(v1.DELETED,0)=0
       AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE v2
                       WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci))
  UNION ALL
  SELECT 'person',
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON_V1 v WHERE COALESCE(v.DELETED,0)=0),
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON    v WHERE COALESCE(v.DELETED,0)=0),
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_PERSON_V1 v1 WHERE COALESCE(v1.DELETED,0)=0
       AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON v2
                       WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci))
  UNION ALL
  SELECT 'season',
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_SEASON_V1 v WHERE COALESCE(v.DELETED,0)=0),
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_SEASON    v WHERE COALESCE(v.DELETED,0)=0),
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_SEASON_V1 v1 WHERE COALESCE(v1.DELETED,0)=0
       AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON v2
                       WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci))
  UNION ALL
  SELECT 'episode',
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_EPISODE_V1 v WHERE COALESCE(v.DELETED,0)=0),
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_EPISODE    v WHERE COALESCE(v.DELETED,0)=0),
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_EPISODE_V1 v1 WHERE COALESCE(v1.DELETED,0)=0
       AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE v2
                       WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci))
  UNION ALL
  SELECT 'character',
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_CHARACTER_V1 v WHERE COALESCE(v.DELETED,0)=0),
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_CHARACTER    v WHERE COALESCE(v.DELETED,0)=0),
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_CHARACTER_V1 v1 WHERE COALESCE(v1.DELETED,0)=0
       AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER v2
                       WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci))
  UNION ALL
  SELECT 'item (QID distincts)',
    (SELECT COUNT(DISTINCT v.ID_WIKIDATA) FROM T_WC_WIKIDATA_ITEM_V1 v WHERE COALESCE(v.DELETED,0)=0),
    (SELECT COUNT(*) FROM T_WC_WIKIDATA_ITEM v WHERE COALESCE(v.DELETED,0)=0),
    (SELECT COUNT(DISTINCT v1.ID_WIKIDATA) FROM T_WC_WIKIDATA_ITEM_V1 v1 WHERE COALESCE(v1.DELETED,0)=0
       AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM v2
                       WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci))
) t;


SELECT '=== 1B . ou sont les absents ? (autre table V2, ou nulle part) ===  [lent]' AS section;
-- Un QID absent de la table V2 de meme type n'est pas forcement perdu : il peut
-- avoir ete range ailleurs (typiquement dans ITEM, ou reclasse movie <-> serie).
-- Seul "absents_partout" est une vraie perte d'entite.

SELECT 'movie' AS entite, SUM(pres) AS ailleurs_en_v2, SUM(1-pres) AS absents_partout FROM (
  SELECT (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE   x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM      x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)) AS pres
  FROM T_WC_WIKIDATA_MOVIE_V1 v1
  WHERE COALESCE(v1.DELETED,0)=0
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE v2 WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'serie', SUM(pres), SUM(1-pres) FROM (
  SELECT (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE   x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM      x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)) AS pres
  FROM T_WC_WIKIDATA_SERIE_V1 v1
  WHERE COALESCE(v1.DELETED,0)=0
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE v2 WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'person', SUM(pres), SUM(1-pres) FROM (
  SELECT (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM      x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)) AS pres
  FROM T_WC_WIKIDATA_PERSON_V1 v1
  WHERE COALESCE(v1.DELETED,0)=0
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON v2 WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'season', SUM(pres), SUM(1-pres) FROM (
  SELECT (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE   x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)) AS pres
  FROM T_WC_WIKIDATA_SEASON_V1 v1
  WHERE COALESCE(v1.DELETED,0)=0
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON v2 WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'episode', SUM(pres), SUM(1-pres) FROM (
  SELECT (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE  x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM   x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)) AS pres
  FROM T_WC_WIKIDATA_EPISODE_V1 v1
  WHERE COALESCE(v1.DELETED,0)=0
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE v2 WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'character', SUM(pres), SUM(1-pres) FROM (
  SELECT (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE  x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM   x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)) AS pres
  FROM T_WC_WIKIDATA_CHARACTER_V1 v1
  WHERE COALESCE(v1.DELETED,0)=0
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER v2 WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t;


-- ############################################################################
-- ### 2 . SERIE : le correctif Q15416 a-t-il pris ?                        ###
-- ############################################################################
-- Rappel : avant le re-run, 60 448 series V1 manquaient en V2, dont 57 908
-- (96 %) portaient l'unique classe Q15416 "television program", volontairement
-- exclue. Q15416 est desormais dans SERIES_ROOTS et EXCLUDED_SERIES_ROOTS est
-- vide (wikidata_dump_etl.py:54-62). On verifie l'effet.

SELECT '=== 2A . Q15416 est-il entre en V2 ? ===  [moyen]' AS section;

SELECT
  (SELECT COUNT(DISTINCT st.ID_WIKIDATA)
     FROM T_WC_WIKIDATA_ITEM_VALUE iv
     JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_STATEMENT = iv.ID_STATEMENT
    WHERE iv.ID_ITEM = 'Q15416' AND st.ID_PROPERTY = 'P31')            AS entites_p31_q15416_en_v2,
  (SELECT COUNT(DISTINCT st.ID_WIKIDATA)
     FROM T_WC_WIKIDATA_ITEM_VALUE iv
     JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_STATEMENT = iv.ID_STATEMENT
     JOIN T_WC_WIKIDATA_SERIE     s  ON s.ID_WIKIDATA   = st.ID_WIKIDATA
    WHERE iv.ID_ITEM = 'Q15416' AND st.ID_PROPERTY = 'P31')            AS dont_classees_serie_v2;

SELECT '=== 2B . series V1 encore absentes, ventilees par INSTANCE_OF ===  [moyen]' AS section;
-- Attendu apres correctif : Q15416 ne doit plus dominer. Ce qui reste en tete
-- designe les prochaines classes candidates a SERIES_ROOTS.

-- Le libelle de classe est cherche APRES le regroupement (sous-requete scalaire)
-- et non par une jointure : ITEM_V1 fait une ligne par langue, une jointure
-- gonflerait les COUNT(*) par double comptage FR / EN.

SELECT t.classe_qid,
       (SELECT i.LABEL FROM T_WC_WIKIDATA_ITEM_V1 i
        WHERE i.ID_WIKIDATA = t.classe_qid AND i.LANG = 'en' LIMIT 1) AS classe_libelle,
       t.nb_series_v1_absentes
FROM (
  SELECT v1.INSTANCE_OF AS classe_qid, COUNT(*) AS nb_series_v1_absentes
  FROM   T_WC_WIKIDATA_SERIE_V1 v1
  WHERE  COALESCE(v1.DELETED,0)=0
    AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE v2
                     WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  GROUP BY v1.INSTANCE_OF
  ORDER BY nb_series_v1_absentes DESC
  LIMIT 30
) t
ORDER BY t.nb_series_v1_absentes DESC;

SELECT '=== 2C . sens inverse : V2 est-il devenu un sur-ensemble ? ===  [moyen]' AS section;
-- L'ajout de Q15416 tire tout son sous-arbre P279 (infos, talk-shows, jeux).
-- Consequence assumee : SERIE V2 doit desormais contenir bien plus que V1.

SELECT COUNT(*) AS series_v2_absentes_de_v1
FROM   T_WC_WIKIDATA_SERIE v2
WHERE  COALESCE(v2.DELETED,0)=0
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE_V1 v1
                   WHERE v1.ID_WIKIDATA = v2.ID_WIKIDATA COLLATE utf8mb4_unicode_ci);


-- ############################################################################
-- ### 3 . PERSON : que reste-t-il du gap, et de quelle nature ?            ###
-- ############################################################################
-- Rappel : 51 415 manquants avant le re-run, dont 48 819 sans IMDb (regle V2
-- "person doit avoir un IMDb", assumee) et 2 596 AVEC IMDb (anomalie non
-- expliquee). L'elargissement serie devait en rattraper une partie par la
-- regle 2 (personne referencee par une oeuvre).

SELECT '=== 3A . manquants person, avec ou sans IMDb ===  [moyen]' AS section;

SELECT CASE WHEN v1.ID_IMDB IS NULL OR v1.ID_IMDB = '' THEN 'sans_imdb (attendu)'
            ELSE 'avec_imdb (anomalie)' END AS categorie,
       COUNT(*) AS nb_manquants
FROM   T_WC_WIKIDATA_PERSON_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON v2
                   WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
GROUP BY categorie;

SELECT '=== 3B . les manquants AVEC IMDb : sont-ils quelque part en V2 ? ===  [moyen]' AS section;
-- Si "absent_partout" domine, c'est une divergence dump / crawl live (le QID
-- n'est plus dans le dump : fusion, suppression, redirection).

SELECT SUM(CASE WHEN dans_item = 1 THEN 1 ELSE 0 END) AS presents_en_item_v2,
       SUM(CASE WHEN dans_item = 0 AND ailleurs = 1 THEN 1 ELSE 0 END) AS ailleurs_en_v2,
       SUM(CASE WHEN dans_item = 0 AND ailleurs = 0 THEN 1 ELSE 0 END) AS absents_partout
FROM (
  SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci) AS dans_item,
         (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)) AS ailleurs
  FROM T_WC_WIKIDATA_PERSON_V1 v1
  WHERE COALESCE(v1.DELETED,0)=0
    AND v1.ID_IMDB IS NOT NULL AND v1.ID_IMDB <> ''
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON v2
                    WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t;

SELECT '=== 3C . echantillon de 25 manquants AVEC IMDb (a verifier a la main) ===  [rapide]' AS section;
-- A recouper sur wikidata.org : un QID qui redirige aujourd'hui vers un autre
-- confirme l'hypothese fusion / redirection plutot qu'un trou d'ingestion.

SELECT v1.ID_WIKIDATA, v1.NAME, v1.ID_IMDB, v1.INSTANCE_OF, v1.TIM_UPDATED
FROM   T_WC_WIKIDATA_PERSON_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0
  AND  v1.ID_IMDB IS NOT NULL AND v1.ID_IMDB <> ''
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON v2
                   WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
LIMIT 25;


-- ############################################################################
-- ### 4 . COLONNES : inventaire mecanique de l'ecart de schema  [rapide]   ###
-- ############################################################################
-- Liste, sans jugement, les colonnes que porte une table V1 et que sa jumelle
-- V2 n'a pas. C'est le point de depart du volet colonnes : la suite (sections
-- 5 a 10) dit lesquelles sont vraiment perdues et lesquelles sont retrouvables.

SELECT '=== 4A . colonnes presentes en V1, absentes de la table V2 ===' AS section;

SELECT c1.TABLE_NAME AS table_v1,
       REPLACE(c1.TABLE_NAME, '_V1', '') AS table_v2,
       c1.COLUMN_NAME, c1.COLUMN_TYPE
FROM   information_schema.COLUMNS c1
WHERE  c1.TABLE_SCHEMA = DATABASE()
  AND  c1.TABLE_NAME IN ('T_WC_WIKIDATA_MOVIE_V1','T_WC_WIKIDATA_SERIE_V1',
                         'T_WC_WIKIDATA_PERSON_V1','T_WC_WIKIDATA_ITEM_V1',
                         'T_WC_WIKIDATA_SEASON_V1','T_WC_WIKIDATA_EPISODE_V1',
                         'T_WC_WIKIDATA_CHARACTER_V1')
  AND  NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS c2
                   WHERE c2.TABLE_SCHEMA = c1.TABLE_SCHEMA
                     AND c2.TABLE_NAME   = REPLACE(c1.TABLE_NAME, '_V1', '')
                     AND c2.COLUMN_NAME  = c1.COLUMN_NAME)
ORDER BY c1.TABLE_NAME, c1.ORDINAL_POSITION;


-- ############################################################################
-- ### 5 . COLONNES : lesquelles sont REELLEMENT remplies en V1 ?  [moyen]  ###
-- ############################################################################
-- Une colonne qui existe au schema mais qui est vide ne se perd pas. Ce bloc
-- mesure le volume reel a reloger, colonne par colonne. Un seul balayage par
-- table.

SELECT '=== 5A . movie : remplissage des colonnes V1 ===' AS section;
SELECT COUNT(*) AS lignes_actives,
       SUM(TITLE           IS NOT NULL AND TITLE           <> '') AS title,
       SUM(ALIASES         IS NOT NULL AND ALIASES         <> '') AS aliases,
       SUM(ID_MOVIE        IS NOT NULL AND ID_MOVIE        <> 0)  AS id_movie_tmdb,
       SUM(ID_IMDB         IS NOT NULL AND ID_IMDB         <> '') AS id_imdb,
       SUM(DAT_RELEASE     IS NOT NULL)                           AS dat_release,
       SUM(INSTANCE_OF     IS NOT NULL AND INSTANCE_OF     <> '') AS instance_of,
       SUM(PLEX_MEDIA_KEY  IS NOT NULL AND PLEX_MEDIA_KEY  <> '') AS plex_media_key,
       SUM(ID_CRITERION    IS NOT NULL)                           AS id_criterion,
       SUM(ID_CRITERION_SPINE IS NOT NULL)                        AS id_criterion_spine,
       SUM(WIKIPEDIA_POSTER_PATH IS NOT NULL AND WIKIPEDIA_POSTER_PATH <> '') AS wikipedia_poster_path
FROM   T_WC_WIKIDATA_MOVIE_V1 WHERE COALESCE(DELETED,0)=0;

SELECT '=== 5B . serie : remplissage des colonnes V1 ===' AS section;
SELECT COUNT(*) AS lignes_actives,
       SUM(TITLE           IS NOT NULL AND TITLE           <> '') AS title,
       SUM(ALIASES         IS NOT NULL AND ALIASES         <> '') AS aliases,
       SUM(ID_SERIE        IS NOT NULL AND ID_SERIE        <> 0)  AS id_serie_tmdb,
       SUM(ID_IMDB         IS NOT NULL AND ID_IMDB         <> '') AS id_imdb,
       SUM(DAT_START       IS NOT NULL)                           AS dat_start,
       SUM(DAT_END         IS NOT NULL)                           AS dat_end,
       SUM(INSTANCE_OF     IS NOT NULL AND INSTANCE_OF     <> '') AS instance_of,
       SUM(PLEX_MEDIA_KEY  IS NOT NULL AND PLEX_MEDIA_KEY  <> '') AS plex_media_key,
       SUM(ID_CRITERION    IS NOT NULL)                           AS id_criterion,
       SUM(WIKIPEDIA_POSTER_PATH IS NOT NULL AND WIKIPEDIA_POSTER_PATH <> '') AS wikipedia_poster_path
FROM   T_WC_WIKIDATA_SERIE_V1 WHERE COALESCE(DELETED,0)=0;

SELECT '=== 5C . person : remplissage des colonnes V1 ===' AS section;
-- Note : PERSON_V1 ne porte NI SEX_GENDER NI COUNTRY_OF_CITIZENSHIP. Ces deux
-- colonnes n'existent que sur CHARACTER_V1 (voir 5F). La question "V2 sait-il
-- rendre le genre et la nationalite d'une personne" ne se pose donc pas au titre
-- de la parite : V1 ne les a jamais portees pour les personnes.
SELECT COUNT(*) AS lignes_actives,
       SUM(NAME    IS NOT NULL AND NAME    <> '') AS name,
       SUM(ALIASES IS NOT NULL AND ALIASES <> '') AS aliases,
       SUM(ID_PERSON IS NOT NULL AND ID_PERSON <> 0) AS id_person_tmdb,
       SUM(ID_IMDB IS NOT NULL AND ID_IMDB <> '') AS id_imdb,
       SUM(BIRTHDAY IS NOT NULL)                  AS birthday,
       SUM(DEATHDAY IS NOT NULL)                  AS deathday,
       SUM(INSTANCE_OF IS NOT NULL AND INSTANCE_OF <> '') AS instance_of,
       SUM(WIKIPEDIA_PROFILE_PATH IS NOT NULL AND WIKIPEDIA_PROFILE_PATH <> '') AS wikipedia_profile_path
FROM   T_WC_WIKIDATA_PERSON_V1 WHERE COALESCE(DELETED,0)=0;

SELECT '=== 5D . item : remplissage des colonnes V1 (par langue) ===' AS section;
SELECT LANG,
       COUNT(*) AS lignes,
       COUNT(DISTINCT ID_WIKIDATA) AS qid_distincts,
       SUM(LABEL       IS NOT NULL AND LABEL       <> '') AS label,
       SUM(DESCRIPTION IS NOT NULL AND DESCRIPTION <> '') AS description,
       SUM(ALIASES     IS NOT NULL AND ALIASES     <> '') AS aliases,
       SUM(WIKIPEDIA_IMAGE_PATH IS NOT NULL AND WIKIPEDIA_IMAGE_PATH <> '') AS wikipedia_image_path
FROM   T_WC_WIKIDATA_ITEM_V1 WHERE COALESCE(DELETED,0)=0
GROUP BY LANG ORDER BY lignes DESC LIMIT 15;

SELECT '=== 5E . season / episode / character : remplissage ===' AS section;
SELECT 'season' AS entite, COUNT(*) AS lignes_actives,
       SUM(ID_SEASON IS NOT NULL AND ID_SEASON <> 0) AS id_tmdb,
       SUM(SEASON_NUMBER IS NOT NULL)                AS numero,
       SUM(ID_WIKIDATA_SERIE IS NOT NULL AND ID_WIKIDATA_SERIE <> '') AS lien_serie,
       SUM(ID_IMDB IS NOT NULL AND ID_IMDB <> '')    AS id_imdb,
       NULL AS aliases
FROM   T_WC_WIKIDATA_SEASON_V1 WHERE COALESCE(DELETED,0)=0
UNION ALL
SELECT 'episode', COUNT(*),
       SUM(ID_EPISODE IS NOT NULL AND ID_EPISODE <> 0),
       SUM(EPISODE_NUMBER IS NOT NULL),
       SUM(ID_WIKIDATA_SERIE IS NOT NULL AND ID_WIKIDATA_SERIE <> ''),
       SUM(ID_IMDB IS NOT NULL AND ID_IMDB <> ''),
       NULL
FROM   T_WC_WIKIDATA_EPISODE_V1 WHERE COALESCE(DELETED,0)=0
UNION ALL
SELECT 'character', COUNT(*),
       SUM(ID_PERSON IS NOT NULL AND ID_PERSON <> 0),
       NULL,
       NULL,
       SUM(ID_IMDB IS NOT NULL AND ID_IMDB <> ''),
       SUM(ALIASES IS NOT NULL AND ALIASES <> '')
FROM   T_WC_WIKIDATA_CHARACTER_V1 WHERE COALESCE(DELETED,0)=0;

SELECT '=== 5F . character : les 2 colonnes que V1 ne porte QUE la ===' AS section;
-- SEX_GENDER (P21) et COUNTRY_OF_CITIZENSHIP (P27) n'existent que sur
-- CHARACTER_V1. Aucun code du parc ne les ecrit ni ne les lit (elles
-- n'apparaissent que dans les dumps de DDL) : si ces compteurs sont a zero, ce
-- sont des colonnes mortes, a rayer de l'inventaire de decommission plutot qu'a
-- reloger.
SELECT COUNT(*) AS lignes_actives,
       SUM(SEX_GENDER IS NOT NULL AND SEX_GENDER <> '') AS sex_gender,
       SUM(COUNTRY_OF_CITIZENSHIP IS NOT NULL AND COUNTRY_OF_CITIZENSHIP <> '') AS country_of_citizenship,
       SUM(BIRTHDAY IS NOT NULL) AS birthday,
       SUM(DEATHDAY IS NOT NULL) AS deathday,
       SUM(WIKIPEDIA_PROFILE_PATH IS NOT NULL AND WIKIPEDIA_PROFILE_PATH <> '') AS wikipedia_profile_path
FROM   T_WC_WIKIDATA_CHARACTER_V1 WHERE COALESCE(DELETED,0)=0;


-- ############################################################################
-- ### 6 . COLONNES : la donnee est-elle retrouvable dans les statements ?  ###
-- ###     (echantillon de 20 000 entites communes par bloc)      [lent]    ###
-- ############################################################################
-- Chaque colonne V1 issue de Wikidata correspond a une propriete. La question
-- n'est pas "la colonne existe-t-elle en V2" (non, par construction) mais
-- "la propriete est-elle dans T_WC_WIKIDATA_STATEMENT". Les proprietes sont
-- celles reellement interrogees par les crawlers V1 (sparql-movies-persons.py).
--
-- Lecture : non_retrouves > 0 = la colonne n'est PAS reconstituable par requete
-- pour ces entites. Attention, deux causes possibles et distinctes : soit V2 n'a
-- pas ingere la propriete, soit Wikidata a change depuis le crawl V1 (V1 est un
-- historique accumule, V2 un instantane du dump).
--
-- Cas particulier des colonnes rares (PLEX_MEDIA_KEY, ID_CRITERION, DEATHDAY) :
-- l'echantillon est pris PARMI LES LIGNES REMPLIES, sinon 20 000 lignes tirees
-- au fil de l'index n'en contiendraient presque aucune. Pour ces blocs,
-- echantillon = v1_rempli, c'est voulu.

SELECT '=== 6A . movie : colonnes V1 vs proprietes V2 ===' AS section;

SELECT 'movie.ID_IMDB -> P345' AS colonne_v1, COUNT(*) AS echantillon,
       SUM(v1_rempli) AS v1_rempli,
       SUM(v1_rempli AND v2_present) AS retrouves_en_v2,
       SUM(v1_rempli AND NOT v2_present) AS non_retrouves
FROM (
  SELECT (v1.ID_IMDB IS NOT NULL AND v1.ID_IMDB <> '') AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P345') AS v2_present
  FROM (SELECT ID_WIKIDATA, ID_IMDB FROM T_WC_WIKIDATA_MOVIE_V1
        WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE m
                WHERE m.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'movie.DAT_RELEASE -> P577', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_present), SUM(v1_rempli AND NOT v2_present)
FROM (
  SELECT (v1.DAT_RELEASE IS NOT NULL) AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P577') AS v2_present
  FROM (SELECT ID_WIKIDATA, DAT_RELEASE FROM T_WC_WIKIDATA_MOVIE_V1
        WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE m
                WHERE m.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'movie.INSTANCE_OF -> P31', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_present), SUM(v1_rempli AND NOT v2_present)
FROM (
  SELECT (v1.INSTANCE_OF IS NOT NULL AND v1.INSTANCE_OF <> '') AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P31') AS v2_present
  FROM (SELECT ID_WIKIDATA, INSTANCE_OF FROM T_WC_WIKIDATA_MOVIE_V1
        WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE m
                WHERE m.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
-- ID_MOVIE vient de P4947 (TMDb movie ID) en V1, pas d'un enrichissement local.
SELECT 'movie.ID_MOVIE -> P4947', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_present), SUM(v1_rempli AND NOT v2_present)
FROM (
  SELECT (v1.ID_MOVIE IS NOT NULL AND v1.ID_MOVIE <> 0) AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P4947') AS v2_present
  FROM (SELECT ID_WIKIDATA, ID_MOVIE FROM T_WC_WIKIDATA_MOVIE_V1
        WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE m
                WHERE m.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
-- PLEX_MEDIA_KEY = P11460, ID_CRITERION = P9584, ID_CRITERION_SPINE = P12279 :
-- ce sont des proprietes Wikidata, contrairement a ce que supposait l'inventaire
-- initial du ticket -015 (qui les classait en "enrichissement local").
SELECT 'movie.PLEX_MEDIA_KEY -> P11460', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_present), SUM(v1_rempli AND NOT v2_present)
FROM (
  SELECT (v1.PLEX_MEDIA_KEY IS NOT NULL AND v1.PLEX_MEDIA_KEY <> '') AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P11460') AS v2_present
  FROM (SELECT ID_WIKIDATA, PLEX_MEDIA_KEY FROM T_WC_WIKIDATA_MOVIE_V1
        WHERE COALESCE(DELETED,0)=0 AND PLEX_MEDIA_KEY IS NOT NULL AND PLEX_MEDIA_KEY <> ''
        LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE m
                WHERE m.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'movie.ID_CRITERION -> P9584', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_present), SUM(v1_rempli AND NOT v2_present)
FROM (
  SELECT (v1.ID_CRITERION IS NOT NULL) AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P9584') AS v2_present
  FROM (SELECT ID_WIKIDATA, ID_CRITERION FROM T_WC_WIKIDATA_MOVIE_V1
        WHERE COALESCE(DELETED,0)=0 AND ID_CRITERION IS NOT NULL LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE m
                WHERE m.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t;

SELECT '=== 6B . serie : colonnes V1 vs proprietes V2 ===' AS section;
-- Note : V1 interroge P4947 (TMDb *movie* ID) meme pour les series
-- (sparql-movies-persons.py:526,546). On teste donc aussi P4983, la propriete
-- correcte pour les series, pour savoir laquelle porte reellement l'id.

SELECT 'serie.ID_IMDB -> P345' AS colonne_v1, COUNT(*) AS echantillon,
       SUM(v1_rempli) AS v1_rempli,
       SUM(v1_rempli AND v2_present) AS retrouves_en_v2,
       SUM(v1_rempli AND NOT v2_present) AS non_retrouves
FROM (
  SELECT (v1.ID_IMDB IS NOT NULL AND v1.ID_IMDB <> '') AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P345') AS v2_present
  FROM (SELECT ID_WIKIDATA, ID_IMDB FROM T_WC_WIKIDATA_SERIE_V1
        WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE s
                WHERE s.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'serie.DAT_START -> P580', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_present), SUM(v1_rempli AND NOT v2_present)
FROM (
  SELECT (v1.DAT_START IS NOT NULL) AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P580') AS v2_present
  FROM (SELECT ID_WIKIDATA, DAT_START FROM T_WC_WIKIDATA_SERIE_V1
        WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE s
                WHERE s.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'serie.DAT_END -> P582', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_present), SUM(v1_rempli AND NOT v2_present)
FROM (
  SELECT (v1.DAT_END IS NOT NULL) AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P582') AS v2_present
  FROM (SELECT ID_WIKIDATA, DAT_END FROM T_WC_WIKIDATA_SERIE_V1
        WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE s
                WHERE s.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'serie.ID_SERIE -> P4947 ou P4983', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_present), SUM(v1_rempli AND NOT v2_present)
FROM (
  SELECT (v1.ID_SERIE IS NOT NULL AND v1.ID_SERIE <> 0) AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY IN ('P4947','P4983')) AS v2_present
  FROM (SELECT ID_WIKIDATA, ID_SERIE FROM T_WC_WIKIDATA_SERIE_V1
        WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE s
                WHERE s.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t;

SELECT '=== 6C . person : colonnes V1 vs proprietes V2 ===' AS section;

SELECT 'person.ID_IMDB -> P345' AS colonne_v1, COUNT(*) AS echantillon,
       SUM(v1_rempli) AS v1_rempli,
       SUM(v1_rempli AND v2_present) AS retrouves_en_v2,
       SUM(v1_rempli AND NOT v2_present) AS non_retrouves
FROM (
  SELECT (v1.ID_IMDB IS NOT NULL AND v1.ID_IMDB <> '') AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P345') AS v2_present
  FROM (SELECT ID_WIKIDATA, ID_IMDB FROM T_WC_WIKIDATA_PERSON_V1
        WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON p
                WHERE p.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'person.BIRTHDAY -> P569', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_present), SUM(v1_rempli AND NOT v2_present)
FROM (
  SELECT (v1.BIRTHDAY IS NOT NULL) AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P569') AS v2_present
  FROM (SELECT ID_WIKIDATA, BIRTHDAY FROM T_WC_WIKIDATA_PERSON_V1
        WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON p
                WHERE p.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'person.DEATHDAY -> P570', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_present), SUM(v1_rempli AND NOT v2_present)
FROM (
  SELECT (v1.DEATHDAY IS NOT NULL) AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P570') AS v2_present
  FROM (SELECT ID_WIKIDATA, DEATHDAY FROM T_WC_WIKIDATA_PERSON_V1
        WHERE COALESCE(DELETED,0)=0 AND DEATHDAY IS NOT NULL LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON p
                WHERE p.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'person.ID_PERSON -> P4985', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_present), SUM(v1_rempli AND NOT v2_present)
FROM (
  SELECT (v1.ID_PERSON IS NOT NULL AND v1.ID_PERSON <> 0) AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P4985') AS v2_present
  FROM (SELECT ID_WIKIDATA, ID_PERSON FROM T_WC_WIKIDATA_PERSON_V1
        WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON p
                WHERE p.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t;

SELECT '=== 6D . episode : le rattachement a la serie est-il reconstituable ? ===' AS section;
-- V1 stocke ID_WIKIDATA_SERIE / SEASON_NUMBER / EPISODE_NUMBER, obtenus par
-- P179 (part of the series) et son qualificatif P1545 (series ordinal), plus
-- P4908 (season) pour le chemin episode -> saison -> serie. V2 a les statements
-- ET les qualificatifs : on verifie que les deux sont bien la.

SELECT 'episode.ID_WIKIDATA_SERIE -> P179' AS colonne_v1, COUNT(*) AS echantillon,
       SUM(v1_rempli) AS v1_rempli,
       SUM(v1_rempli AND v2_present) AS retrouves_en_v2,
       SUM(v1_rempli AND NOT v2_present) AS non_retrouves
FROM (
  SELECT (v1.ID_WIKIDATA_SERIE IS NOT NULL AND v1.ID_WIKIDATA_SERIE <> '') AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY IN ('P179','P4908')) AS v2_present
  FROM (SELECT ID_WIKIDATA, ID_WIKIDATA_SERIE FROM T_WC_WIKIDATA_EPISODE_V1
        WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE e
                WHERE e.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t
UNION ALL
SELECT 'episode.EPISODE_NUMBER -> qualificatif P1545', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_present), SUM(v1_rempli AND NOT v2_present)
FROM (
  SELECT (v1.EPISODE_NUMBER IS NOT NULL) AS v1_rempli,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q ON q.ID_STATEMENT = st.ID_STATEMENT
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY IN ('P179','P4908')
                   AND q.ID_QUALIFIER_PROPERTY = 'P1545') AS v2_present
  FROM (SELECT ID_WIKIDATA, EPISODE_NUMBER FROM T_WC_WIKIDATA_EPISODE_V1
        WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE e
                WHERE e.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t;


-- ############################################################################
-- ### 7 . TEXTE LOCALISE : l'angle mort de -015                            ###
-- ###     (WIKIDATA-CRAWLER-017)                                 [lent]    ###
-- ############################################################################
-- ITEM_V1(ID_WIKIDATA, LANG, LABEL) alimente 6 colonnes *_FR de T2S via
-- tmdb-movie-preprocess (WHERE LANG='fr'). V2 stocke la meme chose autrement :
-- un blob LABELS_JSON {lang: valeur}. Avant de couper V1, il faut la preuve que
-- le FR et l'EN sont bien la en V2.
--
-- Lecture : perdu_en_v2 > 0 = un libelle que V1 sait afficher et pas V2. C'est
-- ce chiffre qui doit tomber a zero (ou etre backfille) avant decommission.

SELECT '=== 7A . item : label et description FR / EN, V1 vs V2 (echantillon) ===' AS section;

SELECT 'item.LABEL fr' AS champ, COUNT(*) AS echantillon,
       SUM(v1_rempli) AS rempli_en_v1,
       SUM(v1_rempli AND v2_rempli) AS aussi_en_v2,
       SUM(v1_rempli AND NOT v2_rempli) AS perdu_en_v2
FROM (
  SELECT (v1.LABEL IS NOT NULL AND v1.LABEL <> '') AS v1_rempli,
         (JSON_UNQUOTE(JSON_EXTRACT(i2.LABELS_JSON, '$.fr')) IS NOT NULL) AS v2_rempli
  FROM (SELECT ID_WIKIDATA, LABEL FROM T_WC_WIKIDATA_ITEM_V1
        WHERE COALESCE(DELETED,0)=0 AND LANG='fr' LIMIT 20000) v1
  JOIN T_WC_WIKIDATA_ITEM i2 ON i2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
) t
UNION ALL
SELECT 'item.LABEL en', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_rempli), SUM(v1_rempli AND NOT v2_rempli)
FROM (
  SELECT (v1.LABEL IS NOT NULL AND v1.LABEL <> '') AS v1_rempli,
         (JSON_UNQUOTE(JSON_EXTRACT(i2.LABELS_JSON, '$.en')) IS NOT NULL) AS v2_rempli
  FROM (SELECT ID_WIKIDATA, LABEL FROM T_WC_WIKIDATA_ITEM_V1
        WHERE COALESCE(DELETED,0)=0 AND LANG='en' LIMIT 20000) v1
  JOIN T_WC_WIKIDATA_ITEM i2 ON i2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
) t
UNION ALL
SELECT 'item.DESCRIPTION fr', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_rempli), SUM(v1_rempli AND NOT v2_rempli)
FROM (
  SELECT (v1.DESCRIPTION IS NOT NULL AND v1.DESCRIPTION <> '') AS v1_rempli,
         (JSON_UNQUOTE(JSON_EXTRACT(i2.DESCRIPTIONS_JSON, '$.fr')) IS NOT NULL) AS v2_rempli
  FROM (SELECT ID_WIKIDATA, DESCRIPTION FROM T_WC_WIKIDATA_ITEM_V1
        WHERE COALESCE(DELETED,0)=0 AND LANG='fr' LIMIT 20000) v1
  JOIN T_WC_WIKIDATA_ITEM i2 ON i2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
) t
UNION ALL
SELECT 'item.DESCRIPTION en', COUNT(*), SUM(v1_rempli),
       SUM(v1_rempli AND v2_rempli), SUM(v1_rempli AND NOT v2_rempli)
FROM (
  SELECT (v1.DESCRIPTION IS NOT NULL AND v1.DESCRIPTION <> '') AS v1_rempli,
         (JSON_UNQUOTE(JSON_EXTRACT(i2.DESCRIPTIONS_JSON, '$.en')) IS NOT NULL) AS v2_rempli
  FROM (SELECT ID_WIKIDATA, DESCRIPTION FROM T_WC_WIKIDATA_ITEM_V1
        WHERE COALESCE(DELETED,0)=0 AND LANG='en' LIMIT 20000) v1
  JOIN T_WC_WIKIDATA_ITEM i2 ON i2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
) t;

SELECT '=== 7B . movie et person : titre / nom FR et EN cote V2 ===' AS section;
-- V2 ne stocke le titre que dans LABEL_EN + LABELS_JSON. On mesure la couverture
-- FR sur les entites communes : c'est ce que la bascule offrirait en plus (V1
-- n'a pas de titre FR sur MOVIE_V1, seulement TITLE).

SELECT 'movie' AS entite, COUNT(*) AS echantillon,
       SUM(label_en_rempli) AS label_en, SUM(label_fr_rempli) AS label_fr
FROM (
  SELECT (m.LABEL_EN IS NOT NULL AND m.LABEL_EN <> '') AS label_en_rempli,
         (JSON_UNQUOTE(JSON_EXTRACT(m.LABELS_JSON,'$.fr')) IS NOT NULL) AS label_fr_rempli
  FROM (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_MOVIE_V1 WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  JOIN T_WC_WIKIDATA_MOVIE m ON m.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
) t
UNION ALL
SELECT 'person', COUNT(*), SUM(label_en_rempli), SUM(label_fr_rempli)
FROM (
  SELECT (p.LABEL_EN IS NOT NULL AND p.LABEL_EN <> '') AS label_en_rempli,
         (JSON_UNQUOTE(JSON_EXTRACT(p.LABELS_JSON,'$.fr')) IS NOT NULL) AS label_fr_rempli
  FROM (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_PERSON_V1 WHERE COALESCE(DELETED,0)=0 LIMIT 20000) v1
  JOIN T_WC_WIKIDATA_PERSON p ON p.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
) t;


-- ############################################################################
-- ### 8 . ALIASES : la perte franche (WIKIDATA-CRAWLER-016)      [moyen]   ###
-- ############################################################################
-- L'ETL V2 extrait labels et descriptions, jamais doc["aliases"]. Aucune table
-- V2 ne porte de colonne ALIASES (la section 4 le confirme mecaniquement). Ici
-- on chiffre ce que la coupure de V1 ferait perdre au matching de noms.

SELECT '=== 8A . volume d aliases detenu par V1 ===' AS section;

SELECT 'movie' AS entite, COUNT(*) AS lignes_avec_aliases,
       SUM(LENGTH(ALIASES) - LENGTH(REPLACE(ALIASES,'|','')) + 1) AS aliases_estimes
FROM   T_WC_WIKIDATA_MOVIE_V1 WHERE COALESCE(DELETED,0)=0 AND ALIASES IS NOT NULL AND ALIASES <> ''
UNION ALL
SELECT 'serie', COUNT(*), SUM(LENGTH(ALIASES) - LENGTH(REPLACE(ALIASES,'|','')) + 1)
FROM   T_WC_WIKIDATA_SERIE_V1 WHERE COALESCE(DELETED,0)=0 AND ALIASES IS NOT NULL AND ALIASES <> ''
UNION ALL
SELECT 'person', COUNT(*), SUM(LENGTH(ALIASES) - LENGTH(REPLACE(ALIASES,'|','')) + 1)
FROM   T_WC_WIKIDATA_PERSON_V1 WHERE COALESCE(DELETED,0)=0 AND ALIASES IS NOT NULL AND ALIASES <> ''
UNION ALL
SELECT 'character', COUNT(*), SUM(LENGTH(ALIASES) - LENGTH(REPLACE(ALIASES,'|','')) + 1)
FROM   T_WC_WIKIDATA_CHARACTER_V1 WHERE COALESCE(DELETED,0)=0 AND ALIASES IS NOT NULL AND ALIASES <> ''
UNION ALL
SELECT CONCAT('item (lang=', LANG, ')'), COUNT(*), SUM(LENGTH(ALIASES) - LENGTH(REPLACE(ALIASES,'|','')) + 1)
FROM   T_WC_WIKIDATA_ITEM_V1 WHERE COALESCE(DELETED,0)=0 AND ALIASES IS NOT NULL AND ALIASES <> ''
GROUP BY LANG;


-- ############################################################################
-- ### 9 . CLES TMDb : la jointure de remplacement tient-elle ?   [moyen]   ###
-- ############################################################################
-- V1 porte ID_MOVIE / ID_SERIE / ID_PERSON en dur. Deux voies de remplacement :
--   (a) la propriete Wikidata (P4947 / P4983 / P4985), testee en section 6 ;
--   (b) la jointure inverse sur T_WC_TMDB_*.ID_WIKIDATA.
-- La voie (b) ne tient que si la table TMDb porte bien le QID en retour. Ce bloc
-- compte les liens V1 qui n'ont PAS de contrepartie cote TMDb : ce sont eux qui
-- disparaitraient vraiment.

SELECT '=== 9A . liens V1 sans reciproque dans les tables TMDb ===' AS section;

SELECT 'movie' AS entite,
       COUNT(*) AS liens_v1,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM T_WC_TMDB_MOVIE t
                             WHERE t.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                               AND t.ID_MOVIE = v1.ID_MOVIE) THEN 1 ELSE 0 END) AS lien_reciproque_ok,
       SUM(CASE WHEN NOT EXISTS (SELECT 1 FROM T_WC_TMDB_MOVIE t
                             WHERE t.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                               AND t.ID_MOVIE = v1.ID_MOVIE) THEN 1 ELSE 0 END) AS lien_perdu_si_v1_coupe
FROM   T_WC_WIKIDATA_MOVIE_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0 AND v1.ID_MOVIE IS NOT NULL AND v1.ID_MOVIE <> 0
UNION ALL
SELECT 'serie', COUNT(*),
       SUM(CASE WHEN EXISTS (SELECT 1 FROM T_WC_TMDB_SERIE t
                             WHERE t.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                               AND t.ID_SERIE = v1.ID_SERIE) THEN 1 ELSE 0 END),
       SUM(CASE WHEN NOT EXISTS (SELECT 1 FROM T_WC_TMDB_SERIE t
                             WHERE t.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                               AND t.ID_SERIE = v1.ID_SERIE) THEN 1 ELSE 0 END)
FROM   T_WC_WIKIDATA_SERIE_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0 AND v1.ID_SERIE IS NOT NULL AND v1.ID_SERIE <> 0
UNION ALL
SELECT 'person', COUNT(*),
       SUM(CASE WHEN EXISTS (SELECT 1 FROM T_WC_TMDB_PERSON t
                             WHERE t.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                               AND t.ID_PERSON = v1.ID_PERSON) THEN 1 ELSE 0 END),
       SUM(CASE WHEN NOT EXISTS (SELECT 1 FROM T_WC_TMDB_PERSON t
                             WHERE t.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                               AND t.ID_PERSON = v1.ID_PERSON) THEN 1 ELSE 0 END)
FROM   T_WC_WIKIDATA_PERSON_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0 AND v1.ID_PERSON IS NOT NULL AND v1.ID_PERSON <> 0;


-- ############################################################################
-- ### 10 . IMAGES : le nouveau toit couvre-t-il l'ancien ?       [moyen]   ###
-- ###      SECTION OPTIONNELLE : depend de MAIN_IMAGE_URL, colonne ajoutee ###
-- ###      par wikipedia-crawler (WIKIPEDIA-CRAWLER-020). Si elle n'existe ###
-- ###      pas encore sur cette base, ce bloc echoue seul, d'ou --force.   ###
-- ############################################################################
-- V1 porte WIKIPEDIA_IMAGE_PATH / _POSTER_PATH / _PROFILE_PATH. V2 n'en porte
-- aucune, et ne doit pas : une image de tete Wikipedia est une donnee Wikipedia.
-- Son toit est T_WC_WIKIPEDIA_PAGE_LANG.MAIN_IMAGE_URL, clef (ID_WIKIDATA, LANG).

SELECT '=== 10A . couverture des images V1 par T_WC_WIKIPEDIA_PAGE_LANG ===' AS section;

SELECT 'movie' AS entite,
       COUNT(*) AS v1_avec_image,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM T_WC_WIKIPEDIA_PAGE_LANG w
                             WHERE w.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                               AND w.MAIN_IMAGE_URL IS NOT NULL AND w.MAIN_IMAGE_URL <> '')
                THEN 1 ELSE 0 END) AS couvert_par_wikipedia_page_lang
FROM   T_WC_WIKIDATA_MOVIE_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0
  AND  v1.WIKIPEDIA_POSTER_PATH IS NOT NULL AND v1.WIKIPEDIA_POSTER_PATH <> ''
UNION ALL
SELECT 'person', COUNT(*),
       SUM(CASE WHEN EXISTS (SELECT 1 FROM T_WC_WIKIPEDIA_PAGE_LANG w
                             WHERE w.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                               AND w.MAIN_IMAGE_URL IS NOT NULL AND w.MAIN_IMAGE_URL <> '')
                THEN 1 ELSE 0 END)
FROM   T_WC_WIKIDATA_PERSON_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0
  AND  v1.WIKIPEDIA_PROFILE_PATH IS NOT NULL AND v1.WIKIPEDIA_PROFILE_PATH <> '';


SELECT '========== FIN ==========' AS section;
-- ============================================================================
-- POUR MEMOIRE, CE QUE CE FICHIER NE MESURE PAS
--   . les datatypes Wikidata que l'ETL V2 n'ingere pas (monolingualtext, url,
--     globe-coordinate, math...) : V1 ne les portait pas non plus, donc ce
--     n'est pas un ecart V1 -> V2, mais c'est un plafond de V2 a connaitre.
--   . la fraicheur : V1 est un historique accumule par crawl live, V2 un
--     instantane du dump. Un ecart peut venir de Wikidata, pas du pipeline.
--   . la qualite des valeurs : on teste la presence d'une propriete, pas
--     l'egalite des valeurs V1 et V2.
-- ============================================================================
