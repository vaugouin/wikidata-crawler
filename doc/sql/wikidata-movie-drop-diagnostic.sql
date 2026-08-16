-- ============================================================================
-- Pourquoi un film sur cinq a-t-il disparu ? (batch wikidata_full_20260807_1043)
-- ============================================================================
--
-- CONSTAT qui motive ce fichier. Le run du 2026-08-09 au 2026-08-14 s'est termine
-- en SUCCESS, d'un seul tenant, avec tous les controles de non-regression au vert
-- (voir wikidata-run-report.sql). Mais la volumetrie a baisse sur un dump PLUS
-- RECENT, ce qui est contre-intuitif :
--
--   MOVIE      340 401  contre  438 146 le 31/07   ->  -22,3 %
--   PERSON     732 600  contre  780 430            ->   -6,1 %
--   SERIE      344 167  contre  356 481            ->   -3,5 %
--   STATEMENT  34,76 M  contre  37,22 M            ->   -6,6 %
--   ITEM       654 935  contre  629 439            ->   +4,1 %  (seule hausse)
--
-- Le journal git disculpe le code : entre le lancement du run precedent (26/07) et
-- celui-ci (09/08), les seuls commits touchant l'ETL sont les deux du correctif
-- -019 sur les qualificatifs et un sur la detection de dump. MOVIE_ROOTS vaut
-- toujours {Q11424, Q506240}.
--
-- HYPOTHESE A TESTER. Le code ne classe pas sur MOVIE_ROOTS mais sur
-- descendants_of_roots(MOVIE_ROOTS) (wikidata_dump_etl.py:1135), c'est-a-dire sur
-- la fermeture transitive du graphe P279 CONSTRUIT A PARTIR DU DUMP, en memoire, a
-- chaque run. Si ce graphe a change entre les deux dumps, des milliers de films
-- sortent du pool sans qu'une ligne de code ait bouge. La categorie « film » n'est
-- pas definie ici : elle est definie par ce que Wikidata declare sous-classe de
-- film, et elle bouge d'un dump a l'autre.
--
-- CE QUE CHAQUE BLOC TRANCHE.
--   D0 . le graphe P279 est-il seulement present en base ? (conditionne D1)
--   D1 . quelle est la taille du pool « film » que cette base connait ?
--   D2 . les films perdus sont-ils PERDUS ou RECLASSES ailleurs ?
--   D3 . la reponse : leur classe P31 est-elle dans le pool, ou en dehors ?
--
-- REFERENTIEL. Il n'existe aucun instantane du 31/07 : les tables ont ete
-- reecrites et le step 114 purge les anciens lots. Le seul referentiel stable est
-- T_WC_WIKIDATA_MOVIE_V1 (354 568 lignes), produit par l'ancien crawler SPARQL et
-- jamais reecrit par l'ETL V2. Il porte un INSTANCE_OF, ce qui permet de ventiler
-- les disparus par classe Wikidata. Ce n'est pas la photo du 31/07, mais c'est un
-- temoin independant, ce qui vaut mieux.
--
-- LECTURE SEULE. Executer avec --force -t. D1 demande MariaDB 10.2 ou superieur
-- (CTE recursif).
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET SESSION max_statement_time = 0;


SELECT '=== D0 . le graphe des sous-classes est-il en base ? ===' AS section;
-- Le graphe est bati en memoire pendant pass1, il n'est pas persiste comme graphe.
-- Ne subsistent en base que les statements P279 des entites retenues. Si ce compte
-- est nul ou derisoire, D1 mesurera le reflet appauvri du graphe reel, et non le
-- graphe qui a servi a classer : le dire franchement plutot que de lire un chiffre
-- faux avec confiance.

SELECT COUNT(*) AS statements_p279_en_base
FROM   T_WC_WIKIDATA_STATEMENT
WHERE  ID_PROPERTY = 'P279';


SELECT '=== D1 . taille du pool « film » reconstitue depuis la base ===' AS section;
-- Fermeture transitive descendante depuis les deux racines. UNION (et non UNION
-- ALL) dedoublonne, ce qui absorbe au passage les cycles de P279, qui existent
-- dans Wikidata. Repere : aucun, c'est la premiere fois qu'on le mesure. Le chiffre
-- vaut surtout pour etre compare a celui du PROCHAIN run.

WITH RECURSIVE pool_film (qid) AS (
    SELECT 'Q11424'  AS qid          -- film
    UNION
    SELECT 'Q506240'                 -- television film
    UNION
    SELECT st.ID_WIKIDATA
    FROM   T_WC_WIKIDATA_STATEMENT  st
    JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
    JOIN   pool_film                p  ON p.qid = iv.ID_ITEM
    WHERE  st.ID_PROPERTY = 'P279'
)
SELECT COUNT(*) AS classes_dans_le_pool_film FROM pool_film;


SELECT '=== D2 . les films V1 absents de MOVIE : perdus, ou reclasses ? ===' AS section;
-- Chaque film actif de V1 est cherche dans TOUTES les tables d'entite V2, dans
-- l'ordre. La ligne qui compte est la derniere : un film introuvable partout est
-- sorti du perimetre, alors qu'un film retrouve dans ITEM ou SERIE a seulement
-- change de tiroir. ITEM ayant gagne 25 496 lignes pendant que MOVIE en perdait
-- 97 745, les deux mouvements sont peut-etre le meme.

SELECT CASE
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA) THEN '1 . toujours dans MOVIE'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA) THEN '2 . bascule dans SERIE'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM      x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA) THEN '3 . bascule dans ITEM'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE   x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA) THEN '4 . bascule dans EPISODE'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA) THEN '5 . bascule dans SEASON'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA) THEN '6 . bascule dans CHARACTER'
         ELSE                                                                                            '7 . INTROUVABLE dans toute la base V2'
       END      AS destination,
       COUNT(*) AS films_v1
FROM   T_WC_WIKIDATA_MOVIE_V1 v1
WHERE  COALESCE(v1.DELETED,0) = 0
GROUP  BY destination
ORDER  BY destination;


SELECT '=== D3 . LA question : la classe des disparus est-elle dans le pool ? ===' AS section;
-- Le croisement decisif. Pour chaque classe P31 portee par les films V1 devenus
-- introuvables, on demande si cette classe appartient encore au pool « film ».
--
--   « classe HORS pool » en tete  -> le graphe P279 a change entre les deux dumps.
--                                    La cause est en amont, chez Wikidata, pas ici.
--   « classe DANS le pool » en tete -> la classe est toujours reconnue mais
--                                    l'entite n'a pas ete emise : la cause est
--                                    dans l'ETL, et c'est plus grave.

WITH RECURSIVE pool_film (qid) AS (
    SELECT 'Q11424'  AS qid
    UNION
    SELECT 'Q506240'
    UNION
    SELECT st.ID_WIKIDATA
    FROM   T_WC_WIKIDATA_STATEMENT  st
    JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
    JOIN   pool_film                p  ON p.qid = iv.ID_ITEM
    WHERE  st.ID_PROPERTY = 'P279'
)
SELECT v1.INSTANCE_OF                                AS classe_p31,
       COUNT(*)                                      AS films_disparus,
       CASE WHEN v1.INSTANCE_OF IN (SELECT qid FROM pool_film)
            THEN 'classe DANS le pool  -> defaut d emission (ETL)'
            ELSE 'classe HORS pool     -> le graphe P279 a change'
       END                                           AS diagnostic
FROM   T_WC_WIKIDATA_MOVIE_V1 v1
WHERE  COALESCE(v1.DELETED,0) = 0
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM      x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE   x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA)
GROUP  BY v1.INSTANCE_OF
ORDER  BY films_disparus DESC
LIMIT  25;


SELECT '=== D3-bis . vingt disparus nommes, pour aller les regarder ===' AS section;
-- Un chiffre ne se verifie qu'en ouvrant quelques cas. Coller un ID_WIKIDATA dans
-- https://www.wikidata.org/wiki/<ID> montre ce que l'entite declare aujourd'hui.

SELECT v1.ID_WIKIDATA, v1.TITLE, v1.DAT_RELEASE, v1.INSTANCE_OF
FROM   T_WC_WIKIDATA_MOVIE_V1 v1
WHERE  COALESCE(v1.DELETED,0) = 0
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM  x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA)
  AND  v1.TITLE IS NOT NULL
ORDER  BY v1.DAT_RELEASE DESC
LIMIT  20;

SELECT '========== FIN ==========' AS section;
