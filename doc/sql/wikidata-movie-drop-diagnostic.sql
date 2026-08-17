-- ============================================================================
-- Pourquoi un film sur cinq a-t-il disparu ? (batch wikidata_full_20260807_1043)
-- ============================================================================
--
-- /!\ AMENDEMENT 2026-08-16, A LIRE AVANT TOUT LE RESTE : LA PREMISSE EST TOMBEE.
--
-- Aucun film n'avait disparu. Le constat ci-dessous reposait sur A3, qui lisait
-- information_schema.TABLE_ROWS, une ESTIMATION, et la comparait a des reperes.
-- Les comptages exacts du 2026-08-16 donnent, en face des reperes du 31/07 :
--
--   MOVIE    438 956  contre  438 146   ->  +0,18 %
--   SERIE    357 683  contre  356 481   ->  +0,34 %
--   PERSON   783 141  contre  780 430   ->  +0,35 %
--   ITEM     702 502  contre  629 439   -> +11,6 %  (effet du correctif B4)
--   EPISODE  187 463  contre  188 721   ->  -0,67 % (bruit)
--
-- Tout monte, comme il se doit sur un dump plus recent. L'estimation sous-evaluait
-- MOVIE de 22,45 %, soit trait pour trait l'ampleur de la « disparition ».
--
-- CE QUI RESTE VALABLE ICI. D2 et D3-bis comparent utilement V1 et V2 : 1332 films
-- de V1 sont absents de toute la base V2, et la verification directe chez Wikidata
-- (six identifiants testes le 16/08) montre que quatre sur six ont ete SUPPRIMES de
-- Wikidata, fiches de films annonces sans source. Le dump ne peut pas les contenir,
-- V1 les garde parce que le crawler SPARQL n'efface jamais. Ce sont des fantomes
-- dans V1, pas des pertes dans V2.
--
-- D0, D1 ET D3 SONT REPARES DEPUIS LE 2026-08-17. Ils ne valaient rien tant que le
-- graphe des sous-classes restait sur le disque : D0 rendait 3202 aretes la ou le
-- graphe reel en compte des millions, D1 ne trouvait que 9 classes, et la colonne
-- « diagnostic » de D3 etait un artefact dont il ne fallait rien conclure.
--
-- WIKIDATA-CRAWLER-020 voie (b) a charge le graphe complet dans
-- T_WC_WIKIDATA_SUBCLASS, 5 227 784 aretes. Les trois blocs s'appuient desormais
-- sur cette table, c'est-a-dire sur le meme graphe que celui dont l'ETL derive ses
-- pools, et le verdict « classe HORS pool » de D3 redevient interpretable.
--
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
-- REECRIT LE 2026-08-17. La premiere version comptait les statements P279 de
-- T_WC_WIKIDATA_STATEMENT et rendait 3202, la ou le graphe reel en compte des
-- millions : seules y figuraient les aretes des entites retenues. D1 et D3 ne
-- mesuraient donc qu'un reflet appauvri, et leur verdict etait un artefact.
--
-- Depuis WIKIDATA-CRAWLER-020 voie (b), le graphe complet est charge dans sa propre
-- table depuis /shared/pass1/subclass_edges.jsonl : 5 227 784 aretes distinctes pour
-- le batch wikidata_full_20260807_1043. C'est desormais le meme graphe que celui que
-- l'ETL construit en memoire pour calculer ses pools, donc D1 et D3 mesurent enfin
-- ce qu'ils pretendent mesurer.

SELECT COUNT(*) AS aretes_p279_en_base,
       '5 227 784 pour le batch du 07/08' AS repere_20260816
FROM   T_WC_WIKIDATA_SUBCLASS
WHERE  DELETED = 0;


SELECT '=== D1 . taille du pool « film » reconstitue depuis la base ===' AS section;
-- Fermeture transitive descendante depuis les deux racines. UNION (et non UNION
-- ALL) dedoublonne, ce qui absorbe au passage les cycles de P279, qui existent
-- dans Wikidata. Repere : aucun, c'est la premiere fois qu'on le mesure. Le chiffre
-- vaut surtout pour etre compare a celui du PROCHAIN run.

-- DEUX PIEGES DU CTE RECURSIF SOUS MARIADB, rencontres l'un apres l'autre. Le
-- second est le plus dangereux : il ne dit rien.
--
-- 1. ERROR 1406 « Data too long for column 'qid' » (2026-08-16). MariaDB fixe le
--    type de la colonne d'apres la partie NON recursive uniquement. Une ancre
--    ecrite « SELECT 'Q11424' » type qid sur 7 caracteres, puis la recursion y
--    injecte des ID_WIKIDATA plus longs et le serveur refuse. D'ou le CAST en
--    CHAR(50), aligne sur la colonne source. Le COLLATE explicite est pose sur
--    cette valeur produite par fonction, jamais sur la colonne indexee, pour
--    eviter ERROR 1267 sans perdre l'index (cf. AGENTS.md).
--
-- 2. ANCRE SILENCIEUSEMENT AMPUTEE (2026-08-17). Ecrite en TROIS branches
--    (racine UNION racine UNION recursion), la requete a rendu 1, alors que
--    l'ancre a elle seule porte deux racines et que Q11424 compte 166
--    sous-classes directes en base. MariaDB ne retient que la premiere branche
--    comme partie non recursive, et la descente ne part jamais. Aucun message,
--    aucune erreur : un chiffre faux qu'on peut lire avec confiance. Forme
--    canonique retenue : les racines sont regroupees dans une sous-requete, donc
--    UNE branche d'ancrage et UNE branche recursive, ce que toutes les
--    implementations traitent pareil. Le controle « plancher 167 » ci-dessous
--    existe pour que ce mode de defaillance ne puisse plus passer inapercu.

WITH RECURSIVE pool_film (qid) AS (
    SELECT CAST(r.qid AS CHAR(50)) COLLATE utf8mb4_unicode_ci AS qid
    FROM   (SELECT 'Q11424' AS qid          -- film
            UNION ALL SELECT 'Q506240') AS r -- television film
    UNION
    SELECT sc.ID_CHILD
    FROM   T_WC_WIKIDATA_SUBCLASS sc
    JOIN   pool_film              p ON p.qid = sc.ID_PARENT
    WHERE  sc.DELETED = 0
)
SELECT COUNT(*) AS classes_dans_le_pool_film,
       '842 le 2026-08-17, batch wikidata_full_20260807_1043'           AS repere,
       'plancher 167 : 2 racines + 166 sous-classes directes de Q11424' AS garde_fou
FROM   pool_film;


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

WITH RECURSIVE pool_film (qid) AS (   -- voir les deux pieges notes en D1
    SELECT CAST(r.qid AS CHAR(50)) COLLATE utf8mb4_unicode_ci AS qid
    FROM   (SELECT 'Q11424' AS qid UNION ALL SELECT 'Q506240') AS r
    UNION
    SELECT sc.ID_CHILD
    FROM   T_WC_WIKIDATA_SUBCLASS sc
    JOIN   pool_film              p ON p.qid = sc.ID_PARENT
    WHERE  sc.DELETED = 0
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
