-- ============================================================================
-- Recette de WIKIDATA-CRAWLER-025 : les aliases arrivent en V2
-- ============================================================================
--
-- CE QUE LE TICKET DEMANDE, ET CE QUE CE FICHIER MESURE. Le backlog Nestor
-- (projets/t2s-backlog/repos/wikidata-crawler.md, WIKIDATA-CRAWLER-025) pose
-- cinq conditions d'acceptation. Quatre se lisent en base et sont ici :
--
--   F1  la colonne ALIASES_JSON existe sur les 7 tables cibles et les 7 tables
--       de staging, posee par apply_to_live_db.sql AU COURS DU RUN          (1)
--   F2  apres un run complet, PERSON, ITEM et MOVIE ont des aliases non vides (2)
--   F3  le volume est mesure : octets reels, langues et alias par entite     (4)
--   F4  sur 100 QID tires de PERSON_V1, V2 rend au moins ce que V1 detenait  (3)
--
-- La cinquieme (python tests/test_etl_smoke.py passe, cas alias compris) ne
-- touche pas la base et se joue dans le depot.
--
-- QUAND LE JOUER. F1 des la fin de l'etape 108 : c'est la que la colonne doit
-- exister, et si elle manque le chargement echoue sur une colonne inconnue apres
-- une passe qui vient de tourner un jour. F2 a F4 apres l'etape 110, la colonne
-- ne se remplissant qu'au chargement en cible.
--
-- LE POINT DE DEPART SE PREND AVANT LE RUN, ET SEULE F3b LE DONNE SANS RIEN
-- EXIGER. Le ticket demande la taille des sept tables AVANT et APRES. Avant, la
-- seule mesure qui ait un sens est celle des tables elles-memes (F3b, qui lit le
-- catalogue), puisque tout ce qui concerne la colonne vaut zero par construction.
-- Passe ce moment, il n'y a plus de "avant" a mesurer.
--
-- ATTENTION, F2, F3 et F4 NOMMENT ALIASES_JSON DIRECTEMENT : tant que la colonne
-- n'existe pas, elles echouent sur ERROR 1054 Unknown column, elles ne rendent pas
-- zero. C'est sans gravite avec --force, le fichier continue, mais il faut le
-- savoir. La colonne arrive au debut de l'etape 108, posee par apply_to_live_db.sql
-- que l'orchestrateur applique lui-meme. Pour la poser plus tot et jouer le fichier
-- entier des maintenant, passer apply_to_live_db.sql a la main : il est idempotent.
--
-- CE QUE MESURE F3, ET POURQUOI C'EST LA SEULE QUESTION OUVERTE. Un libelle par
-- langue, mais n alias par langue : personne n'a mesure ce que cela pese sur
-- 2,5 millions de lignes d'entites. Le chiffre qui decide s'il faut un filtre de
-- langues n'est pas le nombre d'alias, c'est le rapport MO_FR_EN / MO_TOTAL :
-- il dit exactement ce qu'un filtre EN+FR economiserait. Poser le filtre sur la
-- mesure, pas d'avance.
--
-- LECTURE SEULE. Executer avec --force -t.
--
-- DUREE. F2 et F3 comptent sept tables d'entites pour de vrai (COUNT(*), pas
-- information_schema.TABLE_ROWS qui est un estimateur : voir AGENTS.md, section
-- "Never conclude from information_schema") : quelques secondes chacune, ces
-- tables sont petites. F4 est borne a 100 entites et coute quelques secondes.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET SESSION max_statement_time = 0;


-- ############################################################################
-- F0 . DE QUEL RUN PARLE-T-ON ?
-- ############################################################################

SELECT '=== F0 . contexte du run ===' AS SECTION;

SELECT VAR_NAME, VAR_VALUE
FROM T_WC_SERVER_VARIABLE
WHERE VAR_NAME IN ('strwikidatacrawlerimportbatchid',
                   'strwikidatacrawlerstatus',
                   'strwikidatacrawlerdumpsize')
ORDER BY VAR_NAME;


-- ############################################################################
-- F1 . ACCEPTATION 1 : la colonne existe, des deux cotes
-- ############################################################################
--
-- Usage legitime d'information_schema : on lui demande une EXISTENCE, pas un
-- compte. C'est sur les comptes qu'il ment (TABLE_ROWS est un estimateur).
--
-- 14 lignes attendues : 7 cibles + 7 staging. Moins que 14 veut dire que
-- apply_to_live_db.sql n'a pas tourne, ou qu'une table a ete creee a la main
-- depuis une version anterieure du schema. Ne jamais rattraper avec un ALTER
-- ecrit pour la table manquante : rejouer apply_to_live_db.sql en entier, ou
-- l'etape 108 qui l'applique. Un seul fichier decrit ce que la base doit porter,
-- et une colonne posee a cote de lui est une divergence que personne ne relira.

SELECT '=== F1 . presence de la colonne (14 attendues) ===' AS SECTION;

SELECT
    COUNT(*)                                     AS TABLES_AVEC_ALIASES_JSON,
    SUM(TABLE_NAME LIKE 'STG\_%')                AS DONT_STAGING,
    CASE WHEN COUNT(*) = 14 THEN 'OK'
         ELSE 'MANQUE : relancer l etape 108 (apply_to_live_db.sql)' END AS VERDICT
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND COLUMN_NAME  = 'ALIASES_JSON';

SELECT TABLE_NAME, COLUMN_TYPE, IS_NULLABLE, ORDINAL_POSITION
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND COLUMN_NAME  = 'ALIASES_JSON'
ORDER BY TABLE_NAME;


-- ############################################################################
-- F2 . ACCEPTATION 2 : la colonne est remplie
-- ############################################################################
--
-- Le ticket nomme PERSON, ITEM et MOVIE ; les sept sont la, parce que SEASON,
-- EPISODE et CHARACTER n'ont JAMAIS eu d'aliases (leurs tables V1 n'en portent
-- pas), donc c'est la seule lecture qui dise ce qu'ils y gagnent.
--
-- Un zero sur une ligne ne veut pas dire la meme chose partout : sur MOVIE il
-- signale un defaut, sur CHARACTER il peut simplement vouloir dire que Wikidata
-- ne leur en donne pas. Comparer a la colonne des libelles, juste a cote.

SELECT '=== F2 . remplissage par table cible ===' AS SECTION;

SELECT 'T_WC_WIKIDATA_MOVIE' AS TABLE_CIBLE, COUNT(*) AS LIGNES,
       SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}') AS AVEC_ALIASES,
       ROUND(100 * SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}') / NULLIF(COUNT(*), 0), 1) AS PCT_ALIASES,
       ROUND(100 * SUM(LABELS_JSON  IS NOT NULL AND LABELS_JSON  <> '{}') / NULLIF(COUNT(*), 0), 1) AS PCT_LIBELLES
FROM T_WC_WIKIDATA_MOVIE
UNION ALL
SELECT 'T_WC_WIKIDATA_SERIE', COUNT(*),
       SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'),
       ROUND(100 * SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}') / NULLIF(COUNT(*), 0), 1),
       ROUND(100 * SUM(LABELS_JSON  IS NOT NULL AND LABELS_JSON  <> '{}') / NULLIF(COUNT(*), 0), 1)
FROM T_WC_WIKIDATA_SERIE
UNION ALL
SELECT 'T_WC_WIKIDATA_PERSON', COUNT(*),
       SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'),
       ROUND(100 * SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}') / NULLIF(COUNT(*), 0), 1),
       ROUND(100 * SUM(LABELS_JSON  IS NOT NULL AND LABELS_JSON  <> '{}') / NULLIF(COUNT(*), 0), 1)
FROM T_WC_WIKIDATA_PERSON
UNION ALL
SELECT 'T_WC_WIKIDATA_ITEM', COUNT(*),
       SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'),
       ROUND(100 * SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}') / NULLIF(COUNT(*), 0), 1),
       ROUND(100 * SUM(LABELS_JSON  IS NOT NULL AND LABELS_JSON  <> '{}') / NULLIF(COUNT(*), 0), 1)
FROM T_WC_WIKIDATA_ITEM
UNION ALL
SELECT 'T_WC_WIKIDATA_SEASON', COUNT(*),
       SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'),
       ROUND(100 * SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}') / NULLIF(COUNT(*), 0), 1),
       ROUND(100 * SUM(LABELS_JSON  IS NOT NULL AND LABELS_JSON  <> '{}') / NULLIF(COUNT(*), 0), 1)
FROM T_WC_WIKIDATA_SEASON
UNION ALL
SELECT 'T_WC_WIKIDATA_EPISODE', COUNT(*),
       SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'),
       ROUND(100 * SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}') / NULLIF(COUNT(*), 0), 1),
       ROUND(100 * SUM(LABELS_JSON  IS NOT NULL AND LABELS_JSON  <> '{}') / NULLIF(COUNT(*), 0), 1)
FROM T_WC_WIKIDATA_EPISODE
UNION ALL
SELECT 'T_WC_WIKIDATA_CHARACTER', COUNT(*),
       SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'),
       ROUND(100 * SUM(ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}') / NULLIF(COUNT(*), 0), 1),
       ROUND(100 * SUM(LABELS_JSON  IS NOT NULL AND LABELS_JSON  <> '{}') / NULLIF(COUNT(*), 0), 1)
FROM T_WC_WIKIDATA_CHARACTER;


-- ############################################################################
-- F3 . ACCEPTATION 4 : ce que le blob pese, et ce qu'un filtre economiserait
-- ############################################################################
--
-- MO_TOTAL est exact : c'est la somme des longueurs de la colonne, pas une
-- estimation de catalogue. MO_FR_EN est la part que porteraient EN et FR seuls.
-- Le rapport des deux est LE chiffre de la decision : si MO_FR_EN vaut le
-- cinquieme de MO_TOTAL, un filtre de langues economise quatre cinquiemes du
-- poids, et c'est alors, seulement alors, qu'il vaut sa variable d'environnement.
--
-- Les moyennes portent sur les entites QUI ONT des aliases (F2 donne la part qui
-- en a), sinon les lignes vides tirent tout vers zero et la moyenne ne decrit
-- plus rien. LANGUES compte les cles du document, ALIAS_FR et ALIAS_EN comptent
-- les elements d'un tableau de langue.

SELECT '=== F3 . volume reel de la colonne ===' AS SECTION;

SELECT 'T_WC_WIKIDATA_MOVIE' AS TABLE_CIBLE,
       COUNT(*) AS LIGNES_AVEC_ALIASES,
       ROUND(SUM(LENGTH(ALIASES_JSON)) / 1048576, 1) AS MO_TOTAL,
       ROUND(SUM(LENGTH(COALESCE(JSON_EXTRACT(ALIASES_JSON, '$.fr'), ''))
               + LENGTH(COALESCE(JSON_EXTRACT(ALIASES_JSON, '$.en'), ''))) / 1048576, 1) AS MO_FR_EN,
       ROUND(AVG(JSON_LENGTH(ALIASES_JSON)), 2) AS LANGUES,
       ROUND(AVG(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.fr'), 0)), 2) AS ALIAS_FR,
       ROUND(AVG(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.en'), 0)), 2) AS ALIAS_EN
FROM T_WC_WIKIDATA_MOVIE WHERE ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'
UNION ALL
SELECT 'T_WC_WIKIDATA_SERIE', COUNT(*),
       ROUND(SUM(LENGTH(ALIASES_JSON)) / 1048576, 1),
       ROUND(SUM(LENGTH(COALESCE(JSON_EXTRACT(ALIASES_JSON, '$.fr'), ''))
               + LENGTH(COALESCE(JSON_EXTRACT(ALIASES_JSON, '$.en'), ''))) / 1048576, 1),
       ROUND(AVG(JSON_LENGTH(ALIASES_JSON)), 2),
       ROUND(AVG(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.fr'), 0)), 2),
       ROUND(AVG(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.en'), 0)), 2)
FROM T_WC_WIKIDATA_SERIE WHERE ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'
UNION ALL
SELECT 'T_WC_WIKIDATA_PERSON', COUNT(*),
       ROUND(SUM(LENGTH(ALIASES_JSON)) / 1048576, 1),
       ROUND(SUM(LENGTH(COALESCE(JSON_EXTRACT(ALIASES_JSON, '$.fr'), ''))
               + LENGTH(COALESCE(JSON_EXTRACT(ALIASES_JSON, '$.en'), ''))) / 1048576, 1),
       ROUND(AVG(JSON_LENGTH(ALIASES_JSON)), 2),
       ROUND(AVG(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.fr'), 0)), 2),
       ROUND(AVG(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.en'), 0)), 2)
FROM T_WC_WIKIDATA_PERSON WHERE ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'
UNION ALL
SELECT 'T_WC_WIKIDATA_ITEM', COUNT(*),
       ROUND(SUM(LENGTH(ALIASES_JSON)) / 1048576, 1),
       ROUND(SUM(LENGTH(COALESCE(JSON_EXTRACT(ALIASES_JSON, '$.fr'), ''))
               + LENGTH(COALESCE(JSON_EXTRACT(ALIASES_JSON, '$.en'), ''))) / 1048576, 1),
       ROUND(AVG(JSON_LENGTH(ALIASES_JSON)), 2),
       ROUND(AVG(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.fr'), 0)), 2),
       ROUND(AVG(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.en'), 0)), 2)
FROM T_WC_WIKIDATA_ITEM WHERE ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'
UNION ALL
SELECT 'T_WC_WIKIDATA_SEASON', COUNT(*),
       ROUND(SUM(LENGTH(ALIASES_JSON)) / 1048576, 1),
       ROUND(SUM(LENGTH(COALESCE(JSON_EXTRACT(ALIASES_JSON, '$.fr'), ''))
               + LENGTH(COALESCE(JSON_EXTRACT(ALIASES_JSON, '$.en'), ''))) / 1048576, 1),
       ROUND(AVG(JSON_LENGTH(ALIASES_JSON)), 2),
       ROUND(AVG(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.fr'), 0)), 2),
       ROUND(AVG(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.en'), 0)), 2)
FROM T_WC_WIKIDATA_SEASON WHERE ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'
UNION ALL
SELECT 'T_WC_WIKIDATA_EPISODE', COUNT(*),
       ROUND(SUM(LENGTH(ALIASES_JSON)) / 1048576, 1),
       ROUND(SUM(LENGTH(COALESCE(JSON_EXTRACT(ALIASES_JSON, '$.fr'), ''))
               + LENGTH(COALESCE(JSON_EXTRACT(ALIASES_JSON, '$.en'), ''))) / 1048576, 1),
       ROUND(AVG(JSON_LENGTH(ALIASES_JSON)), 2),
       ROUND(AVG(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.fr'), 0)), 2),
       ROUND(AVG(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.en'), 0)), 2)
FROM T_WC_WIKIDATA_EPISODE WHERE ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}'
UNION ALL
SELECT 'T_WC_WIKIDATA_CHARACTER', COUNT(*),
       ROUND(SUM(LENGTH(ALIASES_JSON)) / 1048576, 1),
       ROUND(SUM(LENGTH(COALESCE(JSON_EXTRACT(ALIASES_JSON, '$.fr'), ''))
               + LENGTH(COALESCE(JSON_EXTRACT(ALIASES_JSON, '$.en'), ''))) / 1048576, 1),
       ROUND(AVG(JSON_LENGTH(ALIASES_JSON)), 2),
       ROUND(AVG(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.fr'), 0)), 2),
       ROUND(AVG(COALESCE(JSON_LENGTH(ALIASES_JSON, '$.en'), 0)), 2)
FROM T_WC_WIKIDATA_CHARACTER WHERE ALIASES_JSON IS NOT NULL AND ALIASES_JSON <> '{}';

-- Taille des sept tables, INDICATIVE et sans chiffre de reference : c'est le
-- catalogue qui parle, et il estime. Elle sert a voir un ordre de grandeur avant
-- et apres, pas a conclure. Le chiffre exact de la colonne est au-dessus.

SELECT '=== F3b . taille des tables (INDICATIF, catalogue) ===' AS SECTION;

SELECT TABLE_NAME,
       ROUND((DATA_LENGTH + INDEX_LENGTH) / 1048576) AS MO_INDICATIF
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('T_WC_WIKIDATA_MOVIE', 'T_WC_WIKIDATA_SERIE', 'T_WC_WIKIDATA_PERSON',
                     'T_WC_WIKIDATA_ITEM', 'T_WC_WIKIDATA_SEASON', 'T_WC_WIKIDATA_EPISODE',
                     'T_WC_WIKIDATA_CHARACTER')
ORDER BY TABLE_NAME;


-- ############################################################################
-- F4 . ACCEPTATION 3 : V2 rend au moins ce que V1 detenait
-- ############################################################################
--
-- LA COMPARAISON SE FAIT SUR L'UNION EN+FR, pas langue par langue, et ce n'est
-- pas une commodite : V1 a FONDU les deux dans une seule chaine tubee pour les
-- films et les personnes (sparql-crawler.py:1060-1079 et 1138-1164), la langue
-- n'y est pas. Un alias francais de V1 peut donc legitimement se retrouver sous
-- '$.en' en V2, et l'inverse.
--
-- LA FORME DE V1 : '|alias1|alias2|', avec les tubes aux deux bouts, et '|' tout
-- court quand il n'y a rien. Le CTE recursif decoupe ; TRIM enleve les tubes de
-- bord avant de recoller un separateur final, pour ne dependre d'aucune des deux
-- formes. Un seul branchement d'ancrage et un seul de recursion, avec CAST sur
-- les deux colonnes de travail : MariaDB derive le type d'une colonne de CTE du
-- SEUL branchement non recursif, et sur trois branchements il ne garde que le
-- premier, en silence (voir AGENTS.md).
--
-- L'echantillon est tire dans l'ordre des ID_ROW, donc REPRODUCTIBLE : deux
-- executions comparent les memes entites. Il est joint a PERSON pour ne pas
-- melanger deux questions ; les personnes de V1 absentes de V2 sont un defaut de
-- couverture (WIKIDATA-CRAWLER-023), pas un defaut d'aliases, et la requete de
-- contexte juste en dessous les compte a part.
--
-- JSON_SEARCH traite '%' et '_' comme des jokers : un alias qui en contient peut
-- etre declare trouve a tort. C'est rare sur des noms de personnes, et cela va
-- dans le sens indulgent, donc lire MANQUANTS comme un plancher.

SELECT '=== F4 . contexte : couverture V1 vers V2 sur les personnes a alias ===' AS SECTION;

SELECT COUNT(*)                        AS PERSONNES_V1_AVEC_ALIASES,
       SUM(v2.ID_WIKIDATA IS NOT NULL) AS DONT_PRESENTES_EN_V2,
       SUM(v2.ID_WIKIDATA IS NULL)     AS DONT_ABSENTES_DE_V2
FROM T_WC_WIKIDATA_PERSON_V1 v1
LEFT JOIN T_WC_WIKIDATA_PERSON v2 ON v2.ID_WIKIDATA = v1.ID_WIKIDATA
WHERE v1.ALIASES IS NOT NULL AND v1.ALIASES NOT IN ('', '|');

SELECT '=== F4 . 100 QID : V2 rend-il les alias de V1 ? ===' AS SECTION;

WITH RECURSIVE echantillon AS (
    SELECT v1.ID_WIKIDATA, v1.ALIASES
    FROM T_WC_WIKIDATA_PERSON_V1 v1
    JOIN T_WC_WIKIDATA_PERSON v2 ON v2.ID_WIKIDATA = v1.ID_WIKIDATA
    WHERE v1.ALIASES IS NOT NULL AND v1.ALIASES NOT IN ('', '|')
    ORDER BY v1.ID_ROW
    LIMIT 100
),
jetons AS (
    SELECT ID_WIKIDATA,
           CAST(CONCAT(TRIM(BOTH '|' FROM ALIASES), '|') AS CHAR(8000)) AS reste,
           CAST('' AS CHAR(500)) AS alias_v1
    FROM echantillon
    UNION ALL
    SELECT ID_WIKIDATA,
           CAST(SUBSTRING(reste, LOCATE('|', reste) + 1) AS CHAR(8000)),
           CAST(SUBSTRING(reste, 1, LOCATE('|', reste) - 1) AS CHAR(500))
    FROM jetons
    WHERE LOCATE('|', reste) > 0
),
verdict AS (
    SELECT j.ID_WIKIDATA, j.alias_v1,
           CASE WHEN JSON_SEARCH(p.ALIASES_JSON, 'one', j.alias_v1, NULL, '$.fr') IS NOT NULL
                  OR JSON_SEARCH(p.ALIASES_JSON, 'one', j.alias_v1, NULL, '$.en') IS NOT NULL
                THEN 1 ELSE 0 END AS trouve
    FROM jetons j
    JOIN T_WC_WIKIDATA_PERSON p ON p.ID_WIKIDATA = j.ID_WIKIDATA
    WHERE j.alias_v1 <> ''
)
SELECT COUNT(DISTINCT ID_WIKIDATA) AS ENTITES_TESTEES,
       COUNT(*)                    AS ALIAS_V1_TESTES,
       SUM(trouve)                 AS RETROUVES_EN_V2,
       COUNT(*) - SUM(trouve)      AS MANQUANTS,
       ROUND(100 * SUM(trouve) / NULLIF(COUNT(*), 0), 1) AS PCT_RETROUVES
FROM verdict;

-- Le detail des manquants, a regarder a l'oeil : un alias perdu par Wikidata
-- depuis le passage du crawler SPARQL n'est pas un defaut de ce ticket, une
-- serie d'accents mal rendus en serait un.

SELECT '=== F4b . les alias de V1 que V2 ne rend pas (50 premiers) ===' AS SECTION;

WITH RECURSIVE echantillon AS (
    SELECT v1.ID_WIKIDATA, v1.ALIASES
    FROM T_WC_WIKIDATA_PERSON_V1 v1
    JOIN T_WC_WIKIDATA_PERSON v2 ON v2.ID_WIKIDATA = v1.ID_WIKIDATA
    WHERE v1.ALIASES IS NOT NULL AND v1.ALIASES NOT IN ('', '|')
    ORDER BY v1.ID_ROW
    LIMIT 100
),
jetons AS (
    SELECT ID_WIKIDATA,
           CAST(CONCAT(TRIM(BOTH '|' FROM ALIASES), '|') AS CHAR(8000)) AS reste,
           CAST('' AS CHAR(500)) AS alias_v1
    FROM echantillon
    UNION ALL
    SELECT ID_WIKIDATA,
           CAST(SUBSTRING(reste, LOCATE('|', reste) + 1) AS CHAR(8000)),
           CAST(SUBSTRING(reste, 1, LOCATE('|', reste) - 1) AS CHAR(500))
    FROM jetons
    WHERE LOCATE('|', reste) > 0
)
SELECT j.ID_WIKIDATA,
       p.LABEL_EN,
       j.alias_v1                                                     AS ALIAS_ABSENT_DE_V2,
       LEFT(COALESCE(JSON_EXTRACT(p.ALIASES_JSON, '$.fr'), '[]'), 120) AS V2_FR,
       LEFT(COALESCE(JSON_EXTRACT(p.ALIASES_JSON, '$.en'), '[]'), 120) AS V2_EN
FROM jetons j
JOIN T_WC_WIKIDATA_PERSON p ON p.ID_WIKIDATA = j.ID_WIKIDATA
WHERE j.alias_v1 <> ''
  AND JSON_SEARCH(p.ALIASES_JSON, 'one', j.alias_v1, NULL, '$.fr') IS NULL
  AND JSON_SEARCH(p.ALIASES_JSON, 'one', j.alias_v1, NULL, '$.en') IS NULL
ORDER BY j.ID_WIKIDATA, j.alias_v1
LIMIT 50;
