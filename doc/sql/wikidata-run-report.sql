-- ============================================================================
-- Compte rendu d'un run wikidata-crawler, et controle de non-regression
-- ============================================================================
--
-- A lancer apres CHAQUE run. Deux questions, dans cet ordre :
--
--   A . Comment le run s'est-il passe ? (statut, duree, etapes, volumes)
--   B . A-t-il PRESERVE les acquis du 2026-07-31, ou les a-t-il defaits ?
--
-- La partie B n'est pas de la paranoia. Le correctif des qualificatifs
-- (WIKIDATA-CRAWLER-019) vit dans le code de l'ETL : un run lance depuis une
-- image Docker construite AVANT ce correctif re-ecraserait les qualificatifs, et
-- rien dans les journaux ne le dirait. `wikidata-crawler.sh` reconstruit l'image
-- a chaque lancement, donc le cas normal est bon, mais il se verifie en trois
-- chiffres plutot qu'il ne se suppose.
--
-- Chaque controle porte sa valeur de reference, mesuree le 2026-07-31 sur le
-- batch wikidata_full_20260726_1300 apres reparation.
--
-- LECTURE SEULE. Executer avec --force -t.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET SESSION max_statement_time = 0;


-- ############################################################################
-- ### A . COMMENT LE RUN S'EST-IL PASSE ?                        [rapide]   ###
-- ############################################################################

SELECT '=== A1 . statut general du dernier run ===' AS section;
-- A lire en premier : status doit valoir SUCCESS, et lasterror doit dater d'un
-- run anterieur (comparer sa date a startdatetime).

SELECT VAR_NAME, VAR_VALUE, TIM_UPDATED
FROM   T_WC_SERVER_VARIABLE
WHERE  VAR_NAME LIKE 'strwikidatacrawler%'
  AND  VAR_NAME NOT LIKE '%step1%'
ORDER BY VAR_NAME;

SELECT '=== A2 . deroule etape par etape ===' AS section;
-- Piege connu : les etapes longues (102 pass1, 104 pass2, 106 item_cache)
-- restent affichees RUNNING alors qu'elles ont un finishedat et que leurs
-- validateurs (103, 105, 107) sont SUCCESS. C'est un defaut de comptabilite du
-- suivi, pas un run bloque. Se fier a finishedat et aux etapes de validation.

SELECT VAR_NAME, VAR_VALUE
FROM   T_WC_SERVER_VARIABLE
WHERE  VAR_NAME LIKE 'strwikidatacrawlerstep1%'
ORDER BY VAR_NAME;

SELECT '=== A3 . volumetrie, avec les reperes du 2026-07-31 ===' AS section;
-- TABLE_ROWS est une estimation InnoDB, pas un COUNT(*). Suffisant pour situer.

SELECT TABLE_NAME, TABLE_ROWS AS lignes_estimees,
       ROUND((DATA_LENGTH + INDEX_LENGTH)/1024/1024/1024, 2) AS taille_go,
       UPDATE_TIME AS derniere_ecriture,
       CASE TABLE_NAME
         WHEN 'T_WC_WIKIDATA_STATEMENT'           THEN '37 218 735 le 30/07'
         WHEN 'T_WC_WIKIDATA_STATEMENT_QUALIFIER' THEN '5 577 076 apres reparation'
         WHEN 'T_WC_WIKIDATA_MOVIE'               THEN '438 146'
         WHEN 'T_WC_WIKIDATA_SERIE'               THEN '356 481'
         WHEN 'T_WC_WIKIDATA_PERSON'              THEN '780 430'
         WHEN 'T_WC_WIKIDATA_ITEM'                THEN '629 439'
         WHEN 'T_WC_WIKIDATA_EPISODE'             THEN '188 721'
         ELSE NULL END AS repere_precedent
FROM   information_schema.TABLES
WHERE  TABLE_SCHEMA = DATABASE()
  AND  TABLE_NAME LIKE 'T_WC_WIKIDATA%'
ORDER BY TABLE_ROWS DESC;


-- ############################################################################
-- ### B . LES ACQUIS DU 2026-07-31 TIENNENT-ILS ?                [moyen]    ###
-- ############################################################################

SELECT '=== B1 . LE controle : les qualificatifs se sont-ils re-effondres ? ===' AS section;
-- Regle de lecture, la meme depuis le debut : si lignes = valeurs_distinctes,
-- la table stocke des valeurs et non des occurrences, et le correctif n'est PAS
-- dans l'image qui a tourne. Reperes du 31/07 apres reparation :
--   P453 112 279 / 42 572   |   P1686 70 386 / 30 811   |   P155 41 513 / 34 102
-- Avant reparation, les trois etaient rigoureusement egales.

SELECT q.ID_QUALIFIER_PROPERTY,
       COUNT(*)                   AS lignes,
       COUNT(DISTINCT qi.ID_ITEM) AS valeurs_distinctes,
       CASE WHEN COUNT(*) = COUNT(DISTINCT qi.ID_ITEM)
            THEN 'ALERTE : effondrement, le correctif -019 est absent'
            ELSE 'ok, une ligne par occurrence' END AS verdict
FROM   T_WC_WIKIDATA_STATEMENT_QUALIFIER q
JOIN   T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qi ON qi.ID_STATEMENT_QUALIFIER = q.ID_STATEMENT_QUALIFIER
WHERE  q.ID_QUALIFIER_PROPERTY IN ('P453','P1686','P155')
GROUP BY q.ID_QUALIFIER_PROPERTY;

SELECT '=== B2 . les awards ont-ils garde leur annee ? ===' AS section;
-- Repere : 64,8 % le 31/07 (contre 2,4 % avant reparation). Un effondrement de
-- ce taux est le symptome le plus lisible d'une regression.

SELECT COUNT(*) AS statements_p166,
       SUM(a_p585) AS avec_annee,
       ROUND(100 * SUM(a_p585) / NULLIF(COUNT(*),0), 1) AS pct_annee,
       '64,8 % le 31/07' AS repere
FROM ( SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER q
                      WHERE q.ID_STATEMENT = st.ID_STATEMENT
                        AND q.ID_QUALIFIER_PROPERTY = 'P585') AS a_p585
       FROM T_WC_WIKIDATA_STATEMENT st WHERE st.ID_PROPERTY = 'P166' ) t;

SELECT '=== B3 . les numeros d episode sont-ils toujours la ? ===' AS section;
-- Repere : 92 % le 31/07 (contre 1,1 % avant reparation).

SELECT COUNT(*) AS statements_p179_p4908,
       SUM(a_p1545) AS avec_numero,
       ROUND(100 * SUM(a_p1545) / NULLIF(COUNT(*),0), 1) AS pct,
       '92 % le 31/07' AS repere
FROM ( SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER q
                      WHERE q.ID_STATEMENT = st.ID_STATEMENT
                        AND q.ID_QUALIFIER_PROPERTY = 'P1545') AS a_p1545
       FROM T_WC_WIKIDATA_STATEMENT st WHERE st.ID_PROPERTY IN ('P179','P4908') ) t;

SELECT '=== B4 . le second defaut de -019 est-il comble ? ===' AS section;
-- Les items qui n'apparaissent QUE comme valeur de qualificatif n'entraient pas
-- dans le cache de libelles : 26 924 items concernes, dont les ceremonies. Le
-- correctif est dans le code depuis le 30/07 mais demande pass2 ET item_cache.
-- Temoin : Q85314819, la 96e ceremonie des Oscars, qui s'affichait en identifiant
-- brut dans les requetes de prix. Si ce run a rejoue item_cache avec le
-- correctif, elle a maintenant un libelle.

SELECT 'Q85314819 (96th Academy Awards)' AS temoin,
       (SELECT LABEL_EN FROM T_WC_WIKIDATA_ITEM WHERE ID_WIKIDATA='Q85314819') AS libelle,
       CASE WHEN (SELECT COUNT(*) FROM T_WC_WIKIDATA_ITEM WHERE ID_WIKIDATA='Q85314819') = 0
            THEN 'toujours absente : item_cache n a pas rejoue avec le correctif'
            ELSE 'presente : le second defaut est comble' END AS verdict;

SELECT '=== B5 . et la hierarchie (-020) ? ===' AS section;
-- Attendu tant que -020 n'est pas livre : zero statement. Les categories de prix
-- restent des coquilles, donc « combien d'Oscars » reste sans reponse. Si ce
-- compte devient non nul, -020 est livre et Q2/Q3 des requetes de prix marchent.

SELECT 'Q103618 (Academy Award for Best Actress)' AS temoin,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT WHERE ID_WIKIDATA='Q103618') AS nb_statements,
       CASE WHEN (SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT WHERE ID_WIKIDATA='Q103618') = 0
            THEN 'attendu : -020 n est pas livre, pas de question hierarchique'
            ELSE 'nouveau : -020 est livre, relancer Q2/Q3 des requetes de prix' END AS verdict;

SELECT '========== FIN ==========' AS section;
