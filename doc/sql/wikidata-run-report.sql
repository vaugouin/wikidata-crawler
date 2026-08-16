-- ============================================================================
-- Compte rendu d'un run wikidata-crawler, et controle de non-regression
-- ============================================================================
--
-- A lancer apres CHAQUE run. Deux questions, dans cet ordre :
--
--   A . Comment le run s'est-il passe ? (statut, duree, etapes, volumes)
--   B . A-t-il PRESERVE les acquis du 2026-07-31, ou les a-t-il defaits ?
--   C . Le run est-il HOMOGENE, ou deux versions du code l'ont-elles produit ?
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
-- AMENDEMENT 2026-08-16. Le bloc A3 lisait information_schema.TABLE_ROWS et
-- comparait cette ESTIMATION a des reperes chiffres. Applique au run du 09/08 au
-- 14/08, il a annonce une chute de 22 % de T_WC_WIKIDATA_MOVIE qui n'a jamais eu
-- lieu : 340 401 lignes estimees contre 438 956 comptees. Toutes les entites
-- etaient en legere HAUSSE, comme il se doit sur un dump plus recent. A3 est
-- desormais scinde en A3a, qui compte reellement les tables d'entite, et A3b,
-- explicitement indicatif. Regle qui en decoule, portee dans AGENTS.md :
-- information_schema sert a explorer, jamais a conclure.
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

SELECT '=== A3a . volumetrie EXACTE des tables d entite ===' AS section;
-- REECRIT LE 2026-08-16, APRES UNE FAUSSE ALERTE. La version precedente lisait
-- information_schema.TABLE_ROWS et comparait cette ESTIMATION a des reperes. Sur
-- le run du 09/08 au 14/08, elle a sous-evalue T_WC_WIKIDATA_MOVIE de 22 %
-- (340 401 estime contre 438 956 reel) et fait conclure a la disparition d un
-- film sur cinq. Rien n avait disparu. Les tables d entite sont petites : on les
-- compte pour de vrai, cela coute quelques secondes et cela ne ment pas.
--
-- Reperes : comptages EXACTS du 2026-08-16, batch wikidata_full_20260807_1043.
-- Ecart attendu d un run a l autre : quelques dixiemes de pour cent a la hausse,
-- le dump grossissant. Une BAISSE franche est le signal a instruire.

SELECT            'T_WC_WIKIDATA_MOVIE'     AS table_entite, COUNT(*) AS lignes, '438 956'         AS repere_20260816 FROM T_WC_WIKIDATA_MOVIE
UNION ALL SELECT  'T_WC_WIKIDATA_SERIE',                     COUNT(*),           '357 683'                            FROM T_WC_WIKIDATA_SERIE
UNION ALL SELECT  'T_WC_WIKIDATA_PERSON',                    COUNT(*),           '783 141'                            FROM T_WC_WIKIDATA_PERSON
UNION ALL SELECT  'T_WC_WIKIDATA_ITEM',                      COUNT(*),           '702 502'                            FROM T_WC_WIKIDATA_ITEM
UNION ALL SELECT  'T_WC_WIKIDATA_EPISODE',                   COUNT(*),           '187 463'                            FROM T_WC_WIKIDATA_EPISODE
UNION ALL SELECT  'T_WC_WIKIDATA_SEASON',                    COUNT(*),           '(jamais compte)'                    FROM T_WC_WIKIDATA_SEASON
UNION ALL SELECT  'T_WC_WIKIDATA_CHARACTER',                 COUNT(*),           '(jamais compte)'                    FROM T_WC_WIKIDATA_CHARACTER;


SELECT '=== A3b . taille disque et grosses tables (INDICATIF, ne rien conclure) ===' AS section;
-- Ce bloc sert a voir l occupation disque et l ordre de grandeur des deux grosses
-- tables, qu on ne compte pas ici parce qu un COUNT(*) sur 35 millions de lignes
-- coute plusieurs minutes. Trois pieges d information_schema, tous rencontres le
-- 2026-08-16, justifient qu aucune decision ne sorte de ce tableau :
--   1. TABLE_ROWS est une estimation statistique, qui a devie de 22 % sur MOVIE.
--   2. Une estimation PERIMEE se lit comme une table figee : EPISODE affichait
--      exactement son ancienne valeur (188 721) alors qu il en comptait 187 463.
--      ANALYZE TABLE rafraichit l estimation quand on en a besoin.
--   3. UPDATE_TIME est en UTC, alors que les colonnes TIM_UPDATED sont en heure
--      locale : deux heures d ecart en ete, verifiees sur MOVIE_V1 et PERSON_V1.
-- Ordres de grandeur au 2026-08-16 : STATEMENT ~34,8 M, STATEMENT_QUALIFIER ~5,58 M.

SELECT TABLE_NAME, TABLE_ROWS AS lignes_ESTIMEES,
       ROUND((DATA_LENGTH + INDEX_LENGTH)/1024/1024/1024, 2) AS taille_go,
       UPDATE_TIME AS derniere_ecriture_UTC
FROM   information_schema.TABLES
WHERE  TABLE_SCHEMA = DATABASE()
  AND  TABLE_NAME LIKE 'T_WC_WIKIDATA%'
ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC;


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

SELECT '=== B4 . combien d items ne sont connus que par les qualificatifs ? ===' AS section;
-- Les items qui n'apparaissent QUE comme valeur de qualificatif n'entraient pas
-- dans le cache de libelles : 26 924 items le 31/07. Le correctif est dans le code
-- depuis le 30/07 et demande pass2 ET item_cache.
--
-- CE CONTROLE A ETE REECRIT LE 2026-08-07. Sa premiere version prenait Q85314819,
-- la 96e ceremonie des Oscars, comme temoin, et la cherchait dans le cache d'items.
-- Elle a rendu un faux negatif : la ceremonie est en fait dans T_WC_WIKIDATA_SERIE,
-- entite de plein droit, parce que la reintegration de Q15416 « television program »
-- dans SERIES_ROOTS a fait entrer les ceremonies retransmises. Un temoin nomme est
-- fragile : on mesure desormais la population entiere, sans presupposer ou vit un
-- item donne.

SELECT COUNT(*) AS items_connus_seulement_par_qualificatif,
       '26 924 le 31/07, doit tendre vers 0' AS repere
FROM ( SELECT DISTINCT qv.ID_ITEM
       FROM   T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qv
       WHERE  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM      x WHERE x.ID_WIKIDATA = qv.ID_ITEM)
         AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE     x WHERE x.ID_WIKIDATA = qv.ID_ITEM)
         AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE     x WHERE x.ID_WIKIDATA = qv.ID_ITEM)
         AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON    x WHERE x.ID_WIKIDATA = qv.ID_ITEM)
         AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER x WHERE x.ID_WIKIDATA = qv.ID_ITEM)
         AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON    x WHERE x.ID_WIKIDATA = qv.ID_ITEM)
         AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE   x WHERE x.ID_WIKIDATA = qv.ID_ITEM) ) t;

SELECT '=== B4-bis . le temoin, cherche dans TOUTES les tables d entite ===' AS section;

SELECT 'movie' AS table_v2, LABEL_EN FROM T_WC_WIKIDATA_MOVIE     WHERE ID_WIKIDATA='Q85314819'
UNION ALL SELECT 'serie',   LABEL_EN FROM T_WC_WIKIDATA_SERIE     WHERE ID_WIKIDATA='Q85314819'
UNION ALL SELECT 'item',    LABEL_EN FROM T_WC_WIKIDATA_ITEM      WHERE ID_WIKIDATA='Q85314819'
UNION ALL SELECT 'person',  LABEL_EN FROM T_WC_WIKIDATA_PERSON    WHERE ID_WIKIDATA='Q85314819'
UNION ALL SELECT 'character',LABEL_EN FROM T_WC_WIKIDATA_CHARACTER WHERE ID_WIKIDATA='Q85314819'
UNION ALL SELECT 'season',  LABEL_EN FROM T_WC_WIKIDATA_SEASON    WHERE ID_WIKIDATA='Q85314819'
UNION ALL SELECT 'episode', LABEL_EN FROM T_WC_WIKIDATA_EPISODE   WHERE ID_WIKIDATA='Q85314819';

SELECT '=== B5 . et la hierarchie (-020) ? ===' AS section;
-- Attendu tant que -020 n'est pas livre : zero statement. Les categories de prix
-- restent des coquilles, donc « combien d'Oscars » reste sans reponse. Si ce
-- compte devient non nul, -020 est livre et Q2/Q3 des requetes de prix marchent.

SELECT 'Q103618 (Academy Award for Best Actress)' AS temoin,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT WHERE ID_WIKIDATA='Q103618') AS nb_statements,
       CASE WHEN (SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT WHERE ID_WIKIDATA='Q103618') = 0
            THEN 'attendu : -020 n est pas livre, pas de question hierarchique'
            ELSE 'nouveau : -020 est livre, relancer Q2/Q3 des requetes de prix' END AS verdict;

-- ############################################################################
-- ### C . LE RUN EST-IL HOMOGENE ? (une seule version du code ?)   [lent]  ###
-- ############################################################################
--
-- Ajout du 2026-08-16. Les parties A et B verifient que le code QUI A TOURNE
-- portait le correctif -019. Elles supposent un run d'un seul tenant. Or un run
-- de plusieurs jours peut avoir ete repris apres incident (README : reprise par
-- --start-step, image reconstruite a chaque lancement), et deux portions du meme
-- resultat auraient alors ete produites par deux versions du code. B1 rendrait
-- dans ce cas un verdict global qui masque une cohorte defectueuse minoritaire.
-- Les deux tables porteuses ont un IMPORT_BATCH_ID indexe et un DAT_CREAT : la
-- question se mesure au lieu de se supposer.

SELECT '=== C1 . quels lots ont ecrit dans cette base, et sur quelle duree ? ===' AS section;
-- Un lot par run attendu. Le nom porte l'horodatage (wikidata_full_AAAAMMJJ_HHMM),
-- donc l'ordre lexicographique est l'ordre chronologique.

SELECT IMPORT_BATCH_ID,
       COUNT(*)                                            AS lignes,
       MIN(DAT_CREAT)                                      AS premiere_ecriture,
       MAX(DAT_CREAT)                                      AS derniere_ecriture,
       TIMESTAMPDIFF(HOUR, MIN(DAT_CREAT), MAX(DAT_CREAT)) AS etalement_heures
FROM   T_WC_WIKIDATA_STATEMENT
GROUP  BY IMPORT_BATCH_ID
ORDER  BY IMPORT_BATCH_ID DESC;


SELECT '=== C2 . chronologie du dernier lot : y a-t-il un trou ? ===' AS section;
-- Mesure sur la table des qualificatifs, six fois plus petite que STATEMENT pour
-- la meme information de rythme. Un trou de plusieurs heures au milieu du lot est
-- la signature d'un arret suivi d'une reprise : c'est la que deux versions du code
-- ont pu se succeder. Un rythme regulier ferme la question.

SET @BATCH = (SELECT MAX(IMPORT_BATCH_ID) FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER);
SELECT @BATCH AS lot_analyse;

SELECT DATE_FORMAT(DAT_CREAT, '%Y-%m-%d %H:00') AS heure,
       COUNT(*)                                 AS lignes_ecrites
FROM   T_WC_WIKIDATA_STATEMENT_QUALIFIER
WHERE  IMPORT_BATCH_ID = @BATCH
GROUP  BY 1
ORDER  BY 1;


SELECT '=== C3 . le controle B1, rejoue lot par lot ===' AS section;
-- Le controle decisif de la partie B, mais cohorte par cohorte. Si un lot est
-- effondre et un autre non, le code a change entre les deux, et le verdict global
-- de B1 n'etait qu'un artefact de moyenne.

SELECT q.IMPORT_BATCH_ID,
       q.ID_QUALIFIER_PROPERTY,
       COUNT(*)                   AS lignes,
       COUNT(DISTINCT qi.ID_ITEM) AS valeurs_distinctes,
       CASE WHEN COUNT(*) = COUNT(DISTINCT qi.ID_ITEM)
            THEN 'ALERTE : cette cohorte est effondree'
            ELSE 'ok' END          AS verdict
FROM   T_WC_WIKIDATA_STATEMENT_QUALIFIER q
JOIN   T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qi
       ON qi.ID_STATEMENT_QUALIFIER = q.ID_STATEMENT_QUALIFIER
WHERE  q.ID_QUALIFIER_PROPERTY IN ('P453','P1686','P155')
GROUP  BY q.IMPORT_BATCH_ID, q.ID_QUALIFIER_PROPERTY
ORDER  BY q.IMPORT_BATCH_ID DESC, q.ID_QUALIFIER_PROPERTY;


-- C4 . QUELLE VERSION DU CODE A PRODUIT CE LOT ? (hors SQL, a faire ensuite)
--
-- Aucune requete ne peut y repondre : la reponse est dans le depot, pas dans la
-- base. wikidata-crawler.sh reconstruit l'image a chaque lancement, donc l'image
-- porte l'etat du depot a l'heure du lancement. Relever startdatetime en A1, puis
-- dans le depot :
--
--   git log --oneline --until="AAAA-MM-JJ HH:MM" -15
--
-- Les commits listes sont ceux que ce run contient ; ceux d'apres n'y sont pas,
-- meme s'ils figurent dans les fichiers aujourd'hui. Consigner le SHA de tete a
-- cote des chiffres du run : sans lui, les chiffres ne sont pas reproductibles.

SELECT '========== FIN ==========' AS section;
