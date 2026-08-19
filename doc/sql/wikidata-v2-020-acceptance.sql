-- ============================================================================
-- Recette de WIKIDATA-CRAWLER-020, apres le rejeu depuis l'etape 106
-- ============================================================================
--
-- CE QUE CE RUN DEVAIT LIVRER. Il ne s'agit pas d'un run complet mais d'un rejeu
-- partiel, lance avec --start-step 106, qui porte cinq commits :
--
--   fa48050  le graphe P279 entre enfin en base           (voie b du ticket)
--   8cb5e9b  les items en cache emettent leurs faits      (voie a du ticket)
--   3c57a50  reprendre a une etape qui lit le dump resout la source
--   7fca878  le tuple des entites en cache couvre aussi les colonnes de V1
--   695d11c  le nettoyage des anciens lots couvre le graphe P279
--
-- La question a trancher n'est donc pas « le run s'est-il bien passe » (partie A
-- de wikidata-run-report.sql y repond) mais « a-t-il produit l'effet attendu ».
--
-- L'ORDRE DE PASSAGE. Ce fichier ne remplace pas les deux autres, il s'ajoute :
--
--   1. wikidata-run-report.sql          A statut et volumes, B non-regression du
--                                       correctif -019, C homogeneite du lot
--   2. wikidata-v2-020-acceptance.sql   E, ce fichier, l'effet de -020
--   3. wikidata-movie-drop-diagnostic.sql   D, seulement si A3a montre une baisse
--
-- La partie B garde tout son sens sur un rejeu : l'etape 106 reecrit des lignes,
-- et une image construite avant le correctif des qualificatifs les reecraserait.
--
-- LES CRITERES VIENNENT DU TICKET, pas d'une intuition. Le backlog Nestor
-- (projets/t2s-backlog/repos/wikidata-crawler.md, WIKIDATA-CRAWLER-020) fixe
-- trois conditions d'acceptation : un compte non nul de statements sur Q103618,
-- Q2 et Q3 de wikidata-v2-awards-queries.sql qui rendent des lignes sans qu'on y
-- touche, et « combien d'Oscars a recu Katharine Hepburn » qui rend 4. E2 et E6
-- les couvrent ; le reste mesure la portee et cherche les degats collateraux.
--
-- LECTURE SEULE. Executer avec --force -t.
--
-- DUREE. E3, E4 et E7a balaient des tables entieres et prennent quelques minutes
-- chacun sur la base de production ; E5 est volontairement borne a un echantillon
-- (voir son commentaire). Rien ici ne pose de verrou d'ecriture.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;


-- ############################################################################
-- E0 . DE QUEL RUN PARLE-T-ON ?
-- ############################################################################
-- A lire avant tout le reste : si startstep ne vaut pas 106, ce fichier n'est
-- pas le bon, et si status ne vaut pas SUCCESS, rien de ce qui suit ne se lit
-- comme un acquis.

SELECT '=== E0 . identite du run ===' AS section;

SELECT VAR_NAME AS variable, VAR_VALUE AS valeur
FROM   T_WC_SERVER_VARIABLE
WHERE  VAR_NAME IN ('strwikidatacrawlerstatus',
                    'strwikidatacrawlerstartstep',
                    'strwikidatacrawlerstartdatetime',
                    'strwikidatacrawlerenddatetime',
                    'strwikidatacrawlertotalruntime',
                    'strwikidatacrawlerprocessesexecuted',
                    'strwikidatacrawlerlasterror')
ORDER  BY VAR_NAME;


-- ############################################################################
-- E1 . VOIE (b) : LE GRAPHE P279 EST-IL EN BASE, ET DE LA BONNE TAILLE ?
-- ############################################################################
-- Avant fa48050, collect_subclass_edges ecrivait subclass_edges.jsonl sur le
-- disque et aucune table ne le lisait. La table doit exister et etre pleine.
--
-- REFERENCE, mesuree le 2026-08-17 : 5 227 784 aretes en base pour 5 228 221
-- lignes dans subclass_edges.jsonl. L'ecart de quelques centaines est la cle
-- primaire composite qui dedoublonne, ce n'est pas une perte. Une valeur du meme
-- ordre est bonne ; quelques milliers signifierait qu'un fragment seulement est
-- charge, symptome exact d'avant le correctif (3 202 aretes vues par D0).

SELECT '=== E1a . volumetrie du graphe des sous-classes ===' AS section;

SELECT COUNT(*)                  AS aretes,
       COUNT(DISTINCT ID_PARENT) AS classes_parentes,
       COUNT(DISTINCT ID_CHILD)  AS classes_filles,
       SUM(DELETED = 1)          AS aretes_supprimees,
       MIN(DAT_CREAT)            AS premiere_ecriture,
       MAX(TIM_UPDATED)          AS derniere_ecriture
FROM   T_WC_WIKIDATA_SUBCLASS;

SELECT '=== E1b . un seul lot, ou plusieurs ? ===' AS section;
-- Plusieurs lots ici veut dire que le nettoyage 695d11c n'a pas fait son travail,
-- ou que deux rejeux se sont superposes.

SELECT IMPORT_BATCH_ID AS lot, COUNT(*) AS aretes
FROM   T_WC_WIKIDATA_SUBCLASS
GROUP  BY IMPORT_BATCH_ID
ORDER  BY aretes DESC;

SELECT '=== E1c . la taille du pool film que cette base connait ===' AS section;
-- Le CAST est obligatoire : sans lui l'ancre du CTE se type sur la longueur du
-- litteral et MariaDB refuse ses propres Q-ids (ERROR 1406). Les deux racines
-- passent par une sous-requete : un UNION a plusieurs branches place directement
-- dans l'ancre ne garde silencieusement que la premiere.
--
-- REFERENCE : 842 classes. Plancher d'alerte : 167. En dessous, le graphe est
-- tronque et tout verdict tire de D1 ou D3 est a jeter.

WITH RECURSIVE pool_film (qid) AS (
    SELECT CAST(r.qid AS CHAR(50)) COLLATE utf8mb4_unicode_ci AS qid
    FROM   (SELECT 'Q11424' AS qid UNION ALL SELECT 'Q506240') AS r
    UNION
    SELECT sc.ID_CHILD
    FROM   T_WC_WIKIDATA_SUBCLASS sc
    JOIN   pool_film p ON p.qid = sc.ID_PARENT
    WHERE  sc.DELETED = 0
)
SELECT COUNT(*) AS classes_du_pool_film,
       842      AS reference_20260817,
       167      AS plancher_alerte
FROM   pool_film;


-- ############################################################################
-- E2 . VOIE (a) : LE CRITERE D'ACCEPTATION LITTERAL DU TICKET
-- ############################################################################
-- « SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT WHERE ID_WIKIDATA='Q103618'
--   rend un compte non nul ». Q103618 = Academy Award for Best Actress, item en
-- cache, qui avait son libelle et zero fait. Q19020 = Academy Award, son parent.
--
-- ATTENDU : nb_statements >= 1, et le P31 de Q103618 doit pointer sur Q19020.
-- Si nb_statements vaut encore 0, -020 n'est pas livre, inutile de lire la suite.

SELECT '=== E2a . Q103618 a-t-elle enfin des faits ? ===' AS section;

SELECT (SELECT LABEL_EN FROM T_WC_WIKIDATA_ITEM       WHERE ID_WIKIDATA = 'Q103618') AS libelle,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT  WHERE ID_WIKIDATA = 'Q103618') AS nb_statements,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT  WHERE ID_WIKIDATA = 'Q19020')  AS nb_statements_du_parent;

SELECT '=== E2b . le detail, propriete par propriete ===' AS section;

SELECT st.ID_PROPERTY AS propriete,
       st.VALUE_TYPE  AS type_valeur,
       COALESCE(iv.ID_ITEM, ev.VALUE_EXTERNAL_ID, CAST(tv.YEAR_VALUE AS CHAR)) AS valeur,
       cible.LABEL_EN AS libelle_cible
FROM   T_WC_WIKIDATA_STATEMENT st
LEFT   JOIN T_WC_WIKIDATA_ITEM_VALUE        iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT   JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = st.ID_STATEMENT
LEFT   JOIN T_WC_WIKIDATA_TIME_VALUE        tv ON tv.ID_STATEMENT = st.ID_STATEMENT
LEFT   JOIN T_WC_WIKIDATA_ITEM           cible ON cible.ID_WIKIDATA = iv.ID_ITEM
WHERE  st.ID_WIKIDATA = 'Q103618'
ORDER  BY st.ID_PROPERTY;


-- ############################################################################
-- E3 . LA PORTEE : COMBIEN D'ITEMS EN CACHE SONT SORTIS DU MUTISME ?
-- ############################################################################
-- Le ticket mesurait 93,9 % d'items muets le 2026-07-31 et estimait ~1,2 M
-- statements de plus pour deux proprietes. Le tuple ayant ete elargi a six
-- (7fca878), le volume attendu est superieur, sans qu'aucune reference n'existe :
-- ce bloc EN CREE UNE pour les prochains runs. Ce qui compte aujourd'hui est le
-- taux de muets, qui doit s'effondrer.

SELECT '=== E3 . items en cache, avec et sans faits ===' AS section;

SELECT COUNT(*) AS items_total,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                   WHERE s.ID_WIKIDATA = i.ID_WIKIDATA)) AS items_avec_faits,
       ROUND(100 * SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                   WHERE s.ID_WIKIDATA = i.ID_WIKIDATA)) / COUNT(*), 1) AS pct_avec_faits,
       '6,1 % le 2026-07-31' AS reference_avant_020
FROM   T_WC_WIKIDATA_ITEM i
WHERE  i.DELETED = 0;


-- ############################################################################
-- E4 . LE TUPLE EST-IL BIEN CELUI DU CODE, NI PLUS NI MOINS ?
-- ############################################################################
-- CACHED_ENTITY_PROPERTIES vaut exactement P31, P279, P345, P569, P570, P577.
--
-- LIRE CE BLOC AVEC SA LIMITE, ELLE EST REELLE. La premiere version de E4a
-- annoncait des centaines de proprietes « HORS TUPLE » et c'etait un artefact de
-- la requete, pas une fuite du filtre. Deux raisons, mesurees le 2026-08-19.
--
-- 1. T_WC_WIKIDATA_ITEM ne contient pas que des vignettes de cache. Elle recoit
--    aussi les entites EN PORTEE qui n'ont pas de table dediee : jeux video,
--    livres, albums, ceremonies. Celles-la portent leurs claims complets par la
--    voie normale, d'ou P527, P161, P1441 et la longue traine. C'etait deja vrai
--    avant -020, et c'est ce que mesuraient les 6,1 % d'items non muets.
-- 2. Symetriquement, une entite mise en cache n'atterrit pas forcement dans ITEM :
--    une personne citee mais hors portee va dans PERSON. C'est pourquoi le tuple
--    contient P569 et P570, et pourquoi la somme lue ici (1 055 186 le 2026-08-19)
--    reste sous les 1 428 106 statements emis par l'etape 106.
--
-- Ce bloc dit donc ce que les sujets d'ITEM portent, pas ce que le cache a emis.
-- Les deux lignes a regarder sont P31 et P279 : elles doivent dominer largement.
-- La preuve que la garde n'a pas fui est en E5, pas ici.

SELECT '=== E4a . proprietes portees par les sujets stockes dans ITEM ===' AS section;

SELECT st.ID_PROPERTY AS propriete,
       CASE st.ID_PROPERTY
            WHEN 'P31'  THEN 'instance de         (attendu)'
            WHEN 'P279' THEN 'sous-classe de      (attendu)'
            WHEN 'P345' THEN 'ID IMDb             (attendu)'
            WHEN 'P569' THEN 'date de naissance   (attendu)'
            WHEN 'P570' THEN 'date de deces       (attendu)'
            WHEN 'P577' THEN 'date de publication (attendu)'
            ELSE             'hors tuple, entite en portee logee dans ITEM'
       END AS statut,
       COUNT(*) AS nb_statements,
       COUNT(DISTINCT st.ID_WIKIDATA) AS nb_items_concernes
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = st.ID_WIKIDATA
GROUP  BY st.ID_PROPERTY
ORDER  BY nb_statements DESC
LIMIT  25;

SELECT '=== E4b . les trois types de valeur atterrissent-ils ? ===' AS section;
-- emit_class_claims_for_cached_item traite item, external_id et time. Chacune des
-- trois lignes doit etre non vide, sinon une branche est morte sans le dire.

SELECT st.VALUE_TYPE AS type_valeur,
       COUNT(*)      AS nb_statements,
       SUM(iv.ID_STATEMENT IS NOT NULL) AS avec_ligne_item,
       SUM(ev.ID_STATEMENT IS NOT NULL) AS avec_ligne_external_id,
       SUM(tv.ID_STATEMENT IS NOT NULL) AS avec_ligne_time
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = st.ID_WIKIDATA
LEFT   JOIN T_WC_WIKIDATA_ITEM_VALUE        iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT   JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = st.ID_STATEMENT
LEFT   JOIN T_WC_WIKIDATA_TIME_VALUE        tv ON tv.ID_STATEMENT = st.ID_STATEMENT
GROUP  BY st.VALUE_TYPE
ORDER  BY nb_statements DESC;


-- ############################################################################
-- E5 . DEGAT COLLATERAL A EXCLURE : DES STATEMENTS ORPHELINS
-- ############################################################################
-- Le code emet sous condition du drapeau `cached`, precisement pour qu'aucun fait
-- ne designe une entite absente. Ce bloc verifie la promesse sur les statements
-- du lot qui vient d'etre charge.
--
-- ATTENDU : 0. Toute autre valeur veut dire que la garde a fui, et que des
-- jointures aval rendront des lignes a libelle vide.
--
-- ECHANTILLON ASSUME, ET DIT. Balayer les 37 M de statements avec sept NOT EXISTS
-- couterait des heures pour une question qui se tranche sur un echantillon. On
-- prend les 200 000 statements d'ID le plus eleve parmi les six proprietes du
-- tuple : ce sont exactement ceux que le rejeu vient d'ecrire, et l'ORDER BY sur
-- la cle primaire les sort sans tri. Un orphelin ici suffit a condamner ; zero
-- orphelin ici ne prouve pas zero orphelin partout.

SELECT '=== E5 . statements dont le sujet n existe dans aucune table d entite ===' AS section;

SELECT COUNT(*) AS statements_orphelins,
       200000   AS taille_echantillon
FROM   (SELECT s.ID_WIKIDATA
        FROM   T_WC_WIKIDATA_STATEMENT s
        WHERE  s.ID_PROPERTY IN ('P31','P279','P345','P569','P570','P577')
        ORDER  BY s.ID_STATEMENT DESC
        LIMIT  200000) st
WHERE  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE     e WHERE e.ID_WIKIDATA = st.ID_WIKIDATA)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE     e WHERE e.ID_WIKIDATA = st.ID_WIKIDATA)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON    e WHERE e.ID_WIKIDATA = st.ID_WIKIDATA)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM      e WHERE e.ID_WIKIDATA = st.ID_WIKIDATA)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON    e WHERE e.ID_WIKIDATA = st.ID_WIKIDATA)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE   e WHERE e.ID_WIKIDATA = st.ID_WIKIDATA)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER e WHERE e.ID_WIKIDATA = st.ID_WIKIDATA);


-- ############################################################################
-- E6 . LA QUESTION QUI ETAIT MORTE : COMBIEN D'OSCARS ?
-- ############################################################################
-- Forme cible, inchangee depuis le 2026-07-31, conservee telle quelle dans
-- wikidata-v2-awards-queries.sql (Q2) pour qu'elle serve de test et non de
-- reecriture. Elle etait vide parce que Q103618 n'avait pas son P31.
--
-- ATTENDU : Katharine Hepburn (Q56016) rend 4. C'est le troisieme critere du
-- ticket, et le seul verifiable a la main sur une source publique.

SELECT '=== E6 . nombre d Oscars par la hierarchie P31 -> Q19020 ===' AS section;

SELECT p.LABEL_EN AS personne,
       COUNT(*)   AS nb_oscars
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_PERSON p ON p.ID_WIKIDATA = st.ID_WIKIDATA
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
WHERE  st.ID_PROPERTY = 'P166'
  AND  st.ID_WIKIDATA IN ('Q56016','Q8704')
  AND  EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT parent
               JOIN T_WC_WIKIDATA_ITEM_VALUE piv ON piv.ID_STATEMENT = parent.ID_STATEMENT
               WHERE parent.ID_WIKIDATA = iv.ID_ITEM
                 AND parent.ID_PROPERTY = 'P31'
                 AND piv.ID_ITEM        = 'Q19020')
GROUP  BY p.LABEL_EN;


-- ############################################################################
-- E7 . LE GAIN EN AVAL : TMDB-MOVIE-PREPROCESS-036 DEVIENT-IL FAISABLE ?
-- ############################################################################
-- -036 constate que T_WC_T2S_AWARD melange des vraies recompenses, des ceremonies
-- et des films : 44 084 lignes, dont 18 792 sans nom et 7 805 qui pointent un
-- film. La troisieme piste de resolution consiste a trancher par le P31 de V2,
-- ce qui etait impossible tant que les items en cache etaient muets.
--
-- Ce bloc ne corrige rien. Il mesure si la matiere du tri existe desormais.

SELECT '=== E7a . couverture P31 des lignes de T_WC_T2S_AWARD ===' AS section;

SELECT COUNT(*) AS lignes_award,
       SUM(a.ID_WIKIDATA IS NOT NULL) AS avec_id_wikidata,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                   WHERE s.ID_WIKIDATA = a.ID_WIKIDATA
                     AND s.ID_PROPERTY = 'P31')) AS avec_p31_en_v2,
       'proche de 0 avant le rejeu, par construction' AS reference
FROM   T_WC_T2S_AWARD a
WHERE  a.DELETED IS NULL OR a.DELETED = 0;

SELECT '=== E7b . sur quoi pointe ce P31 ? le tri se lit ici ===' AS section;
-- Chaque ligne est une classe. On doit y reconnaitre les trois populations que
-- -036 decrit : les categories de prix, les ceremonies, les films. C'est cette
-- table qui dira si un filtre par classe suffit, ou s'il faut la fermeture P279.

SELECT piv.ID_ITEM AS classe,
       COALESCE(c.LABEL_EN, '(libelle absent)') AS libelle_classe,
       COUNT(DISTINCT a.ID_AWARD) AS nb_lignes_award
FROM   T_WC_T2S_AWARD a
JOIN   T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = a.ID_WIKIDATA
                                 AND st.ID_PROPERTY = 'P31'
JOIN   T_WC_WIKIDATA_ITEM_VALUE piv ON piv.ID_STATEMENT = st.ID_STATEMENT
LEFT   JOIN T_WC_WIKIDATA_ITEM c ON c.ID_WIKIDATA = piv.ID_ITEM
WHERE  a.DELETED IS NULL OR a.DELETED = 0
GROUP  BY piv.ID_ITEM, c.LABEL_EN
ORDER  BY nb_lignes_award DESC
LIMIT  30;

SELECT '=== E7c . le tri par le cone P279 sous Q618779 (award) ===' AS section;
-- E7b montre que la pollution est plus large que « des ceremonies et des films » :
-- des humains, des oeuvres litteraires, des series, des albums. Une liste de
-- classes a la main raterait la traine. Les deux voies de -020 se combinent ici :
-- la voie (a) donne le P31 de chaque ligne, la voie (b) donne le graphe qui dit
-- si cette classe est une sorte de recompense. C'est la troisieme piste de
-- TMDB-MOVIE-PREPROCESS-036 rendue calculable.
--
-- Ce bloc ne propose pas encore le filtre definitif : il mesure ce qu'il
-- garderait et ce qu'il jetterait, pour que l'arbitrage se fasse sur des chiffres.

WITH RECURSIVE cone_award (qid) AS (
    SELECT CAST(r.qid AS CHAR(50)) COLLATE utf8mb4_unicode_ci AS qid
    FROM   (SELECT 'Q618779' AS qid) AS r
    UNION
    SELECT sc.ID_CHILD
    FROM   T_WC_WIKIDATA_SUBCLASS sc
    JOIN   cone_award c ON c.qid = sc.ID_PARENT
    WHERE  sc.DELETED = 0
)
SELECT (SELECT COUNT(*) FROM cone_award) AS classes_du_cone_award,
       COUNT(DISTINCT a.ID_AWARD)        AS lignes_award_total,
       COUNT(DISTINCT CASE WHEN piv.ID_ITEM IN (SELECT qid FROM cone_award)
                           THEN a.ID_AWARD END) AS dans_le_cone,
       COUNT(DISTINCT CASE WHEN piv.ID_ITEM NOT IN (SELECT qid FROM cone_award)
                           THEN a.ID_AWARD END) AS hors_du_cone
FROM   T_WC_T2S_AWARD a
LEFT   JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = a.ID_WIKIDATA
                                      AND st.ID_PROPERTY = 'P31'
LEFT   JOIN T_WC_WIKIDATA_ITEM_VALUE piv ON piv.ID_STATEMENT = st.ID_STATEMENT
WHERE  a.DELETED IS NULL OR a.DELETED = 0;
