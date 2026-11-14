-- ============================================================================
-- Les prix en Wikidata V2 : les requetes de reference
-- ============================================================================
--
-- Repond aux questions du backlog qui restaient sans reponse tant que les
-- qualificatifs etaient effondres (WIKIDATA-CRAWLER-019, corrige le 2026-07-31) :
--
--   Q1  tous les prix d'une personne, avec annee et oeuvre   (-006, eval eid 2313)
--   Q2  combien de prix d'une famille donnee (les Oscars)     (eval eid 2313, 2349)
--   Q3  les femmes qui en ont plusieurs, toutes categories    (eval eid 2301)
--   Q4  qui a recu tel prix, par annee                        (eval eid 75)
--   Q5  les prix d'un film                                    (front)
--   Q6  nomme mais non recompense                             (P1411)
--
-- LE MODELE, en une phrase. Un fait de recompense tient dans un statement P166
-- dont la VALEUR est la categorie de prix, et dont les QUALIFICATIFS portent le
-- reste : P585 l'annee, P1686 l'oeuvre, P805 la ceremonie. Les quatre entrees
-- que le backlog reclamait sont donc nativement separees, la ou V1 les ecrasait
-- toutes dans une seule colonne (cf. WIKIDATA-CRAWLER-014).
--
-- LA HIERARCHIE, verifiee sur wikidata.org le 2026-07-31, etendue le 2026-11-10.
-- La question ouverte du backlog etait : « comment regrouper toutes les
-- categories d'Academy Award ? Like '%Academy award%' ? Custom list ? ».
-- Reponse : NI L'UN NI L'AUTRE. La categorie tient a sa famille par trois routes :
--   P31   Q103618 « Academy Award for Best Actress » P31 -> Q19020
--   P361  la categorie est « part of » la famille
--   P527  la famille liste la categorie (« has part »), lu a l'envers
-- Aucune ne suffit seule. P31 : « Academy Award for Best Picture » (Q102427) et
-- Best Director n'ont pas de P31 vers Q19020 (mesure du 2026-11-10,
-- TMDB-MOVIE-PREPROCESS-071). P527 : 18 valeurs seulement pour Q19020. On prend
-- donc les trois ensemble, et jamais le libelle.
-- P31 est charge sur les entites en cache depuis WIKIDATA-CRAWLER-020
-- (2026-08-17), P361 et P527 depuis WIKIDATA-CRAWLER-032 (premier run qui les
-- porte : dump du 2026-11-11, charge le 2026-11-14).
--
-- Prix parents utiles : Q19020 Academy Awards, Q174389 Cesar Awards.
--
-- CORRECTION DU 2026-08-07. Le libelle d'une ceremonie etait cherche dans le seul
-- cache d'items, et sortait donc en identifiant brut. Motif : depuis la
-- reintegration de Q15416 « television program » dans SERIES_ROOTS, une ceremonie
-- retransmise est une entite de plein droit. Q85314819 vit dans
-- T_WC_WIKIDATA_SERIE sous le nom « 96th Academy Awards ». Les jointures de
-- libelle cherchent desormais dans les deux tables. Lecon generale : ne jamais
-- presupposer la table ou vit un QID, le classement V2 depend des racines P31.
--
-- LECTURE SEULE, a une table temporaire pres (TMP_CATEGORIE_OSCAR, les categories
-- de la famille Q19020, construite avant Q2 et lue par Q2, Q2-bis et Q3).
-- Executer avec --force -t.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET SESSION max_statement_time = 0;


-- ############################################################################
-- ### Q1 . Tous les prix d'une personne, avec annee et oeuvre               ###
-- ############################################################################
-- « Tous les awards par Katharine Hepburn » (WIKIDATA-CRAWLER-006).
-- Q56016 = Katharine Hepburn. Elle porte quatre statements P166 vers la meme
-- categorie Q103618 : ce sont ses quatre Oscars, que SEULS les qualificatifs
-- distinguent. C'est exactement ce qui etait impossible avant le correctif.

SELECT '=== Q1 . les prix de Katharine Hepburn (Q56016) ===' AS section;

SELECT COALESCE(prix.LABEL_EN, iv.ID_ITEM)         AS recompense,
       qt.YEAR_VALUE                               AS annee,
       COALESCE(film.LABEL_EN, qi_work.ID_ITEM)    AS pour_l_oeuvre,
       COALESCE(cer.LABEL_EN, cer2.LABEL_EN, qi_cer.ID_ITEM) AS ceremonie
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM prix ON prix.ID_WIKIDATA = iv.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q_an ON q_an.ID_STATEMENT = st.ID_STATEMENT
       AND q_an.ID_QUALIFIER_PROPERTY = 'P585'
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_TIME_VALUE qt ON qt.ID_STATEMENT_QUALIFIER = q_an.ID_STATEMENT_QUALIFIER
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q_oe ON q_oe.ID_STATEMENT = st.ID_STATEMENT
       AND q_oe.ID_QUALIFIER_PROPERTY = 'P1686'
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qi_work ON qi_work.ID_STATEMENT_QUALIFIER = q_oe.ID_STATEMENT_QUALIFIER
LEFT JOIN T_WC_WIKIDATA_MOVIE film ON film.ID_WIKIDATA = qi_work.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q_ce ON q_ce.ID_STATEMENT = st.ID_STATEMENT
       AND q_ce.ID_QUALIFIER_PROPERTY = 'P805'
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qi_cer ON qi_cer.ID_STATEMENT_QUALIFIER = q_ce.ID_STATEMENT_QUALIFIER
LEFT JOIN T_WC_WIKIDATA_ITEM  cer  ON cer.ID_WIKIDATA  = qi_cer.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_SERIE cer2 ON cer2.ID_WIKIDATA = qi_cer.ID_ITEM
WHERE  st.ID_WIKIDATA = 'Q56016'
  AND  st.ID_PROPERTY = 'P166'
ORDER BY qt.YEAR_VALUE;


-- ############################################################################
-- ### Q2 . Combien de prix d'une famille donnee ?                           ###
-- ############################################################################
-- « How many Oscars did Hepburn receive? » (eid 2313)
-- « How many academy awards did Walt Disney receive? » (eid 2349, Q8704)
--
-- Les categories de la famille sont reunies une fois, par les trois routes de
-- LA HIERARCHIE. Aucun filtre sur le libelle, aucune liste a maintenir.
-- PAR_P31 vaut 1 quand la route P31 existe : il montre ce que les deux autres
-- routes ajoutent (Walt Disney : 20 par P31 seul le 2026-11-09).
--
-- HISTORIQUE. Jusqu'au 2026-08-17, Q2, Q2-bis et Q3 ne rendaient rien : une
-- categorie de prix n'etait qu'un item en cache, avec un libelle et aucun claim.
-- Corrige par WIKIDATA-CRAWLER-020 (item_cache emet P31 et P279), puis -032
-- (P361 et P527).
--
-- LE MEME CALCUL EN T2S. T_WC_T2S_AWARD.ID_AWARD_PARENT (tmdb-movie-preprocess,
-- process 44) suit la meme regle, a une nuance pres : il garde UNE famille par
-- categorie (celle qui compte le plus de categories), la ou cette requete teste
-- l'appartenance a Q19020. Les deux comptes peuvent donc differer de quelques
-- unites pour une categorie rattachee a deux familles.

DROP TEMPORARY TABLE IF EXISTS TMP_CATEGORIE_OSCAR;

CREATE TEMPORARY TABLE TMP_CATEGORIE_OSCAR AS
SELECT r.ID_CATEGORIE, MAX(r.PAR_P31) AS PAR_P31
FROM  (SELECT c.ID_WIKIDATA AS ID_CATEGORIE,
              CASE WHEN c.ID_PROPERTY = 'P31' THEN 1 ELSE 0 END AS PAR_P31
       FROM   T_WC_WIKIDATA_STATEMENT c
       JOIN   T_WC_WIKIDATA_ITEM_VALUE cv ON cv.ID_STATEMENT = c.ID_STATEMENT
       WHERE  c.ID_PROPERTY IN ('P31','P361')
         AND  cv.ID_ITEM = 'Q19020'
       UNION ALL
       SELECT fv.ID_ITEM, 0
       FROM   T_WC_WIKIDATA_STATEMENT f
       JOIN   T_WC_WIKIDATA_ITEM_VALUE fv ON fv.ID_STATEMENT = f.ID_STATEMENT
       WHERE  f.ID_WIKIDATA = 'Q19020'
         AND  f.ID_PROPERTY = 'P527') r
GROUP BY r.ID_CATEGORIE;

SELECT '=== Q2-pre . les categories de la famille Q19020, par route ===' AS section;

SELECT COUNT(*) AS categories, SUM(PAR_P31) AS dont_par_p31, COUNT(*) - SUM(PAR_P31) AS par_p361_ou_p527_seulement
FROM   TMP_CATEGORIE_OSCAR;

SELECT '=== Q2 . nombre d Oscars, par la famille Q19020 (P31, P361 ou P527 inverse) ===' AS section;

SELECT p.LABEL_EN AS personne,
       COUNT(*)   AS nb_oscars,
       SUM(fam.PAR_P31) AS dont_par_p31
SELECT p.LABEL_EN AS personne,
       COUNT(*)   AS nb_oscars
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_PERSON p ON p.ID_WIKIDATA = st.ID_WIKIDATA
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
JOIN   TMP_CATEGORIE_OSCAR fam ON fam.ID_CATEGORIE = iv.ID_ITEM
WHERE  st.ID_PROPERTY = 'P166'
  AND  st.ID_WIKIDATA IN ('Q56016','Q8704')
GROUP BY p.LABEL_EN;

SELECT '=== Q2-bis . le detail, categorie par categorie ===' AS section;
-- Utile pour verifier a la main : Hepburn doit sortir 4 fois « Best Actress ».

SELECT p.LABEL_EN AS personne,
       COALESCE(prix.LABEL_EN, iv.ID_ITEM) AS categorie,
       qt.YEAR_VALUE AS annee
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_PERSON p ON p.ID_WIKIDATA = st.ID_WIKIDATA
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
JOIN   TMP_CATEGORIE_OSCAR fam ON fam.ID_CATEGORIE = iv.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_ITEM prix ON prix.ID_WIKIDATA = iv.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q_an ON q_an.ID_STATEMENT = st.ID_STATEMENT
       AND q_an.ID_QUALIFIER_PROPERTY = 'P585'
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_TIME_VALUE qt ON qt.ID_STATEMENT_QUALIFIER = q_an.ID_STATEMENT_QUALIFIER
WHERE  st.ID_PROPERTY = 'P166'
  AND  st.ID_WIKIDATA IN ('Q56016','Q8704')
ORDER BY personne, annee;


SELECT '=== Q2-ter . CONTROLE : la categorie de prix porte-t-elle ses claims ? ===' AS section;
-- Attendu depuis -020 : un libelle, et des statements non nuls des deux cotes.
-- Zero statement voudrait dire que item_cache n'emet plus les claims des items
-- en cache : Q2 et Q3 ne rendraient plus rien.

SELECT 'Q103618 (Academy Award for Best Actress)' AS categorie,
       (SELECT LABEL_EN FROM T_WC_WIKIDATA_ITEM WHERE ID_WIKIDATA='Q103618')            AS libelle_en_cache,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT WHERE ID_WIKIDATA='Q103618')       AS nb_statements,
       (SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT WHERE ID_WIKIDATA='Q19020')        AS nb_statements_du_parent;

SELECT '=== Q2-quater . COMPARAISON : le meme compte par le libelle ===' AS section;
-- L'approche que le backlog voulait eviter (`Like '%Academy award%'`), gardee
-- comme temoin de ce qu'elle rate : les libelles non anglais, « Oscar » quand
-- c'est le nom retenu, les categories dont le nom ne commence pas par « Academy
-- Award » (Irving G. Thalberg Memorial Award), et ce qui commence pareil sans
-- etre un Oscar. Ne pas s'en servir : l'ecart avec Q2 est sa mesure.

SELECT p.LABEL_EN AS personne, COUNT(*) AS nb_oscars_approx
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_PERSON p ON p.ID_WIKIDATA = st.ID_WIKIDATA
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
JOIN   T_WC_WIKIDATA_ITEM prix ON prix.ID_WIKIDATA = iv.ID_ITEM
WHERE  st.ID_PROPERTY = 'P166'
  AND  st.ID_WIKIDATA IN ('Q56016','Q8704')
  AND  prix.LABEL_EN LIKE 'Academy Award%'
GROUP BY p.LABEL_EN;


-- ############################################################################
-- ### Q3 . Les femmes qui ont plusieurs Oscars, toutes categories           ###
-- ############################################################################
-- « Actresses with several Academy Awards » (eid 2301).
-- Le genre vient de P21 -> Q6581072 (femme). Toutes categories confondues : la
-- liste commence donc par des costumieres (Edith Head, 8) et non par des
-- actrices. Pour les seules actrices, restreindre TMP_CATEGORIE_OSCAR aux
-- categories d'interpretation.

SELECT '=== Q3 . femmes ayant plusieurs Oscars, toutes categories ===' AS section;

SELECT p.LABEL_EN AS laureate,
       COUNT(*)   AS nb_oscars
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_PERSON p ON p.ID_WIKIDATA = st.ID_WIKIDATA
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
JOIN   TMP_CATEGORIE_OSCAR fam ON fam.ID_CATEGORIE = iv.ID_ITEM
WHERE  st.ID_PROPERTY = 'P166'
  AND  EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT g
               JOIN T_WC_WIKIDATA_ITEM_VALUE giv ON giv.ID_STATEMENT = g.ID_STATEMENT
               WHERE g.ID_WIKIDATA = st.ID_WIKIDATA
                 AND g.ID_PROPERTY = 'P21'
                 AND giv.ID_ITEM   = 'Q6581072')
GROUP BY p.LABEL_EN
HAVING COUNT(*) >= 2
ORDER BY nb_oscars DESC, laureate
LIMIT 40;


-- ############################################################################
-- ### Q4 . Qui a recu tel prix, annee par annee                             ###
-- ############################################################################
-- « Cesar du meilleur film » (eid 75). Q645595 = Cesar Award for Best Film.
-- Le laureat peut etre une oeuvre comme une personne : on cherche donc le
-- libelle dans les deux tables.

SELECT '=== Q4 . les laureats du Cesar du meilleur film (Q645595) ===' AS section;

SELECT qt.YEAR_VALUE AS annee,
       COALESCE(film.LABEL_EN, pers.LABEL_EN, it.LABEL_EN, st.ID_WIKIDATA) AS laureat
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q_an ON q_an.ID_STATEMENT = st.ID_STATEMENT
       AND q_an.ID_QUALIFIER_PROPERTY = 'P585'
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_TIME_VALUE qt ON qt.ID_STATEMENT_QUALIFIER = q_an.ID_STATEMENT_QUALIFIER
LEFT JOIN T_WC_WIKIDATA_MOVIE  film ON film.ID_WIKIDATA = st.ID_WIKIDATA
LEFT JOIN T_WC_WIKIDATA_PERSON pers ON pers.ID_WIKIDATA = st.ID_WIKIDATA
LEFT JOIN T_WC_WIKIDATA_ITEM   it   ON it.ID_WIKIDATA   = st.ID_WIKIDATA
WHERE  st.ID_PROPERTY = 'P166'
  AND  iv.ID_ITEM     = 'Q645595'
ORDER BY annee DESC
LIMIT 40;


-- ############################################################################
-- ### Q5 . Les prix d'un film                                               ###
-- ############################################################################
-- Le sens que le front affiche sur une fiche film. Q116413183 = American Fiction.
-- Attention : un film peut recevoir un prix directement (P166 sur le film) OU
-- etre l'oeuvre pour laquelle une personne a ete recompensee (P1686 en
-- qualificatif). Les deux comptent, d'ou l'union.

SELECT '=== Q5 . les prix lies a un film, les deux sens ===' AS section;

SELECT 'recu par le film' AS sens,
       COALESCE(prix.LABEL_EN, iv.ID_ITEM) AS recompense,
       qt.YEAR_VALUE AS annee, NULL AS laureat
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM prix ON prix.ID_WIKIDATA = iv.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q_an ON q_an.ID_STATEMENT = st.ID_STATEMENT
       AND q_an.ID_QUALIFIER_PROPERTY = 'P585'
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_TIME_VALUE qt ON qt.ID_STATEMENT_QUALIFIER = q_an.ID_STATEMENT_QUALIFIER
WHERE  st.ID_WIKIDATA = 'Q116413183' AND st.ID_PROPERTY = 'P166'
UNION ALL
SELECT 'recu POUR le film',
       COALESCE(prix.LABEL_EN, iv.ID_ITEM),
       qt.YEAR_VALUE,
       COALESCE(pers.LABEL_EN, st.ID_WIKIDATA)
FROM   T_WC_WIKIDATA_STATEMENT_QUALIFIER q_oe
JOIN   T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qv ON qv.ID_STATEMENT_QUALIFIER = q_oe.ID_STATEMENT_QUALIFIER
JOIN   T_WC_WIKIDATA_STATEMENT st ON st.ID_STATEMENT = q_oe.ID_STATEMENT
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM prix ON prix.ID_WIKIDATA = iv.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_PERSON pers ON pers.ID_WIKIDATA = st.ID_WIKIDATA
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q_an ON q_an.ID_STATEMENT = st.ID_STATEMENT
       AND q_an.ID_QUALIFIER_PROPERTY = 'P585'
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_TIME_VALUE qt ON qt.ID_STATEMENT_QUALIFIER = q_an.ID_STATEMENT_QUALIFIER
WHERE  q_oe.ID_QUALIFIER_PROPERTY = 'P1686'
  AND  qv.ID_ITEM = 'Q116413183'
  AND  st.ID_PROPERTY = 'P166';


-- ############################################################################
-- ### Q6 . Nomme mais non recompense                                        ###
-- ############################################################################
-- P1411 « nominated for » se lit exactement comme P166. La nuance interessante
-- pour un conseiller : les nominations SANS victoire correspondante.

SELECT '=== Q6 . les nominations sans victoire, pour une personne ===' AS section;

SELECT COALESCE(prix.LABEL_EN, iv.ID_ITEM) AS nomme_pour,
       qt.YEAR_VALUE AS annee,
       COALESCE(film.LABEL_EN, qi_work.ID_ITEM) AS pour_l_oeuvre
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM prix ON prix.ID_WIKIDATA = iv.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q_an ON q_an.ID_STATEMENT = st.ID_STATEMENT
       AND q_an.ID_QUALIFIER_PROPERTY = 'P585'
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_TIME_VALUE qt ON qt.ID_STATEMENT_QUALIFIER = q_an.ID_STATEMENT_QUALIFIER
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q_oe ON q_oe.ID_STATEMENT = st.ID_STATEMENT
       AND q_oe.ID_QUALIFIER_PROPERTY = 'P1686'
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qi_work ON qi_work.ID_STATEMENT_QUALIFIER = q_oe.ID_STATEMENT_QUALIFIER
LEFT JOIN T_WC_WIKIDATA_MOVIE film ON film.ID_WIKIDATA = qi_work.ID_ITEM
WHERE  st.ID_WIKIDATA = 'Q56016'
  AND  st.ID_PROPERTY = 'P1411'
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT w
                   JOIN T_WC_WIKIDATA_ITEM_VALUE wiv ON wiv.ID_STATEMENT = w.ID_STATEMENT
                   WHERE w.ID_WIKIDATA = st.ID_WIKIDATA
                     AND w.ID_PROPERTY = 'P166'
                     AND wiv.ID_ITEM   = iv.ID_ITEM)
ORDER BY annee;

DROP TEMPORARY TABLE IF EXISTS TMP_CATEGORIE_OSCAR;

SELECT '========== FIN ==========' AS section;
-- ============================================================================
-- DEUX PIEGES A CONNAITRE
--
-- 1. Le libelle de la ceremonie (P805) et de certaines categories peut etre
--    VIDE. Ces items n'apparaissent qu'en valeur de qualificatif, et le pass
--    item_cache ne les mettait pas en cache : residu documente en -019,
--    26 924 items le 2026-07-31, non remesure depuis. La donnee est la, seul le
--    nom lisible manque. D'ou les COALESCE vers l'identifiant brut partout.
--
-- 2. Ne PAS lire les prix depuis T_WC_WIKIDATA_ITEM_PROPERTY (le magasin V1) :
--    il aplatit la valeur principale et les qualificatifs sous la meme
--    propriete, si bien qu'une ceremonie ou un film s'y presente comme une
--    recompense (WIKIDATA-CRAWLER-014). C'est de la que vient la pollution de
--    T_WC_T2S_AWARD (TMDB-MOVIE-PREPROCESS-036).
-- ============================================================================
