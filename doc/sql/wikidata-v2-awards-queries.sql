-- ============================================================================
-- Les prix en Wikidata V2 : les requetes de reference
-- ============================================================================
--
-- Repond aux questions du backlog qui restaient sans reponse tant que les
-- qualificatifs etaient effondres (WIKIDATA-CRAWLER-019, corrige le 2026-07-31) :
--
--   Q1  tous les prix d'une personne, avec annee et oeuvre   (-006, eval eid 2313)
--   Q2  combien de prix d'une famille donnee (les Oscars)     (eval eid 2313, 2349)
--   Q3  les personnes qui en ont plusieurs                    (eval eid 2301)
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
-- LA HIERARCHIE, verifiee sur wikidata.org le 2026-07-31. La question ouverte du
-- backlog etait : « comment regrouper toutes les categories d'Academy Award ?
-- Like '%Academy award%' ? Custom list ? ». Reponse : NI L'UN NI L'AUTRE. Chaque
-- categorie declare son prix parent par P31.
--   Q103618 « Academy Award for Best Actress »          P31 -> Q19020
--   Q107258 « Academy Award for Best Adapted Screenplay » P31 -> Q19020
-- La direction inverse existe (Q19020 P527 « has part ») mais elle est
-- INCOMPLETE : 18 valeurs seulement, la ou les Oscars comptent bien plus de
-- categories. Filtrer par P31 depuis la categorie, jamais par P527 depuis le
-- parent, et surtout jamais par le libelle.
--
-- Prix parents utiles : Q19020 Academy Awards, Q174389 Cesar Awards.
--
-- LECTURE SEULE. Executer avec --force -t.
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
       COALESCE(cer.LABEL_EN, qi_cer.ID_ITEM)      AS ceremonie
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
LEFT JOIN T_WC_WIKIDATA_ITEM cer ON cer.ID_WIKIDATA = qi_cer.ID_ITEM
WHERE  st.ID_WIKIDATA = 'Q56016'
  AND  st.ID_PROPERTY = 'P166'
ORDER BY qt.YEAR_VALUE;


-- ############################################################################
-- ### Q2 . Combien de prix d'une famille donnee ?                           ###
-- ############################################################################
-- « How many Oscars did Hepburn receive? » (eid 2313)
-- « How many academy awards did Walt Disney receive? » (eid 2349, Q8704)
--
-- La jointure hierarchique tient en une ligne : la categorie gagnee doit avoir
-- P31 -> Q19020. Aucun filtre sur le libelle, aucune liste a maintenir.

SELECT '=== Q2 . nombre d Oscars, par la hierarchie P31 -> Q19020 ===' AS section;

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
GROUP BY p.LABEL_EN;

SELECT '=== Q2-bis . le detail, categorie par categorie ===' AS section;
-- Utile pour verifier a la main : Hepburn doit sortir 4 fois « Best Actress ».

SELECT p.LABEL_EN AS personne,
       COALESCE(prix.LABEL_EN, iv.ID_ITEM) AS categorie,
       qt.YEAR_VALUE AS annee
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_PERSON p ON p.ID_WIKIDATA = st.ID_WIKIDATA
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM prix ON prix.ID_WIKIDATA = iv.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q_an ON q_an.ID_STATEMENT = st.ID_STATEMENT
       AND q_an.ID_QUALIFIER_PROPERTY = 'P585'
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_TIME_VALUE qt ON qt.ID_STATEMENT_QUALIFIER = q_an.ID_STATEMENT_QUALIFIER
WHERE  st.ID_PROPERTY = 'P166'
  AND  st.ID_WIKIDATA IN ('Q56016','Q8704')
  AND  EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT parent
               JOIN T_WC_WIKIDATA_ITEM_VALUE piv ON piv.ID_STATEMENT = parent.ID_STATEMENT
               WHERE parent.ID_WIKIDATA = iv.ID_ITEM
                 AND parent.ID_PROPERTY = 'P31'
                 AND piv.ID_ITEM        = 'Q19020')
ORDER BY personne, annee;


-- ############################################################################
-- ### Q3 . Les actrices qui ont plusieurs Oscars                            ###
-- ############################################################################
-- « Actresses with several Academy Awards » (eid 2301).
-- Le genre vient de P21 -> Q6581072 (femme). On aurait pu filtrer sur les seules
-- categories d'actrice, mais la question porte sur toutes categories confondues.

SELECT '=== Q3 . actrices ayant plusieurs Oscars ===' AS section;

SELECT p.LABEL_EN AS actrice,
       COUNT(*)   AS nb_oscars
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_PERSON p ON p.ID_WIKIDATA = st.ID_WIKIDATA
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
WHERE  st.ID_PROPERTY = 'P166'
  AND  EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT parent
               JOIN T_WC_WIKIDATA_ITEM_VALUE piv ON piv.ID_STATEMENT = parent.ID_STATEMENT
               WHERE parent.ID_WIKIDATA = iv.ID_ITEM
                 AND parent.ID_PROPERTY = 'P31'
                 AND piv.ID_ITEM        = 'Q19020')
  AND  EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT g
               JOIN T_WC_WIKIDATA_ITEM_VALUE giv ON giv.ID_STATEMENT = g.ID_STATEMENT
               WHERE g.ID_WIKIDATA = st.ID_WIKIDATA
                 AND g.ID_PROPERTY = 'P21'
                 AND giv.ID_ITEM   = 'Q6581072')
GROUP BY p.LABEL_EN
HAVING COUNT(*) >= 2
ORDER BY nb_oscars DESC, actrice
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

SELECT '========== FIN ==========' AS section;
-- ============================================================================
-- DEUX PIEGES A CONNAITRE
--
-- 1. Le libelle de la ceremonie (P805) et de certaines categories peut etre
--    VIDE. Ces items n'apparaissent qu'en valeur de qualificatif, et le pass
--    item_cache ne les met donc pas en cache : residu documente en -019,
--    26 924 items, comble au prochain run complet. La donnee est la, seul le
--    nom lisible manque. D'ou les COALESCE vers l'identifiant brut partout.
--
-- 2. Ne PAS lire les prix depuis T_WC_WIKIDATA_ITEM_PROPERTY (le magasin V1) :
--    il aplatit la valeur principale et les qualificatifs sous la meme
--    propriete, si bien qu'une ceremonie ou un film s'y presente comme une
--    recompense (WIKIDATA-CRAWLER-014). C'est de la que vient la pollution de
--    T_WC_T2S_AWARD (TMDB-MOVIE-PREPROCESS-036).
-- ============================================================================
