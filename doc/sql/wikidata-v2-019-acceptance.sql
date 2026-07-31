-- ============================================================================
-- WIKIDATA-CRAWLER-019 : recette apres rechargement des qualificatifs
-- ============================================================================
--
-- A lancer une fois 11_bulk_load_qualifiers_only.sql passe. Chaque section porte
-- le chiffre d'AVANT, mesure le 2026-07-30 sur la base effondree, pour que la
-- comparaison soit immediate.
--
-- LECTURE SEULE. Executer avec --force -t.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET SESSION max_statement_time = 0;

SELECT '=== 1 . volumetrie : la table stocke-t-elle enfin des occurrences ? ===' AS section;
-- AVANT : 1 389 536 lignes.  ATTENDU : 5 577 076.

SELECT COUNT(*) AS qualificatifs, '5 577 076 attendus' AS repere
FROM   T_WC_WIKIDATA_STATEMENT_QUALIFIER;

SELECT '=== 2 . LE test : lignes vs valeurs distinctes ===' AS section;
-- AVANT, l'egalite etait EXACTE, ce qui prouvait le stockage par valeur :
--   P453 42 572 / 42 572   |   P1686 30 811 / 30 811   |   P155 34 102 / 34 102
-- ATTENDU : lignes NETTEMENT superieur a valeurs_distinctes.

SELECT q.ID_QUALIFIER_PROPERTY,
       COUNT(*)                   AS lignes,
       COUNT(DISTINCT qi.ID_ITEM) AS valeurs_distinctes,
       ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT qi.ID_ITEM), 0), 1) AS occurrences_par_valeur
FROM   T_WC_WIKIDATA_STATEMENT_QUALIFIER q
JOIN   T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qi ON qi.ID_STATEMENT_QUALIFIER = q.ID_STATEMENT_QUALIFIER
WHERE  q.ID_QUALIFIER_PROPERTY IN ('P453','P1686','P155')
GROUP BY q.ID_QUALIFIER_PROPERTY;

SELECT '=== 3 . awards : l epic est-il debloque ? ===' AS section;
-- AVANT : 252 325 statements P166, dont 6 055 avec annee (2,4 %) et 12 625 avec oeuvre (5 %).

SELECT COUNT(*) AS statements_p166,
       SUM(a_p585)  AS avec_annee_p585,
       ROUND(100 * SUM(a_p585) / COUNT(*), 1)  AS pct_annee,
       SUM(a_p1686) AS avec_oeuvre_p1686,
       ROUND(100 * SUM(a_p1686) / COUNT(*), 1) AS pct_oeuvre
FROM (
  SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER q
                 WHERE q.ID_STATEMENT = st.ID_STATEMENT AND q.ID_QUALIFIER_PROPERTY='P585')  AS a_p585,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER q
                 WHERE q.ID_STATEMENT = st.ID_STATEMENT AND q.ID_QUALIFIER_PROPERTY='P1686') AS a_p1686
  FROM T_WC_WIKIDATA_STATEMENT st
  WHERE st.ID_PROPERTY = 'P166'
) t;

SELECT '=== 4 . episodes : les numeros sont-ils revenus ? ===' AS section;
-- AVANT : 241 663 statements P179, dont 2 635 avec P1545 (1,1 %).

SELECT COUNT(*) AS statements_p179_p4908,
       SUM(a_p1545) AS avec_numero_p1545,
       ROUND(100 * SUM(a_p1545) / COUNT(*), 1) AS pct
FROM (
  SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER q
                 WHERE q.ID_STATEMENT = st.ID_STATEMENT AND q.ID_QUALIFIER_PROPERTY='P1545') AS a_p1545
  FROM T_WC_WIKIDATA_STATEMENT st
  WHERE st.ID_PROPERTY IN ('P179','P4908')
) t;

SELECT '=== 5 . temoin episode : Q100268982, que V1 numerotait 1 ===' AS section;
-- AVANT : le statement P179 etait la, sans aucun qualificatif.
-- ATTENDU : P1545 = 1, exactement ce que porte wikidata.org.

SELECT st.ID_PROPERTY, iv.ID_ITEM AS valeur,
       q.ID_QUALIFIER_PROPERTY AS qualificatif, qs.VALUE_STRING AS valeur_qualificatif
FROM   T_WC_WIKIDATA_STATEMENT st
LEFT JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q ON q.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_STRING_VALUE qs ON qs.ID_STATEMENT_QUALIFIER = q.ID_STATEMENT_QUALIFIER
WHERE  st.ID_WIKIDATA = 'Q100268982' AND st.ID_PROPERTY IN ('P179','P4908');

SELECT '=== 6 . LA requete de l epic Awards, enfin possible ===' AS section;
-- Temoin : Cord Jefferson (Q100146356). Wikidata porte P166 -> Oscar du meilleur
-- scenario adapte, qualifie par l annee, l oeuvre et la ceremonie. V1 aplatissait
-- les trois dans une seule colonne ; on les lit ici separement.
--
-- Note : le libelle de la ceremonie peut etre vide. C'est le residu connu du
-- second defaut de -019 (les items qui n'apparaissent QUE comme valeur de
-- qualificatif n'entrent pas dans le cache de libelles). Il se comblera au
-- prochain run complet, la donnee elle-meme est bien la.

SELECT COALESCE(lab_prix.LABEL_EN, iv.ID_ITEM)        AS recompense,
       qt.YEAR_VALUE                                  AS annee,
       COALESCE(lab_oeuvre.LABEL_EN, qi_work.ID_ITEM) AS pour_l_oeuvre,
       COALESCE(lab_ceremonie.LABEL_EN, qi_cer.ID_ITEM) AS ceremonie
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM lab_prix ON lab_prix.ID_WIKIDATA = iv.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q_time ON q_time.ID_STATEMENT = st.ID_STATEMENT
      AND q_time.ID_QUALIFIER_PROPERTY = 'P585'
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_TIME_VALUE qt ON qt.ID_STATEMENT_QUALIFIER = q_time.ID_STATEMENT_QUALIFIER
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q_work ON q_work.ID_STATEMENT = st.ID_STATEMENT
      AND q_work.ID_QUALIFIER_PROPERTY = 'P1686'
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qi_work ON qi_work.ID_STATEMENT_QUALIFIER = q_work.ID_STATEMENT_QUALIFIER
LEFT JOIN T_WC_WIKIDATA_MOVIE lab_oeuvre ON lab_oeuvre.ID_WIKIDATA = qi_work.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q_cer ON q_cer.ID_STATEMENT = st.ID_STATEMENT
      AND q_cer.ID_QUALIFIER_PROPERTY = 'P805'
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qi_cer ON qi_cer.ID_STATEMENT_QUALIFIER = q_cer.ID_STATEMENT_QUALIFIER
LEFT JOIN T_WC_WIKIDATA_ITEM lab_ceremonie ON lab_ceremonie.ID_WIKIDATA = qi_cer.ID_ITEM
WHERE  st.ID_WIKIDATA = 'Q100146356' AND st.ID_PROPERTY = 'P166';

SELECT '=== 7 . la meme chose a l echelle : 15 prix recents, dates et oeuvres ===' AS section;
-- Si cette requete rend des lignes completes, l epic a son socle : le prix, la
-- personne, l annee et l oeuvre, chacun dans sa colonne.

SELECT p.LABEL_EN                              AS laureat,
       COALESCE(lab_prix.LABEL_EN, iv.ID_ITEM) AS recompense,
       qt.YEAR_VALUE                           AS annee,
       COALESCE(m.LABEL_EN, qi_work.ID_ITEM)   AS pour_l_oeuvre
FROM   T_WC_WIKIDATA_STATEMENT st
JOIN   T_WC_WIKIDATA_PERSON p ON p.ID_WIKIDATA = st.ID_WIKIDATA
JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
JOIN   T_WC_WIKIDATA_STATEMENT_QUALIFIER q_time ON q_time.ID_STATEMENT = st.ID_STATEMENT
      AND q_time.ID_QUALIFIER_PROPERTY = 'P585'
JOIN   T_WC_WIKIDATA_QUALIFIER_TIME_VALUE qt ON qt.ID_STATEMENT_QUALIFIER = q_time.ID_STATEMENT_QUALIFIER
LEFT JOIN T_WC_WIKIDATA_ITEM lab_prix ON lab_prix.ID_WIKIDATA = iv.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q_work ON q_work.ID_STATEMENT = st.ID_STATEMENT
      AND q_work.ID_QUALIFIER_PROPERTY = 'P1686'
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qi_work ON qi_work.ID_STATEMENT_QUALIFIER = q_work.ID_STATEMENT_QUALIFIER
LEFT JOIN T_WC_WIKIDATA_MOVIE m ON m.ID_WIKIDATA = qi_work.ID_ITEM
WHERE  st.ID_PROPERTY = 'P166'
  AND  qt.YEAR_VALUE BETWEEN 2020 AND 2025
LIMIT 15;

SELECT '========== FIN ==========' AS section;
