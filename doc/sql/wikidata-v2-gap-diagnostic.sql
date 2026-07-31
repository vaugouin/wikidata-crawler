-- ============================================================================
-- V2 : deux diagnostics ouverts apres la validation V1 -> V2 du 2026-07-30
-- ============================================================================
--
--   A . ITEMS : 197 401 QID que V1 connait et que V2 n'a nulle part. D'ou vient
--       ce gap ? Trois causes possibles, ces requetes les separent.
--   B . QUALIFICATIFS : preuve (ou refutation) de l'effondrement par hash.
--       Hypothese : `derive_qualifier_identity` (wikidata_dump_etl.py:646-655)
--       prend le `hash` du snak Wikidata comme identite. Or ce hash porte sur
--       le CONTENU du snak (propriete + valeur), pas sur son occurrence. Deux
--       statements portant "P1545 = 1" ont donc le meme hash, donc le meme
--       ID_STATEMENT_QUALIFIER, et la UNIQUE KEY sur QUALIFIER_HASH n'en garde
--       qu'un seul. Si c'est vrai, le nombre de lignes par propriete doit etre
--       EGAL au nombre de valeurs distinctes.
--
-- LECTURE SEULE. Executer avec --force -t.
-- ============================================================================

-- La connexion doit parler la meme collation que les tables (toutes en
-- utf8mb4_unicode_ci depuis la standardisation). Sans cette ligne, la collation
-- de connexion reste utf8mb4_general_ci et toute valeur FABRIQUEE par une
-- fonction (CAST, CONVERT, CONCAT sur un nombre) porte general_ci : la comparer
-- a une colonne leve l'erreur 1267 "Illegal mix of collations".
SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET SESSION max_statement_time = 0;

-- ############################################################################
-- ### A . D'OU VIENT LE GAP ITEMS ?                                         ###
-- ############################################################################

SELECT '=== A1 . les items absents de V2 sont-ils references par V2 ? ===  [lent]' AS section;
-- Trois seaux, trois causes differentes :
--   valeur_principale : V2 reference l'item dans un statement mais ne l'a pas
--     mis en cache -> defaut du pass item_cache, a corriger dans l'ETL.
--   qualificatif_seul : l'item n'apparait que comme valeur de qualificatif.
--     Cause connue et lisible dans le code : `emit_claims_for_in_scope_entity`
--     n'alimente `referenced_item_ids` que depuis les valeurs principales
--     (wikidata_dump_etl.py:1456), jamais depuis la boucle des qualificatifs.
--   jamais_reference : V2 n'a aucun besoin de cet item. Ce n'est pas une perte,
--     c'est une difference de perimetre (V1 gardait tout ce qu'il a croise).

SELECT SUM(princ) AS valeur_principale,
       SUM(CASE WHEN princ = 0 AND qual = 1 THEN 1 ELSE 0 END) AS qualificatif_seul,
       SUM(CASE WHEN princ = 0 AND qual = 0 THEN 1 ELSE 0 END) AS jamais_reference
FROM (
  SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM_VALUE iv
                 WHERE iv.ID_ITEM = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci) AS princ,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qv
                 WHERE qv.ID_ITEM = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci) AS qual
  FROM (SELECT DISTINCT ID_WIKIDATA FROM T_WC_WIKIDATA_ITEM_V1 WHERE COALESCE(DELETED,0)=0) v1
  WHERE NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM      x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE   x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t;

SELECT '=== A2 . cote V1 : par quelle propriete ces items sont-ils arrives ? ===  [lent]' AS section;
-- V1 remplit ITEM_V1 (scope 109) depuis tout ID_ITEM vu dans ITEM_PROPERTY.
-- Le palmares des proprietes porteuses dit de quelle nature sont ces items :
-- genres (P136), sujets (P921), categories Wikimedia (P910)...

SELECT ip.ID_PROPERTY, COUNT(DISTINCT ip.ID_ITEM) AS nb_items_absents_de_v2
FROM   T_WC_WIKIDATA_ITEM_PROPERTY ip
WHERE  COALESCE(ip.DELETED,0)=0
  AND  ip.ID_ITEM LIKE 'Q%'
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM   x WHERE x.ID_WIKIDATA = ip.ID_ITEM COLLATE utf8mb4_unicode_ci)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE  x WHERE x.ID_WIKIDATA = ip.ID_ITEM COLLATE utf8mb4_unicode_ci)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON x WHERE x.ID_WIKIDATA = ip.ID_ITEM COLLATE utf8mb4_unicode_ci)
GROUP BY ip.ID_PROPERTY
ORDER BY nb_items_absents_de_v2 DESC
LIMIT 25;

SELECT '=== A3 . les sujets qui les referencent sont-ils, eux, dans V2 ? ===  [lent]' AS section;
-- Si les sujets porteurs sont absents de V2, alors le gap items n'est qu'une
-- consequence du gap entites (surtout des 49 107 personnes) et se refermera de
-- lui-meme si on elargit le perimetre. Sinon, c'est bien l'item_cache qui fuit.

SELECT SUM(sujet_dans_v2) AS sujets_presents_en_v2,
       SUM(1-sujet_dans_v2) AS sujets_absents_de_v2
FROM (
  SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE  x WHERE x.ID_WIKIDATA = ip.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
      OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE  x WHERE x.ID_WIKIDATA = ip.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
      OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON x WHERE x.ID_WIKIDATA = ip.ID_WIKIDATA COLLATE utf8mb4_unicode_ci) AS sujet_dans_v2
  FROM (SELECT ID_WIKIDATA, ID_ITEM FROM T_WC_WIKIDATA_ITEM_PROPERTY
        WHERE COALESCE(DELETED,0)=0 AND ID_ITEM LIKE 'Q%' LIMIT 200000) ip
  WHERE NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM   x WHERE x.ID_WIKIDATA = ip.ID_ITEM COLLATE utf8mb4_unicode_ci)
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE  x WHERE x.ID_WIKIDATA = ip.ID_ITEM COLLATE utf8mb4_unicode_ci)
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON x WHERE x.ID_WIKIDATA = ip.ID_ITEM COLLATE utf8mb4_unicode_ci)
) t;


-- ############################################################################
-- ### B . QUALIFICATIFS : l'effondrement par hash                           ###
-- ############################################################################

SELECT '=== B1 . LA requete : lignes vs valeurs distinctes ===  [moyen]' AS section;
-- Si lignes = valeurs_distinctes A L'UNITE PRES, c'est demontre : la table ne
-- stocke pas les occurrences mais les valeurs, une seule fois chacune.
-- P1545 = numero d'ordre (les memes "1", "2", "3" partout), P1810 = nom cite
-- (texte libre, donc quasi toujours distinct : sert de temoin negatif).

SELECT q.ID_QUALIFIER_PROPERTY,
       COUNT(*)                          AS lignes,
       COUNT(DISTINCT qs.VALUE_STRING)   AS valeurs_distinctes
FROM   T_WC_WIKIDATA_STATEMENT_QUALIFIER q
JOIN   T_WC_WIKIDATA_QUALIFIER_STRING_VALUE qs ON qs.ID_STATEMENT_QUALIFIER = q.ID_STATEMENT_QUALIFIER
WHERE  q.ID_QUALIFIER_PROPERTY IN ('P1545','P1810','P4633')
GROUP BY q.ID_QUALIFIER_PROPERTY;

SELECT '=== B2 . meme test sur les qualificatifs a valeur item ===' AS section;
-- P453 (role joue) : le meme personnage revient sur des milliers de statements
-- de casting. Si lignes = valeurs distinctes, meme diagnostic.

SELECT q.ID_QUALIFIER_PROPERTY,
       COUNT(*)                       AS lignes,
       COUNT(DISTINCT qi.ID_ITEM)     AS valeurs_distinctes
FROM   T_WC_WIKIDATA_STATEMENT_QUALIFIER q
JOIN   T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qi ON qi.ID_STATEMENT_QUALIFIER = q.ID_STATEMENT_QUALIFIER
WHERE  q.ID_QUALIFIER_PROPERTY IN ('P453','P1686','P155')
GROUP BY q.ID_QUALIFIER_PROPERTY;

SELECT '=== B3 . un qualificatif est-il partage par plusieurs statements ? ===' AS section;
-- Corollaire : si l'identite est le contenu, chaque ligne de qualificatif ne
-- peut etre rattachee qu'a UN statement (le dernier charge), les autres sont
-- perdus. On regarde donc combien de statements portent le meme P1545.

SELECT COUNT(*) AS statements_p179_total,
       SUM(a_qualif) AS avec_un_qualificatif_quelconque,
       SUM(a_p1545)  AS avec_p1545
FROM (
  SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER q
                 WHERE q.ID_STATEMENT = st.ID_STATEMENT) AS a_qualif,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER q
                 WHERE q.ID_STATEMENT = st.ID_STATEMENT AND q.ID_QUALIFIER_PROPERTY='P1545') AS a_p1545
  FROM T_WC_WIKIDATA_STATEMENT st
  WHERE st.ID_PROPERTY = 'P179'
) t;

SELECT '=== B4 . impact sur les awards (epic -006) ===' AS section;
-- La lecture des awards prevue par WIKIDATA-CRAWLER-006 repose sur P166 joint a
-- ses qualificatifs P585 (annee) et P1686 (oeuvre). Si l'effondrement est reel,
-- cette lecture est aujourd'hui impossible : la date de remise est le cas type
-- d'une valeur qui se repete.

SELECT COUNT(*) AS statements_p166,
       SUM(a_p585)  AS avec_annee_p585,
       SUM(a_p1686) AS avec_oeuvre_p1686
FROM (
  SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER q
                 WHERE q.ID_STATEMENT = st.ID_STATEMENT AND q.ID_QUALIFIER_PROPERTY='P585') AS a_p585,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER q
                 WHERE q.ID_STATEMENT = st.ID_STATEMENT AND q.ID_QUALIFIER_PROPERTY='P1686') AS a_p1686
  FROM T_WC_WIKIDATA_STATEMENT st
  WHERE st.ID_PROPERTY = 'P166'
) t;

SELECT '========== FIN ==========' AS section;
