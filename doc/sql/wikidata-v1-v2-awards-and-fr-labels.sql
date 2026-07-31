-- ============================================================================
-- V1 -> V2 : trois verifications nees des mesures du 2026-07-30
-- ============================================================================
--
--   A . AWARDS ET NOMINATIONS : V2 ne couvre que 38 % des items de recompense
--       (P166) et 10 % des nominations (P1411) que V1 relie. Hypothese a
--       tester : les sujets porteurs sont muets en V2, c'est-a-dire presents
--       comme ligne d'entite mais sans aucun statement (pass item_cache).
--       Si elle tient, ce trou et celui des 203 263 personnes muettes sont un
--       seul et meme probleme.
--
--   B . LIBELLES FR : la mesure 4C annonce 364 601 libelles perdus sur
--       T2S_ITEM. C'est une BORNE HAUTE, gonflee par deux artefacts connus,
--       qu'il faut deduire avant de decider quoi que ce soit :
--         1. la requete ne cherchait le libelle V2 que dans T_WC_WIKIDATA_ITEM,
--            alors qu'un item V1 devenu entite typee en V2 porte son
--            LABELS_JSON sur MOVIE / SERIE / PERSON / ... ;
--         2. le "libelle FR" de V1 est souvent le libelle ANGLAIS. Le crawler
--            V1 interroge `SERVICE wikibase:label` en "[AUTO_LANGUAGE],mul,en",
--            qui retombe sur l'anglais quand le francais manque. D'ou des
--            "Category:Members of the USSR Academy of Sciences" ranges en FR.
--            Perdre ceux-la ne perd rien.
--
--   C . La section 5A du fichier precedent n'a pas rendu la main. Reprise ici
--       en deux requetes separees, pour que le resultat film s'affiche sans
--       attendre le resultat personne.
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
-- ### A . LE TROU AWARDS VIENT-IL DES SUJETS MUETS ?             [lent]     ###
-- ############################################################################
-- Lecture : si dont_sujet_sans_statement approche liens_non_couverts, alors
-- emettre les claims des personnes promues par la regle 2 rebouche d'un coup
-- le trou des recompenses ET celui des nominations. Sinon, la cause est
-- ailleurs (divergence dump / crawl live) et il faudra chercher.

SELECT '=== A1 . liens P166 / P1411 non couverts par V2, par etat du sujet ===' AS section;

SELECT ID_PROPERTY,
       COUNT(*)                    AS liens_non_couverts,
       SUM(sujet_sans_statement)   AS dont_sujet_sans_statement,
       SUM(sujet_absent_de_v2)     AS dont_sujet_absent_de_toute_table_v2
FROM (
  SELECT ip.ID_PROPERTY,
         NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                     WHERE st.ID_WIKIDATA = ip.ID_WIKIDATA COLLATE utf8mb4_unicode_ci) AS sujet_sans_statement,
         NOT (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON x WHERE x.ID_WIKIDATA = ip.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
           OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE  x WHERE x.ID_WIKIDATA = ip.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
           OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE  x WHERE x.ID_WIKIDATA = ip.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)) AS sujet_absent_de_v2
  FROM   T_WC_WIKIDATA_ITEM_PROPERTY ip
  WHERE  COALESCE(ip.DELETED,0)=0
    AND  ip.ID_PROPERTY IN ('P166','P1411')
    AND  ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = ip.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')
        OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = ip.ID_WIKIDATA AND s.ID_WIKIDATA  <> '')
        OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = ip.ID_WIKIDATA AND pe.ID_WIKIDATA <> ''))
    AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                     JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
                     WHERE st.ID_WIKIDATA = ip.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                       AND st.ID_PROPERTY = ip.ID_PROPERTY
                       AND iv.ID_ITEM     = ip.ID_ITEM COLLATE utf8mb4_unicode_ci)
) t
GROUP BY ID_PROPERTY;

SELECT '=== A2 . combien de sujets distincts sont en cause ? ===' AS section;
-- Si le nombre de sujets est petit devant le nombre de liens, quelques milliers
-- de personnes portent a elles seules des dizaines de milliers de recompenses.

SELECT COUNT(DISTINCT ip.ID_WIKIDATA) AS sujets_distincts_concernes
FROM   T_WC_WIKIDATA_ITEM_PROPERTY ip
WHERE  COALESCE(ip.DELETED,0)=0
  AND  ip.ID_PROPERTY IN ('P166','P1411')
  AND  ( EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = ip.ID_WIKIDATA AND pe.ID_WIKIDATA <> '')
      OR EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = ip.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')
      OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = ip.ID_WIKIDATA AND s.ID_WIKIDATA  <> ''))
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                   WHERE st.ID_WIKIDATA = ip.ID_WIKIDATA COLLATE utf8mb4_unicode_ci);


-- ############################################################################
-- ### B . LES LIBELLES FR : DEGONFLER LES 364 601                [lent]     ###
-- ############################################################################

SELECT '=== B1 . combien de "libelles FR" de V1 sont en fait l anglais ? ===' AS section;
-- Le service de libelles de V1 retombe sur l'anglais quand le francais manque.
-- fr_identique_a_en = des lignes FR qui ne portent aucune traduction.

SELECT COUNT(*)                        AS qid_avec_un_libelle_fr,
       SUM(fr.LABEL = en.LABEL)        AS fr_identique_a_en,
       SUM(fr.LABEL <> en.LABEL)       AS fr_vraiment_traduit
FROM   T_WC_WIKIDATA_ITEM_V1 fr
JOIN   T_WC_WIKIDATA_ITEM_V1 en
       ON en.ID_WIKIDATA = fr.ID_WIKIDATA AND en.LANG = 'en'
WHERE  fr.LANG = 'fr'
  AND  fr.LABEL IS NOT NULL AND fr.LABEL <> ''
  AND  COALESCE(fr.DELETED,0)=0;

SELECT '=== B2 . la vraie perte sur T2S_ITEM, les deux artefacts deduits ===' AS section;
-- fr_v2_partout cherche le libelle francais dans TOUTES les tables V2, pas
-- seulement le cache d'items. Le CASE evite les 6 EXISTS quand le premier suffit.
-- perte_reelle ne compte que les libelles reellement traduits et introuvables
-- ailleurs : c'est le seul chiffre sur lequel decider.

SELECT COUNT(*)                                          AS lignes_t2s_item,
       SUM(fr_v1)                                        AS fr_en_v1,
       SUM(fr_v1_traduit)                                AS fr_en_v1_vraiment_traduit,
       SUM(fr_v2)                                        AS fr_en_v2_toutes_tables,
       SUM(fr_v1 AND NOT fr_v2)                          AS perte_brute,
       SUM(fr_v1_traduit AND NOT fr_v2)                  AS perte_reelle
FROM (
  SELECT
    (fr.LABEL IS NOT NULL AND fr.LABEL <> '')                                  AS fr_v1,
    (fr.LABEL IS NOT NULL AND fr.LABEL <> '' AND fr.LABEL <> COALESCE(en.LABEL,'')) AS fr_v1_traduit,
    CASE WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM i2
                      WHERE i2.ID_WIKIDATA = t2s.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                        AND JSON_UNQUOTE(JSON_EXTRACT(i2.LABELS_JSON,'$.fr')) IS NOT NULL)
         THEN 1
         ELSE (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE     x WHERE x.ID_WIKIDATA = t2s.ID_WIKIDATA COLLATE utf8mb4_unicode_ci AND JSON_UNQUOTE(JSON_EXTRACT(x.LABELS_JSON,'$.fr')) IS NOT NULL)
            OR   EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE     x WHERE x.ID_WIKIDATA = t2s.ID_WIKIDATA COLLATE utf8mb4_unicode_ci AND JSON_UNQUOTE(JSON_EXTRACT(x.LABELS_JSON,'$.fr')) IS NOT NULL)
            OR   EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON    x WHERE x.ID_WIKIDATA = t2s.ID_WIKIDATA COLLATE utf8mb4_unicode_ci AND JSON_UNQUOTE(JSON_EXTRACT(x.LABELS_JSON,'$.fr')) IS NOT NULL)
            OR   EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER x WHERE x.ID_WIKIDATA = t2s.ID_WIKIDATA COLLATE utf8mb4_unicode_ci AND JSON_UNQUOTE(JSON_EXTRACT(x.LABELS_JSON,'$.fr')) IS NOT NULL)
            OR   EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON    x WHERE x.ID_WIKIDATA = t2s.ID_WIKIDATA COLLATE utf8mb4_unicode_ci AND JSON_UNQUOTE(JSON_EXTRACT(x.LABELS_JSON,'$.fr')) IS NOT NULL)
            OR   EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE   x WHERE x.ID_WIKIDATA = t2s.ID_WIKIDATA COLLATE utf8mb4_unicode_ci AND JSON_UNQUOTE(JSON_EXTRACT(x.LABELS_JSON,'$.fr')) IS NOT NULL))
    END AS fr_v2
  FROM T_WC_T2S_ITEM t2s
  LEFT JOIN T_WC_WIKIDATA_ITEM_V1 fr ON fr.ID_WIKIDATA = t2s.ID_WIKIDATA AND fr.LANG = 'fr'
  LEFT JOIN T_WC_WIKIDATA_ITEM_V1 en ON en.ID_WIKIDATA = t2s.ID_WIKIDATA AND en.LANG = 'en'
  WHERE t2s.ID_WIKIDATA IS NOT NULL AND t2s.ID_WIKIDATA <> ''
) t;

SELECT '=== B3 . meme deflation sur les 5 colonnes *_FR (awards, etc.) ===' AS section;

SELECT SUM(fr_v1)                       AS fr_en_v1,
       SUM(fr_v1_traduit)               AS fr_en_v1_vraiment_traduit,
       SUM(fr_v2)                       AS fr_en_v2,
       SUM(fr_v1_traduit AND NOT fr_v2) AS perte_reelle
FROM (
  SELECT (fr.LABEL IS NOT NULL AND fr.LABEL <> '')                                  AS fr_v1,
         (fr.LABEL IS NOT NULL AND fr.LABEL <> '' AND fr.LABEL <> COALESCE(en.LABEL,'')) AS fr_v1_traduit,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM i2
                 WHERE i2.ID_WIKIDATA = q.ID_ITEM COLLATE utf8mb4_unicode_ci
                   AND JSON_UNQUOTE(JSON_EXTRACT(i2.LABELS_JSON,'$.fr')) IS NOT NULL) AS fr_v2
  FROM (
    SELECT DISTINCT ip.ID_ITEM
    FROM   T_WC_WIKIDATA_ITEM_PROPERTY ip
    WHERE  COALESCE(ip.DELETED,0)=0
      AND  ip.ID_PROPERTY IN ('P166','P1411','P463','P108','P54','P509','P1196','P135')
      AND  ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = ip.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')
          OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = ip.ID_WIKIDATA AND s.ID_WIKIDATA  <> '')
          OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = ip.ID_WIKIDATA AND pe.ID_WIKIDATA <> ''))
  ) q
  LEFT JOIN T_WC_WIKIDATA_ITEM_V1 fr ON fr.ID_WIKIDATA = q.ID_ITEM AND fr.LANG = 'fr'
  LEFT JOIN T_WC_WIKIDATA_ITEM_V1 en ON en.ID_WIKIDATA = q.ID_ITEM AND en.LANG = 'en'
) t;


-- ############################################################################
-- ### C . REPRISE DE 5A, EN DEUX REQUETES SEPAREES               [moyen]    ###
-- ############################################################################

SELECT '=== C1 . liens film : recuperables par quelle voie ? ===' AS section;

SELECT COUNT(*) AS liens_v1,
       SUM(voie_a) AS voie_a_propriete_meme_id,
       SUM(voie_b) AS voie_b_jointure_inverse,
       SUM(voie_a OR voie_b) AS recuperable,
       SUM(NOT voie_a AND NOT voie_b) AS irrecuperable
FROM (
  SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = st.ID_STATEMENT
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P4947'
                   AND ev.VALUE_EXTERNAL_ID = CAST(v1.ID_MOVIE AS CHAR) COLLATE utf8mb4_unicode_ci) AS voie_a,
         EXISTS (SELECT 1 FROM T_WC_TMDB_MOVIE t
                 WHERE t.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND t.ID_MOVIE = v1.ID_MOVIE) AS voie_b
  FROM T_WC_WIKIDATA_MOVIE_V1 v1
  WHERE COALESCE(v1.DELETED,0)=0 AND v1.ID_MOVIE IS NOT NULL AND v1.ID_MOVIE <> 0
) t;

SELECT '=== C2 . liens personne : recuperables par quelle voie ? ===' AS section;

SELECT COUNT(*) AS liens_v1,
       SUM(voie_a) AS voie_a_propriete_meme_id,
       SUM(voie_b) AS voie_b_jointure_inverse,
       SUM(voie_a OR voie_b) AS recuperable,
       SUM(NOT voie_a AND NOT voie_b) AS irrecuperable
FROM (
  SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = st.ID_STATEMENT
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P4985'
                   AND ev.VALUE_EXTERNAL_ID = CAST(v1.ID_PERSON AS CHAR) COLLATE utf8mb4_unicode_ci) AS voie_a,
         EXISTS (SELECT 1 FROM T_WC_TMDB_PERSON t
                 WHERE t.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND t.ID_PERSON = v1.ID_PERSON) AS voie_b
  FROM T_WC_WIKIDATA_PERSON_V1 v1
  WHERE COALESCE(v1.DELETED,0)=0 AND v1.ID_PERSON IS NOT NULL AND v1.ID_PERSON <> 0
) t;

-- ############################################################################
-- ### D . AWARDS : PUISQUE CE N'EST PAS LES SUJETS MUETS, QUOI ?  [lent]    ###
-- ############################################################################
-- La section A a refute l'hypothese : sur 98 096 liens P1411 non couverts, 161
-- seulement ont un sujet muet. Les sujets sont donc vivants en V2, avec des
-- statements, mais pas CE lien la. Deux causes restent, et elles appellent des
-- suites opposees :
--   . le sujet a bien la propriete en V2, avec d'AUTRES valeurs -> V1 porte des
--     valeurs que Wikidata a depuis retirees ou deplacees. C'est V1 qui est
--     perime, il n'y a rien a corriger dans V2, et la "perte" est un gain.
--   . le sujet n'a pas du tout la propriete en V2 -> V2 n'a pas ingere ces
--     statements, et la il faut chercher pourquoi.

SELECT '=== D1 . le sujet a-t-il la propriete en V2, avec d autres valeurs ? ===' AS section;

SELECT ID_PROPERTY,
       COUNT(*) AS liens_non_couverts,
       SUM(sujet_a_la_propriete)     AS sujet_a_la_propriete_avec_autres_valeurs,
       SUM(1 - sujet_a_la_propriete) AS sujet_sans_cette_propriete_du_tout
FROM (
  SELECT ip.ID_PROPERTY,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = ip.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = ip.ID_PROPERTY) AS sujet_a_la_propriete
  FROM   T_WC_WIKIDATA_ITEM_PROPERTY ip
  WHERE  COALESCE(ip.DELETED,0)=0
    AND  ip.ID_PROPERTY IN ('P166','P1411')
    AND  ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = ip.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')
        OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = ip.ID_WIKIDATA AND s.ID_WIKIDATA  <> '')
        OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = ip.ID_WIKIDATA AND pe.ID_WIKIDATA <> ''))
    AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                     JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
                     WHERE st.ID_WIKIDATA = ip.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                       AND st.ID_PROPERTY = ip.ID_PROPERTY
                       AND iv.ID_ITEM     = ip.ID_ITEM COLLATE utf8mb4_unicode_ci)
) t
GROUP BY ID_PROPERTY;

SELECT '=== D2 . 20 triplets manquants, nommes, pour verification sur Wikidata ===' AS section;
-- Ces 20 lignes se verifient une par une sur wikidata.org : si le lien n'y est
-- plus, V1 est perime et le dossier est clos.

SELECT ip.ID_WIKIDATA AS sujet_qid,
       (SELECT i.LABEL FROM T_WC_WIKIDATA_ITEM_V1 i
        WHERE i.ID_WIKIDATA = ip.ID_WIKIDATA AND i.LANG='en' LIMIT 1) AS sujet_libelle,
       ip.ID_PROPERTY,
       ip.ID_ITEM AS valeur_qid,
       (SELECT i.LABEL FROM T_WC_WIKIDATA_ITEM_V1 i
        WHERE i.ID_WIKIDATA = ip.ID_ITEM AND i.LANG='en' LIMIT 1) AS valeur_libelle,
       ip.DAT_CREAT AS vu_par_v1_le
FROM   T_WC_WIKIDATA_ITEM_PROPERTY ip
WHERE  COALESCE(ip.DELETED,0)=0
  AND  ip.ID_PROPERTY IN ('P166','P1411')
  AND  EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = ip.ID_WIKIDATA AND pe.ID_WIKIDATA <> '')
  AND  EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
               WHERE st.ID_WIKIDATA = ip.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                   JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
                   WHERE st.ID_WIKIDATA = ip.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                     AND st.ID_PROPERTY = ip.ID_PROPERTY
                     AND iv.ID_ITEM     = ip.ID_ITEM COLLATE utf8mb4_unicode_ci)
LIMIT 20;

SELECT '========== FIN ==========' AS section;
