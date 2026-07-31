-- ============================================================================
-- V1 -> V2 : les trois mesures qui restent avant de decider la decommission
-- ============================================================================
--
--   1 . ENTITES MUETTES : combien de lignes d'entite V2 n'ont AUCUN statement ?
--       Le pass item_cache ecrit la ligne et jamais les claims
--       (wikidata_dump_etl.py:1330-1344). Une personne entree par la regle 2
--       (referencee par un film, sans IMDb propre) a donc un libelle et rien
--       d'autre, la ou PERSON_V1 portait ID_IMDB, BIRTHDAY, DEATHDAY en colonnes.
--       Indice mesure le 2026-07-30 : sur 18 792 personnes communes, 6 794
--       n'avaient ni P31, ni P569, ni P570. Ici on mesure l'ampleur exacte.
--
--   4 . LES 6 COLONNES *_FR DE T2S : se degradent-elles vraiment si on coupe V1 ?
--       Question posee par Philippe. La reponse demande deux mesures, pas une,
--       parce que le preprocessing depend de V1 par DEUX bouts (voir plus bas).
--
--   5 . CLES TMDb : combien de liens ne sont recuperables par AUCUNE des deux
--       voies (propriete Wikidata, ou jointure inverse sur les tables TMDb) ?
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
-- ### 1 . ENTITES SANS AUCUN STATEMENT                           [lent]     ###
-- ############################################################################
-- Lecture : muettes / total. Une entite muette est presente (elle a un libelle,
-- elle repond a une recherche par nom) mais ne porte aucun fait. Pour un
-- conseiller cinema c'est une coquille : ni date de naissance, ni metier, ni
-- identifiant IMDb.
--
-- Cout : un EXISTS indexe par ligne d'entite, sur ~2 M lignes au total. Compter
-- quelques minutes. L'ordre va du plus revelateur (person) au moins.

SELECT '=== 1A . entites muettes, par type ===' AS section;

SELECT 'person' AS entite, COUNT(*) AS lignes_v2,
       SUM(muette) AS sans_aucun_statement,
       ROUND(100 * SUM(muette) / NULLIF(COUNT(*),0), 2) AS pct
FROM ( SELECT NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                          WHERE st.ID_WIKIDATA = e.ID_WIKIDATA) AS muette
       FROM T_WC_WIKIDATA_PERSON e WHERE COALESCE(e.DELETED,0)=0 ) t
UNION ALL
SELECT 'movie', COUNT(*), SUM(muette), ROUND(100 * SUM(muette) / NULLIF(COUNT(*),0), 2)
FROM ( SELECT NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                          WHERE st.ID_WIKIDATA = e.ID_WIKIDATA) AS muette
       FROM T_WC_WIKIDATA_MOVIE e WHERE COALESCE(e.DELETED,0)=0 ) t
UNION ALL
SELECT 'serie', COUNT(*), SUM(muette), ROUND(100 * SUM(muette) / NULLIF(COUNT(*),0), 2)
FROM ( SELECT NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                          WHERE st.ID_WIKIDATA = e.ID_WIKIDATA) AS muette
       FROM T_WC_WIKIDATA_SERIE e WHERE COALESCE(e.DELETED,0)=0 ) t
UNION ALL
SELECT 'character', COUNT(*), SUM(muette), ROUND(100 * SUM(muette) / NULLIF(COUNT(*),0), 2)
FROM ( SELECT NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                          WHERE st.ID_WIKIDATA = e.ID_WIKIDATA) AS muette
       FROM T_WC_WIKIDATA_CHARACTER e WHERE COALESCE(e.DELETED,0)=0 ) t
UNION ALL
SELECT 'season', COUNT(*), SUM(muette), ROUND(100 * SUM(muette) / NULLIF(COUNT(*),0), 2)
FROM ( SELECT NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                          WHERE st.ID_WIKIDATA = e.ID_WIKIDATA) AS muette
       FROM T_WC_WIKIDATA_SEASON e WHERE COALESCE(e.DELETED,0)=0 ) t
UNION ALL
SELECT 'episode', COUNT(*), SUM(muette), ROUND(100 * SUM(muette) / NULLIF(COUNT(*),0), 2)
FROM ( SELECT NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                          WHERE st.ID_WIKIDATA = e.ID_WIKIDATA) AS muette
       FROM T_WC_WIKIDATA_EPISODE e WHERE COALESCE(e.DELETED,0)=0 ) t
UNION ALL
SELECT 'item (cache)', COUNT(*), SUM(muette), ROUND(100 * SUM(muette) / NULLIF(COUNT(*),0), 2)
FROM ( SELECT NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                          WHERE st.ID_WIKIDATA = e.ID_WIKIDATA) AS muette
       FROM T_WC_WIKIDATA_ITEM e WHERE COALESCE(e.DELETED,0)=0 ) t;

SELECT '=== 1B . ce que V1 savait de ces personnes muettes ===' AS section;
-- Le chiffre qui decide : une personne muette en V2 mais dont V1 porte le nom,
-- l'identifiant IMDb et la date de naissance est une perte seche a la bascule.
-- Si ce compte est eleve, il faut emettre les claims des personnes promues par
-- la regle 2 dans le pass item_cache.

SELECT COUNT(*) AS personnes_muettes_en_v2_connues_de_v1,
       SUM(v1.ID_IMDB  IS NOT NULL AND v1.ID_IMDB <> '') AS dont_v1_a_l_imdb,
       SUM(v1.BIRTHDAY IS NOT NULL)                      AS dont_v1_a_la_naissance,
       SUM(v1.DEATHDAY IS NOT NULL)                      AS dont_v1_a_le_deces
FROM   T_WC_WIKIDATA_PERSON_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0
  AND  EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON p
               WHERE p.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                   WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci);

SELECT '=== 1C . ces personnes muettes sont-elles servies par l app ? ===' AS section;
-- Une personne muette qui n'est referencee par aucun film ni serie du read model
-- T2S ne se voit pas. Celles qui y sont rattachees, si.

SELECT COUNT(*) AS muettes_presentes_dans_t2s_person
FROM   T_WC_WIKIDATA_PERSON p
WHERE  COALESCE(p.DELETED,0)=0
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                   WHERE st.ID_WIKIDATA = p.ID_WIKIDATA)
  AND  EXISTS (SELECT 1 FROM T_WC_T2S_PERSON tp
               WHERE tp.ID_WIKIDATA = p.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                 AND tp.ID_WIKIDATA <> '');


-- ############################################################################
-- ### 4 . LES 6 COLONNES *_FR : QUE PERD-ON EXACTEMENT ?                    ###
-- ############################################################################
-- Etabli en lisant tmdb-movie-preprocess.py. Le preprocessing depend de V1 par
-- DEUX bouts, et c'est la raison pour laquelle une seule requete ne peut pas
-- repondre :
--
--   (a) le JEU DE DEPART : quels QID sont une recompense, une nomination, un
--       groupe, un mouvement, une cause de deces. Il vient de
--       T_WC_WIKIDATA_ITEM_PROPERTY (le magasin de statements V1), filtre aux
--       sujets presents dans T2S (tmdb-movie-preprocess.py:2716-2723).
--   (b) le LIBELLE FR de chacun : SELECT LABEL FROM ITEM_V1 WHERE LANG='fr'
--       (tmdb-movie-preprocess.py:2753-2764, meme motif pour les 5 autres).
--
--   colonne T2S              propriete V1     process
--   AWARD_NAME_FR            P166             44
--   NOMINATION_NAME_FR       P1411            (nomination)
--   GROUP_NAME_FR            P463, P108, P54  (group)
--   DEATH_NAME_FR            P509, P1196      (death)
--   MOVEMENT_NAME_FR         (liste custom + wikidata)
--   ITEM_LABEL_FR            toute la table   40 (T2S_ITEM)
--
-- Couper V1 casse donc potentiellement les deux. Les deux requetes ci-dessous
-- mesurent chaque bout separement.

SELECT '=== 4A . bout (a) : V2 sait-il reconstruire le jeu de depart ? ===  [lent]' AS section;
-- Pour chaque propriete, les triplets (sujet, propriete, item) que V1 connait et
-- que V2 ne porte pas dans ses statements. Si non_couverts est proche de 0, V2
-- peut nourrir ces 5 processus sans V1.

SELECT ip.ID_PROPERTY,
       COUNT(DISTINCT ip.ID_ITEM) AS items_distincts_v1,
       COUNT(DISTINCT CASE WHEN EXISTS (
              SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
              JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
              WHERE st.ID_WIKIDATA = ip.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                AND st.ID_PROPERTY = ip.ID_PROPERTY
                AND iv.ID_ITEM     = ip.ID_ITEM COLLATE utf8mb4_unicode_ci)
            THEN ip.ID_ITEM END) AS items_couverts_par_v2
FROM   T_WC_WIKIDATA_ITEM_PROPERTY ip
WHERE  COALESCE(ip.DELETED,0)=0
  AND  ip.ID_PROPERTY IN ('P166','P1411','P463','P108','P54','P509','P1196','P135')
  AND  ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = ip.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')
      OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = ip.ID_WIKIDATA AND s.ID_WIKIDATA  <> '')
      OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = ip.ID_WIKIDATA AND pe.ID_WIKIDATA <> ''))
GROUP BY ip.ID_PROPERTY
ORDER BY items_distincts_v1 DESC;

SELECT '=== 4B . bout (b) : le libelle FR survit-il a la bascule ? ===  [moyen]' AS section;
-- Sur exactement les memes QID (ceux qui alimentent les 5 colonnes), on compare
-- le libelle FR de V1 a celui de V2. perdu_a_la_bascule est LE chiffre demande :
-- le nombre de recompenses / nominations / groupes / causes de deces qui
-- perdraient leur nom francais.

SELECT SUM(fr_en_v1) AS libelle_fr_en_v1,
       SUM(fr_en_v2) AS libelle_fr_en_v2,
       SUM(fr_en_v1 AND NOT fr_en_v2) AS perdu_a_la_bascule,
       SUM(NOT fr_en_v1 AND fr_en_v2) AS gagne_a_la_bascule
FROM (
  SELECT q.ID_ITEM,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM_V1 i1
                 WHERE i1.ID_WIKIDATA = q.ID_ITEM AND i1.LANG='fr'
                   AND i1.LABEL IS NOT NULL AND i1.LABEL <> '') AS fr_en_v1,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM i2
                 WHERE i2.ID_WIKIDATA = q.ID_ITEM COLLATE utf8mb4_unicode_ci
                   AND JSON_UNQUOTE(JSON_EXTRACT(i2.LABELS_JSON,'$.fr')) IS NOT NULL) AS fr_en_v2
  FROM (
    SELECT DISTINCT ip.ID_ITEM
    FROM   T_WC_WIKIDATA_ITEM_PROPERTY ip
    WHERE  COALESCE(ip.DELETED,0)=0
      AND  ip.ID_PROPERTY IN ('P166','P1411','P463','P108','P54','P509','P1196','P135')
      AND  ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = ip.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')
          OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = ip.ID_WIKIDATA AND s.ID_WIKIDATA  <> '')
          OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = ip.ID_WIKIDATA AND pe.ID_WIKIDATA <> ''))
  ) q
) t;

SELECT '=== 4C . la 6e colonne : ITEM_LABEL_FR, sur toute la table T2S_ITEM ===  [lent]' AS section;
-- T2S_ITEM est reconstruite entierement depuis ITEM_V1 (process 40). Ici on
-- mesure sur les QID reellement presents dans T2S_ITEM aujourd'hui, donc sur ce
-- que l'application sert vraiment, et non sur tout ITEM_V1.

SELECT COUNT(*) AS lignes_t2s_item,
       SUM(fr_en_v1) AS libelle_fr_en_v1,
       SUM(fr_en_v2) AS libelle_fr_en_v2,
       SUM(fr_en_v1 AND NOT fr_en_v2) AS perdu_a_la_bascule
FROM (
  SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM_V1 i1
                 WHERE i1.ID_WIKIDATA = t2s.ID_WIKIDATA AND i1.LANG='fr'
                   AND i1.LABEL IS NOT NULL AND i1.LABEL <> '') AS fr_en_v1,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM i2
                 WHERE i2.ID_WIKIDATA = t2s.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND JSON_UNQUOTE(JSON_EXTRACT(i2.LABELS_JSON,'$.fr')) IS NOT NULL) AS fr_en_v2
  FROM T_WC_T2S_ITEM t2s
  WHERE t2s.ID_WIKIDATA IS NOT NULL AND t2s.ID_WIKIDATA <> ''
) t;

SELECT '=== 4D . echantillon de 25 libelles FR qui disparaitraient ===' AS section;

SELECT i1.ID_WIKIDATA, i1.LABEL AS libelle_fr_v1, i1.INSTANCE_OF
FROM   T_WC_WIKIDATA_ITEM_V1 i1
WHERE  i1.LANG='fr' AND i1.LABEL IS NOT NULL AND i1.LABEL <> ''
  AND  COALESCE(i1.DELETED,0)=0
  AND  EXISTS (SELECT 1 FROM T_WC_T2S_ITEM t2s WHERE t2s.ID_WIKIDATA = i1.ID_WIKIDATA)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM i2
                   WHERE i2.ID_WIKIDATA = i1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                     AND JSON_UNQUOTE(JSON_EXTRACT(i2.LABELS_JSON,'$.fr')) IS NOT NULL)
LIMIT 25;


-- ############################################################################
-- ### 5 . CLES TMDb : les liens recuperables par AUCUNE voie      [moyen]   ###
-- ############################################################################
-- Voie A, la propriete Wikidata : P4947 (id TMDb film), P4985 (id TMDb personne).
--   On ne se contente pas de constater qu'un statement existe, on verifie que la
--   valeur est BIEN LE MEME identifiant que celui que V1 portait en dur.
-- Voie B, la jointure inverse : T_WC_TMDB_*.ID_WIKIDATA pointe en retour.
-- Le chiffre utile est la derniere colonne : ni l'une ni l'autre.

SELECT '=== 5A . liens film et personne, par voie de recuperation ===' AS section;

SELECT 'movie' AS entite,
       COUNT(*) AS liens_v1,
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
) t
UNION ALL
SELECT 'person', COUNT(*), SUM(voie_a), SUM(voie_b),
       SUM(voie_a OR voie_b), SUM(NOT voie_a AND NOT voie_b)
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

SELECT '=== 5B . echantillon de 20 liens irrecuperables (films) ===' AS section;

SELECT v1.ID_WIKIDATA, v1.ID_MOVIE, v1.TITLE, v1.DAT_RELEASE
FROM   T_WC_WIKIDATA_MOVIE_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0 AND v1.ID_MOVIE IS NOT NULL AND v1.ID_MOVIE <> 0
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                   JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE ev ON ev.ID_STATEMENT = st.ID_STATEMENT
                   WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                     AND st.ID_PROPERTY = 'P4947'
                     AND ev.VALUE_EXTERNAL_ID = CAST(v1.ID_MOVIE AS CHAR) COLLATE utf8mb4_unicode_ci)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_TMDB_MOVIE t
                   WHERE t.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                     AND t.ID_MOVIE = v1.ID_MOVIE)
LIMIT 20;

SELECT '========== FIN ==========' AS section;
