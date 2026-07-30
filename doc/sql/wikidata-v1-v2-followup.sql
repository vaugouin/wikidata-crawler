-- ============================================================================
-- Wikidata V1 -> V2 : COMPLEMENTS apres le run du 2026-07-30
-- ============================================================================
--
-- Le fichier principal (wikidata-v1-v2-validation.sql) a tourne en entier sur le
-- batch wikidata_full_20260726_1300. Il a repondu a la question principale et
-- laisse cinq points ouverts. Ce fichier ne traite QUE ces cinq points.
--
--   A . ITEM : ou sont les 280 925 QID absents ? (la section 1B du fichier
--       principal omettait item : c'est un oubli de ma part, pas un resultat)
--   B . ID_CRITERION : le test IS NOT NULL comptait les zeros, le resultat
--       "0 retrouve sur 19 924" etait un artefact. Version corrigee ici.
--   C . QUALIFICATIFS : V2 a la relation episode -> serie (P179 / P4908) mais
--       pas le numero d'episode (P1545) dans 99,6 % des cas. Deux causes
--       possibles, ces requetes tranchent.
--   D . DEATHDAY : 36 % des dates de deces V1 n'ont pas de P570 en V2. Meme
--       logique : soit V1 stocke du bruit, soit V2 a perdu la propriete.
--   E . PERSON avec IMDb absentes : l'echantillon montrait des ID_IMDB tronques
--       (`https://ww`, `//www.imdb`). Quelle part des 1 641 est du dechet ?
--
-- LECTURE SEULE. Executer avec --force -t comme le fichier principal.
-- ============================================================================

SET SESSION max_statement_time = 0;

-- ############################################################################
-- ### A . ITEM : localisation des QID V1 absents de ITEM V2      [lent]     ###
-- ############################################################################
-- V2 ITEM (629 439) < V1 ITEM (694 922) et 280 925 QID V1 n'y sont pas. C'est
-- attendu en partie : V2 ITEM est un cache d'items REFERENCES, pas un fourre-tout,
-- et beaucoup d'items V1 sont devenus des entites typees en V2 (movie, serie,
-- person...). Cette requete dit quelle part est reclassee et quelle part est
-- vraiment perdue.

SELECT '=== A1 . item : ou sont les absents ? ===' AS section;

SELECT SUM(pres) AS ailleurs_en_v2, SUM(1-pres) AS absents_partout
FROM (
  SELECT (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE   x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
       OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)) AS pres
  FROM (SELECT DISTINCT ID_WIKIDATA FROM T_WC_WIKIDATA_ITEM_V1 WHERE COALESCE(DELETED,0)=0) v1
  WHERE NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM i2
                    WHERE i2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t;

SELECT '=== A2 . item : les vraiment absents portent-ils un libelle FR ? ===' AS section;
-- Enjeu localisation : un item absent de partout et qui avait un libelle FR en V1
-- est une perte seche pour les 6 colonnes *_FR de T2S.

SELECT COUNT(*) AS absents_partout_avec_label_fr
FROM   T_WC_WIKIDATA_ITEM_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0 AND v1.LANG='fr' AND v1.LABEL IS NOT NULL AND v1.LABEL <> ''
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM      x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci);

SELECT '=== A3 . item : echantillon de 25 absents partout ===' AS section;

SELECT v1.ID_WIKIDATA, v1.LANG, v1.LABEL, v1.INSTANCE_OF
FROM   T_WC_WIKIDATA_ITEM_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0 AND v1.LANG='en'
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM      x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
LIMIT 25;


-- ############################################################################
-- ### B . ID_CRITERION et PLEX_MEDIA_KEY : mesure corrigee       [moyen]    ###
-- ############################################################################
-- ID_CRITERION / ID_CRITERION_SPINE sont des int : "IS NOT NULL" comptait les
-- zeros, d'ou les 370 134 lignes "remplies" (100 % des films) et les 19 924
-- "non retrouves" du fichier principal. Ces deux chiffres sont a jeter.

SELECT '=== B1 . remplissage reel (zero exclu) ===' AS section;

SELECT 'movie' AS entite,
       COUNT(*) AS lignes_actives,
       SUM(ID_CRITERION       IS NOT NULL AND ID_CRITERION       <> 0) AS id_criterion_non_nul,
       SUM(ID_CRITERION_SPINE IS NOT NULL AND ID_CRITERION_SPINE <> 0) AS id_criterion_spine_non_nul,
       SUM(PLEX_MEDIA_KEY IS NOT NULL AND PLEX_MEDIA_KEY <> '' AND PLEX_MEDIA_KEY <> '0') AS plex_media_key_non_vide
FROM   T_WC_WIKIDATA_MOVIE_V1 WHERE COALESCE(DELETED,0)=0
UNION ALL
SELECT 'serie', COUNT(*),
       SUM(ID_CRITERION       IS NOT NULL AND ID_CRITERION       <> 0),
       SUM(ID_CRITERION_SPINE IS NOT NULL AND ID_CRITERION_SPINE <> 0),
       SUM(PLEX_MEDIA_KEY IS NOT NULL AND PLEX_MEDIA_KEY <> '' AND PLEX_MEDIA_KEY <> '0')
FROM   T_WC_WIKIDATA_SERIE_V1 WHERE COALESCE(DELETED,0)=0;

SELECT '=== B2 . Criterion : la donnee est-elle dans les statements V2 ? ===' AS section;
-- Sur les VRAIS remplis cette fois. P9584 = Criterion Collection film ID,
-- P12279 = Criterion Collection spine number.

SELECT 'movie.ID_CRITERION -> P9584' AS colonne_v1, COUNT(*) AS lignes_v1_remplies,
       SUM(v2_present) AS retrouves_en_v2, SUM(1-v2_present) AS non_retrouves
FROM (
  SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P9584') AS v2_present
  FROM T_WC_WIKIDATA_MOVIE_V1 v1
  WHERE COALESCE(v1.DELETED,0)=0 AND v1.ID_CRITERION IS NOT NULL AND v1.ID_CRITERION <> 0
) t
UNION ALL
SELECT 'movie.ID_CRITERION_SPINE -> P12279', COUNT(*), SUM(v2_present), SUM(1-v2_present)
FROM (
  SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                 WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                   AND st.ID_PROPERTY = 'P12279') AS v2_present
  FROM T_WC_WIKIDATA_MOVIE_V1 v1
  WHERE COALESCE(v1.DELETED,0)=0 AND v1.ID_CRITERION_SPINE IS NOT NULL AND v1.ID_CRITERION_SPINE <> 0
) t;


-- ############################################################################
-- ### C . QUALIFICATIFS : pourquoi le numero d'episode manque-t-il ?        ###
-- ############################################################################
-- Constat du fichier principal : 19 915 episodes sur 19 916 ont bien leur
-- statement P179/P4908 en V2, mais seulement 81 portent le qualificatif P1545
-- (series ordinal) qui donne le numero. Or c'est exactement de la que V1 tire
-- EPISODE_NUMBER (sparql-movies-persons.py:1157, "P1545 qualifier of P4908"),
-- et V1 l'a pour 173 307 episodes sur 179 290.
--
-- L'ETL, lui, ingere bien les qualificatifs de toute propriete dont le type de
-- valeur est supporte (wikidata_dump_etl.py:1518-1530), et P1545 est de type
-- string, donc supporte. Il faut donc mesurer, pas supposer.

SELECT '=== C1 . volumetrie des qualificatifs par propriete (top 25) ===  [lent]' AS section;

SELECT ID_QUALIFIER_PROPERTY, COUNT(*) AS nb
FROM   T_WC_WIKIDATA_STATEMENT_QUALIFIER
GROUP BY ID_QUALIFIER_PROPERTY
ORDER BY nb DESC
LIMIT 25;

SELECT '=== C2 . P1545 existe-t-il, et sur quelles proprietes porteuses ? ===' AS section;

SELECT st.ID_PROPERTY AS propriete_porteuse, COUNT(*) AS nb_qualificatifs_p1545
FROM   T_WC_WIKIDATA_STATEMENT_QUALIFIER q
JOIN   T_WC_WIKIDATA_STATEMENT st ON st.ID_STATEMENT = q.ID_STATEMENT
WHERE  q.ID_QUALIFIER_PROPERTY = 'P1545'
GROUP BY st.ID_PROPERTY
ORDER BY nb_qualificatifs_p1545 DESC
LIMIT 20;

SELECT '=== C3 . episode temoin : tout ce que V2 sait de lui ===' AS section;
-- Prend un episode que V1 numerote, et deplie ses statements V2 + qualificatifs.
-- Si la ligne P179/P4908 est la sans aucun qualificatif, la perte est a
-- l'ingestion. Si le qualificatif est la mais sur une autre propriete porteuse,
-- c'est ma requete qui visait a cote.

SELECT @qid_episode := (
  SELECT v1.ID_WIKIDATA FROM T_WC_WIKIDATA_EPISODE_V1 v1
  WHERE  COALESCE(v1.DELETED,0)=0 AND v1.EPISODE_NUMBER IS NOT NULL AND v1.EPISODE_NUMBER > 0
    AND  EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE e
                 WHERE e.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  LIMIT 1) AS episode_temoin;

SELECT v1.ID_WIKIDATA, v1.TITLE, v1.SEASON_NUMBER, v1.EPISODE_NUMBER, v1.ID_WIKIDATA_SERIE
FROM   T_WC_WIKIDATA_EPISODE_V1 v1 WHERE v1.ID_WIKIDATA = @qid_episode;

SELECT st.ID_STATEMENT, st.ID_PROPERTY, st.VALUE_TYPE, st.RANK,
       q.ID_QUALIFIER_PROPERTY, q.VALUE_TYPE AS qual_value_type,
       qs.VALUE_STRING AS qual_string, qq.AMOUNT AS qual_amount, qi.ID_ITEM AS qual_item
FROM   T_WC_WIKIDATA_STATEMENT st
LEFT JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q ON q.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_STRING_VALUE   qs ON qs.ID_STATEMENT_QUALIFIER = q.ID_STATEMENT_QUALIFIER
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_QUANTITY_VALUE qq ON qq.ID_STATEMENT_QUALIFIER = q.ID_STATEMENT_QUALIFIER
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE     qi ON qi.ID_STATEMENT_QUALIFIER = q.ID_STATEMENT_QUALIFIER
WHERE  st.ID_WIKIDATA = @qid_episode
ORDER BY st.ID_PROPERTY;

SELECT '=== C4 . combien de statements portent AU MOINS un qualificatif ? ===  [lent]' AS section;
-- Repere general : si le taux est ridiculement bas sur des proprietes ou
-- Wikidata qualifie systematiquement (P166 recompense, P161 casting), le
-- probleme est global et non specifique a P1545.

SELECT st.ID_PROPERTY,
       COUNT(*) AS nb_statements,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER q
                   WHERE q.ID_STATEMENT = st.ID_STATEMENT)) AS avec_qualificatif
FROM   T_WC_WIKIDATA_STATEMENT st
WHERE  st.ID_PROPERTY IN ('P179','P4908','P166','P161','P577')
GROUP BY st.ID_PROPERTY;


-- ############################################################################
-- ### D . DEATHDAY : 36 % sans P570 en V2, bruit ou perte ?      [moyen]    ###
-- ############################################################################

SELECT '=== D1 . distribution des annees de deces en V1 ===' AS section;
-- Un pic sur une annee unique (1970, 0000, l annee courante) signerait une
-- valeur par defaut ecrite par le crawler, donc du bruit et non une perte.

SELECT YEAR(DEATHDAY) AS annee_deces, COUNT(*) AS nb
FROM   T_WC_WIKIDATA_PERSON_V1
WHERE  COALESCE(DELETED,0)=0 AND DEATHDAY IS NOT NULL
GROUP BY annee_deces
ORDER BY nb DESC
LIMIT 15;

SELECT '=== D2 . les persons sans P570 en V2 ont-elles d autres statements ? ===' AS section;
-- Si elles ont P569 (naissance) mais pas P570 (deces), V2 les connait bien et
-- c est la date de deces qui est en cause. Si elles n ont ni l un ni l autre,
-- c est l entite entiere qui est mince en V2.

SELECT COUNT(*) AS echantillon,
       SUM(a_p569) AS avec_naissance_v2,
       SUM(a_p570) AS avec_deces_v2,
       SUM(a_p31)  AS avec_p31_v2
FROM (
  SELECT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci AND st.ID_PROPERTY='P569') AS a_p569,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci AND st.ID_PROPERTY='P570') AS a_p570,
         EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci AND st.ID_PROPERTY='P31')  AS a_p31
  FROM (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_PERSON_V1
        WHERE COALESCE(DELETED,0)=0 AND DEATHDAY IS NOT NULL LIMIT 20000) v1
  WHERE EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON p
                WHERE p.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
) t;

SELECT '=== D3 . echantillon comparatif V1 / V2 sur 20 defunts ===' AS section;

SELECT v1.ID_WIKIDATA, v1.NAME, v1.BIRTHDAY, v1.DEATHDAY,
       (SELECT tv.DATE_START FROM T_WC_WIKIDATA_STATEMENT st
        JOIN T_WC_WIKIDATA_TIME_VALUE tv ON tv.ID_STATEMENT = st.ID_STATEMENT
        WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
          AND st.ID_PROPERTY='P570' LIMIT 1) AS deces_v2
FROM   T_WC_WIKIDATA_PERSON_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0 AND v1.DEATHDAY IS NOT NULL
  AND  EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON p WHERE p.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                   WHERE st.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci AND st.ID_PROPERTY='P570')
LIMIT 20;


-- ############################################################################
-- ### E . PERSON absentes AVEC IMDb : quelle part est du dechet ? [moyen]   ###
-- ############################################################################
-- L echantillon 3C montrait des ID_IMDB tronques a 20 caracteres : `https://ww`,
-- `//www.imdb`, ou des chaines libres (`joshuahost`). Un identifiant IMDb valide
-- vaut `nm` suivi de chiffres. Ce comptage separe l anomalie reelle du bruit.

SELECT '=== E1 . forme des ID_IMDB des 1 641 manquantes ===' AS section;

SELECT CASE WHEN v1.ID_IMDB REGEXP '^nm[0-9]+$'  THEN 'nm valide'
            WHEN v1.ID_IMDB REGEXP '^[0-9]+$'    THEN 'chiffres seuls'
            WHEN v1.ID_IMDB LIKE '%//%'          THEN 'url tronquee'
            ELSE 'autre chaine' END AS forme,
       COUNT(*) AS nb,
       MIN(v1.ID_IMDB) AS exemple
FROM   T_WC_WIKIDATA_PERSON_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0
  AND  v1.ID_IMDB IS NOT NULL AND v1.ID_IMDB <> ''
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON v2
                   WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
GROUP BY forme
ORDER BY nb DESC;

SELECT '=== E2 . anciennete des QID manquants (Q recent = cree puis supprime) ===' AS section;
-- Un QID au-dela de ~Q100000000 a ete cree recemment. Beaucoup de ces items sont
-- des creations promotionnelles supprimees depuis par Wikidata : le dump ne les a
-- plus, V1 les garde parce qu il n efface jamais.

SELECT CASE WHEN CAST(SUBSTRING(v1.ID_WIKIDATA,2) AS UNSIGNED) >= 100000000 THEN 'Q >= 100M (recent)'
            WHEN CAST(SUBSTRING(v1.ID_WIKIDATA,2) AS UNSIGNED) >=  50000000 THEN 'Q 50M-100M'
            ELSE 'Q < 50M (ancien)' END AS tranche_qid,
       COUNT(*) AS nb,
       SUM(v1.ID_IMDB REGEXP '^nm[0-9]+$') AS dont_imdb_valide
FROM   T_WC_WIKIDATA_PERSON_V1 v1
WHERE  COALESCE(v1.DELETED,0)=0
  AND  v1.ID_IMDB IS NOT NULL AND v1.ID_IMDB <> ''
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON v2
                   WHERE v2.ID_WIKIDATA = v1.ID_WIKIDATA COLLATE utf8mb4_unicode_ci)
GROUP BY tranche_qid
ORDER BY nb DESC;

SELECT '=== E3 . V1 ecrit-il encore ? (la cible bouge tant que V1 tourne) ===' AS section;
-- PERSON_V1 etait ecrite pendant l execution du fichier principal. Tant que les
-- crawlers V1 tournent, "V1 inclus dans V2" vise une cible mouvante : V1 ajoute
-- des lignes que le dump du 26/07 ne pouvait pas contenir.

SELECT 'person' AS entite,
       SUM(TIM_UPDATED >= '2026-07-26') AS ecrites_depuis_le_dump,
       MAX(TIM_UPDATED) AS derniere_ecriture
FROM   T_WC_WIKIDATA_PERSON_V1 WHERE COALESCE(DELETED,0)=0
UNION ALL
SELECT 'movie', SUM(TIM_UPDATED >= '2026-07-26'), MAX(TIM_UPDATED)
FROM   T_WC_WIKIDATA_MOVIE_V1 WHERE COALESCE(DELETED,0)=0
UNION ALL
SELECT 'serie', SUM(TIM_UPDATED >= '2026-07-26'), MAX(TIM_UPDATED)
FROM   T_WC_WIKIDATA_SERIE_V1 WHERE COALESCE(DELETED,0)=0
UNION ALL
SELECT 'item', SUM(TIM_UPDATED >= '2026-07-26'), MAX(TIM_UPDATED)
FROM   T_WC_WIKIDATA_ITEM_V1 WHERE COALESCE(DELETED,0)=0;

SELECT '========== FIN ==========' AS section;
