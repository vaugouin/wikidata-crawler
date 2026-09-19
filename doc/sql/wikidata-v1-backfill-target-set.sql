-- ============================================================================
-- WIKIDATA-CRAWLER-023 : l'ensemble cible du relogement V1 vers le cache V2
-- ============================================================================
--
-- LECTURE SEULE. A jouer AVANT un run pour dimensionner l'import, et APRES pour
-- constater ce qu'il a produit. Jumeau lisible de
-- `python build_v1_backfill_seed.py --report`, qui rend la meme ventilation d'un
-- seul coup depuis un conteneur (`./wikidata-crawler.sh --v1-backfill-report`).
--
-- CE QU'ON MESURE ICI, ET CE QU'ON NE MESURE PAS. Ce fichier calcule la
-- DIFFERENCE : ce que V1 detient et que V2 n'a pas. C'est le chiffre qui
-- dimensionne le run et qui alimente le plancher de WIKIDATA-CRAWLER-022.
--
-- CE N'EST PAS LA GRAINE, et confondre les deux casse le dispositif en silence.
-- La graine que l'etape 106 seme est ancree sur V1 SEUL (tout ID_WIKIDATA de
-- T_WC_WIKIDATA_ITEM_V1), parce que la difference S'AUTO-EFFACE : apres un import
-- reussi elle rend zero, la graine se viderait, ces items sortiraient du filtre
-- item_cache, et l'etape 114 supprimerait leurs faits des le run suivant. Le tout
-- sous un run en succes. La difference chiffre, l'ancrage V1 seme.
--
-- LE PLANCHER, section 3, est la partie que cette route ne peut pas franchir :
-- les Q-ids que V2 detient deja comme film, serie, personne, saison, episode ou
-- personnage. item_cache refuse d'ecrire une entite du perimetre coeur dans
-- T_WC_WIKIDATA_ITEM, et f_getwikidatalabel ne lit que cette table
-- (TMDB-MOVIE-PREPROCESS-036, et l'elargir a l'aveugle ecrirait un titre de film
-- dans AWARD_NAME_FR). Ces lignes resteront servies par le repli V1 quoi qu'on
-- seme : c'est une perte residuelle a acter, pas un echec de l'import.
--
-- COUT. La section 2 fait sept sondes d'index par ligne francaise de V1, soit
-- environ 4,8 millions de recherches : comptez une a trois minutes.
--
-- Executer avec --force -t. Voir aussi, cote tmdb-movie-preprocess,
-- doc/sql/test-017-repli-v1-taux.sql, qui mesure le taux de repli a l'ecran.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET SESSION max_statement_time = 0;


SELECT '=== 1. la graine : ce que l etape 106 va semer (ancree sur V1 seul) ===' AS section;
-- A comparer a la variable serveur strwikidatacrawlerv1backfillseeded apres le run.
-- Un ecart franc veut dire que quelqu'un a commence a vider les tables V1.

SELECT COUNT(DISTINCT ID_WIKIDATA) AS ids_semes_toutes_langues
FROM   T_WC_WIKIDATA_ITEM_V1
WHERE  COALESCE(DELETED, 0) = 0
  AND  ID_WIKIDATA LIKE 'Q%';


SELECT '=== 2. la cible : ventilation du reliquat francais ===' AS section;
-- cible_importable est le chiffre qui dimensionne le run (~250 k attendu au
-- 2026-09-19, a confirmer ici). ailleurs_en_v2_plancher est la perte residuelle.

SELECT
    COUNT(*)                                                     AS lignes_v1_fr,
    SUM(dans_item)                                               AS deja_dans_item,
    SUM(dans_item = 0 AND dans_autre_v2 = 0)                     AS cible_importable,
    SUM(dans_item = 0 AND dans_autre_v2 = 1)                     AS ailleurs_en_v2_plancher,
    ROUND(100 * SUM(dans_item = 0 AND dans_autre_v2 = 0) / COUNT(*), 1) AS pct_importable,
    ROUND(100 * SUM(dans_item = 0 AND dans_autre_v2 = 1) / COUNT(*), 1) AS pct_plancher
FROM (
    SELECT
        EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM      x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA) AS dans_item,
        (   EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA)
         OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE     x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA)
         OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA)
         OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON    x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA)
         OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE   x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA)
         OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER x WHERE x.ID_WIKIDATA = v1.ID_WIKIDATA)
        )                                                                                     AS dans_autre_v2
    FROM   T_WC_WIKIDATA_ITEM_V1 v1
    WHERE  v1.LANG = 'fr'
      AND  COALESCE(v1.DELETED, 0) = 0
      AND  NULLIF(v1.LABEL, '') IS NOT NULL
) t;


SELECT '=== 3. le plancher, par table V2 qui detient deja l entite ===' AS section;
-- Ou vivent les Q-ids que l'import ne pourra pas servir. Une ligne peut compter
-- dans deux tables si V2 la detient deux fois : c'est alors un defaut de
-- classification, a instruire pour lui-meme.

SELECT 'MOVIE'     AS table_v2, COUNT(*) AS lignes FROM T_WC_WIKIDATA_ITEM_V1 v1
  WHERE v1.LANG='fr' AND COALESCE(v1.DELETED,0)=0 AND NULLIF(v1.LABEL,'') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM x WHERE x.ID_WIKIDATA=v1.ID_WIKIDATA)
    AND     EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE x WHERE x.ID_WIKIDATA=v1.ID_WIKIDATA)
UNION ALL
SELECT 'SERIE',     COUNT(*) FROM T_WC_WIKIDATA_ITEM_V1 v1
  WHERE v1.LANG='fr' AND COALESCE(v1.DELETED,0)=0 AND NULLIF(v1.LABEL,'') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM x WHERE x.ID_WIKIDATA=v1.ID_WIKIDATA)
    AND     EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE x WHERE x.ID_WIKIDATA=v1.ID_WIKIDATA)
UNION ALL
SELECT 'PERSON',    COUNT(*) FROM T_WC_WIKIDATA_ITEM_V1 v1
  WHERE v1.LANG='fr' AND COALESCE(v1.DELETED,0)=0 AND NULLIF(v1.LABEL,'') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM x WHERE x.ID_WIKIDATA=v1.ID_WIKIDATA)
    AND     EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON x WHERE x.ID_WIKIDATA=v1.ID_WIKIDATA)
UNION ALL
SELECT 'SEASON',    COUNT(*) FROM T_WC_WIKIDATA_ITEM_V1 v1
  WHERE v1.LANG='fr' AND COALESCE(v1.DELETED,0)=0 AND NULLIF(v1.LABEL,'') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM x WHERE x.ID_WIKIDATA=v1.ID_WIKIDATA)
    AND     EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON x WHERE x.ID_WIKIDATA=v1.ID_WIKIDATA)
UNION ALL
SELECT 'EPISODE',   COUNT(*) FROM T_WC_WIKIDATA_ITEM_V1 v1
  WHERE v1.LANG='fr' AND COALESCE(v1.DELETED,0)=0 AND NULLIF(v1.LABEL,'') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM x WHERE x.ID_WIKIDATA=v1.ID_WIKIDATA)
    AND     EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE x WHERE x.ID_WIKIDATA=v1.ID_WIKIDATA)
UNION ALL
SELECT 'CHARACTER', COUNT(*) FROM T_WC_WIKIDATA_ITEM_V1 v1
  WHERE v1.LANG='fr' AND COALESCE(v1.DELETED,0)=0 AND NULLIF(v1.LABEL,'') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM x WHERE x.ID_WIKIDATA=v1.ID_WIKIDATA)
    AND     EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER x WHERE x.ID_WIKIDATA=v1.ID_WIKIDATA);


SELECT '=== 4. apres le run : ce que la graine a produit ===' AS section;
-- Les quatre compteurs ecrits par l'etape 107. Ils doivent s'additionner a la
-- graine : semes = caches + reroutes vers PERSON + refuses (coeur) + absents du
-- dump. Les absents sont les Q-ids supprimes ou fusionnes chez Wikidata depuis
-- que le crawler SPARQL les a enregistres, parfois il y a des annees.

SELECT VAR_NAME, VAR_VALUE
FROM   T_WC_SERVER_VARIABLE
WHERE  VAR_NAME IN ('strwikidatacrawlerv1backfillseeded',
                    'strwikidatacrawlerv1backfillemitted',
                    'strwikidatacrawlerv1backfilldiverted',
                    'strwikidatacrawlerv1backfillskippedcore',
                    'strwikidatacrawlerv1backfillmissing')
ORDER BY VAR_NAME;


SELECT '=== 5. controle croise : le cache a-t-il grossi ? ===' AS section;
-- Repere du 2026-08-16, avant tout relogement : 702 502 lignes.

SELECT COUNT(*) AS lignes_item_v2, '702 502' AS repere_avant_relogement_20260816
FROM   T_WC_WIKIDATA_ITEM;


SELECT '========== FIN ==========' AS section;
