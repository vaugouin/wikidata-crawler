-- ============================================================================
-- Chargement du SEUL graphe de sous-classes P279 (WIKIDATA-CRAWLER-020, voie b)
-- ============================================================================
--
-- Extrait mecaniquement de la section ajoutee le 2026-08-16 a la fin de
-- 03_bulk_load_from_staging_FULL.sql. Aucune ligne modifiee : seuls l'en-tete, les
-- SET de session, le COMMIT et le controle final sont ajoutes, pour que la section
-- puisse tourner seule.
--
-- POURQUOI. pass1 ecrit depuis toujours /shared/pass1/subclass_edges.jsonl, et rien
-- ne le chargeait : le graphe des classes existait sur le disque et restait
-- invisible au SQL. Consequence relevee dans wikidata-v2-awards-queries.sql, aucune
-- question hierarchique n'etait interrogeable en V2. Depuis le 2026-08-16 le
-- pipeline le charge, mais le run du 09/08 au 14/08 est deja passe : ce fichier
-- rattrape ce run-la, sans rejouer une seule etape d'ETL, tant que le repertoire
-- pass1 est encore sur le disque (run-if-new-dump.sh l'efface au lancement suivant).
--
-- CE QUE CELA DEBLOQUE. Les pools d'entites sont derives de ce graphe a chaque run
-- (descendants_of_roots(MOVIE_ROOTS) et les autres). Ce qui compte comme « film »
-- n'est donc pas defini par ce depot mais par les aretes P279 que Wikidata publie
-- ce jour-la. Charger les aretes rend cette definition auditable, et d'un run a
-- l'autre rend sa derive mesurable. C'est ce qui manquait aux blocs D1 et D3 de
-- doc/sql/wikidata-movie-drop-diagnostic.sql, qui ne trouvaient que 9 classes la ou
-- le pool reel en compte des milliers.
--
-- PREREQUIS, dans cet ordre :
--   1. les deux tables existent :
--        mariadb --force -t vaugouindb < apply_to_live_db.sql
--      (idempotent, il ne touche a rien d'autre) ;
--   2. le staging porte les aretes de ce batch :
--        load_staging_jsonl.py --only-table STG_T_WC_WIKIDATA_SUBCLASS
--      soit 5 228 221 lignes pour le batch wikidata_full_20260807_1043.
--
-- Aucune cle etrangere ici : une arete nomme couramment des classes qui ne sont pas
-- importees comme entites, exactement comme T_WC_WIKIDATA_ITEM_VALUE.
--
-- LECTURE : ce script ECRIT. Executer avec --force.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET FOREIGN_KEY_CHECKS = 1;
SET SESSION max_statement_time = 0;

-- Le CONVERT ... COLLATE evite ERROR 1267 : une valeur produite par une fonction
-- porte une coercibilite implicite (cf. AGENTS.md).
SET @IMPORT_BATCH_ID = CONVERT('wikidata_full_20260807_1043' USING utf8mb4) COLLATE utf8mb4_unicode_ci;


-- ----------------------------------------------------------------------------
-- P279 subclass graph. Duplicate edges are absorbed by the composite primary key
-- (ID_CHILD, ID_PARENT), so re-running a batch is idempotent. IMPORT_BATCH_ID is
-- refreshed on conflict, which lets one tell an edge Wikidata still asserts from
-- one carried over by an older run.
-- ----------------------------------------------------------------------------
INSERT INTO T_WC_WIKIDATA_SUBCLASS (
    ID_CHILD,
    ID_PARENT,
    DELETED,
    IMPORT_BATCH_ID,
    TIM_UPDATED
)
SELECT
    s.ID_CHILD,
    s.ID_PARENT,
    0,
    s.IMPORT_BATCH_ID,
    NOW()
FROM STG_T_WC_WIKIDATA_SUBCLASS s
WHERE s.IMPORT_BATCH_ID = @IMPORT_BATCH_ID
  AND s.ROW_STATUS IN ('NEW','VALID')
ON DUPLICATE KEY UPDATE
    DELETED         = VALUES(DELETED),
    IMPORT_BATCH_ID = VALUES(IMPORT_BATCH_ID),
    TIM_UPDATED     = NOW();

UPDATE STG_T_WC_WIKIDATA_SUBCLASS
SET ROW_STATUS = 'LOADED',
    ERROR_MESSAGE = NULL
WHERE IMPORT_BATCH_ID = @IMPORT_BATCH_ID
  AND ROW_STATUS IN ('NEW','VALID');

COMMIT;


-- ----------------------------------------------------------------------------
-- Controle 1 : volumetrie. Le staging doit porter 5 228 221 lignes pour ce batch.
-- La table cible peut en compter MOINS, et ce n'est pas une perte : la cle primaire
-- (ID_CHILD, ID_PARENT) absorbe les aretes repetees. Un ecart de quelques milliers
-- est normal, un ecart massif signale un staging incomplet.
-- ----------------------------------------------------------------------------
SELECT (SELECT COUNT(*) FROM T_WC_WIKIDATA_SUBCLASS)     AS aretes_distinctes_chargees,
       (SELECT COUNT(*) FROM STG_T_WC_WIKIDATA_SUBCLASS
        WHERE IMPORT_BATCH_ID = @IMPORT_BATCH_ID)        AS lignes_en_staging,
       '5 228 221 attendues en staging'                  AS repere_20260811;


-- ----------------------------------------------------------------------------
-- Controle 2 : le but de l'operation. Taille du pool « film », c'est-a-dire la
-- fermeture transitive de P279 sous Q11424 (film) et Q506240 (telefilm), telle que
-- l'ETL la calcule en memoire a chaque run. Avant ce chargement, la meme requete
-- rendait 9. Noter le chiffre : c'est le premier point d'une serie qui rendra la
-- derive de la classification mesurable d'un run a l'autre.
--
-- Le CAST dans l'ancre est obligatoire : MariaDB type la colonne d'un CTE recursif
-- d'apres sa seule partie non recursive, et un litteral court fait echouer la
-- descente avec ERROR 1406 (cf. doc/sql/wikidata-movie-drop-diagnostic.sql).
-- ----------------------------------------------------------------------------
WITH RECURSIVE pool_film (qid) AS (
    SELECT CAST('Q11424' AS CHAR(50)) COLLATE utf8mb4_unicode_ci AS qid
    UNION
    SELECT CAST('Q506240' AS CHAR(50)) COLLATE utf8mb4_unicode_ci
    UNION
    SELECT sc.ID_CHILD
    FROM   T_WC_WIKIDATA_SUBCLASS sc
    JOIN   pool_film p ON p.qid = sc.ID_PARENT
    WHERE  sc.DELETED = 0
)
SELECT COUNT(*) AS classes_dans_le_pool_film,
       '9 avant chargement, artefact d un graphe absent' AS repere;
