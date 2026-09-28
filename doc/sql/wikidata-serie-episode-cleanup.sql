-- ============================================================================
-- Retirer de T_WC_WIKIDATA_SERIE les episodes et saisons qui s'y sont glisses
-- ============================================================================
--
-- ORIGINE. Q21191270 (television series episode) descend de Q15416 (television
-- program), racine de serie depuis le 2026-07-12, et classify_qids testait la serie
-- avant l'episode. Mesure du 2026-09-28
-- (doc/sql/wikidata-episode-classification-check-20260928.txt) : sur 181 664
-- entites P31 = episode, 181 031 sont dans SERIE, dont 179 509 AUSSI dans EPISODE
-- (lignes d'aout, jamais purgees). SERIE (359 630 lignes) est donc a moitie faite
-- d'episodes. Temoin : 132 saisons sont dans SERIE en plus de SEASON.
--
-- CORRECTIF. f599c40 (2026-09-28) : saison et episode passent avant la serie. Les
-- runs suivants n'ecriront plus d'episode dans SERIE, mais les tables d'entite ne
-- sont jamais purgees : les lignes deja la y restent. D'ou ce fichier.
--
-- CE QUI EST SUPPRIME, ET SEULEMENT CELA. Une ligne de SERIE dont l'entite est
-- AUSSI dans EPISODE (resp. SEASON) ET porte P31 = Q21191270 (resp. Q3464665).
-- La double condition garantit que rien ne disparait de l'ecran : l'entite reste
-- lisible dans sa bonne table. Les ~1 500 episodes presents dans SERIE seulement
-- ne sont PAS touches ici : le prochain run (avec f599c40) les ecrira dans EPISODE,
-- et une seconde passe de ce fichier les retirera alors de SERIE.
-- Les statements ne sont pas concernes : ils sont indexes par ID_WIKIDATA, pas par
-- table d'entite. Aucune cle etrangere ne pointe vers T_WC_WIKIDATA_SERIE.
--
-- QUAND. Seulement apres que f599c40 est sur le VPS (git pull ; l'image est
-- reconstruite a chaque lancement), sinon le run suivant remet tout. Hors run du
-- crawler. Ne pas lancer pendant une campagne d'evaluation text2sql : les
-- requetes sur les series changent de resultat.
--
-- DESTRUCTIF. Sections 1 et 3 en lecture ; section 2 supprime, dans une
-- transaction. Pour lire le decompte d'abord, lancer une copie de ce fichier dont
-- la section 2 est mise en commentaire.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ----------------------------------------------------------------------------
-- 1 . Avant : ce qui va partir
-- ----------------------------------------------------------------------------
SELECT '=== 1 . lignes SERIE a retirer (avant) ===' AS section;

SELECT 'episode' AS nature, COUNT(*) AS lignes_serie_a_retirer
FROM   T_WC_WIKIDATA_SERIE s
JOIN   T_WC_WIKIDATA_EPISODE e ON e.ID_WIKIDATA = s.ID_WIKIDATA
WHERE  EXISTS (SELECT 1
               FROM   T_WC_WIKIDATA_STATEMENT  st
               JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
               WHERE  st.ID_WIKIDATA = s.ID_WIKIDATA AND st.ID_PROPERTY = 'P31'
                 AND  st.DELETED = 0 AND iv.ID_ITEM = 'Q21191270')
UNION ALL
SELECT 'saison', COUNT(*)
FROM   T_WC_WIKIDATA_SERIE s
JOIN   T_WC_WIKIDATA_SEASON a ON a.ID_WIKIDATA = s.ID_WIKIDATA
WHERE  EXISTS (SELECT 1
               FROM   T_WC_WIKIDATA_STATEMENT  st
               JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
               WHERE  st.ID_WIKIDATA = s.ID_WIKIDATA AND st.ID_PROPERTY = 'P31'
                 AND  st.DELETED = 0 AND iv.ID_ITEM = 'Q3464665');
-- Attendu au 2026-09-28 : environ 179 509 episodes et au plus 132 saisons.

SELECT COUNT(*) AS lignes_serie_avant FROM T_WC_WIKIDATA_SERIE;

-- ----------------------------------------------------------------------------
-- 2 . Suppression
-- ----------------------------------------------------------------------------
SELECT '=== 2 . suppression ===' AS section;

START TRANSACTION;

DELETE s
FROM   T_WC_WIKIDATA_SERIE s
JOIN   T_WC_WIKIDATA_EPISODE e ON e.ID_WIKIDATA = s.ID_WIKIDATA
WHERE  EXISTS (SELECT 1
               FROM   T_WC_WIKIDATA_STATEMENT  st
               JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
               WHERE  st.ID_WIKIDATA = s.ID_WIKIDATA AND st.ID_PROPERTY = 'P31'
                 AND  st.DELETED = 0 AND iv.ID_ITEM = 'Q21191270');
SELECT ROW_COUNT() AS episodes_retires;

DELETE s
FROM   T_WC_WIKIDATA_SERIE s
JOIN   T_WC_WIKIDATA_SEASON a ON a.ID_WIKIDATA = s.ID_WIKIDATA
WHERE  EXISTS (SELECT 1
               FROM   T_WC_WIKIDATA_STATEMENT  st
               JOIN   T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
               WHERE  st.ID_WIKIDATA = s.ID_WIKIDATA AND st.ID_PROPERTY = 'P31'
                 AND  st.DELETED = 0 AND iv.ID_ITEM = 'Q3464665');
SELECT ROW_COUNT() AS saisons_retirees;

COMMIT;

-- ----------------------------------------------------------------------------
-- 3 . Apres : controle
-- ----------------------------------------------------------------------------
SELECT '=== 3 . apres ===' AS section;

SELECT COUNT(*) AS lignes_serie_apres FROM T_WC_WIKIDATA_SERIE;
-- Attendu : environ 359 630 - 179 641 = 180 000 lignes, les series proprement dites.

SELECT '========== FIN ==========' AS section;
