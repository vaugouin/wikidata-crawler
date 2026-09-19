# JOURNAL

Mémoire opérationnelle du dépôt, antéchronologique (l'entrée la plus récente en haut).
Au démarrage d'une session, lire les premières entrées pour retrouver le contexte, sans
charger tout le fichier.

## 2026-09-19 : l'ensemble cible de -023 est chiffré, et il vaut un tiers de moins que le ticket

Mesure faite sur la base par `runsqlvaugouindb.sh`, trace conservée dans
`doc/sql/wikidata-v1-backfill-target-set-20260919.txt`. Sur 651 696 lignes françaises de
`T_WC_WIKIDATA_ITEM_V1` (non supprimées, libellé non vide) :

| | lignes | part |
|---|---:|---:|
| déjà dans `T_WC_WIKIDATA_ITEM` | 402 312 | 61,7 % |
| **cible importable** | **167 928** | 25,8 % |
| plancher, ailleurs en V2 | 81 456 | 12,5 % |

Graine semée : 696 660 identifiants. `T_WC_WIKIDATA_ITEM` pèse 710 556 lignes et passerait
donc à 878 484 au plus, +23,6 %.

**Le « ~250 k » du ticket comptait le plancher comme importable.** La cible réelle est
167 928. Les deux mesures se réconcilient à 801 lignes près, qui sont les entités présentes
dans `ITEM` mais dont `LABELS_JSON` n'a ni `fr` ni `en`.

**Prédiction à vérifier après le run** : le taux de repli de `test-017-repli-v1-taux.sql`
devrait tomber de 36,2 % à environ **11,9 %** (82 257 sur 691 320), plus les Q-ids que le
dump ne contient plus, comptés par `strwikidatacrawlerv1backfillmissing`.

**Le plancher n'est pas une perte, et c'est une correction à ce que j'écrivais hier.** Ces
81 456 entités ont bien leur libellé dans V2, dans `SERIE`, `EPISODE`, `CHARACTER` ou
`SEASON`. C'est `f_getwikidatalabel` qui ne lit que `T_WC_WIKIDATA_ITEM`, volontairement
(TMDB-MOVIE-PREPROCESS-036). Le sujet se règle donc côté lecteur, par appelant pour ne pas
réintroduire le bug des titres de films dans `AWARD_NAME_FR`, et non par un run de plus
ici. Ce qui reste une vraie perte résiduelle pour -022, c'est le seul compteur `missing`.

**Trouvaille collatérale, à instruire pour elle-même.** La ventilation par table du plancher
somme à 135 660 pour 81 456 entités distinctes : **54 204 appartenances multiples**, des
entités que V2 détient dans deux tables à la fois. `SERIE` 54 890 et `EPISODE` 53 031 côte à
côte pointent vers l'explication : les tables d'entités ne sont jamais purgées, donc une
entité classée série avant l'arrivée des types SEASON / EPISODE / CHARACTER (août 2026) a
gardé sa ligne dans `SERIE` en plus de la nouvelle. Rien ne le signalait jusqu'ici.

## 2026-09-19 : WIKIDATA-CRAWLER-023 implémenté, la graine du relogement V1 est dans l'étape 106

**Ce qui est livré.** L'étape 106 reconstruit désormais, avant de lancer la passe, la liste des
items que seul V1 connaît, et la donne à `item_cache` comme second fichier d'entrée. Nouveau
script `build_v1_backfill_seed.py` (lançable seul, `--report` ventile sans rien écrire),
nouveau paramètre `extra_item_ids_path` dans `WikidataDumpETL`, ventilation en variables
serveur à l'étape 107, `doc/sql/wikidata-v1-backfill-target-set.sql` pour la mesure à la main,
et `./wikidata-crawler.sh --v1-backfill-report` pour la poser depuis l'hôte sans rien lancer.
Le test de fumée couvre les deux comportements et les deux planchers.

**Les trois points ouverts, tranchés.**
1. *La requête.* Deux requêtes, pas une. La **mesure** est la différence (dans V1, absent de
   V2), ventilée par table V2 ; la **graine** est ancrée sur V1 seul. Le volume réel n'a pas pu
   être chiffré en écrivant le code (pas d'accès à la base depuis le PC, clé SSH à passphrase
   et session non interactive) : il se lit maintenant en une commande, `--v1-backfill-report`.
   Bornes connues : 250 185 lignes servies par le repli au 2026-09-19, `T_WC_WIKIDATA_ITEM` à
   702 502 lignes, donc l'import la grossit d'au plus un tiers.
2. *La provenance.* Le `referenced_item_ids.txt` de pass2 n'est pas touché, la graine vit dans
   `/shared/seed/`, et le `run_summary.json` de la passe compte les deux sources séparément.
3. *La ré-injection.* Reconstruction depuis la base à chaque run, pas de fichier persistant
   (`run-if-new-dump.sh` vide `/shared` à chaque lancement) et pas de pass2 modifié.

**Deux corrections au ticket, vérifiées dans le code.**
- *Le piège de la différence auto-effaçante.* Semer « absent de V2 » aurait marché une fois,
  puis se serait vidé tout seul au run suivant, et l'étape 114 aurait supprimé les faits de ces
  items, le tout sous un run en succès. D'où l'ancrage sur V1 seul, écrit noir sur blanc dans
  les trois fichiers concernés.
- *Ce que la ré-injection protège.* Le ticket disait « sans elle, le repli réapparaît la semaine
  suivante ». C'est faux pour les libellés : les tables d'entités ne portent pas
  d'`IMPORT_BATCH_ID` et ne sont jamais purgées (`08_cleanup_old_batches.sql`), une ligne `ITEM`
  importée reste. Ce que l'absence de ré-injection détruit, ce sont les **faits** de ces items,
  supprimés dès le run suivant par l'étape 114, plus la fraîcheur et la reproductibilité. La
  décision ne change pas, sa raison si.

**Le plancher que cette route ne franchira pas, à remonter à -022.** `f_getwikidatalabel` ne lit
que `T_WC_WIKIDATA_ITEM` (volontairement, TMDB-MOVIE-PREPROCESS-036) et `item_cache` refuse
d'écrire une entité du périmètre cœur dans `ITEM` : les Q-ids que V2 détient déjà comme film,
série ou personne resteront servis par V1 quoi qu'on sème. Les compteurs `...skippedcore` et
`...missing` de l'étape 107 le chiffrent sur le dump réel.

**Route retenue pour l'exécution : (a), le prochain run hebdomadaire.** Rien à lancer à la main,
le mécanisme est dans le pipeline. Ce qui reste à faire après ce run : relever
`--v1-backfill-report` et `test-017-repli-v1-taux.sql`, et reporter le plancher dans -022.

## 2026-09-19 : décision route A pour la décommission V1, ticket WIKIDATA-CRAWLER-023 à implémenter

**Contexte.** La migration Wikidata V1 vers V2 est dans un état transitoire stable : le code
lit V2 d'abord et retombe sur V1. Au 2026-09-19, **36,2 % des libellés français** sont encore
servis par le repli V1 (250 185 sur 691 320, mesure `test-017-repli-v1-taux.sql` dans
`tmdb-movie-preprocess/doc/sql/`). Ce reliquat est presque exclusivement des **entités absentes
de V2** (ligne dans `T_WC_WIKIDATA_ITEM_V1`, aucune dans `T_WC_WIKIDATA_ITEM`). Aucun correctif
« code seul » ne peut donc les servir depuis V2 : il faut les y faire exister.

**Décision (route A).** Plutôt que de couper V1 en assumant une perte (route B, écartée), on
**rapatrie dans le cache V2 le reliquat que seul V1 sert**, pour que la suppression des tables V1
ne fasse disparaître aucun libellé. C'est un import **borné** (~250 k Q-ids déjà référencés), ce
que WIKIDATA-CRAWLER-011 (refus d'importer les ~119M pour la découverte) n'interdit pas. À noter,
sans se mentir : pour l'essentiel ce relogement ne crée pas de français, il déplace d'où vient le
repli anglais (de V2 au lieu de V1), et rend V1 supprimable sans perte visible.

**Découpage.** Deux tickets, dans le backlog Nestor
(`%USERPROFILE%/Nestor/projets/t2s-backlog/repos/wikidata-crawler.md`, grep l'ID) :
- **WIKIDATA-CRAWLER-022** : plan de gouvernance de la coupure (séquence des états, sauvegarde
  nommée, plancher résiduel, point de non-retour daté). Ne pas coder ici.
- **WIKIDATA-CRAWLER-023** : l'exécution ETL de l'import. **C'est le ticket à implémenter.**

**Mécanisme, vérifié dans le code.** La passe `item_cache` (étape 106) n'émet une ligne
`T_WC_WIKIDATA_ITEM` que pour les entités présentes dans son filtre d'items référencés, chargé
depuis le fichier pointé par la variable `REFERENCED_ITEM_IDS` (`wikidata_dump_etl.py:993` et
`:1385`, défaut `referenced_item_ids.txt` produit par pass2). Le geste : **étendre cet ensemble**
avec les Q-ids du reliquat, relancer `item_cache`, et leurs `LABELS_JSON` se matérialisent depuis
le dump (plus leurs faits `CACHED_ENTITY_PROPERTIES` : P31, P279, P345, P569, P570, P577).

**Trois points à trancher AVANT de coder (proposer un plan d'abord) :**
1. La requête exacte qui calcule l'ensemble cible (`ID_WIKIDATA` de `T_WC_WIKIDATA_ITEM_V1`
   `LANG='fr'` absents de `T_WC_WIKIDATA_ITEM`) et son **volume réel** (le « ~250 k » est à
   confirmer, il dimensionne le run).
2. Comment étendre la graine sans casser la provenance du run.
3. La **ré-injection hebdomadaire** : V1 est gelé donc l'ensemble est un stock fixe, mais
   `item_cache` reconstruit son périmètre à chaque run ; sans ré-injection, le repli réapparaît la
   semaine suivante. Deux options : fichier d'extension persistant unifié à la graine, ou pass2
   enseigné à toujours référencer ce stock.

**Réalités opérationnelles (ne pas les découvrir en route).**
- `item_cache` relit le **dump complet (~102 Go)** : le run se fait **sur le VPS**, là où le dump
  est présent sur le volume partagé, pas en local. Le code de la graine se prépare en local.
- Le calcul de l'ensemble et la vérification `test-017` demandent la **base MariaDB** (VPS).
- Si le dump a changé depuis le dernier run, l'`AGENTS.md` de ce dépôt impose un **run complet**,
  pas un resume partiel (sinon la provenance du lot devient indicible). Vérifier la taille du dump
  contre `strwikidatacrawlerdumpsize` avant de relancer.

**Acceptance de -023.** Ensemble cible chiffré par une requête conservée ; après import et re-run,
`test-017-repli-v1-taux.sql` rend un taux résiduel documenté et ventilé (absent du dump, sans
libellé) ; la ré-injection hebdomadaire est en place et vérifiée sur un run suivant.
