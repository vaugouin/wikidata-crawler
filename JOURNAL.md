# JOURNAL

Mémoire opérationnelle du dépôt, antéchronologique (l'entrée la plus récente en haut).
Au démarrage d'une session, lire les premières entrées pour retrouver le contexte, sans
charger tout le fichier.

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
