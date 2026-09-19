#!/usr/bin/env python3
"""Fabrique la graine d'items a remettre dans le cache V2 : le reliquat que seul V1 sert.

WIKIDATA-CRAWLER-023, execution de la route A de la decommission V1.

POURQUOI CE SCRIPT EXISTE. Au 2026-09-19, 36,2 % des libelles francais affiches
(250 185 sur 691 320, test-017-repli-v1-taux.sql) viennent encore du repli sur
T_WC_WIKIDATA_ITEM_V1 : ces entites ont une ligne dans V1 et aucune dans
T_WC_WIKIDATA_ITEM. Supprimer les tables V1 les ferait disparaitre de l'ecran.
Aucun correctif cote lecture ne peut les servir depuis V2, il faut les y faire
exister, et la seule chose qui fabrique une ligne T_WC_WIKIDATA_ITEM est la passe
item_cache, qui n'emet que pour les entites presentes dans son filtre d'items
references (wikidata_dump_etl.py, referenced_item_ids_filter). Ce script produit
l'extension de ce filtre.

DEUX REQUETES, ET C'EST TOUT LE SUJET.

  * LA GRAINE est ancree sur V1 SEUL : tout ID_WIKIDATA de T_WC_WIKIDATA_ITEM_V1.
    Elle ne regarde pas ce que V2 contient deja.
  * LA MESURE (--report, et son jumeau lisible doc/sql/wikidata-v1-backfill-target-set.sql)
    est la difference : ce que V1 detient et que V2 n'a pas.

NE JAMAIS SEMER LA DIFFERENCE. Elle s'auto-efface : apres un premier import reussi
elle rend zero, la graine se vide, ces items sortent du filtre au run suivant, et
leurs faits sont supprimes par l'etape 114 (qui purge tout statement d'un lot plus
ancien). Le tout sous un run en succes, sans rien dans les journaux. La difference
sert a chiffrer, l'ancrage V1 sert a semer.

CE QUE LA RE-INJECTION PROTEGE VRAIMENT, car le ticket le disait autrement. Les
tables d'entites ne portent pas d'IMPORT_BATCH_ID et ne sont jamais purgees
(08_cleanup_old_batches.sql) : une ligne T_WC_WIKIDATA_ITEM importee une fois reste.
Le libelle ne retomberait donc pas sur V1 la semaine suivante. Ce que l'absence de
re-injection detruit, c'est (1) les faits P31/P279/P345/P569/P570/P577 de ces items,
qui portent un lot et que l'etape 114 supprime des le run suivant, ce qui recree
chaque semaine le symptome de WIKIDATA-CRAWLER-020 (un libelle sans un seul fait),
(2) la fraicheur des libelles, geles a la date d'import, et (3) la reproductibilite,
un 04_reset_for_full_rerun.sql perdant tout. D'ou la reconstruction a chaque run.

POURQUOI RELIRE LA BASE PLUTOT QUE GARDER UN FICHIER. run-if-new-dump.sh vide
/shared entierement a chaque lancement : un fichier persistant n'y survit pas. Le
stock durable existe deja, c'est la table V1 elle-meme, gelee, relue en quelques
secondes.

USAGE
    python build_v1_backfill_seed.py              # ecrit /shared/seed/v1_backfill_item_ids.txt
    python build_v1_backfill_seed.py --report     # ne rien ecrire, ventiler la cible
    python build_v1_backfill_seed.py --shared-dir /shared

Depuis l'hote, sans rien lancer d'autre :
    ./wikidata-crawler.sh --v1-backfill-report

L'etape 106 de wikidata_crawler.py appelle build_seed() en prologue, donc le run
hebdomadaire re-injecte tout seul. V1_BACKFILL_SEED=0 desactive l'extension.

CODES DE RETOUR
    0  la graine est ecrite (ou le rapport est rendu)
    1  plancher non atteint, ou base injoignable : on echoue bruyamment plutot que
       de laisser tourner une passe de 23 h sans son extension
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Set

import pymysql
import pymysql.cursors
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import citizenphil as cp  # noqa: E402

DEFAULT_SHARED_DIR = Path("/shared")
SEED_DIR_NAME = "seed"
SEED_FILE_NAME = "v1_backfill_item_ids.txt"
PROVENANCE_FILE_NAME = "v1_backfill_seed.json"

# Plancher de securite. Une graine anormalement petite veut dire que la lecture de V1
# a echoue a moitie, ou que quelqu'un a commence a vider les tables V1 : dans les deux
# cas il faut s'arreter, pas semer le peu qui reste. Le stock connu est de l'ordre de
# 700 000 identifiants (691 320 lignes francaises au 2026-09-19).
DEFAULT_MIN_SEED_IDS = 100_000

# La graine. Ancree sur V1 seul, toutes langues : la coupure emportera aussi les lignes
# anglaises, et l'ecart de volume est marginal puisque V1 cree ses lignes par paires.
SEED_SQL = """
SELECT DISTINCT ID_WIKIDATA
FROM   T_WC_WIKIDATA_ITEM_V1
WHERE  COALESCE(DELETED, 0) = 0
  AND  ID_WIKIDATA LIKE 'Q%'
"""

# La mesure. Jumelle de doc/sql/wikidata-v1-backfill-target-set.sql, qui est la version
# a lire et a rejouer a la main ; celle-ci sert au rapport d'un seul coup.
VENTILATION_SQL = """
SELECT
    COUNT(*)                                        AS lignes_v1_fr,
    SUM(dans_item)                                  AS deja_dans_item,
    SUM(dans_item = 0 AND dans_autre_v2 = 1)        AS ailleurs_en_v2_plancher,
    SUM(dans_item = 0 AND dans_autre_v2 = 0)        AS cible_importable
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
) t
"""

# Un Q-id, et rien d'autre. V1 a ete rempli par deux crawlers SPARQL sur des annees :
# une ligne mal formee ne doit pas partir dans un filtre que la passe compare a chacune
# des 120 millions d'entites du dump.
QID_RE = re.compile(r"^Q[1-9][0-9]*$")


def seed_enabled() -> bool:
    """Interrupteur explicite, pour pouvoir lancer un run sans l'extension."""
    return os.environ.get("V1_BACKFILL_SEED", "1").strip().lower() not in ("0", "false", "no", "off")


def min_seed_ids() -> int:
    try:
        return int(os.environ.get("V1_BACKFILL_MIN_IDS", str(DEFAULT_MIN_SEED_IDS)))
    except ValueError:
        return DEFAULT_MIN_SEED_IDS


def seed_path(shared_dir: Path = DEFAULT_SHARED_DIR) -> Path:
    return Path(shared_dir) / SEED_DIR_NAME / SEED_FILE_NAME


def _connect(streaming: bool = False) -> pymysql.connections.Connection:
    """Connexion dediee. La collation est posee a l'ouverture : la regle du depot veut
    que le client s'ouvre en unicode_ci, faute de quoi toute valeur produite par une
    fonction herite de general_ci et se compare mal a une colonne unicode_ci (1267)."""
    return pymysql.connect(
        host=os.environ.get("DB_HOST", ""),
        port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ.get("DB_USER", ""),
        password=os.environ.get("DB_PASSWORD", ""),
        database=os.environ.get("DB_NAME", ""),
        charset="utf8mb4",
        init_command="SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci",
        autocommit=True,
        cursorclass=pymysql.cursors.SSCursor if streaming else pymysql.cursors.Cursor,
    )


def fetch_seed_ids(tentatives: int = 3) -> Dict[str, object]:
    """Lit V1 en flux. 700 000 identifiants tiennent en memoire, mais les lire d'un
    bloc ferait porter au serveur un jeu de resultats complet pour rien.

    Trois tentatives, parce que l'appelant est l'etape 106 : elle demarre apres deux
    passes de dump, soit une soixantaine d'heures de calcul deja payees, et une coupure
    d'une seconde ne doit pas les couter. Si les trois echouent, on laisse l'exception
    remonter : un run qui s'arrete est reprenable, un run qui n'a rien seme ne se voit
    pas."""
    derniere: Optional[Exception] = None
    for tentative in range(1, tentatives + 1):
        ids: Set[str] = set()
        rejected = 0
        try:
            connection = _connect(streaming=True)
            try:
                with connection.cursor() as cursor:
                    cursor.execute(SEED_SQL)
                    for row in cursor:
                        value = (row[0] or "").strip()
                        if QID_RE.match(value):
                            ids.add(value)
                        elif value:
                            rejected += 1
            finally:
                connection.close()
            return {"ids": ids, "rejected": rejected}
        except Exception as exc:
            derniere = exc
            print(f"Lecture de la graine V1, tentative {tentative}/{tentatives} echouee : {exc}",
                  file=sys.stderr)
            if tentative < tentatives:
                time.sleep(5 * tentative)
    raise RuntimeError(f"Lecture de T_WC_WIKIDATA_ITEM_V1 impossible apres {tentatives} tentatives : {derniere}")


def build_seed(shared_dir: Path = DEFAULT_SHARED_DIR, *, batch_id: str = "") -> Dict[str, object]:
    """Ecrit la graine et son fichier de provenance, et rend les compteurs.

    Leve une exception si le plancher n'est pas atteint : mieux vaut un run qui
    s'arrete tout de suite qu'une passe de 23 h qui reussit sans avoir seme."""
    result = fetch_seed_ids()
    ids: Set[str] = result["ids"]  # type: ignore[assignment]
    floor = min_seed_ids()
    if len(ids) < floor:
        raise RuntimeError(
            f"Graine V1 anormalement petite : {len(ids)} identifiants lus dans "
            f"T_WC_WIKIDATA_ITEM_V1, plancher {floor}. La lecture a echoue, ou les tables "
            f"V1 sont en cours de suppression. On ne seme pas : verifier la base, ou poser "
            f"V1_BACKFILL_SEED=0 pour lancer un run sans l'extension."
        )

    target = seed_path(shared_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as fh:
        for qid in sorted(ids):
            fh.write(qid + "\n")

    provenance = {
        "ticket": "WIKIDATA-CRAWLER-023",
        "built_at": datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S"),
        "import_batch_id": batch_id,
        "source_query": " ".join(SEED_SQL.split()),
        "ids_seeded": len(ids),
        "ids_rejected_not_a_qid": result["rejected"],
        "seed_file": str(target),
    }
    (target.parent / PROVENANCE_FILE_NAME).write_text(
        json.dumps(provenance, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return provenance


def ventilate() -> Optional[Dict[str, int]]:
    """La mesure : ou en est le reliquat que seul V1 sert."""
    connection = _connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(VENTILATION_SQL)
            row = cursor.fetchone()
    finally:
        connection.close()
    if not row:
        return None
    return {
        "lignes_v1_fr": int(row[0] or 0),
        "deja_dans_item": int(row[1] or 0),
        "ailleurs_en_v2_plancher": int(row[2] or 0),
        "cible_importable": int(row[3] or 0),
    }


def _espace(nombre: int) -> str:
    return f"{nombre:,}".replace(",", " ")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--shared-dir", default=str(DEFAULT_SHARED_DIR),
                        help="volume partage ou ecrire la graine (defaut: /shared)")
    parser.add_argument("--report", action="store_true",
                        help="ne rien ecrire : ventiler la cible et s'arreter")
    args = parser.parse_args()

    if args.report:
        try:
            mesure = ventilate()
        except Exception as exc:
            print(f"ECHEC : base injoignable ou requete refusee ({exc}).", file=sys.stderr)
            return 1
        if mesure is None:
            print("ECHEC : la requete de ventilation n'a rien rendu.", file=sys.stderr)
            return 1
        total = mesure["lignes_v1_fr"] or 1
        print("=== WIKIDATA-CRAWLER-023 : ou en est le reliquat que seul V1 sert ===")
        print(f"  lignes V1 francaises (non vides)  : {_espace(mesure['lignes_v1_fr']):>11}")
        print(f"  deja dans T_WC_WIKIDATA_ITEM      : {_espace(mesure['deja_dans_item']):>11}"
              f"  ({100 * mesure['deja_dans_item'] / total:.1f} %)")
        print(f"  cible importable (absente de V2)  : {_espace(mesure['cible_importable']):>11}"
              f"  ({100 * mesure['cible_importable'] / total:.1f} %)")
        print(f"  PLANCHER, ailleurs en V2          : {_espace(mesure['ailleurs_en_v2_plancher']):>11}"
              f"  ({100 * mesure['ailleurs_en_v2_plancher'] / total:.1f} %)")
        print()
        print("  Le plancher, ce sont les Q-ids que V2 detient deja comme film, serie,")
        print("  personne, saison, episode ou personnage. f_getwikidatalabel ne lit que")
        print("  T_WC_WIKIDATA_ITEM (TMDB-MOVIE-PREPROCESS-036), et item_cache refuse")
        print("  d'ecrire une entite du perimetre coeur dans ITEM : cette route ne peut")
        print("  pas les servir, quoi qu'on seme. A remonter a -022 comme plancher acte.")
        return 0

    if not seed_enabled():
        print("V1_BACKFILL_SEED=0 : graine desactivee, rien a faire.")
        return 0

    try:
        provenance = build_seed(Path(args.shared_dir),
                                batch_id=os.environ.get("IMPORT_BATCH_ID", ""))
    except Exception as exc:
        print(f"ECHEC : {exc}", file=sys.stderr)
        return 1

    print(f"Graine ecrite : {provenance['seed_file']}")
    print(f"  identifiants   : {provenance['ids_seeded']}")
    if provenance["ids_rejected_not_a_qid"]:
        print(f"  ecartes (pas un Q-id) : {provenance['ids_rejected_not_a_qid']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
