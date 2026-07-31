#!/usr/bin/env python3
"""Renumber an existing pass2 output so the qualifiers stop collapsing.

WIKIDATA-CRAWLER-019. Until 2026-07-31 the ETL keyed a qualifier on the `hash` of its
own snak. That hash covers the snak's CONTENT (property + value), not its occurrence, so
"P1545 = 1" carried the same hash on every one of the 173k episodes numbered 1. Every
occurrence therefore got the same ID_STATEMENT_QUALIFIER and the same QUALIFIER_HASH, and
the UNIQUE KEY over QUALIFIER_HASH kept exactly one of them. The table ended up holding
one row per distinct VALUE instead of one row per occurrence: 2,4 % of P166 award
statements still had their P585 date, and the episode ordinals were gone.

The ETL is fixed. This script exists so the FIX CAN BE APPLIED WITHOUT RESCANNING THE
DUMP. The pass2 NDJSON already contains every occurrence as its own line (the collapse
happened at load time, in the database, not at emission), and each qualifier line carries
its parent ID_STATEMENT. That is all the new identity needs, so the files can simply be
renumbered in place. Reload then starts at step 108 (~13 h) instead of step 104 (~2 days).

What it rewrites, in /shared/pass2:

    T_WC_WIKIDATA_STATEMENT_QUALIFIER.jsonl   ID_STATEMENT_QUALIFIER, QUALIFIER_HASH
    T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE.jsonl        keyed by the id above, so each
    T_WC_WIKIDATA_QUALIFIER_STRING_VALUE.jsonl      old key fans out into as many
    T_WC_WIKIDATA_QUALIFIER_EXTERNAL_ID_VALUE.jsonl rows as there were occurrences
    T_WC_WIKIDATA_QUALIFIER_MEDIA_VALUE.jsonl
    T_WC_WIKIDATA_QUALIFIER_TIME_VALUE.jsonl
    T_WC_WIKIDATA_QUALIFIER_QUANTITY_VALUE.jsonl

The fan-out is safe because all occurrences sharing an old id share the same value BY
CONSTRUCTION: that identity was the value's own hash. So one value row per old id is
enough to rebuild them all.

Originals are kept as `<name>.jsonl.before-019`. The script never touches the database.

Usage (inside the crawler container, or anywhere the NDJSON is readable):

    python migrations/repair_qualifier_identity.py --dir /shared/pass2 --dry-run
    python migrations/repair_qualifier_identity.py --dir /shared/pass2

Then, in the database, drop the collapsed rows and reload from the repaired files:

    mariadb vaugouindb < 06_repair_qualifier_tables.sql     # empties the 7 qualifier tables
    python wikidata_crawler.py --start-step 108             # staging load, then bulk load

Expect the qualifier tables to grow by roughly an order of magnitude: they held 1 389 536
rows for 37,2 M statements, which was the count of distinct VALUES, not of occurrences.
The report printed at the end gives the exact figure before you commit to the reload.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Dict, Iterator, Tuple

QUALIFIER_FILE = "T_WC_WIKIDATA_STATEMENT_QUALIFIER.jsonl"

VALUE_FILES = (
    "T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE.jsonl",
    "T_WC_WIKIDATA_QUALIFIER_STRING_VALUE.jsonl",
    "T_WC_WIKIDATA_QUALIFIER_EXTERNAL_ID_VALUE.jsonl",
    "T_WC_WIKIDATA_QUALIFIER_MEDIA_VALUE.jsonl",
    "T_WC_WIKIDATA_QUALIFIER_TIME_VALUE.jsonl",
    "T_WC_WIKIDATA_QUALIFIER_QUANTITY_VALUE.jsonl",
)

BACKUP_SUFFIX = ".before-019"


def stable_bigint_from_text(text: str) -> int:
    """Byte-for-byte the same function as wikidata_dump_etl.stable_bigint_from_text.

    Duplicated on purpose: this script must stay runnable on an old checkout, next to a
    pass2 output produced by the ETL as it was. Any drift between the two would silently
    produce ids the next ETL run would not match, so the smoke test pins them together.
    """
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF
    return value or 1


def new_identity(statement_id: int, qualifier_property_id: str, occurrence_key: str) -> Tuple[int, str]:
    """Mirror of wikidata_dump_etl.derive_qualifier_identity, from NDJSON fields."""
    qualifier_hash = f"qualifier|{statement_id}|{qualifier_property_id}|{occurrence_key}"
    return stable_bigint_from_text(qualifier_hash), qualifier_hash


def occurrence_key_from_old_hash(old_hash: str) -> str:
    """Recover the snak key the old identity was built from.

    The old QUALIFIER_HASH was `"qualifier|" + snak_hash` (or, when the snak had no hash,
    `"qualifier|" + guid|property|order|json`). Stripping the prefix gives back exactly
    the string the new identity has to reuse, so a repaired file and a fresh ETL run
    converge on the same ids.
    """
    return old_hash[len("qualifier|"):] if old_hash.startswith("qualifier|") else old_hash


def read_jsonl(path: Path) -> Iterator[dict]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{path}:{line_number}: JSON illisible: {exc}")


def repair(directory: Path, dry_run: bool) -> int:
    qualifier_path = directory / QUALIFIER_FILE
    if not qualifier_path.exists():
        print(f"ERREUR: {qualifier_path} est introuvable.", file=sys.stderr)
        print("        La sortie pass2 n'est plus sur le disque : il faut alors relancer", file=sys.stderr)
        print("        le pipeline depuis l'etape 104 (python wikidata_crawler.py --start-step 104).", file=sys.stderr)
        return 1

    # Pass 1: read the parent file, build old id -> [new ids]. Only the ids are kept in
    # memory (two int64 per occurrence), never the rows themselves.
    print(f"Lecture de {qualifier_path.name} ...")
    remap: Dict[int, list] = {}
    occurrences = 0
    collisions = 0
    seen_new: set = set()
    repaired_rows = []

    for row in read_jsonl(qualifier_path):
        occurrences += 1
        old_id = int(row["ID_STATEMENT_QUALIFIER"])
        statement_id = int(row["ID_STATEMENT"])
        prop = row["ID_QUALIFIER_PROPERTY"]
        key = occurrence_key_from_old_hash(row.get("QUALIFIER_HASH") or "")
        nid, nhash = new_identity(statement_id, prop, key)
        if nid in seen_new:
            collisions += 1
        seen_new.add(nid)
        remap.setdefault(old_id, []).append(nid)
        row["ID_STATEMENT_QUALIFIER"] = nid
        row["QUALIFIER_HASH"] = nhash
        repaired_rows.append(row)

    distinct_old = len(remap)
    distinct_new = len(seen_new)
    print(f"  occurrences lues                : {occurrences:>12,}")
    print(f"  identifiants distincts AVANT    : {distinct_old:>12,}   <- ce que la base a garde")
    print(f"  identifiants distincts APRES    : {distinct_new:>12,}")
    print(f"  collisions residuelles          : {collisions:>12,}")
    if occurrences:
        print(f"  facteur de recuperation         : {occurrences / max(distinct_old, 1):>12.1f}x")

    if collisions:
        print("\nATTENTION: des collisions subsistent apres renumerotation. Elles signalent")
        print("deux occurrences que (statement, propriete, snak) ne separe pas, ce qui ne")
        print("devrait pas exister dans un dump Wikidata. Ne pas charger sans comprendre.")
        return 2

    if dry_run:
        print("\n--dry-run: aucun fichier ecrit.")
        return 0

    # Parent file.
    shutil.copy2(qualifier_path, qualifier_path.with_name(qualifier_path.name + BACKUP_SUFFIX))
    with qualifier_path.open("w", encoding="utf-8") as out:
        for row in repaired_rows:
            out.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
            out.write("\n")
    print(f"\n{qualifier_path.name}: reecrit ({occurrences:,} lignes)")
    del repaired_rows

    # Value files: one stored row per old id, fanned out over its new ids.
    for name in VALUE_FILES:
        path = directory / name
        if not path.exists():
            print(f"{name}: absent, ignore")
            continue
        payload_by_old: Dict[int, dict] = {}
        for row in read_jsonl(path):
            payload_by_old.setdefault(int(row["ID_STATEMENT_QUALIFIER"]), row)
        shutil.copy2(path, path.with_name(path.name + BACKUP_SUFFIX))
        written = 0
        orphans = 0
        with path.open("w", encoding="utf-8") as out:
            for old_id, row in payload_by_old.items():
                new_ids = remap.get(old_id)
                if not new_ids:
                    orphans += 1
                    continue
                for nid in new_ids:
                    row["ID_STATEMENT_QUALIFIER"] = nid
                    out.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
                    out.write("\n")
                    written += 1
        note = f", {orphans} valeur(s) sans parent ignoree(s)" if orphans else ""
        print(f"{name}: {len(payload_by_old):,} valeur(s) -> {written:,} ligne(s){note}")

    print("\nTermine. Originaux conserves en *.jsonl" + BACKUP_SUFFIX + ".")
    print("Suite: vider les 7 tables de qualificatifs (06_repair_qualifier_tables.sql),")
    print("       puis python wikidata_crawler.py --start-step 108")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--dir", default="/shared/pass2", type=Path,
                        help="repertoire de sortie de pass2 (defaut: /shared/pass2)")
    parser.add_argument("--dry-run", action="store_true",
                        help="compter et rapporter sans rien reecrire")
    args = parser.parse_args()
    return repair(args.dir, args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
