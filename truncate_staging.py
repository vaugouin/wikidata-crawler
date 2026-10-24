#!/usr/bin/env python3
"""Vide les tables STG_* apres un run reussi et sauvegarde (WIKIDATA-CRAWLER-030).

POURQUOI. Apres l'etape 115, le staging garde le lot qui vient d'etre charge :
plus de 100 M lignes que plus rien ne lit. Le front ne le lit pas, la sauvegarde
l'exclut (backupvaugouindb.sh ignore STG_%), et le run suivant le recharge depuis
ses propres NDJSON a l'etape 108. Il ne sert qu'a reprendre un run en cours
(--start-step 110), et un run reussi n'a plus rien a reprendre.

POURQUOI TRUNCATE ET PAS DELETE. Un DELETE InnoDB ne rend pas la place au disque :
le fichier .ibd garde sa taille, c'est ce que fait l'etape 115. TRUNCATE recree
la table, donc un fichier vide, et la place revient au systeme de fichiers. La
structure reste : l'etape 108 suivante n'a rien a recreer. Effet de bord utile,
le run suivant ne porte plus deux lots dans le staging, mais un seul.

TROIS GARDES, toutes necessaires :
  1. le lot passe en argument est charge dans T_WC_WIKIDATA_STATEMENT ;
  2. aucune ligne de staging ne porte un lot plus recent (un run en cours) ;
  3. le lot est celui de la sentinelle et de la sauvegarde, ce que
     run-if-new-dump.sh verifie avant d'appeler ce script.

USAGE
    python truncate_staging.py --batch <IMPORT_BATCH_ID>            # vide
    python truncate_staging.py --batch <IMPORT_BATCH_ID> --dry-run  # dit ce qu'il viderait

CODES DE RETOUR
    0  fait, ou rien a faire
    1  refus d'une garde
    2  erreur
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import citizenphil as cp  # noqa: E402

CRAWLER_PREFIX = "strwikidatacrawler"
STAGING_PATTERN = "STG!_T!_WC!_WIKIDATA!_%"


def scalar(cursor, sql: str, args: tuple = ()) -> int:
    cursor.execute(sql, args)
    row = cursor.fetchone()
    return int(next(iter(row.values()))) if row else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--batch", required=True, help="IMPORT_BATCH_ID du run reussi et sauvegarde")
    parser.add_argument("--dry-run", action="store_true", help="dire ce qui serait vide, sans rien vider")
    args = parser.parse_args()
    batch = args.batch.strip()
    if not batch:
        print("REFUS : lot vide.")
        return 1

    try:
        connection = cp.f_getconnection()
        with connection.cursor() as cursor:
            if scalar(cursor, "SELECT EXISTS(SELECT 1 FROM T_WC_WIKIDATA_STATEMENT WHERE IMPORT_BATCH_ID = %s) AS P", (batch,)) <= 0:
                print(f"REFUS : le lot {batch} n'est pas charge dans T_WC_WIKIDATA_STATEMENT.")
                return 1
            if scalar(cursor, "SELECT EXISTS(SELECT 1 FROM STG_T_WC_WIKIDATA_STATEMENT WHERE IMPORT_BATCH_ID > %s) AS P", (batch,)) > 0:
                print(f"REFUS : le staging porte un lot plus recent que {batch}, un run est en cours.")
                return 1

            cursor.execute(
                "SELECT TABLE_NAME, ROUND((DATA_LENGTH + INDEX_LENGTH + DATA_FREE) / POW(1024, 3), 1) AS GB "
                "FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE %s ESCAPE '!' "
                "ORDER BY TABLE_NAME",
                (STAGING_PATTERN,),
            )
            tables = [(row["TABLE_NAME"], float(row["GB"] or 0)) for row in cursor.fetchall()]

            to_truncate = []
            for table_name, gb in tables:
                if scalar(cursor, f"SELECT EXISTS(SELECT 1 FROM {table_name}) AS P") > 0:
                    to_truncate.append((table_name, gb))
            if not to_truncate:
                print("Staging deja vide, rien a faire.")
                return 0

            total = sum(gb for _, gb in to_truncate)
            verb = "je viderais" if args.dry_run else "vidage de"
            print(f"Lot {batch} : {verb} {len(to_truncate)} tables STG_*, environ {total:.1f} Go sur disque :")
            for table_name, gb in to_truncate:
                print(f"    {table_name:50s} {gb:8.1f} Go")
            if args.dry_run:
                return 0

            for table_name, _ in to_truncate:
                cursor.execute(f"TRUNCATE TABLE {table_name}")
        cp.f_setservervariable(
            f"{CRAWLER_PREFIX}stagingtruncatebatchid",
            batch,
            "Import batch after which the STG_* tables were truncated (WIKIDATA-CRAWLER-030)",
            0,
        )
        cp.f_setservervariable(
            f"{CRAWLER_PREFIX}stagingtruncategb",
            f"{total:.1f}",
            "Approximate disk space returned by the STG_* truncation, in GB (WIKIDATA-CRAWLER-030)",
            0,
        )
        print(f"Staging vide, environ {total:.1f} Go rendus au disque.")
        return 0
    except Exception as err:  # noqa: BLE001
        print(f"ERREUR : {err}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
