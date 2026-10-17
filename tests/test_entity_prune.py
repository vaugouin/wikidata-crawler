#!/usr/bin/env python3
"""Smoke test for the step-114 entity prune guard (WIKIDATA-CRAWLER-024).

No database: it pins entity_prune_refusal and entity_prune_enabled, the two pure
pieces that decide whether step 114 may delete entity rows, on the figures measured
2026-10-17 (check-024) and on the two failures the guard exists for.

Run:  python tests/test_entity_prune.py     (exit 0 = pass, 1 = fail)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
os.environ.pop("DB_HOST", None)

import wikidata_crawler as wc  # noqa: E402

failures = 0


def check(condition: bool, message: str) -> None:
    global failures
    print(("ok   " if condition else "FAIL ") + message)
    if not condition:
        failures += 1


refusal = wc.entity_prune_refusal

# The first backlog, 2026-10-17: every table must pass.
measured = {
    "T_WC_WIKIDATA_ITEM": (887_560, 40_224),
    "T_WC_WIKIDATA_SERIE": (179_463, 11_942),
    "T_WC_WIKIDATA_PERSON": (792_051, 1_120),
    "T_WC_WIKIDATA_MOVIE": (443_451, 872),
    "T_WC_WIKIDATA_CHARACTER": (125_484, 284),
    "T_WC_WIKIDATA_EPISODE": (189_500, 104),
    "T_WC_WIKIDATA_SEASON": (28_520, 67),
}
for table, (total, orphans) in measured.items():
    check(refusal(table, total, total - orphans, orphans) is None, f"{table}: 2026-10-17 backlog accepted")

# A run without the V1 backfill seed would orphan the 110 650 backfilled items.
check(refusal("T_WC_WIKIDATA_ITEM", 887_560, 776_910, 110_650) is not None, "seedless ITEM run refused")
# A staging table the current batch never reached: everything looks orphaned.
check(refusal("T_WC_WIKIDATA_SERIE", 179_463, 0, 179_463) is not None, "empty staging refused")
check("no staging rows" in (refusal("T_WC_WIKIDATA_SERIE", 0, 0, 0) or ""), "empty staging refused even on an empty table")
# Half-loaded staging.
check(refusal("T_WC_WIKIDATA_PERSON", 792_051, 396_000, 396_051) is not None, "half-loaded staging refused")
# Steady state, nothing to delete.
check(refusal("T_WC_WIKIDATA_MOVIE", 443_451, 443_451, 0) is None, "nothing to prune accepted")

# Every entity table is covered, each with its own staging table.
targets = [t for t, _ in wc.ENTITY_PRUNE_TABLES]
check(len(set(targets)) == 7 and all(s == "STG_" + t for t, s in wc.ENTITY_PRUNE_TABLES), "seven tables, paired with STG_<table>")

for value, expected in (("0", False), ("off", False), ("1", True), ("", True)):
    os.environ["ENTITY_PRUNE"] = value
    check(wc.entity_prune_enabled() is expected, f"ENTITY_PRUNE={value!r} -> {expected}")
os.environ.pop("ENTITY_PRUNE", None)
check(wc.entity_prune_enabled(), "enabled by default")

print("PASS" if failures == 0 else f"{failures} FAILURE(S)")
sys.exit(1 if failures else 0)
