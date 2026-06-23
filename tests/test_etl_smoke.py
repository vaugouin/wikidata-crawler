#!/usr/bin/env python3
"""Standalone smoke test for the Wikidata dump ETL classification + emission.

Builds a tiny synthetic .bz2 dump and runs pass1 -> pass2 -> item_cache through
WikidataDumpETL (no database, no network), then asserts:

  * a movie typed with a *subclass* of film (animated film) is detected as core
    EVEN THOUGH the instance appears in the dump BEFORE the class that defines its
    subclass edge  (this is the bug the pass1-sidecar redesign fixes);
  * the new SEASON / EPISODE / CHARACTER entity types are classified and emitted,
    including a character reached only via a P279 descendant (fictional human);
  * the persons-need-IMDb rule still gates the core set but keeps candidates;
  * pass2 emits statements for in-scope entities (incl. P2079);
  * item_cache emits a referenced, non-core value entity to T_WC_WIKIDATA_ITEM.

Run:  python tests/test_etl_smoke.py     (exit 0 = pass, 1 = fail)
"""
from __future__ import annotations

import bz2
import json
import os
import sys
import tempfile
from pathlib import Path

# Import the ETL from the repo root regardless of CWD, and ensure no stray
# DB_* env makes the server-variable writer try to connect.
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
os.environ.pop("DB_HOST", None)

from wikidata_dump_etl import WikidataDumpETL  # noqa: E402


# --- synthetic entity builders ------------------------------------------------

def _item_snak(qid: str) -> dict:
    return {
        "snaktype": "value",
        "datatype": "wikibase-item",
        "datavalue": {"type": "wikibase-entityid", "value": {"entity-type": "item", "id": qid}},
    }


def _string_snak(value: str, datatype: str = "external-id") -> dict:
    return {"snaktype": "value", "datatype": datatype, "datavalue": {"type": "string", "value": value}}


def _claim(snak: dict, guid: str) -> dict:
    return {"mainsnak": snak, "type": "statement", "rank": "normal", "id": guid}


def item(qid: str, *, p31=None, p279=None, imdb=None, p2079=None, label="x") -> dict:
    claims: dict = {}
    if p31:
        claims["P31"] = [_claim(_item_snak(q), f"{qid}$P31-{i}") for i, q in enumerate(p31)]
    if p279:
        claims["P279"] = [_claim(_item_snak(q), f"{qid}$P279-{i}") for i, q in enumerate(p279)]
    if imdb:
        claims["P345"] = [_claim(_string_snak(imdb), f"{qid}$P345-0")]
    if p2079:
        claims["P2079"] = [_claim(_item_snak(q), f"{qid}$P2079-{i}") for i, q in enumerate(p2079)]
    return {
        "type": "item",
        "id": qid,
        "labels": {"en": {"language": "en", "value": label}},
        "descriptions": {},
        "claims": claims,
    }


# Order is deliberate: the subclass-typed movie (Q1001) and the descendant-typed
# character (Q1008) appear BEFORE the class entities (Q202866, Q15632617) that
# define their P279 edges — the situation a single streaming pass cannot resolve.
ENTITIES = [
    item("Q1001", p31=["Q202866"], p2079=["Q184781"], label="Subclass Animated Movie"),
    item("Q1008", p31=["Q15632617"], label="Fictional Human Character"),
    item("Q202866", p279=["Q11424"], label="animated film"),          # -> movie descendant
    item("Q15632617", p279=["Q95074"], label="fictional human"),       # -> character descendant
    item("Q1002", p31=["Q11424"], label="Bare Film Movie"),
    item("Q1003", p31=["Q5"], imdb="nm0000001", label="Person With IMDb"),
    item("Q1004", p31=["Q5"], label="Person Without IMDb"),
    item("Q1005", p31=["Q3464665"], label="A Season"),
    item("Q1006", p31=["Q21191270"], label="An Episode"),
    item("Q1007", p31=["Q95074"], label="A Character"),
    item("Q1009", p31=["Q515"], label="Irrelevant City"),
    item("Q184781", p31=["Q2412849"], label="traditional animation"),  # referenced value, not core
]


def write_dump(path: Path) -> None:
    lines = ["["]
    lines += [json.dumps(e, ensure_ascii=False) + "," for e in ENTITIES]
    lines.append("]")
    path.write_bytes(bz2.compress(("\n".join(lines) + "\n").encode("utf-8")))


# --- helpers to read ETL outputs ---------------------------------------------

def read_ids(path: Path) -> set:
    return set(path.read_text(encoding="utf-8").split()) if path.exists() else set()


def read_jsonl(path: Path) -> list:
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]


def ids_in(path: Path) -> set:
    return {row["ID_WIKIDATA"] for row in read_jsonl(path)}


# --- the test -----------------------------------------------------------------

def run() -> int:
    failures = []

    def check(cond: bool, msg: str) -> None:
        print(("  PASS " if cond else "  FAIL ") + msg)
        if not cond:
            failures.append(msg)

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        dump = tmp / "sample.json.bz2"
        write_dump(dump)
        p1, p2, ic = tmp / "pass1", tmp / "pass2", tmp / "item_cache"

        # pass1 -------------------------------------------------------------
        WikidataDumpETL(
            out_dir=p1, pass_name="pass1", dump_url=None, dump_file=dump,
            class_roots_json=None, core_entity_ids_path=None, referenced_item_ids_path=None,
            candidate_person_ids_path=None, referenced_person_ids_path=None,
        ).run()

        core = read_ids(p1 / "core_entity_ids.txt")
        candidates = read_ids(p1 / "candidate_person_ids.txt")
        print("pass1 core/candidates:")
        check("Q1001" in core, "subclass-typed movie (instance before class) is core [THE BUG FIX]")
        check("Q1002" in core, "bare-root movie is core")
        check("Q1005" in core and "Q1006" in core and "Q1007" in core, "season/episode/character are core")
        check("Q1008" in core, "character via P279 descendant (fictional human) is core")
        check("Q1003" in core, "person with IMDb is core")
        check("Q1004" not in core, "person without IMDb is NOT core")
        check("Q1009" not in core, "irrelevant city is NOT core")
        check("Q184781" not in core, "referenced value entity is NOT core")
        check({"Q1003", "Q1004"} <= candidates, "both persons are candidate persons (incl. no-IMDb)")

        # pass2 -------------------------------------------------------------
        WikidataDumpETL(
            out_dir=p2, pass_name="pass2", dump_url=None, dump_file=dump,
            class_roots_json=p1 / "class_roots.jsonl",
            core_entity_ids_path=p1 / "core_entity_ids.txt",
            referenced_item_ids_path=None,
            candidate_person_ids_path=p1 / "candidate_person_ids.txt",
            referenced_person_ids_path=None,
        ).run()

        print("pass2 emission:")
        check({"Q1001", "Q1002"} <= ids_in(p2 / "T_WC_WIKIDATA_MOVIE.jsonl"), "both movies emitted to MOVIE")
        check("Q1005" in ids_in(p2 / "T_WC_WIKIDATA_SEASON.jsonl"), "season emitted to SEASON")
        check("Q1006" in ids_in(p2 / "T_WC_WIKIDATA_EPISODE.jsonl"), "episode emitted to EPISODE")
        check({"Q1007", "Q1008"} <= ids_in(p2 / "T_WC_WIKIDATA_CHARACTER.jsonl"), "characters emitted to CHARACTER")
        stmts = read_jsonl(p2 / "T_WC_WIKIDATA_STATEMENT.jsonl")
        check(any(s["ID_WIKIDATA"] == "Q1001" and s["ID_PROPERTY"] == "P2079" for s in stmts),
              "P2079 statement emitted for the subclass-typed movie")
        check("Q1009" not in ids_in(p2 / "T_WC_WIKIDATA_MOVIE.jsonl"), "irrelevant city not emitted")
        referenced = read_ids(p2 / "referenced_item_ids.txt")
        check("Q184781" in referenced, "P2079 value recorded as a referenced item")

        # item_cache --------------------------------------------------------
        WikidataDumpETL(
            out_dir=ic, pass_name="item_cache", dump_url=None, dump_file=dump,
            class_roots_json=None,
            core_entity_ids_path=p1 / "core_entity_ids.txt",
            referenced_item_ids_path=p2 / "referenced_item_ids.txt",
            candidate_person_ids_path=None,
            referenced_person_ids_path=p2 / "referenced_person_ids.txt",
        ).run()

        print("item_cache emission:")
        item_ids = ids_in(ic / "T_WC_WIKIDATA_ITEM.jsonl")
        check("Q184781" in item_ids, "referenced non-core value emitted to ITEM cache")
        check("Q1001" not in item_ids, "core entity NOT duplicated into ITEM cache")

    print()
    if failures:
        print(f"RESULT: FAIL ({len(failures)} check(s) failed)")
        return 1
    print("RESULT: PASS (all checks passed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
