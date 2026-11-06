# /// script
# requires-python = ">=3.10"
# dependencies = ["requests"]
# ///
"""Read-only check of the DATA-CONTRIBUTION-003 person worksheet against the live TMDb API.

For each person: find by IMDb id, else search by name and keep the candidate whose
TMDb credits overlap the Wikidata credits. Then list the worksheet credits TMDb lacks.
The API token is read from tmdb-crawler/.env and never printed.

Usage (from the repository root):
    uv run doc/sql/data-contribution-003-tmdb-person-check.py         doc/sql/data-contribution-003-person-worksheet-20261105.txt         doc/sql/data-contribution-003-tmdb-person-check-YYYYMMDD.json

Statuses: present_with_imdb (TMDb has an IMDb id, compare it with Wikidata's),
present_without_imdb (found by name, confirmed by shared credits), absent
(no IMDb match and no name candidate sharing a credit: a creation candidate).
First run 2026-11-05 on the top 50: 49 present, 1 absent; result in archive/.
Next step: data-contribution-003-tmdb-id-precheck.py; full procedure and edit log in
doc/data-contribution-003-tmdb-persons.md.
"""
import json
import re
import sys
import time
from pathlib import Path

import requests

WORKSHEET = Path(sys.argv[1])
OUT = Path(sys.argv[2])
ENV = Path(r"C:\Users\vaugo\Code\tmdb-crawler\.env")

token = None
for line in ENV.read_text(encoding="utf-8").splitlines():
    if line.startswith("TMDB_API_TOKEN="):
        token = line.split("=", 1)[1].strip().strip('"').strip("'")
if not token:
    sys.exit("TMDB_API_TOKEN missing")
S = requests.Session()
S.headers["Authorization"] = "Bearer " + token
S.headers["accept"] = "application/json"


def get(path, **params):
    for _ in range(3):
        r = S.get("https://api.themoviedb.org/3" + path, params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(2)
            continue
        r.raise_for_status()
        time.sleep(0.05)
        return r.json()
    r.raise_for_status()


def rows(section_start, section_end):
    text = WORKSHEET.read_text(encoding="utf-8")
    block = text.split(section_start, 1)[1]
    if section_end:
        block = block.split(section_end, 1)[0]
    out = []
    for line in block.splitlines():
        if re.match(r"^\|\s+\d+\s+\|", line):
            out.append([c.strip() for c in line.strip().strip("|").split("|")])
    return out


persons = rows("P. Persons:", "C. Credits")
credits = rows("C. Credits", None)
by_person = {}
for c in credits:
    # RANK_, PERSON_QID, TMDB_TYPE, ID_TMDB, TITLE, YEAR_, JOB, CHARACTER_, WORK_QID
    by_person.setdefault(c[1], []).append(c)


def credit_keys(pid):
    cc = get(f"/person/{pid}/combined_credits")
    return {(x["media_type"], str(x["id"])) for x in cc.get("cast", []) + cc.get("crew", [])}


results = []
for p in persons:
    rank, qid, imdb, name = p[0], p[1], p[2], p[3]
    wd = by_person.get(qid, [])
    wd_keys = {(c[2], c[3]) for c in wd}
    status, pid, tmdb_name, tmdb_imdb, overlap, keys = "absent", None, None, None, 0, set()
    found = get(f"/find/{imdb}", external_source="imdb_id").get("person_results", [])
    if found:
        pid = found[0]["id"]
        status = "present_with_imdb"
        keys = credit_keys(pid)
    else:
        best = None
        for cand in get("/search/person", query=name).get("results", [])[:8]:
            k = credit_keys(cand["id"])
            ov = len(k & wd_keys)
            if ov and (best is None or ov > best[1]):
                best = (cand["id"], ov, k)
        if best:
            pid, _, keys = best
            status = "present_without_imdb"
    if pid:
        det = get(f"/person/{pid}", append_to_response="external_ids")
        tmdb_name = det.get("name")
        tmdb_imdb = (det.get("external_ids") or {}).get("imdb_id")
        wikidata = (det.get("external_ids") or {}).get("wikidata_id")
        if status == "present_without_imdb" and tmdb_imdb:
            status = "present_with_imdb"
    else:
        wikidata = None
    overlap = len(keys & wd_keys)
    missing = [c for c in wd if (c[2], c[3]) not in keys]
    results.append({
        "rank": int(rank), "qid": qid, "imdb": imdb, "name": name, "status": status,
        "tmdb_id": pid, "tmdb_name": tmdb_name, "tmdb_imdb": tmdb_imdb, "tmdb_wikidata": wikidata,
        "tmdb_credits": len(keys), "wikidata_credits_on_tmdb_works": len(wd_keys),
        "already_on_tmdb": overlap, "missing": [
            {"type": c[2], "tmdb_id": c[3], "title": c[4], "year": c[5], "job": c[6], "character": c[7]}
            for c in missing],
    })
    print(f"{rank:>3} {status:<22} {str(pid):>8} {name}", flush=True)

OUT.write_text(json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8")
