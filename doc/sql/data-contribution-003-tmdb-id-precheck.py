# /// script
# requires-python = ">=3.10"
# dependencies = ["requests"]
# ///
"""Read-only pre-check before adding IMDb and Wikidata ids to existing TMDb persons.

Input: the JSON written by data-contribution-003-tmdb-person-check.py. Keeps the
persons found on TMDb by name and credits but carrying no IMDb id
(status present_without_imdb). For each one, asks the live TMDb API:

- /find/{imdb}?external_source=imdb_id: is the IMDb id already on another TMDb person?
- /find/{qid}?external_source=wikidata_id: same for the Wikidata QID;
- /person/{id}/external_ids: are the two fields still empty on the target person?

An edit is safe only when imdb_used and qid_used are empty and cur_imdb and
cur_wd are null; anything else is reviewed by hand, never written.
The API token is read from tmdb-crawler/.env and never printed.

Usage (from the repository root):
    uv run doc/sql/data-contribution-003-tmdb-id-precheck.py         doc/sql/archive/data-contribution-003-tmdb-person-check-20261105.json         doc/sql/data-contribution-003-tmdb-id-precheck-YYYYMMDD.json

First run 2026-11-05: 35 persons, all safe; result in archive/. That run skipped
rank 1 (G. Devarajan), identified beforehand by Philippe with person-search.php.
The edits themselves are documented in doc/data-contribution-003-tmdb-persons.md.
"""
import json
import sys
import time
from pathlib import Path

import requests

CHECK = Path(sys.argv[1])
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
    r = S.get("https://api.themoviedb.org/3" + path, params=params, timeout=30)
    r.raise_for_status()
    time.sleep(0.1)
    return r.json()


out = []
for x in json.loads(CHECK.read_text(encoding="utf-8")):
    if x["status"] != "present_without_imdb":
        continue
    f1 = get(f"/find/{x['imdb']}", external_source="imdb_id")["person_results"]
    f2 = get(f"/find/{x['qid']}", external_source="wikidata_id")["person_results"]
    ext = get(f"/person/{x['tmdb_id']}/external_ids")
    o = dict(
        rank=x["rank"], tmdb=x["tmdb_id"], imdb=x["imdb"], qid=x["qid"], name=x["name"],
        shared=x["already_on_tmdb"],
        imdb_used=[p["id"] for p in f1], qid_used=[p["id"] for p in f2],
        cur_imdb=ext.get("imdb_id"), cur_wd=ext.get("wikidata_id"),
    )
    o["safe"] = not o["imdb_used"] and not o["qid_used"] and not o["cur_imdb"] and not o["cur_wd"]
    out.append(o)
    print(o["rank"], o["tmdb"], o["imdb"], o["qid"], o["shared"], "safe" if o["safe"] else "REVIEW")

OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
print(len(out), "persons,", sum(o["safe"] for o in out), "safe")
