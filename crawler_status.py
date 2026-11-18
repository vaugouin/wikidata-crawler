#!/usr/bin/env python3
"""Which step is the crawler on, and what would stopping it now cost (WIKIDATA-CRAWLER-036).

Read-only: a few SELECTs on the server variables, nothing written. Meant to be piped
into a container by crawler-status.sh, so it also answers inside a container whose
image was built before this file existed (it only needs citizenphil):

    docker exec -i wikidata-crawler python - < crawler_status.py

The cost of a stop is the cost of killing the step where it is and resuming at its
beginning (./wikidata-crawler.sh --start-step N). A stop at a step boundary costs
nothing: see stop-at-step-boundary.sh.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import citizenphil as cp

PREFIX = "strwikidatacrawler"
SHARED = Path("/shared")
PASSES = (102, 104, 106)
VALIDATIONS = (103, 105, 107, 109, 111, 113)


def var(name: str) -> str:
    return str(cp.f_getservervariable(PREFIX + name, 0) or "").strip()


def elapsed_since(text: str) -> str:
    try:
        start = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return "unknown"
    seconds = int((datetime.now(cp.paris_tz).replace(tzinfo=None) - start).total_seconds())
    if seconds < 0:
        return "unknown"
    return f"{seconds // 3600} h {seconds % 3600 // 60:02d} min"


def stop_cost(code: int, elapsed: str) -> list[str]:
    """What killing step `code` now, then resuming at its beginning, costs."""
    if code == 101:
        return ["LOW. The download resumes where it stopped (HTTP Range on the .part file).",
                "Run `bash off.sh` first: until step 101 has recorded the dump size, the hourly",
                "check would take the dump for a new one, wipe /shared and start a full run."]
    if code in PASSES:
        return [f"HIGH. The pass restarts from zero: the {elapsed} already spent are lost (a pass",
                "takes 20 to 33 h). The database is untouched, the pass only writes files.",
                "Better: stop at the end of this step (bash stop-at-step-boundary.sh)."]
    if code in VALIDATIONS:
        return ["NONE. Read-only checks, a few seconds to minutes."]
    if code == 108:
        return [f"MEDIUM. The staging load restarts from zero: the {elapsed} already spent are lost.",
                "Resume at 108, never at 110: 110 would load a partial staging."]
    if code == 110:
        return ["MEDIUM, and it touches the live tables. Statements already committed are not redone",
                "(see bulkloadlaststatement below), but the statement in progress rolls back, which",
                "can take long, and some statements stay without their values until 110 has rerun.",
                "Better: let it finish and stop at the boundary."]
    if code == 112:
        return ["LOW. Upserts, the resume redoes them."]
    if code == 114:
        return ["LOW to resume, but the live tables stay partly pruned until 114 has rerun,",
                "and 114 must finish before 115 runs."]
    if code == 115:
        return ["NONE. Chunked deletes, each committed; the resume finishes the job."]
    return ["unknown step."]


def main() -> int:
    status = var("status")
    current = var("currentprocess")
    print(f"status            : {status or '?'}")
    print(f"current process   : {current or '?'}")
    print(f"batch             : {os.environ.get('IMPORT_BATCH_ID', '?')}")
    print(f"run started       : {var('startdatetime')} (start step {var('startstep')})")
    print(f"steps executed    : {var('processesexecuted')}")

    request = SHARED / "STOP_AFTER_STEP"
    if request.exists():
        text = request.read_text(encoding="utf-8", errors="replace").strip()
        print(f"stop request      : PENDING ({'after step ' + text if text else 'after the step in progress'})")
    marker = SHARED / "stopped_run.json"
    if marker.exists():
        print(f"stopped run       : {marker.read_text(encoding='utf-8', errors='replace').strip()}")

    head = current.split(":", 1)[0].strip()
    if not head.isdigit():
        if status == "STOPPED":
            print(f"\nStopped on request. Resume: ./wikidata-crawler.sh --start-step {var('resumestep')}")
        return 0
    code = int(head)
    started = var(f"step{code}startedat")
    elapsed = elapsed_since(started)
    print(f"step {code} started  : {started} ({elapsed} ago)")
    if code in PASSES:
        print(f"pass progress     : {var('entitiesprocessed')} entities, {var('rate')}, pass elapsed {var('elapsed')}")
    if code == 110:
        print(f"bulk load         : last statement committed {var('bulkloadlaststatement')}")
    if status != "RUNNING":
        print(f"\nNot running (status {status}). If it was interrupted in step {code}, resume with:")
        print(f"  ./wikidata-crawler.sh --start-step {code}")
        return 0
    print(f"\nCost of stopping NOW (docker stop wikidata-crawler) and resuming at the beginning of step {code}:")
    for line in stop_cost(code, elapsed):
        print("  " + line)
    print(f"Resume command    : ./wikidata-crawler.sh --start-step {code}   (same IMPORT_BATCH_ID)")
    print("Stop for free at the next step boundary: bash stop-at-step-boundary.sh")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
