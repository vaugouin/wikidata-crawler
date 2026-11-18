#!/usr/bin/env python3
"""Smoke test for the stop request checked between steps (WIKIDATA-CRAWLER-036).

No database, no dump: the server variables are kept in a dict, the shared volume is a
temporary directory and every step handler only records that it ran. Pins:

- stop_request_applies, the pure rule (empty file, step code, unreadable content);
- run() stops between two steps, never inside one, and never before the first step of
  a launch; it removes the request, leaves stopped_run.json, writes no success sentinel
  and records STOPPED with the step to resume from;
- a leftover request and a leftover marker are removed at launch, so a resume runs;
- without a request the run ends in SUCCESS as before.

Run:  python tests/test_stop_request.py     (exit 0 = pass, 1 = fail)
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
os.environ.pop("DB_HOST", None)
os.environ["IMPORT_BATCH_ID"] = "wikidata_full_test"

import wikidata_crawler as wc  # noqa: E402

failures = 0


def check(condition: bool, message: str) -> None:
    global failures
    print(("ok   " if condition else "FAIL ") + message)
    if not condition:
        failures += 1


# 1. The pure rule.
check(wc.stop_request_applies(None, 104) is False, "no file, no stop")
check(wc.stop_request_applies("", 104) is True, "empty file stops at the first boundary")
check(wc.stop_request_applies("\n", 104) is True, "a file holding a newline counts as empty")
check(wc.stop_request_applies("107", 104) is False, "stop after 107: not yet after 104")
check(wc.stop_request_applies("107\n", 107) is True, "stop after 107: holds once 107 has finished")
check(wc.stop_request_applies("107", 108) is True, "stop after 107: still holds past it")
check(wc.stop_request_applies("please", 104) is True, "unreadable content is obeyed, not ignored")


def launch(shared: Path, start_step: int, during: dict[int, str] | None = None):
    """Run the orchestrator on fake steps. `during` maps a step code to the request text
    that appears on the shared volume while that step runs. Returns (steps run, variables)."""
    variables: dict[str, str] = {}
    ran: list[int] = []
    wc.SHARED_DIR = shared
    wc.cp.f_setservervariable = lambda name, value, *rest: variables.__setitem__(name, str(value))
    wc.cp.f_getservervariable = lambda name, *rest: variables.get(name, "")
    crawler = wc.WikidataCrawler(start_step=start_step)

    def handler_for(code: int):
        def handler(_self) -> None:
            ran.append(code)
            if during and code in during:
                (shared / wc.STOP_REQUEST_NAME).write_text(during[code], encoding="utf-8")
        return handler

    for code in list(crawler.steps):
        crawler.steps[code] = wc.ProcessStep(code, crawler.arrwikidatascope[code], handler_for(code))
    crawler.run()
    return ran, variables


with tempfile.TemporaryDirectory() as tmp:
    shared = Path(tmp)
    request = shared / wc.STOP_REQUEST_NAME
    marker = shared / wc.STOPPED_MARKER_NAME
    sentinel = shared / wc.SUCCESS_SENTINEL_NAME

    # 2. A request written while pass2 (104) runs: 104 finishes, 105 never starts.
    ran, variables = launch(shared, 101, {104: ""})
    check(ran == [101, 102, 103, 104], f"empty request during 104: stops after 104 (ran {ran})")
    check(variables.get("strwikidatacrawlerstatus") == "STOPPED", "status is STOPPED")
    check(variables.get("strwikidatacrawlerstep104status") == "SUCCESS", "the finished step is recorded as SUCCESS")
    check(variables.get("strwikidatacrawlerresumestep") == "105", "resume step recorded")
    check(not request.exists(), "the request is removed once obeyed")
    check(marker.exists() and json.loads(marker.read_text(encoding="utf-8"))["resume_step"] == 105, "stopped_run.json names the resume step")
    check(not sentinel.exists(), "no success sentinel: no backup, no purge")

    # 3. The resume: the leftover marker goes, and a request left behind does not stop it.
    request.write_text("", encoding="utf-8")
    ran, variables = launch(shared, 105)
    check(ran[0] == 101 and ran[1:] == list(range(105, 116)), f"resume at 105 runs 101 then 105 to 115 (ran {ran})")
    check(variables.get("strwikidatacrawlerstatus") == "SUCCESS", "the resumed run ends in SUCCESS")
    check(not marker.exists() and not request.exists(), "leftover marker and request removed at launch")
    check(sentinel.exists(), "success sentinel written")

    # 4. A step code in the request: the run goes on until that step has finished.
    sentinel.unlink()
    ran, variables = launch(shared, 101, {102: "107"})
    check(ran == [101, 102, 103, 104, 105, 106, 107], f"request 107 during 102: stops after 107 (ran {ran})")
    check(variables.get("strwikidatacrawlerresumestep") == "108", "resume step is 108")
    check(not sentinel.exists(), "still no success sentinel")

    # 5. A request during the last step changes nothing: the run is over.
    marker.unlink()
    ran, variables = launch(shared, 114, {115: ""})
    check(ran == [114, 115] and variables.get("strwikidatacrawlerstatus") == "SUCCESS", f"request during 115: run ends in SUCCESS (ran {ran})")

print("\n" + ("PASS" if failures == 0 else f"{failures} FAILED"))
sys.exit(1 if failures else 0)
