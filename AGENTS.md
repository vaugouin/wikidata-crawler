# AGENTS.md - Agent Guide for Wikidata Crawler

This file gives you the agentic context you need to work on this codebase safely. For project overview, features, install / deploy steps and human-facing security / performance / troubleshooting material, read @README.md — that file is canonical and not duplicated here.

This is the single canonical guide for autonomous coding agents in this repository. Assistant-specific files such as @CLAUDE.md, and any future tool-specific guide such as `GEMINI.md`, should only point here and should not duplicate repository instructions.

Deeper specs live in their own files:
- @doc/sql/*.sql — reference DDL for the database schema; treat these files as read-only unless the user explicitly asks you to edit schema documentation
- @WIKIDATA.md — conceptual data model (statement / typed-value / qualifier layers, award modeling)
- @wikidata_dump_etl_README.md — lower-level ETL pass details and manual single-pass execution

- For any project update, keep documentation aligned:
  - Update `README.md` for user-facing behavior, configuration, setup, deployment, troubleshooting, or verification changes.
  - Update this file only when agent workflow or safety context changes.

---

## Related repositories (project ecosystem)

This repo is the dump-based Wikidata V2 ingestion stage of the larger **Agent BBB** movie/TV database system (GitHub user `vaugouin`; sibling repos live at `C:\Users\vaugo\Code\<repo>` and `github.com/vaugouin/<repo>`). It streams the official full Wikidata JSON dump (`latest-all.json.bz2`) into a statement-centric V2 schema for movies/series/persons/generic items, **superseding the SPARQL-driven V1 model** of sibling repos `sparql-crawler` and `sparql-movies-persons`. Its `T_WC_WIKIDATA_*` output tables converge on the shared MySQL/MariaDB database and feed `tmdb-movie-preprocess` and the PHP front-end `tmdb-front`.

The canonical sibling-repo roster lives in `%USERPROFILE%/Nestor/projets/t2s-backlog/topics/related-repositories.txt`.

Note: `sparql-crawler.py`, `sparql-movies-persons.py`, and `tmdb_functions.py` are large legacy/shared files carried in this repo from the V1 era; the V2 pipeline does not invoke them. Do not edit them as part of V2 work unless explicitly asked.

## Where things live (file → role)

Python:
- `wikidata_crawler.py` — the orchestrator. Defines the resumable `--start-step` workflow (steps `101`→`113`), runs each pass via `WikidataDumpETL`, loads staging, executes the bulk-load and media-resolution SQL, and writes progress/status to `T_WC_SERVER_VARIABLE`. This is the Docker `ENTRYPOINT`.
- `wikidata_dump_etl.py` — the ETL engine (`WikidataDumpETL`). Streams the `.bz2` dump chunk by chunk, does `P31/P279*` classification, and emits NDJSON staging files. Runs three passes: `pass1`, `pass2`, `item_cache`.
- `load_staging_jsonl.py` — maps each `T_WC_WIKIDATA_*.jsonl` to its `STG_T_WC_WIKIDATA_*` table via `TABLE_SPECS` and batch-inserts (batch size 100). Imported by the orchestrator (step 108) and also runnable standalone.
- `citizenphil.py` — shared DB helper module (Hungarian-named `f_*` functions, server-variable read/write, connection pooling). Imported as `cp`.

Bash orchestration:
- `wikidata-crawler.sh` — host entry point. Builds the image, runs the container detached (`docker run -d`), tails logs. Forwards all args straight to `wikidata_crawler.py` (so `--start-step N` works through it).
- `run_etl.sh` — legacy 3-pass driver that sets per-pass env vars and calls `wikidata_dump_etl.py` directly (the `wikidata_crawler.py` orchestrator now supersedes it for full runs).
- `backup-after-run.sh`: host-side database backup, triggered by a successful run. Reads `/shared/last_successful_run.json` (written by `WikidataCrawler._write_success_sentinel`), compares its `IMPORT_BATCH_ID` with the marker file `.last-backup-batch`, and calls `~/docker/tools/backupvaugouindb.sh` when they differ, so one run produces exactly one backup. `run-if-new-dump.sh` calls it on every hourly tick, **before** the dump check, so the finished state is saved before a new run starts modifying the database. `--wait` blocks on `docker wait` first, `--force` re-backs-up a batch already done, `--dry-run` reports without acting. On failure the marker is not written, so the next tick retries. Paths are overridable via `STACK_DIR`, `SHARED_DIR` and `BACKUP_SCRIPT`.
  **Verify the backup; do not trust the called script's exit code, and do not simplify that away.** Until 2026-08-31 `backupvaugouindb.sh` (source then in `tmdb-front`, in the `tools` repo since) ended on `if [ $? -eq 0 ] ... else echo "Backup failed!"`: it printed the failure and still exited 0, and its own `$?` was `gzip`'s rather than `mariadb-dump`'s, so a truncated dump reported success. Fixed since, in `tools`: the `backupvaugouindb*.sh` scripts share `backupvaugouindb-common.sh`, which adds `set -o pipefail`, real exit codes, a refusal to run on an empty table pattern, `MYSQL_PWD` instead of a command-line password, and size + `-- Dump completed on` checks; the full dump also excludes `STG_*` and uses `--single-transaction`. `backup-after-run.sh` still checks the exit code, then the output for `Backup failed!` / `Error:`, then the real size of the `.gz` (`docker exec <container> stat -c %s <file>`, floor `TAILLE_MINIMALE`, default 1 MB) parsed from the script's own confirmation line, because the copy deployed on the VPS can lag behind that repo.
- `check_network_speed.sh`, `on.sh`, `off.sh` — small operational helpers.

SQL (run in numeric order for a fresh environment):
- `01_create_schema.sql` — creates the final `T_WC_WIKIDATA_*` target tables.
- `02_staging_and_triggers.sql` — creates the `STG_T_WC_WIKIDATA_*` staging tables and validation triggers.
- `03_bulk_load_from_staging_FULL.sql` — merges staging rows into target tables (step 110). Idempotent.
- `04_reset_for_full_rerun.sql` — ordered `DELETE`s (FK checks off) to clear staging + targets before a fresh run. **Not part of the normal rerun** — see "Rerun strategy" below before suggesting it.
- `05_progress_checks.sql` — per-batch progress counts (edit `@IMPORT_BATCH_ID` before running).
- `06_repair_qualifier_tables.sql` — one-off repair script for qualifier tables.
- `07_resolve_media_resources.sql` — populates the media-resource tables (step 112). Idempotent.
- `08_cleanup_old_batches.sql` — deletes target rows whose `IMPORT_BATCH_ID` is strictly older than the current batch (step 114). Removes stale orphans the upsert-only bulk load leaves behind. Idempotent; FK checks off; entity/property tables (no batch column) untouched.
- `09_fix_value_type_conflicts.sql` — manual repair for trigger error 1644 ("statement/qualifier already exists in another child table"). Deletes, for the current `@IMPORT_BATCH_ID`, target typed-value rows whose statement/qualifier is classified as a different `VALUE_TYPE` in this batch's staging — the stale sibling left when a statement's type flips between dumps. The same purge is built into `03_bulk_load_from_staging_FULL.sql` (a fresh run self-heals); this standalone file unblocks an in-flight load without rebuilding the image. Idempotent. Set `@IMPORT_BATCH_ID` before running.
- `10_clear_staging_batch.sql`: surgical staging cleanup, deletes every `STG_*` row for one specific `@OLD_BATCH_ID`, leaving every other batch intact. Use it to drop one named batch; for the routine "keep only the latest" cleanup the pipeline now runs step 115 itself. Lighter than `04_reset_for_full_rerun.sql`, which clears all staging + targets. Set `@OLD_BATCH_ID` to the batch to remove.
- `13_cleanup_staging_old_batches.sql`: hand-runnable twin of step 115, deletes every `STG_*` row whose `IMPORT_BATCH_ID` is strictly older than `@IMPORT_BATCH_ID`, so staging keeps exactly the batch that just loaded. The crawler does **not** execute this file (step 115 issues the same deletes in committed 50 000-row chunks, with its table list derived from `TABLE_SPECS`); it exists to clean a database by hand and to catch up runs that predate step 115. **Nothing to set by hand since 2026-09-02**: the cutoff is read from the data, `MAX(IMPORT_BATCH_ID)` over staging, and guard 2 is enforced in SQL by nulling that cutoff when the batch is not yet in `T_WC_WIKIDATA_STATEMENT`, which turns all 25 deletes into no-ops. It used to carry a literal batch id, and that literal went one run stale: a cutoff older than everything in staging matches nothing, so the file reported success and deleted nothing. Prefer a silent-success failure mode you can see, hence sections 1, 3 and 5 print the batches, the ARMED/DISARMED verdict and the result.
- `apply_to_live_db.sql` — idempotent additive DDL applied automatically by the orchestrator at steps 108 and 110. Creates the SEASON/EPISODE/CHARACTER target tables and the **full** `STG_T_WC_WIKIDATA_*` staging set with `CREATE TABLE IF NOT EXISTS`, then widens `YEAR_VALUE` to BIGINT. This is what keeps a long-lived DB in sync without a fresh-database rebuild, and what makes a hand-dropped staging table a non-event instead of a crash after days of ETL. Its staging definitions duplicate `02_staging_and_triggers.sql` (which stays canonical, and is the only file that creates the triggers) — change both together. It must stay `DELIMITER`-free: the orchestrator executes it through pymysql by splitting on `;`.

Docs: `README.md` (operational runbook, canonical), `WIKIDATA.md` (conceptual model), `wikidata_dump_etl_README.md` (ETL internals).

## Code conventions

- **Hungarian-ish notation** in the legacy/shared layer (`citizenphil.py`, the `sparql-*`/`tmdb_functions.py` files): `str*` strings, `lng*` integers, `arr*` collections, `int*` flags.
- **`f_*` helper functions** in `citizenphil.py`: `f_getconnection`, `f_setservervariable` / `f_getservervariable`, `f_sqlupdatearray`, `f_stringtosql`, `f_fieldfromquery`, etc. Reuse these rather than opening ad-hoc connections where practical.
- The newer V2 pipeline modules (`wikidata_crawler.py`, `wikidata_dump_etl.py`, `load_staging_jsonl.py`) use modern typed Python (`from __future__ import annotations`, dataclasses, `pathlib`, type hints) — match that style when editing them.
- **DB helpers**: `citizenphil.f_getconnection()` returns a lazily-created, reused `pymysql` `DictCursor` connection. The orchestrator opens its own multi-statement connection (`CLIENT.MULTI_STATEMENTS`, `utf8mb4` / `utf8mb4_unicode_ci`) only for executing the bulk-load and media-resolution SQL files.
- **Server variables**: all live operational state (progress, status, per-step timestamps, row counts, last error) is written to the `T_WC_SERVER_VARIABLE` table via `cp.f_setservervariable(...)`, with names prefixed `strwikidatacrawler` (e.g. `strwikidatacrawlerstatus`, `strwikidatacrawlerbulkloadlaststatement`, `strwikidatacrawlermediaresourcecommons`). The table name may carry a prefix from `DB_NAMESPACE`.
- **Env naming**: this repo uses the same `DB_*` connection variables as the sibling repos (`sparql-crawler`, `tmdb-crawler`) and as `citizenphil.py` itself — `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_NAMESPACE`. There is no `MARIADB_*` bridge; the orchestrator reads `DB_*` directly.

## Pipeline / passes

The orchestrator (`wikidata_crawler.py`) runs an ordered, resumable workflow. `--start-step N` skips every step with a code below `N` (validated against the known step codes). `wikidata-crawler.sh` forwards args through to the container.

| Step | Label | What it does |
|------|-------|--------------|
| 101 | resolve dump source | Resolve `DUMP_FILE` / `DUMP_URL`; download to `DUMP_FILE` only if it does not already exist. |
| 102 | run ETL pass1 | Classification pass: stream the full dump, emit `T_WC_WIKIDATA_PROPERTY_METADATA`, build the `P279` subclass graph, classify movies/series/persons, produce `class_roots.jsonl`, `core_entity_ids.txt`, `candidate_person_ids.txt`. |
| 103 | validate ETL pass1 | Assert pass1 outputs exist, no parse errors, core entities detected. |
| 104 | run ETL pass2 | Entity/statement pass: emit entity rows + statements + typed values for in-scope movies/series/persons; produce `referenced_item_ids.txt`, `referenced_person_ids.txt`. Requires pass1 outputs. |
| 105 | validate ETL pass2 | Assert `T_WC_WIKIDATA_STATEMENT.jsonl` exists, no parse errors, statements emitted. |
| 106 | run ETL item_cache | Item-cache pass: emit `T_WC_WIKIDATA_ITEM` rows for referenced items and extra `T_WC_WIKIDATA_PERSON` rows for referenced (no-IMDb) persons, plus their `CACHED_ENTITY_PROPERTIES` claims (P31, P279, P345, P569, P570, P577). Requires pass1 + pass2 outputs **and the dump**. |
| 107 | validate ETL item_cache | Assert `run_summary.json` and item/person outputs exist. |
| 108 | load staging tables | Delete prior rows for this `IMPORT_BATCH_ID`, then load every NDJSON file into its `STG_*` table via `load_staging_jsonl.load_table`. |
| 109 | validate staging data | Assert staged statement rows exist for the batch and none have NULL `IMPORT_BATCH_ID`. |
| 110 | bulk load target tables | Execute `03_bulk_load_from_staging_FULL.sql` statement by statement; idempotent (filters `ROW_STATUS IN ('NEW','VALID')`, flips to `'LOADED'`). |
| 111 | validate target tables | Assert `T_WC_WIKIDATA_STATEMENT` is non-empty and staging rows were marked `LOADED`. |
| 112 | resolve media resources | Execute `07_resolve_media_resources.sql`; populate the media-resource tables from V2 statement/value tables. Idempotent (`INSERT ... ON DUPLICATE KEY UPDATE`); fully downstream of V2, safe to run alone via `--start-step 112`. |
| 113 | validate media resources | Assert media-resource tables non-empty; record per-platform counts. |
| 114 | cleanup old import batches | Execute `08_cleanup_old_batches.sql`; delete every target row whose `IMPORT_BATCH_ID` is strictly older than the current batch (stale "orphans" the upsert-only bulk load never overwrites). Guarded: refuses to run if the current batch has no statements. Idempotent; safe to run alone via `--start-step 114`. |
| 115 | cleanup old staging batches | Delete every `STG_*` row whose `IMPORT_BATCH_ID` is strictly older than the current batch, in committed 50 000-row chunks, so staging is left holding exactly the batch that just loaded. Table list comes from `TABLE_SPECS`, the same source step 108 uses. Doubly guarded: the current batch must be **in staging** and **already in `T_WC_WIKIDATA_STATEMENT`**, since older staging is the fallback until the bulk load has succeeded. Idempotent; safe to run alone via `--start-step 115`. |

**Resuming at a dump-streaming step (102, 104, 106) is safe since 2026-08-17.** This note used to read "do not start from 104 unless the code is changed to initialize the dump source when 101 is skipped". That change is now made: `run()` executes step 101 first whenever the resume point would skip it and a step in `DUMP_CONSUMING_STEPS` is still due. Before the fix, `--start-step 106` died deep in the ETL on a bare `assert self.dump_file is not None`, naming neither the cause nor the remedy. Resolution stays on demand, so resuming at 108 or 110 still never touches the dump.

Two things to check before resuming at 102, 104 or 106, because all three stream the **full 102 GB dump**, they do not work from the pass1/pass2 NDJSON:

- Is `latest-all.json.bz2` still in `/shared`? It is routinely deleted after a run to reclaim disk space. If it is gone, step 101 will re-download it, which costs about 7 h 30.
- If it is gone, what comes back is a **different, more recent dump** than the one pass1 and pass2 consumed. Mixing an item_cache built from this week's dump with pools and filters derived from last week's produces a batch whose provenance can no longer be stated. In that case prefer a full run on the new dump over a partial resume. Compare the file size against `strwikidatacrawlerdumpsize` to confirm you have the same dump.

The ETL passes each stream the full multi-GB dump and can take **multiple days** (the full ETL can exceed a week). Treat them as expensive: prefer resuming from a later step over re-running passes. The bulk load (110) and media resolution (112) are both idempotent and cheap by comparison.

## Rerun strategy (do not reset the target tables)

A full rerun **loads on top of the existing target tables**. Never advise clearing them first, and never run `04_reset_for_full_rerun.sql` as part of a routine rerun: it would leave the front-end with no Wikidata V2 data for the entire multi-day ETL, for no benefit.

The incremental path is safe because three things hold together:

1. `derive_statement_identity` / `derive_qualifier_identity` in `wikidata_dump_etl.py` hash the Wikidata `STATEMENT_GUID` (resp. the statement GUID **plus** the qualifier property and snak hash) into a stable BIGINT — the same claim yields the same `ID_STATEMENT` in every run.

   > **Never make a qualifier's identity its snak hash alone.** A dump snak's `hash` covers the snak's *content* (property + value), so "P1545 = 1" carries the same hash on every statement that uses it. Identity built on it collides across statements, and the `UNIQUE KEY` on `QUALIFIER_HASH` then keeps **one row per distinct value instead of one per occurrence** — silently, since the ETL still emits every row and the load reports no error. That bug shipped and was only caught in the database months later (2026-07-30): P453, P1686 and P155 each had exactly as many rows as distinct values, 97,6 % of `P166` award statements had lost their `P585` date, and episode ordinals were gone. The identity is therefore keyed on the **parent `ID_STATEMENT`**, which is what turns a value into an occurrence, and which — unlike the statement GUID — is present in the emitted NDJSON, so an existing pass2 output can be renumbered offline. `tests/test_etl_smoke.py` guards both the property and its offline twin.

   > **Repairing a pass2 output instead of rescanning the dump.** `migrations/repair_qualifier_identity.py` renumbers `/shared/pass2`'s qualifier files in place (originals kept as `*.jsonl.before-019`), fanning each value row out over its occurrences. It works because the collapse happened at *load* time, not at emission: every occurrence is already its own NDJSON line. This turns a 2-day rerun from step 104 into a ~13 h reload from step 108. The script carries its own copy of `stable_bigint_from_text` and of the identity formula so it stays runnable next to an old checkout; the smoke test pins the two copies together, because any drift would produce ids the next ETL run no longer matches and the reload would duplicate instead of update.
2. Step 110 is upsert-only (`ON DUPLICATE KEY UPDATE` throughout `03_bulk_load_from_staging_FULL.sql`), so re-loaded rows are updated in place rather than duplicated.
3. Step 114 (`08_cleanup_old_batches.sql`) deletes rows whose `IMPORT_BATCH_ID` is strictly older than the current batch — the same deletion the reset would have done, but after the new data has landed.

Result: the V2 tables are continuously readable, and the changeover happens row by row during step 110.

Consequences worth knowing when reasoning about the data:

- Entity tables (`MOVIE` / `SERIE` / `PERSON` / `ITEM` / `SEASON` / `EPISODE` / `CHARACTER`) and `PROPERTY_METADATA` carry no `IMPORT_BATCH_ID` and are never pruned. An entity that falls out of scope keeps its row with zero statements. Only a full reset removes it.
- Loading over an existing batch is exactly the condition that produces trigger error 1644 (`VALUE_TYPE` flip). It is handled by the purge in section 3B of `03_bulk_load_from_staging_FULL.sql`; `09_fix_value_type_conflicts.sql` is the standalone equivalent for an in-flight load.
- Step 108 clears only the current `IMPORT_BATCH_ID` from staging, which is what makes a `--start-step 108` resume safe. Older batches are removed at the very end instead, by step 115, once the new data is in the targets. So during a run staging legitimately holds two batches (well over 100 M rows), and holds one again when the run ends. A run that fails before step 115, or a resume started after it, leaves the predecessor behind: `13_cleanup_staging_old_batches.sql` or a standalone `--start-step 115` clears it. Harmless to the front-end either way, but it consumes real space.

Reset is the right call in exactly four cases: the ID-derivation logic changed; the target schema changed such that existing rows are meaningless; the data is known corrupt; or the entity tables need pruning of out-of-scope rows. Flag the downtime explicitly when recommending it.

## Before launching: is there actually a new dump?

**Check with `python check_new_dump.py` before deleting the local dump file.** Wikidata's weekly JSON dump *starts* on its cycle date but takes about **four days** to finish, and `latest-all.json.bz2` is only refreshed when that generation completes: the `20260803` cycle only became available on **7 Aug 03:57 UTC**. Launching in between re-downloads the *previous* dump under a fresh `IMPORT_BATCH_ID` and re-ingests data already in the database.

That happened on 2026-08-03: **3 days 18 hours** of VPS for a byte-identical result (120 986 268 entities, 35 122 018 statements, the same figures as the 26 July run). Nothing in the logs flagged it, because the run *was* a success. The trap is sharpened by the launch procedure itself, which deletes the local file first, destroying the only thing you could have compared against.

**Put `run-if-new-dump.sh` in cron, hourly** — `17 * * * *`, off the top of the hour where everyone polls Wikimedia at once. A check costs one HEAD request and does nothing 167 times out of 168. Two guards make it safe unattended: a `flock` (one execution at a time) and an early exit when the `wikidata-crawler` container is running. That second one matters: during the three to four days of a run, the comparison anchor is unstable (step 101 has downloaded the new dump but not yet recorded its size), so checking would be useless at best and destructive at worst, since relaunching wipes the shared volume under the running job. Related: `wikidata-crawler.sh` now follows container logs **only when stdout is a terminal**, otherwise a cron job would stay alive for days writing gigabytes.

**A successful run leaves a sentinel for the host.** The container runs with `--rm`, so once it exits nothing on the host says whether the run worked; `/shared` is the only common ground. `run()` therefore writes `/shared/last_successful_run.json` (batch id, start step, steps executed, runtime, end time) on success, and never fails the run if that write fails, since by then the data work is done and committed. `backup-after-run.sh` is its only consumer. Note `run-if-new-dump.sh` wipes `/shared` at the next launch, which is why the backup is triggered from the same hourly tick, before the wipe.

**`run-if-new-dump.sh` wraps the whole decision**: it checks, and only if the dump is new does it write a fresh `IMPORT_BATCH_ID` into `.env` (backing the file up first), wipe `/home/debian/docker/shared_data/wikidata-crawler`, and launch. Put *that* in cron, not the crawler itself: six days out of seven it does nothing. `--dry-run` prints what it would do. Every step that can run in a container does, the glue stays in this versioned script rather than in a command line to retype, because it wipes 102 GB.

**The wipe itself must run in a container, and that is a correctness requirement, not a habit.** `/shared/pass1`, `/shared/pass2` and `/shared/item_cache` are created by the crawler, which runs as root, so they belong to root. Deleting a file depends on write permission on its *parent directory*, not on the file, so the `debian` user can remove the dump (sitting at the root of a directory it owns) but **not** the contents of those three. A host-side `rm -rf` leaves the three passes' output in place and pass1 then re-reads a previous run's `core_entity_ids.txt`. The script wipes via `docker run --entrypoint find … /shared -mindepth 1 -delete`, checks the exit code, counts what remains, and refuses to launch if anything survived: a run started on stale pass output produces a wrong result without ever saying so.

`check_new_dump.py` sends a HEAD request (no download) and compares the advertised size to the size recorded by the last run in `strwikidatacrawlerdumpsize` — two consecutive dumps differ by hundreds of MB. Exit code 0 = new dump, 1 = same as last run, 2 = cannot tell. It needs `WIKIMEDIA_USER_AGENT` set in `.env`: Wikimedia answers 403 to default library agents, so an unset variable makes the check silently inconclusive rather than wrong.

The hand-run form is `./wikidata-crawler.sh --check-dump`, which is NOT forwarded to `wikidata_crawler.py`: the launcher intercepts it and runs `check_new_dump.py` in a throwaway container instead, so asking the question can never start a run. Add `--vs-local` to compare against the file on the shared volume rather than against the last processed run, and `--quiet` for the exit code alone. The two anchors answer different questions: the default one answers "should I relaunch the crawler", `--vs-local` answers "is my downloaded file stale". They agree after a successful run and diverge whenever a download was not followed by a full ingestion.

Note what the launcher does NOT do, because the intuition runs the other way: a plain `./wikidata-crawler.sh` does **not** re-download when the dump is already on the shared volume. Step 101 reuses an existing `DUMP_FILE` (`step_resolve_dump_source`), and only downloads when the file is absent. The cost of launching blind is therefore not bandwidth, it is three to four days of re-ingesting the same data. `run-if-new-dump.sh` is what deletes the volume, and it checks first.

NDJSON output dirs (inside the container): `/shared/pass1`, `/shared/pass2`, `/shared/item_cache` (host: `/home/debian/docker/shared_data/wikidata-crawler/<pass>`).

**The P279 subclass graph is loaded since 2026-08-16.** `collect_subclass_edges` had always written `/shared/pass1/subclass_edges.jsonl` (5 228 221 edges on a full dump) and nothing loaded it, so the class graph lived on disk and was invisible to SQL, which is why no hierarchical question was answerable in V2. It now lands in `T_WC_WIKIDATA_SUBCLASS` through the ordinary path: `STG_T_WC_WIKIDATA_SUBCLASS`, a `TableSpec` in `load_staging_jsonl.py`, and a section at the end of `03_bulk_load_from_staging_FULL.sql`. This matters beyond convenience: the entity pools are derived from that graph at every run (`descendants_of_roots(MOVIE_ROOTS)` and friends), so what counts as a "film" is defined by the P279 edges Wikidata publishes that day, not by this codebase. Loading them makes the definition auditable and its run-over-run drift measurable. To backfill a run that predates this change without replaying any ETL step, use `12_bulk_load_subclass_only.sql`; it works only while that run's `pass1` directory is still on disk, since `run-if-new-dump.sh` wipes it at the next launch. First measurement, 2026-08-17: the transitive closure under `Q11424` (film) and `Q506240` (television film) holds **842 classes** on batch `wikidata_full_20260807_1043`, against 166 direct subclasses of `Q11424` alone. Record that figure at every run: a drop is what an upstream reclassification looks like, and until now nothing made it visible. Note this covers **P279 only**: WIKIDATA-CRAWLER-020 track (a), making `item_cache` emit `P31` and `P279` for cached items, remains the fix for award-category questions and still needs an item_cache replay.

One trap worth carrying to any recursive CTE written against this schema: MariaDB derives a CTE column's type from the **non-recursive part alone**, and on a three-branch anchor (`root UNION root UNION recursion`) it keeps only the first branch, silently. The first failure is loud (`ERROR 1406`, fixed with `CAST(... AS CHAR(50))`); the second returns a wrong number with no error at all. Group the roots in a subquery so the CTE has exactly one anchor branch and one recursive branch, and give every such query a floor value to check against.

**`item_cache` emits `CACHED_ENTITY_PROPERTIES` since 2026-08-17**, namely `P31`, `P279`, `P345`, `P569`, `P570`, `P577`. A cached entity used to carry its label and not one fact, so `Q103618` "Academy Award for Best Actress" existed by name with zero statements and every hierarchical question was dead: no award count, no grouping by class, no animation-film cone. That is WIKIDATA-CRAWLER-020 track (a), and `P31`/`P279` answer it. The other three serve **-015**, the decommission of the SPARQL crawlers: `PERSON_V1` and `MOVIE_V1` keep `ID_IMDB`, `BIRTHDAY`, `DEATHDAY` and `DAT_RELEASE` as columns, and for a cached entity those facts were reachable nowhere in V2. Measured 2026-07-30, 6 794 of the 18 792 persons shared with V1 had neither `P31`, nor `P569`, nor `P570`. The set is closed on purpose: each added property costs a 23 h replay of step 106. `emit_class_claims_for_cached_item` is a dedicated method rather than the full emitter behind a property filter, on purpose. The full emitter walks every property and feeds `referenced_item_ids` / `referenced_person_ids`, which item_cache being the last pass are already consumed, so touching them would be a pure side effect; it also emits qualifiers, which these two properties rarely carry. Emission is gated on the entity row having actually been written, so no statement ever points at an entity missing from the entity tables.

Two consequences to keep in mind. The pass now produces `T_WC_WIKIDATA_STATEMENT.jsonl` and `T_WC_WIKIDATA_ITEM_VALUE.jsonl` under `/shared/item_cache`, and `load_staging_jsonl.py` gained the matching `TableSpec` entries: without them the files would be written and never loaded, which is precisely how the subclass graph stayed invisible for months. No SQL change was needed, since those rows join the existing staging tables and the existing bulk-load section. Cost: about two claims per cached item, ~1.2 M statements against the 35 M already loaded, and it needs a replay of **step 106 only** (`--start-step 106`), measured at 22 h 45 on the 2026-08 run, not the 18 h the ticket estimated. Acceptance: `SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT WHERE ID_WIKIDATA='Q103618'` returns non-zero, and Q2/Q3 of `doc/sql/wikidata-v2-awards-queries.sql` return rows untouched.

## Database tables

Staging tables (`STG_T_WC_WIKIDATA_*`, created by `02_staging_and_triggers.sql`, loaded in step 108):
`STG_T_WC_WIKIDATA_PROPERTY_METADATA`, `_MOVIE`, `_SERIE`, `_PERSON`, `_ITEM`, `_STATEMENT`, `_ITEM_VALUE`, `_STRING_VALUE`, `_EXTERNAL_ID_VALUE`, `_MEDIA_VALUE`, `_TIME_VALUE`, `_QUANTITY_VALUE`, `_STATEMENT_QUALIFIER`, `_QUALIFIER_ITEM_VALUE`, `_QUALIFIER_STRING_VALUE`, `_QUALIFIER_EXTERNAL_ID_VALUE`, `_QUALIFIER_MEDIA_VALUE`, `_QUALIFIER_TIME_VALUE`, `_QUALIFIER_QUANTITY_VALUE`. Staging metadata columns include `IMPORT_BATCH_ID`, `SOURCE_FILE`, `ROW_STATUS`.

Target tables (`T_WC_WIKIDATA_*`, created by `01_create_schema.sql`, loaded in step 110):

- Entity tables: `T_WC_WIKIDATA_MOVIE`, `T_WC_WIKIDATA_SERIE`, `T_WC_WIKIDATA_PERSON`, `T_WC_WIKIDATA_ITEM` (referenced-item cache only — not a mirror of all Wikidata items).
- Property metadata: `T_WC_WIKIDATA_PROPERTY_METADATA`.
- Statement parent: `T_WC_WIKIDATA_STATEMENT` (`ID_STATEMENT`, `ID_WIKIDATA`, `ID_PROPERTY`, `STATEMENT_GUID`, `VALUE_TYPE`, `WIKIDATA_DATATYPE`, `RANK`).
- Main typed-value tables (exactly one per statement): `T_WC_WIKIDATA_ITEM_VALUE`, `T_WC_WIKIDATA_STRING_VALUE`, `T_WC_WIKIDATA_EXTERNAL_ID_VALUE`, `T_WC_WIKIDATA_MEDIA_VALUE`, `T_WC_WIKIDATA_TIME_VALUE`, `T_WC_WIKIDATA_QUANTITY_VALUE`.
- Qualifier parent: `T_WC_WIKIDATA_STATEMENT_QUALIFIER`.
- Qualifier typed-value tables: `T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE`, `_QUALIFIER_STRING_VALUE`, `_QUALIFIER_EXTERNAL_ID_VALUE`, `_QUALIFIER_MEDIA_VALUE`, `_QUALIFIER_TIME_VALUE`, `_QUALIFIER_QUANTITY_VALUE`.
- Media-resource tables (populated by step 112): `T_WC_WIKIDATA_MEDIA_RESOURCE`, `T_WC_WIKIDATA_MEDIA_RESOURCE_URL`, `T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK` (the CHECK table is reserved for an out-of-band HTTP-check job and is left empty by the pipeline).

The legacy V1 `T_WC_WIKIDATA_ITEM_PROPERTY` (item-valued only) is no longer authoritative; the front-end reads V2 and only falls back to V1 for entity types not yet populated in V2. See `README.md` "Front-end consumption" and `WIKIDATA.md` for the conceptual model and award/qualifier patterns.

## SQL Object Naming Conventions

Consistent with the sibling repos:

- Uppercase snake case for all tables and columns.
- Table prefixes: `T_WC_*` (project tables), `T_WC_WIKIDATA_*` (this repo's V2 targets), `STG_T_WC_WIKIDATA_*` (staging).
- Primary keys: `ID_{ENTITY}` (e.g. `ID_STATEMENT`, `ID_WIKIDATA`, `ID_PROPERTY`).
- Date columns prefixed `DAT_*`; datetime/timestamp columns prefixed `TIM_*`.
- Boolean/flag columns prefixed `IS_*` (and `DELETED` as a soft-delete flag).
- Standard audit columns on project tables: `DELETED`, `DISPLAY_ORDER`, `ID_CREATOR`, `DAT_CREAT`, `ID_OWNER`, `TIM_UPDATED`, `ID_USER_UPDATED` (managed by `citizenphil.f_sqlupdatearray` when `intaddstdfields = 1`).
- Server-side runtime state lives in `T_WC_SERVER_VARIABLE` (`VAR_NAME` / `VAR_VALUE`), keyed by the `strwikidatacrawler*` names this pipeline writes.

## Configuration & secrets

Configuration is environment-driven; copy `.env.example` to `.env`. Do not commit `.env` (it holds credentials). Key variables:

- Database: `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, optional `DB_NAMESPACE` (table-name prefix, e.g. `T_WC_`). Same names used by `citizenphil.py` and the sibling repos — no bridging.
- Batch identity: `IMPORT_BATCH_ID` (required). Use a fresh id per full run; recommended format `wikidata_full_YYYYMMDD_HHMM`.
- Dump source: `DUMP_URL` (remote `.bz2`) and/or `DUMP_FILE` (local path on the shared volume). See `.env.example` and `wikidata_dump_etl_README.md` for the three valid combinations. A cached `DUMP_FILE` is reused if present — delete it to force a fresh download.
- HTTP identity: `WIKIMEDIA_USER_AGENT` — Wikimedia policy requires a descriptive User-Agent (`ToolName/version (URL; contact-email)`). Set it before hitting Wikimedia servers. Both the step-101 dump download (`WikidataCrawler._download_dump`) and the ETL's remote streaming (`wikidata_dump_etl.py`) send it; a default library User-Agent gets a `403 Forbidden` from `dumps.wikimedia.org`.
- Other: `USER_TIMEZONE` (default `Europe/Paris`).

Docker: built from `Dockerfile` (`ENTRYPOINT ["python", "wikidata_crawler.py"]`). Run via `wikidata-crawler.sh` (detached, `--network=host`, `--env-file .env`, host `/home/debian/docker/shared_data/wikidata-crawler` mounted as `/shared`). Args to the script are forwarded to the entrypoint. Rebuild the image after changing Python or SQL files.

## Encoding

All files are UTF-8. The database and connections use `utf8mb4` / `utf8mb4_unicode_ci` to preserve multilingual Wikidata labels and aliases (the bulk-load and media-resolution connections force this collation explicitly). NDJSON is written with `ensure_ascii=False`. Keep this file and any edits UTF-8.

**Every hand-written `.sql` for this database must start with `SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;`.** All tables are `utf8mb4_unicode_ci` (verified 2026-07-31: 27 V2 tables, 7 `*_V1`, 75 `T2S_*`, 76 `TMDB_*`), so table-to-table comparisons are safe and the old "V1 `general_ci` vs V2 `unicode_ci`" warning is obsolete. What still breaks is the **connection** collation: the `mariadb` client connects as `utf8mb4_general_ci`, and any value *produced by a function* (`CAST(x AS CHAR)`, `CONVERT`, `CONCAT` over a number) inherits it with implicit coercibility, so comparing it to a column raises `ERROR 1267 Illegal mix of collations`. String literals are coercible and never trigger it. `--default-character-set=utf8mb4` does **not** fix this: it sets the character set, not the collation. Also pass `--force`, since the client otherwise aborts the whole file on the first error. When an explicit `COLLATE` is still needed, put it on the non-indexed side of the comparison, never on the indexed column, or the index is lost.

---

## Never conclude from `information_schema`

Use it to explore, never to decide. Three of its columns misled a run review on 2026-08-16, and each failure mode costs an afternoon if you meet it fresh.

- **`TABLE_ROWS` is a statistical estimate, not a count.** On batch `wikidata_full_20260807_1043` it reported 340 401 rows for `T_WC_WIKIDATA_MOVIE` against **438 956 actual**, a 22 % under-estimate. Compared against a stored reference figure, that reads as "one film in five has vanished", and the whole anomaly was the estimator. Entity tables are small: `COUNT(*)` them, it costs seconds. Keep the estimate only for `T_WC_WIKIDATA_STATEMENT` and `_STATEMENT_QUALIFIER`, where a real count costs minutes, and never compare an estimate to a reference.
- **A stale estimate looks exactly like a table the run never touched.** `T_WC_WIKIDATA_EPISODE` reported its previous value to the unit, 188 721, which read as "not rewritten". It actually held 187 463 rows; the statistics had simply not been refreshed. `ANALYZE TABLE` refreshes the estimate when one is genuinely needed.
- **`UPDATE_TIME` is UTC while the application's `TIM_UPDATED` columns are local time.** `MOVIE_V1` and `PERSON_V1` each showed exactly two hours of difference (Europe/Paris is UTC+2 in summer). Read last-write times from `MAX(TIM_UPDATED)`, not from the catalogue, or you will date a write to the wrong evening.

Corollary for the review scripts: `doc/sql/wikidata-run-report.sql` §A3 was split on 2026-08-16 into **A3a**, which counts entity tables for real and carries exact reference figures, and **A3b**, explicitly labelled indicative and reference-free so nothing invites a comparison.

Related but distinct: the V1 tables keep being written after both SPARQL crawlers are stopped. That is `wikipedia-crawler`, which fills the image columns only (`WIKIPEDIA_POSTER_PATH` on `MOVIE_V1`/`SERIE_V1`, `WIKIPEDIA_PROFILE_PATH` on `PERSON_V1`/`CHARACTER_V1`, `WIKIPEDIA_IMAGE_PATH` on `ITEM_V1`). A recent write time on a V1 table is not evidence that a SPARQL crawler is still running.

---

**Last Updated**: 2026-08-16
**Current Version**: 1.0.0

## Backlog (Nestor second-brain)

The prioritized, agent-ready implementation backlog for this repo lives in the **Nestor**
knowledge repo (a separate repo, not cloned alongside this one):

- This repo: `C:\Users\vaugo\Nestor\projets\t2s-backlog\repos\wikidata-crawler.md`
- Cross-repo dashboard: `C:\Users\vaugo\Nestor\projets\t2s-backlog\index.md`

Consult it before implementing: tasks are `WIKIDATA-CRAWLER-NNN` with status (done / in-progress /
todo), priority, and quick-wins. NOTE: these are local paths on Philippe's PC and do not
resolve on the VPS or on cloud agents (claude.ai/code).
