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

The canonical sibling-repo roster lives in `tmdb-front/doc/related-repositories/related-repositories.txt`.

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
- `check_network_speed.sh`, `on.sh`, `off.sh` — small operational helpers.

SQL (run in numeric order for a fresh environment):
- `01_create_schema.sql` — creates the final `T_WC_WIKIDATA_*` target tables.
- `02_staging_and_triggers.sql` — creates the `STG_T_WC_WIKIDATA_*` staging tables and validation triggers.
- `03_bulk_load_from_staging_FULL.sql` — merges staging rows into target tables (step 110). Idempotent.
- `04_reset_for_full_rerun.sql` — ordered `DELETE`s (FK checks off) to clear staging + targets before a fresh run.
- `05_progress_checks.sql` — per-batch progress counts (edit `@IMPORT_BATCH_ID` before running).
- `06_repair_qualifier_tables.sql` — one-off repair script for qualifier tables.
- `07_resolve_media_resources.sql` — populates the media-resource tables (step 112). Idempotent.
- `08_cleanup_old_batches.sql` — deletes target rows whose `IMPORT_BATCH_ID` is strictly older than the current batch (step 114). Removes stale orphans the upsert-only bulk load leaves behind. Idempotent; FK checks off; entity/property tables (no batch column) untouched.
- `09_fix_value_type_conflicts.sql` — manual repair for trigger error 1644 ("statement/qualifier already exists in another child table"). Deletes, for the current `@IMPORT_BATCH_ID`, target typed-value rows whose statement/qualifier is classified as a different `VALUE_TYPE` in this batch's staging — the stale sibling left when a statement's type flips between dumps. The same purge is built into `03_bulk_load_from_staging_FULL.sql` (a fresh run self-heals); this standalone file unblocks an in-flight load without rebuilding the image. Idempotent. Set `@IMPORT_BATCH_ID` before running.
- `10_clear_staging_batch.sql` — surgical staging cleanup: deletes every `STG_*` row for one specific `@OLD_BATCH_ID`, leaving the current batch intact. Use after a run stacked a new batch on top of an old one in staging (step 108 loads staging but never clears it; step 114 prunes targets only). Lighter than `04_reset_for_full_rerun.sql`, which clears all staging + targets. Set `@OLD_BATCH_ID` to the batch to remove.

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
| 106 | run ETL item_cache | Item-cache pass: emit `T_WC_WIKIDATA_ITEM` rows for referenced items and extra `T_WC_WIKIDATA_PERSON` rows for referenced (no-IMDb) persons. Requires pass1 + pass2 outputs. |
| 107 | validate ETL item_cache | Assert `run_summary.json` and item/person outputs exist. |
| 108 | load staging tables | Delete prior rows for this `IMPORT_BATCH_ID`, then load every NDJSON file into its `STG_*` table via `load_staging_jsonl.load_table`. |
| 109 | validate staging data | Assert staged statement rows exist for the batch and none have NULL `IMPORT_BATCH_ID`. |
| 110 | bulk load target tables | Execute `03_bulk_load_from_staging_FULL.sql` statement by statement; idempotent (filters `ROW_STATUS IN ('NEW','VALID')`, flips to `'LOADED'`). |
| 111 | validate target tables | Assert `T_WC_WIKIDATA_STATEMENT` is non-empty and staging rows were marked `LOADED`. |
| 112 | resolve media resources | Execute `07_resolve_media_resources.sql`; populate the media-resource tables from V2 statement/value tables. Idempotent (`INSERT ... ON DUPLICATE KEY UPDATE`); fully downstream of V2, safe to run alone via `--start-step 112`. |
| 113 | validate media resources | Assert media-resource tables non-empty; record per-platform counts. |
| 114 | cleanup old import batches | Execute `08_cleanup_old_batches.sql`; delete every target row whose `IMPORT_BATCH_ID` is strictly older than the current batch (stale "orphans" the upsert-only bulk load never overwrites). Guarded: refuses to run if the current batch has no statements. Idempotent; safe to run alone via `--start-step 114`. |

Important: do **not** start from `104` unless the code is changed to initialize the dump source when `101` is skipped — pass2/item_cache rely on `resolved_dump_file`/`resolved_dump_url` set by step 101.

The ETL passes each stream the full multi-GB dump and can take **multiple days** (the full ETL can exceed a week). Treat them as expensive: prefer resuming from a later step over re-running passes. The bulk load (110) and media resolution (112) are both idempotent and cheap by comparison.

NDJSON output dirs (inside the container): `/shared/pass1`, `/shared/pass2`, `/shared/item_cache` (host: `/home/debian/docker/shared_data/wikidata-crawler/<pass>`).

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
- HTTP identity: `WIKIMEDIA_USER_AGENT` — Wikimedia policy requires a descriptive User-Agent (`ToolName/version (URL; contact-email)`). Set it before hitting Wikimedia servers.
- Other: `USER_TIMEZONE` (default `Europe/Paris`).

Docker: built from `Dockerfile` (`ENTRYPOINT ["python", "wikidata_crawler.py"]`). Run via `wikidata-crawler.sh` (detached, `--network=host`, `--env-file .env`, host `/home/debian/docker/shared_data/wikidata-crawler` mounted as `/shared`). Args to the script are forwarded to the entrypoint. Rebuild the image after changing Python or SQL files.

## Encoding

All files are UTF-8. The database and connections use `utf8mb4` / `utf8mb4_unicode_ci` to preserve multilingual Wikidata labels and aliases (the bulk-load and media-resolution connections force this collation explicitly). NDJSON is written with `ensure_ascii=False`. Keep this file and any edits UTF-8.

---

**Last Updated**: 2026-06-01
**Current Version**: 1.0.0

## Backlog (Nestor second-brain)

The prioritized, agent-ready implementation backlog for this repo lives in the **Nestor**
knowledge repo (a separate repo, not cloned alongside this one):

- This repo: `C:\Users\vaugo\Nestor\projets\t2s-backlog\repos\wikidata-crawler.md`
- Cross-repo dashboard: `C:\Users\vaugo\Nestor\projets\t2s-backlog\index.md`

Consult it before implementing: tasks are `WIKIDATA-CRAWLER-NNN` with status (done / in-progress /
todo), priority, and quick-wins. NOTE: these are local paths on Philippe's PC and do not
resolve on the VPS or on cloud agents (claude.ai/code).
