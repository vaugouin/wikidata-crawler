# AGENTS.md - Agent Guide for Wikidata Crawler

Rules and traps for agents. Everything descriptive lives elsewhere, read it there:
- @README.md: canonical operational runbook (steps 101 to 115, rerun and resume, SQL file roles, backup, media resolution, V1 backfill, aliases, V2 extensions).
- @WIKIDATA.md: conceptual data model (statement / typed value / qualifier layers, tables, award modeling).
- @wikidata_dump_etl_README.md: ETL pass internals and manual single-pass runs.
- `doc/sql/*.sql`: reference DDL, read-only unless the user asks to edit schema documentation.
- Header comments of `run-if-new-dump.sh`, `check_new_dump.py`, `backup-after-run.sh`, and the docstrings of `wikidata_dump_etl.py`, carry the why of each mechanism.

Update `README.md` for any user-facing behavior change; update this file only when agent workflow or safety context changes.

---

## Related repositories (project ecosystem)

Dump-based Wikidata V2 ingestion stage of the **Agent BBB** system (siblings at `C:\Users\vaugo\Code\<repo>` and `github.com/vaugouin/<repo>`; roster in `%USERPROFILE%/Nestor/projets/t2s-backlog/topics/related-repositories.txt`). It **supersedes the SPARQL-driven V1 model** of `sparql-crawler` and `sparql-movies-persons`; its `T_WC_WIKIDATA_*` tables feed `tmdb-movie-preprocess` and `tmdb-front`.

`sparql-crawler.py`, `sparql-movies-persons.py` and `tmdb_functions.py` are V1-era legacy files the V2 pipeline does not invoke: do not edit them during V2 work unless asked. `tmdb_functions.py` is a byte-identical copy of tmdb-crawler's: change it there first, then here, and compare hashes. `run_etl.sh` is the legacy 3-pass driver, superseded by `wikidata_crawler.py`.

## Code conventions

- Legacy/shared layer (`citizenphil.py`, `sparql-*`, `tmdb_functions.py`): Hungarian-ish names (`str*`, `lng*`, `arr*`, `int*`) and `f_*` helpers; reuse `citizenphil` helpers (`f_getconnection`, `f_setservervariable`, `f_sqlupdatearray`...) rather than ad-hoc connections. V2 modules (`wikidata_crawler.py`, `wikidata_dump_etl.py`, `load_staging_jsonl.py`) use modern typed Python; match that style there.
- Env uses `DB_*` (`DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_NAMESPACE`) like the sibling repos; there is no `MARIADB_*` bridge. Never commit `.env`. Use a fresh `IMPORT_BATCH_ID` per full run (`wikidata_full_YYYYMMDD_HHMM`, which sorts chronologically: steps 114 and 115 rely on string `<`). `WIKIMEDIA_USER_AGENT` must be descriptive or Wikimedia answers 403. See `.env.example`.
- Live state goes to `T_WC_SERVER_VARIABLE` via `cp.f_setservervariable`, names prefixed `strwikidatacrawler`. **Keep run-level and pass-level times apart**: the orchestrator owns `startdatetime` / `enddatetime` / `totalruntime`; `wikidata_dump_etl.py` writes `passstartdatetime` / `passenddatetime` / `passruntime` / `passruntimeseconds`. (Until 2026-09-27 every pass overwrote the run's start.)
- **Clock (DEPLOYMENT-015):** naive `DATETIME` / `DATE` columns hold Europe/Paris time from `datetime.now(cp.paris_tz)`; never bare `datetime.now()` (UTC in the container), never `NOW()` / `CURDATE()`. A human-read date carries its Paris offset, never a fixed `+02:00`; labelled UTC stamps in shell scripts (batch names) may stay.
- Rebuild the Docker image after changing any Python or SQL file: the container runs what was baked in. Ship code changes that affect the ETL **before** `run-if-new-dump.sh` fires, or they wait a week (or cost a replay).

## SQL Object Naming Conventions

Shared with the sibling repos: uppercase snake case; prefixes `T_WC_*` (project), `T_WC_WIKIDATA_*` (this repo's targets), `STG_T_WC_WIKIDATA_*` (staging); keys `ID_{ENTITY}`; `DAT_*` dates, `TIM_*` timestamps, `IS_*` flags, `DELETED` soft delete; standard audit columns (`DELETED`, `DISPLAY_ORDER`, `ID_CREATOR`, `DAT_CREAT`, `ID_OWNER`, `TIM_UPDATED`, `ID_USER_UPDATED`) managed by `citizenphil.f_sqlupdatearray` when `intaddstdfields = 1`.

## Encoding

UTF-8 everywhere; DB and connections `utf8mb4` / `utf8mb4_unicode_ci`; NDJSON written with `ensure_ascii=False`.

**Every hand-written `.sql` for this database starts with `SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;`.** All tables are `utf8mb4_unicode_ci`, but the `mariadb` client connects as `utf8mb4_general_ci`, and any function-produced value (`CAST`, `CONVERT`, `CONCAT` over a number) inherits it, so comparing it to a column raises `ERROR 1267`. `--default-character-set=utf8mb4` does not fix it (charset, not collation). Pass `--force` too. When an explicit `COLLATE` is needed, put it on the non-indexed side.

## Schema changes: where to edit

- **`apply_to_live_db.sql`** is applied automatically at steps 108 and 110 and is the only way a new table or column reaches the live DB (`CREATE TABLE IF NOT EXISTS` never alters an existing table, hence its `ADD COLUMN IF NOT EXISTS`). It must stay **`DELIMITER`-free**: the orchestrator splits it on `;`. Its staging definitions duplicate `02_staging_and_triggers.sql` (canonical, and the only file with triggers): change both together. A new entity column means four edits at once: `01_create_schema.sql`, `02_staging_and_triggers.sql`, and both DDL sets in `apply_to_live_db.sql`.
- **DDL first, emission second.** `load_staging_jsonl.py` derives columns from the first NDJSON line, so a new key travels on its own *only if the staging column exists*; otherwise step 108 dies after a day-long pass.
- **A new NDJSON file needs a `TableSpec`** in `load_staging_jsonl.py`, or it is written and never loaded (that is how the P279 graph stayed invisible for months).
- **Adding a value type** touches every value-table list: `01`, `02`, `03`, `11`, `04`, `05`, `08`, `09`, `10`, `13`, `TABLE_SPECS`, steps 109 and 111 (`06` is a one-off repair, leave it). Three traps: the three `VALUE_TYPE` columns are ENUMs, and `MODIFY` is instant on MariaDB 11.2 only when the new value goes at the END of the list; the live DB has **none** of `02`'s `BEFORE INSERT` value triggers (the one-statement-one-table rule rests on the section-3B purges of `03`), so never hand-run that trigger block on the live DB without measuring the load cost; `emit_class_claims_for_cached_item` has its own per-type branches, so a type added only to `SUPPORTED_LOCAL_VALUE_TYPES` is never emitted for cached items.
- **`CACHED_ENTITY_PROPERTIES` is closed on purpose**: each added property costs a ~23 h replay of step 106. Keep `emit_class_claims_for_cached_item` a dedicated method (its docstring says why); emission stays gated on the entity row having been written.
- `ALIASES_JSON` is a blob on the entity tables by design, fr + en only (`ALIAS_LANGUAGES`); after shrinking it, `OPTIMIZE TABLE` on `ITEM` and `PERSON` to reclaim space. See README § aliases.

## Pipeline / passes

Steps, resume rules and costs: README § "End-to-end workflow". What an agent must not forget:

- Passes 102, 104 and 106 stream the full 102 GB dump and take 20 to 33 h each; prefer resuming from a later step. Before resuming at one of them, check the dump is still in `/shared` and is the same dump (size vs `strwikidatacrawlerdumpsize`); if not, prefer a full run.
- **V1 backfill seed (step 106, WIKIDATA-CRAWLER-023): anchor it on V1 alone, never on the difference "in V1, absent from V2".** That difference self-erases after one import, the seed empties, and step 114 then deletes those items under a run reporting SUCCESS. The difference is the *measure* (`--v1-backfill-report`), V1 is the *seed*. Never modify pass2's `referenced_item_ids.txt`; the seed is a separate file.
- `run-if-new-dump.sh` must wipe `/shared` **from a container** (`docker run ... find /shared -mindepth 1 -delete`): the pass directories belong to root, a host `rm -rf` leaves them behind, and pass1 then silently reuses stale output. Do not "simplify" it to a host-side delete. Likewise `wikidata-crawler.sh` follows logs only on a TTY, so cron does not stay alive for days.
- `backup-after-run.sh` **verifies the backup itself** (exit code, `Backup failed!` / `Error:` in output, real `.gz` size): do not reduce it to trusting `backupvaugouindb.sh`'s exit code, the VPS copy can lag behind the `tools` repo. History in README § "Ce que fait le script appele".
- Recursive CTEs on this schema: MariaDB types CTE columns from the non-recursive part alone, and with a multi-branch anchor (`root UNION root UNION recursion`) it keeps only the first branch, silently. Group the roots in a subquery (one anchor branch, one recursive branch), `CAST(... AS CHAR(50))` the id, and give every such query a floor value (842 classes under film, see README § V2 extensions).

## Rerun strategy (do not reset the target tables)

A full rerun **loads on top of the existing target tables**. Never advise clearing them first, and never run `04_reset_for_full_rerun.sql` in a routine rerun: the front-end would have no V2 data for the whole multi-day ETL. Why it is safe (stable ids, upsert-only step 110, step 114 pruning afterwards) and the four cases that do justify a reset: README § "Rerun strategy: incremental by default". Flag the downtime explicitly when recommending a reset.

> **Never make a qualifier's identity its snak hash alone.** The snak hash covers the value, not the occurrence; identity built on it collapsed every repeated qualifier into one row (WIKIDATA-CRAWLER-019, caught months later). It is keyed on the parent `ID_STATEMENT`. Any change to `derive_statement_identity`, `derive_qualifier_identity` or `stable_bigint_from_text` breaks the upsert match and requires a reset; `migrations/repair_qualifier_identity.py` carries its own copy of the formula, pinned by `tests/test_etl_smoke.py`, so change both.

## Before launching: is there actually a new dump?

Never launch blind, and never delete the local dump before checking: the weekly dump takes about four days to publish, and launching in between re-ingests the previous one for three to four days of VPS. Use `./wikidata-crawler.sh --check-dump` (never forwarded to the crawler) or let the hourly `run-if-new-dump.sh` cron decide; the reasoning is in the headers of `check_new_dump.py` and `run-if-new-dump.sh`.

## Never conclude from `information_schema`

Use it to explore, never to decide:
- `TABLE_ROWS` is an estimate (22 % under on `T_WC_WIKIDATA_MOVIE` on 2026-08-16); `COUNT(*)` the entity tables, keep estimates for `STATEMENT` / `STATEMENT_QUALIFIER` only, and never compare an estimate to a reference figure. A stale estimate looks exactly like a table the run never touched; `ANALYZE TABLE` refreshes it.
- `UPDATE_TIME` is UTC while `TIM_UPDATED` is Paris time: read last writes from `MAX(TIM_UPDATED)`.
- A recent write on a V1 table is `wikipedia-crawler` filling image columns, not evidence that a SPARQL crawler still runs.

---

**Last Updated**: 2026-10-31
**Current Version**: 1.1.12

## Backlog (Nestor second-brain)

Backlog for this repo: `C:\Users\vaugo\Nestor\projets\t2s-backlog\repos\wikidata-crawler.md` (dashboard: `...\t2s-backlog\index.md`). Tasks are `WIKIDATA-CRAWLER-NNN`; consult it before implementing. Local paths on Philippe's PC only, they do not resolve on the VPS or cloud agents.
