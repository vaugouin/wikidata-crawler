# Wikidata crawler

This repository contains the full dump-based Wikidata pipeline used by the project:

- ETL from the Wikidata JSON dump
- MariaDB schema and staging tables
- bulk load from staging into final tables
- documentation for the current Wikidata V2 data model

This `README.md` is the operational runbook to use when you need to run the whole process again after days or weeks and want one place that explains what to do.

## Current status

The V2 schema is the production data model. A full end-to-end run of `wikidata_crawler.py` (steps `101` → `111`) has populated:

- `T_WC_WIKIDATA_MOVIE` / `T_WC_WIKIDATA_SERIE` / `T_WC_WIKIDATA_PERSON` / `T_WC_WIKIDATA_ITEM`
- `T_WC_WIKIDATA_PROPERTY_METADATA`
- `T_WC_WIKIDATA_STATEMENT` and all six main typed value tables
- `T_WC_WIKIDATA_STATEMENT_QUALIFIER` and all six qualifier typed value tables

The old `T_WC_WIKIDATA_ITEM_PROPERTY` table (V1, item-valued only) is no longer authoritative. Front-end pages should read from the V2 tables.

## Documentation map

Use these documents as the main references:

- `README.md`
  - operational runbook
  - rerun checklist
  - Docker commands
  - rerun strategy: incremental (zero-downtime, the default) vs full reset
- `wikidata_dump_etl_README.md`
  - lower-level ETL details
  - pass1 / pass2 / item_cache behavior
  - manual pass execution examples
- `WIKIDATA.md`
  - conceptual schema documentation
  - statement / typed value / qualifier model
  - award modeling notes
- `doc/mariadb-server-tuning.md`
  - **server installation**: InnoDB buffer pool sizing + bulk-load settings
  - belongs in the server install runbook
- `doc/wikidata-v1-v2-gap-analysis.md`
  - V1→V2 coverage gap, the classifier fix, new entity types, run-time breakdown
- `doc/collation-standardization.md`
  - database-wide charset/collation standardization plan (fixes cross-table `#1267` join errors)

## Main files

### Python

- `wikidata_crawler.py`
- `wikidata_dump_etl.py`
- `load_staging_jsonl.py`
- `citizenphil.py`

### SQL

- `01_create_schema.sql`
- `02_staging_and_triggers.sql`
- `03_bulk_load_from_staging_FULL.sql`
- `04_reset_for_full_rerun.sql` — **not** part of the normal rerun. Wipes staging + targets, which
  leaves the front-end without V2 data for the whole multi-day run. Reserved for the cases listed in
  "When a full reset IS required".
- `07_resolve_media_resources.sql`
- `08_cleanup_old_batches.sql`
- `09_fix_value_type_conflicts.sql` — manual repair for trigger error 1644 (statement/qualifier
  already exists in another child table); deletes stale typed-value siblings left when a
  statement's `VALUE_TYPE` flips between dumps. Same purge is built into
  `03_bulk_load_from_staging_FULL.sql`, so this is only for unblocking an in-flight load.
- `10_clear_staging_batch.sql`: deletes all `STG_*` rows for one `@OLD_BATCH_ID`, keeping every
  other batch; use it to drop one specific batch. Surgical alternative to
  `04_reset_for_full_rerun.sql`.
- `13_cleanup_staging_old_batches.sql`: hand-runnable twin of step `115`, deletes every `STG_*` row
  older than `@IMPORT_BATCH_ID`, leaving staging with just the batch that finished. The crawler does
  not execute this file (step 115 issues the same deletes in chunks); it is for cleaning a database
  by hand, or catching up runs that predate step 115.
- `apply_to_live_db.sql` — idempotent additive DDL (SEASON/EPISODE/CHARACTER target tables + the
  **full** `STG_T_WC_WIKIDATA_*` staging set); auto-applied by the crawler at steps 108 and 110 so a
  long-lived DB stays in sync with new tables and any staging table that was dropped by hand is
  re-created in place instead of aborting the run. Column definitions mirror
  `02_staging_and_triggers.sql`, which stays canonical — keep the two in sync when adding a column.

### Tests

- `tests/test_etl_smoke.py` — standalone, no-DB/no-network parity check of the ETL classifier and
  emission (subclass-typed detection, new entity types, IMDb gating, item cache). Run:
  `python tests/test_etl_smoke.py`

### Documentation and scripts

- `README.md`
- `wikidata_dump_etl_README.md`
- `WIKIDATA.md`
- `doc/mariadb-server-tuning.md` — server-install MariaDB tuning (InnoDB buffer pool + load settings)
- `doc/wikidata-v1-v2-gap-analysis.md` — V1→V2 gap, classifier fix, new entity types, ETL performance
- `doc/collation-standardization.md` — database-wide charset/collation standardization plan
- `wikidata-crawler.sh`
- `backup-after-run.sh` : sauvegarde de la base declenchee par un run reussi, voir
  "Sauvegarde de la base apres un run" plus bas

## Current architecture summary

The current model is no longer just statement + typed values.

It is now:

- statement layer
- main typed value layer
- qualifier layer

### Statement layer

Main parent table:

```text
T_WC_WIKIDATA_STATEMENT
```

Each row represents one Wikidata statement.

Important fields include:

- `ID_STATEMENT`
- `ID_WIKIDATA`
- `ID_PROPERTY`
- `STATEMENT_GUID`
- `VALUE_TYPE`
- `WIKIDATA_DATATYPE`
- `RANK`

### Main typed value layer

Typed child tables for the statement main value:

```text
T_WC_WIKIDATA_ITEM_VALUE
T_WC_WIKIDATA_STRING_VALUE
T_WC_WIKIDATA_EXTERNAL_ID_VALUE
T_WC_WIKIDATA_MEDIA_VALUE
T_WC_WIKIDATA_TIME_VALUE
T_WC_WIKIDATA_QUANTITY_VALUE
```

Each statement must have exactly one main typed value row in exactly one of these tables.

### Qualifier layer

Qualifiers are modeled in a parallel parent/typed-value structure:

```text
T_WC_WIKIDATA_STATEMENT_QUALIFIER
T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE
T_WC_WIKIDATA_QUALIFIER_STRING_VALUE
T_WC_WIKIDATA_QUALIFIER_EXTERNAL_ID_VALUE
T_WC_WIKIDATA_QUALIFIER_MEDIA_VALUE
T_WC_WIKIDATA_QUALIFIER_TIME_VALUE
T_WC_WIKIDATA_QUALIFIER_QUANTITY_VALUE
```

Each qualifier belongs to a statement and must have exactly one qualifier typed value row.

### Why qualifiers matter

This is important for awards and similar patterns.

Typical Wikidata award modeling is:

- main statement `P166` = award
- qualifier `P585` = date/year
- qualifier `P1686` = related work

So the model now supports deriving award tables without flattening qualifier columns into the main statement table.

## End-to-end workflow

The orchestrator is `wikidata_crawler.py`.

Its steps are:

- `101` resolve dump source
- `102` run ETL pass1
- `103` validate ETL pass1
- `104` run ETL pass2
- `105` validate ETL pass2
- `106` run ETL item_cache
- `107` validate ETL item_cache
- `108` load staging tables
- `109` validate staging data
- `110` bulk load target tables
- `111` validate target tables
- `112` resolve media resources
- `113` validate media resources
- `114` cleanup old import batches
- `115` cleanup old staging batches

For a full rerun, start from `101`.

Do not start from `104` unless the code is explicitly changed to initialize the dump source when `101` is skipped.

Steps `112` and `113` are fully downstream of V2 and can be re-run on their own with `--start-step 112` — no need to redo the dump ETL or the bulk load. See "Media resolution (steps 112 & 113)" below.

## Rerun strategy: incremental by default (zero downtime)

**Do not clear the target tables before a rerun.** The pipeline is built so a full rerun never leaves the V2 tables empty: the front-end keeps serving the previous run's data throughout the multi-day ETL, and the switch-over happens row by row during the bulk load.

Three properties make this work:

1. **Statement and qualifier IDs are deterministic.** `derive_statement_identity` in `wikidata_dump_etl.py` hashes the Wikidata `STATEMENT_GUID` (SHA-256 truncated to a positive BIGINT), so the same statement gets the same `ID_STATEMENT` in every run. Qualifier IDs are derived the same way from the snak hash.
2. **The bulk load (step `110`) is an upsert, not a replace.** Every INSERT in `03_bulk_load_from_staging_FULL.sql` ends with `ON DUPLICATE KEY UPDATE`. Rows that already exist are updated **in place** — value and `IMPORT_BATCH_ID` refreshed — never duplicated.
3. **Step `114` performs the deletion `04_reset` would have done, but afterwards.** `08_cleanup_old_batches.sql` removes every target row whose `IMPORT_BATCH_ID` is strictly older than the current batch — precisely the statements that disappeared from the new dump.

### What the front-end sees during a rerun

| Pipeline stage | State of the V2 target tables |
|---|---|
| Steps `101` → `107` (ETL, several days) | previous run's data, untouched |
| Steps `108` / `109` (staging load + validation) | previous run's data, untouched |
| Step `110` (bulk load, hours) | updated row by row — never empty |
| Steps `112` / `113` (media resolution) | current batch |
| Step `114` (cleanup) | old-batch orphans pruned |
| Step `115` (staging cleanup) | untouched (staging only) |

Time with no Wikidata V2 data in the database: **zero**.

### Trade-offs of the incremental path

- **Disk.** Because IDs are stable, re-loading a statement updates its row instead of adding one, so the tables do not double in size. Only genuinely new statements grow them, plus the old-batch orphans that linger until step `114`. Keep headroom anyway — an InnoDB `DELETE` does not return space to the filesystem.
- **Entity tables are never pruned.** `T_WC_WIKIDATA_MOVIE` / `SERIE` / `PERSON` / `ITEM` / `SEASON` / `EPISODE` / `CHARACTER` and `T_WC_WIKIDATA_PROPERTY_METADATA` have no `IMPORT_BATCH_ID`, so step `114` leaves them alone by design. An entity that falls out of scope keeps its row (with zero statements after the cleanup). Cosmetic, but it never self-heals — only a full reset removes it.
- **`VALUE_TYPE` flips (trigger error 1644)** are the classic hazard of loading on top of existing data — they happen precisely *because* the old batch is still there. Already handled: the purge of stale typed-value siblings is built into `03_bulk_load_from_staging_FULL.sql` (section 3B). Just rebuild the Docker image so the container runs the current SQL.
- **Staging holds two batches for the length of a run.** Step `108` deletes rows for the *current* `IMPORT_BATCH_ID` before loading (which is what makes a `--start-step 108` resume safe) and leaves older batches alone; step `115` removes them at the very end, once the bulk load has succeeded. So staging peaks at two full batches, well over 100 M rows, and comes back down to one when the run finishes. A run that fails before `115` leaves the predecessor behind: clear it with a standalone `--start-step 115` or with `13_cleanup_staging_old_batches.sql`. Staging is not read by the front-end, so the timing is unconstrained.

### When a full reset IS required

Run `04_reset_for_full_rerun.sql` only when one of these applies:

- the ID-derivation logic changed (`derive_statement_identity`, `derive_qualifier_identity`, or `stable_bigint_from_text` in `wikidata_dump_etl.py`) — old rows would no longer be matched by the upsert and would survive as permanent duplicates;
- the target schema changed in a way that makes existing rows meaningless;
- the current data is known to be corrupt and you want a guaranteed-clean rebuild;
- you specifically want the entity tables pruned of out-of-scope rows.

In those cases accept that the V2 tables stay empty for the whole run, ETL included. A clean rebuild *without* downtime is not supported out of the box — it would mean loading into a copy of the schema and swapping with `RENAME TABLE`.

## What must be done before a new full run

This section is the most important one for future reruns.

### 1. Remove the existing `latest-all.json.bz2`

If `.env` contains both:

- `DUMP_URL`
- `DUMP_FILE`

then the pipeline will reuse the local file pointed to by `DUMP_FILE` if it already exists.

That means if you want a fresh latest dump, you must delete the cached file before the run.

Host example:

```bash
rm -f /home/debian/docker/shared_data/wikidata-crawler/latest-all.json.bz2
```

This is required because the workflow is designed to download from `DUMP_URL` into `DUMP_FILE` only when the local file does not already exist.

### 2. Change `IMPORT_BATCH_ID` in `.env`

Always use a new batch id for a new full run.

Example:

```env
IMPORT_BATCH_ID=wikidata_full_20260509_1730
```

Recommended format:

```text
wikidata_full_YYYYMMDD_HHMM
```

This lets you distinguish one full run from another in staging and in validation.

### 3. Leave the target tables alone

**Nothing to do here for a normal rerun.** Do not run `04_reset_for_full_rerun.sql`: the bulk load upserts on top of the existing rows and step `114` prunes what is left over, so the V2 tables stay populated and serviceable from start to finish. See "Rerun strategy: incremental by default (zero downtime)" above for the full rationale, the trade-offs, and the short list of situations that genuinely call for a reset.

Nothing to do about staging either: step `115` clears the previous batch at the end of the run, once the new one is loaded. To reclaim that space *before* the run instead (staging is dead weight, tens of millions of rows, invisible to the front-end), drop one named batch by hand:

```sql
-- set @OLD_BATCH_ID to the batch you want gone, then:
SOURCE 10_clear_staging_batch.sql;
```

If you have determined that a full reset is required, run it now — and expect no V2 data in the database until step `110` completes:

```sql
SOURCE 04_reset_for_full_rerun.sql;
```

The reset script uses ordered `DELETE` statements with `FOREIGN_KEY_CHECKS` disabled, not `TRUNCATE`, because MariaDB/MySQL does not allow truncating a table that is referenced by a foreign key constraint.

### 4. Rebuild the Docker image

If you changed Python or SQL files, rebuild before the run:

```bash
docker build -t wikidata-crawler-python-app .
```

The rebuild also installs `indexed_bzip2` (in `requirements.txt`), which enables multi-core
decompression of the local dump — a significant speedup across all three ETL passes. The ETL falls
back to single-threaded `bz2` if it is unavailable, so the build/run never breaks on its account.
Tune the core count with the `BZ2_PARALLELISM` env var (default: all cores).

### 4b. Tune the MariaDB server (one-time, highly recommended)

Before the first big run, raise the InnoDB buffer pool and apply the load settings — the single
highest-value, lowest-risk performance change, and it benefits every database on the instance. See
`doc/mariadb-server-tuning.md`. The crawler already applies the safe session-scoped load pragmas
itself at step 110; the buffer pool is the one server-side step you set manually. The new
SEASON/EPISODE/CHARACTER tables are created automatically by `apply_to_live_db.sql` during the run —
no manual DDL needed.

### 5. Start the container in detached mode

The recommended mode is detached/background execution.

Use:

```bash
docker run -d --rm --network="host" \
  --name wikidata-crawler \
  --env-file .env \
  -v /home/debian/docker/shared_data/wikidata-crawler:/shared \
  wikidata-crawler-python-app
```

This matches the pattern used in `wikidata-crawler.sh`.

### 6. Follow logs explicitly

After starting the container, inspect the logs:

```bash
docker logs -f wikidata-crawler
```

This is important because a full rebuild can take a long time and you want the logs continuously visible.

`wikidata-crawler.sh` now follows this exact pattern:

- build image
- run container with `docker run -d`
- follow logs with `docker logs -f wikidata-crawler`

Any arguments passed to `wikidata-crawler.sh` are forwarded to `wikidata_crawler.py` inside the container, so the same script handles fresh runs and resumes:

```bash
./wikidata-crawler.sh                       # full run, starts at step 101
./wikidata-crawler.sh --start-step 110      # resume bulk load + final validation only
./wikidata-crawler.sh --start-step 108      # resume from staging load onward
```

See the "Resuming after a failure" section below for when to use `--start-step`.

## Full rerun checklist

When restarting everything from scratch, use this checklist in order.

### Host and environment

- confirm `.env` contains the correct MariaDB credentials
- confirm `.env` contains the expected `DUMP_URL`
- confirm `.env` contains the expected `DUMP_FILE`
- change `IMPORT_BATCH_ID`
- remove `/home/debian/docker/shared_data/wikidata-crawler/latest-all.json.bz2`

### Database

- **do not** reset the target tables — the rerun loads on top of them (see "Rerun strategy" above)
- **do not** clear staging either: step `115` drops the previous batch at the end of the run; run `SOURCE 10_clear_staging_batch.sql;` by hand only if you need that disk space *before* the run
- run `SOURCE 04_reset_for_full_rerun.sql;` only in the cases listed under "When a full reset IS required"

### Docker

- rebuild image with `docker build -t wikidata-crawler-python-app .`
- run container in detached mode
- follow logs with `docker logs -f wikidata-crawler`

## Recommended command sequence for a fresh full rerun

### 1. Remove the cached dump

```bash
rm -f /home/debian/docker/shared_data/wikidata-crawler/latest-all.json.bz2
```

### 2. Rebuild the image

```bash
docker build -t wikidata-crawler-python-app .
```

### 3. Skip the database reset

There is deliberately no reset step here. The target tables keep serving the previous run while the new one builds; step `114` prunes the target leftovers at the end and step `115` prunes the staging ones.

Optional housekeeping, drop the previous batch from staging *early*, if you need the disk space before the run rather than after it (set `@OLD_BATCH_ID` first):

```sql
SOURCE 10_clear_staging_batch.sql;
```

### 4. Launch the full pipeline

Easiest path — `wikidata-crawler.sh` builds the image, starts the container detached, and tails the logs:

```bash
./wikidata-crawler.sh
```

Equivalent raw docker command (use this if you need to tweak flags):

```bash
docker run -d --rm --network="host" \
  --name wikidata-crawler \
  --env-file .env \
  -v /home/debian/docker/shared_data/wikidata-crawler:/shared \
  wikidata-crawler-python-app \
  --start-step 101
```

### 5. Watch logs

```bash
docker logs -f wikidata-crawler
```

(`wikidata-crawler.sh` already tails the logs after starting the container, so this is only needed if you used the raw `docker run` form.)

### 6. Check database progress periodically

Use:

```sql
SOURCE 05_progress_checks.sql;
```

Before running it, edit `@IMPORT_BATCH_ID` at the top of the file so it matches the current run.

## Resuming after a failure

If a previous run got partway through and crashed, you can resume from the failed step instead of redoing the whole pipeline (the ETL passes alone can take more than a week).

`wikidata_crawler.py` accepts `--start-step N`, where `N` is one of the step codes listed in the "End-to-end workflow" section. `wikidata-crawler.sh` forwards any arguments straight through to the container.

The bulk load (step `110`) is **idempotent and safe to resume**: every INSERT in `03_bulk_load_from_staging_FULL.sql` filters staging rows on `ROW_STATUS IN ('NEW','VALID')`, and the matching UPDATE flips them to `'LOADED'`. On a re-run, already-loaded rows are filtered out by the SELECT and `ON DUPLICATE KEY UPDATE` covers any partial inserts.

Check the server variable `strwikidatacrawlerbulkloadlaststatement` to see which SQL statement index the previous run committed last. That tells you where to look in `03_bulk_load_from_staging_FULL.sql` for the cause of the failure.

### Common resume scenarios

```bash
./wikidata-crawler.sh --start-step 110      # bulk load + final validation (skips the multi-day ETL)
./wikidata-crawler.sh --start-step 108      # reload staging tables, then bulk load
./wikidata-crawler.sh --start-step 104      # rerun pass2 onward (only valid if dump source already cached)
```

Do not start from `104` unless the code is explicitly changed to initialize the dump source when `101` is skipped.

### Before resuming, fix the root cause

Resuming only makes sense once whatever broke the previous run has been corrected. Typical causes:

- **`Unknown column` / `Table doesn't exist`** — the live MariaDB schema has drifted from `01_create_schema.sql`. Run `SHOW CREATE TABLE <name>` and align with an `ALTER TABLE`, or drop and recreate the offending tables. Then resume with `--start-step 110`.
- **`Table 'vaugouindb.STG_T_WC_WIKIDATA_*' doesn't exist` at step `108`** — the staging tables were dropped from the live DB (they hold tens of millions of rows and are often cleared by hand to reclaim disk space). Nothing is lost: staging is rebuilt from the ETL NDJSON, which is still in `/shared`. `apply_to_live_db.sql` now re-creates the full staging set, so **rebuild the image** and resume with `--start-step 108`. To repair without rebuilding, run `SOURCE 02_staging_and_triggers.sql;` on the live DB first (it drops and re-creates every `STG_*` table and re-installs the target-table triggers; it never touches `T_WC_WIKIDATA_*` data).
- **ETL aborted mid-pass** — the partial output in `/shared/pass1` (or `pass2`, `item_cache`) is no longer trustworthy. Re-run from the failing pass, not the bulk load.
- **Staging load failed** — fix the data issue, then resume with `--start-step 108`.
- **`❌ MySQL Error: (2013, 'Lost connection to MySQL server during query')` after an ETL pass** — not fatal, but it means the pass's progress/status never reached `T_WC_SERVER_VARIABLE`. An ETL pass runs for ~20–33 h between two server-variable writes, so MariaDB closes the pooled connection on `wait_timeout` while pymysql still believes it is open. `citizenphil.f_getconnection()` now pings and reconnects before reusing a pooled connection; rebuild the image to pick it up.
- **`403 Forbidden` on `dumps.wikimedia.org` at step `101`** — Wikimedia refuses requests carrying a default library User-Agent. Set `WIKIMEDIA_USER_AGENT` in `.env` to a descriptive value (`ToolName/version (URL; contact-email)`); the download sends it as required. If you rebuilt an old image, rebuild again — the header was missing from step `101` before this fix, even when the variable was set (the ETL passes always sent it, so the failure only showed up on the download).

The step `101` download resumes with an HTTP `Range` request and retries up to 20 times with exponential backoff, writing to `<DUMP_FILE>.part` and renaming only on success — a dropped connection mid-download no longer costs the whole transfer, and never leaves a truncated file that a later run would mistake for a complete cached dump.

## Why the reset is no longer the default

Earlier versions of this runbook recommended clearing staging **and** the target tables before every rerun. That advice predates two changes that removed its justification:

- **deterministic statement/qualifier IDs** (commit `2ec8534`) — the same claim resolves to the same `ID_STATEMENT` across dumps, so the upsert genuinely updates rather than accumulating parallel rows;
- **step `114` old-batch cleanup** and the built-in `VALUE_TYPE`-flip purge (commit `d117de1`) — the two failure modes that made loading on top of existing data unpleasant are now handled by the pipeline itself.

The remaining concerns from that era still hold and are addressed elsewhere:

- staging reloads can duplicate rows if rerun carelessly → step `108` deletes the current batch before loading, and step `115` removes the older ones at the end of the run (`13_cleanup_staging_old_batches.sql` is its manual twin);
- step `104` depends on dump-source initialization from step `101` → still true, still a reason to start full reruns at `101`;
- statement and qualifier IDs must remain stable across reruns → still true, and it is exactly the condition under which the incremental path is safe. If you change that logic, reset (see "When a full reset IS required").

So the reliable rerun procedure is now:

- clear the cached dump file
- use a fresh batch id
- leave the target tables in place
- rerun from `101`

## SQL files and their roles

- `01_create_schema.sql`
  - creates final target tables
- `02_staging_and_triggers.sql`
  - creates staging tables and validation triggers
- `03_bulk_load_from_staging_FULL.sql`
  - loads staging rows into target tables
- `04_reset_for_full_rerun.sql`
  - clears staging and target tables (including the media-resolution layer) before a fresh full rebuild. **Not part of the normal rerun** — it leaves the front-end with no V2 data for the entire multi-day run. Use only in the cases listed under "When a full reset IS required"; the default path loads on top of the existing tables and lets step `114` prune the leftovers.
- `05_progress_checks.sql`
  - reports staging and target progress counts for a given `IMPORT_BATCH_ID`
- `07_resolve_media_resources.sql`
  - executed by step `112`; populates `T_WC_WIKIDATA_MEDIA_RESOURCE` and `T_WC_WIKIDATA_MEDIA_RESOURCE_URL` from V2 statement/value tables using `INSERT ... ON DUPLICATE KEY UPDATE` (idempotent)
- `08_cleanup_old_batches.sql`
  - executed by step `114`; deletes every target row whose `IMPORT_BATCH_ID` is strictly older than the current batch — the stale "orphans" the upsert-only bulk load never overwrites (entities that fell out of scope, claims deleted/edited between dumps). Idempotent; FK checks off. Entity and property-metadata tables have no `IMPORT_BATCH_ID` and are left untouched. The step is guarded: it refuses to run unless the current batch already has statements loaded.
- `09_fix_value_type_conflicts.sql`
  - manual repair for bulk-load trigger error 1644 (`<TABLE>: statement/qualifier already exists in another child table`). When a statement's (or qualifier's) `VALUE_TYPE` changes between two dumps, the upsert-only bulk load updates the parent type but leaves the earlier batch's value row in the now-wrong sibling table, which the "one statement → one value table" triggers reject. This script deletes, for the current `@IMPORT_BATCH_ID`, every target typed-value row whose statement/qualifier is classified as a different `VALUE_TYPE` in this batch's staging. Idempotent. The same purge is built into `03_bulk_load_from_staging_FULL.sql`, so a fresh run self-heals; run this standalone only to unblock an in-flight load without rebuilding the image (set `@IMPORT_BATCH_ID`, then resume with `--start-step 110`).
- `10_clear_staging_batch.sql`
  - surgical staging cleanup: deletes every `STG_*` row for one `@OLD_BATCH_ID`, leaving every other batch intact. Use it to drop one named batch, typically to reclaim its space *before* a run rather than waiting for step `115` to do it at the end. Set `@OLD_BATCH_ID` to the batch to remove. Lighter than `04_reset_for_full_rerun.sql`, which wipes all staging + targets for a full rebuild.
- `13_cleanup_staging_old_batches.sql`
  - hand-runnable twin of step `115`: deletes every `STG_*` row whose `IMPORT_BATCH_ID` is strictly older than `@IMPORT_BATCH_ID`, leaving staging with exactly the batch that just loaded. The crawler does **not** execute this file. Step 115 issues the same deletes in committed 50 000-row chunks, deriving its table list from `TABLE_SPECS` in `load_staging_jsonl.py`. Use it to clean a database by hand, or to catch up a run that predates step 115. Set `@IMPORT_BATCH_ID` to the batch to **keep**, and run the two guard queries in its header first.

## Cleanup of old import batches (step 114)

Step `114` runs at the very end of the pipeline and prunes leftovers from previous runs. Because the bulk load (step 110) is **upsert-only** and filtered to the current `IMPORT_BATCH_ID`, any statement that existed in an earlier run but is no longer emitted (its entity went out of scope, or the claim was deleted/edited in Wikidata) keeps its **old** `IMPORT_BATCH_ID` forever and is never overwritten. Over successive runs these stale "orphan" rows accumulate.

Step `114` deletes every target row whose `IMPORT_BATCH_ID` is **strictly older** than the current batch (the recommended `wikidata_full_YYYYMMDD_HHMM` id format sorts chronologically, so a plain string `<` is a valid "is older than" test). It cascades through the child value/URL/check tables (via a join to their old-batch parent) before deleting the four batch-stamped parents (`T_WC_WIKIDATA_STATEMENT`, `T_WC_WIKIDATA_STATEMENT_QUALIFIER`, `T_WC_WIKIDATA_MEDIA_RESOURCE`, `T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK`).

It records `strwikidatacrawlercleanuprowsdeleted` (total rows removed) and `strwikidatacrawlercleanupbatchid` (the cutoff) as server variables. It is idempotent and safe to run on its own:

```bash
./wikidata-crawler.sh --start-step 114      # prune old batches only
```

**Safety guard.** The step refuses to delete anything unless the current `IMPORT_BATCH_ID` already has statements in `T_WC_WIKIDATA_STATEMENT`. This prevents a misconfigured or not-yet-loaded batch id from wiping every prior batch. It is a lighter, incremental alternative to `04_reset_for_full_rerun.sql`: the reset clears *everything* before a fresh rebuild, whereas step 114 keeps the current batch and removes only what is older.

This is what makes the reset unnecessary: step `114` performs the same deletion, but **after** the new data has landed instead of before. That single reordering is what turns a multi-day outage of the V2 tables into no outage at all. See "Rerun strategy: incremental by default (zero downtime)".

## Cleanup of old staging batches (step 115)

Step `115` is the staging counterpart of step `114`, and the last step of the pipeline. Step `108` deletes only the *current* `IMPORT_BATCH_ID` from the `STG_*` tables before loading it (that is what makes a `--start-step 108` resume safe), so without a final sweep every run leaves its predecessor behind, and staging grows one full batch per run: well over 100 M rows and several GB that nothing reads again.

Step `115` deletes every `STG_*` row whose `IMPORT_BATCH_ID` is **strictly older** than the current batch, using the same chronological-string comparison as step `114`. When it finishes, staging holds exactly one batch: the one that just loaded, still available for a `--start-step 110` re-load.

Two details worth knowing:

- **It deletes in chunks.** A whole previous batch is far more rows than step `114` ever touches, so the step issues `DELETE ... LIMIT 50000` in a loop and commits each chunk, instead of building one enormous undo log on a step whose whole point is to give disk space back. That also makes it interruptible: killing it mid-way loses nothing, and re-running finishes the job.
- **It is guarded twice.** It refuses to run unless the current batch is present in `STG_T_WC_WIKIDATA_STATEMENT` (otherwise a wrong or empty `IMPORT_BATCH_ID` would sort above every real batch and empty staging completely) **and** already present in `T_WC_WIKIDATA_STATEMENT` (older staging is the fallback until the bulk load has succeeded).

It records `strwikidatacrawlerstagingcleanuprowsdeleted`, `strwikidatacrawlerstagingcleanupbatchid` and `strwikidatacrawlerstagingcleanuplasttable` as server variables. Idempotent, and safe to run on its own:

```bash
./wikidata-crawler.sh --start-step 115      # prune old staging batches only
```

The table list comes from `TABLE_SPECS` in `load_staging_jsonl.py`, the same source step `108` uses, so a staging table added to the loader is pruned here automatically. The three `STG_T_WC_WIKIDATA_MEDIA_RESOURCE*` tables are not in that list because the pipeline never writes them (step `112` fills the target tables directly); `13_cleanup_staging_old_batches.sql` covers them anyway.

**InnoDB does not return freed pages to the filesystem.** The rows are gone and the space is reused by the next batch, but the `.ibd` files do not shrink. If you need the disk back on the host, run `OPTIMIZE TABLE` on the large staging tables at a quiet moment.

## Sauvegarde de la base apres un run (`backup-after-run.sh`)

The end of a run is the right moment for a full database backup: the target tables carry the current batch, step `114` has removed the previous batches' rows and step `115` has cleared their staging. The database is then in the cleanest, smallest state of the week, and nothing will touch it for another three or four days.

That backup cannot be a step `116`. The backup script lives on the host, in `~/docker/tools/backupvaugouindb.sh`, which is not mounted into the crawler's container. The decision therefore stays on the host, where both are reachable.

**How the host knows a run succeeded.** The container runs with `--rm`, so once it exits nothing is left on the host and `/shared` is the only common ground. A successful run drops `/shared/last_successful_run.json` there:

```json
{"status": "SUCCESS", "import_batch_id": "wikidata_full_20260807_1043", "start_step": 101, "steps_executed": "101, 102, ...", "total_runtime": "3 days, 4 hours", "finished_at": "2026-08-10 14:22:07"}
```

`backup-after-run.sh` reads it, compares the batch id with the marker file `.last-backup-batch` in the stack directory, and backs up only when they differ. One run therefore produces exactly one backup, however many times the script is invoked. If the backup fails, the marker is **not** written, so the next hourly tick retries.

**No cron line to add.** `run-if-new-dump.sh` calls it on every hourly tick, before checking the dump. The order matters: if the dump turns out to be new, that same script wipes the shared volume and launches a run that will modify the database for three days, so the backup of the finished state has to go first. A backup failure is reported but does not block the launch.

By hand:

```bash
./backup-after-run.sh              # sauvegarde si un run reussi l'attend
./backup-after-run.sh --wait       # attend la fin du conteneur en cours, puis sauvegarde
./backup-after-run.sh --force      # refait la sauvegarde d'un lot deja fait
./backup-after-run.sh --dry-run    # dit ce qu'il ferait, sans rien faire
```

`BACKUP_SCRIPT=/chemin/vers/autre.sh` overrides the script called; `STACK_DIR` and `SHARED_DIR` override the two directories. The backup script is invoked from its own directory (`cd` first), since a stack script usually expects to find its `.env` or `docker-compose.yml` next to it.

### Ce que fait le script appele, et pourquoi on verifie derriere lui

`backupvaugouindb.sh` (source: `tmdb-front`, deployed in `~/docker/tools/` since 2026-08-31, previously in `~/docker/damp-vaugouin-com/`) sources the database stack's `.env`, whose path is set inside the script and overridable with `ENV_FILE`, then runs `mariadb-dump` **inside** the database container via `docker exec`, writing `/backups/<db>-backup-<timestamp>.sql.gz` (a path inside that container).

Until 2026-08-31 its exit code could not be used as a success signal, for two independent reasons: it ended on `if [ $? -eq 0 ] ... else echo "Backup failed!"`, which **printed** the failure but never exited non-zero, so its status was that of the final `echo`, always `0`; and that internal `$?` was `gzip`'s rather than `mariadb-dump`'s, so an interrupted dump gave a truncated `.gz` and a cheerful "Backup successful!".

Both are fixed in `tmdb-front` now. The five `backupvaugouindb*.sh` scripts share one implementation, `backupvaugouindb-common.sh`, which runs the dump pipeline under `set -o pipefail`, exits non-zero on any failure, refuses to run when a table pattern matches nothing (an empty table list makes `mariadb-dump` dump the *whole* database under a filename promising a subset), passes the password through `MYSQL_PWD` instead of the command line, and verifies both the size of the `.gz` and the presence of mariadb-dump's `-- Dump completed on` marker.

`backup-after-run.sh` nevertheless keeps its own three checks, in order: the exit code, the absence of `Backup failed!` / `Error:` in the output, and the **actual size** of the produced file, read back with `docker exec <container> stat -c %s <file>` from the path the script prints. Two reasons to keep them: the copy deployed on the VPS can lag behind the repo, and the caller of a backup should not depend on the callee's discipline for something that only fails once every few months. The floor is 1 MB (`TAILLE_MINIMALE`). If the confirmation line ever changes shape and the path cannot be parsed, the size check is skipped with a warning rather than failing the chain.

### Deux choses corrigees au passage

**Les tables de staging sont exclues de la sauvegarde complete.** `backupvaugouindb.sh` passe desormais un `--ignore-table` par table `STG_*` (26 aujourd'hui, resolues depuis `information_schema` a chaque execution, donc une nouvelle table de staging est couverte sans rien editer). Apres l'etape `115`, le staging contient encore le lot courant, plus de 100 millions de lignes, et le dump tourne avec `--skip-extended-insert`, un `INSERT` par ligne : les inclure doublait a peu pres la taille et la duree de la sauvegarde, pour des donnees entierement reconstructibles depuis le NDJSON encore present sur `/shared`. Consequence a la restauration : `--ignore-table` saute la structure **et** les donnees, donc une base restauree n'a plus de tables `STG_*`. L'etape `108` les recree (`apply_to_live_db.sql`), ou bien lancer `02_staging_and_triggers.sql` a la main.

**Le dump est maintenant un instantane coherent.** `--lock-tables=false` a ete remplace par `--single-transaction`, qui enveloppe tout le dump dans une transaction REPEATABLE READ ouverte sur un instantane coherent. Il ne verrouille rien (lecteurs et ecrivains continuent, InnoDB sert au dump les anciennes versions des lignes depuis l'undo log) et ne coute pas de temps mesurable. Ce qu'il coute : l'historique undo ne peut pas etre purge tant que la transaction est ouverte, donc un long dump sur une periode d'ecriture soutenue fait grossir l'undo tablespace et laisse du travail de purge derriere lui ; et un DDL sur une table en cours de dump (`ALTER`, `TRUNCATE`, `DROP`) attend le verrou de metadonnees du dump, ou casse le dump. Aucun des deux n'est un souci dans cette fenetre, puisque le crawler a fini quand la sauvegarde demarre. Le gain, lui, est reel : ce schema porte de vraies cles etrangeres entre `T_WC_WIKIDATA_STATEMENT` et ses tables de valeurs typees, et un dump lu table par table pendant plusieurs heures peut tres bien contenir un parent sans ses enfants.

## Media resolution (steps 112 & 113)

Step `112` translates V2 statement rows into the three resolution tables:

- `T_WC_WIKIDATA_MEDIA_RESOURCE`     — one row per `(ID_STATEMENT, source_platform, identifier)`
- `T_WC_WIKIDATA_MEDIA_RESOURCE_URL` — URL variants (page / watch / embed / thumbnail / file)
- `T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK` — left empty (reserved for the optional HTTP-check job)

Sources covered:

| Source           | V2 input table                                              | Property filter | URL variants                |
|------------------|-------------------------------------------------------------|-----------------|-----------------------------|
| Wikimedia Commons | `T_WC_WIKIDATA_MEDIA_VALUE`                                | any             | `page`, `thumbnail`, `file` |
| YouTube          | `T_WC_WIKIDATA_EXTERNAL_ID_VALUE`                          | `P1651`         | `watch`, `embed`, `thumbnail` |
| Internet Archive | `T_WC_WIKIDATA_EXTERNAL_ID_VALUE`                          | `P724`          | `page`, `file`              |

Scope: only statements whose `ID_WIKIDATA` exists in `T_WC_WIKIDATA_MOVIE` / `SERIE` / `PERSON`. Deprecated and deleted statements are skipped.

All URLs are built from deterministic patterns — no network is hit during steps 112 / 113. Re-running step `112` is cheap and safe (`INSERT ... ON DUPLICATE KEY UPDATE`), so it is the right move whenever:

- a new wave of MOVIE / SERIE / PERSON entities is loaded,
- the URL patterns are changed in `07_resolve_media_resources.sql`,
- you want to refresh `LAST_RESOLVED_AT` timestamps with a new `IMPORT_BATCH_ID`.

Common command:

```bash
./wikidata-crawler.sh --start-step 112      # resolver + validation only (skips the dump ETL + bulk load)
```

Step `113` enforces non-zero row counts in `T_WC_WIKIDATA_MEDIA_RESOURCE` and `T_WC_WIKIDATA_MEDIA_RESOURCE_URL` and exposes per-platform counts as server variables (`strwikidatacrawlermediaresourcecommons`, `…youtube`, `…archive`).

A later, separate job is expected to populate `T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK` via HTTP HEAD / GET. That job is deliberately not part of the main pipeline because of its network cost.

## Notes on outputs

The ETL produces NDJSON files in:

```text
/shared/pass1
/shared/pass2
/shared/item_cache
```

Plus one file at the root of the shared volume, `/shared/last_successful_run.json`, written at the end of a successful run and read from the host by `backup-after-run.sh` (see "Sauvegarde de la base apres un run").

Important generated files include:

- `T_WC_WIKIDATA_PROPERTY_METADATA.jsonl`
- `T_WC_WIKIDATA_MOVIE.jsonl`
- `T_WC_WIKIDATA_SERIE.jsonl`
- `T_WC_WIKIDATA_PERSON.jsonl`
- `T_WC_WIKIDATA_ITEM.jsonl`
- `T_WC_WIKIDATA_STATEMENT.jsonl`
- `T_WC_WIKIDATA_ITEM_VALUE.jsonl`
- `T_WC_WIKIDATA_STRING_VALUE.jsonl`
- `T_WC_WIKIDATA_EXTERNAL_ID_VALUE.jsonl`
- `T_WC_WIKIDATA_MEDIA_VALUE.jsonl`
- `T_WC_WIKIDATA_TIME_VALUE.jsonl`
- `T_WC_WIKIDATA_QUANTITY_VALUE.jsonl`
- `T_WC_WIKIDATA_STATEMENT_QUALIFIER.jsonl`
- `T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE.jsonl`
- `T_WC_WIKIDATA_QUALIFIER_STRING_VALUE.jsonl`
- `T_WC_WIKIDATA_QUALIFIER_EXTERNAL_ID_VALUE.jsonl`
- `T_WC_WIKIDATA_QUALIFIER_MEDIA_VALUE.jsonl`
- `T_WC_WIKIDATA_QUALIFIER_TIME_VALUE.jsonl`
- `T_WC_WIKIDATA_QUALIFIER_QUANTITY_VALUE.jsonl`

## Minimal reminder for future you

Before the next full run:

- delete `latest-all.json.bz2`
- change `IMPORT_BATCH_ID` in `.env`
- **do not** run `04_reset_for_full_rerun.sql` — the target tables stay live and get upserted in place
- **do not** clear staging: step `115` does it at the end of the run
- rebuild Docker image
- run with `docker run -d`
- watch with `docker logs -f wikidata-crawler`
- start from `--start-step 101`
- step `114` auto-prunes target rows from older `IMPORT_BATCH_ID`s, which is what replaces the up-front reset, and it runs only once the new data is in
- step `115` does the same for staging, so the run ends with exactly one batch staged

## Additional references

- use `WIKIDATA.md` for the detailed conceptual model
- use `wikidata_dump_etl_README.md` for lower-level ETL and pass details

## Front-end consumption

The PHP front-end (`tmdb-front`) reads V2 directly:

- `lib/global-light.inc.php` exposes `f_wikidataallpropertiesv2($struilang, $stritemidwikidata, $strsep, $strexcludedproperties)`.
- It joins `T_WC_WIKIDATA_STATEMENT` with the six main typed value tables, attaches qualifiers from `T_WC_WIKIDATA_STATEMENT_QUALIFIER` + qualifier value tables, and resolves item labels against `T_WC_WIKIDATA_MOVIE` / `SERIE` / `PERSON` / `ITEM` (with a `LABELS_JSON` lookup for the UI language and a `LABEL_EN` fallback).
- Helper functions `f_wikidataitemlabel_v2`, `f_wikidataformattimevalue_v2`, and `f_wikidataformatqualifiers_v2` live in the same file.
- It is invoked from every `lib/*.inc.php` companion that renders Wikidata data (movie, serie, person, season, episode, award, death, group, movement, nomination, criterion, technical, list, t2scollection, t2stopic, t2slist, and `wikidata-query.inc.php`).

**V1 fallback.** `f_wikidataallpropertiesv2` probes `T_WC_WIKIDATA_STATEMENT` for the entity first; if no V2 statement exists for that `ID_WIKIDATA`, it transparently delegates to the legacy `f_wikidataallproperties` (which reads V1's `T_WC_WIKIDATA_ITEM_PROPERTY`). This keeps entity types not yet populated in V2 (e.g. some `technical.php` / `movement.php` records) from rendering an empty block.
