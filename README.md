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
  - reset procedure before a fresh full run
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
- `04_reset_for_full_rerun.sql`
- `07_resolve_media_resources.sql`
- `apply_to_live_db.sql` — idempotent additive DDL (SEASON/EPISODE/CHARACTER target + staging tables);
  auto-applied by the crawler at steps 108 and 110 so a long-lived DB stays in sync with new tables

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

For a full rerun, start from `101`.

Do not start from `104` unless the code is explicitly changed to initialize the dump source when `101` is skipped.

Steps `112` and `113` are fully downstream of V2 and can be re-run on their own with `--start-step 112` — no need to redo the dump ETL or the bulk load. See "Media resolution (steps 112 & 113)" below.

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

### 2. Change `MARIADB_IMPORT_BATCH_ID` in `.env`

Always use a new batch id for a new full run.

Example:

```env
MARIADB_IMPORT_BATCH_ID=wikidata_full_20260509_1730
```

Recommended format:

```text
wikidata_full_YYYYMMDD_HHMM
```

This lets you distinguish one full run from another in staging and in validation.

### 3. Clear the database before the rerun

Before a new full rerun, clear both:

- staging tables
- target tables

This is especially important now that statement and qualifier IDs are generated deterministically and older data loaded with previous ID logic should not be mixed with the new run.

Use:

```text
04_reset_for_full_rerun.sql
```

This file clears:

- all `STG_T_WC_WIKIDATA_*` tables used by the pipeline
- all `T_WC_WIKIDATA_*` entity, statement, value, and qualifier target tables populated by the pipeline

Run it in MariaDB before starting a fresh full run.

The reset script uses ordered `DELETE` statements with `FOREIGN_KEY_CHECKS` disabled, not `TRUNCATE`, because MariaDB/MySQL does not allow truncating a table that is referenced by a foreign key constraint.

Example:

```sql
SOURCE 04_reset_for_full_rerun.sql;
```

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
- change `MARIADB_IMPORT_BATCH_ID`
- remove `/home/debian/docker/shared_data/wikidata-crawler/latest-all.json.bz2`

### Database

- connect to MariaDB
- run `SOURCE 04_reset_for_full_rerun.sql;`

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

### 3. Reset the database

In MariaDB:

```sql
SOURCE 04_reset_for_full_rerun.sql;
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
- **ETL aborted mid-pass** — the partial output in `/shared/pass1` (or `pass2`, `item_cache`) is no longer trustworthy. Re-run from the failing pass, not the bulk load.
- **Staging load failed** — fix the data issue, then resume with `--start-step 108`.

## Why a full reset is recommended

Several operational issues were discovered during recent reruns:

- staging reloads can duplicate rows if rerun carelessly
- step `104` depends on dump-source initialization from step `101`
- statement IDs and qualifier IDs must remain stable across reruns
- if an older run populated target statement/value tables with different ID logic, the safest correction is a clean reload

Because of that, the most reliable rerun procedure is:

- clear cached dump file
- use a fresh batch id
- reset staging and target tables
- rerun from `101`

## SQL files and their roles

- `01_create_schema.sql`
  - creates final target tables
- `02_staging_and_triggers.sql`
  - creates staging tables and validation triggers
- `03_bulk_load_from_staging_FULL.sql`
  - loads staging rows into target tables
- `04_reset_for_full_rerun.sql`
  - clears staging and target tables (including the media-resolution layer) before a fresh full rebuild
- `05_progress_checks.sql`
  - reports staging and target progress counts for a given `IMPORT_BATCH_ID`
- `07_resolve_media_resources.sql`
  - executed by step `112`; populates `T_WC_WIKIDATA_MEDIA_RESOURCE` and `T_WC_WIKIDATA_MEDIA_RESOURCE_URL` from V2 statement/value tables using `INSERT ... ON DUPLICATE KEY UPDATE` (idempotent)

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
- change `MARIADB_IMPORT_BATCH_ID` in `.env`
- run `04_reset_for_full_rerun.sql`
- rebuild Docker image
- run with `docker run -d`
- watch with `docker logs -f wikidata-crawler`
- start from `--start-step 101`

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
