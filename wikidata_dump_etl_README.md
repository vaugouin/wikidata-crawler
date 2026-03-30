# wikidata_dump_etl.py

## Overview

`wikidata_dump_etl.py` is a production-oriented Python ETL script for streaming the official Wikidata JSON dump and generating NDJSON staging files for a MariaDB Wikidata schema.

It implements:

- `P31/P279*` classification for movies, series, and persons
- property metadata extraction
- staging-file generation for:
  - `T_WC_WIKIDATA_PROPERTY_METADATA`
  - `T_WC_WIKIDATA_MOVIE`
  - `T_WC_WIKIDATA_SERIE`
  - `T_WC_WIKIDATA_PERSON`
  - `T_WC_WIKIDATA_ITEM`
  - `T_WC_WIKIDATA_STATEMENT`
  - `T_WC_WIKIDATA_ITEM_VALUE`
  - `T_WC_WIKIDATA_STRING_VALUE`
  - `T_WC_WIKIDATA_EXTERNAL_ID_VALUE`
  - `T_WC_WIKIDATA_MEDIA_VALUE`
  - `T_WC_WIKIDATA_TIME_VALUE`
  - `T_WC_WIKIDATA_QUANTITY_VALUE`

The script does **not** resolve Commons / Internet Archive / YouTube URLs. It only emits raw claims that feed your media-resolution workflow.

---

## Dependencies

```bash
pip install pysimdjson httpx
```

---

## Configuration — environment variables

The script reads **all parameters from environment variables**. There are no command-line arguments.

| Variable | Required | Description |
|---|---|---|
| `DUMP_URL` | see below | Remote `.bz2` Wikidata dump URL |
| `DUMP_FILE` | see below | Local `.bz2` file path inside the container |
| `PASS_NAME` | yes | ETL pass: `pass1`, `pass2`, or `item_cache` (set by `run_etl.sh`) |
| `OUT_DIR` | no | Output directory (default: `/shared`; set by `run_etl.sh`) |
| `CLASS_ROOTS_JSON` | pass2 | `class_roots.jsonl` produced by pass1 (set by `run_etl.sh`) |
| `CORE_ENTITY_IDS` | pass2, item_cache | `core_entity_ids.txt` produced by pass1 (set by `run_etl.sh`) |
| `CANDIDATE_PERSON_IDS` | pass2 | `candidate_person_ids.txt` produced by pass1 (set by `run_etl.sh`) |
| `REFERENCED_ITEM_IDS` | item_cache | `referenced_item_ids.txt` produced by pass2 (set by `run_etl.sh`) |
| `REFERENCED_PERSON_IDS` | item_cache | `referenced_person_ids.txt` produced by pass2 (set by `run_etl.sh`) |

### DUMP_URL vs DUMP_FILE

`DUMP_URL` and `DUMP_FILE` are **not related** — they are independent alternatives for supplying the dump. `DUMP_FILE` is never auto-created from `DUMP_URL` by the Python script itself.

In both cases the `.bz2` is read **chunk by chunk** using a streaming `BZ2Decompressor`. The full decompressed dump is never stored on disk or held entirely in memory.

| Combination in `.env` | Behaviour |
|---|---|
| `DUMP_URL` only | Dump is streamed from HTTP for each of the 3 passes. No local file is written. |
| `DUMP_FILE` only | Local `.bz2` is read for each pass. File must already exist on the shared volume. |
| `DUMP_URL` + `DUMP_FILE` | `run_etl.sh` downloads from `DUMP_URL` to `DUMP_FILE` if the file does not yet exist, then all 3 passes read from the local file. If the file already exists, `DUMP_URL` is ignored. |

**Recommended setup for minimum bandwidth usage:** set both `DUMP_URL` and `DUMP_FILE`. The first run downloads the compressed file once; subsequent runs reuse it.

Copy `.env.example` to `.env` and configure the combination that fits your setup.

---

## Running in Docker

The script is designed to run inside the Docker container defined by `Dockerfile`.
The host path `/home/debian/docker/shared_data` is mounted as `/shared` inside the container.

### Normal full run (all three passes in sequence)

The container's default command is `run_etl.sh`, which runs all three passes automatically:

```bash
./wikidata-crawler.sh
```

`run_etl.sh` manages the per-pass env vars internally. The only variable you need in `.env` is `DUMP_URL` (or `DUMP_FILE`). Intermediate files and final staging files are written to:

```
/shared/pass1/       → pass1 output
/shared/pass2/       → pass2 output
/shared/item_cache/  → item_cache output
```

Which map to the host at:

```
/home/debian/docker/shared_data/pass1/
/home/debian/docker/shared_data/pass2/
/home/debian/docker/shared_data/item_cache/
```

### Running a single pass manually

Override `DUMP_URL` or any env var with `-e`:

```bash
docker run --rm --network=host \
  --env-file .env \
  -e PASS_NAME=pass1 \
  -e OUT_DIR=/shared/pass1 \
  -v /home/debian/docker/shared_data:/shared \
  wikidata-crawler-python-app \
  python /app/wikidata_dump_etl.py
```

To resume from pass2 without re-running pass1 (pass1 output already in `/shared/pass1`):

```bash
docker run --rm --network=host \
  --env-file .env \
  -e PASS_NAME=pass2 \
  -e OUT_DIR=/shared/pass2 \
  -e CLASS_ROOTS_JSON=/shared/pass1/class_roots.jsonl \
  -e CORE_ENTITY_IDS=/shared/pass1/core_entity_ids.txt \
  -e CANDIDATE_PERSON_IDS=/shared/pass1/candidate_person_ids.txt \
  -v /home/debian/docker/shared_data:/shared \
  wikidata-crawler-python-app \
  python /app/wikidata_dump_etl.py
```

---

## Output format

Each output file is **NDJSON** (one JSON object per line), named after its target table:

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

Helper/intermediate files:

- `subclass_edges.jsonl`
- `class_roots.jsonl` (pass1 output, pass2 input)
- `core_entity_ids.txt` (pass1 output, pass2/item_cache input)
- `candidate_person_ids.txt` (pass1 output, pass2 input)
- `referenced_item_ids.txt` (pass2 output, item_cache input)
- `referenced_person_ids.txt` (pass2 output, item_cache input)
- `run_summary.json`

---

## Passes

### Pass 1

Streams the full dump once to:
- emit `T_WC_WIKIDATA_PROPERTY_METADATA`
- build the `P279` subclass graph
- classify movies / series / persons using `P31/P279*`
- produce `core_entity_ids.txt` (movies + series + persons with IMDb ID)
- produce `candidate_person_ids.txt` (all Q5 instances, used in pass2 to identify persons referenced by movies/series)
- produce `class_roots.jsonl` (subclass descendants of the root Q-ids)

### Pass 2

Streams the full dump a second time to:
- emit entity rows for in-scope movies, series, and persons
- emit statements and typed values for those entities
- produce `referenced_item_ids.txt` (distinct items referenced via item-valued claims)
- produce `referenced_person_ids.txt` (persons from `candidate_person_ids` that appear in movie/series statements — these go to `T_WC_WIKIDATA_PERSON`, not `T_WC_WIKIDATA_ITEM`)

Requires pass1 outputs: `class_roots.jsonl`, `core_entity_ids.txt`, `candidate_person_ids.txt`.

### Item-cache pass

Streams the full dump a third time to:
- emit `T_WC_WIKIDATA_ITEM` rows for items referenced from movie/series/person statements (but not themselves in scope)
- emit additional `T_WC_WIKIDATA_PERSON` rows for rule-2 persons (persons referenced in movie/series statements who lack an IMDb ID but were cast/crew)

Requires pass1 + pass2 outputs: `core_entity_ids.txt`, `referenced_item_ids.txt`, `referenced_person_ids.txt`.

---

## Classification roots

### Movies
- `Q11424` film
- `Q506240` television film

### Series
- `Q5398426` television series
- `Q1259759` miniseries
- `Q526877` web series

Excluded from series classification:
- `Q15416` television program (too broad)

### Persons
- `Q5` human

Person filtering rules:
1. **Core entities** (emitted in pass2): must have an IMDb ID (`P345`)
2. **Referenced persons** (emitted in item_cache): persons found as item-valued claims in any movie or series statement — included even without IMDb ID

---

## Loading staging files into MariaDB

After the ETL completes, load the NDJSON staging files into the `STG_*` staging tables, then run the bulk-load SQL.

### Recommended workflow

1. Run `01_create_schema.sql` (once, to create tables)
2. Run `02_staging_and_triggers.sql` (once, to create staging tables)
3. Run the ETL (`./wikidata-crawler.sh`) — produces NDJSON in `/shared/pass1`, `/shared/pass2`, `/shared/item_cache`
4. Load NDJSON files into `STG_*` tables (see below)
5. Run `03_bulk_load_from_staging_FULL.sql` to merge staging into final tables

### How to load NDJSON into staging tables

MariaDB does not natively import NDJSON. Use `load_staging_jsonl.py`, which reads the ETL `.jsonl` files and batch-inserts them into the matching `STG_*` tables with `PyMySQL`.

The loader reads connection settings from `.env`:

- `MARIADB_HOST`
- `MARIADB_PORT`
- `MARIADB_USER`
- `MARIADB_PASSWORD`
- `MARIADB_DATABASE`
- `MARIADB_IMPORT_BATCH_ID`

Run it after ETL completes:

```bash
python load_staging_jsonl.py --shared-dir /shared --skip-missing
```

What the script does:

- loads files from `/shared/pass1`, `/shared/pass2`, and `/shared/item_cache`
- maps each `T_WC_WIKIDATA_*.jsonl` file to its matching `STG_T_WC_WIKIDATA_*` table
- adds staging metadata columns such as `IMPORT_BATCH_ID`, `SOURCE_FILE`, and `ROW_STATUS` where applicable
- inserts rows in batches of `100`

Notes about identical filenames across pass folders:

- Some ETL files can appear in more than one folder because multiple passes stream the full dump and may emit the same table-shaped output name.
- This does not always mean the files are duplicates in purpose. For example, `T_WC_WIKIDATA_PERSON.jsonl` from `pass2` contains core persons, while `T_WC_WIKIDATA_PERSON.jsonl` from `item_cache` contains additional referenced persons that were not part of the core pass.
- The loader handles this explicitly through per-table specs. It can load the same target staging table from multiple source folders when that is intentional, such as `STG_T_WC_WIKIDATA_PERSON` from both `pass2` and `item_cache`.
- For `T_WC_WIKIDATA_PROPERTY_METADATA.jsonl`, the loader currently uses only the `pass1` copy and ignores copies from `pass2` and `item_cache`.
- `SOURCE_FILE` is populated where the staging table supports it so you can trace which folder a loaded row came from.

Examples:

```bash
python load_staging_jsonl.py --shared-dir /shared
python load_staging_jsonl.py --shared-dir /shared --skip-missing
python load_staging_jsonl.py --shared-dir /shared --only-table STG_T_WC_WIKIDATA_MOVIE
python load_staging_jsonl.py --shared-dir /shared --only-table STG_T_WC_WIKIDATA_STATEMENT --only-table STG_T_WC_WIKIDATA_ITEM_VALUE
```

If you run the loader inside Docker with the existing volume mapping, `/shared` corresponds to `/home/debian/docker/shared_data` on the host.

---

## Notes

- The script streams the `.bz2` dump without writing the decompressed content to disk.
- A single reused `simdjson.Parser()` is used throughout for performance.
- `T_WC_WIKIDATA_ITEM` is a referenced-item cache only — it does not mirror all Wikidata items.
- No FK is assumed from `ITEM_VALUE.ID_ITEM` to `T_WC_WIKIDATA_ITEM`.
