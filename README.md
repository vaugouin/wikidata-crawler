# Wikidata crawler

This repository contains the dump-based Wikidata ETL, the MariaDB schema and staging SQL, and the architectural documentation for the Wikidata statement/value model used by the project.

## Documentation map

Use these documents as the authoritative sources:

- `README.md`
  - top-level project overview and quick navigation
- `wikidata_dump_etl_README.md`
  - operational runbook for the ETL
  - Docker usage
  - pass1 / pass2 / item_cache workflow
  - staging loader usage
  - bulk-load sequence into MariaDB
- `WIKIDATA.md`
  - conceptual schema and data-model documentation
  - statement/value architecture
  - entity tables and media-resolution layer responsibilities

## Main files

### Python
- `wikidata_dump_etl.py`
- `load_staging_jsonl.py`

### SQL
- `01_create_schema.sql`
- `02_staging_and_triggers.sql`
- `03_bulk_load_from_staging_FULL.sql`

### Documentation
- `README.md`
- `wikidata_dump_etl_README.md`
- `WIKIDATA.md`

## Recommended workflow

1. Run `01_create_schema.sql`
2. Run `02_staging_and_triggers.sql`
3. Run the ETL to generate staging files
4. Load the generated `.jsonl` files into `STG_*` with `load_staging_jsonl.py`
5. Set `@IMPORT_BATCH_ID`
6. Run `03_bulk_load_from_staging_FULL.sql`

## Quick start

For the full ETL runbook, start with `wikidata_dump_etl_README.md`.

Typical sequence:

```bash
python load_staging_jsonl.py --shared-dir /shared --skip-missing
```

Then in MariaDB:

```sql
SET @IMPORT_BATCH_ID = 'your_batch_id';
SOURCE 03_bulk_load_from_staging_FULL.sql;
```

## Notes

- The ETL produces NDJSON staging files; loading those files into `STG_*` is a separate operational step.
- `03_bulk_load_from_staging_FULL.sql` is the fully expanded bulk-load script for the final merge from staging into the target tables.
- `T_WC_WIKIDATA_ITEM` is a selective referenced-item cache, not a mirror of all Wikidata items.
