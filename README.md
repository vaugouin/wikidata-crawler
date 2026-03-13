# Wikidata ETL bundle — fully expanded version

This archive contains the Python ETL script, SQL schema scripts, and the latest markdown design documents.

## Included files

### Python
- `wikidata_dump_etl.py`
- `wikidata_dump_etl_README.md`

### SQL
- `01_create_schema.sql`
- `02_staging_and_triggers.sql`
- `03_bulk_load_from_staging_FULL.sql`

### Markdown design documents
- `README.md`
- `PIPELINE.md`
- `WIKIDATA.md`

## What is different in this bundle

`03_bulk_load_from_staging_FULL.sql` is the fully expanded bulk-load script.
It includes the full field-by-field `INSERT ... SELECT` / `ON DUPLICATE KEY UPDATE` logic for:

- property metadata
- entity tables
- statement table
- all typed value tables
- `T_WC_WIKIDATA_MEDIA_RESOURCE`
- `T_WC_WIKIDATA_MEDIA_RESOURCE_URL`
- `T_WC_WIKIDATA_MEDIA_RESOURCE_CHECK`

This is the version to use if you want the complete SQL companion exactly aligned with the long version from our conversation.

## Recommended order

1. Run `01_create_schema.sql`
2. Run `02_staging_and_triggers.sql`
3. Generate staging files with `wikidata_dump_etl.py`
4. Load those files into the `STG_*` tables
5. Set your batch id:
   `SET @IMPORT_BATCH_ID = 'your_batch_id';`
6. Run `03_bulk_load_from_staging_FULL.sql`

## Notes

- `PIPELINE.md` summarizes the dump-based ETL architecture and `P31/P279*` classification strategy.
- `WIKIDATA.md` summarizes the broader data model, statements architecture, and media-resolution layer.
- The Python ETL script produces NDJSON staging files; importing NDJSON into `STG_*` is a separate operational step.
