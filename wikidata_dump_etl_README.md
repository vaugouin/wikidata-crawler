# wikidata_dump_etl.py

## Overview

`wikidata_dump_etl.py` is a production-oriented Python ETL script for streaming the official Wikidata JSON dump and generating staging files for your MariaDB Wikidata schema.

It implements:

- `P31/P279*` classification for:
  - movies
  - series
  - persons
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

The script intentionally does **not** resolve Commons / Internet Archive / YouTube URLs. It only emits raw claims that can later feed your media-resolution workflow.

## Dependencies

```bash
pip install pysimdjson httpx
```

## Output format

The script generates **NDJSON staging files**.

Each output file name matches the target table name, for example:

- `T_WC_WIKIDATA_PROPERTY_METADATA.jsonl`
- `T_WC_WIKIDATA_STATEMENT.jsonl`
- `T_WC_WIKIDATA_ITEM_VALUE.jsonl`

Additional helper files are also produced:

- `subclass_edges.jsonl`
- `class_roots.jsonl`
- `core_entity_ids.txt`
- `referenced_item_ids.txt`
- `run_summary.json`

## Classification roots

### Movies
- `Q11424` film
- `Q506240` television film

### Series
- `Q5398426` television series
- `Q1259759` miniseries
- `Q526877` web series

### Persons
- `Q5` human

Excluded from series root usage:
- `Q15416` television program

## Passes

### Pass 1
Builds:
- property metadata staging
- subclass edges
- root descendant support data
- `core_entity_ids.txt`

Example:

```bash
python wikidata_dump_etl.py   --dump-url https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2   --out-dir /data/wikidata/staging/pass1   --pass-name pass1
```

### Pass 2
Builds:
- movie / serie / person staging
- statement staging
- typed value staging
- `referenced_item_ids.txt`

Example:

```bash
python wikidata_dump_etl.py   --dump-url https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2   --out-dir /data/wikidata/staging/pass2   --pass-name pass2   --class-roots-json /data/wikidata/staging/pass1/class_roots.jsonl   --core-entity-ids /data/wikidata/staging/pass1/core_entity_ids.txt
```

### Item-cache replay pass
Builds:
- `T_WC_WIKIDATA_ITEM.jsonl`

using only the distinct `ID_ITEM` values referenced in pass 2.

Example:

```bash
python wikidata_dump_etl.py   --dump-url https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2   --out-dir /data/wikidata/staging/item_cache   --pass-name item_cache   --referenced-item-ids /data/wikidata/staging/pass2/referenced_item_ids.txt   --core-entity-ids /data/wikidata/staging/pass1/core_entity_ids.txt
```

## Notes

- The script is designed for **limited disk space** and streams the `.bz2` dump.
- It avoids storing the decompressed dump.
- It uses a single reused `simdjson.Parser()` for speed.
- It is best used with a downstream MariaDB bulk-load process.
- It does not create SQL and does not load MariaDB directly.
