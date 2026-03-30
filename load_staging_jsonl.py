from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import pymysql
from dotenv import load_dotenv


BATCH_SIZE = 100
DEFAULT_SHARED_DIR = Path(os.environ.get("SHARED_DIR", "/shared"))


@dataclass(frozen=True)
class TableSpec:
    stg_file_name: str
    stg_file_location: str
    table_name: str
    additional_columns: Dict[str, Any]


TABLE_SPECS: List[TableSpec] = [
    TableSpec(
        stg_file_name="T_WC_WIKIDATA_PROPERTY_METADATA.jsonl",
        stg_file_location="pass1",
        table_name="STG_T_WC_WIKIDATA_PROPERTY_METADATA",
        additional_columns={"IMPORT_BATCH_ID": None, "SOURCE_FILE": None, "ROW_STATUS": "NEW"},
    ),
    TableSpec(
        stg_file_name="T_WC_WIKIDATA_MOVIE.jsonl",
        stg_file_location="pass2",
        table_name="STG_T_WC_WIKIDATA_MOVIE",
        additional_columns={"IMPORT_BATCH_ID": None, "SOURCE_FILE": None, "ROW_STATUS": "NEW"},
    ),
    TableSpec(
        stg_file_name="T_WC_WIKIDATA_SERIE.jsonl",
        stg_file_location="pass2",
        table_name="STG_T_WC_WIKIDATA_SERIE",
        additional_columns={"IMPORT_BATCH_ID": None, "SOURCE_FILE": None, "ROW_STATUS": "NEW"},
    ),
    TableSpec(
        stg_file_name="T_WC_WIKIDATA_PERSON.jsonl",
        stg_file_location="pass2",
        table_name="STG_T_WC_WIKIDATA_PERSON",
        additional_columns={"IMPORT_BATCH_ID": None, "SOURCE_FILE": None, "ROW_STATUS": "NEW"},
    ),
    TableSpec(
        stg_file_name="T_WC_WIKIDATA_PERSON.jsonl",
        stg_file_location="item_cache",
        table_name="STG_T_WC_WIKIDATA_PERSON",
        additional_columns={"IMPORT_BATCH_ID": None, "SOURCE_FILE": None, "ROW_STATUS": "NEW"},
    ),
    TableSpec(
        stg_file_name="T_WC_WIKIDATA_ITEM.jsonl",
        stg_file_location="item_cache",
        table_name="STG_T_WC_WIKIDATA_ITEM",
        additional_columns={"IMPORT_BATCH_ID": None, "SOURCE_FILE": None, "ROW_STATUS": "NEW"},
    ),
    TableSpec(
        stg_file_name="T_WC_WIKIDATA_STATEMENT.jsonl",
        stg_file_location="pass2",
        table_name="STG_T_WC_WIKIDATA_STATEMENT",
        additional_columns={"IMPORT_BATCH_ID": None, "SOURCE_FILE": None, "ROW_STATUS": "NEW"},
    ),
    TableSpec(
        stg_file_name="T_WC_WIKIDATA_ITEM_VALUE.jsonl",
        stg_file_location="pass2",
        table_name="STG_T_WC_WIKIDATA_ITEM_VALUE",
        additional_columns={"IMPORT_BATCH_ID": None, "ROW_STATUS": "NEW"},
    ),
    TableSpec(
        stg_file_name="T_WC_WIKIDATA_STRING_VALUE.jsonl",
        stg_file_location="pass2",
        table_name="STG_T_WC_WIKIDATA_STRING_VALUE",
        additional_columns={"IMPORT_BATCH_ID": None, "ROW_STATUS": "NEW"},
    ),
    TableSpec(
        stg_file_name="T_WC_WIKIDATA_EXTERNAL_ID_VALUE.jsonl",
        stg_file_location="pass2",
        table_name="STG_T_WC_WIKIDATA_EXTERNAL_ID_VALUE",
        additional_columns={"IMPORT_BATCH_ID": None, "ROW_STATUS": "NEW"},
    ),
    TableSpec(
        stg_file_name="T_WC_WIKIDATA_MEDIA_VALUE.jsonl",
        stg_file_location="pass2",
        table_name="STG_T_WC_WIKIDATA_MEDIA_VALUE",
        additional_columns={"IMPORT_BATCH_ID": None, "ROW_STATUS": "NEW"},
    ),
    TableSpec(
        stg_file_name="T_WC_WIKIDATA_TIME_VALUE.jsonl",
        stg_file_location="pass2",
        table_name="STG_T_WC_WIKIDATA_TIME_VALUE",
        additional_columns={"IMPORT_BATCH_ID": None, "ROW_STATUS": "NEW"},
    ),
    TableSpec(
        stg_file_name="T_WC_WIKIDATA_QUANTITY_VALUE.jsonl",
        stg_file_location="pass2",
        table_name="STG_T_WC_WIKIDATA_QUANTITY_VALUE",
        additional_columns={"IMPORT_BATCH_ID": None, "ROW_STATUS": "NEW"},
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--shared-dir",
        default=str(DEFAULT_SHARED_DIR),
        help="Base directory containing pass1, pass2, and item_cache folders.",
    )
    parser.add_argument(
        "--only-table",
        action="append",
        default=[],
        help="Load only the specified staging table name. Can be passed multiple times.",
    )
    parser.add_argument(
        "--skip-missing",
        action="store_true",
        help="Skip missing JSONL files instead of failing.",
    )
    return parser.parse_args()


def get_required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def create_connection() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=get_required_env("MARIADB_HOST"),
        port=int(os.environ.get("MARIADB_PORT", "3306")),
        user=get_required_env("MARIADB_USER"),
        password=get_required_env("MARIADB_PASSWORD"),
        database=get_required_env("MARIADB_DATABASE"),
        charset="utf8mb4",
        autocommit=False,
    )


def iter_jsonl_rows(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        for line_number, line in enumerate(fh, start=1):
            text = line.strip()
            if not text:
                continue
            row = json.loads(text)
            if not isinstance(row, dict):
                raise ValueError(f"Expected JSON object in {path} line {line_number}")
            yield row


def normalize_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def build_additional_columns(spec: TableSpec, file_path: Path, import_batch_id: str) -> Dict[str, Any]:
    resolved: Dict[str, Any] = {}
    for key, value in spec.additional_columns.items():
        if key == "IMPORT_BATCH_ID":
            resolved[key] = import_batch_id
        elif key == "SOURCE_FILE":
            resolved[key] = str(file_path)
        else:
            resolved[key] = value
    return resolved


def build_insert_sql(table_name: str, columns: Sequence[str]) -> str:
    column_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    return f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"


def load_table(
    connection: pymysql.connections.Connection,
    spec: TableSpec,
    shared_dir: Path,
    import_batch_id: str,
    skip_missing: bool,
) -> int:
    file_path = shared_dir / spec.stg_file_location / spec.stg_file_name
    if not file_path.exists():
        if skip_missing:
            print(f"[SKIP] Missing file for {spec.table_name}: {file_path}")
            return 0
        raise FileNotFoundError(f"Missing file for {spec.table_name}: {file_path}")

    additional_columns = build_additional_columns(spec, file_path, import_batch_id)
    inserted_rows = 0
    batch_rows: List[List[Any]] = []
    insert_sql: str | None = None
    ordered_columns: List[str] | None = None

    with connection.cursor() as cursor:
        for row in iter_jsonl_rows(file_path):
            merged_row = {**additional_columns, **row}

            if ordered_columns is None:
                ordered_columns = list(merged_row.keys())
                insert_sql = build_insert_sql(spec.table_name, ordered_columns)

            assert ordered_columns is not None
            assert insert_sql is not None

            batch_rows.append([normalize_value(merged_row.get(column)) for column in ordered_columns])

            if len(batch_rows) == BATCH_SIZE:
                cursor.executemany(insert_sql, batch_rows)
                connection.commit()
                inserted_rows += len(batch_rows)
                print(f"[LOAD] {spec.table_name}: inserted {inserted_rows:,} rows from {file_path}")
                batch_rows.clear()

        if batch_rows:
            assert insert_sql is not None
            cursor.executemany(insert_sql, batch_rows)
            connection.commit()
            inserted_rows += len(batch_rows)

    print(f"[DONE] {spec.table_name}: inserted {inserted_rows:,} rows from {file_path}")
    return inserted_rows


def select_specs(only_tables: Sequence[str]) -> List[TableSpec]:
    if not only_tables:
        return TABLE_SPECS
    selected = set(only_tables)
    return [spec for spec in TABLE_SPECS if spec.table_name in selected]


def main() -> int:
    load_dotenv()
    args = parse_args()
    shared_dir = Path(args.shared_dir)
    import_batch_id = get_required_env("MARIADB_IMPORT_BATCH_ID")
    specs = select_specs(args.only_table)

    if not specs:
        raise RuntimeError("No matching table specs selected.")

    total_rows = 0
    connection = create_connection()
    try:
        for spec in specs:
            total_rows += load_table(
                connection=connection,
                spec=spec,
                shared_dir=shared_dir,
                import_batch_id=import_batch_id,
                skip_missing=args.skip_missing,
            )
    finally:
        connection.close()

    print(f"[SUMMARY] Loaded {total_rows:,} rows across {len(specs)} table specs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
