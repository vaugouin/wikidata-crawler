from __future__ import annotations

import argparse
import os
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional

import httpx
import pymysql.cursors
from dotenv import load_dotenv
from pymysql.constants import CLIENT

load_dotenv()


def _bridge_env(primary: str, fallback: str) -> None:
    if os.environ.get(primary):
        os.environ.setdefault(fallback, os.environ[primary])


_bridge_env("MARIADB_HOST", "DB_HOST")
_bridge_env("MARIADB_PORT", "DB_PORT")
_bridge_env("MARIADB_USER", "DB_USER")
_bridge_env("MARIADB_PASSWORD", "DB_PASSWORD")
_bridge_env("MARIADB_DATABASE", "DB_NAME")
_bridge_env("MARIADB_TABLE_PREFIX", "DB_NAMESPACE")

import citizenphil as cp
from load_staging_jsonl import TABLE_SPECS, create_connection as create_staging_connection, load_table
from wikidata_dump_etl import WikidataDumpETL


PASS1_DIR = Path("/shared/pass1")
PASS2_DIR = Path("/shared/pass2")
ITEM_CACHE_DIR = Path("/shared/item_cache")
SHARED_DIR = Path("/shared")
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_BULK_SQL_NAME = "03_bulk_load_from_staging_FULL.sql"
CRAWLER_PREFIX = "strwikidatacrawler"


@dataclass(frozen=True)
class ProcessStep:
    code: int
    label: str
    handler: Callable[["WikidataCrawler"], None]


class ValidationError(RuntimeError):
    pass


class WikidataCrawler:
    def __init__(self, start_step: int = 101) -> None:
        self.processes_executed_desc = "List of processes executed in the Wikidata dump crawler"
        self.total_runtime_desc = "Total runtime of the Wikidata dump crawler"
        self.import_batch_id = self._require_env("MARIADB_IMPORT_BATCH_ID")
        self.start_step = start_step
        self.dump_url = os.environ.get("DUMP_URL", "").strip() or None
        dump_file_value = os.environ.get("DUMP_FILE", "").strip()
        self.dump_file = Path(dump_file_value) if dump_file_value else None
        self.resolved_dump_url: Optional[str] = None
        self.resolved_dump_file: Optional[Path] = None
        self.processes_executed = ""
        self.arrwikidatascope = OrderedDict([
            (101, "resolve dump source"),
            (102, "run ETL pass1"),
            (103, "validate ETL pass1"),
            (104, "run ETL pass2"),
            (105, "validate ETL pass2"),
            (106, "run ETL item_cache"),
            (107, "validate ETL item_cache"),
            (108, "load staging tables"),
            (109, "validate staging data"),
            (110, "bulk load target tables"),
            (111, "validate target tables"),
        ])
        self.steps = OrderedDict([
            (101, ProcessStep(101, self.arrwikidatascope[101], WikidataCrawler.step_resolve_dump_source)),
            (102, ProcessStep(102, self.arrwikidatascope[102], WikidataCrawler.step_run_pass1)),
            (103, ProcessStep(103, self.arrwikidatascope[103], WikidataCrawler.step_validate_pass1)),
            (104, ProcessStep(104, self.arrwikidatascope[104], WikidataCrawler.step_run_pass2)),
            (105, ProcessStep(105, self.arrwikidatascope[105], WikidataCrawler.step_validate_pass2)),
            (106, ProcessStep(106, self.arrwikidatascope[106], WikidataCrawler.step_run_item_cache)),
            (107, ProcessStep(107, self.arrwikidatascope[107], WikidataCrawler.step_validate_item_cache)),
            (108, ProcessStep(108, self.arrwikidatascope[108], WikidataCrawler.step_load_staging)),
            (109, ProcessStep(109, self.arrwikidatascope[109], WikidataCrawler.step_validate_staging)),
            (110, ProcessStep(110, self.arrwikidatascope[110], WikidataCrawler.step_bulk_load)),
            (111, ProcessStep(111, self.arrwikidatascope[111], WikidataCrawler.step_validate_targets)),
        ])

    def run(self) -> None:
        start_time = time.time()
        strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        self._validate_start_step()
        self._set_previous_and_running_runtime()
        cp.f_setservervariable(f"{CRAWLER_PREFIX}startdatetime", strnow, "Date and time of the last start of the Wikidata dump crawler", 0)
        previous_processes = cp.f_getservervariable(f"{CRAWLER_PREFIX}processesexecuted", 0)
        cp.f_setservervariable(f"{CRAWLER_PREFIX}processesexecutedprevious", previous_processes, self.processes_executed_desc + " (previous execution)", 0)
        cp.f_setservervariable(f"{CRAWLER_PREFIX}processesexecuted", self.processes_executed, self.processes_executed_desc, 0)
        cp.f_setservervariable(f"{CRAWLER_PREFIX}startstep", str(self.start_step), "First workflow step executed by the Wikidata dump crawler", 0)

        try:
            for code, step in self.steps.items():
                if code < self.start_step:
                    continue
                self._run_step(code, step)
        except Exception as exc:
            self._mark_failure(exc, start_time)
            raise
        else:
            duration = cp.convert_seconds_to_duration(int(time.time() - start_time))
            cp.f_setservervariable(f"{CRAWLER_PREFIX}totalruntime", duration, self.total_runtime_desc, 0)
            cp.f_setservervariable(f"{CRAWLER_PREFIX}status", "SUCCESS", "Overall status of the Wikidata dump crawler", 0)
            cp.f_setservervariable(f"{CRAWLER_PREFIX}enddatetime", datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S"), "Date and time of the last successful end of the Wikidata dump crawler", 0)

    def _run_step(self, code: int, step: ProcessStep) -> None:
        current_process = f"{code}: {step.label}"
        self.processes_executed += f"{code}, "
        cp.f_setservervariable(f"{CRAWLER_PREFIX}processesexecuted", self.processes_executed, self.processes_executed_desc, 0)
        cp.f_setservervariable(f"{CRAWLER_PREFIX}currentprocess", current_process, "Current process in the Wikidata dump crawler", 0)
        cp.f_setservervariable(f"{CRAWLER_PREFIX}status", "RUNNING", "Overall status of the Wikidata dump crawler", 0)
        cp.f_setservervariable(f"{CRAWLER_PREFIX}step{code}status", "RUNNING", f"Status of step {code}: {step.label}", 0)
        cp.f_setservervariable(f"{CRAWLER_PREFIX}step{code}startedat", datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S"), f"Start time of step {code}: {step.label}", 0)
        print(current_process)
        step.handler(self)
        cp.f_setservervariable(f"{CRAWLER_PREFIX}step{code}status", "SUCCESS", f"Status of step {code}: {step.label}", 0)
        cp.f_setservervariable(f"{CRAWLER_PREFIX}step{code}finishedat", datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S"), f"End time of step {code}: {step.label}", 0)

    def _mark_failure(self, exc: Exception, start_time: float) -> None:
        duration = cp.convert_seconds_to_duration(int(time.time() - start_time))
        cp.f_setservervariable(f"{CRAWLER_PREFIX}totalruntime", duration, self.total_runtime_desc, 0)
        cp.f_setservervariable(f"{CRAWLER_PREFIX}status", "FAILURE", "Overall status of the Wikidata dump crawler", 0)
        cp.f_setservervariable(f"{CRAWLER_PREFIX}lasterror", str(exc), "Last error raised by the Wikidata dump crawler", 0)
        cp.f_setservervariable(f"{CRAWLER_PREFIX}enddatetime", datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S"), "Date and time of the last failed end of the Wikidata dump crawler", 0)

    def _set_previous_and_running_runtime(self) -> None:
        previous = cp.f_getservervariable(f"{CRAWLER_PREFIX}totalruntime", 0)
        cp.f_setservervariable(f"{CRAWLER_PREFIX}totalruntimeprevious", previous, self.total_runtime_desc + " (previous execution)", 0)
        cp.f_setservervariable(f"{CRAWLER_PREFIX}totalruntime", "RUNNING", self.total_runtime_desc, 0)

    def _validate_start_step(self) -> None:
        if self.start_step not in self.steps:
            valid_steps = ", ".join(str(code) for code in self.steps)
            raise ValidationError(f"Invalid start step {self.start_step}. Valid values: {valid_steps}")

    def _require_env(self, name: str) -> str:
        value = os.environ.get(name, "").strip()
        if not value:
            raise RuntimeError(f"Missing required environment variable: {name}")
        return value

    def step_resolve_dump_source(self) -> None:
        if self.dump_file and self.dump_url:
            if self.dump_file.exists():
                self.resolved_dump_file = self.dump_file
                self.resolved_dump_url = None
            else:
                self.dump_file.parent.mkdir(parents=True, exist_ok=True)
                with httpx.stream("GET", self.dump_url, timeout=120.0, follow_redirects=True) as response:
                    response.raise_for_status()
                    with self.dump_file.open("wb") as fh:
                        for chunk in response.iter_bytes(chunk_size=8 * 1024 * 1024):
                            fh.write(chunk)
                self.resolved_dump_file = self.dump_file
                self.resolved_dump_url = None
        elif self.dump_file:
            if not self.dump_file.exists():
                raise ValidationError(f"DUMP_FILE does not exist: {self.dump_file}")
            self.resolved_dump_file = self.dump_file
            self.resolved_dump_url = None
        elif self.dump_url:
            self.resolved_dump_url = self.dump_url
            self.resolved_dump_file = None
        else:
            raise ValidationError("Set DUMP_URL and/or DUMP_FILE in the environment.")
        cp.f_setservervariable(f"{CRAWLER_PREFIX}resolveddumpsource", str(self.resolved_dump_file or self.resolved_dump_url), "Resolved dump source used by the Wikidata dump crawler", 0)

    def step_run_pass1(self) -> None:
        self._run_pass(
            pass_name="pass1",
            out_dir=PASS1_DIR,
            class_roots_json=None,
            core_entity_ids_path=None,
            referenced_item_ids_path=None,
            candidate_person_ids_path=None,
            referenced_person_ids_path=None,
        )

    def step_validate_pass1(self) -> None:
        summary = self._validate_summary(PASS1_DIR, "pass1")
        self._require_file(PASS1_DIR / "class_roots.jsonl")
        self._require_file(PASS1_DIR / "core_entity_ids.txt")
        self._require_file(PASS1_DIR / "candidate_person_ids.txt")
        if summary["parse_errors"] != 0:
            raise ValidationError("pass1 has parse errors")
        if summary["movies_detected"] <= 0 and summary["series_detected"] <= 0 and summary["persons_detected"] <= 0:
            raise ValidationError("pass1 detected no core entities")
        cp.f_setservervariable(f"{CRAWLER_PREFIX}pass1entities", str(summary["entities_seen"]), "Entities seen during ETL pass1", 0)

    def step_run_pass2(self) -> None:
        self._run_pass(
            pass_name="pass2",
            out_dir=PASS2_DIR,
            class_roots_json=PASS1_DIR / "class_roots.jsonl",
            core_entity_ids_path=PASS1_DIR / "core_entity_ids.txt",
            referenced_item_ids_path=None,
            candidate_person_ids_path=PASS1_DIR / "candidate_person_ids.txt",
            referenced_person_ids_path=None,
        )

    def step_validate_pass2(self) -> None:
        summary = self._validate_summary(PASS2_DIR, "pass2")
        self._require_file(PASS2_DIR / "referenced_item_ids.txt")
        self._require_file(PASS2_DIR / "referenced_person_ids.txt")
        self._require_file(PASS2_DIR / "T_WC_WIKIDATA_STATEMENT.jsonl")
        if summary["parse_errors"] != 0:
            raise ValidationError("pass2 has parse errors")
        if summary["statements_emitted"] <= 0:
            raise ValidationError("pass2 emitted no statements")
        cp.f_setservervariable(f"{CRAWLER_PREFIX}pass2statements", str(summary["statements_emitted"]), "Statements emitted during ETL pass2", 0)

    def step_run_item_cache(self) -> None:
        self._run_pass(
            pass_name="item_cache",
            out_dir=ITEM_CACHE_DIR,
            class_roots_json=None,
            core_entity_ids_path=PASS1_DIR / "core_entity_ids.txt",
            referenced_item_ids_path=PASS2_DIR / "referenced_item_ids.txt",
            candidate_person_ids_path=None,
            referenced_person_ids_path=PASS2_DIR / "referenced_person_ids.txt",
        )

    def step_validate_item_cache(self) -> None:
        summary = self._validate_summary(ITEM_CACHE_DIR, "item_cache")
        self._require_file(ITEM_CACHE_DIR / "run_summary.json")
        if summary["parse_errors"] != 0:
            raise ValidationError("item_cache has parse errors")
        if not (ITEM_CACHE_DIR / "T_WC_WIKIDATA_ITEM.jsonl").exists() and not (ITEM_CACHE_DIR / "T_WC_WIKIDATA_PERSON.jsonl").exists():
            raise ValidationError("item_cache produced neither item nor person outputs")
        cp.f_setservervariable(f"{CRAWLER_PREFIX}itemcacheentities", str(summary["entities_seen"]), "Entities seen during ETL item_cache", 0)

    def step_load_staging(self) -> None:
        total_rows = 0
        connection = create_staging_connection()
        try:
            for spec in TABLE_SPECS:
                total_rows += load_table(
                    connection=connection,
                    spec=spec,
                    shared_dir=SHARED_DIR,
                    import_batch_id=self.import_batch_id,
                    skip_missing=True,
                )
        finally:
            connection.close()
        if total_rows <= 0:
            raise ValidationError("Staging loader inserted no rows")
        cp.f_setservervariable(f"{CRAWLER_PREFIX}stagingrowsloaded", str(total_rows), "Total rows loaded into staging tables", 0)

    def step_validate_staging(self) -> None:
        expected_tables = [
            "STG_T_WC_WIKIDATA_PROPERTY_METADATA",
            "STG_T_WC_WIKIDATA_MOVIE",
            "STG_T_WC_WIKIDATA_SERIE",
            "STG_T_WC_WIKIDATA_PERSON",
            "STG_T_WC_WIKIDATA_ITEM",
            "STG_T_WC_WIKIDATA_STATEMENT",
            "STG_T_WC_WIKIDATA_ITEM_VALUE",
            "STG_T_WC_WIKIDATA_STRING_VALUE",
            "STG_T_WC_WIKIDATA_EXTERNAL_ID_VALUE",
            "STG_T_WC_WIKIDATA_MEDIA_VALUE",
            "STG_T_WC_WIKIDATA_TIME_VALUE",
            "STG_T_WC_WIKIDATA_QUANTITY_VALUE",
        ]
        counts = self._count_rows_by_batch(expected_tables)
        if counts["STG_T_WC_WIKIDATA_STATEMENT"] <= 0:
            raise ValidationError("No staged statements found for the import batch id")
        null_count = self._fetch_scalar(
            "SELECT COUNT(*) FROM STG_T_WC_WIKIDATA_STATEMENT WHERE IMPORT_BATCH_ID IS NULL"
        )
        if int(null_count) > 0:
            raise ValidationError("Some staged statements still have NULL IMPORT_BATCH_ID")
        cp.f_setservervariable(f"{CRAWLER_PREFIX}stagingstatementrows", str(counts["STG_T_WC_WIKIDATA_STATEMENT"]), "Statement rows available in staging for the import batch id", 0)

    def step_bulk_load(self) -> None:
        sql_path = self._resolve_bulk_sql_path()
        sql_text = sql_path.read_text(encoding="utf-8")
        sql_text = sql_text.replace("SET NAMES utf8mb4;", "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;")
        sql_text = sql_text.replace(
            "SET @IMPORT_BATCH_ID = 'BATCH_20260309_001';",
            f"SET @IMPORT_BATCH_ID = CONVERT('{self.import_batch_id}' USING utf8mb4) COLLATE utf8mb4_unicode_ci;",
        )
        sql_text = re.sub(
            r"(?<!COLLATE )IMPORT_BATCH_ID\s*=\s*@IMPORT_BATCH_ID",
            "IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci = @IMPORT_BATCH_ID COLLATE utf8mb4_unicode_ci",
            sql_text,
        )
        statements = self._split_sql_statements(sql_text)
        connection = self._create_multi_statement_connection()
        try:
            with connection.cursor() as cursor:
                for index, statement in enumerate(statements, start=1):
                    normalized = statement.strip().upper()
                    if normalized in ("START TRANSACTION", "COMMIT"):
                        continue
                    cursor.execute(statement)
                    connection.commit()
                    cp.f_setservervariable(
                        f"{CRAWLER_PREFIX}bulkloadlaststatement",
                        str(index),
                        "Last successfully committed statement index in the bulk-load process",
                        0,
                    )
        finally:
            connection.close()
        cp.f_setservervariable(f"{CRAWLER_PREFIX}bulkloadbatchid", self.import_batch_id, "Last import batch id used for bulk load into target tables", 0)

    def step_validate_targets(self) -> None:
        final_tables = [
            "T_WC_WIKIDATA_PROPERTY_METADATA",
            "T_WC_WIKIDATA_MOVIE",
            "T_WC_WIKIDATA_SERIE",
            "T_WC_WIKIDATA_PERSON",
            "T_WC_WIKIDATA_ITEM",
            "T_WC_WIKIDATA_STATEMENT",
            "T_WC_WIKIDATA_ITEM_VALUE",
            "T_WC_WIKIDATA_STRING_VALUE",
            "T_WC_WIKIDATA_EXTERNAL_ID_VALUE",
            "T_WC_WIKIDATA_MEDIA_VALUE",
            "T_WC_WIKIDATA_TIME_VALUE",
            "T_WC_WIKIDATA_QUANTITY_VALUE",
        ]
        counts = self._count_rows(final_tables)
        if counts["T_WC_WIKIDATA_STATEMENT"] <= 0:
            raise ValidationError("Target statement table is empty after bulk load")
        loaded_status_count = self._fetch_scalar(
            "SELECT COUNT(*) FROM STG_T_WC_WIKIDATA_STATEMENT WHERE IMPORT_BATCH_ID = %s AND ROW_STATUS = 'LOADED'",
            (self.import_batch_id,),
        )
        if int(loaded_status_count) <= 0:
            raise ValidationError("No staging statements were marked as LOADED for the import batch id")
        cp.f_setservervariable(f"{CRAWLER_PREFIX}targetstatementrows", str(counts["T_WC_WIKIDATA_STATEMENT"]), "Current row count in T_WC_WIKIDATA_STATEMENT", 0)

    def _run_pass(
        self,
        *,
        pass_name: str,
        out_dir: Path,
        class_roots_json: Optional[Path],
        core_entity_ids_path: Optional[Path],
        referenced_item_ids_path: Optional[Path],
        candidate_person_ids_path: Optional[Path],
        referenced_person_ids_path: Optional[Path],
    ) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        etl = WikidataDumpETL(
            out_dir=out_dir,
            pass_name=pass_name,
            dump_url=self.resolved_dump_url,
            dump_file=self.resolved_dump_file,
            class_roots_json=class_roots_json,
            core_entity_ids_path=core_entity_ids_path,
            referenced_item_ids_path=referenced_item_ids_path,
            candidate_person_ids_path=candidate_person_ids_path,
            referenced_person_ids_path=referenced_person_ids_path,
        )
        etl.run()

    def _validate_summary(self, out_dir: Path, expected_pass_name: str) -> Dict[str, int]:
        summary_path = out_dir / "run_summary.json"
        self._require_file(summary_path)
        import json
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if summary.get("pass_name") != expected_pass_name:
            raise ValidationError(f"Unexpected pass_name in {summary_path}: {summary.get('pass_name')}")
        if summary.get("entities_seen", 0) <= 0:
            raise ValidationError(f"No entities seen in {summary_path}")
        return summary

    def _require_file(self, path: Path) -> None:
        if not path.exists() or path.stat().st_size == 0:
            raise ValidationError(f"Required file missing or empty: {path}")

    def _count_rows_by_batch(self, tables: List[str]) -> Dict[str, int]:
        connection = cp.f_getconnection()
        try:
            with connection.cursor() as cursor:
                results: Dict[str, int] = {}
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) AS CNT FROM {table} WHERE IMPORT_BATCH_ID = %s", (self.import_batch_id,))
                    row = cursor.fetchone()
                    results[table] = int(row["CNT"])
                return results
        finally:
            connection.close()

    def _count_rows(self, tables: List[str]) -> Dict[str, int]:
        connection = cp.f_getconnection()
        try:
            with connection.cursor() as cursor:
                results: Dict[str, int] = {}
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) AS CNT FROM {table}")
                    row = cursor.fetchone()
                    results[table] = int(row["CNT"])
                return results
        finally:
            connection.close()

    def _fetch_scalar(self, sql: str, params: tuple = ()) -> int:
        connection = cp.f_getconnection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()
                if not row:
                    return 0
                return int(next(iter(row.values())))
        finally:
            connection.close()

    def _resolve_bulk_sql_path(self) -> Path:
        candidates = [
            BASE_DIR / DEFAULT_BULK_SQL_NAME,
            Path.cwd() / DEFAULT_BULK_SQL_NAME,
            SHARED_DIR / DEFAULT_BULK_SQL_NAME,
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        searched = ", ".join(str(path) for path in candidates)
        raise ValidationError(f"Bulk-load SQL file not found. Searched: {searched}")

    def _split_sql_statements(self, sql_text: str) -> List[str]:
        statements: List[str] = []
        current_lines: List[str] = []
        for line in sql_text.splitlines():
            stripped = line.strip()
            if stripped.startswith("--"):
                continue
            current_lines.append(line)
        cleaned_sql = "\n".join(current_lines)
        for part in cleaned_sql.split(";"):
            statement = part.strip()
            if statement:
                statements.append(statement)
        return statements

    def _create_multi_statement_connection(self) -> pymysql.connections.Connection:
        return pymysql.connect(
            host=self._require_env("MARIADB_HOST"),
            port=int(os.environ.get("MARIADB_PORT", "3306")),
            user=self._require_env("MARIADB_USER"),
            password=self._require_env("MARIADB_PASSWORD"),
            database=self._require_env("MARIADB_DATABASE"),
            charset="utf8mb4",
            collation="utf8mb4_unicode_ci",
            autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
            client_flag=CLIENT.MULTI_STATEMENTS,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full Wikidata dump ETL, staging load, and bulk-load workflow.")
    parser.add_argument(
        "--start-step",
        type=int,
        default=101,
        help="Workflow step code to start from. Example: 109 to start after staging load, 110 to run only bulk load + final validation.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    crawler = WikidataCrawler(start_step=args.start_step)
    crawler.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
