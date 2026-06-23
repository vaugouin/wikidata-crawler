#!/usr/bin/env python3
from __future__ import annotations

"""
wikidata_dump_etl.py

Production-oriented streaming ETL for Wikidata JSON dumps with:
- P31/P279* classification
- property metadata extraction
- staging-file generation for MariaDB imports

This script:
- streams the official Wikidata JSON dump (.bz2) from HTTP or a local file
- avoids storing the full decompressed dump on disk
- reuses a single simdjson parser
- generates NDJSON staging files aligned with the target MariaDB tables
- builds a local P279 subclass graph
- classifies in-scope entities: movies, series, persons
- emits raw claims into the 2-level statement/value architecture
- emits a referenced-item cache candidate list for later T_WC_WIKIDATA_ITEM refresh

Notes:
- T_WC_WIKIDATA_ITEM is populated as a referenced-item cache only
- No FK is assumed from ITEM_VALUE.ID_ITEM to T_WC_WIKIDATA_ITEM
- Statement/value tables store raw claims
- Media resolution is NOT performed here; only raw media-related claims are emitted
- The script uses a 2-pass design plus an optional 3rd replay pass for referenced items

Dependencies:
    pip install pysimdjson httpx
"""

import bz2
import hashlib
import json
import os
import re
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, Optional, Set, Tuple

import httpx
import simdjson

# Prefix for all server variable names written to MariaDB SERVER_VARIABLE table.
# Mirrors the pattern used by citizenphil.f_setservervariable in sparql-crawler.py.
_SV_PREFIX = "strwikidatacrawler"

MOVIE_ROOTS = {"Q11424", "Q506240"}  # film, television film
SERIES_ROOTS = {"Q5398426", "Q1259759", "Q526877"}  # television series, miniseries, web series
PERSON_ROOTS = {"Q5"}  # human
SEASON_ROOTS = {"Q3464665"}  # television series season
EPISODE_ROOTS = {"Q21191270"}  # television series episode
CHARACTER_ROOTS = {"Q95074"}  # fictional character
EXCLUDED_SERIES_ROOTS = {"Q15416"}  # television program

# Maps a classification result to its target table / NDJSON file name. New entity
# types are added by extending the *_ROOTS sets above, classify_entity(), and this map.
CLASS_TO_TABLE = {
    "movie": "T_WC_WIKIDATA_MOVIE",
    "series": "T_WC_WIKIDATA_SERIE",
    "person": "T_WC_WIKIDATA_PERSON",
    "season": "T_WC_WIKIDATA_SEASON",
    "episode": "T_WC_WIKIDATA_EPISODE",
    "character": "T_WC_WIKIDATA_CHARACTER",
}

# Cheap top-level id extractor used to skip full JSON materialization of entities
# the current pass cannot possibly emit. Anchored at the start of the line, so it
# only matches the entity's own id (Wikidata dump field order is type, then id),
# never a claim GUID or snak value. On any non-match it returns None and the caller
# falls back to the normal full-parse path — so it can only skip, never corrupt.
_ENTITY_ID_RE = re.compile(rb'^\{"type":"(?:item|property)","id":"([A-Za-z]\d+)"')


def fast_entity_id(entity_json: bytes) -> Optional[str]:
    match = _ENTITY_ID_RE.match(entity_json)
    return match.group(1).decode("ascii") if match else None

P_INSTANCE_OF = "P31"
P_SUBCLASS_OF = "P279"
P_IMDB_ID = "P345"
P_FORMATTER_URL = "P1630"
P_FORMATTER_URI_FOR_RDF = "P3303"

SUPPORTED_LOCAL_VALUE_TYPES = {
    "item",
    "string",
    "external_id",
    "media",
    "time",
    "quantity",
}

WIKIDATA_DATATYPE_TO_LOCAL = {
    "wikibase-item": "item",
    "string": "string",
    "external-id": "external_id",
    "commonsMedia": "media",
    "time": "time",
    "quantity": "quantity",
}


@dataclass(slots=True)
class Stats:
    entities_seen: int = 0
    items_seen: int = 0
    properties_seen: int = 0
    parse_errors: int = 0
    statements_emitted: int = 0
    value_rows_emitted: int = 0
    movies_detected: int = 0
    series_detected: int = 0
    persons_detected: int = 0
    seasons_detected: int = 0
    episodes_detected: int = 0
    characters_detected: int = 0
    started_at: float = field(default_factory=time.perf_counter)

    def elapsed(self) -> float:
        return time.perf_counter() - self.started_at

    def count_detected(self, entity_class: str) -> None:
        if entity_class == "movie":
            self.movies_detected += 1
        elif entity_class == "series":
            self.series_detected += 1
        elif entity_class == "person":
            self.persons_detected += 1
        elif entity_class == "season":
            self.seasons_detected += 1
        elif entity_class == "episode":
            self.episodes_detected += 1
        elif entity_class == "character":
            self.characters_detected += 1


class NDJSONWriter:
    # 4 MB write buffer per file: pass2/item_cache emit tens of millions of small
    # NDJSON rows across many files on /shared; large buffering collapses the
    # write() syscall storm that otherwise dominates those passes.
    _WRITE_BUFFER_BYTES = 4 * 1024 * 1024

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = path.open("w", encoding="utf-8", buffering=self._WRITE_BUFFER_BYTES)
        print(f"[FILE] Created {path}", file=sys.stderr, flush=True)

    def write(self, row: Dict[str, Any]) -> None:
        self._fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")

    def close(self) -> None:
        self._fh.close()


class WriterRegistry:
    def __init__(self, out_dir: Path) -> None:
        self.out_dir = out_dir
        self._writers: Dict[str, NDJSONWriter] = {}

    def write(self, name: str, row: Dict[str, Any]) -> None:
        if name not in self._writers:
            self._writers[name] = NDJSONWriter(self.out_dir / f"{name}.jsonl")
        self._writers[name].write(row)

    def close(self) -> None:
        for writer in self._writers.values():
            writer.close()


class ServerVariableWriter:
    """
    Optional MariaDB writer for live ETL progress variables.
    Mirrors citizenphil.f_setservervariable / f_getservervariable.
    Silently disabled when DB_HOST is not set or connection fails.
    """

    def __init__(self) -> None:
        self._conn: Any = None
        self._table: str = ""
        self._enabled: bool = False
        try:
            import pymysql
            import pymysql.cursors as _cursors  # type: ignore[import]
            host = os.environ.get("DB_HOST", "")
            if not host:
                print("[DB] DB_HOST not set — server variable tracking disabled", file=sys.stderr, flush=True)
                return
            port = int(os.environ.get("DB_PORT", "3306"))
            prefix = os.environ.get("DB_NAMESPACE", "")
            self._table = f"{prefix}SERVER_VARIABLE"
            self._conn = pymysql.connect(
                host=host,
                port=port,
                user=os.environ.get("DB_USER", ""),
                password=os.environ.get("DB_PASSWORD", ""),
                database=os.environ.get("DB_NAME", ""),
                cursorclass=_cursors.DictCursor,
                autocommit=True,
            )
            self._enabled = True
            print(f"[DB] Connected — server variable table: {self._table}", file=sys.stderr, flush=True)
        except Exception as exc:
            print(f"[DB] Server variable tracking disabled: {exc}", file=sys.stderr, flush=True)

    @property
    def enabled(self) -> bool:
        return self._enabled

    def get(self, name: str) -> str:
        if not self._enabled:
            return ""
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT VAR_VALUE FROM {self._table} WHERE DELETED=0 AND VAR_NAME=%s",
                    (name,),
                )
                row = cursor.fetchone()
                return row["VAR_VALUE"] if row else ""
        except Exception as exc:
            print(f"[DB] Error reading {name}: {exc}", file=sys.stderr, flush=True)
            return ""

    def set(self, name: str, value: Any, description: str = "") -> None:
        """Mirror of citizenphil.f_setservervariable — upserts a row in SERVER_VARIABLE."""
        if not self._enabled:
            return
        try:
            str_value = "" if value is None else str(value)
            with self._conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT VAR_NAME FROM {self._table} WHERE DELETED=0 AND VAR_NAME=%s",
                    (name,),
                )
                if cursor.fetchone():
                    cursor.execute(
                        f"UPDATE {self._table} SET VAR_VALUE=%s, LONG_DESC=%s"
                        f" WHERE DELETED=0 AND VAR_NAME=%s",
                        (str_value, description, name),
                    )
                else:
                    cursor.execute(
                        f"INSERT INTO {self._table}"
                        f" (VAR_NAME, VAR_VALUE, DESCRIPTION, LONG_DESC, ID_LANG, DELETED)"
                        f" VALUES (%s, %s, %s, %s, 0, 0)",
                        (name, str_value, name, description),
                    )
        except Exception as exc:
            print(f"[DB] Error setting {name}: {exc}", file=sys.stderr, flush=True)

    def close(self) -> None:
        self._enabled = False
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass


def write_id_set(path: Path, values: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for value in sorted(set(values)):
            fh.write(value + "\n")


def load_id_set(path: Optional[Path]) -> Set[str]:
    if path is None or not path.exists():
        return set()
    values: Set[str] = set()
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            value = line.strip()
            if value:
                values.add(value)
    return values


def normalize_wikidata_entity_line(raw_line: bytes) -> Optional[bytes]:
    line = raw_line.strip()
    if not line or line in (b"[", b"]"):
        return None
    if line.startswith(b"["):
        line = line[1:].lstrip()
    if line.endswith(b"]"):
        line = line[:-1].rstrip()
    if line.endswith(b","):
        line = line[:-1].rstrip()
    return line or None


def iter_bz2_lines_from_http(
    url: str,
    chunk_size: int = 8 * 1024 * 1024,
    max_retries: int = 20,
) -> Generator[bytes, None, None]:
    """Stream-decompress a remote .bz2 file line by line with automatic retry.

    On connection drop, reconnects from byte 0 and skips already-yielded lines,
    so the caller receives a seamless uninterrupted stream.
    Wikimedia servers frequently drop large downloads mid-stream.
    """
    user_agent = os.environ.get("WIKIMEDIA_USER_AGENT", "python-httpx")
    headers = {"User-Agent": user_agent}
    # No read timeout: Wikimedia can be very slow; individual chunk reads may
    # take minutes.  Connect timeout is kept short to fail fast on bad address.
    timeout = httpx.Timeout(connect=30.0, read=None, write=None, pool=30.0)

    lines_emitted = 0  # total lines yielded to caller across all attempts

    for attempt in range(max_retries + 1):
        if attempt > 0:
            wait = min(120, 10 * (2 ** (attempt - 1)))
            print(
                f"[HTTP] Retry {attempt}/{max_retries} in {wait}s "
                f"(will skip first {lines_emitted:,} already-processed lines)...",
                file=sys.stderr, flush=True,
            )
            time.sleep(wait)

        print(f"[HTTP] Connecting to {url} (attempt {attempt + 1})", file=sys.stderr, flush=True)
        if attempt == 0:
            print(f"[HTTP] User-Agent: {user_agent}", file=sys.stderr, flush=True)

        decompressor = bz2.BZ2Decompressor()
        buffered = bytearray()
        chunks_received = 0
        total_compressed = 0
        total_decompressed = 0
        lines_this_attempt = 0

        try:
            with httpx.stream("GET", url, timeout=timeout, follow_redirects=True, headers=headers) as response:
                print(f"[HTTP] Connected — status {response.status_code}", file=sys.stderr, flush=True)
                response.raise_for_status()

                for compressed_chunk in response.iter_bytes(chunk_size=chunk_size):
                    if not compressed_chunk:
                        continue

                    chunks_received += 1
                    total_compressed += len(compressed_chunk)

                    try:
                        data = decompressor.decompress(compressed_chunk)
                    except Exception as exc:
                        print(f"[HTTP] bz2 decompression error on chunk {chunks_received}: {exc}", file=sys.stderr, flush=True)
                        raise

                    if not data:
                        continue

                    total_decompressed += len(data)
                    if chunks_received % 10 == 0 or chunks_received <= 3:
                        print(
                            f"[HTTP] chunk {chunks_received}: "
                            f"compressed={total_compressed/1024/1024:.1f} MB  "
                            f"decompressed={total_decompressed/1024/1024:.1f} MB  "
                            f"lines={lines_emitted:,}",
                            file=sys.stderr, flush=True,
                        )

                    buffered.extend(data)
                    start = 0
                    while True:
                        nl = buffered.find(b"\n", start)
                        if nl == -1:
                            if start > 0:
                                del buffered[:start]
                            break
                        lines_this_attempt += 1
                        line = bytes(buffered[start:nl + 1])
                        start = nl + 1
                        if lines_this_attempt <= lines_emitted:
                            continue  # skip lines already sent to caller
                        yield line
                        lines_emitted += 1

            if buffered and lines_this_attempt >= lines_emitted:
                yield bytes(buffered)

            print(
                f"[HTTP] Stream complete — {chunks_received} chunks, "
                f"{total_compressed/1024/1024:.1f} MB compressed, {lines_emitted:,} lines",
                file=sys.stderr, flush=True,
            )
            return  # success — stop retrying

        except httpx.HTTPStatusError as exc:
            print(f"[HTTP] HTTP error: {exc.response.status_code} — {exc}", file=sys.stderr, flush=True)
            raise  # 4xx/5xx: retrying won't help

        except Exception as exc:
            if attempt >= max_retries:
                print(f"[HTTP] Connection error (giving up after {max_retries} retries): {exc}", file=sys.stderr, flush=True)
                raise
            print(f"[HTTP] Connection error (attempt {attempt + 1}/{max_retries + 1}): {exc}", file=sys.stderr, flush=True)


def _open_parallel_bz2(path: Path):
    """Return a multi-core bz2 decompressing reader (indexed_bzip2) or None.

    stdlib bz2 is single-threaded and is the per-pass throughput ceiling for a local
    dump. indexed_bzip2 decompresses the same .bz2 across cores. Optional dependency:
    if it is missing or fails, we return None and the caller uses the stdlib path.
    Set BZ2_PARALLELISM to pin core count (default 0 = use all cores)."""
    try:
        import indexed_bzip2  # type: ignore[import]
    except Exception:
        return None
    try:
        parallelization = int(os.environ.get("BZ2_PARALLELISM", "0"))
        reader = indexed_bzip2.IndexedBzip2File(str(path), parallelization=parallelization)
        print(
            f"[FILE] Parallel bz2 decompression via indexed_bzip2 "
            f"(parallelization={parallelization or 'auto'})",
            file=sys.stderr, flush=True,
        )
        return reader
    except Exception as exc:
        print(f"[FILE] indexed_bzip2 unavailable ({exc}); using single-threaded bz2", file=sys.stderr, flush=True)
        return None


def iter_bz2_lines_from_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> Generator[bytes, None, None]:
    # Fast path: multi-core decompression. read() here already returns DECOMPRESSED bytes,
    # so we only line-split (no per-chunk decompress step).
    reader = _open_parallel_bz2(path)
    if reader is not None:
        buffered = bytearray()
        lines_yielded = 0
        try:
            while True:
                data = reader.read(chunk_size)
                if not data:
                    break
                buffered.extend(data)
                start = 0
                while True:
                    nl = buffered.find(b"\n", start)
                    if nl == -1:
                        if start > 0:
                            del buffered[:start]
                        break
                    lines_yielded += 1
                    yield bytes(buffered[start:nl + 1])
                    start = nl + 1
            if buffered:
                yield bytes(buffered)
        finally:
            reader.close()
        print(f"[FILE] Read complete (parallel) — {lines_yielded:,} lines", file=sys.stderr, flush=True)
        return

    # Fallback: stdlib single-threaded streaming decompression.
    decompressor = bz2.BZ2Decompressor()
    buffered = bytearray()

    print(f"[FILE] Opening {path}", file=sys.stderr, flush=True)
    chunks_read = 0
    total_compressed = 0
    total_decompressed = 0
    lines_yielded = 0

    with path.open("rb") as fh:
        while True:
            compressed_chunk = fh.read(chunk_size)
            if not compressed_chunk:
                break

            chunks_read += 1
            total_compressed += len(compressed_chunk)

            try:
                data = decompressor.decompress(compressed_chunk)
            except Exception as exc:
                print(f"[FILE] bz2 decompression error on chunk {chunks_read}: {exc}", file=sys.stderr, flush=True)
                raise

            if not data:
                continue

            total_decompressed += len(data)
            if chunks_read % 10 == 0 or chunks_read <= 3:
                print(
                    f"[FILE] chunk {chunks_read}: "
                    f"compressed={total_compressed/1024/1024:.1f} MB  "
                    f"decompressed={total_decompressed/1024/1024:.1f} MB  "
                    f"lines={lines_yielded:,}",
                    file=sys.stderr, flush=True,
                )

            buffered.extend(data)
            start = 0
            while True:
                nl = buffered.find(b"\n", start)
                if nl == -1:
                    if start > 0:
                        del buffered[:start]
                    break
                lines_yielded += 1
                yield bytes(buffered[start:nl + 1])
                start = nl + 1

    if buffered:
        yield bytes(buffered)

    print(f"[FILE] Read complete — {chunks_read} chunks, {total_compressed/1024/1024:.1f} MB, {lines_yielded:,} lines", file=sys.stderr, flush=True)


def get_label(doc: Any, lang: str = "en") -> Optional[str]:
    try:
        labels = doc.get("labels")
        if labels is None:
            return None
        payload = labels.get(lang)
        if payload is None:
            return None
        value = payload.get("value")
        return value if isinstance(value, str) else None
    except Exception:
        return None


def get_description(doc: Any, lang: str = "en") -> Optional[str]:
    try:
        descriptions = doc.get("descriptions")
        if descriptions is None:
            return None
        payload = descriptions.get(lang)
        if payload is None:
            return None
        value = payload.get("value")
        return value if isinstance(value, str) else None
    except Exception:
        return None


def extract_labels(doc: Any) -> Dict[str, str]:
    result: Dict[str, str] = {}
    try:
        labels = doc.get("labels")
        if labels is None:
            return result
        for lang, payload in labels.items():
            try:
                value = payload.get("value")
                if isinstance(value, str):
                    result[str(lang)] = value
            except Exception:
                continue
    except Exception:
        pass
    return result


def extract_descriptions(doc: Any) -> Dict[str, str]:
    result: Dict[str, str] = {}
    try:
        descriptions = doc.get("descriptions")
        if descriptions is None:
            return result
        for lang, payload in descriptions.items():
            try:
                value = payload.get("value")
                if isinstance(value, str):
                    result[str(lang)] = value
            except Exception:
                continue
    except Exception:
        pass
    return result


def get_claim_list(doc: Any, pid: str) -> list[Any]:
    try:
        claims = doc.get("claims")
        if claims is None:
            return []
        lst = claims.get(pid)
        if lst is None:
            return []
        return list(lst)
    except Exception:
        return []


def iter_claims_map(doc: Any) -> Iterable[Tuple[str, list[Any]]]:
    try:
        claims = doc.get("claims")
        if claims is None:
            return []
        result = []
        for pid, claim_list in claims.items():
            try:
                result.append((str(pid), list(claim_list)))
            except Exception:
                continue
        return result
    except Exception:
        return []


def extract_rank(claim: Any) -> Optional[str]:
    try:
        value = claim.get("rank")
        return value if isinstance(value, str) else None
    except Exception:
        return None


def extract_statement_guid(claim: Any) -> Optional[str]:
    try:
        value = claim.get("id")
        return value if isinstance(value, str) else None
    except Exception:
        return None


def stable_bigint_from_text(text: str) -> int:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF
    return value or 1


def stable_json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def derive_statement_identity(subject_id: str, property_id: str, claim: Any) -> tuple[int, Optional[str]]:
    statement_guid = extract_statement_guid(claim)
    identity_text = statement_guid or f"{subject_id}|{property_id}|{stable_json_text(claim)}"
    return stable_bigint_from_text(f"statement|{identity_text}"), statement_guid


def derive_qualifier_identity(statement_guid: Optional[str], qualifier_property_id: str, snak: Any, display_order: int) -> tuple[int, str]:
    snak_hash = None
    try:
        value = snak.get("hash") if isinstance(snak, dict) else None
        snak_hash = value if isinstance(value, str) else None
    except Exception:
        snak_hash = None
    identity_text = snak_hash or f"{statement_guid or 'no-guid'}|{qualifier_property_id}|{display_order}|{stable_json_text(snak)}"
    qualifier_hash = f"qualifier|{identity_text}"
    return stable_bigint_from_text(qualifier_hash), qualifier_hash


def extract_mainsnak_datatype_and_value(claim: Any) -> tuple[Optional[str], Any]:
    try:
        mainsnak = claim.get("mainsnak")
        if mainsnak is None:
            return None, None
        datatype = mainsnak.get("datatype")
        snaktype = mainsnak.get("snaktype")
        if snaktype != "value":
            return datatype, None
        datavalue = mainsnak.get("datavalue")
        if datavalue is None:
            return datatype, None
        return datatype, datavalue.get("value")
    except Exception:
        return None, None


def extract_snak_datatype_and_value(snak: Any) -> tuple[Optional[str], Any]:
    try:
        if snak is None:
            return None, None
        datatype = snak.get("datatype")
        snaktype = snak.get("snaktype")
        if snaktype != "value":
            return datatype, None
        datavalue = snak.get("datavalue")
        if datavalue is None:
            return datatype, None
        return datatype, datavalue.get("value")
    except Exception:
        return None, None


def extract_qid_from_wikibase_value(value: Any) -> Optional[str]:
    try:
        if value is None:
            return None
        qid = value.get("id")
        return qid if isinstance(qid, str) else None
    except Exception:
        return None


def extract_string_value(value: Any) -> Optional[str]:
    return value if isinstance(value, str) else None


def extract_time_payload(value: Any) -> Dict[str, Any]:
    if not value:
        return {}

    raw_time = value.get("time")
    precision = value.get("precision")
    calendarmodel = value.get("calendarmodel")
    timezone_offset = value.get("timezone")

    year = month = day = None
    if isinstance(raw_time, str):
        try:
            date_part = raw_time.split("T", 1)[0]
            sign = -1 if date_part.startswith("-") else 1
            date_core = date_part.lstrip("+-")
            parts = date_core.split("-")
            if len(parts) >= 1 and parts[0]:
                year = sign * int(parts[0])
            if len(parts) >= 2 and parts[1]:
                month = int(parts[1])
            if len(parts) >= 3 and parts[2]:
                day = int(parts[2])
        except Exception:
            pass

    return {
        "RAW_TIME_VALUE": raw_time,
        "TIME_PRECISION": precision,
        "CALENDAR_MODEL": calendarmodel,
        "TIMEZONE_OFFSET": timezone_offset,
        "YEAR_VALUE": year,
        "MONTH_VALUE": month,
        "DAY_VALUE": day,
        "DATE_START": None,
        "DATE_END": None,
    }


def extract_quantity_payload(value: Any) -> Dict[str, Any]:
    if not value:
        return {}

    amount = value.get("amount")
    unit = value.get("unit")
    lower = value.get("lowerBound")
    upper = value.get("upperBound")

    unit_qid = None
    if isinstance(unit, str) and unit:
        unit_qid = unit.rsplit("/", 1)[-1] if "/" in unit else unit

    return {
        "AMOUNT": amount,
        "UNIT_ID_WIKIDATA": unit_qid,
        "LOWER_BOUND": lower,
        "UPPER_BOUND": upper,
        "AMOUNT_NORMALIZED": None,
        "DISPLAY_VALUE": None,
    }


def extract_formatter_url(doc: Any) -> Optional[str]:
    for claim in get_claim_list(doc, P_FORMATTER_URL):
        _, value = extract_mainsnak_datatype_and_value(claim)
        s = extract_string_value(value)
        if s:
            return s
    return None


def extract_formatter_uri_for_rdf(doc: Any) -> Optional[str]:
    for claim in get_claim_list(doc, P_FORMATTER_URI_FOR_RDF):
        _, value = extract_mainsnak_datatype_and_value(claim)
        s = extract_string_value(value)
        if s:
            return s
    return None


class SubclassGraph:
    def __init__(self) -> None:
        self.parents_by_child: Dict[str, Set[str]] = defaultdict(set)
        self.children_by_parent: Dict[str, Set[str]] = defaultdict(set)

    def add_edge(self, child: str, parent: str) -> None:
        self.parents_by_child[child].add(parent)
        self.children_by_parent[parent].add(child)

    def descendants_of_roots(self, roots: Set[str]) -> Set[str]:
        seen: Set[str] = set()
        queue = deque(roots)
        while queue:
            node = queue.popleft()
            if node in seen:
                continue
            seen.add(node)
            for child in self.children_by_parent.get(node, ()):
                if child not in seen:
                    queue.append(child)
        return seen


class StatementEmitter:
    def __init__(self, writers: WriterRegistry, stats: Stats) -> None:
        self.writers = writers
        self.stats = stats

    def emit(
        self,
        *,
        subject_id: str,
        property_id: str,
        claim: Any,
        local_value_type: str,
        wikidata_datatype: Optional[str],
        payload: Dict[str, Any],
    ) -> int:
        statement_id, statement_guid = derive_statement_identity(subject_id, property_id, claim)

        self.writers.write("T_WC_WIKIDATA_STATEMENT", {
            "ID_STATEMENT": statement_id,
            "ID_WIKIDATA": subject_id,
            "ID_PROPERTY": property_id,
            "STATEMENT_GUID": statement_guid,
            "STATEMENT_HASH": None,
            "VALUE_TYPE": local_value_type,
            "WIKIDATA_DATATYPE": wikidata_datatype,
            "RANK": extract_rank(claim),
            "IS_BEST_VALUE": None,
            "DISPLAY_ORDER": None,
            "DELETED": 0,
            "DAT_CREAT": None,
            "TIM_UPDATED": None,
            "ID_CREATOR": None,
            "ID_OWNER": None,
            "ID_USER_UPDATED": None,
            "IMPORT_SOURCE": "wikidata_json_dump",
            "IMPORT_BATCH_ID": None,
            "LAST_SYNC_AT": None,
            "IS_VALID": None,
            "VALIDATION_ERROR": None,
            "RAW_VALUE_TEXT": None,
        })
        self.stats.statements_emitted += 1

        row = {"ID_STATEMENT": statement_id, **payload}
        table_map = {
            "item": "T_WC_WIKIDATA_ITEM_VALUE",
            "string": "T_WC_WIKIDATA_STRING_VALUE",
            "external_id": "T_WC_WIKIDATA_EXTERNAL_ID_VALUE",
            "media": "T_WC_WIKIDATA_MEDIA_VALUE",
            "time": "T_WC_WIKIDATA_TIME_VALUE",
            "quantity": "T_WC_WIKIDATA_QUANTITY_VALUE",
        }
        table = table_map.get(local_value_type)
        if table:
            self.writers.write(table, row)
            self.stats.value_rows_emitted += 1
        return statement_id

    def emit_qualifier(
        self,
        *,
        statement_id: int,
        statement_guid: Optional[str],
        qualifier_property_id: str,
        snak: Any,
        local_value_type: str,
        wikidata_datatype: Optional[str],
        payload: Dict[str, Any],
        display_order: int,
    ) -> int:
        qualifier_id, qualifier_hash = derive_qualifier_identity(statement_guid, qualifier_property_id, snak, display_order)
        self.writers.write("T_WC_WIKIDATA_STATEMENT_QUALIFIER", {
            "ID_STATEMENT_QUALIFIER": qualifier_id,
            "ID_STATEMENT": statement_id,
            "ID_QUALIFIER_PROPERTY": qualifier_property_id,
            "QUALIFIER_HASH": qualifier_hash,
            "VALUE_TYPE": local_value_type,
            "WIKIDATA_DATATYPE": wikidata_datatype,
            "DISPLAY_ORDER": display_order,
            "DELETED": 0,
            "DAT_CREAT": None,
            "TIM_UPDATED": None,
            "ID_CREATOR": None,
            "ID_OWNER": None,
            "ID_USER_UPDATED": None,
            "IMPORT_SOURCE": "wikidata_json_dump",
            "IMPORT_BATCH_ID": None,
            "LAST_SYNC_AT": None,
            "IS_VALID": None,
            "VALIDATION_ERROR": None,
            "RAW_VALUE_TEXT": None,
        })
        table_map = {
            "item": "T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE",
            "string": "T_WC_WIKIDATA_QUALIFIER_STRING_VALUE",
            "external_id": "T_WC_WIKIDATA_QUALIFIER_EXTERNAL_ID_VALUE",
            "media": "T_WC_WIKIDATA_QUALIFIER_MEDIA_VALUE",
            "time": "T_WC_WIKIDATA_QUALIFIER_TIME_VALUE",
            "quantity": "T_WC_WIKIDATA_QUALIFIER_QUANTITY_VALUE",
        }
        table = table_map.get(local_value_type)
        if table:
            self.writers.write(table, {"ID_STATEMENT_QUALIFIER": qualifier_id, **payload})
            self.stats.value_rows_emitted += 1
        return qualifier_id


class WikidataDumpETL:
    def __init__(
        self,
        *,
        out_dir: Path,
        pass_name: str,
        dump_url: Optional[str],
        dump_file: Optional[Path],
        class_roots_json: Optional[Path],
        core_entity_ids_path: Optional[Path],
        referenced_item_ids_path: Optional[Path],
        candidate_person_ids_path: Optional[Path],
        referenced_person_ids_path: Optional[Path],
    ) -> None:
        self.out_dir = out_dir
        self.pass_name = pass_name
        self.dump_url = dump_url
        self.dump_file = dump_file

        self.stats = Stats()
        self.writers = WriterRegistry(out_dir)
        self._server_vars = ServerVariableWriter()
        self.subclass_graph = SubclassGraph()
        self.statement_emitter = StatementEmitter(self.writers, self.stats)

        self.movie_descendants: Set[str] = set()
        self.series_descendants: Set[str] = set()
        self.person_descendants: Set[str] = set()
        self.season_descendants: Set[str] = set()
        self.episode_descendants: Set[str] = set()
        self.character_descendants: Set[str] = set()

        self.in_scope_entity_ids: Set[str] = load_id_set(core_entity_ids_path)
        self.referenced_item_ids_filter: Set[str] = load_id_set(referenced_item_ids_path)
        # candidate_person_ids_filter: all Q5 instances from pass1, used in pass2 to identify
        # referenced items that are persons (rule 2: persons in movie/series item values)
        self.candidate_person_ids_filter: Set[str] = load_id_set(candidate_person_ids_path)
        # referenced_person_ids_filter: persons referenced in movie/series statements, from pass2,
        # used in item_cache to emit them to T_WC_WIKIDATA_PERSON instead of T_WC_WIKIDATA_ITEM
        self.referenced_person_ids_filter: Set[str] = load_id_set(referenced_person_ids_path)

        # run() skips the expensive full JSON parse for any Q-item that the current pass cannot
        # possibly emit. pass2 only emits the in-scope core entities; item_cache only emits
        # referenced items/persons. pass1 must see everything (None = no skipping).
        self._fastskip_set: Optional[Set[str]] = None
        if pass_name == "pass2":
            self._fastskip_set = self.in_scope_entity_ids
        elif pass_name == "item_cache":
            self._fastskip_set = self.referenced_item_ids_filter | self.referenced_person_ids_filter

        # pass1 records (id, P31 qids, has_imdb) here during the scan; after the scan the complete
        # subclass graph is used to classify these records into the authoritative core entity set.
        self._p31_sidecar = None
        self._p31_sidecar_path = out_dir / "entity_class_input.tsv"

        self.detected_core_entity_ids: Set[str] = set()
        # candidate_person_ids: built in pass1, all Q5 instances (regardless of IMDb/birth date)
        self.candidate_person_ids: Set[str] = set()
        self.referenced_item_ids: Set[str] = set()
        # referenced_person_ids: built in pass2, persons (from candidate_person_ids) that appear
        # as item-type values in statements of movies or series
        self.referenced_person_ids: Set[str] = set()

        if class_roots_json and class_roots_json.exists():
            self._load_root_sets(class_roots_json)
        # Build classifier pools from whatever descendants are known now (complete in pass2 /
        # item_cache; empty in pass1 until its scan finishes, where _build_pools() is called again).
        self._build_pools()

    def _load_root_sets(self, path: Path) -> None:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                row = json.loads(line)
                root_type = row["ROOT_TYPE"]
                qid = row["QID"]
                if root_type == "movie":
                    self.movie_descendants.add(qid)
                elif root_type == "series":
                    self.series_descendants.add(qid)
                elif root_type == "person":
                    self.person_descendants.add(qid)
                elif root_type == "season":
                    self.season_descendants.add(qid)
                elif root_type == "episode":
                    self.episode_descendants.add(qid)
                elif root_type == "character":
                    self.character_descendants.add(qid)

    def _write_root_sets(self) -> None:
        path = self.out_dir / "class_roots.jsonl"
        descendant_sets = (
            ("movie", self.movie_descendants),
            ("series", self.series_descendants),
            ("person", self.person_descendants),
            ("season", self.season_descendants),
            ("episode", self.episode_descendants),
            ("character", self.character_descendants),
        )
        with path.open("w", encoding="utf-8") as fh:
            for root_type, descendants in descendant_sets:
                for qid in sorted(descendants):
                    fh.write(json.dumps({"ROOT_TYPE": root_type, "QID": qid}, ensure_ascii=False) + "\n")

    def iter_lines(self) -> Generator[bytes, None, None]:
        if self.dump_url:
            yield from iter_bz2_lines_from_http(self.dump_url)
        else:
            assert self.dump_file is not None
            yield from iter_bz2_lines_from_file(self.dump_file)

    def run(self) -> None:
        # simdjson.loads() is used instead of a reusable Parser because
        # Parser.parse() returns a lazy object tied to the parser's internal
        # buffer; reusing the parser while any previous Object/Array is still
        # alive raises a RuntimeError. simdjson.loads() converts each entity
        # to plain Python dicts/lists immediately, avoiding the lifetime issue.

        print(f"[ETL] Starting pass={self.pass_name} out_dir={self.out_dir}", file=sys.stderr, flush=True)
        self._sv_start()
        first_entity_logged = False

        for raw_line in self.iter_lines():
            entity_json = normalize_wikidata_entity_line(raw_line)
            if entity_json is None:
                continue

            # Fast path (pass2 / item_cache): a Q-item the pass cannot emit is skipped
            # before the expensive full parse. Properties and any line the cheap matcher
            # can't read fall through to normal parsing, so this only ever skips, never drops.
            if self._fastskip_set is not None:
                fast_id = fast_entity_id(entity_json)
                if fast_id is not None and fast_id.startswith("Q") and fast_id not in self._fastskip_set:
                    self.stats.entities_seen += 1
                    continue

            try:
                doc = simdjson.loads(entity_json)
            except Exception as exc:
                self.stats.parse_errors += 1
                if self.stats.parse_errors <= 5:
                    print(f"[ETL] Parse error #{self.stats.parse_errors}: {exc} — line[:120]={raw_line[:120]}", file=sys.stderr, flush=True)
                continue

            self.stats.entities_seen += 1

            if not first_entity_logged:
                first_entity_logged = True
                try:
                    print(f"[ETL] First entity: id={doc.get('id')} type={doc.get('type')}", file=sys.stderr, flush=True)
                except Exception:
                    pass

            try:
                entity_id = doc.get("id")
                entity_type = doc.get("type")
            except Exception as exc:
                self.stats.parse_errors += 1
                print(f"[ETL] Error reading id/type: {exc}", file=sys.stderr, flush=True)
                continue

            if entity_type == "property":
                self.stats.properties_seen += 1
                self.process_property(doc, entity_id)
            elif entity_type == "item":
                self.stats.items_seen += 1
                self.process_item(doc, entity_id)

            if self.stats.entities_seen % 10_000 == 0:
                elapsed = self.stats.elapsed()
                rate = self.stats.entities_seen / elapsed if elapsed > 0 else 0.0
                print(
                    f"[{self.stats.entities_seen:,}] "
                    f"items={self.stats.items_seen:,} "
                    f"properties={self.stats.properties_seen:,} "
                    f"movies={self.stats.movies_detected:,} "
                    f"series={self.stats.series_detected:,} "
                    f"persons={self.stats.persons_detected:,} "
                    f"seasons={self.stats.seasons_detected:,} "
                    f"episodes={self.stats.episodes_detected:,} "
                    f"characters={self.stats.characters_detected:,} "
                    f"statements={self.stats.statements_emitted:,} "
                    f"values={self.stats.value_rows_emitted:,} "
                    f"errors={self.stats.parse_errors:,} "
                    f"rate={rate:,.0f} entities/s",
                    file=sys.stderr,
                    flush=True,
                )
                self._sv_progress(elapsed, rate)

        if self.pass_name == "pass1":
            # Whole dump streamed: the subclass graph is now complete.
            self.movie_descendants = self.subclass_graph.descendants_of_roots(MOVIE_ROOTS)
            self.series_descendants = self.subclass_graph.descendants_of_roots(SERIES_ROOTS)
            self.person_descendants = self.subclass_graph.descendants_of_roots(PERSON_ROOTS)
            self.season_descendants = self.subclass_graph.descendants_of_roots(SEASON_ROOTS)
            self.episode_descendants = self.subclass_graph.descendants_of_roots(EPISODE_ROOTS)
            self.character_descendants = self.subclass_graph.descendants_of_roots(CHARACTER_ROOTS)
            self._build_pools()
            self._write_root_sets()
            # Classify the recorded P31 sidecar with the complete graph -> authoritative
            # core_entity_ids.txt and candidate_person_ids.txt.
            self._classify_sidecar_and_write_core()

        if self.pass_name == "pass2":
            write_id_set(self.out_dir / "referenced_item_ids.txt", self.referenced_item_ids)
            # Persons (from candidate_person_ids) referenced in movie/series statements (rule 2)
            write_id_set(self.out_dir / "referenced_person_ids.txt", self.referenced_person_ids)

        self.writers.close()
        self._sv_end(self.stats.elapsed())
        self._server_vars.close()
        self._write_summary()

    def _write_summary(self) -> None:
        elapsed = self.stats.elapsed()
        rate = self.stats.entities_seen / elapsed if elapsed > 0 else 0.0
        summary = {
            "pass_name": self.pass_name,
            "entities_seen": self.stats.entities_seen,
            "items_seen": self.stats.items_seen,
            "properties_seen": self.stats.properties_seen,
            "parse_errors": self.stats.parse_errors,
            "movies_detected": self.stats.movies_detected,
            "series_detected": self.stats.series_detected,
            "persons_detected": self.stats.persons_detected,
            "seasons_detected": self.stats.seasons_detected,
            "episodes_detected": self.stats.episodes_detected,
            "characters_detected": self.stats.characters_detected,
            "statements_emitted": self.stats.statements_emitted,
            "value_rows_emitted": self.stats.value_rows_emitted,
            "elapsed_seconds": round(elapsed, 2),
            "entities_per_second": round(rate, 2),
        }
        (self.out_dir / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))

    @staticmethod
    def _fmt_elapsed(seconds: float) -> str:
        h, rem = divmod(int(seconds), 3600)
        m, s = divmod(rem, 60)
        return f"{h}h {m:02d}m {s:02d}s" if h else f"{m}m {s:02d}s"

    def _sv_start(self) -> None:
        db = self._server_vars
        if not db.enabled:
            return
        # Preserve previous run's end time and runtime before overwriting
        db.set(f"{_SV_PREFIX}enddatetimeprevious", db.get(f"{_SV_PREFIX}enddatetime"),
               "End datetime of the previous Wikidata dump ETL run")
        db.set(f"{_SV_PREFIX}totalruntimeprevious", db.get(f"{_SV_PREFIX}totalruntime"),
               "Total runtime of the previous Wikidata dump ETL run")
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        db.set(f"{_SV_PREFIX}startdatetime", now,
               "Date and time of the last start of the Wikidata dump ETL")
        db.set(f"{_SV_PREFIX}enddatetime", "",
               "Date and time of the last end of the Wikidata dump ETL")
        db.set(f"{_SV_PREFIX}totalruntime", "",
               "Total runtime of the last Wikidata dump ETL run")
        db.set(f"{_SV_PREFIX}currentpass", self.pass_name,
               "Current pass of the Wikidata dump ETL (pass1 / pass2 / item_cache)")
        db.set(f"{_SV_PREFIX}entitiesprocessed", "0",
               "Entities processed so far in the current pass")
        db.set(f"{_SV_PREFIX}itemsprocessed", "0",
               "Items processed so far in the current pass")
        db.set(f"{_SV_PREFIX}propertiesprocessed", "0",
               "Properties processed so far in the current pass")
        db.set(f"{_SV_PREFIX}moviesdetected", "0",
               "Movies detected so far in the current pass")
        db.set(f"{_SV_PREFIX}seriesdetected", "0",
               "Series detected so far in the current pass")
        db.set(f"{_SV_PREFIX}personsdetected", "0",
               "Persons detected so far in the current pass")
        db.set(f"{_SV_PREFIX}statementsemitted", "0",
               "Statements emitted so far in the current pass")
        db.set(f"{_SV_PREFIX}valuerowsemitted", "0",
               "Value rows emitted so far in the current pass")
        db.set(f"{_SV_PREFIX}parseerrors", "0",
               "Parse errors encountered so far in the current pass")
        db.set(f"{_SV_PREFIX}rate", "",
               "Processing speed of the Wikidata dump ETL (entities/s)")
        db.set(f"{_SV_PREFIX}elapsed", "",
               "Elapsed time of the current Wikidata dump ETL pass")

    def _sv_progress(self, elapsed: float, rate: float) -> None:
        db = self._server_vars
        if not db.enabled:
            return
        s = self.stats
        db.set(f"{_SV_PREFIX}entitiesprocessed", f"{s.entities_seen:,}",
               "Entities processed so far in the current pass")
        db.set(f"{_SV_PREFIX}itemsprocessed", f"{s.items_seen:,}",
               "Items processed so far in the current pass")
        db.set(f"{_SV_PREFIX}propertiesprocessed", f"{s.properties_seen:,}",
               "Properties processed so far in the current pass")
        db.set(f"{_SV_PREFIX}moviesdetected", f"{s.movies_detected:,}",
               "Movies detected so far in the current pass")
        db.set(f"{_SV_PREFIX}seriesdetected", f"{s.series_detected:,}",
               "Series detected so far in the current pass")
        db.set(f"{_SV_PREFIX}personsdetected", f"{s.persons_detected:,}",
               "Persons detected so far in the current pass")
        db.set(f"{_SV_PREFIX}statementsemitted", f"{s.statements_emitted:,}",
               "Statements emitted so far in the current pass")
        db.set(f"{_SV_PREFIX}valuerowsemitted", f"{s.value_rows_emitted:,}",
               "Value rows emitted so far in the current pass")
        db.set(f"{_SV_PREFIX}parseerrors", f"{s.parse_errors:,}",
               "Parse errors encountered so far in the current pass")
        db.set(f"{_SV_PREFIX}rate", f"{rate:,.0f} entities/s",
               "Processing speed of the Wikidata dump ETL (entities/s)")
        db.set(f"{_SV_PREFIX}elapsed", self._fmt_elapsed(elapsed),
               "Elapsed time of the current Wikidata dump ETL pass")

    def _sv_end(self, elapsed: float) -> None:
        db = self._server_vars
        if not db.enabled:
            return
        rate = self.stats.entities_seen / elapsed if elapsed > 0 else 0.0
        self._sv_progress(elapsed, rate)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        db.set(f"{_SV_PREFIX}enddatetime", now,
               "Date and time of the last end of the Wikidata dump ETL")
        db.set(f"{_SV_PREFIX}totalruntime", self._fmt_elapsed(elapsed),
               "Total runtime of the last Wikidata dump ETL run")
        db.set(f"{_SV_PREFIX}totalruntimeseconds", str(int(elapsed)),
               "Total runtime in seconds of the last Wikidata dump ETL run")
        db.set(f"{_SV_PREFIX}currentpass", "",
               "Current pass of the Wikidata dump ETL (cleared when complete)")

    def process_property(self, doc: Any, entity_id: str) -> None:
        datatype = doc.get("datatype")
        local_value_type = WIKIDATA_DATATYPE_TO_LOCAL.get(datatype)
        child_table = {
            "item": "T_WC_WIKIDATA_ITEM_VALUE",
            "string": "T_WC_WIKIDATA_STRING_VALUE",
            "external_id": "T_WC_WIKIDATA_EXTERNAL_ID_VALUE",
            "media": "T_WC_WIKIDATA_MEDIA_VALUE",
            "time": "T_WC_WIKIDATA_TIME_VALUE",
            "quantity": "T_WC_WIKIDATA_QUANTITY_VALUE",
        }.get(local_value_type)

        self.writers.write("T_WC_WIKIDATA_PROPERTY_METADATA", {
            "ID_PROPERTY": entity_id,
            "PROPERTY_LABEL": get_label(doc, "en"),
            "PROPERTY_DESCRIPTION": get_description(doc, "en"),
            "WIKIDATA_DATATYPE": datatype,
            "LOCAL_VALUE_TYPE": local_value_type,
            "EXPECTED_CHILD_TABLE": child_table,
            "FORMATTER_URL": extract_formatter_url(doc),
            "FORMATTER_URI_FOR_RDF": extract_formatter_uri_for_rdf(doc),
            "IS_SUPPORTED": 1 if local_value_type in SUPPORTED_LOCAL_VALUE_TYPES else 0,
            "IS_ACTIVE": 1,
            "LAST_SYNC_AT": None,
        })

    def _has_imdb(self, doc: Any) -> bool:
        """Return True if entity has at least one P345 (IMDb ID) value.

        This implements rule 1 for person filtering: only persons with an IMDb ID are included
        as core entities.
        """
        return any(
            extract_mainsnak_datatype_and_value(claim)[1] is not None
            for claim in get_claim_list(doc, P_IMDB_ID)
        )

    def process_item(self, doc: Any, entity_id: str) -> None:
        if self.pass_name == "pass1":
            # pass1 builds the subclass graph and records each entity's P31 (+ an IMDb
            # flag for humans) to a sidecar. It does NOT classify here: the P279
            # descendant sets are only complete once the whole dump has been streamed.
            # After the scan, _classify_sidecar_and_write_core() classifies the sidecar
            # with the COMPLETE graph, producing the authoritative core entity set — so
            # subclass-typed films/series are captured, and pass2 can cheaply id-gate.
            self.collect_subclass_edges(doc)
            p31 = self.direct_p31_qids(doc)
            if p31:
                # Only compute has_imdb for humans (the only class gated on it) to avoid a
                # P345 scan across all ~120M entities.
                has_imdb = 1 if ("Q5" in p31 and self._has_imdb(doc)) else 0
                if self._p31_sidecar is None:
                    self._p31_sidecar_path.parent.mkdir(parents=True, exist_ok=True)
                    self._p31_sidecar = self._p31_sidecar_path.open("w", encoding="utf-8", buffering=4 * 1024 * 1024)
                self._p31_sidecar.write(f"{entity_id}\t{','.join(sorted(p31))}\t{has_imdb}\n")
            return

        if self.pass_name == "pass2":
            # Gate by the authoritative core set produced in pass1. run()'s fast-skip
            # already drops most non-core entities before the full parse; this is the
            # exact guard for properties and any line the fast matcher couldn't read.
            if entity_id not in self.in_scope_entity_ids:
                return
            entity_class = self.classify_entity(doc)
            if entity_class is None:
                return
            self.stats.count_detected(entity_class)

            base_row = {
                "ID_WIKIDATA": entity_id,
                "LABEL_EN": get_label(doc, "en"),
                "DESCRIPTION_EN": get_description(doc, "en"),
                "LABELS_JSON": extract_labels(doc),
                "DESCRIPTIONS_JSON": extract_descriptions(doc),
            }

            table = CLASS_TO_TABLE.get(entity_class)
            if table:
                self.writers.write(table, base_row)

            self.emit_claims_for_in_scope_entity(doc, entity_id)
            return

        if self.pass_name == "item_cache":
            if entity_id in self.in_scope_entity_ids:
                return
            base_row = {
                "ID_WIKIDATA": entity_id,
                "LABEL_EN": get_label(doc, "en"),
                "DESCRIPTION_EN": get_description(doc, "en"),
                "LABELS_JSON": extract_labels(doc),
                "DESCRIPTIONS_JSON": extract_descriptions(doc),
            }
            # Rule 2: persons referenced in movie/series statements go to T_WC_WIKIDATA_PERSON
            if entity_id in self.referenced_person_ids_filter:
                self.writers.write("T_WC_WIKIDATA_PERSON", base_row)
            elif entity_id in self.referenced_item_ids_filter:
                self.writers.write("T_WC_WIKIDATA_ITEM", base_row)

    def collect_subclass_edges(self, doc: Any) -> None:
        child_qid = doc.get("id")
        for claim in get_claim_list(doc, P_SUBCLASS_OF):
            datatype, value = extract_mainsnak_datatype_and_value(claim)
            if datatype != "wikibase-item":
                continue
            parent_qid = extract_qid_from_wikibase_value(value)
            if child_qid and parent_qid:
                self.subclass_graph.add_edge(child_qid, parent_qid)
                self.writers.write("subclass_edges", {
                    "ID_CHILD": child_qid,
                    "ID_PARENT": parent_qid,
                })

    def direct_p31_qids(self, doc: Any) -> Set[str]:
        qids: Set[str] = set()
        for claim in get_claim_list(doc, P_INSTANCE_OF):
            datatype, value = extract_mainsnak_datatype_and_value(claim)
            if datatype != "wikibase-item":
                continue
            qid = extract_qid_from_wikibase_value(value)
            if qid:
                qids.add(qid)
        return qids

    def _build_pools(self) -> None:
        """Precompute root|descendants pools once. Called after descendant sets are
        known (loaded from class_roots.jsonl, or computed at the end of pass1).
        Rebuilding these per entity was an O(entities x roots) set-union cost."""
        self._person_pool = PERSON_ROOTS | self.person_descendants
        self._movie_pool = MOVIE_ROOTS | self.movie_descendants
        self._series_pool = SERIES_ROOTS | self.series_descendants
        self._season_pool = SEASON_ROOTS | self.season_descendants
        self._episode_pool = EPISODE_ROOTS | self.episode_descendants
        self._character_pool = CHARACTER_ROOTS | self.character_descendants

    def classify_qids(self, qids: Set[str]) -> Optional[str]:
        if not qids:
            return None
        if any(qid in self._person_pool for qid in qids):
            return "person"
        if any(qid in self._movie_pool for qid in qids):
            return "movie"
        if any((qid not in EXCLUDED_SERIES_ROOTS) and (qid in self._series_pool) for qid in qids):
            return "series"
        # Checked after series: a season/episode is part-of (not a kind-of) a series,
        # so these pools do not overlap the series pool in Wikidata's P279 graph.
        if any(qid in self._season_pool for qid in qids):
            return "season"
        if any(qid in self._episode_pool for qid in qids):
            return "episode"
        if any(qid in self._character_pool for qid in qids):
            return "character"
        return None

    def classify_entity(self, doc: Any) -> Optional[str]:
        return self.classify_qids(self.direct_p31_qids(doc))

    def _classify_sidecar_and_write_core(self) -> None:
        """pass1 post-scan: classify the recorded (id, P31, has_imdb) sidecar with the
        now-complete subclass graph, producing the authoritative core entity set and the
        all-humans candidate-person set. Streamed line by line — no large in-memory map."""
        if self._p31_sidecar is not None:
            self._p31_sidecar.close()
            self._p31_sidecar = None

        core: Set[str] = set()
        candidate_persons: Set[str] = set()
        if self._p31_sidecar_path.exists():
            with self._p31_sidecar_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) != 3:
                        continue
                    entity_id, p31_csv, imdb_flag = parts
                    qids = set(p31_csv.split(",")) if p31_csv else set()
                    entity_class = self.classify_qids(qids)
                    if entity_class is None:
                        continue
                    if entity_class == "person":
                        # All humans are candidate persons (rule 2: referenced by movies/series).
                        candidate_persons.add(entity_id)
                        # Rule 1: only humans with an IMDb ID become core entities.
                        if imdb_flag == "1":
                            core.add(entity_id)
                            self.stats.persons_detected += 1
                        continue
                    core.add(entity_id)
                    self.stats.count_detected(entity_class)

        self.detected_core_entity_ids = core
        self.candidate_person_ids = candidate_persons
        write_id_set(self.out_dir / "core_entity_ids.txt", core)
        write_id_set(self.out_dir / "candidate_person_ids.txt", candidate_persons)

    def emit_claims_for_in_scope_entity(self, doc: Any, entity_id: str) -> None:
        for property_id, claim_list in iter_claims_map(doc):
            for claim in claim_list:
                wikidata_datatype, value = extract_mainsnak_datatype_and_value(claim)
                local_value_type = WIKIDATA_DATATYPE_TO_LOCAL.get(wikidata_datatype)
                if local_value_type not in SUPPORTED_LOCAL_VALUE_TYPES:
                    continue

                payload: Optional[Dict[str, Any]] = None

                if local_value_type == "item":
                    id_item = extract_qid_from_wikibase_value(value)
                    if not id_item:
                        continue
                    payload = {"ID_ITEM": id_item}
                    self.referenced_item_ids.add(id_item)
                    # Rule 2: track persons referenced in movie/series statements
                    if id_item in self.candidate_person_ids_filter:
                        self.referenced_person_ids.add(id_item)

                elif local_value_type == "string":
                    s = extract_string_value(value)
                    if s is None:
                        continue
                    payload = {
                        "VALUE_STRING": s,
                        "VALUE_STRING_NORMALIZED": s,
                        "LANG_CODE": None,
                    }

                elif local_value_type == "external_id":
                    s = extract_string_value(value)
                    if s is None:
                        continue
                    payload = {
                        "VALUE_EXTERNAL_ID": s,
                        "VALUE_EXTERNAL_ID_NORMALIZED": s,
                        "FORMATTER_URL": None,
                        "FORMATTER_URI_RDF": None,
                        "VALIDATION_STATUS": None,
                    }

                elif local_value_type == "media":
                    s = extract_string_value(value)
                    if s is None:
                        continue
                    payload = {
                        "FILE_NAME": s,
                        "MEDIA_REPOSITORY": "commons",
                        "FILE_PAGE_URL": None,
                        "FILE_DIRECT_URL": None,
                        "MIME_TYPE": None,
                        "FILE_EXTENSION": None,
                    }

                elif local_value_type == "time":
                    payload = extract_time_payload(value)
                    if not payload:
                        continue

                elif local_value_type == "quantity":
                    payload = extract_quantity_payload(value)
                    if not payload:
                        continue

                if payload is None:
                    continue

                statement_id = self.statement_emitter.emit(
                    subject_id=entity_id,
                    property_id=property_id,
                    claim=claim,
                    local_value_type=local_value_type,
                    wikidata_datatype=wikidata_datatype,
                    payload=payload,
                )
                statement_guid = extract_statement_guid(claim)
                qualifiers = claim.get("qualifiers") if isinstance(claim, dict) else None
                if not isinstance(qualifiers, dict):
                    continue
                for qualifier_property_id, snaks in qualifiers.items():
                    if not isinstance(snaks, list):
                        continue
                    for qualifier_index, snak in enumerate(snaks, start=1):
                        qualifier_datatype, qualifier_value = extract_snak_datatype_and_value(snak)
                        qualifier_local_value_type = WIKIDATA_DATATYPE_TO_LOCAL.get(qualifier_datatype)
                        if qualifier_local_value_type not in SUPPORTED_LOCAL_VALUE_TYPES:
                            continue

                        qualifier_payload: Optional[Dict[str, Any]] = None

                        if qualifier_local_value_type == "item":
                            id_item = extract_qid_from_wikibase_value(qualifier_value)
                            if not id_item:
                                continue
                            qualifier_payload = {"ID_ITEM": id_item}

                        elif qualifier_local_value_type == "string":
                            s = extract_string_value(qualifier_value)
                            if s is None:
                                continue
                            qualifier_payload = {
                                "VALUE_STRING": s,
                                "VALUE_STRING_NORMALIZED": s,
                                "LANG_CODE": None,
                            }

                        elif qualifier_local_value_type == "external_id":
                            s = extract_string_value(qualifier_value)
                            if s is None:
                                continue
                            qualifier_payload = {
                                "VALUE_EXTERNAL_ID": s,
                                "VALUE_EXTERNAL_ID_NORMALIZED": s,
                                "FORMATTER_URL": None,
                                "FORMATTER_URI_RDF": None,
                                "VALIDATION_STATUS": None,
                            }

                        elif qualifier_local_value_type == "media":
                            s = extract_string_value(qualifier_value)
                            if s is None:
                                continue
                            qualifier_payload = {
                                "FILE_NAME": s,
                                "MEDIA_REPOSITORY": "commons",
                                "FILE_PAGE_URL": None,
                                "FILE_DIRECT_URL": None,
                                "MIME_TYPE": None,
                                "FILE_EXTENSION": None,
                            }

                        elif qualifier_local_value_type == "time":
                            qualifier_payload = extract_time_payload(qualifier_value)
                            if not qualifier_payload:
                                continue

                        elif qualifier_local_value_type == "quantity":
                            qualifier_payload = extract_quantity_payload(qualifier_value)
                            if not qualifier_payload:
                                continue

                        if qualifier_payload is None:
                            continue

                        self.statement_emitter.emit_qualifier(
                            statement_id=statement_id,
                            statement_guid=statement_guid,
                            qualifier_property_id=qualifier_property_id,
                            snak=snak,
                            local_value_type=qualifier_local_value_type,
                            wikidata_datatype=qualifier_datatype,
                            payload=qualifier_payload,
                            display_order=qualifier_index,
                        )


def _env_path(name: str) -> Optional[Path]:
    value = os.environ.get(name)
    return Path(value) if value else None


def main() -> int:
    dump_url = os.environ.get("DUMP_URL")
    dump_file = _env_path("DUMP_FILE")

    if not dump_url and not dump_file:
        print("ERROR: set DUMP_URL or DUMP_FILE in the environment.", file=sys.stderr)
        return 1
    if dump_url and dump_file:
        print("ERROR: set only one of DUMP_URL or DUMP_FILE, not both.", file=sys.stderr)
        return 1

    pass_name = os.environ.get("PASS_NAME")
    if pass_name not in ("pass1", "pass2", "item_cache"):
        print(f"ERROR: PASS_NAME must be pass1, pass2, or item_cache (got {pass_name!r}).", file=sys.stderr)
        return 1

    out_dir = Path(os.environ.get("OUT_DIR", "/shared"))

    etl = WikidataDumpETL(
        out_dir=out_dir,
        pass_name=pass_name,
        dump_url=dump_url,
        dump_file=dump_file,
        class_roots_json=_env_path("CLASS_ROOTS_JSON"),
        core_entity_ids_path=_env_path("CORE_ENTITY_IDS"),
        referenced_item_ids_path=_env_path("REFERENCED_ITEM_IDS"),
        candidate_person_ids_path=_env_path("CANDIDATE_PERSON_IDS"),
        referenced_person_ids_path=_env_path("REFERENCED_PERSON_IDS"),
    )
    etl.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
