#!/bin/bash
# Runs the full 3-pass ETL pipeline inside the Docker container.
#
# Dump source logic (set in .env):
#   DUMP_FILE only          — use the local .bz2 file directly
#   DUMP_URL only           — stream from HTTP for every pass (no local file written)
#   DUMP_FILE + DUMP_URL    — download from DUMP_URL to DUMP_FILE if the file does not
#                             exist yet, then use the local file for all 3 passes
#
# In every case the .bz2 is read by streaming chunk-by-chunk decompression;
# the full decompressed dump is never stored on disk.

set -euo pipefail

PASS1_DIR=/shared/pass1
PASS2_DIR=/shared/pass2
ITEM_CACHE_DIR=/shared/item_cache

_DUMP_URL="${DUMP_URL:-}"
_DUMP_FILE="${DUMP_FILE:-}"

# ── resolve dump source ──────────────────────────────────────────────────────

if [ -n "$_DUMP_FILE" ] && [ -n "$_DUMP_URL" ]; then
    if [ -f "$_DUMP_FILE" ]; then
        echo "=== Using existing local dump: $_DUMP_FILE ==="
        export DUMP_FILE="$_DUMP_FILE"
        unset DUMP_URL
    else
        echo "=== Downloading dump from $_DUMP_URL to $_DUMP_FILE ==="
        python - <<EOF
import httpx, os, sys
url  = os.environ["DUMP_URL"]
dest = os.environ["DUMP_FILE"]
with httpx.stream("GET", url, timeout=120.0, follow_redirects=True) as r:
    r.raise_for_status()
    with open(dest, "wb") as fh:
        for chunk in r.iter_bytes(chunk_size=8 * 1024 * 1024):
            fh.write(chunk)
            sys.stderr.write(".")
            sys.stderr.flush()
sys.stderr.write("\nDownload complete.\n")
EOF
        echo "=== Download complete. Using local dump: $_DUMP_FILE ==="
        export DUMP_FILE="$_DUMP_FILE"
        unset DUMP_URL
    fi
elif [ -n "$_DUMP_FILE" ]; then
    if [ ! -f "$_DUMP_FILE" ]; then
        echo "ERROR: DUMP_FILE=$_DUMP_FILE does not exist and DUMP_URL is not set." >&2
        exit 1
    fi
    echo "=== Using existing local dump: $_DUMP_FILE ==="
    export DUMP_FILE="$_DUMP_FILE"
    unset DUMP_URL
elif [ -n "$_DUMP_URL" ]; then
    echo "=== Streaming dump from URL (no local file): $_DUMP_URL ==="
    export DUMP_URL="$_DUMP_URL"
    unset DUMP_FILE
else
    echo "ERROR: set DUMP_URL and/or DUMP_FILE in the environment." >&2
    exit 1
fi

# ── pass 1 ───────────────────────────────────────────────────────────────────

echo "=== ETL: Pass 1 — build classification graph and core entity IDs ==="
OUT_DIR=$PASS1_DIR \
PASS_NAME=pass1 \
python /app/wikidata_dump_etl.py

# ── pass 2 ───────────────────────────────────────────────────────────────────

echo "=== ETL: Pass 2 — emit entity rows and statements ==="
OUT_DIR=$PASS2_DIR \
PASS_NAME=pass2 \
CLASS_ROOTS_JSON=$PASS1_DIR/class_roots.jsonl \
CORE_ENTITY_IDS=$PASS1_DIR/core_entity_ids.txt \
CANDIDATE_PERSON_IDS=$PASS1_DIR/candidate_person_ids.txt \
python /app/wikidata_dump_etl.py

# ── item-cache pass ───────────────────────────────────────────────────────────

echo "=== ETL: Item-cache pass — emit referenced items ==="
OUT_DIR=$ITEM_CACHE_DIR \
PASS_NAME=item_cache \
CORE_ENTITY_IDS=$PASS1_DIR/core_entity_ids.txt \
REFERENCED_ITEM_IDS=$PASS2_DIR/referenced_item_ids.txt \
REFERENCED_PERSON_IDS=$PASS2_DIR/referenced_person_ids.txt \
python /app/wikidata_dump_etl.py

echo "=== ETL complete. Staging files in /shared/pass1, /shared/pass2, /shared/item_cache ==="
