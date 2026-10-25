#!/bin/bash
# Switch wikidata-crawler.sh and run-if-new-dump.sh back on: removes the flag file CRON_OFF that off.sh wrote.
cd "$(dirname "$(readlink -f "$0")")" || exit 1
# A launcher renamed by the old off.sh (<name>-off.sh, before 2026-10-25) is put back,
# or dropped when git has already restored the tracked one.
[ -f wikidata-crawler-off.sh ] && { [ -f wikidata-crawler.sh ] && rm -f wikidata-crawler-off.sh || mv wikidata-crawler-off.sh wikidata-crawler.sh; }
[ -f run-if-new-dump-off.sh ] && { [ -f run-if-new-dump.sh ] && rm -f run-if-new-dump-off.sh || mv run-if-new-dump-off.sh run-if-new-dump.sh; }
rm -f CRON_OFF && echo "wikidata-crawler: on (CRON_OFF removed)"
