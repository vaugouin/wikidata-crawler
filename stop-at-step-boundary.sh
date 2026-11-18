#!/bin/bash
# Stop the running crawler between two steps, so nothing is lost and the run resumes
# with --start-step (WIKIDATA-CRAWLER-036).
#
#   bash stop-at-step-boundary.sh               stop once the step in progress has finished
#   bash stop-at-step-boundary.sh --after 107   stop once step 107 has finished
#   bash stop-at-step-boundary.sh --notify-only do not stop: say when the next step starts
#   bash stop-at-step-boundary.sh --cancel      withdraw a pending stop request
#
# A step can last a day or more, so run it detached:
#   nohup bash stop-at-step-boundary.sh > /tmp/stop-at-step-boundary.log 2>&1 &
#
# TWO MECHANISMS, because the container runs the code baked in its image:
# 1. the request file $SHARED/STOP_AFTER_STEP. A crawler built from 1.1.33 on reads it
#    between two steps, stops by itself with exit code 0, and leaves stopped_run.json;
# 2. the run log. A crawler built before does not read the file: this script sees the
#    line of the next step appear in the log and runs `docker stop` within seconds of
#    its start, which is the cheapest moment to kill a step. It then writes
#    stopped_run.json itself, and marks the run as already reported so the hourly check
#    does not send a failure alert for a stop that was asked for.
# Either way, stopped_run.json keeps run-if-new-dump.sh from launching a new run (a newer
# dump would wipe the work files) until the resume, which removes it.
#
# With a 1.1.33+ image the request file is enough: `touch $SHARED/STOP_AFTER_STEP` and
# leave. This script only adds the wait, the report and the fallback.
cd "$(dirname "$(readlink -f "$0")")" || exit 2
# The three paths and the poll period can be overridden, for a dry rehearsal with a fake docker.
SHARED=${WIKIDATA_SHARED:-/home/debian/docker/shared_data/wikidata-crawler}
LOG=${WIKIDATA_RUN_LOG:-/home/debian/docker/shared_data/logs/wikidata-crawler/latest.log}
NAME=wikidata-crawler
REQUEST="$SHARED/STOP_AFTER_STEP"
MARKER="$SHARED/stopped_run.json"
POLL=${WIKIDATA_STOP_POLL:-20}

MODE=stop
AFTER=""
while [ $# -gt 0 ]; do
    case "$1" in
        --notify-only) MODE=notify ;;
        --cancel) MODE=cancel ;;
        --after) AFTER="${2:-}"; shift ;;
        *) echo "Unknown argument: $1 (see the header of this script)"; exit 2 ;;
    esac
    shift
done

if [ "$MODE" = "cancel" ]; then
    if [ -f "$REQUEST" ]; then rm -f "$REQUEST" && echo "Stop request withdrawn."; else echo "No stop request pending."; fi
    echo "A stop-at-step-boundary.sh still waiting in the background ends by itself within a minute."
    exit 0
fi

f_running() { [ -n "$(docker ps -q -f name="^${NAME}$")" ]; }
f_laststep() { grep -E '^1[01][0-9]: ' "$LOG" 2>/dev/null | tail -n 1 | cut -c1-3; }
f_notify() {
    # Discord, when the webhook file of run-if-new-dump.sh is there. The message is built
    # here from step codes only, so it needs no JSON escaping.
    local file="${DISCORD_WEBHOOK_FILE:-$PWD/.discord-webhook}"
    [ -s "$file" ] || return 0
    curl -fsS -m 20 -H 'Content-Type: application/json' -d "{\"content\": \"$1\"}" \
        "$(head -1 "$file" | tr -d '[:space:]')" >/dev/null || echo "WARNING: Discord message not sent."
}

if ! f_running; then echo "The crawler is not running: nothing to stop."; exit 1; fi
if [ ! -f "$LOG" ]; then echo "ERROR: run log $LOG not found."; exit 2; fi
CURRENT=$(f_laststep)
if [ -z "$CURRENT" ]; then echo "ERROR: no step line in $LOG yet. Try again in a minute."; exit 2; fi
if [ -n "$AFTER" ]; then
    case "$AFTER" in 10[1-9]|11[0-4]) : ;; *) echo "ERROR: --after takes a step code from 101 to 114."; exit 2 ;; esac
    if [ "$AFTER" -lt "$CURRENT" ]; then echo "ERROR: step $AFTER is already finished (step $CURRENT is running)."; exit 2; fi
fi
THRESHOLD=${AFTER:-$CURRENT}
RUNLOG=$(readlink -f "$LOG")

if [ "$MODE" = "notify" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') step $CURRENT is running. Waiting for the next step to start (nothing will be stopped) ..."
    while f_running; do
        NOW=$(f_laststep)
        if [ "$NOW" != "$CURRENT" ]; then
            echo "$(date '+%Y-%m-%d %H:%M:%S') step $NOW has just started: $(grep -E "^$NOW: " "$LOG" | tail -n 1)"
            echo "Good moment to stop: docker stop $NAME, then resume with ./wikidata-crawler.sh --start-step $NOW"
            echo "(a validation step lasts seconds: bash stop-at-step-boundary.sh does it for you, in time.)"
            printf '\a'
            f_notify "**wikidata-crawler : l'etape $CURRENT est finie, l'etape $NOW demarre.** Bon moment pour arreter (docker stop $NAME), reprise par ./wikidata-crawler.sh --start-step $NOW"
            exit 0
        fi
        sleep "$POLL"
    done
    echo "$(date '+%Y-%m-%d %H:%M:%S') the crawler ended while step $CURRENT was the last one started."
    exit 0
fi

# Stop mode.
printf '%s' "$AFTER" > "$REQUEST" || { echo "ERROR: cannot write $REQUEST."; exit 2; }
echo "$(date '+%Y-%m-%d %H:%M:%S') stop requested: once step $THRESHOLD has finished (step $CURRENT is running)."
echo "Request file: $REQUEST. Withdraw with: bash stop-at-step-boundary.sh --cancel"
while f_running; do
    if [ ! -f "$REQUEST" ]; then
        # Withdrawn (--cancel), or taken by the crawler, which is then about to exit.
        sleep 30
        f_running || break
        echo "$(date '+%Y-%m-%d %H:%M:%S') the stop request is gone and the crawler still runs: withdrawn, nothing stopped."
        exit 0
    fi
    NOW=$(f_laststep)
    if [ -n "$NOW" ] && [ "$NOW" -gt "$THRESHOLD" ]; then
        # The crawler started a step past the threshold: its image does not read the
        # request file. Kill the step in its first seconds.
        echo "$(date '+%Y-%m-%d %H:%M:%S') step $NOW has just started and the crawler did not stop by itself (image older than 1.1.33): docker stop."
        echo "$RUNLOG" > "${WIKIDATA_FAILURE_MARKER:-.last-failure-notified}"
        docker stop "$NAME" >/dev/null
        rm -f "$REQUEST"
        printf '{"status": "STOPPED", "stopped_after_step": %s, "resume_step": %s, "stopped_at": "%s", "by": "stop-at-step-boundary.sh, docker stop"}\n' \
            "$THRESHOLD" "$NOW" "$(date '+%Y-%m-%d %H:%M:%S')" > "$MARKER"
        echo "Stopped at the start of step $NOW. The status variables still say RUNNING: the resume overwrites them."
        echo "Resume with the same IMPORT_BATCH_ID: ./wikidata-crawler.sh --start-step $NOW"
        f_notify "**wikidata-crawler : arrete sur demande au debut de l'etape $NOW.** Reprise : ./wikidata-crawler.sh --start-step $NOW"
        exit 0
    fi
    sleep "$POLL"
done

rm -f "$REQUEST"
if [ -f "$MARKER" ]; then
    RESUME=$(sed -nE 's/.*"resume_step": *([0-9]+).*/\1/p' "$MARKER")
    echo "$(date '+%Y-%m-%d %H:%M:%S') the crawler stopped by itself between two steps: $(cat "$MARKER")"
    echo "Resume with the same IMPORT_BATCH_ID: ./wikidata-crawler.sh --start-step $RESUME"
    f_notify "**wikidata-crawler : arrete sur demande entre deux etapes.** Reprise : ./wikidata-crawler.sh --start-step $RESUME"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') the crawler ended without stopping on request (finished, or failed): $(tail -n 1 "$LOG")"
fi
