#!/bin/bash

# Any arguments to this script are forwarded to wikidata_crawler.py inside the
# container (the Dockerfile's ENTRYPOINT is `python wikidata_crawler.py`).
#
# Examples:
#   ./wikidata-crawler.sh                       # full run, starts at step 101
#   ./wikidata-crawler.sh --start-step 110      # resume only the bulk load + final validation
#   ./wikidata-crawler.sh --start-step 108      # resume from staging load onward
#
# One argument is NOT forwarded, --check-dump, because it must not start a run:
#   ./wikidata-crawler.sh --check-dump          # is a newer dump published? reads only
#   ./wikidata-crawler.sh --check-dump --vs-local   # compare with the local file
#   ./wikidata-crawler.sh --check-dump --quiet  # nothing printed, exit code only
#
# WHY THIS FLAG EXISTS AT ALL, since the question sounds like the launcher's job.
# It is not: a plain ./wikidata-crawler.sh does NOT download when the dump is
# already on the shared volume (step 101 reuses an existing DUMP_FILE, see
# step_resolve_dump_source). What it does is spend three to four days re-ingesting
# it. So the risk this flag guards against is not bandwidth, it is a pointless run,
# and asking the question has to be possible without starting one.
#
# It is a thin wrapper over check_new_dump.py in a throwaway container: one HEAD
# request, not a byte of dump transferred, nothing written anywhere. The real
# weekly automation is run-if-new-dump.sh (cron), which asks the same question and
# acts on it; this flag is for asking by hand.

if [ "${1:-}" = "--check-dump" ]; then
    shift
    cd /home/debian/docker/wikidata-crawler || exit 2
    # Read-only, so it is allowed during a run, but the answer is then misleading:
    # step 101 may already have downloaded the new dump without having recorded its
    # size yet, so both anchors are unstable. Warn rather than refuse.
    if [ "$(docker ps -q -f name=wikidata-crawler)" ]; then
        echo "WARNING: the crawler is running. The comparison anchor is unstable" >&2
        echo "         while step 101 downloads; read the verdict with that in mind." >&2
    fi
    docker build -q -t wikidata-crawler-python-app . >/dev/null || {
        echo "ERROR: image build failed." >&2; exit 2; }
    exec docker run --rm --network="host" --env-file .env \
        -v /home/debian/docker/shared_data/wikidata-crawler:/shared \
        --entrypoint python wikidata-crawler-python-app check_new_dump.py "$@"
fi

# Check if the wikidata-crawler Docker container is running
if [ $(docker ps -q -f name=wikidata-crawler) ]; then
    echo "wikidata-crawler Docker container is already running."
else
    # Start the wikidata-crawler container if it is not running
    # Create the per-stack shared_data subdir if it doesn't exist.
    # The crawler reads its own staging files (pass1/pass2/item_cache) and loads
    # them via INSERT, so it never needs the shared_data root — a dedicated subdir
    # keeps its ~22 GB of staging data isolated from other stacks.
    mkdir -p /home/debian/docker/shared_data/wikidata-crawler
    cd /home/debian/docker/wikidata-crawler
    docker build -t wikidata-crawler-python-app .
    # docker run -it --rm --network="host" --name wikidata-crawler --env-file .env -v /home/debian/docker/shared_data/wikidata-crawler:/shared wikidata-crawler-python-app "$@"
    docker run -d --rm --network="host" --name wikidata-crawler --env-file .env -v /home/debian/docker/shared_data/wikidata-crawler:/shared wikidata-crawler-python-app "$@"
    echo "wikidata-crawler Docker container started."
    # Follow the logs only when someone is watching. Under cron (or any pipe),
    # `docker logs -f` would keep the caller alive for the three to four days the
    # run lasts, writing gigabytes into the cron log. The container is detached
    # (-d), so not following changes nothing about the run itself.
    if [ -t 1 ]; then
        docker logs -f wikidata-crawler
    else
        echo "Logs not followed (non-interactive output). Follow them with:"
        echo "  docker logs -f wikidata-crawler"
    fi
fi
