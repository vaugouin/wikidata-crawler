#!/bin/bash
# Which step is the crawler on, and what would stopping it now cost (WIKIDATA-CRAWLER-036).
# Read-only. Usage: bash crawler-status.sh
#
# crawler_status.py is piped into a container rather than run from the image, so the
# answer also comes from a container whose image was built before the file existed:
# the running one when there is one, a throwaway one otherwise (last known state).
cd "$(dirname "$(readlink -f "$0")")" || exit 2
LOG=/home/debian/docker/shared_data/logs/wikidata-crawler/latest.log
if [ -n "$(docker ps -q -f name=^wikidata-crawler$)" ]; then
    echo "container         : running since $(docker inspect -f '{{.State.StartedAt}}' wikidata-crawler | cut -c1-19) UTC"
    docker exec -i wikidata-crawler python - < crawler_status.py
else
    echo "container         : not running"
    docker run --rm -i --network="host" --env-file .env \
        -v /home/debian/docker/shared_data/wikidata-crawler:/shared \
        --entrypoint python wikidata-crawler-python-app - < crawler_status.py
fi
if [ -f "$LOG" ]; then
    echo
    echo "last step lines of $(readlink -f "$LOG"):"
    grep -E '^1[01][0-9]: |^=== wikidata-crawler |^run: ' "$LOG" | tail -n 4 | sed 's/^/  /'
fi
