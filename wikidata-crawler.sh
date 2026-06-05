#!/bin/bash

# Any arguments to this script are forwarded to wikidata_crawler.py inside the
# container (the Dockerfile's ENTRYPOINT is `python wikidata_crawler.py`).
#
# Examples:
#   ./wikidata-crawler.sh                       # full run, starts at step 101
#   ./wikidata-crawler.sh --start-step 110      # resume only the bulk load + final validation
#   ./wikidata-crawler.sh --start-step 108      # resume from staging load onward

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
    docker logs -f wikidata-crawler
fi
