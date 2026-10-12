#!/usr/bin/env sh
# Local PGMQ for development and the examples (see docker-compose.yaml).
#
#   scripts/db.sh start   start the container and wait until Postgres accepts connections
#   scripts/db.sh stop    stop the container and delete its data
#   scripts/db.sh psql    open psql inside the container
#   scripts/db.sh url     print the connection string
#
# PGMQ_PORT (default 54329) and PGMQ_IMAGE_TAG (default v1.13.0) override the compose defaults.
set -eu

cd "$(dirname "$0")/.."

port="${PGMQ_PORT:-54329}"
url="postgres://postgres:postgres@localhost:${port}/postgres"

case "${1:-}" in
  start)
    docker compose up -d --wait
    echo "PGMQ is ready. Examples read DATABASE_URL and default to:"
    echo "  ${url}"
    ;;
  stop)
    docker compose down --volumes
    ;;
  psql)
    docker compose exec postgres psql -U postgres -d postgres
    ;;
  url)
    echo "${url}"
    ;;
  *)
    echo "usage: scripts/db.sh start|stop|psql|url" >&2
    exit 64
    ;;
esac
