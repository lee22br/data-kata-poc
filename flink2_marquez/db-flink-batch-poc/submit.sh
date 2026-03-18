#!/usr/bin/env bash
# Submits the fat JAR to the running Flink cluster.
# Requires: docker compose up (postgres + flink-jobmanager + flink-taskmanager) already running.
# Requires: ./build.sh has been run first.
#
# Usage:
#   ./submit.sh --from 2024-01-01 --to 2024-01-31
set -euo pipefail
cd "$(dirname "$0")"

FROM=""
TO=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --from) FROM="$2"; shift 2 ;;
    --to)   TO="$2";   shift 2 ;;
    *) echo "Unknown argument: $1" >&2; echo "Usage: ./submit.sh --from yyyy-MM-dd --to yyyy-MM-dd" >&2; exit 1 ;;
  esac
done

if [[ -z "$FROM" || -z "$TO" ]]; then
  echo "Usage: ./submit.sh --from yyyy-MM-dd --to yyyy-MM-dd" >&2
  exit 1
fi

JAR="$(pwd)/flink-job/target/flink-job-1.0.jar"
if [[ ! -f "$JAR" ]]; then
  echo "JAR not found: $JAR" >&2
  echo "Run ./build.sh first." >&2
  exit 1
fi

echo "Submitting batch job  from=$FROM  to=$TO"
docker run --rm \
  --network db-flink-batch-poc_default \
  -v "$JAR":/job.jar \
  -e SOURCE_DB_URL=jdbc:postgresql://postgres:5432/salesdb \
  -e SOURCE_DB_USER=poc \
  -e SOURCE_DB_PASS=poc123 \
  -e SINK_DB_URL=jdbc:postgresql://postgres:5432/salesdb \
  -e SINK_DB_USER=poc \
  -e SINK_DB_PASS=poc123 \
  flink:1.18-java11 \
  flink run -m flink-jobmanager:8081 -c com.poc.BatchJob /job.jar \
    --from "$FROM" --to "$TO"

echo "Done."
