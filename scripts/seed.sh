#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
command -v docker >/dev/null || { echo "docker is required for seeding" >&2; exit 1; }
seed_dir="$(mktemp -d)"
trap 'rm -rf "$seed_dir"' EXIT
# Generate public toy data only. No host MinIO credentials or aliases are read.
docker run --rm --network none --entrypoint duckdb -v "$seed_dir:/work" \
  duckdb/duckdb:latest \
  -c "COPY (SELECT * FROM (VALUES (1,'click'),(2,'view'),(3,'purchase')) AS t(event_id,event_type)) TO '/work/events.parquet' (FORMAT PARQUET);"
test -f "$seed_dir/events.parquet"
docker compose run --rm -T --no-deps --entrypoint /bin/sh createbuckets -c '
  mc alias set demo http://minio:9000 minioadmin minioadmin >/dev/null &&
  mc mb --ignore-existing demo/lakehouse >/dev/null &&
  mc pipe demo/lakehouse/demo/events.parquet
' < "$seed_dir/events.parquet"
echo "seeded s3://lakehouse/demo/events.parquet"
