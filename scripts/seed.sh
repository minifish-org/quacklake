#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for seeding"
  exit 1
fi

mc_cmd() {
  if command -v mc >/dev/null 2>&1; then
    mc "$@"
  else
    docker run --rm --network host \
      -v "${HOME}/.mc:/root/.mc" \
      minio/mc:latest "$@"
  fi
}

mc_put_file() {
  local src="$1"
  local dst="$2"

  if command -v mc >/dev/null 2>&1; then
    mc cp "$src" "$dst"
  else
    cat "$src" | docker run --rm --network host -i \
      -v "${HOME}/.mc:/root/.mc" \
      minio/mc:latest pipe "$dst"
  fi
}

mc_cmd alias set local http://127.0.0.1:9000 minioadmin minioadmin
mc_cmd mb local/lakehouse || true

workdir="$(pwd)/.tmp-seed"
rm -rf "$workdir"
mkdir -p "$workdir"
trap 'rm -rf "$workdir"' EXIT

docker run --rm -v "$workdir:/work" duckdb/duckdb:latest \
  duckdb \
  -c "COPY (SELECT * FROM (VALUES (1,'click'),(2,'view'),(3,'purchase')) AS t(event_id,event_type)) TO '/work/events.parquet' (FORMAT PARQUET);"

if [ ! -f "$workdir/events.parquet" ]; then
  echo "failed to generate parquet seed at $workdir/events.parquet"
  ls -la "$workdir"
  exit 1
fi

mc_put_file "$workdir/events.parquet" local/lakehouse/demo/events.parquet

echo "seeded s3://lakehouse/demo/events.parquet"
