#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required"
  exit 1
fi

echo "[1/4] Start storage + gateway"
docker compose up -d minio createbuckets gateway

echo "[2/4] Wait for gateway health"
for i in $(seq 1 30); do
  if curl -sf http://localhost:8080/healthz >/dev/null; then
    break
  fi
  sleep 1
  if [ "$i" -eq 30 ]; then
    echo "gateway health check failed"
    docker compose ps
    exit 1
  fi
done

echo "[3/4] Seed demo parquet"
./scripts/seed.sh

echo "[4/4] Validate HEAD and Range semantics"
head_status=$(curl -sS -o /dev/null -w '%{http_code}' -I \
  http://localhost:8080/objects/lakehouse/demo/events.parquet)
if [ "$head_status" != "200" ]; then
  echo "HEAD expected 200, got $head_status"
  exit 1
fi

range_headers=$(curl -sS -D - -o /dev/null -H 'Range: bytes=0-10' \
  http://localhost:8080/objects/lakehouse/demo/events.parquet)
echo "$range_headers" | grep -i '^HTTP/1.1 206' >/dev/null
echo "$range_headers" | grep -i '^accept-ranges: bytes' >/dev/null
echo "$range_headers" | grep -i '^content-range:' >/dev/null
echo "$range_headers" | grep -i '^content-length: 11' >/dev/null

echo "gateway range test passed"
