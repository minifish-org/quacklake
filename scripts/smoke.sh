#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required"
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required"
  exit 1
fi

echo "[1/6] Start core services"
docker compose up -d minio createbuckets runner gateway api

echo "[2/6] Wait for health"
for i in $(seq 1 30); do
  if curl -sf http://localhost:8000/healthz >/dev/null && curl -sf http://localhost:8080/healthz >/dev/null; then
    break
  fi
  sleep 1
  if [ "$i" -eq 30 ]; then
    echo "health checks failed"
    docker compose ps
    exit 1
  fi
done

echo "[3/6] Seed demo parquet"
./scripts/seed.sh

echo "[4/6] Execute run_sql"
payload='{
    "sql": "select count(*) as n from read_parquet(\"s3://lakehouse/demo/events.parquet\")",
    "output": {"format": "parquet", "s3_key": "agent/ws1/results/smoke-count.parquet"},
    "budget": {"max_seconds": 20, "max_scan_bytes": 268435456, "max_output_bytes": 67108864, "max_memory_mb": 512}
  }'
if [ -n "${API_KEY:-}" ]; then
  run_resp=$(curl -sS -X POST http://localhost:8000/v1/run_sql \
    -H "x-api-key: ${API_KEY}" \
    -H 'content-type: application/json' \
    -d "${payload}")
else
  run_resp=$(curl -sS -X POST http://localhost:8000/v1/run_sql \
    -H 'content-type: application/json' \
    -d "${payload}")
fi

job_id=$(echo "$run_resp" | jq -r '.job_id // empty')
if [ -z "$job_id" ]; then
  echo "run_sql failed"
  echo "$run_resp" | jq .
  exit 1
fi

echo "[5/6] Validate lineage"
if [ -n "${API_KEY:-}" ]; then
  lineage_resp=$(curl -sS -H "x-api-key: ${API_KEY}" "http://localhost:8000/v1/lineage/${job_id}")
else
  lineage_resp=$(curl -sS "http://localhost:8000/v1/lineage/${job_id}")
fi
echo "$lineage_resp" | jq -e '.job_id == "'"$job_id"'"' >/dev/null

echo "[6/6] Validate gateway range"
artifact_url=$(echo "$run_resp" | jq -r '.output.gateway_url' | sed 's#http://gateway:8080#http://localhost:8080#')
status_code=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Range: bytes=0-127' "$artifact_url")
if [ "$status_code" != "206" ]; then
  echo "range check failed: expected 206 got $status_code"
  exit 1
fi

echo "smoke passed"
echo "job_id: $job_id"
echo "artifact_url: $artifact_url"
