#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required"
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required"
  exit 1
fi

echo "[1/7] Core smoke"
./scripts/smoke.sh

echo "[2/7] Browser WASM smoke"
./scripts/browser_smoke.sh

echo "[3/7] Gateway range e2e"
./scripts/test_gateway_range.sh

echo "[4/7] Runner extensions"
ext_json="$(curl -sS http://localhost:3000/extensions)"
echo "$ext_json" | jq -e '.extensions.fts and .extensions.vss' >/dev/null
echo "extensions ok: $(echo "$ext_json" | jq -c '.extensions')"

echo "[5/7] API success + lineage"
run_ok="$(curl -sS -X POST http://localhost:8000/v1/run_sql \
  -H 'content-type: application/json' \
  -d '{
    "sql": "select count(*) as n from read_parquet(\"s3://lakehouse/demo/events.parquet\")",
    "output": {"format": "parquet", "s3_key": "agent/ws1/results/full-validation.parquet"},
    "budget": {"max_seconds": 20, "max_scan_bytes": 268435456, "max_output_bytes": 67108864, "max_memory_mb": 512}
  }')"
job_id="$(echo "$run_ok" | jq -r '.job_id // empty')"
if [ -z "$job_id" ]; then
  echo "run_sql success test failed"
  echo "$run_ok" | jq .
  exit 1
fi
lineage="$(curl -sS "http://localhost:8000/v1/lineage/${job_id}")"
echo "$lineage" | jq -e ".job_id == \"${job_id}\"" >/dev/null
echo "run_sql ok: job_id=${job_id}"

echo "[6/7] Capability and budget enforcement"
cap_read="$(curl -sS -X POST http://localhost:8000/v1/run_sql \
  -H 'content-type: application/json' \
  -d '{
    "sql": "select * from read_parquet(\"s3://lakehouse/private/events.parquet\")",
    "output": {"format": "parquet", "s3_key": "agent/ws1/results/private.parquet"}
  }')"
echo "$cap_read" | jq -e '.error.code == "capability_denied"' >/dev/null

cap_write="$(curl -sS -X POST http://localhost:8000/v1/run_sql \
  -H 'content-type: application/json' \
  -d '{
    "sql": "select 1 as n",
    "output": {"format": "parquet", "s3_key": "forbidden/results.parquet"}
  }')"
echo "$cap_write" | jq -e '.error.code == "capability_denied"' >/dev/null

budget_low="$(curl -sS -X POST http://localhost:8000/v1/run_sql \
  -H 'content-type: application/json' \
  -d '{
    "sql": "select * from read_parquet(\"s3://lakehouse/demo/events.parquet\")",
    "output": {"format": "parquet", "s3_key": "agent/ws1/results/low-budget.parquet"},
    "budget": {"max_seconds": 20, "max_scan_bytes": 1, "max_output_bytes": 67108864, "max_memory_mb": 512}
  }')"
echo "$budget_low" | jq -e '.error.code == "budget_exceeded"' >/dev/null
echo "policy/budget checks ok"

echo "[7/7] Production preflight (optional)"
if [ -f ".env.production" ]; then
  ./scripts/preflight_production.sh .env.production
  echo "production preflight ok"
else
  echo "skipped: .env.production not found"
fi

echo "full validation passed"
