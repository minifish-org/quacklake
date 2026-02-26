#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required"
  exit 1
fi

echo "[1/5] Start storage + gateway + browser"
docker compose up -d minio createbuckets gateway browser

echo "[2/5] Wait for health"
for i in $(seq 1 40); do
  if curl -sf http://localhost:8080/healthz >/dev/null && curl -sf http://localhost:8081 >/dev/null; then
    break
  fi
  sleep 1
  if [ "$i" -eq 40 ]; then
    echo "health checks failed"
    docker compose ps
    exit 1
  fi
done

echo "[3/5] Seed parquet"
./scripts/seed.sh

echo "[4/5] Browser query smoke (headless)"
cd tools/browser-smoke
if [ "${BROWSER_SMOKE_SKIP_PLAYWRIGHT_INSTALL:-0}" != "1" ]; then
  npx playwright install chromium >/dev/null
fi
npm run -s run
cd ..
cd ..

echo "[5/5] Browser smoke passed"
