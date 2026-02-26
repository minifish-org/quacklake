#!/usr/bin/env bash
set -euo pipefail

check_cmd() {
  local name="$1"
  if command -v "$name" >/dev/null 2>&1; then
    echo "ok: $name"
  else
    echo "missing: $name"
    return 1
  fi
}

err=0

check_cmd docker || err=1
check_cmd curl || err=1
check_cmd jq || err=1

if docker compose version >/dev/null 2>&1; then
  echo "ok: docker compose"
else
  echo "missing: docker compose"
  err=1
fi

if [ "$err" -ne 0 ]; then
  echo "doctor failed"
  exit 1
fi

echo "doctor passed"
