#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

runner_version=$(node -p "require('./runner/package.json').dependencies['@duckdb/duckdb-wasm']")
browser_version=$(sed -n "s/^const DUCKDB_WASM_VERSION = '\(.*\)';$/\1/p" browser/main.js)

if [ -z "${browser_version}" ]; then
  echo "failed to parse DUCKDB_WASM_VERSION from browser/main.js"
  exit 1
fi

if [ "${runner_version}" != "${browser_version}" ]; then
  echo "duckdb-wasm version mismatch:"
  echo "  runner/package.json: ${runner_version}"
  echo "  browser/main.js:    ${browser_version}"
  exit 1
fi

echo "duckdb-wasm versions are in sync: ${runner_version}"
