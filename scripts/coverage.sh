#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "cargo-llvm-cov is required. Install with:"
  echo "cargo install cargo-llvm-cov"
  exit 1
fi

if ! command -v npm >/dev/null 2>&1; then
  echo "npm is required"
  exit 1
fi

mkdir -p coverage

echo "[1/3] Rust coverage"
cargo llvm-cov --workspace --all-features --lcov --output-path coverage/rust.lcov

echo "[2/3] Node coverage"
(
  cd runner
  npm ci
  npm run test:coverage
)

if [ -f runner/coverage/lcov.info ]; then
  cp runner/coverage/lcov.info coverage/runner.lcov
fi

echo "[3/3] Coverage artifacts"
ls -la coverage
