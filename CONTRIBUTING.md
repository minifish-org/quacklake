# Contributing

Thanks for contributing to `Quacklake`.

## Prerequisites

- Docker + Docker Compose
- Rust 1.94.1 toolchain (see rust-toolchain.toml)
- Node.js 22+
- `jq`

## Local development

```bash
docker compose up --build
./scripts/seed.sh
./scripts/smoke.sh
```

## Required checks before opening a PR

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace --all-features

cd runner
npm ci
npm test
cd ..

./scripts/check_duckdb_wasm_version.sh
./scripts/test_gateway_range.sh
./scripts/browser_smoke.sh
```

## Commit and PR guidelines

- Keep PRs focused and small.
- Add tests for behavior changes.
- Update docs for endpoint or config changes.
- Keep public APIs and error shapes backward-compatible where possible.

## Project conventions

- Rust code must avoid panics in request paths.
- Budgets and capability checks are required behavior.
- Runner remains stateless and policy-free.

See `AGENTS.md` for architectural intent and boundaries.
