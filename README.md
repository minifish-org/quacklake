# Quacklake

WASM-first analytics prototype with:
- DuckDB-WASM compute
- MinIO object storage
- Rust API and range gateway

## Project status

- Stage: prototype / pre-1.0
- Target: single-machine Docker Compose, plus split homelab deployment

## Components

- `api` (`:8000`): Rust Axum control plane (`POST /v1/run_sql`, `GET /v1/lineage/{job_id}`)
- `gateway` (`:8080`): Rust HTTP Range proxy over MinIO
- `runner` (`:3000`): Node service using DuckDB-WASM
- `browser` (`:8081`): Browser DuckDB-WASM query UI
- `minio` (`:9000`, console `:9001`): S3-compatible storage

## Requirements and safety

Requires Docker with Compose v2.24.4+, curl and jq. Rust 1.94.1 and Node.js 22 are needed only
for native development. This is an experimental analytics prototype, not a
production database or a security boundary for hostile SQL. The development
profile uses public sample data and demonstration credentials, binds host ports
to loopback, and must not be exposed directly to the internet.

The demo pins upstream MinIO and mc images from Quay by digest. The MinIO
community repository is archived; this dependency is suitable here for a local
prototype, not a maintained production-storage recommendation.

## Quickstart

```bash
docker compose up --build
./scripts/seed.sh
./scripts/smoke.sh
```

Then open [http://localhost:8081](http://localhost:8081).

## Easy mode (recommended)

The helper CLI retains its original `ducklake` command name:


```bash
./scripts/ducklake doctor
./scripts/ducklake quickstart --yes --build
./scripts/ducklake up --build
./scripts/ducklake seed
./scripts/ducklake query
./scripts/ducklake browser
```

Or with `make`:

```bash
make doctor
make up-build
make seed
make query
```

Shell completion:

```bash
# bash
source ./scripts/completion/ducklake.bash

# zsh
source ./scripts/completion/ducklake.zsh
```

## API example

```bash
curl -sS -X POST http://localhost:8000/v1/run_sql \
  -H 'content-type: application/json' \
  -d '{
    "sql": "select count(*) as n from read_parquet(\"s3://lakehouse/demo/events.parquet\")",
    "output": {
      "format": "parquet",
      "s3_key": "agent/ws1/results/count.parquet"
    },
    "budget": {
      "max_seconds": 20,
      "max_scan_bytes": 268435456,
      "max_output_bytes": 67108864,
      "max_memory_mb": 512
    }
  }' | jq .
```

## Scripts

- smoke: `./scripts/smoke.sh`
- full matrix: `./scripts/full_validation.sh`
- gateway range e2e: `./scripts/test_gateway_range.sh`
- browser smoke e2e: `./scripts/browser_smoke.sh`
- rust + node coverage: `./scripts/coverage.sh`
- DuckDB-WASM version sync: `./scripts/check_duckdb_wasm_version.sh`

## Configuration highlights

- Network trust model (default): run inside Tailscale/private network; app auth is optional.
- Runner input guardrail: `RUNNER_MAX_INPUT_BYTES` (default `268435456`)
- Runner remote URL allow-list: `RUNNER_ALLOWED_URL_PREFIXES`
  - default: `http://gateway:8080/objects/,http://localhost:8080/objects/,http://127.0.0.1:8080/objects/`
- Runner trust token(s): `RUNNER_AUTH_TOKEN` or `RUNNER_AUTH_TOKENS` (comma-separated; for rotation)
- API auth:
  - API key mode: `API_KEYS` (comma-separated), requests send `x-api-key`
  - JWT mode: `JWT_HS256_SECRET`, requests send `Authorization: Bearer <token>`
- Gateway CORS allow-list: `CORS_ALLOWED_ORIGINS` (default `http://localhost:8081`)

## Production profile

Create production env file from template:

```bash
cp .env.production.example .env.production
```

Then run with the production override:

```bash
./scripts/preflight_production.sh .env.production
docker compose --env-file .env.production \
  -f docker-compose.yml -f docker-compose.production.yml \
  up -d --build
```

Minimum production settings:

1. `RUNNER_AUTH_TOKENS` set on both `api` and `runner`
2. `API_KEYS` or `JWT_HS256_SECRET` set on `api` (at least one)
3. `CORS_ALLOWED_ORIGINS` set to your real browser origin(s)
4. Tailnet-only exposure for service ports
5. Rotate tokens/keys regularly

## Network exposure

- Preferred mode: Tailscale-only network access.
- The production override is a starting template, not a production-readiness guarantee.
- Before any broader network exposure:
  - enable API auth (`API_KEYS` or `JWT_HS256_SECRET`)
  - keep `RUNNER_AUTH_TOKENS` enabled
  - set strict `CORS_ALLOWED_ORIGINS` (never `*`)
  - terminate TLS at ingress/proxy
  - monitor `cargo audit` and `npm audit` results from CI

## DuckDB-WASM pins

- `runner/package.json`
- `browser/main.js` (`DUCKDB_WASM_VERSION`)

## Open source

- License: [AGPL-3.0-only](LICENSE)
- Contributing guide: `CONTRIBUTING.md`
- Security policy: `SECURITY.md`
- Code of conduct: `CODE_OF_CONDUCT.md`

## Maintainers

For roadmap and design context, see `ARCHITECTURE.md` and `AGENTS.md`.
