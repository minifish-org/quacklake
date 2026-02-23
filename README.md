# ducklake-agent

Single-machine Docker Compose prototype for a WASM-first analytics Agent DB.

## Services

- `api` (`:8000`): Rust Axum control plane (`POST /v1/run_sql`, `GET /v1/lineage/{job_id}`)
- `gateway` (`:8080`): Rust HTTP Range proxy over MinIO
- `runner` (`:3000`): Node + DuckDB WASM execution (`POST /execute`)
- `minio` (`:9000`, console `:9001`): S3-compatible object store

## Quickstart

```bash
docker compose up --build
./scripts/seed.sh
```

## Smoke Test

```bash
./scripts/smoke.sh
```

Run a query:

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

Fetch lineage:

```bash
curl -sS http://localhost:8000/v1/lineage/<job_id> | jq .
```

## Behavior

- API enforces default budgets and prefix-based capabilities.
- API rewrites `s3://...` SQL references to gateway HTTP URLs before runner execution.
- Gateway supports `HEAD`, full object reads, and byte-range reads (`206` + `Content-Range`).
- Runner is stateless and executes SQL with DuckDB WASM, exporting result artifacts as parquet bytes.
