# ducklake-agent

Single-machine Docker Compose prototype for a WASM-first analytics Agent DB.

## Services

- `api` (`:8000`): Rust Axum control plane (`POST /v1/run_sql`, `GET /v1/lineage/{job_id}`)
- `gateway` (`:8080`): Rust HTTP Range proxy over MinIO (CORS enabled for browser reads)
- `runner` (`:3000`): Node + DuckDB WASM execution (`POST /execute`)
- `browser` (`:8081`): Browser DuckDB-WASM playground (reads parquet via gateway)
- `minio` (`:9000`, console `:9001`): S3-compatible object store

## Quickstart

```bash
docker compose up --build
./scripts/seed.sh
```

## Browser WASM mode

Open [http://localhost:8081](http://localhost:8081), keep the default parquet URL, and run the query. DuckDB-WASM runs in the browser and reads parquet over the gateway.

## Smoke Test

```bash
./scripts/smoke.sh
```

Run a policy-governed API query:

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

## Homelab split deployment

You can deploy storage/control and compute separately:

- Storage side: `minio` + `gateway`
- Control side: `api`
- Compute side: `runner` (optional for governed materialization)
- Browser analytics: `browser` app or your own frontend using DuckDB-WASM

For browser reads, expose gateway over HTTPS and allow CORS with `Range`, `Content-Range`, and `Content-Length` headers.
