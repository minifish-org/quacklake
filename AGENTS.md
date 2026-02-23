# AGENTS.md

This repo targets a **single-machine Docker Compose** prototype of a **WASM-first AP (analytics) “Agent DB”**:

- **Compute:** DuckDB compiled to WASM (executed in a server-side JS runtime)
- **Storage:** MinIO (S3-compatible object storage)
- **Data layout:** Parquet-first; Iceberg metadata supported progressively (start with artifacts + lineage, then add catalog/commit)
- **Control plane:** Rust (Axum) API for agent-facing tasks, budgets, and audit
- **Gateway:** Rust HTTP Range proxy to make MinIO objects efficiently readable by DuckDB WASM

Repo name (suggested): **`ducklake-agent`**

---

## What “good” looks like

This system should make it easy for an AI agent (or human tool) to:

1. **Run SQL** against object storage data.
2. **Materialize outputs** back to MinIO as Parquet artifacts.
3. Enforce **budgets** (time, memory, scan bytes) and **capabilities** (what data can be read/written).
4. Record **lineage**: inputs + SQL + snapshot/versions + output artifacts.

Non-goals for the prototype:

- OLTP correctness / high-concurrency transactions
- Fully distributed joins
- Building a new storage engine

---

## Agents

Treat “agents” here as clearly scoped roles that can operate independently.

### Agent: Architect

- Owns end-to-end architecture and interfaces.
- Maintains `ARCHITECTURE.md` and all RFCs.
- Decides what is native vs WASM vs service boundaries.

### Agent: Data Plane (WASM Runner)

- Owns the DuckDB WASM runner (Node service).
- Ensures query execution + result export are correct and measurable.
- Keeps the runner thin (no business logic).

### Agent: Storage Gateway

- Owns MinIO connectivity, HTTP Range correctness, caching hooks.
- Ensures Parquet scans are efficient (Range, HEAD, Content-Range).

### Agent: Control Plane (API)

- Owns Axum API, job lifecycle, budgets, auth/capabilities, artifact registry.
- Ensures clear errors for agents (e.g., “scan bytes exceeded” with suggestions).

### Agent: Security & Policy

- Owns capability model, sandbox restrictions, audit logs.
- Establishes defaults: **no network** for extensions, explicit allow-lists, budget-by-default.

---

## High-level flow

1. Client calls **API**: `POST /v1/run_sql`
2. API validates:
   - capabilities (what S3 prefixes are readable/writable)
   - budgets (max_seconds, max_scan_bytes, max_output_bytes)
3. API rewrites inputs:
   - `s3://lakehouse/...` → `http://gateway:8080/objects/lakehouse/...`
4. API calls **Runner**: execute SQL in DuckDB WASM
5. Runner produces an output Parquet artifact
6. API uploads artifact to MinIO and returns:
   - `s3://...` + gateway HTTP URL
7. API records lineage + metrics

---

## Local dev quickstart (Docker Compose)

### 1) Bring everything up

```bash
docker compose up --build
```

### 2) Seed demo data (if repo includes scripts)

```bash
./scripts/seed.sh
```

### 3) Run a query (example)

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
      "max_scan_bytes": 268435456
    }
  }' | jq .
```

---

## Repo conventions

### Rust

- Format: `cargo fmt`
- Lint: `cargo clippy --all-targets --all-features -D warnings`
- Tests: `cargo test`

Guidelines:

- Prefer explicit error types (`thiserror`) and structured logs.
- All public API structs must be versioned (`v1`) and documented.
- No panics in request path; treat panics as bugs.

### Runner (Node)

- Keep dependencies minimal.
- Runner must be stateless: no storing credentials or policy.
- All SQL comes from API; runner never accepts raw S3 credentials.

### HTTP Range correctness

Gateway must:

- Support `Range: bytes=a-b` and return `206 Partial Content`.
- Include `Accept-Ranges: bytes`, `Content-Range`, correct `Content-Length`.
- Support `HEAD` for size discovery.

This is essential for Parquet performance.

---

## Budgets and guardrails

Budgets are not optional; default budgets apply even if the caller omits them.

Minimum enforced budgets:

- `max_seconds` (wall time)
- `max_scan_bytes` (estimated or measured)
- `max_output_bytes` (artifact limit)
- `max_memory_mb` (runner/instance limit; best-effort in prototype)

When budget is exceeded:

- Return a **structured error** with:
  - budget violated
  - measured usage
  - suggested mitigations (add partition filter, column pruning, limit)

---

## Capability model (prototype)

Capabilities are prefix-based:

- Read allow-list: e.g. `demo/`, `datasets/public/`
- Write allow-list: e.g. `agent/<workspace_id>/`

API resolves S3 URIs to gateway URLs only if allowed.
Runner only ever sees gateway HTTP URLs.

---

## Iceberg support plan

Start:

- Parquet artifacts + lineage registry
- Optional: read Iceberg metadata to expand to Parquet file list

Next:

- Introduce a minimal catalog:
  - table name → current metadata pointer
- Add commit service (can be native/JVM or dedicated Rust impl later)

Avoid:

- Implementing full Iceberg commit protocol in week 1

---

## Logging & observability

Every query must emit:

- job_id
- input URIs (sanitized)
- output artifact
- elapsed_ms
- bytes_scanned (best-effort)
- peak_memory_mb (best-effort)
- error category (budget/auth/runtime)

Prefer structured JSON logs.

---

## Security defaults

- No arbitrary outbound network from extensions.
- No filesystem access outside temp directories.
- Never log secrets (S3 keys, tokens).
- All external access goes through the gateway/API.

---

## PR checklist

- [ ] Tests pass locally
- [ ] `cargo fmt` and `cargo clippy` clean
- [ ] Added/updated docs for new endpoints
- [ ] Budget/capability behavior has tests
- [ ] Error responses are structured and stable

---

## Glossary

- **Artifact**: A persisted output (usually Parquet) written to MinIO.
- **Gateway**: HTTP Range proxy over MinIO objects.
- **Runner**: DuckDB WASM execution service.
- **Catalog**: Mapping from logical table name to Iceberg metadata pointer.
- **Lineage**: Record of inputs + code + versions → outputs.
