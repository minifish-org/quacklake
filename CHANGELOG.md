# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - 2026-02-26

### Added
- WASM-first query path with DuckDB-WASM runner and browser mode.
- Rust control-plane API (`/v1/run_sql`, `/v1/lineage/{job_id}`).
- Rust gateway with HTTP range support for parquet reads.
- MinIO-backed artifact storage and lineage recording.
- Budget/capability controls and structured error responses.
- Security hardening:
  - runner URL allow-list
  - optional API key / JWT auth
  - optional runner token auth with rotation
  - CORS origin allow-list
  - CI security audits

### Operations
- Production compose override: `docker-compose.production.yml`
- Production env template: `.env.production.example`
- E2E smoke scripts and coverage scripts.
