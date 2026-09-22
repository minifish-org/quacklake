# Open-source readiness — 2026-09-22

Quacklake is an experimental project, not a production service guarantee.

Verified for this publication:

- Full fetched Git history and staged changes scanned with Gitleaks; no findings.
- Rust formatting and Clippy with warnings denied; 21 workspace tests pass.
- Runner: 10 Node tests pass.
- Cargo audit and runner npm audit report no known vulnerabilities at review time.
- All Docker images build with Rust 1.94.1 and Node 22.
- Local Compose SQL execution, Parquet result artifacts, lineage, HEAD and byte Range requests pass.
- Browser DuckDB-WASM query returns the three expected event groups.
- Production Compose override removes runner and MinIO host ports.

Not verified: internet-facing deployment, hostile SQL isolation, high load, disaster recovery, or a production security assessment. Demo storage uses public sample credentials; replace them and design isolation before deploying. MinIO upstream is archived; its pinned image is a local demonstration dependency, not a maintenance commitment.

Source is AGPL-3.0-only. Dependencies, images and downloaded DuckDB components retain their respective upstream licenses. No model weights or private datasets are included.
