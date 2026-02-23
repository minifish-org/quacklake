# Architecture

## Boundaries

- Control plane (`control-plane`): policy, budgets, lineage, artifact registry.
- Storage gateway (`gateway`): MinIO object access with HTTP range behavior for parquet scans.
- Runner (`runner`): compute execution surface called by API; stateless and policy-free.

## v1 flow

1. Client calls `POST /v1/run_sql`.
2. API resolves effective budget and capabilities.
3. API extracts `s3://` URIs from SQL and validates allowed prefixes.
4. API estimates scan bytes with MinIO `HeadObject` checks.
5. API rewrites SQL URIs to `http://gateway:8080/objects/<bucket>/<key>`.
6. API calls runner (`POST /execute`) with rewritten SQL.
7. API enforces measured budgets from runner response.
8. API uploads artifact bytes to MinIO and returns S3 + gateway URLs.
9. API records lineage and emits structured logs.

## Security defaults

- Prefix allow-list capability model for read/write paths.
- No S3 credentials are sent to runner.
- Gateway/API mediate all external object access.
