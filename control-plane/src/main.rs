use std::{collections::HashMap, env, net::SocketAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Credentials, primitives::ByteStream, Client};
use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::{DateTime, Utc};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use regex::Regex;
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::{Mutex, Semaphore},
    time::timeout,
};
use tracing::info;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    runner_url: String,
    gateway_base_url: String,
    s3_bucket: String,
    api_keys: Vec<String>,
    jwt_hs256_secret: Option<String>,
    default_budget: BudgetV1,
    read_prefixes: Vec<String>,
    write_prefixes: Vec<String>,
    uri_regex: Regex,
    object_store: Arc<dyn ObjectStore>,
    runner: Arc<dyn RunnerExecutor>,
    lineage: Arc<Mutex<HashMap<Uuid, LineageRecordV1>>>,
    job_limit: Arc<Semaphore>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunSqlRequestV1 {
    sql: String,
    output: OutputRequestV1,
    #[serde(default)]
    budget: Option<BudgetV1>,
    #[serde(default)]
    capabilities: Option<CapabilitiesV1>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutputRequestV1 {
    format: String,
    s3_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BudgetV1 {
    max_seconds: u64,
    max_scan_bytes: u64,
    max_output_bytes: u64,
    max_memory_mb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CapabilitiesV1 {
    read_prefixes: Vec<String>,
    write_prefixes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunSqlResponseV1 {
    job_id: Uuid,
    output: ArtifactV1,
    metrics: MetricsV1,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArtifactV1 {
    s3_uri: String,
    gateway_url: String,
    bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetricsV1 {
    elapsed_ms: u64,
    bytes_scanned: u64,
    peak_memory_mb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LineageRecordV1 {
    job_id: Uuid,
    created_at: DateTime<Utc>,
    sql: String,
    input_uris: Vec<String>,
    output: ArtifactV1,
    budget: BudgetV1,
    metrics: MetricsV1,
}

#[derive(Debug, Serialize)]
struct ErrorResponseV1 {
    error: ErrorBodyV1,
}

#[derive(Debug, Serialize)]
struct ErrorBodyV1 {
    code: &'static str,
    category: &'static str,
    message: String,
    measured: Option<MeasuredUsageV1>,
    suggestions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct MeasuredUsageV1 {
    budget: &'static str,
    actual: u64,
    limit: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RunnerRequest {
    sql: String,
    output_format: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RunnerResponse {
    artifact_base64: String,
    elapsed_ms: u64,
    bytes_scanned: u64,
    peak_memory_mb: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct JwtClaims {
    sub: Option<String>,
    exp: Option<usize>,
}

#[async_trait]
trait ObjectStore: Send + Sync {
    async fn head_object(&self, bucket: &str, key: &str) -> Result<u64, String>;
    async fn put_object(&self, bucket: &str, key: &str, bytes: Vec<u8>) -> Result<(), String>;
}

#[derive(Clone)]
struct S3ObjectStore {
    s3: Client,
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn head_object(&self, bucket: &str, key: &str) -> Result<u64, String> {
        let head = self
            .s3
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        Ok(head.content_length().unwrap_or(0).max(0) as u64)
    }

    async fn put_object(&self, bucket: &str, key: &str, bytes: Vec<u8>) -> Result<(), String> {
        self.s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .content_type("application/octet-stream")
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[async_trait]
trait RunnerExecutor: Send + Sync {
    async fn execute(
        &self,
        runner_url: &str,
        request: &RunnerRequest,
        max_seconds: u64,
    ) -> Result<RunnerResponse, ApiError>;
}

#[derive(Clone)]
struct HttpRunner {
    http: HttpClient,
    auth_tokens: Vec<String>,
}

#[async_trait]
impl RunnerExecutor for HttpRunner {
    async fn execute(
        &self,
        runner_url: &str,
        request: &RunnerRequest,
        max_seconds: u64,
    ) -> Result<RunnerResponse, ApiError> {
        let runner_fut = self
            .http
            .post(format!("{runner_url}/execute"))
            .json(request);
        let runner_fut = if let Some(token) = self.auth_tokens.first() {
            runner_fut.header("x-runner-token", token)
        } else {
            runner_fut
        }
        .send();

        let runner_resp = timeout(Duration::from_secs(max_seconds), runner_fut)
            .await
            .map_err(|_| {
                ApiError::BudgetExceeded(
                    "query exceeded max_seconds".to_string(),
                    MeasuredUsageV1 {
                        budget: "max_seconds",
                        actual: max_seconds,
                        limit: max_seconds,
                    },
                )
            })?
            .map_err(|e| ApiError::Runner(e.to_string()))?;

        if !runner_resp.status().is_success() {
            let body = runner_resp
                .text()
                .await
                .unwrap_or_else(|_| "runner failure".to_string());
            return Err(ApiError::Runner(body));
        }

        runner_resp
            .json()
            .await
            .map_err(|e| ApiError::Runner(e.to_string()))
    }
}

#[derive(Debug, Clone, Error)]
enum ApiError {
    #[error("invalid request: {0}")]
    BadRequest(String),
    #[error("capability denied: {0}")]
    CapabilityDenied(String),
    #[error("budget exceeded: {0}")]
    BudgetExceeded(String, MeasuredUsageV1),
    #[error("runner error: {0}")]
    Runner(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("too many requests: {0}")]
    TooManyRequests(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, body) = match self {
            ApiError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                ErrorBodyV1 {
                    code: "bad_request",
                    category: "validation",
                    message: msg,
                    measured: None,
                    suggestions: vec!["Check request schema and required fields".to_string()],
                },
            ),
            ApiError::CapabilityDenied(msg) => (
                StatusCode::FORBIDDEN,
                ErrorBodyV1 {
                    code: "capability_denied",
                    category: "auth",
                    message: msg,
                    measured: None,
                    suggestions: vec![
                        "Use an allowed read/write prefix for this workspace".to_string()
                    ],
                },
            ),
            ApiError::BudgetExceeded(msg, measured) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                ErrorBodyV1 {
                    code: "budget_exceeded",
                    category: "budget",
                    message: msg,
                    measured: Some(measured),
                    suggestions: vec![
                        "Add partition filters to reduce scanned files".to_string(),
                        "Select fewer columns to reduce scan bytes".to_string(),
                        "Add LIMIT for exploratory queries".to_string(),
                    ],
                },
            ),
            ApiError::Runner(msg) => (
                StatusCode::BAD_GATEWAY,
                ErrorBodyV1 {
                    code: "runner_error",
                    category: "runtime",
                    message: msg,
                    measured: None,
                    suggestions: vec!["Retry the query or simplify SQL complexity".to_string()],
                },
            ),
            ApiError::Storage(msg) => (
                StatusCode::BAD_GATEWAY,
                ErrorBodyV1 {
                    code: "storage_error",
                    category: "runtime",
                    message: msg,
                    measured: None,
                    suggestions: vec!["Verify MinIO availability and credentials".to_string()],
                },
            ),
            ApiError::Unauthorized(msg) => (
                StatusCode::UNAUTHORIZED,
                ErrorBodyV1 {
                    code: "unauthorized",
                    category: "auth",
                    message: msg,
                    measured: None,
                    suggestions: vec!["Include a valid x-api-key header".to_string()],
                },
            ),
            ApiError::TooManyRequests(msg) => (
                StatusCode::TOO_MANY_REQUESTS,
                ErrorBodyV1 {
                    code: "too_many_requests",
                    category: "runtime",
                    message: msg,
                    measured: None,
                    suggestions: vec![
                        "Retry shortly".to_string(),
                        "Reduce concurrent jobs or increase MAX_CONCURRENT_JOBS".to_string(),
                    ],
                },
            ),
        };

        (status, Json(ErrorResponseV1 { error: body })).into_response()
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter("info")
        .init();

    let api_port = env::var("API_PORT").unwrap_or_else(|_| "8000".to_string());
    let runner_url = env::var("RUNNER_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());
    let gateway_base_url =
        env::var("GATEWAY_BASE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());
    let s3_bucket = env::var("S3_BUCKET").unwrap_or_else(|_| "lakehouse".to_string());

    let endpoint = env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let region = env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let access_key = env::var("S3_ACCESS_KEY_ID").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key = env::var("S3_SECRET_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());

    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new(region))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new(
            access_key, secret_key, None, None, "static",
        ))
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
        .force_path_style(true)
        .build();

    let default_budget = BudgetV1 {
        max_seconds: parse_env_u64("DEFAULT_MAX_SECONDS", 20),
        max_scan_bytes: parse_env_u64("DEFAULT_MAX_SCAN_BYTES", 268_435_456),
        max_output_bytes: parse_env_u64("DEFAULT_MAX_OUTPUT_BYTES", 67_108_864),
        max_memory_mb: parse_env_u64("DEFAULT_MAX_MEMORY_MB", 512),
    };

    let api_keys = parse_prefixes("API_KEYS", "");
    let jwt_hs256_secret = env::var("JWT_HS256_SECRET")
        .ok()
        .filter(|v| !v.trim().is_empty());
    let runner_auth_tokens = {
        let fallback = env::var("RUNNER_AUTH_TOKEN").unwrap_or_default();
        parse_prefixes("RUNNER_AUTH_TOKENS", &fallback)
    };
    let app_env = env::var("APP_ENV").unwrap_or_else(|_| "development".to_string());
    validate_production_settings(
        &app_env,
        &api_keys,
        jwt_hs256_secret.as_deref(),
        &runner_auth_tokens,
    );

    let app_state = AppState {
        runner_url,
        gateway_base_url,
        s3_bucket,
        api_keys,
        jwt_hs256_secret,
        default_budget,
        read_prefixes: parse_prefixes("DEFAULT_READ_PREFIXES", "demo/,datasets/public/"),
        write_prefixes: parse_prefixes("DEFAULT_WRITE_PREFIXES", "agent/"),
        uri_regex: Regex::new(r#"s3://([a-zA-Z0-9._-]+)/([a-zA-Z0-9._\-/]+)"#).expect("regex"),
        object_store: Arc::new(S3ObjectStore {
            s3: Client::from_conf(s3_config),
        }),
        runner: Arc::new(HttpRunner {
            http: HttpClient::new(),
            auth_tokens: runner_auth_tokens,
        }),
        lineage: Arc::new(Mutex::new(HashMap::new())),
        job_limit: Arc::new(Semaphore::new(
            parse_env_u64("MAX_CONCURRENT_JOBS", 8) as usize
        )),
    };

    let app = build_router(app_state);

    let addr: SocketAddr = format!("0.0.0.0:{api_port}").parse().expect("addr");
    info!(%addr, "control-plane listening");
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    axum::serve(listener, app).await.expect("serve");
}

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/run_sql", post(run_sql_v1))
        .route("/v1/lineage/{job_id}", get(get_lineage_v1))
        .with_state(state)
}

async fn run_sql_v1(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RunSqlRequestV1>,
) -> Result<Json<RunSqlResponseV1>, ApiError> {
    ensure_request_auth(&state.api_keys, state.jwt_hs256_secret.as_deref(), &headers)?;
    let _permit = state
        .job_limit
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError::TooManyRequests("concurrency limit reached".to_string()))?;

    if req.output.format.to_lowercase() != "parquet" {
        return Err(ApiError::BadRequest(
            "only parquet output is supported in v1".to_string(),
        ));
    }

    let caps = req.capabilities.unwrap_or(CapabilitiesV1 {
        read_prefixes: state.read_prefixes.clone(),
        write_prefixes: state.write_prefixes.clone(),
    });

    ensure_prefix_allowed(&req.output.s3_key, &caps.write_prefixes)
        .map_err(|e| ApiError::CapabilityDenied(format!("output path denied: {e}")))?;

    let budget = merge_budget(req.budget.clone(), &state.default_budget);
    let input_uris = extract_s3_uris(&state.uri_regex, &req.sql);

    for uri in &input_uris {
        let (_, key) = split_s3_uri(uri)?;
        ensure_prefix_allowed(&key, &caps.read_prefixes)
            .map_err(|e| ApiError::CapabilityDenied(format!("input path denied: {e}")))?;
    }

    let mut estimated_scan_bytes = 0u64;
    for uri in &input_uris {
        let (bucket, key) = split_s3_uri(uri)?;
        let size = state
            .object_store
            .head_object(&bucket, &key)
            .await
            .map_err(ApiError::Storage)?;
        estimated_scan_bytes = estimated_scan_bytes.saturating_add(size);
    }

    if estimated_scan_bytes > budget.max_scan_bytes {
        return Err(ApiError::BudgetExceeded(
            "estimated scan bytes exceed budget".to_string(),
            MeasuredUsageV1 {
                budget: "max_scan_bytes",
                actual: estimated_scan_bytes,
                limit: budget.max_scan_bytes,
            },
        ));
    }

    let rewritten_sql =
        rewrite_sql_s3_to_gateway(&state.uri_regex, &state.gateway_base_url, &req.sql);
    let runner_req = RunnerRequest {
        sql: rewritten_sql,
        output_format: "parquet".to_string(),
    };

    let runner_payload = state
        .runner
        .execute(&state.runner_url, &runner_req, budget.max_seconds)
        .await?;

    if runner_payload.bytes_scanned > budget.max_scan_bytes {
        return Err(ApiError::BudgetExceeded(
            "measured scan bytes exceed budget".to_string(),
            MeasuredUsageV1 {
                budget: "max_scan_bytes",
                actual: runner_payload.bytes_scanned,
                limit: budget.max_scan_bytes,
            },
        ));
    }

    if runner_payload.peak_memory_mb > budget.max_memory_mb {
        return Err(ApiError::BudgetExceeded(
            "peak memory exceeds budget".to_string(),
            MeasuredUsageV1 {
                budget: "max_memory_mb",
                actual: runner_payload.peak_memory_mb,
                limit: budget.max_memory_mb,
            },
        ));
    }

    let artifact_bytes = BASE64
        .decode(runner_payload.artifact_base64)
        .map_err(|e| ApiError::Runner(e.to_string()))?;

    if artifact_bytes.len() as u64 > budget.max_output_bytes {
        return Err(ApiError::BudgetExceeded(
            "artifact output exceeds budget".to_string(),
            MeasuredUsageV1 {
                budget: "max_output_bytes",
                actual: artifact_bytes.len() as u64,
                limit: budget.max_output_bytes,
            },
        ));
    }

    state
        .object_store
        .put_object(&state.s3_bucket, &req.output.s3_key, artifact_bytes.clone())
        .await
        .map_err(ApiError::Storage)?;

    let artifact = ArtifactV1 {
        s3_uri: format!("s3://{}/{}", state.s3_bucket, req.output.s3_key),
        gateway_url: format!(
            "{}/objects/{}/{}",
            state.gateway_base_url, state.s3_bucket, req.output.s3_key
        ),
        bytes: artifact_bytes.len() as u64,
    };

    let metrics = MetricsV1 {
        elapsed_ms: runner_payload.elapsed_ms,
        bytes_scanned: runner_payload.bytes_scanned,
        peak_memory_mb: runner_payload.peak_memory_mb,
    };

    let job_id = Uuid::new_v4();
    let lineage = LineageRecordV1 {
        job_id,
        created_at: Utc::now(),
        sql: req.sql,
        input_uris: input_uris.clone(),
        output: artifact.clone(),
        budget: budget.clone(),
        metrics: metrics.clone(),
    };

    state.lineage.lock().await.insert(job_id, lineage);

    info!(
        job_id = %job_id,
        input_uris = ?input_uris,
        output = %artifact.s3_uri,
        elapsed_ms = metrics.elapsed_ms,
        bytes_scanned = metrics.bytes_scanned,
        peak_memory_mb = metrics.peak_memory_mb,
        error_category = "none",
        "query executed"
    );

    Ok(Json(RunSqlResponseV1 {
        job_id,
        output: artifact,
        metrics,
    }))
}

async fn get_lineage_v1(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<Uuid>,
) -> Result<Json<LineageRecordV1>, ApiError> {
    ensure_request_auth(&state.api_keys, state.jwt_hs256_secret.as_deref(), &headers)?;

    let lineage = state.lineage.lock().await;
    let record = lineage
        .get(&job_id)
        .ok_or_else(|| ApiError::BadRequest("unknown job_id".to_string()))?;
    Ok(Json(record.clone()))
}

fn parse_env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_prefixes(name: &str, fallback: &str) -> Vec<String> {
    let raw = env::var(name).unwrap_or_else(|_| fallback.to_string());
    parse_prefix_list(&raw)
}

fn parse_prefix_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn merge_budget(user: Option<BudgetV1>, default: &BudgetV1) -> BudgetV1 {
    user.unwrap_or_else(|| default.clone())
}

fn ensure_prefix_allowed(key: &str, prefixes: &[String]) -> Result<(), String> {
    if prefixes.iter().any(|p| key.starts_with(p)) {
        return Ok(());
    }
    Err(format!("{key} not allowed by configured prefixes"))
}

fn extract_s3_uris(regex: &Regex, sql: &str) -> Vec<String> {
    regex
        .captures_iter(sql)
        .map(|cap| format!("s3://{}/{}", &cap[1], &cap[2]))
        .collect()
}

fn split_s3_uri(uri: &str) -> Result<(String, String), ApiError> {
    let without_scheme = uri
        .strip_prefix("s3://")
        .ok_or_else(|| ApiError::BadRequest(format!("invalid s3 uri: {uri}")))?;
    let mut parts = without_scheme.splitn(2, '/');
    let bucket = parts
        .next()
        .ok_or_else(|| ApiError::BadRequest(format!("invalid s3 uri: {uri}")))?;
    let key = parts
        .next()
        .ok_or_else(|| ApiError::BadRequest(format!("invalid s3 uri: {uri}")))?;
    Ok((bucket.to_string(), key.to_string()))
}

fn rewrite_sql_s3_to_gateway(uri_regex: &Regex, gateway_base_url: &str, sql: &str) -> String {
    uri_regex
        .replace_all(sql, |caps: &regex::Captures| {
            format!("{}/objects/{}/{}", gateway_base_url, &caps[1], &caps[2])
        })
        .to_string()
}

fn ensure_request_auth(
    api_keys: &[String],
    jwt_secret: Option<&str>,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    if let Some(secret) = jwt_secret {
        if let Some(token) = bearer_token(headers) {
            let mut validation = Validation::new(Algorithm::HS256);
            validation.validate_exp = true;
            let claims = decode::<JwtClaims>(
                token,
                &DecodingKey::from_secret(secret.as_bytes()),
                &validation,
            )
            .map_err(|_| ApiError::Unauthorized("invalid bearer token".to_string()))?;
            let _ = (&claims.claims.sub, &claims.claims.exp);
            return Ok(());
        }
    }

    if !api_keys.is_empty() {
        let Some(provided) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) else {
            return Err(ApiError::Unauthorized(
                "missing x-api-key header".to_string(),
            ));
        };

        if api_keys.iter().any(|k| k == provided) {
            return Ok(());
        }
        return Err(ApiError::Unauthorized("invalid x-api-key".to_string()));
    }

    if jwt_secret.is_some() {
        return Err(ApiError::Unauthorized(
            "missing Authorization bearer token".to_string(),
        ));
    }

    Ok(())
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let auth = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    auth.strip_prefix("Bearer ")
}

fn validate_production_settings(
    app_env: &str,
    api_keys: &[String],
    jwt_secret: Option<&str>,
    runner_auth_tokens: &[String],
) {
    if app_env != "production" {
        return;
    }
    if api_keys.is_empty() && jwt_secret.is_none() {
        panic!("production mode requires API_KEYS or JWT_HS256_SECRET");
    }
    if runner_auth_tokens.is_empty() {
        panic!("production mode requires RUNNER_AUTH_TOKENS or RUNNER_AUTH_TOKEN");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode as HttpStatusCode};
    use http_body_util::BodyExt;
    use serde_json::json;
    use tower::util::ServiceExt;

    type PutObjects = Arc<Mutex<Vec<(String, String, Vec<u8>)>>>;

    #[derive(Clone, Default)]
    struct MockObjectStore {
        heads: Arc<Mutex<HashMap<(String, String), u64>>>,
        puts: PutObjects,
        fail_put: Arc<Mutex<Option<String>>>,
    }

    #[async_trait]
    impl ObjectStore for MockObjectStore {
        async fn head_object(&self, bucket: &str, key: &str) -> Result<u64, String> {
            let heads = self.heads.lock().await;
            heads
                .get(&(bucket.to_string(), key.to_string()))
                .copied()
                .ok_or_else(|| "missing mock head object".to_string())
        }

        async fn put_object(&self, bucket: &str, key: &str, bytes: Vec<u8>) -> Result<(), String> {
            if let Some(err) = self.fail_put.lock().await.clone() {
                return Err(err);
            }
            self.puts
                .lock()
                .await
                .push((bucket.to_string(), key.to_string(), bytes));
            Ok(())
        }
    }

    #[derive(Clone)]
    struct MockRunner {
        result: Arc<Mutex<Result<RunnerResponse, ApiError>>>,
    }

    #[async_trait]
    impl RunnerExecutor for MockRunner {
        async fn execute(
            &self,
            _runner_url: &str,
            _request: &RunnerRequest,
            _max_seconds: u64,
        ) -> Result<RunnerResponse, ApiError> {
            self.result.lock().await.clone()
        }
    }

    fn test_state(
        store: Arc<dyn ObjectStore>,
        runner: Arc<dyn RunnerExecutor>,
        lineage: Arc<Mutex<HashMap<Uuid, LineageRecordV1>>>,
        api_keys: Vec<String>,
    ) -> AppState {
        AppState {
            runner_url: "http://runner:3000".to_string(),
            gateway_base_url: "http://gateway:8080".to_string(),
            s3_bucket: "lakehouse".to_string(),
            api_keys,
            jwt_hs256_secret: None,
            default_budget: BudgetV1 {
                max_seconds: 20,
                max_scan_bytes: 268_435_456,
                max_output_bytes: 67_108_864,
                max_memory_mb: 512,
            },
            read_prefixes: vec!["demo/".to_string()],
            write_prefixes: vec!["agent/".to_string()],
            uri_regex: Regex::new(r#"s3://([a-zA-Z0-9._-]+)/([a-zA-Z0-9._\-/]+)"#).unwrap(),
            object_store: store,
            runner,
            lineage,
            job_limit: Arc::new(Semaphore::new(8)),
        }
    }

    #[test]
    fn allowed_prefix_is_enforced() {
        assert!(ensure_prefix_allowed("demo/a.parquet", &["demo/".to_string()]).is_ok());
        assert!(ensure_prefix_allowed("private/a.parquet", &["demo/".to_string()]).is_err());
    }

    #[test]
    fn rewrite_sql_replaces_s3_uris() {
        let uri_regex = Regex::new(r#"s3://([a-zA-Z0-9._-]+)/([a-zA-Z0-9._\-/]+)"#).unwrap();
        let sql = "select * from read_parquet(\"s3://lakehouse/demo/events.parquet\")";
        let out = rewrite_sql_s3_to_gateway(&uri_regex, "http://gateway:8080", sql);
        assert!(out.contains("http://gateway:8080/objects/lakehouse/demo/events.parquet"));
    }

    #[tokio::test]
    async fn capability_denied_error_shape_is_stable() {
        let resp = ApiError::CapabilityDenied("denied".to_string()).into_response();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
        assert_eq!(json["error"]["code"], "capability_denied");
        assert_eq!(json["error"]["category"], "auth");
        assert_eq!(json["error"]["message"], "denied");
        assert!(json["error"]["measured"].is_null());
    }

    #[tokio::test]
    async fn too_many_requests_error_shape_is_stable() {
        let resp = ApiError::TooManyRequests("busy".to_string()).into_response();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
        assert_eq!(json["error"]["code"], "too_many_requests");
        assert_eq!(json["error"]["message"], "busy");
    }

    #[test]
    fn production_validation_requires_auth_and_runner_token() {
        let no_panic = std::panic::catch_unwind(|| {
            validate_production_settings(
                "production",
                &["k".to_string()],
                None,
                &["r".to_string()],
            );
        });
        assert!(no_panic.is_ok());

        let missing = std::panic::catch_unwind(|| {
            validate_production_settings("production", &[], None, &[]);
        });
        assert!(missing.is_err());
    }

    #[tokio::test]
    async fn budget_exceeded_error_includes_measured_usage() {
        let measured = MeasuredUsageV1 {
            budget: "max_scan_bytes",
            actual: 2048,
            limit: 1024,
        };
        let resp = ApiError::BudgetExceeded("too much scan".to_string(), measured).into_response();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
        assert_eq!(json["error"]["code"], "budget_exceeded");
        assert_eq!(json["error"]["category"], "budget");
        assert_eq!(json["error"]["message"], "too much scan");
        assert_eq!(json["error"]["measured"]["budget"], "max_scan_bytes");
        assert_eq!(json["error"]["measured"]["actual"], 2048);
        assert_eq!(json["error"]["measured"]["limit"], 1024);
    }

    #[test]
    fn split_s3_uri_rejects_invalid_input() {
        let err = split_s3_uri("http://not-s3/path").expect_err("expected error");
        assert!(format!("{err}").contains("invalid s3 uri"));
    }

    #[test]
    fn split_s3_uri_accepts_valid_input() {
        let (bucket, key) =
            split_s3_uri("s3://lakehouse/demo/events.parquet").expect("valid s3 uri");
        assert_eq!(bucket, "lakehouse");
        assert_eq!(key, "demo/events.parquet");
    }

    #[test]
    fn parse_prefix_list_drops_empty_entries() {
        let parsed = parse_prefix_list("demo/, datasets/public/ , ,agent/ws1/");
        assert_eq!(parsed, vec!["demo/", "datasets/public/", "agent/ws1/"]);
    }

    #[test]
    fn merge_budget_prefers_user_budget() {
        let default = BudgetV1 {
            max_seconds: 20,
            max_scan_bytes: 100,
            max_output_bytes: 100,
            max_memory_mb: 128,
        };
        let user = BudgetV1 {
            max_seconds: 5,
            max_scan_bytes: 10,
            max_output_bytes: 11,
            max_memory_mb: 12,
        };
        let merged = merge_budget(Some(user.clone()), &default);
        assert_eq!(merged.max_seconds, user.max_seconds);
        assert_eq!(merged.max_scan_bytes, user.max_scan_bytes);
        assert_eq!(merged.max_output_bytes, user.max_output_bytes);
        assert_eq!(merged.max_memory_mb, user.max_memory_mb);
    }

    #[test]
    fn merge_budget_falls_back_to_default() {
        let default = BudgetV1 {
            max_seconds: 20,
            max_scan_bytes: 100,
            max_output_bytes: 101,
            max_memory_mb: 128,
        };
        let merged = merge_budget(None, &default);
        assert_eq!(merged.max_seconds, default.max_seconds);
        assert_eq!(merged.max_scan_bytes, default.max_scan_bytes);
        assert_eq!(merged.max_output_bytes, default.max_output_bytes);
        assert_eq!(merged.max_memory_mb, default.max_memory_mb);
    }

    #[test]
    fn extract_s3_uris_finds_multiple_entries() {
        let uri_regex = Regex::new(r#"s3://([a-zA-Z0-9._-]+)/([a-zA-Z0-9._\-/]+)"#).unwrap();
        let sql = r#"select * from read_parquet("s3://lakehouse/demo/a.parquet")
            union all
            select * from read_parquet("s3://datasets/public/b.parquet")"#;
        let uris = extract_s3_uris(&uri_regex, sql);
        assert_eq!(
            uris,
            vec![
                "s3://lakehouse/demo/a.parquet",
                "s3://datasets/public/b.parquet"
            ]
        );
    }

    #[tokio::test]
    async fn run_sql_success_persists_lineage_and_writes_artifact() {
        let store = MockObjectStore::default();
        store.heads.lock().await.insert(
            ("lakehouse".to_string(), "demo/events.parquet".to_string()),
            349,
        );
        let store = Arc::new(store);
        let runner = Arc::new(MockRunner {
            result: Arc::new(Mutex::new(Ok(RunnerResponse {
                artifact_base64: BASE64.encode(b"parquet-bytes"),
                elapsed_ms: 12,
                bytes_scanned: 349,
                peak_memory_mb: 64,
            }))),
        });
        let lineage = Arc::new(Mutex::new(HashMap::new()));
        let app = build_router(test_state(store.clone(), runner, lineage.clone(), vec![]));

        let req_body = json!({
            "sql": "select count(*) as n from read_parquet(\"s3://lakehouse/demo/events.parquet\")",
            "output": { "format": "parquet", "s3_key": "agent/ws1/results/out.parquet" }
        });
        let response = app
            .oneshot(
                Request::post("/v1/run_sql")
                    .header("content-type", "application/json")
                    .body(Body::from(req_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("run_sql response");

        assert_eq!(response.status(), HttpStatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let job_id = parsed["job_id"].as_str().unwrap().to_string();
        assert!(parsed["output"]["s3_uri"]
            .as_str()
            .unwrap()
            .ends_with("agent/ws1/results/out.parquet"));

        let puts = store.puts.lock().await;
        assert_eq!(puts.len(), 1);
        assert_eq!(puts[0].0, "lakehouse");
        assert_eq!(puts[0].1, "agent/ws1/results/out.parquet");
        assert_eq!(puts[0].2, b"parquet-bytes");

        let lineage_map = lineage.lock().await;
        let id = Uuid::parse_str(&job_id).unwrap();
        assert!(lineage_map.contains_key(&id));
    }

    #[tokio::test]
    async fn run_sql_runner_timeout_returns_budget_error() {
        let store = MockObjectStore::default();
        store.heads.lock().await.insert(
            ("lakehouse".to_string(), "demo/events.parquet".to_string()),
            349,
        );
        let store = Arc::new(store);
        let runner = Arc::new(MockRunner {
            result: Arc::new(Mutex::new(Err(ApiError::BudgetExceeded(
                "query exceeded max_seconds".to_string(),
                MeasuredUsageV1 {
                    budget: "max_seconds",
                    actual: 1,
                    limit: 1,
                },
            )))),
        });
        let app = build_router(test_state(
            store,
            runner,
            Arc::new(Mutex::new(HashMap::new())),
            vec![],
        ));

        let req_body = json!({
            "sql": "select * from read_parquet(\"s3://lakehouse/demo/events.parquet\")",
            "output": { "format": "parquet", "s3_key": "agent/ws1/results/out.parquet" },
            "budget": { "max_seconds": 1, "max_scan_bytes": 99999, "max_output_bytes": 99999, "max_memory_mb": 128 }
        });
        let response = app
            .oneshot(
                Request::post("/v1/run_sql")
                    .header("content-type", "application/json")
                    .body(Body::from(req_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("run_sql response");

        assert_eq!(response.status(), HttpStatusCode::UNPROCESSABLE_ENTITY);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["error"]["code"], "budget_exceeded");
        assert_eq!(parsed["error"]["measured"]["budget"], "max_seconds");
    }

    #[tokio::test]
    async fn run_sql_storage_write_failure_returns_storage_error() {
        let store = MockObjectStore::default();
        store.heads.lock().await.insert(
            ("lakehouse".to_string(), "demo/events.parquet".to_string()),
            349,
        );
        *store.fail_put.lock().await = Some("disk full".to_string());
        let store = Arc::new(store);
        let runner = Arc::new(MockRunner {
            result: Arc::new(Mutex::new(Ok(RunnerResponse {
                artifact_base64: BASE64.encode(b"result"),
                elapsed_ms: 10,
                bytes_scanned: 100,
                peak_memory_mb: 10,
            }))),
        });
        let app = build_router(test_state(
            store,
            runner,
            Arc::new(Mutex::new(HashMap::new())),
            vec![],
        ));

        let req_body = json!({
            "sql": "select * from read_parquet(\"s3://lakehouse/demo/events.parquet\")",
            "output": { "format": "parquet", "s3_key": "agent/ws1/results/out.parquet" }
        });
        let response = app
            .oneshot(
                Request::post("/v1/run_sql")
                    .header("content-type", "application/json")
                    .body(Body::from(req_body.to_string()))
                    .unwrap(),
            )
            .await
            .expect("run_sql response");

        assert_eq!(response.status(), HttpStatusCode::BAD_GATEWAY);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["error"]["code"], "storage_error");
        assert_eq!(parsed["error"]["message"], "disk full");
    }

    #[tokio::test]
    async fn run_sql_requires_api_key_when_configured() {
        let store = MockObjectStore::default();
        store.heads.lock().await.insert(
            ("lakehouse".to_string(), "demo/events.parquet".to_string()),
            349,
        );
        let store = Arc::new(store);
        let runner = Arc::new(MockRunner {
            result: Arc::new(Mutex::new(Ok(RunnerResponse {
                artifact_base64: BASE64.encode(b"result"),
                elapsed_ms: 10,
                bytes_scanned: 100,
                peak_memory_mb: 10,
            }))),
        });
        let app = build_router(test_state(
            store,
            runner,
            Arc::new(Mutex::new(HashMap::new())),
            vec!["secret-key".to_string()],
        ));

        let req_body = json!({
            "sql": "select * from read_parquet(\"s3://lakehouse/demo/events.parquet\")",
            "output": { "format": "parquet", "s3_key": "agent/ws1/results/out.parquet" }
        });
        let response = app
            .oneshot(
                Request::post("/v1/run_sql")
                    .header("content-type", "application/json")
                    .body(Body::from(req_body.to_string()))
                    .expect("valid request"),
            )
            .await
            .expect("run_sql response");

        assert_eq!(response.status(), HttpStatusCode::UNAUTHORIZED);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["error"]["code"], "unauthorized");
    }

    #[test]
    fn ensure_request_auth_accepts_valid_bearer_token() {
        #[derive(Serialize)]
        struct TestClaims {
            sub: &'static str,
            exp: usize,
        }

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(Algorithm::HS256),
            &TestClaims {
                sub: "user-1",
                exp: 4_102_444_800, // year 2100
            },
            &jsonwebtoken::EncodingKey::from_secret(b"secret"),
        )
        .expect("encode jwt");

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().expect("auth header"),
        );

        let out = ensure_request_auth(&[], Some("secret"), &headers);
        assert!(out.is_ok());
    }
}
