use std::{collections::HashMap, env, net::SocketAddr, sync::Arc, time::Duration};

use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Credentials, primitives::ByteStream, Client};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::{DateTime, Utc};
use regex::Regex;
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{sync::Mutex, time::timeout};
use tracing::info;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    runner_url: String,
    gateway_base_url: String,
    s3_bucket: String,
    default_budget: BudgetV1,
    read_prefixes: Vec<String>,
    write_prefixes: Vec<String>,
    uri_regex: Regex,
    s3: Client,
    http: HttpClient,
    lineage: Arc<Mutex<HashMap<Uuid, LineageRecordV1>>>,
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

#[derive(Debug, Serialize)]
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

#[derive(Debug, Deserialize)]
struct RunnerResponse {
    artifact_base64: String,
    elapsed_ms: u64,
    bytes_scanned: u64,
    peak_memory_mb: u64,
}

#[derive(Debug, Error)]
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

    let app_state = AppState {
        runner_url,
        gateway_base_url,
        s3_bucket,
        default_budget,
        read_prefixes: parse_prefixes("DEFAULT_READ_PREFIXES", "demo/,datasets/public/"),
        write_prefixes: parse_prefixes("DEFAULT_WRITE_PREFIXES", "agent/"),
        uri_regex: Regex::new(r#"s3://([a-zA-Z0-9._-]+)/([a-zA-Z0-9._\-/]+)"#).expect("regex"),
        s3: Client::from_conf(s3_config),
        http: HttpClient::new(),
        lineage: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/run_sql", post(run_sql_v1))
        .route("/v1/lineage/{job_id}", get(get_lineage_v1))
        .with_state(app_state);

    let addr: SocketAddr = format!("0.0.0.0:{api_port}").parse().expect("addr");
    info!(%addr, "control-plane listening");
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    axum::serve(listener, app).await.expect("serve");
}

async fn run_sql_v1(
    State(state): State<AppState>,
    Json(req): Json<RunSqlRequestV1>,
) -> Result<Json<RunSqlResponseV1>, ApiError> {
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
        let head = state
            .s3
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| ApiError::Storage(e.to_string()))?;
        estimated_scan_bytes =
            estimated_scan_bytes.saturating_add(head.content_length().unwrap_or(0).max(0) as u64);
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

    let runner_fut = state
        .http
        .post(format!("{}/execute", state.runner_url))
        .json(&runner_req)
        .send();

    let runner_resp = timeout(Duration::from_secs(budget.max_seconds), runner_fut)
        .await
        .map_err(|_| {
            ApiError::BudgetExceeded(
                "query exceeded max_seconds".to_string(),
                MeasuredUsageV1 {
                    budget: "max_seconds",
                    actual: budget.max_seconds,
                    limit: budget.max_seconds,
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

    let runner_payload: RunnerResponse = runner_resp
        .json()
        .await
        .map_err(|e| ApiError::Runner(e.to_string()))?;

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
        .s3
        .put_object()
        .bucket(&state.s3_bucket)
        .key(&req.output.s3_key)
        .content_type("application/octet-stream")
        .body(ByteStream::from(artifact_bytes.clone()))
        .send()
        .await
        .map_err(|e| ApiError::Storage(e.to_string()))?;

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
    Path(job_id): Path<Uuid>,
) -> Result<Json<LineageRecordV1>, ApiError> {
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
    env::var(name)
        .unwrap_or_else(|_| fallback.to_string())
        .split(',')
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

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

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
}
