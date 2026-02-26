use std::{env, net::SocketAddr};

use aws_config::BehaviorVersion;
use aws_sdk_s3::{config::Credentials, Client};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::Response,
    routing::get,
    Router,
};
use tokio_util::io::ReaderStream;
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};

#[derive(Clone)]
struct AppState {
    s3: Client,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter("info")
        .init();

    let endpoint = env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let region = env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let access_key = env::var("S3_ACCESS_KEY_ID").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key = env::var("S3_SECRET_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let cors_origins =
        env::var("CORS_ALLOWED_ORIGINS").unwrap_or_else(|_| "http://localhost:8081".to_string());

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

    let app_state = AppState {
        s3: Client::from_conf(s3_config),
    };

    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/objects/{bucket}/{*key}",
            get(proxy_object).head(proxy_object),
        )
        .layer(
            CorsLayer::new()
                .allow_origin(
                    parse_cors_origins(&cors_origins).expect("invalid CORS_ALLOWED_ORIGINS"),
                )
                .allow_methods([Method::GET, Method::HEAD, Method::OPTIONS])
                .allow_headers([header::RANGE, header::CONTENT_TYPE])
                .expose_headers([
                    header::ACCEPT_RANGES,
                    header::CONTENT_RANGE,
                    header::CONTENT_LENGTH,
                ]),
        )
        .with_state(app_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    info!(%addr, "gateway listening");
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    axum::serve(listener, app).await.expect("serve");
}

async fn proxy_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response<Body>, (StatusCode, String)> {
    if method == Method::HEAD {
        let head = state
            .s3
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(internal)?;

        let len = head.content_length().unwrap_or(0).max(0) as u64;
        let mut resp = Response::new(Body::empty());
        *resp.status_mut() = StatusCode::OK;
        set_common_headers(resp.headers_mut(), len)?;
        return Ok(resp);
    }

    let range_header = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let mut req = state.s3.get_object().bucket(&bucket).key(&key);
    if let Some(range) = &range_header {
        req = req.range(range);
    }

    let output = req.send().await.map_err(internal)?;
    let content_len = output.content_length().unwrap_or(0).max(0) as u64;
    let content_range = output.content_range().map(str::to_string);
    let body = Body::from_stream(ReaderStream::new(output.body.into_async_read()));

    let status = response_status(range_header.as_deref(), content_range.as_deref());

    let mut resp = Response::new(body);
    *resp.status_mut() = status;
    set_common_headers(resp.headers_mut(), content_len)?;

    if let Some(v) = content_range {
        resp.headers_mut().insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&v).map_err(internal)?,
        );
    }

    Ok(resp)
}

fn set_common_headers(
    headers: &mut HeaderMap,
    content_len: u64,
) -> Result<(), (StatusCode, String)> {
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_len.to_string()).map_err(internal)?,
    );
    Ok(())
}

fn internal<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    error!(error = %err, "gateway request failed");
    (StatusCode::BAD_GATEWAY, err.to_string())
}

fn parse_cors_origins(raw: &str) -> Result<tower_http::cors::AllowOrigin, String> {
    let items = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();

    if items.len() == 1 && items[0] == "*" {
        return Ok(Any.into());
    }

    if items.is_empty() {
        return Err("CORS_ALLOWED_ORIGINS cannot be empty".to_string());
    }

    let mut origins = Vec::with_capacity(items.len());
    for item in items {
        let v =
            HeaderValue::from_str(item).map_err(|e| format!("invalid CORS origin {item}: {e}"))?;
        origins.push(v);
    }

    Ok(origins.into())
}

fn response_status(range_header: Option<&str>, content_range: Option<&str>) -> StatusCode {
    if range_header.is_some() && content_range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_status_is_partial_only_with_content_range() {
        assert_eq!(
            response_status(Some("bytes=0-10"), Some("bytes 0-10/100")),
            StatusCode::PARTIAL_CONTENT
        );
        assert_eq!(response_status(Some("bytes=0-10"), None), StatusCode::OK);
        assert_eq!(response_status(None, None), StatusCode::OK);
    }

    #[test]
    fn common_headers_include_accept_ranges_and_length() {
        let mut headers = HeaderMap::new();
        set_common_headers(&mut headers, 128).expect("set headers");

        assert_eq!(
            headers
                .get(header::ACCEPT_RANGES)
                .and_then(|v| v.to_str().ok()),
            Some("bytes")
        );
        assert_eq!(
            headers
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok()),
            Some("128")
        );
    }

    #[test]
    fn parse_cors_origins_accepts_wildcard() {
        assert!(parse_cors_origins("*").is_ok());
    }

    #[test]
    fn parse_cors_origins_rejects_empty() {
        assert!(parse_cors_origins(" , ").is_err());
    }
}
