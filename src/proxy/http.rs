use std::path::PathBuf;

use anyhow::{anyhow, Context};
use async_stream::stream;
use axum::{
    body::Body,
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, Method, Uri},
    response::Response,
};
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
};
use tracing::warn;

use crate::{
    constants::UPSTREAM_MAX_ATTEMPTS,
    recording::{
        headers_to_records, write_bytes_file, write_error_response_meta, write_json_file,
        write_manifest,
    },
    types::{AppState, RequestMeta, ResponseMeta},
    util::{
        build_upstream_url, expects_sse, is_sse_content_type, next_request_index, now_rfc3339,
        request_dir, reqwest_method, session_from_headers, should_forward_http_header,
        should_forward_response_header,
    },
};

pub async fn handle_proxy(
    state: AppState,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    path: String,
    body: Bytes,
) -> anyhow::Result<Response> {
    let started_at = now_rfc3339();
    let (session_id, session_source) = session_from_headers(&headers, &state.session_header);
    let user_agent = headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-");
    tracing::info!(
        profile = %state.profile.name,
        method = %method,
        path = %format!("/{path}"),
        session_id = %session_id,
        user_agent = %user_agent,
        "received http proxy request"
    );
    let index = next_request_index(&state, &session_id).await?;
    let upstream_url = build_upstream_url(&state.profile.upstream, &path, uri.query())?;
    let request_dir = request_dir(&state.output_dir, &session_id, index);
    let request_dir = match create_http_recording(
        &state,
        &request_dir,
        RequestMeta {
            index,
            session_id: session_id.clone(),
            session_source,
            started_at: started_at.clone(),
            method: method.to_string(),
            path: format!("/{path}"),
            query: uri.query().map(ToOwned::to_owned),
            upstream_url: upstream_url.to_string(),
            request_body_bytes: body.len(),
        },
        &headers,
        &body,
        &session_id,
    )
    .await
    {
        Ok(dir) => dir,
        Err(err) => {
            warn!(?err, "http recording setup failed; continuing without recording");
            None
        }
    };

    let upstream_response = match send_upstream_with_retry(
        &state,
        &method,
        &headers,
        &body,
        upstream_url,
        request_dir.as_deref(),
    )
    .await
    {
        Ok(response) => response,
        Err(err) => {
            if let Some(request_dir) = request_dir.as_deref() {
                if let Err(write_err) =
                    write_error_response_meta(request_dir, started_at.clone(), err.to_string()).await
                {
                    warn!(?write_err, "failed to record upstream HTTP error");
                }
            }
            return Err(anyhow!("upstream request failed: {err}"));
        }
    };

    let status = upstream_response.status();
    let response_headers = upstream_response.headers().clone();
    if let Some(request_dir) = request_dir.as_ref() {
        if let Err(err) = write_json_file(
            request_dir.join("response_headers.json"),
            &headers_to_records(&response_headers, state.unsafe_record_secrets),
        )
        .await
        {
            warn!(?err, "failed to record HTTP response headers");
        }
    }

    let is_sse = expects_sse(&headers) || is_sse_content_type(&response_headers);

    let mut response_builder = Response::builder().status(status.as_u16());
    for (name, value) in response_headers.iter() {
        if !should_forward_response_header(name) {
            continue;
        }
        response_builder = response_builder.header(name, value);
    }
    if is_sse && !response_headers.contains_key(CONTENT_TYPE) {
        response_builder =
            response_builder.header(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    }

    if is_sse {
        let body_stream = record_streaming_response(upstream_response, request_dir, started_at);
        response_builder
            .body(Body::from_stream(body_stream))
            .context("build streaming response")
    } else {
        let bytes = match upstream_response.bytes().await {
            Ok(bytes) => bytes,
            Err(err) => {
                if let Some(request_dir) = request_dir.as_deref() {
                    if let Err(write_err) =
                        write_error_response_meta(request_dir, started_at.clone(), err.to_string()).await
                    {
                        warn!(?write_err, "failed to record buffered HTTP read error");
                    }
                }
                return Err(anyhow!("read upstream response body failed: {err}"));
            }
        };
        if let Some(request_dir) = request_dir.as_ref() {
            if let Err(err) = write_bytes_file(request_dir.join("response_body.raw"), &bytes).await {
                warn!(?err, "failed to record HTTP response body");
            }
        }
        let response_meta = ResponseMeta {
            status: status.as_u16(),
            started_at,
            completed_at: now_rfc3339(),
            response_body_bytes: bytes.len(),
            sse_event_count: 0,
            upstream_error: None,
        };
        if let Some(request_dir) = request_dir.as_ref() {
            if let Err(err) = write_json_file(request_dir.join("response_meta.json"), &response_meta).await {
                warn!(?err, "failed to record HTTP response metadata");
            }
        }
        response_builder
            .body(Body::from(bytes))
            .context("build buffered response")
    }
}

async fn send_upstream_with_retry(
    state: &AppState,
    method: &Method,
    headers: &HeaderMap,
    body: &Bytes,
    upstream_url: reqwest::Url,
    request_dir: Option<&std::path::Path>,
) -> anyhow::Result<reqwest::Response> {
    let method = reqwest_method(method)?;
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 1..=UPSTREAM_MAX_ATTEMPTS {
        let mut upstream_request = state.client.request(method.clone(), upstream_url.clone());
        for (name, value) in headers.iter() {
            if !should_forward_http_header(name, state.proxy_mode) {
                continue;
            }
            upstream_request = upstream_request.header(name.as_str(), value.as_bytes());
        }
        let upstream_request = upstream_request.body(body.clone());

        match upstream_request.send().await {
            Ok(response) => {
                if should_retry_status(response.status()) && attempt < UPSTREAM_MAX_ATTEMPTS {
                    warn!(
                        attempt,
                        max_attempts = UPSTREAM_MAX_ATTEMPTS,
                        status = %response.status(),
                        request_dir = ?request_dir.map(|path| path.display().to_string()),
                        profile = %state.profile.name,
                        "retrying upstream request after retryable status"
                    );
                    tokio::time::sleep(retry_delay(attempt)).await;
                    continue;
                }
                return Ok(response);
            }
            Err(err) => {
                if should_retry_error(&err) && attempt < UPSTREAM_MAX_ATTEMPTS {
                    warn!(
                        attempt,
                        max_attempts = UPSTREAM_MAX_ATTEMPTS,
                        error = %err,
                        error_debug = ?err,
                        is_timeout = err.is_timeout(),
                        is_connect = err.is_connect(),
                        is_request = err.is_request(),
                        is_status = err.is_status(),
                        url = ?err.url(),
                        request_dir = ?request_dir.map(|path| path.display().to_string()),
                        profile = %state.profile.name,
                        "retrying upstream request after transport error"
                    );
                    last_error = Some(anyhow!(err));
                    tokio::time::sleep(retry_delay(attempt)).await;
                    continue;
                }
                warn!(
                    attempt,
                    max_attempts = UPSTREAM_MAX_ATTEMPTS,
                    error = %err,
                    error_debug = ?err,
                    is_timeout = err.is_timeout(),
                    is_connect = err.is_connect(),
                    is_request = err.is_request(),
                    is_status = err.is_status(),
                    url = ?err.url(),
                    request_dir = ?request_dir.map(|path| path.display().to_string()),
                    profile = %state.profile.name,
                    "upstream request failed after transport error"
                );
                return Err(anyhow!(err));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("upstream request retries exhausted")))
}

pub(crate) fn should_retry_status(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::BAD_GATEWAY
            | reqwest::StatusCode::SERVICE_UNAVAILABLE
            | reqwest::StatusCode::GATEWAY_TIMEOUT
            | reqwest::StatusCode::TOO_MANY_REQUESTS
    )
}

pub(crate) fn should_retry_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request()
}

pub(crate) fn retry_delay(attempt: usize) -> std::time::Duration {
    match attempt {
        1 => std::time::Duration::from_millis(200),
        2 => std::time::Duration::from_millis(500),
        _ => std::time::Duration::from_secs(1),
    }
}

fn record_streaming_response(
    upstream_response: reqwest::Response,
    request_dir: Option<PathBuf>,
    started_at: String,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> {
    stream! {
        let status = upstream_response.status().as_u16();
        let mut stream = upstream_response.bytes_stream();
        let mut raw_file = open_optional_file(request_dir.as_ref().map(|dir| dir.join("response_sse.raw"))).await;
        let mut response_body_bytes = 0usize;
        let mut upstream_error = None;

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    response_body_bytes += bytes.len();
                    yield Ok(bytes.clone());
                    if let Some(raw_file) = raw_file.as_mut() {
                        if let Err(err) = raw_file.write_all(&bytes).await {
                            warn!(?err, "failed to record raw SSE bytes");
                        }
                    }
                }
                Err(err) => {
                    upstream_error = Some(err.to_string());
                    break;
                }
            }
        }

        if let Some(raw_file) = raw_file.as_mut() {
            if let Err(err) = raw_file.flush().await {
                warn!(?err, "failed to flush raw SSE recording file");
            }
        }
        let response_meta = ResponseMeta {
            status,
            started_at,
            completed_at: now_rfc3339(),
            response_body_bytes,
            sse_event_count: 0,
            upstream_error,
        };
        if let Some(request_dir) = request_dir.as_ref() {
            if let Err(err) = write_json_file(request_dir.join("response_meta.json"), &response_meta).await {
                warn!(?err, "failed to record SSE response metadata");
            }
        }
    }
}

async fn create_http_recording(
    state: &AppState,
    request_dir: &PathBuf,
    request_meta: RequestMeta,
    headers: &HeaderMap,
    body: &Bytes,
    session_id: &str,
) -> anyhow::Result<Option<PathBuf>> {
    fs::create_dir_all(request_dir)
        .await
        .with_context(|| format!("create request dir {}", request_dir.display()))?;
    write_json_file(request_dir.join("request_meta.json"), &request_meta).await?;
    write_json_file(
        request_dir.join("request_headers.json"),
        &headers_to_records(headers, state.unsafe_record_secrets),
    )
    .await?;
    write_bytes_file(request_dir.join("request_body.raw"), body).await?;
    write_manifest(state, session_id).await?;
    Ok(Some(request_dir.clone()))
}

async fn open_optional_file(path: Option<PathBuf>) -> Option<File> {
    let path = path?;
    match File::create(&path).await {
        Ok(file) => Some(file),
        Err(err) => {
            warn!(?err, path = %path.display(), "failed to create recording file");
            None
        }
    }
}
