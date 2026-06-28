use std::path::PathBuf;

use anyhow::{anyhow, Context};
use async_stream::stream;
use axum::{
    body::Body,
    http::{HeaderMap, Method, Uri},
    response::Response,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
};
use tracing::warn;

use crate::{
    constants::UPSTREAM_MAX_ATTEMPTS,
    matcher::build_request_match,
    sse::SseParser,
    types::{AppState, RequestMeta, ResponseMeta, SseEventRecord},
    util::{
        build_upstream_url, expects_sse, headers_to_records, is_sse_content_type, now_rfc3339,
        next_request_index, reqwest_method, request_dir, session_from_headers,
        should_forward_http_header, should_forward_response_header, write_bytes_file,
        write_error_response_meta, write_json_file, write_manifest,
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
    let index = next_request_index(&state, &session_id).await?;
    let request_dir = request_dir(&state.output_dir, &session_id, index);
    fs::create_dir_all(&request_dir)
        .await
        .with_context(|| format!("create request dir {}", request_dir.display()))?;

    let upstream_url = build_upstream_url(&state.profile.upstream, &path, uri.query())?;
    let request_meta = RequestMeta {
        index,
        session_id: session_id.clone(),
        session_source,
        started_at: started_at.clone(),
        method: method.to_string(),
        path: format!("/{path}"),
        query: uri.query().map(ToOwned::to_owned),
        upstream_url: upstream_url.to_string(),
        request_body_bytes: body.len(),
    };

    write_json_file(request_dir.join("request_meta.json"), &request_meta).await?;
    write_json_file(
        request_dir.join("request_headers.json"),
        &headers_to_records(&headers, state.unsafe_record_secrets),
    )
    .await?;
    write_bytes_file(request_dir.join("request_body.raw"), &body).await?;
    match build_request_match(&method, &path, uri.query(), &headers, &body) {
        Ok(request_match) => {
            write_json_file(request_dir.join("request_match.json"), &request_match).await?;
        }
        Err(err) => {
            warn!(?err, request_dir = %request_dir.display(), "failed to build request match index");
        }
    }
    write_manifest(&state, &session_id).await?;

    let upstream_response = match send_upstream_with_retry(
        &state,
        &method,
        &headers,
        &body,
        upstream_url,
        &request_dir,
    )
    .await
    {
        Ok(response) => response,
        Err(err) => {
            write_error_response_meta(&request_dir, started_at, err.to_string()).await?;
            return Err(anyhow!("upstream request failed: {err}"));
        }
    };

    let status = upstream_response.status();
    let response_headers = upstream_response.headers().clone();
    write_json_file(
        request_dir.join("response_headers.json"),
        &headers_to_records(&response_headers, state.unsafe_record_secrets),
    )
    .await?;

    let is_sse = expects_sse(&headers) || is_sse_content_type(&response_headers);

    let mut response_builder = Response::builder().status(status.as_u16());
    for (name, value) in response_headers.iter() {
        if !should_forward_response_header(name) {
            continue;
        }
        response_builder = response_builder.header(name, value);
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
                write_error_response_meta(&request_dir, started_at, err.to_string()).await?;
                return Err(anyhow!("read upstream response body failed: {err}"));
            }
        };
        write_bytes_file(request_dir.join("response_body.raw"), &bytes).await?;
        let response_meta = ResponseMeta {
            status: status.as_u16(),
            started_at,
            completed_at: now_rfc3339(),
            response_body_bytes: bytes.len(),
            sse_event_count: 0,
            upstream_error: None,
        };
        write_json_file(request_dir.join("response_meta.json"), &response_meta).await?;
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
    request_dir: &std::path::Path,
) -> anyhow::Result<reqwest::Response> {
    let method = reqwest_method(method)?;
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 1..=UPSTREAM_MAX_ATTEMPTS {
        let mut upstream_request = state.client.request(method.clone(), upstream_url.clone());
        for (name, value) in headers.iter() {
            if !should_forward_http_header(name, state.strip_responses_lite) {
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
                        request_dir = %request_dir.display(),
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
                        request_dir = %request_dir.display(),
                        profile = %state.profile.name,
                        "retrying upstream request after transport error"
                    );
                    last_error = Some(anyhow!(err));
                    tokio::time::sleep(retry_delay(attempt)).await;
                    continue;
                }
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
    request_dir: PathBuf,
    started_at: String,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> {
    stream! {
        let status = upstream_response.status().as_u16();
        let mut stream = upstream_response.bytes_stream();
        let mut raw_file = match File::create(request_dir.join("response_sse.raw")).await {
            Ok(file) => file,
            Err(err) => {
                yield Err(err);
                return;
            }
        };
        let mut events_file = match File::create(request_dir.join("response_sse.jsonl")).await {
            Ok(file) => file,
            Err(err) => {
                yield Err(err);
                return;
            }
        };
        let mut parser = SseParser::default();
        let mut response_body_bytes = 0usize;
        let mut sse_event_count = 0usize;
        let mut upstream_error = None;

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    response_body_bytes += bytes.len();
                    if let Err(err) = raw_file.write_all(&bytes).await {
                        yield Err(err);
                        return;
                    }
                    let events = parser.push(&bytes);
                    for event in events {
                        let record = SseEventRecord {
                            index: sse_event_count,
                            event: event.event,
                            id: event.id,
                            retry: event.retry,
                            data: event.data,
                            raw_base64: BASE64.encode(event.raw),
                        };
                        match serde_json::to_vec(&record) {
                            Ok(mut line) => {
                                line.push(b'\n');
                                if let Err(err) = events_file.write_all(&line).await {
                                    yield Err(err);
                                    return;
                                }
                            }
                            Err(err) => {
                                yield Err(std::io::Error::other(err));
                                return;
                            }
                        }
                        sse_event_count += 1;
                    }
                    yield Ok(bytes);
                }
                Err(err) => {
                    upstream_error = Some(err.to_string());
                    break;
                }
            }
        }

        if let Err(err) = raw_file.flush().await {
            yield Err(err);
            return;
        }
        if let Err(err) = events_file.flush().await {
            yield Err(err);
            return;
        }
        let response_meta = ResponseMeta {
            status,
            started_at,
            completed_at: now_rfc3339(),
            response_body_bytes,
            sse_event_count,
            upstream_error,
        };
        if let Err(err) = write_json_file(request_dir.join("response_meta.json"), &response_meta).await {
            yield Err(std::io::Error::other(err));
        }
    }
}
