use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};
use axum::{
    body::Body,
    http::{header::CONTENT_LENGTH, HeaderName, HeaderValue, Method},
    response::Response,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use bytes::Bytes;
use tokio::fs;

use crate::{
    matcher::{build_request_match, find_recorded_match, load_or_build_request_match},
    types::{
        AppState, HeaderRecord, HeaderValueRecord, RecordedMatch, ReplaySession,
        ResponseRewriteSpec,
    },
    util::{request_dir, session_from_headers, should_forward_response_header},
};

pub async fn handle_mock_proxy(
    state: AppState,
    method: Method,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    path: String,
    body: Bytes,
) -> anyhow::Result<Response> {
    let live_session_id = session_from_headers(&headers, &state.session_header).0;
    let replay_key = |session_id: &str| format!("{}:{session_id}", state.profile.name);
    let incoming_match = build_request_match(&method, &path, uri.query(), &headers, &body)?;

    let recorded = {
        let mut replay_sessions = state.replay_sessions.lock().await;
        if let Some(session) = replay_sessions.get_mut(&replay_key(&live_session_id)) {
            let request_dir = request_dir(
                &state.output_dir,
                &session.recorded_session_id,
                session.next_index,
            );
            let recorded_match = load_or_build_request_match(&request_dir)
                .await
                .with_context(|| {
                    format!(
                        "read next recorded request {} for live session {}",
                        session.next_index, live_session_id
                    )
                })?;
            if recorded_match.hash != incoming_match.hash {
                return Err(anyhow!(
                    "recorded session mismatch for live session {live_session_id}: expected index {} hash {}, got {}",
                    session.next_index,
                    recorded_match.hash,
                    incoming_match.hash
                ));
            }
            let recorded = RecordedMatch {
                session_id: session.recorded_session_id.clone(),
                index: session.next_index,
                request_dir,
            };
            session.next_index += 1;
            recorded
        } else {
            let recorded = find_recorded_match(&state.output_dir, &incoming_match).await?;
            replay_sessions.insert(
                replay_key(&live_session_id),
                ReplaySession {
                    recorded_session_id: recorded.session_id.clone(),
                    next_index: recorded.index + 1,
                },
            );
            recorded
        }
    };

    build_replay_response(&recorded.request_dir)
        .await
        .with_context(|| {
            format!(
                "build replay response for {}/requests/{:06}",
                recorded.session_id, recorded.index
            )
        })
}

pub async fn build_replay_response(request_dir: &Path) -> anyhow::Result<Response> {
    let status = read_response_status(request_dir).await?;
    let response_headers = read_header_records(request_dir.join("response_headers.json")).await?;
    let rewrite_spec = read_response_rewrite_spec(request_dir).await?;
    let mut response_builder = Response::builder().status(status);
    for header in response_headers {
        let Ok(name) = HeaderName::from_bytes(header.name.as_bytes()) else {
            continue;
        };
        if !should_forward_response_header(&name) || name == CONTENT_LENGTH {
            continue;
        }
        let Some(value) = header_value_for_replay(&header.value) else {
            continue;
        };
        response_builder = response_builder.header(name, value);
    }

    let sse_path = request_dir.join("response_sse.raw");
    if fs::try_exists(&sse_path).await? {
        response_builder = response_builder.header(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        let mut bytes = fs::read(&sse_path)
            .await
            .with_context(|| format!("read {}", sse_path.display()))?;
        if let Some(spec) = rewrite_spec.as_ref() {
            bytes = rewrite_sse_bytes(&bytes, spec)?;
        }
        return response_builder
            .body(Body::from(bytes))
            .context("build replay SSE response");
    }

    let body_path = request_dir.join("response_body.raw");
    let mut bytes = fs::read(&body_path)
        .await
        .with_context(|| format!("read {}", body_path.display()))?;
    if let Some(spec) = rewrite_spec.as_ref() {
        bytes = rewrite_json_body_bytes(&bytes, spec)?;
    }
    response_builder
        .body(Body::from(bytes))
        .context("build replay buffered response")
}

async fn read_response_rewrite_spec(
    request_dir: &Path,
) -> anyhow::Result<Option<ResponseRewriteSpec>> {
    let path = request_dir.join("response_rewrite.json");
    match fs::read(&path).await {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .with_context(|| format!("parse {}", path.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("read {}", path.display())),
    }
}

fn rewrite_json_body_bytes(bytes: &[u8], spec: &ResponseRewriteSpec) -> anyhow::Result<Vec<u8>> {
    let mut value: serde_json::Value =
        serde_json::from_slice(bytes).context("parse replay response body for rewrite")?;
    apply_response_rewrite_spec(&mut value, spec)?;
    serde_json::to_vec(&value).context("serialize rewritten response body")
}

fn rewrite_sse_bytes(bytes: &[u8], spec: &ResponseRewriteSpec) -> anyhow::Result<Vec<u8>> {
    let text = std::str::from_utf8(bytes).context("parse replay SSE as utf-8")?;
    let mut out = String::with_capacity(text.len());
    let normalized = text.replace("\r\n", "\n");
    for event in normalized.split_inclusive("\n\n") {
        if event.trim().is_empty() {
            out.push_str(event);
            continue;
        }
        out.push_str(&rewrite_sse_event(event, spec)?);
    }
    Ok(out.into_bytes())
}

fn rewrite_sse_event(event: &str, spec: &ResponseRewriteSpec) -> anyhow::Result<String> {
    let has_trailing_boundary = event.ends_with("\n\n");
    let body = event.strip_suffix("\n\n").unwrap_or(event);
    let mut event_lines = Vec::new();
    let mut data_lines = Vec::new();
    for line in body.lines() {
        if let Some(data) = line.strip_prefix("data: ") {
            data_lines.push(data.to_owned());
        } else if let Some(data) = line.strip_prefix("data:") {
            data_lines.push(data.to_owned());
        } else {
            event_lines.push(line.to_owned());
        }
    }

    if data_lines.is_empty() {
        return Ok(event.to_owned());
    }

    let data = data_lines.join("\n");
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(&data) else {
        return Ok(event.to_owned());
    };
    apply_response_rewrite_spec(&mut value, spec)?;
    event_lines.push(format!("data: {}", serde_json::to_string(&value)?));
    let mut rewritten = event_lines.join("\n");
    if has_trailing_boundary {
        rewritten.push_str("\n\n");
    }
    Ok(rewritten)
}

fn apply_response_rewrite_spec(
    value: &mut serde_json::Value,
    spec: &ResponseRewriteSpec,
) -> anyhow::Result<()> {
    for replacement in &spec.replacements {
        let target = value.pointer_mut(&replacement.pointer).ok_or_else(|| {
            anyhow!(
                "response rewrite pointer not found in recorded response: {}",
                replacement.pointer
            )
        })?;
        *target = replacement.value.clone();
    }
    Ok(())
}

async fn read_response_status(request_dir: &Path) -> anyhow::Result<u16> {
    let path = request_dir.join("response_meta.json");
    let bytes = fs::read(&path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
    let status = value
        .get("status")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| anyhow!("response_meta.json missing status"))?;
    u16::try_from(status).context("response status out of range")
}

async fn read_header_records(path: PathBuf) -> anyhow::Result<Vec<HeaderRecord>> {
    let bytes = fs::read(&path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

fn header_value_for_replay(value: &HeaderValueRecord) -> Option<HeaderValue> {
    match value {
        HeaderValueRecord::Text { value } => HeaderValue::from_str(value).ok(),
        HeaderValueRecord::BinaryBase64 { value } => BASE64
            .decode(value)
            .ok()
            .and_then(|bytes| HeaderValue::from_bytes(&bytes).ok()),
        HeaderValueRecord::RedactedSha256 { .. } => None,
    }
}
