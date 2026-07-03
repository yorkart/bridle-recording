use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};
use axum::http::{
    header::{
        HeaderName, CONNECTION, CONTENT_TYPE, HOST, SEC_WEBSOCKET_ACCEPT,
        SEC_WEBSOCKET_EXTENSIONS, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL,
        SEC_WEBSOCKET_VERSION, UPGRADE,
    },
    HeaderMap, Method,
};
use chrono::Utc;
use reqwest::Url;
use tokio::fs;

use crate::{
    constants::{
        CODEX_TURN_METADATA_FIELDS, CODEX_TURN_METADATA_HEADER, FALLBACK_SESSION_HEADERS,
        HOP_BY_HOP_RESPONSE_HEADERS, UNKNOWN_SESSION,
    },
    types::{AppState, ProxyMode, SessionSource},
};

pub fn build_upstream_url(upstream: &Url, path: &str, query: Option<&str>) -> anyhow::Result<Url> {
    let mut url = upstream.clone();
    let base_path = upstream.path().trim_end_matches('/');
    let request_path = path.trim_start_matches('/');
    let joined_path = if base_path.is_empty() || base_path == "/" {
        format!("/{request_path}")
    } else if request_path.is_empty() {
        base_path.to_owned()
    } else {
        format!("{base_path}/{request_path}")
    };
    url.set_path(&joined_path);
    url.set_query(query);
    Ok(url)
}

pub fn build_upstream_websocket_url(
    upstream: &Url,
    path: &str,
    query: Option<&str>,
) -> anyhow::Result<Url> {
    let mut url = build_upstream_url(upstream, path, query)?;
    let scheme = match url.scheme() {
        "http" => "ws".to_owned(),
        "https" => "wss".to_owned(),
        "ws" | "wss" => url.scheme().to_owned(),
        scheme => return Err(anyhow!("unsupported upstream websocket scheme: {scheme}")),
    };
    url.set_scheme(&scheme)
        .map_err(|_| anyhow!("set websocket URL scheme"))?;
    Ok(url)
}

pub fn reqwest_method(method: &Method) -> anyhow::Result<reqwest::Method> {
    reqwest::Method::from_bytes(method.as_str().as_bytes())
        .with_context(|| format!("unsupported HTTP method {method}"))
}

pub fn websocket_protocols(headers: &HeaderMap) -> Vec<String> {
    headers
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

pub fn should_forward_http_header(name: &HeaderName, proxy_mode: ProxyMode) -> bool {
    let _ = proxy_mode;
    name != HOST
}

pub fn should_forward_websocket_header(name: &HeaderName, proxy_mode: ProxyMode) -> bool {
    let _ = proxy_mode;
    !matches!(
        name,
        &HOST
            | &CONNECTION
            | &UPGRADE
            | &SEC_WEBSOCKET_ACCEPT
            | &SEC_WEBSOCKET_EXTENSIONS
            | &SEC_WEBSOCKET_KEY
            | &SEC_WEBSOCKET_VERSION
    )
}

pub fn should_forward_response_header(name: &HeaderName) -> bool {
    !HOP_BY_HOP_RESPONSE_HEADERS
        .iter()
        .any(|header| name.as_str().eq_ignore_ascii_case(header))
}

pub fn expects_sse(headers: &HeaderMap) -> bool {
    header_contains_token(headers, axum::http::header::ACCEPT, "text/event-stream")
}

pub fn is_sse_content_type(headers: &HeaderMap) -> bool {
    header_contains_token(headers, CONTENT_TYPE, "text/event-stream")
}

fn header_contains_token(headers: &HeaderMap, name: HeaderName, needle: &str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase().contains(needle))
        .unwrap_or(false)
}

pub fn session_from_headers(
    headers: &HeaderMap,
    session_header: &HeaderName,
) -> (String, SessionSource) {
    if let Some(value) = non_empty_header_value(headers, session_header) {
        return (
            sanitize_session_id(value),
            SessionSource::Header {
                name: session_header.to_string(),
            },
        );
    }

    for header in FALLBACK_SESSION_HEADERS {
        let header = HeaderName::from_static(header);
        if let Some(value) = non_empty_header_value(headers, &header) {
            return (
                sanitize_session_id(value),
                SessionSource::Header {
                    name: header.to_string(),
                },
            );
        }
    }

    let metadata_header = HeaderName::from_static(CODEX_TURN_METADATA_HEADER);
    if let Some((field, value)) = non_empty_metadata_value(headers, &metadata_header) {
        return (
            sanitize_session_id(&value),
            SessionSource::Header {
                name: format!("{CODEX_TURN_METADATA_HEADER}.{field}"),
            },
        );
    }

    (UNKNOWN_SESSION.to_owned(), SessionSource::Unknown)
}

fn non_empty_header_value<'a>(headers: &'a HeaderMap, name: &HeaderName) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn non_empty_metadata_value(
    headers: &HeaderMap,
    name: &HeaderName,
) -> Option<(&'static str, String)> {
    let metadata = non_empty_header_value(headers, name)?;
    let metadata: serde_json::Value = serde_json::from_str(metadata).ok()?;
    for field in CODEX_TURN_METADATA_FIELDS {
        if let Some(value) = metadata
            .get(*field)
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Some((field, value.to_owned()));
        }
    }
    None
}

pub fn sanitize_session_id(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        UNKNOWN_SESSION.to_owned()
    } else {
        out
    }
}

pub async fn next_request_index(state: &AppState, session_id: &str) -> anyhow::Result<u64> {
    let counter_key = format!("{}:{session_id}", state.profile.name);
    let mut counters = state.counters.lock().await;
    if !counters.contains_key(&counter_key) {
        let next = next_existing_request_index(&state.output_dir, session_id).await?;
        counters.insert(counter_key.clone(), next);
    }
    let counter = counters
        .get_mut(&counter_key)
        .expect("session counter inserted above");
    let index = *counter;
    *counter += 1;
    Ok(index)
}

pub fn request_dir(output_dir: &Path, session_id: &str, index: u64) -> PathBuf {
    output_dir
        .join(session_id)
        .join("requests")
        .join(format!("{index:06}"))
}

pub async fn next_existing_request_index(output_dir: &Path, session_id: &str) -> anyhow::Result<u64> {
    let requests_dir = output_dir.join(session_id).join("requests");
    let mut max_seen: Option<u64> = None;
    let mut entries = match fs::read_dir(&requests_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("read requests dir {}", requests_dir.display()))
        }
    };

    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("read entry in {}", requests_dir.display()))?
    {
        let file_type = entry
            .file_type()
            .await
            .with_context(|| format!("read file type for {}", entry.path().display()))?;
        if !file_type.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Ok(index) = name.parse::<u64>() else {
            continue;
        };
        max_seen = Some(max_seen.map_or(index, |current| current.max(index)));
    }

    Ok(max_seen.map_or(0, |index| index + 1))
}

pub fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
