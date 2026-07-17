use std::{collections::BTreeMap, io::Read, path::Path};

use anyhow::{anyhow, Context};
use axum::http::{header::CONTENT_ENCODING, HeaderMap, HeaderName, HeaderValue, Method};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::fs;

use crate::{
    constants::{IGNORED_INPUT_TEXT_PREFIXES, MATCHER_VERSION},
    recording::write_json_file,
    types::{
        HeaderRecord, HeaderValueRecord, MatchRoute, RecordedMatch, RequestMatch, RequestMeta,
    },
};

pub async fn find_recorded_match(
    recordings_dir: &Path,
    mock_derived_dir: &Path,
    incoming: &RequestMatch,
) -> anyhow::Result<RecordedMatch> {
    let mut sessions = fs::read_dir(recordings_dir)
        .await
        .with_context(|| format!("read recordings dir {}", recordings_dir.display()))?;
    while let Some(session_entry) = sessions.next_entry().await? {
        if !session_entry.file_type().await?.is_dir() {
            continue;
        }
        let Some(session_id) = session_entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        let requests_dir = session_entry.path().join("requests");
        let mut requests = match fs::read_dir(&requests_dir).await {
            Ok(requests) => requests,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err).with_context(|| format!("read {}", requests_dir.display()))
            }
        };
        while let Some(request_entry) = requests.next_entry().await? {
            if !request_entry.file_type().await?.is_dir() {
                continue;
            }
            let Some(index) = request_entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u64>().ok())
            else {
                continue;
            };
            let request_dir = request_entry.path();
            let derived_dir = mock_derived_dir
                .join(&session_id)
                .join("requests")
                .join(format!("{index:06}"));
            let Ok(recorded_match) = load_or_build_request_match(&request_dir, &derived_dir).await
            else {
                continue;
            };
            if recorded_match.hash == incoming.hash {
                return Ok(RecordedMatch {
                    session_id,
                    index,
                    request_dir,
                    derived_dir,
                });
            }
        }
    }

    Err(anyhow!(
        "no recorded request matched route {} {} hash {}",
        incoming.route.method,
        incoming.route.path,
        incoming.hash
    ))
}

pub async fn load_or_build_request_match(
    request_dir: &Path,
    derived_dir: &Path,
) -> anyhow::Result<RequestMatch> {
    let match_path = derived_dir.join("request_match.json");
    match fs::read(&match_path).await {
        Ok(bytes) => {
            let request_match: RequestMatch = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse {}", match_path.display()))?;
            if request_match.version == MATCHER_VERSION {
                return Ok(request_match);
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err).with_context(|| format!("read {}", match_path.display())),
    }

    let meta_path = request_dir.join("request_meta.json");
    let meta_bytes = fs::read(&meta_path)
        .await
        .with_context(|| format!("read {}", meta_path.display()))?;
    let meta: RequestMeta = serde_json::from_slice(&meta_bytes)
        .with_context(|| format!("parse {}", meta_path.display()))?;
    let headers = header_records_to_map(
        &read_header_records(request_dir.join("request_headers.json")).await?,
    );
    let body = Bytes::from(
        fs::read(request_dir.join("request_body.raw"))
            .await
            .with_context(|| format!("read {}", request_dir.join("request_body.raw").display()))?,
    );
    let method = Method::from_bytes(meta.method.as_bytes())
        .with_context(|| format!("parse recorded method {}", meta.method))?;
    let path = meta.path.trim_start_matches('/');
    let request_match = build_request_match(&method, path, meta.query.as_deref(), &headers, &body)?;
    write_json_file(match_path, &request_match).await?;
    Ok(request_match)
}

async fn read_header_records(path: std::path::PathBuf) -> anyhow::Result<Vec<HeaderRecord>> {
    let bytes = fs::read(&path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

fn header_records_to_map(records: &[HeaderRecord]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for record in records {
        let Ok(name) = HeaderName::from_bytes(record.name.as_bytes()) else {
            continue;
        };
        let Some(value) = header_value_for_replay(&record.value) else {
            continue;
        };
        headers.insert(name, value);
    }
    headers
}

fn header_value_for_replay(value: &HeaderValueRecord) -> Option<HeaderValue> {
    match value {
        HeaderValueRecord::Text { value } => HeaderValue::from_str(value).ok(),
        HeaderValueRecord::BinaryBase64 { value } => BASE64
            .decode(value)
            .ok()
            .and_then(|bytes| HeaderValue::from_bytes(&bytes).ok()),
    }
}

pub fn build_request_match(
    method: &Method,
    path: &str,
    query: Option<&str>,
    headers: &HeaderMap,
    body: &Bytes,
) -> anyhow::Result<RequestMatch> {
    let decoded_body = decode_request_body(headers, body)?;
    let body_json = if decoded_body.is_empty() {
        None
    } else {
        Some(
            serde_json::from_slice::<serde_json::Value>(&decoded_body)
                .context("parse request body as json for matcher")?,
        )
    };
    let canonical = canonical_request(method, path, query, body_json.as_ref());
    let route = MatchRoute {
        method: method.to_string(),
        path: format!("/{path}"),
        query: query.map(ToOwned::to_owned),
        model: body_json
            .as_ref()
            .and_then(|value| value.get("model"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        stream: body_json
            .as_ref()
            .and_then(|value| value.get("stream"))
            .and_then(serde_json::Value::as_bool),
    };
    let hash = sha256_json(&canonical)?;
    Ok(RequestMatch {
        version: MATCHER_VERSION,
        hash,
        route,
        canonical,
    })
}

fn decode_request_body(headers: &HeaderMap, body: &Bytes) -> anyhow::Result<Vec<u8>> {
    let encoding = headers
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .unwrap_or("");
    if encoding.is_empty() || encoding.eq_ignore_ascii_case("identity") {
        return Ok(body.to_vec());
    }
    if encoding.eq_ignore_ascii_case("zstd") {
        let mut decoder = zstd::stream::read::Decoder::new(std::io::Cursor::new(body.as_ref()))
            .context("create zstd decoder")?;
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .context("decode zstd request body")?;
        return Ok(decoded);
    }
    Err(anyhow!(
        "unsupported request content-encoding for matcher: {encoding}"
    ))
}

fn canonical_request(
    method: &Method,
    path: &str,
    query: Option<&str>,
    body: Option<&serde_json::Value>,
) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    root.insert("version".to_owned(), serde_json::json!(MATCHER_VERSION));
    root.insert("method".to_owned(), serde_json::json!(method.as_str()));
    root.insert("path".to_owned(), serde_json::json!(format!("/{path}")));
    if method == Method::GET {
        root.insert("query".to_owned(), serde_json::json!(query));
    }

    if let Some(body) = body {
        let mut canonical_body = serde_json::Map::new();
        for field in [
            "model",
            "stream",
            "store",
            "include",
            "parallel_tool_calls",
            "tool_choice",
            "reasoning",
            "text",
            "instructions",
        ] {
            if let Some(value) = body.get(field) {
                canonical_body.insert(field.to_owned(), canonicalize_json(value));
            }
        }
        if let Some(value) = body.get("input") {
            canonical_body.insert("input".to_owned(), canonicalize_input(value));
        }
        root.insert("body".to_owned(), serde_json::Value::Object(canonical_body));
    }

    serde_json::Value::Object(root)
}

fn canonicalize_input(value: &serde_json::Value) -> serde_json::Value {
    let Some(items) = value.as_array() else {
        return canonicalize_json(value);
    };
    serde_json::Value::Array(
        items
            .iter()
            .map(|item| {
                let mut out = serde_json::Map::new();
                for field in ["role", "type", "content"] {
                    if let Some(value) = item.get(field) {
                        if field == "content" {
                            if let Some(value) = canonicalize_input_content(value) {
                                out.insert(field.to_owned(), value);
                            }
                        } else {
                            out.insert(field.to_owned(), canonicalize_json(value));
                        }
                    }
                }
                serde_json::Value::Object(out)
            })
            .collect(),
    )
}

fn canonicalize_input_content(value: &serde_json::Value) -> Option<serde_json::Value> {
    match value {
        serde_json::Value::Array(parts) => {
            let filtered = parts
                .iter()
                .filter(|part| !is_ignored_input_content_part(part))
                .map(canonicalize_input_content_part)
                .collect::<Vec<_>>();
            if filtered.is_empty() {
                None
            } else {
                Some(serde_json::Value::Array(filtered))
            }
        }
        serde_json::Value::String(text) if is_ignored_input_text(text) => None,
        other => Some(canonicalize_json(other)),
    }
}

fn canonicalize_input_content_part(value: &serde_json::Value) -> serde_json::Value {
    let mut value = canonicalize_json(value);
    let Some(text) = value
        .get("text")
        .and_then(serde_json::Value::as_str)
        .filter(|text| text.trim_start().starts_with("<environment_context>"))
    else {
        return value;
    };
    let text = canonicalize_environment_context_text(text);
    if let Some(field) = value.get_mut("text") {
        *field = serde_json::Value::String(text);
    }
    value
}

fn canonicalize_environment_context_text(text: &str) -> String {
    text.lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            !trimmed.starts_with("<current_date>") && !trimmed.starts_with("<timezone>")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn is_ignored_input_content_part(value: &serde_json::Value) -> bool {
    value
        .get("text")
        .and_then(serde_json::Value::as_str)
        .is_some_and(is_ignored_input_text)
}

fn is_ignored_input_text(text: &str) -> bool {
    let text = text.trim_start();
    IGNORED_INPUT_TEXT_PREFIXES
        .iter()
        .any(|prefix| text.starts_with(prefix))
}

fn canonicalize_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: BTreeMap<_, _> = map
                .iter()
                .map(|(key, value)| (key.clone(), canonicalize_json(value)))
                .collect();
            serde_json::to_value(sorted).expect("BTreeMap serializes")
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonicalize_json).collect())
        }
        other => other.clone(),
    }
}

fn sha256_json(value: &serde_json::Value) -> anyhow::Result<String> {
    let bytes = serde_json::to_vec(value).context("serialize canonical request")?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}
