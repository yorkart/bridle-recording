use std::path::{Path, PathBuf};

use anyhow::Context;
use axum::http::{HeaderMap, HeaderValue};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncWriteExt,
};

use crate::{
    types::{
        AppState, HeaderRecord, HeaderValueRecord, Manifest, RecorderManifest, ResponseMeta,
        WebSocketMeta,
    },
    util::now_rfc3339,
};

pub async fn append_access_log_line(path: &Path, line: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create access log dir {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("open access log {}", path.display()))?;
    file.write_all(line.as_bytes())
        .await
        .with_context(|| format!("append access log {}", path.display()))?;
    Ok(())
}

pub fn headers_to_records(headers: &HeaderMap, unsafe_record_secrets: bool) -> Vec<HeaderRecord> {
    let _ = unsafe_record_secrets;
    headers
        .iter()
        .map(|(name, value)| HeaderRecord {
            name: name.to_string(),
            value: header_value_record(value),
        })
        .collect()
}

fn header_value_record(value: &HeaderValue) -> HeaderValueRecord {
    match value.to_str() {
        Ok(text) => HeaderValueRecord::Text {
            value: text.to_owned(),
        },
        Err(_) => HeaderValueRecord::BinaryBase64 {
            value: BASE64.encode(value.as_bytes()),
        },
    }
}

pub async fn write_json_file<T: serde::Serialize>(path: PathBuf, value: &T) -> anyhow::Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value).context("serialize json")?;
    bytes.push(b'\n');
    write_bytes_file(path, &bytes).await
}

pub async fn write_bytes_file(path: PathBuf, bytes: &[u8]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create parent dir {}", parent.display()))?;
    }
    let mut file = File::create(&path)
        .await
        .with_context(|| format!("create {}", path.display()))?;
    file.write_all(bytes)
        .await
        .with_context(|| format!("write {}", path.display()))?;
    file.flush()
        .await
        .with_context(|| format!("flush {}", path.display()))
}

pub async fn write_error_response_meta(
    request_dir: &Path,
    started_at: String,
    error: String,
) -> anyhow::Result<()> {
    let response_meta = ResponseMeta {
        status: 502,
        started_at,
        completed_at: now_rfc3339(),
        response_body_bytes: 0,
        sse_event_count: 0,
        upstream_error: Some(error),
    };
    write_json_file(request_dir.join("response_meta.json"), &response_meta).await
}

pub async fn write_manifest(state: &AppState, session_id: &str) -> anyhow::Result<()> {
    let session_dir = state.output_dir.join(session_id);
    fs::create_dir_all(&session_dir)
        .await
        .with_context(|| format!("create session dir {}", session_dir.display()))?;
    let request_count = {
        let counters = state.counters.lock().await;
        counters
            .get(&format!("{}:{session_id}", state.profile.name))
            .copied()
            .unwrap_or_default()
    };
    let path = session_dir.join("manifest.json");
    let created_at = match fs::read_to_string(&path).await {
        Ok(existing) => serde_json::from_str::<serde_json::Value>(&existing)
            .ok()
            .and_then(|value| {
                value
                    .get("created_at")
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            })
            .unwrap_or_else(now_rfc3339),
        Err(_) => now_rfc3339(),
    };
    let manifest = Manifest {
        session_id: session_id.to_owned(),
        created_at,
        updated_at: now_rfc3339(),
        request_count,
        recorder: RecorderManifest {
            version: env!("CARGO_PKG_VERSION"),
            profile: state.profile.name.clone(),
            session_header: state.session_header.to_string(),
            upstream_base_url: state.profile.upstream.to_string(),
            unsafe_record_secrets: state.unsafe_record_secrets,
            proxy_mode: state.proxy_mode,
        },
    };
    write_json_file(path, &manifest).await
}

pub async fn write_websocket_meta(request_dir: &Path, meta: WebSocketMeta) -> anyhow::Result<()> {
    write_json_file(request_dir.join("websocket_meta.json"), &meta).await
}
