use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use anyhow::{anyhow, Context};
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

#[cfg(unix)]
const PRIVATE_DIR_MODE: u32 = 0o700;
#[cfg(unix)]
const PRIVATE_FILE_MODE: u32 = 0o600;

pub async fn create_private_dir_all(path: &Path) -> anyhow::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    builder.mode(PRIVATE_DIR_MODE);
    builder
        .create(path)
        .await
        .with_context(|| format!("create private dir {}", path.display()))?;

    #[cfg(unix)]
    fs::set_permissions(path, std::fs::Permissions::from_mode(PRIVATE_DIR_MODE))
        .await
        .with_context(|| format!("set private dir permissions on {}", path.display()))?;

    Ok(())
}

pub async fn create_private_file(path: &Path) -> anyhow::Result<File> {
    if let Some(parent) = path.parent() {
        create_private_dir_all(parent).await?;
    }
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    options.mode(PRIVATE_FILE_MODE);
    let file = options
        .open(path)
        .await
        .with_context(|| format!("create private file {}", path.display()))?;
    set_private_file_permissions(&file, path).await?;
    Ok(file)
}

async fn set_private_file_permissions(file: &File, path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    file.set_permissions(std::fs::Permissions::from_mode(PRIVATE_FILE_MODE))
        .await
        .with_context(|| format!("set private file permissions on {}", path.display()))?;

    #[cfg(not(unix))]
    let _ = (file, path);

    Ok(())
}

pub async fn append_access_log_line(path: &Path, line: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        create_private_dir_all(parent)
            .await
            .with_context(|| format!("create access log dir {}", parent.display()))?;
    }
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    options.mode(PRIVATE_FILE_MODE);
    let mut file = options
        .open(path)
        .await
        .with_context(|| format!("open access log {}", path.display()))?;
    set_private_file_permissions(&file, path).await?;
    file.write_all(line.as_bytes())
        .await
        .with_context(|| format!("append access log {}", path.display()))?;
    Ok(())
}

pub fn headers_to_records(headers: &HeaderMap) -> Vec<HeaderRecord> {
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
    write_bytes_file_atomically(path, &bytes).await
}

static ATOMIC_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

async fn write_bytes_file_atomically(path: PathBuf, bytes: &[u8]) -> anyhow::Result<()> {
    let pending = PendingAtomicFile::create(path).await?;
    pending.write_and_commit(bytes).await
}

struct PendingAtomicFile {
    final_path: PathBuf,
    temporary_path: PathBuf,
    file: Option<File>,
    committed: bool,
}

impl PendingAtomicFile {
    async fn create(final_path: PathBuf) -> anyhow::Result<Self> {
        let parent = final_path
            .parent()
            .ok_or_else(|| anyhow!("atomic file path has no parent: {}", final_path.display()))?;
        create_private_dir_all(parent).await?;

        let file_name = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("recording.json");
        for _ in 0..100 {
            let id = ATOMIC_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
            let temporary_path =
                parent.join(format!(".{file_name}.{}.{}.tmp", std::process::id(), id));
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            options.mode(PRIVATE_FILE_MODE);
            match options.open(&temporary_path).await {
                Ok(file) => {
                    set_private_file_permissions(&file, &temporary_path).await?;
                    return Ok(Self {
                        final_path,
                        temporary_path,
                        file: Some(file),
                        committed: false,
                    });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("create atomic temporary file {}", temporary_path.display())
                    });
                }
            }
        }

        Err(anyhow!(
            "could not allocate atomic temporary file for {}",
            final_path.display()
        ))
    }

    async fn write_and_commit(mut self, bytes: &[u8]) -> anyhow::Result<()> {
        let file = self.file.as_mut().expect("pending atomic file owns a file");
        file.write_all(bytes)
            .await
            .with_context(|| format!("write {}", self.temporary_path.display()))?;
        file.flush()
            .await
            .with_context(|| format!("flush {}", self.temporary_path.display()))?;
        self.file.take();
        fs::rename(&self.temporary_path, &self.final_path)
            .await
            .with_context(|| {
                format!(
                    "publish atomic file {} as {}",
                    self.temporary_path.display(),
                    self.final_path.display()
                )
            })?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for PendingAtomicFile {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.temporary_path);
        }
    }
}

pub async fn write_bytes_file(path: PathBuf, bytes: &[u8]) -> anyhow::Result<()> {
    let mut file = create_private_file(&path).await?;
    file.write_all(bytes)
        .await
        .with_context(|| format!("write {}", path.display()))?;
    file.flush()
        .await
        .with_context(|| format!("flush {}", path.display()))
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn incomplete_atomic_write_never_truncates_final_file() {
        let temp = tempfile::tempdir().unwrap();
        let final_path = temp.path().join("response_meta.json");
        std::fs::write(&final_path, b"previous metadata\n").unwrap();

        let pending = PendingAtomicFile::create(final_path.clone()).await.unwrap();
        assert_eq!(std::fs::read(&final_path).unwrap(), b"previous metadata\n");
        let temporary_path = pending.temporary_path.clone();
        assert!(temporary_path.exists());

        drop(pending);

        assert_eq!(std::fs::read(&final_path).unwrap(), b"previous metadata\n");
        assert!(!temporary_path.exists());
    }

    #[tokio::test]
    async fn json_file_is_published_without_leaving_a_temporary_file() {
        let temp = tempfile::tempdir().unwrap();
        let final_path = temp.path().join("response_meta.json");

        write_json_file(final_path.clone(), &serde_json::json!({"status": 200}))
            .await
            .unwrap();

        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&final_path).unwrap()).unwrap();
        assert_eq!(value["status"], 200);
        assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recording_paths_are_owner_only() {
        let temp = tempfile::tempdir().unwrap();
        let recording_dir = temp
            .path()
            .join("profile/recordings/session/requests/000000");
        create_private_dir_all(&recording_dir).await.unwrap();

        let header_path = recording_dir.join("request_headers.json");
        std::fs::write(&header_path, b"old headers\n").unwrap();
        std::fs::set_permissions(&header_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_json_file(
            header_path.clone(),
            &serde_json::json!({"authorization": "secret"}),
        )
        .await
        .unwrap();

        let body_path = recording_dir.join("request_body.raw");
        std::fs::write(&body_path, b"old body").unwrap();
        std::fs::set_permissions(&body_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_bytes_file(body_path.clone(), b"new body")
            .await
            .unwrap();

        let access_log_path = temp.path().join("profile/access.log");
        std::fs::write(&access_log_path, b"old log\n").unwrap();
        std::fs::set_permissions(&access_log_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        append_access_log_line(&access_log_path, "new log\n")
            .await
            .unwrap();

        let mut path = recording_dir.as_path();
        while path != temp.path() {
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                PRIVATE_DIR_MODE
            );
            path = path.parent().unwrap();
        }
        for path in [header_path, body_path, access_log_path] {
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                PRIVATE_FILE_MODE
            );
        }
    }
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
    create_private_dir_all(&session_dir)
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
        },
    };
    write_json_file(path, &manifest).await
}

pub async fn write_websocket_meta(request_dir: &Path, meta: WebSocketMeta) -> anyhow::Result<()> {
    write_json_file(request_dir.join("websocket_meta.json"), &meta).await
}

pub async fn recording_failure(
    request_dir: Option<&Path>,
    stage: &str,
    error: &(dyn std::fmt::Display + Sync),
) {
    tracing::warn!(
        recording_stage = stage,
        error = %error,
        request_dir = ?request_dir.map(|path| path.display().to_string()),
        "recording failed; proxy forwarding is unaffected and the recording is incomplete"
    );

    let Some(request_dir) = request_dir else {
        return;
    };
    let marker = serde_json::json!({
        "incomplete": true,
        "stage": stage,
        "error": error.to_string(),
        "updated_at": now_rfc3339(),
    });
    if let Err(marker_error) =
        write_json_file(request_dir.join("recording_incomplete.json"), &marker).await
    {
        tracing::warn!(
            recording_stage = stage,
            error = %marker_error,
            request_dir = %request_dir.display(),
            "failed to persist incomplete recording marker"
        );
    }
}
