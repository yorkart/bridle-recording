use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io::Read,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context};
use axum::{
    extract::{Path as AxumPath, State},
    http::{header::CONTENT_TYPE, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::{DateTime, Utc};
use flate2::read::MultiGzDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{fs, io::AsyncWriteExt};

use crate::{
    constants::UNKNOWN_SESSION,
    sse::{ParsedSseEventWithRaw, SseParser},
    types::{GatewayState, HeaderRecord, HeaderValueRecord, RequestMeta, ResponseMeta},
};

const REDACTED_TESTSET_HEADER_VALUE: &str = "******";
const REQUEST_TESTSET_HEADER_ALLOWLIST: &[&str] = &[
    "accept",
    "content-encoding",
    "content-length",
    "content-type",
    "host",
    "originator",
    "session-id",
    "thread-id",
    "user-agent",
    "x-codex-beta-features",
];
const RESPONSE_TESTSET_HEADER_ALLOWLIST: &[&str] = &[
    "cf-cache-status",
    "connection",
    "cross-origin-opener-policy",
    "date",
    "nel",
    "referrer-policy",
    "report-to",
    "server",
    "strict-transport-security",
    "transfer-encoding",
    "x-content-type-options",
    "x-models-etag",
    "x-openai-proxy-wasm",
];

pub async fn ui() -> Response {
    (
        [(CONTENT_TYPE, "text/html; charset=utf-8")],
        OBSERVABILITY_HTML,
    )
        .into_response()
}

pub async fn profiles(State(state): State<GatewayState>) -> Response {
    let mut profiles = state.profiles.keys().cloned().collect::<Vec<_>>();
    profiles.sort();
    Json(serde_json::json!({ "profiles": profiles })).into_response()
}

pub async fn testsets(State(state): State<GatewayState>) -> Response {
    match testsets_inner(&state.testsets_root, None).await {
        Ok(testsets) => Json(serde_json::json!({ "testsets": testsets })).into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

pub async fn profile_testsets(
    State(state): State<GatewayState>,
    AxumPath(profile): AxumPath<String>,
) -> Response {
    match testsets_inner(&state.testsets_root, Some(&profile)).await {
        Ok(testsets) => Json(serde_json::json!({ "testsets": testsets })).into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

pub async fn sessions(
    State(state): State<GatewayState>,
    AxumPath(profile): AxumPath<String>,
) -> Response {
    match sessions_inner(&state, &profile).await {
        Ok(sessions) => Json(serde_json::json!({ "sessions": sessions })).into_response(),
        Err(err) => api_error(StatusCode::NOT_FOUND, err),
    }
}

pub async fn session(
    State(state): State<GatewayState>,
    AxumPath((profile, session_id)): AxumPath<(String, String)>,
) -> Response {
    match session_inner(&state, &profile, &session_id).await {
        Ok(session) => Json(session).into_response(),
        Err(err) => api_error(StatusCode::NOT_FOUND, err),
    }
}

pub async fn save_testset(
    State(state): State<GatewayState>,
    AxumPath((profile, session_id)): AxumPath<(String, String)>,
    Json(request): Json<SaveTestsetRequest>,
) -> Response {
    match save_testset_inner(&state, &profile, &session_id, request).await {
        Ok(saved) => Json(saved).into_response(),
        Err(SaveTestsetError::Conflict(conflict)) => (
            StatusCode::CONFLICT,
            [(CONTENT_TYPE, "application/json")],
            serde_json::to_string(&conflict).unwrap_or_else(|_| "{}".to_owned()),
        )
            .into_response(),
        Err(SaveTestsetError::Other(err)) => api_error(StatusCode::BAD_REQUEST, err),
    }
}

pub async fn preview_testset(
    State(state): State<GatewayState>,
    AxumPath((profile, session_id)): AxumPath<(String, String)>,
    Json(request): Json<SaveTestsetRequest>,
) -> Response {
    match preview_testset_inner(&state, &profile, &session_id, &request).await {
        Ok(preview) => Json(preview).into_response(),
        Err(err) => api_error(StatusCode::BAD_REQUEST, err),
    }
}

fn api_error(status: StatusCode, err: anyhow::Error) -> Response {
    (
        status,
        [(CONTENT_TYPE, "application/json")],
        serde_json::json!({
            "error": "observability request failed",
            "detail": err.to_string()
        })
        .to_string(),
    )
        .into_response()
}

async fn save_testset_inner(
    state: &GatewayState,
    profile: &str,
    session_id: &str,
    request: SaveTestsetRequest,
) -> Result<SavedTestset, SaveTestsetError> {
    let profile_config = state
        .profiles
        .get(profile)
        .with_context(|| format!("unknown profile: {profile}"))?;
    let source_dir = profile_config.home_dir.join("recordings").join(session_id);
    if !fs::try_exists(&source_dir).await? {
        return Err(anyhow!("recording session not found: {}", source_dir.display()).into());
    }

    let observed = session_inner(state, profile, session_id).await?;
    let plan = build_testset_export_plan(&observed, &request)?;
    let first_user_input = plan.first_user_input.clone();
    let user_inputs = plan.user_inputs.clone();
    let user_input_sha256 = sha256_hex(first_user_input.as_bytes());
    let testset_dir = state.testsets_root.join(profile).join(&user_input_sha256);
    let raw_dir = testset_dir.join("raw").join(session_id);

    if fs::try_exists(&testset_dir).await? && !request.replace {
        return Err(SaveTestsetError::Conflict(SaveTestsetConflict {
            error: "testset already exists".to_owned(),
            replace_required: true,
            profile: profile.to_owned(),
            session_id: session_id.to_owned(),
            first_user_input,
            user_input_sha256,
            testset_path: testset_dir.display().to_string(),
        }));
    }

    let temp_dir = state
        .testsets_root
        .join(profile)
        .join(format!(".{user_input_sha256}.tmp"));
    if fs::try_exists(&temp_dir).await? {
        fs::remove_dir_all(&temp_dir).await?;
    }
    fs::create_dir_all(&temp_dir).await?;
    export_testset_session(
        &source_dir,
        &temp_dir.join("raw").join(session_id),
        &plan,
        &request,
    )
    .await?;

    let manifest = TestsetManifest {
        version: 2,
        profile: profile.to_owned(),
        source_session_id: session_id.to_owned(),
        first_user_input: first_user_input.clone(),
        user_inputs,
        user_input_sha256: user_input_sha256.clone(),
        saved_at: crate::util::now_rfc3339(),
        source_recording_path: source_dir.display().to_string(),
        raw_recording_path: format!("raw/{session_id}"),
        export: Some(TestsetExportManifest {
            selected_requests: plan
                .requests
                .iter()
                .map(|selected| selected.source_index.clone())
                .collect(),
            redact_sensitive_headers: request.redact_sensitive_headers,
            sensitive_value_count: request.sensitive_values().len(),
            remove: request.remove.clone(),
        }),
    };
    write_json_pretty(temp_dir.join("testset.json"), &manifest).await?;

    if fs::try_exists(&testset_dir).await? {
        fs::remove_dir_all(&testset_dir).await?;
    }
    fs::rename(&temp_dir, &testset_dir).await?;

    Ok(SavedTestset {
        status: if request.replace { "replaced" } else { "saved" }.to_owned(),
        profile: profile.to_owned(),
        session_id: session_id.to_owned(),
        first_user_input,
        user_input_sha256,
        testset_path: testset_dir.display().to_string(),
        raw_path: raw_dir.display().to_string(),
        selected_requests: plan.requests.len(),
    })
}

async fn testsets_inner(
    testsets_dir: &Path,
    profile_filter: Option<&str>,
) -> anyhow::Result<Vec<TestsetSummary>> {
    let mut out = Vec::new();
    let mut profile_entries = match fs::read_dir(&testsets_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(err) => return Err(err).with_context(|| format!("read {}", testsets_dir.display())),
    };

    while let Some(profile_entry) = profile_entries.next_entry().await? {
        if !profile_entry.file_type().await?.is_dir() {
            continue;
        }
        let Some(profile) = profile_entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        if profile_filter.is_some_and(|filter| filter != profile) {
            continue;
        }

        let mut testset_entries = fs::read_dir(profile_entry.path())
            .await
            .with_context(|| format!("read testsets for profile {profile}"))?;
        while let Some(testset_entry) = testset_entries.next_entry().await? {
            if !testset_entry.file_type().await?.is_dir() {
                continue;
            }
            let Some(id) = testset_entry.file_name().to_str().map(ToOwned::to_owned) else {
                continue;
            };
            if id.starts_with('.') {
                continue;
            }
            let manifest_path = testset_entry.path().join("testset.json");
            let manifest = read_json::<TestsetManifest>(&manifest_path)
                .await
                .with_context(|| format!("load testset manifest {}", manifest_path.display()))?;
            let user_inputs = if manifest.user_inputs.is_empty() {
                vec![manifest.first_user_input.clone()]
            } else {
                manifest.user_inputs.clone()
            };
            out.push(TestsetSummary {
                profile: manifest.profile,
                id,
                source_session_id: manifest.source_session_id,
                first_user_input: manifest.first_user_input,
                user_inputs,
                user_input_sha256: manifest.user_input_sha256,
                saved_at: manifest.saved_at,
                source_recording_path: manifest.source_recording_path,
                raw_recording_path: manifest.raw_recording_path,
                testset_path: testset_entry.path().display().to_string(),
                export: manifest.export,
            });
        }
    }

    out.sort_by(|left, right| {
        left.profile
            .cmp(&right.profile)
            .then_with(|| left.first_user_input.cmp(&right.first_user_input))
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(out)
}

async fn sessions_inner(
    state: &GatewayState,
    profile: &str,
) -> anyhow::Result<Vec<ObservedSessionSummary>> {
    let profile = state
        .profiles
        .get(profile)
        .with_context(|| format!("unknown profile: {profile}"))?;
    let recordings_dir = profile.home_dir.join("recordings");
    let mut out = Vec::new();
    let mut entries = match fs::read_dir(&recordings_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(err) => return Err(err).with_context(|| format!("read {}", recordings_dir.display())),
    };

    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let Some(session_id) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        if session_id == UNKNOWN_SESSION {
            continue;
        }
        let manifest_path = entry.path().join("manifest.json");
        let manifest = read_json::<serde_json::Value>(&manifest_path).await.ok();
        let request_count = manifest
            .as_ref()
            .and_then(|manifest| manifest.get("request_count"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default();
        let created_at = manifest
            .as_ref()
            .and_then(|manifest| manifest.get("created_at"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_default();
        let updated_at = manifest
            .as_ref()
            .and_then(|manifest| manifest.get("updated_at"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_default();
        out.push(ObservedSessionSummary {
            session_id,
            profile: profile.name.clone(),
            created_at,
            updated_at,
            request_count,
        });
    }

    out.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
    Ok(out)
}

async fn session_inner(
    state: &GatewayState,
    profile: &str,
    session_id: &str,
) -> anyhow::Result<ObservedSession> {
    let profile_config = state
        .profiles
        .get(profile)
        .with_context(|| format!("unknown profile: {profile}"))?;
    let session_dir = profile_config.home_dir.join("recordings").join(session_id);
    let manifest = read_json::<serde_json::Value>(&session_dir.join("manifest.json"))
        .await
        .with_context(|| format!("load session manifest {}", session_dir.display()))?;
    let requests = load_observed_calls(&session_dir).await?;
    let turns = build_turns(&requests);
    Ok(ObservedSession {
        profile: profile.to_owned(),
        session_id: session_id.to_owned(),
        raw_root: session_dir.display().to_string(),
        manifest,
        turns,
        requests,
    })
}

async fn load_observed_calls(session_dir: &Path) -> anyhow::Result<Vec<ObservedCall>> {
    let requests_dir = session_dir.join("requests");
    let mut entries = fs::read_dir(&requests_dir)
        .await
        .with_context(|| format!("read {}", requests_dir.display()))?;
    let mut request_dirs = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            request_dirs.push(entry.path());
        }
    }
    request_dirs.sort();

    let mut calls = Vec::new();
    for request_dir in request_dirs {
        let Some(index) = request_dir
            .file_name()
            .and_then(|name| name.to_str())
            .map(ToOwned::to_owned)
        else {
            continue;
        };
        calls.push(load_observed_call(index, request_dir).await?);
    }
    attach_tool_results(&mut calls);
    Ok(calls)
}

async fn load_observed_call(index: String, request_dir: PathBuf) -> anyhow::Result<ObservedCall> {
    let request_meta = read_json::<RequestMeta>(&request_dir.join("request_meta.json")).await?;
    let request_meta_value = serde_json::to_value(&request_meta)?;
    let files = request_files(&request_dir).await?;
    let is_websocket = files.iter().any(|file| {
        matches!(
            file.name.as_str(),
            "websocket_meta.json" | "websocket_frames.jsonl" | "websocket_response_headers.json"
        )
    });
    let websocket_connected = files
        .iter()
        .any(|file| file.name == "websocket_response_headers.json");
    let websocket_meta = if is_websocket {
        read_json::<serde_json::Value>(&request_dir.join("websocket_meta.json"))
            .await
            .ok()
    } else {
        None
    };
    let (websocket_frames, websocket_warning) = if is_websocket {
        load_websocket_frames(&request_dir.join("websocket_frames.jsonl")).await?
    } else {
        (Vec::new(), None)
    };
    let (response_meta, mut recording_state, mut recording_warning) = if is_websocket {
        let error = websocket_meta
            .as_ref()
            .and_then(|meta| meta.get("error"))
            .and_then(serde_json::Value::as_str)
            .filter(|error| !error.is_empty())
            .map(ToOwned::to_owned);
        let state = if error.is_some() {
            "degraded"
        } else if websocket_meta.is_some() {
            "complete"
        } else {
            "incomplete"
        };
        (
            None,
            state.to_owned(),
            error.or_else(|| {
                websocket_meta
                    .is_none()
                    .then(|| "WebSocket metadata is missing or still being finalized".to_owned())
            }),
        )
    } else {
        load_response_meta(&request_dir.join("response_meta.json")).await
    };
    if let Some(warning) = websocket_warning {
        recording_warning = Some(match recording_warning {
            Some(existing) => format!("{existing}; {warning}"),
            None => warning,
        });
        recording_state = "incomplete".to_owned();
    }
    let request_body_bytes = fs::read(request_dir.join("request_body.raw"))
        .await
        .with_context(|| format!("read request body in {}", request_dir.display()))?;
    if let Some(incomplete_warning) = load_recording_incomplete(
        &request_dir.join("recording_incomplete.json"),
        &request_dir,
        request_body_bytes.len(),
    )
    .await
    {
        recording_state = "incomplete".to_owned();
        recording_warning = Some(match recording_warning {
            Some(existing) => format!("{existing}; {incomplete_warning}"),
            None => incomplete_warning,
        });
    }
    let request_body = if request_body_bytes.is_empty() {
        serde_json::Value::Null
    } else {
        decode_request_body_json(&request_body_bytes).unwrap_or_else(|_| {
            serde_json::json!({
                "raw_body": observed_payload(&request_body_bytes),
                "note": "request body is not JSON or zstd-compressed JSON"
            })
        })
    };
    let mut response_decode_warning = None;
    let sse_path = request_dir.join("response_sse.raw");
    let (sse, response_body, recorded_response_body_bytes) = if fs::try_exists(&sse_path).await? {
        let sse_bytes = fs::read(&sse_path)
            .await
            .with_context(|| format!("read response_sse.raw in {}", request_dir.display()))?;
        let byte_count = sse_bytes.len();
        let observed_bytes =
            match decode_response_body_for_observability(&request_dir, &sse_bytes).await {
                Ok(bytes) => bytes,
                Err(err) => {
                    response_decode_warning =
                        Some(format!("response body could not be decoded: {err}"));
                    sse_bytes
                }
            };
        (parse_response_sse(&observed_bytes), None, Some(byte_count))
    } else {
        let body_path = request_dir.join("response_body.raw");
        if fs::try_exists(&body_path).await? {
            let body = fs::read(&body_path)
                .await
                .with_context(|| format!("read response_body.raw in {}", request_dir.display()))?;
            let byte_count = body.len();
            let observed_body =
                match decode_response_body_for_observability(&request_dir, &body).await {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        response_decode_warning =
                            Some(format!("response body could not be decoded: {err}"));
                        body
                    }
                };
            if looks_like_sse_response(&observed_body) {
                (parse_response_sse(&observed_body), None, Some(byte_count))
            } else {
                (
                    ParsedResponseSse::default(),
                    Some(observed_payload(&observed_body)),
                    Some(byte_count),
                )
            }
        } else {
            if is_websocket {
                (ParsedResponseSse::default(), None, None)
            } else {
                let preview = match response_meta.as_ref() {
                    Some(meta) if meta.upstream_error.is_some() => format!(
                        "<response stream failed: {}>",
                        meta.upstream_error
                            .as_deref()
                            .unwrap_or("unknown upstream error")
                    ),
                    Some(meta) if meta.response_body_bytes == 0 => {
                        "<empty response body>".to_owned()
                    }
                    Some(_) => "<response body recording unavailable>".to_owned(),
                    None => "<response recording incomplete>".to_owned(),
                };
                (
                    ParsedResponseSse::default(),
                    Some(ObservedPayload {
                        encoding: "utf8".to_owned(),
                        content: preview,
                        bytes: 0,
                    }),
                    None,
                )
            }
        }
    };
    if let Some(warning) = response_decode_warning {
        if recording_state == "complete" {
            recording_state = "degraded".to_owned();
        }
        recording_warning = Some(match recording_warning {
            Some(existing) => format!("{existing}; {warning}"),
            None => warning,
        });
    }

    let prompt_blocks = prompt_blocks(&request_body);
    let visible_user_messages = visible_user_messages(&prompt_blocks);
    let tool_definitions = tool_definitions(&request_body);
    let previous_tool_outputs = previous_tool_outputs(&request_body);
    let previous_function_calls = previous_function_calls(&request_body);
    let previous_assistant_messages = previous_assistant_messages(&prompt_blocks);

    let http_duration_ms = response_meta
        .as_ref()
        .and_then(|meta| duration_ms(&meta.started_at, &meta.completed_at));
    let response_id = sse
        .completed_response
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_owned();
    let model = request_body
        .get("model")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            sse.completed_response
                .get("model")
                .and_then(serde_json::Value::as_str)
        })
        .unwrap_or("unknown")
        .to_owned();
    let stream = request_body
        .get("stream")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let input_count = request_body
        .get("input")
        .or_else(|| request_body.get("messages"))
        .and_then(serde_json::Value::as_array)
        .map_or(0, Vec::len);
    let completed_at = if is_websocket {
        websocket_meta
            .as_ref()
            .and_then(|meta| meta.get("completed_at"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned()
    } else {
        response_meta
            .as_ref()
            .map(|meta| meta.completed_at.clone())
            .unwrap_or_default()
    };
    let response_meta_value = response_meta
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;
    let output_text = if sse.output_text.trim().is_empty() {
        response_body
            .as_ref()
            .map(|payload| payload.content.clone())
            .unwrap_or_default()
    } else {
        sse.output_text.trim().to_owned()
    };
    let protocol = if is_websocket { "websocket" } else { "http" }.to_owned();
    let timeline = build_call_timeline(
        &request_meta.started_at,
        response_meta.as_ref(),
        websocket_meta.as_ref(),
        &sse.events,
        &websocket_frames,
    );

    Ok(ObservedCall {
        index,
        request_id: if response_id.is_empty() {
            format!("request-{}", request_meta.index)
        } else {
            response_id
        },
        started_at: request_meta.started_at,
        completed_at,
        duration_ms: if is_websocket {
            websocket_meta.as_ref().and_then(|meta| {
                duration_ms(
                    meta.get("started_at")?.as_str()?,
                    meta.get("completed_at")?.as_str()?,
                )
            })
        } else {
            http_duration_ms
        },
        method: request_meta.method,
        path: request_meta.path,
        status: if is_websocket {
            if websocket_connected {
                101
            } else {
                0
            }
        } else {
            response_meta.as_ref().map(|meta| meta.status).unwrap_or(0)
        },
        protocol,
        recording_state,
        recording_warning,
        model,
        stream,
        input_count,
        tools_count: tool_definitions.len(),
        tool_names: tool_definitions
            .iter()
            .take(16)
            .map(|tool| tool.name.clone())
            .collect(),
        tool_definitions,
        prompt_blocks,
        visible_user_messages,
        previous_tool_outputs,
        previous_function_calls,
        previous_assistant_messages,
        function_calls: sse.function_calls,
        output_text,
        response_body,
        usage: observed_usage(&sse.completed_response),
        event_counts: sse.event_counts,
        sse_events: sse.events,
        websocket_frames,
        websocket_meta,
        request_meta: request_meta_value,
        response_meta: response_meta_value,
        request_body,
        timeline,
        files,
        raw_dir: request_dir.display().to_string(),
        request_body_bytes: request_meta.request_body_bytes,
        response_body_bytes: response_meta
            .as_ref()
            .map(|meta| meta.response_body_bytes)
            .or(recorded_response_body_bytes)
            .unwrap_or(0),
    })
}

async fn load_response_meta(path: &Path) -> (Option<ResponseMeta>, String, Option<String>) {
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return (
                None,
                "incomplete".to_owned(),
                Some("response metadata is missing or still being finalized".to_owned()),
            );
        }
        Err(err) => {
            return (
                None,
                "degraded".to_owned(),
                Some(format!("response metadata could not be read: {err}")),
            );
        }
    };

    if bytes.is_empty() {
        return (
            None,
            "incomplete".to_owned(),
            Some("response metadata is empty or still being finalized".to_owned()),
        );
    }

    match serde_json::from_slice::<ResponseMeta>(&bytes) {
        Ok(meta) => {
            let warning = meta
                .upstream_error
                .as_ref()
                .map(|err| format!("upstream response stream failed: {err}"));
            let state = if warning.is_some() {
                "degraded"
            } else {
                "complete"
            };
            (Some(meta), state.to_owned(), warning)
        }
        Err(err) => (
            None,
            "incomplete".to_owned(),
            Some(format!(
                "response metadata is invalid or still being finalized: {err}"
            )),
        ),
    }
}

async fn load_recording_incomplete(
    path: &Path,
    request_dir: &Path,
    recorded_request_body_bytes: usize,
) -> Option<String> {
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return None,
        Err(err) => {
            return Some(format!(
                "incomplete recording marker could not be read: {err}"
            ))
        }
    };
    let marker = match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(marker) => marker,
        Err(err) => return Some(format!("incomplete recording marker is invalid: {err}")),
    };
    let stage = marker
        .get("stage")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    if stage == "http_request_body_stream"
        && request_body_matches_content_length(request_dir, recorded_request_body_bytes).await
    {
        return None;
    }
    let error = marker
        .get("error")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("recording write failed");
    Some(format!("recording failed during {stage}: {error}"))
}

async fn request_body_matches_content_length(request_dir: &Path, recorded_bytes: usize) -> bool {
    let Ok(headers) =
        read_json::<Vec<HeaderRecord>>(&request_dir.join("request_headers.json")).await
    else {
        return false;
    };
    recorded_header_text(&headers, "content-length")
        .and_then(|value| value.trim().parse::<usize>().ok())
        == Some(recorded_bytes)
}

async fn decode_response_body_for_observability(
    request_dir: &Path,
    bytes: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let headers_path = request_dir.join("response_headers.json");
    if !fs::try_exists(&headers_path).await? {
        return Ok(bytes.to_vec());
    }
    let headers = read_json::<Vec<HeaderRecord>>(&headers_path)
        .await
        .with_context(|| format!("read {}", headers_path.display()))?;
    let Some(encodings) = recorded_header_text(&headers, "content-encoding") else {
        return Ok(bytes.to_vec());
    };

    let mut decoded = bytes.to_vec();
    for encoding in encodings
        .split(',')
        .map(str::trim)
        .filter(|encoding| !encoding.is_empty())
        .rev()
    {
        decoded = match encoding.to_ascii_lowercase().as_str() {
            "identity" => decoded,
            "gzip" | "x-gzip" => {
                let mut decoder = MultiGzDecoder::new(std::io::Cursor::new(&decoded));
                let mut out = Vec::new();
                decoder
                    .read_to_end(&mut out)
                    .with_context(|| format!("decode {encoding} response body"))?;
                out
            }
            "zstd" => {
                let mut decoder = zstd::stream::read::Decoder::new(std::io::Cursor::new(&decoded))
                    .context("create zstd response body decoder")?;
                let mut out = Vec::new();
                decoder
                    .read_to_end(&mut out)
                    .context("decode zstd response body")?;
                out
            }
            unsupported => anyhow::bail!("unsupported content-encoding: {unsupported}"),
        };
    }
    Ok(decoded)
}

fn recorded_header_text<'a>(headers: &'a [HeaderRecord], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case(name))
        .and_then(|header| match &header.value {
            HeaderValueRecord::Text { value } => Some(value.as_str()),
            HeaderValueRecord::BinaryBase64 { .. } => None,
        })
}

fn decode_request_body_json(bytes: &[u8]) -> anyhow::Result<serde_json::Value> {
    match serde_json::from_slice(bytes) {
        Ok(value) => Ok(value),
        Err(json_err) => {
            let mut decoder = zstd::stream::read::Decoder::new(std::io::Cursor::new(bytes))
                .context("create zstd request body decoder")?;
            let mut decoded = Vec::new();
            decoder
                .read_to_end(&mut decoded)
                .context("decode zstd request body")?;
            serde_json::from_slice(&decoded)
                .with_context(|| format!("parse decoded request body as json after {json_err}"))
        }
    }
}

fn observed_payload(bytes: &[u8]) -> ObservedPayload {
    match std::str::from_utf8(bytes) {
        Ok(text) => ObservedPayload {
            encoding: "utf8".to_owned(),
            content: text.to_owned(),
            bytes: bytes.len(),
        },
        Err(_) => ObservedPayload {
            encoding: "base64".to_owned(),
            content: BASE64.encode(bytes),
            bytes: bytes.len(),
        },
    }
}

fn looks_like_sse_response(bytes: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return false;
    };
    text.lines().any(|line| {
        let line = line.trim_start();
        line.starts_with("event:") || line.starts_with("data:")
    })
}

async fn load_websocket_frames(
    path: &Path,
) -> anyhow::Result<(Vec<ObservedWebSocketFrame>, Option<String>)> {
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), None)),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    let mut frames = Vec::new();
    let mut invalid_lines = 0;
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match serde_json::from_slice::<ObservedWebSocketFrame>(line) {
            Ok(frame) => frames.push(frame),
            Err(_) => invalid_lines += 1,
        }
    }
    frames.sort_by_key(|frame| frame.index);
    let warning = (invalid_lines > 0)
        .then(|| format!("{invalid_lines} WebSocket frame record(s) were incomplete or invalid"));
    Ok((frames, warning))
}

fn build_call_timeline(
    request_started_at: &str,
    response_meta: Option<&ResponseMeta>,
    websocket_meta: Option<&serde_json::Value>,
    sse_events: &[ObservedSseEvent],
    websocket_frames: &[ObservedWebSocketFrame],
) -> Vec<ObservedTimelineEvent> {
    let mut timeline = vec![ObservedTimelineEvent {
        sequence: 0,
        kind: "request_started".to_owned(),
        timestamp: Some(request_started_at.to_owned()),
        summary: "request headers and body entered the recorder".to_owned(),
    }];
    if let Some(meta) = response_meta {
        timeline.push(ObservedTimelineEvent {
            sequence: timeline.len(),
            kind: "response_started".to_owned(),
            timestamp: Some(meta.started_at.clone()),
            summary: format!("HTTP response {} started", meta.status),
        });
        for event in sse_events {
            timeline.push(ObservedTimelineEvent {
                sequence: timeline.len(),
                kind: "sse_event".to_owned(),
                timestamp: None,
                summary: format!("SSE #{} · {}", event.index, event.event_type),
            });
        }
        timeline.push(ObservedTimelineEvent {
            sequence: timeline.len(),
            kind: "response_completed".to_owned(),
            timestamp: Some(meta.completed_at.clone()),
            summary: format!(
                "HTTP response completed · {} bytes",
                meta.response_body_bytes
            ),
        });
    } else if let Some(meta) = websocket_meta {
        timeline.push(ObservedTimelineEvent {
            sequence: timeline.len(),
            kind: "websocket_connected".to_owned(),
            timestamp: meta
                .get("started_at")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            summary: "WebSocket upstream connected".to_owned(),
        });
        for frame in websocket_frames {
            timeline.push(ObservedTimelineEvent {
                sequence: timeline.len(),
                kind: "websocket_frame".to_owned(),
                timestamp: Some(frame.timestamp.clone()),
                summary: format!(
                    "frame #{} · {} · {}",
                    frame.index, frame.direction, frame.opcode
                ),
            });
        }
        timeline.push(ObservedTimelineEvent {
            sequence: timeline.len(),
            kind: "websocket_completed".to_owned(),
            timestamp: meta
                .get("completed_at")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            summary: meta
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
        });
    } else if !websocket_frames.is_empty() {
        timeline.push(ObservedTimelineEvent {
            sequence: timeline.len(),
            kind: "websocket_observed".to_owned(),
            timestamp: websocket_frames
                .first()
                .map(|frame| frame.timestamp.clone()),
            summary: "WebSocket frames were recorded before final metadata".to_owned(),
        });
        for frame in websocket_frames {
            timeline.push(ObservedTimelineEvent {
                sequence: timeline.len(),
                kind: "websocket_frame".to_owned(),
                timestamp: Some(frame.timestamp.clone()),
                summary: format!(
                    "frame #{} · {} · {}",
                    frame.index, frame.direction, frame.opcode
                ),
            });
        }
    } else if !sse_events.is_empty() {
        timeline.push(ObservedTimelineEvent {
            sequence: timeline.len(),
            kind: "response_observed".to_owned(),
            timestamp: None,
            summary: "SSE events were recorded before final response metadata".to_owned(),
        });
        for event in sse_events {
            timeline.push(ObservedTimelineEvent {
                sequence: timeline.len(),
                kind: "sse_event".to_owned(),
                timestamp: None,
                summary: format!("SSE #{} · {}", event.index, event.event_type),
            });
        }
    }
    timeline
}

fn attach_tool_results(calls: &mut [ObservedCall]) {
    let results = calls
        .iter()
        .flat_map(|call| call.previous_tool_outputs.iter())
        .filter(|output| !output.call_id.is_empty())
        .map(|output| (output.call_id.clone(), output.output.clone()))
        .collect::<HashMap<_, _>>();
    for call in calls {
        for tool_call in call
            .function_calls
            .iter_mut()
            .chain(call.previous_function_calls.iter_mut())
        {
            tool_call.result = results.get(&tool_call.call_id).cloned();
        }
    }
}

fn parse_response_sse(bytes: &[u8]) -> ParsedResponseSse {
    let mut parser = SseParser::default();
    let mut event_counts = BTreeMap::new();
    let mut events = Vec::new();
    let mut output_text = String::new();
    let mut function_calls = Vec::new();
    let mut tool_inputs = HashMap::new();
    let mut claude_tool_inputs = BTreeMap::<usize, ClaudeToolInput>::new();
    let mut completed_response = serde_json::Value::Object(serde_json::Map::new());

    for event in parser.push(bytes) {
        let data = event.data.join("\n");
        let value = serde_json::from_str::<serde_json::Value>(&data).ok();
        let event_type = value
            .as_ref()
            .and_then(|value| value.get("type"))
            .and_then(serde_json::Value::as_str)
            .or(event.event.as_deref())
            .unwrap_or("unknown")
            .to_owned();
        *event_counts.entry(event_type.clone()).or_insert(0) += 1;
        events.push(ObservedSseEvent {
            index: events.len(),
            event: event.event,
            id: event.id,
            retry: event.retry,
            event_type: event_type.clone(),
            data,
            raw: String::from_utf8_lossy(&event.raw).into_owned(),
        });

        let Some(value) = value else {
            continue;
        };

        match event_type.as_str() {
            "message_start" => {
                completed_response = value
                    .get("message")
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
            }
            "content_block_start" => {
                let index = value
                    .get("index")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|index| usize::try_from(index).ok());
                let block = value.get("content_block");
                match block
                    .and_then(|block| block.get("type"))
                    .and_then(serde_json::Value::as_str)
                {
                    Some("text") => {
                        if let Some(text) = block
                            .and_then(|block| block.get("text"))
                            .and_then(serde_json::Value::as_str)
                        {
                            output_text.push_str(text);
                        }
                    }
                    Some("tool_use") => {
                        if let (Some(index), Some(block)) = (index, block) {
                            let initial_input = block
                                .get("input")
                                .filter(|input| {
                                    !input.as_object().is_some_and(serde_json::Map::is_empty)
                                })
                                .and_then(|input| serde_json::to_string(input).ok())
                                .unwrap_or_default();
                            claude_tool_inputs.insert(
                                index,
                                ClaudeToolInput {
                                    id: block
                                        .get("id")
                                        .and_then(serde_json::Value::as_str)
                                        .unwrap_or_default()
                                        .to_owned(),
                                    name: block
                                        .get("name")
                                        .and_then(serde_json::Value::as_str)
                                        .unwrap_or("unknown")
                                        .to_owned(),
                                    input: initial_input,
                                },
                            );
                        }
                    }
                    _ => {}
                }
            }
            "content_block_delta" => {
                let delta = value.get("delta");
                match delta
                    .and_then(|delta| delta.get("type"))
                    .and_then(serde_json::Value::as_str)
                {
                    Some("text_delta") => {
                        if let Some(text) = delta
                            .and_then(|delta| delta.get("text"))
                            .and_then(serde_json::Value::as_str)
                        {
                            output_text.push_str(text);
                        }
                    }
                    Some("input_json_delta") => {
                        if let (Some(index), Some(partial_json)) = (
                            value
                                .get("index")
                                .and_then(serde_json::Value::as_u64)
                                .and_then(|index| usize::try_from(index).ok()),
                            delta
                                .and_then(|delta| delta.get("partial_json"))
                                .and_then(serde_json::Value::as_str),
                        ) {
                            if let Some(tool) = claude_tool_inputs.get_mut(&index) {
                                tool.input.push_str(partial_json);
                            }
                        }
                    }
                    _ => {}
                }
            }
            "content_block_stop" => {
                if let Some(tool) = value
                    .get("index")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|index| usize::try_from(index).ok())
                    .and_then(|index| claude_tool_inputs.remove(&index))
                {
                    function_calls.push(ObservedFunctionCall {
                        id: tool.id.clone(),
                        call_id: tool.id,
                        name: tool.name,
                        status: "completed".to_owned(),
                        arguments: pretty_json_str(&tool.input),
                        result: None,
                    });
                }
            }
            "message_delta" => {
                merge_json_object(
                    &mut completed_response,
                    value.get("delta").unwrap_or(&serde_json::Value::Null),
                );
                if let Some(usage) = value.get("usage") {
                    let completed_usage = completed_response.as_object_mut().and_then(|response| {
                        response
                            .entry("usage")
                            .or_insert_with(|| serde_json::json!({}))
                            .as_object_mut()
                    });
                    if let (Some(completed_usage), Some(usage)) =
                        (completed_usage, usage.as_object())
                    {
                        completed_usage.extend(usage.clone());
                    }
                }
            }
            "response.output_text.delta" => {
                if let Some(delta) = value.get("delta").and_then(serde_json::Value::as_str) {
                    output_text.push_str(delta);
                }
            }
            "response.function_call_arguments.done" | "response.custom_tool_call_input.done" => {
                if let (Some(item_id), Some(input)) = (
                    value.get("item_id").and_then(serde_json::Value::as_str),
                    value
                        .get("arguments")
                        .or_else(|| value.get("input"))
                        .and_then(serde_json::Value::as_str),
                ) {
                    tool_inputs.insert(item_id.to_owned(), input.to_owned());
                }
            }
            "response.output_item.done" => {
                if let Some(item) = value.get("item") {
                    if item.get("type").and_then(serde_json::Value::as_str) == Some("message") {
                        if output_text.is_empty() {
                            output_text.push_str(&content_text(item.get("content")));
                        }
                    } else if is_tool_call_type(
                        item.get("type").and_then(serde_json::Value::as_str),
                    ) {
                        let item_id = item
                            .get("id")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default();
                        let arguments = item
                            .get("arguments")
                            .or_else(|| item.get("input"))
                            .and_then(serde_json::Value::as_str)
                            .map(ToOwned::to_owned)
                            .or_else(|| tool_inputs.get(item_id).cloned())
                            .unwrap_or_default();
                        function_calls.push(ObservedFunctionCall {
                            id: item_id.to_owned(),
                            call_id: item
                                .get("call_id")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or_default()
                                .to_owned(),
                            name: item
                                .get("name")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("unknown")
                                .to_owned(),
                            status: item
                                .get("status")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or_default()
                                .to_owned(),
                            arguments: pretty_json_str(&arguments),
                            result: None,
                        });
                    }
                }
            }
            "response.completed" => {
                completed_response = value
                    .get("response")
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
            }
            _ => {}
        }
    }

    ParsedResponseSse {
        output_text,
        function_calls,
        completed_response,
        event_counts,
        events,
    }
}

fn merge_json_object(target: &mut serde_json::Value, source: &serde_json::Value) {
    let (Some(target), Some(source)) = (target.as_object_mut(), source.as_object()) else {
        return;
    };
    target.extend(source.clone());
}

fn observed_usage(response: &serde_json::Value) -> Option<serde_json::Value> {
    let mut usage = response.get("usage")?.clone();
    let usage_object = usage.as_object_mut()?;
    if !usage_object.contains_key("total_tokens") {
        let total_tokens = [
            "input_tokens",
            "output_tokens",
            "cache_creation_input_tokens",
            "cache_read_input_tokens",
        ]
        .into_iter()
        .filter_map(|key| usage_object.get(key).and_then(serde_json::Value::as_u64))
        .sum::<u64>();
        usage_object.insert("total_tokens".to_owned(), total_tokens.into());
    }
    Some(usage)
}

fn prompt_blocks(request_body: &serde_json::Value) -> Vec<PromptBlock> {
    let mut blocks = Vec::new();
    if let Some(instructions) = request_body
        .get("instructions")
        .and_then(serde_json::Value::as_str)
        .filter(|instructions| !instructions.trim().is_empty())
    {
        blocks.push(PromptBlock {
            role: "system".to_owned(),
            block_type: "system".to_owned(),
            chars: instructions.chars().count(),
            excerpt: excerpt(instructions, 760),
            text: instructions.to_owned(),
        });
    }

    append_prompt_content_blocks(&mut blocks, "system", request_body.get("system"));

    if let Some(input) = request_body
        .get("input")
        .and_then(serde_json::Value::as_array)
    {
        blocks.extend(input.iter().filter_map(|item| {
            if item.get("type").and_then(serde_json::Value::as_str) != Some("message") {
                return None;
            }
            let role = item
                .get("role")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("message")
                .to_owned();
            let text = content_text(item.get("content"));
            let block_type = classify_prompt_block(&role, &text);
            Some(PromptBlock {
                role,
                block_type,
                chars: text.chars().count(),
                excerpt: excerpt(&text, 760),
                text,
            })
        }));
    }
    if let Some(messages) = request_body
        .get("messages")
        .and_then(serde_json::Value::as_array)
    {
        for message in messages {
            let role = message
                .get("role")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("message");
            append_prompt_content_blocks(&mut blocks, role, message.get("content"));
        }
    }
    blocks
}

fn append_prompt_content_blocks(
    blocks: &mut Vec<PromptBlock>,
    role: &str,
    content: Option<&serde_json::Value>,
) {
    let texts = match content {
        Some(serde_json::Value::String(text)) => vec![text.as_str()],
        Some(serde_json::Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(serde_json::Value::as_str))
            .collect(),
        _ => Vec::new(),
    };
    for text in texts.into_iter().filter(|text| !text.trim().is_empty()) {
        blocks.push(PromptBlock {
            role: role.to_owned(),
            block_type: classify_prompt_block(role, text),
            chars: text.chars().count(),
            excerpt: excerpt(text, 760),
            text: text.to_owned(),
        });
    }
}

fn visible_user_messages(blocks: &[PromptBlock]) -> Vec<String> {
    let remove_derived = TestsetRemovalOptions {
        skills: true,
        apps: true,
        plugins: true,
        derived_prompt: true,
        ..TestsetRemovalOptions::default()
    };
    blocks
        .iter()
        .filter(|block| block.role == "user")
        .filter(|block| block.block_type != "system_reminder")
        .filter_map(|block| {
            let mut text = block.text.clone();
            trim_prompt_text(&mut text, &remove_derived).then(|| text.trim().to_owned())
        })
        .collect()
}

fn previous_assistant_messages(blocks: &[PromptBlock]) -> Vec<String> {
    blocks
        .iter()
        .filter(|block| block.role == "assistant")
        .map(|block| block.excerpt.clone())
        .collect()
}

fn classify_prompt_block(role: &str, text: &str) -> String {
    let trimmed = text.trim_start();
    if trimmed.starts_with("<environment_context>") {
        "environment"
    } else if trimmed.starts_with("<system-reminder>") {
        "system_reminder"
    } else if trimmed.starts_with("<permissions instructions>") {
        "permissions"
    } else if trimmed.starts_with("<skills_instructions>") {
        "skills"
    } else if trimmed.starts_with("<apps_instructions>") {
        "apps"
    } else if trimmed.starts_with("<plugins_instructions>")
        || trimmed.starts_with("<recommended_plugins>")
    {
        "plugins"
    } else {
        role
    }
    .to_owned()
}

fn tool_definitions(request_body: &serde_json::Value) -> Vec<ToolDefinition> {
    let top_level = request_body
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten();
    let additional = request_body
        .get("input")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| {
            item.get("type").and_then(serde_json::Value::as_str) == Some("additional_tools")
        })
        .flat_map(|item| {
            item.get("tools")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
        });

    top_level
        .chain(additional)
        .map(|tool| {
            let function = tool.get("function");
            let name = tool
                .get("name")
                .or_else(|| function.and_then(|function| function.get("name")))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_owned();
            let tool_type = tool
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("function")
                .to_owned();
            let description = tool
                .get("description")
                .or_else(|| function.and_then(|function| function.get("description")))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            ToolDefinition {
                name,
                tool_type,
                description: description.to_owned(),
                definition: tool.clone(),
            }
        })
        .collect()
}

fn previous_tool_outputs(request_body: &serde_json::Value) -> Vec<ToolOutput> {
    let mut outputs = request_body
        .get("input")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| {
            matches!(
                item.get("type").and_then(serde_json::Value::as_str),
                Some("function_call_output" | "custom_tool_call_output")
            )
        })
        .map(|item| ToolOutput {
            call_id: item
                .get("call_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            output: tool_output_text(item.get("output")),
        })
        .collect::<Vec<_>>();
    outputs.extend(
        request_body
            .get("messages")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .flat_map(|message| {
                message
                    .get("content")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .filter(|part| {
                part.get("type").and_then(serde_json::Value::as_str) == Some("tool_result")
            })
            .map(|part| ToolOutput {
                call_id: part
                    .get("tool_use_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                output: tool_output_text(part.get("content")),
            }),
    );
    outputs
}

fn previous_function_calls(request_body: &serde_json::Value) -> Vec<ObservedFunctionCall> {
    let mut calls = request_body
        .get("input")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| is_tool_call_type(item.get("type").and_then(serde_json::Value::as_str)))
        .map(|item| ObservedFunctionCall {
            id: item
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            call_id: item
                .get("call_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            name: item
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
            status: item
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            arguments: pretty_json_str(
                item.get("arguments")
                    .or_else(|| item.get("input"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default(),
            ),
            result: None,
        })
        .collect::<Vec<_>>();
    calls.extend(
        request_body
            .get("messages")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter(|message| {
                message.get("role").and_then(serde_json::Value::as_str) == Some("assistant")
            })
            .flat_map(|message| {
                message
                    .get("content")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .filter(|part| part.get("type").and_then(serde_json::Value::as_str) == Some("tool_use"))
            .map(|part| {
                let id = part
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                ObservedFunctionCall {
                    id: id.clone(),
                    call_id: id,
                    name: part
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown")
                        .to_owned(),
                    status: "completed".to_owned(),
                    arguments: pretty_json_value(part.get("input")),
                    result: None,
                }
            }),
    );
    calls
}

fn is_tool_call_type(item_type: Option<&str>) -> bool {
    matches!(item_type, Some("function_call" | "custom_tool_call"))
}

async fn request_files(request_dir: &Path) -> anyhow::Result<Vec<ObservedFile>> {
    let mut out = Vec::new();
    let mut entries = fs::read_dir(request_dir)
        .await
        .with_context(|| format!("read {}", request_dir.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        let metadata = entry.metadata().await?;
        out.push(ObservedFile {
            name: entry.file_name().to_string_lossy().to_string(),
            bytes: metadata.len(),
        });
    }
    out.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(out)
}

fn build_turns(calls: &[ObservedCall]) -> Vec<ObservedTurn> {
    let mut turns: Vec<ObservedTurn> = Vec::new();
    for call in calls {
        let user = call
            .visible_user_messages
            .last()
            .cloned()
            .unwrap_or_else(|| "(no visible user input)".to_owned());
        let should_start = turns.last().map(|turn| turn.user != user).unwrap_or(true);
        if should_start {
            turns.push(ObservedTurn {
                id: format!("turn-{:06}", turns.len()),
                user,
                started_at: call.started_at.clone(),
                calls: Vec::new(),
                assistant: String::new(),
                tool_outputs: Vec::new(),
            });
        }
        let turn = turns
            .last_mut()
            .expect("turn inserted before attaching observed call");
        if !call.output_text.is_empty() {
            turn.assistant = call.output_text.clone();
        }
        for output in &call.previous_tool_outputs {
            if !turn
                .tool_outputs
                .iter()
                .any(|existing| existing.call_id == output.call_id)
            {
                turn.tool_outputs.push(output.clone());
            }
        }
        turn.calls.push(call.clone());
    }
    turns
}

fn content_text(value: Option<&serde_json::Value>) -> String {
    match value {
        Some(serde_json::Value::String(text)) => text.clone(),
        Some(serde_json::Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| {
                part.get("text")
                    .or_else(|| part.get("output_text"))
                    .and_then(serde_json::Value::as_str)
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn tool_output_text(value: Option<&serde_json::Value>) -> String {
    let text = content_text(value);
    if !text.is_empty() {
        return text;
    }
    match value {
        None | Some(serde_json::Value::Null) => String::new(),
        Some(value) => serde_json::to_string_pretty(value).unwrap_or_default(),
    }
}

fn excerpt(text: &str, max_chars: usize) -> String {
    let text = text.trim();
    if text.chars().count() <= max_chars {
        return text.to_owned();
    }
    let mut out = text.chars().take(max_chars).collect::<String>();
    out.push_str("...");
    out
}

fn pretty_json_str(text: &str) -> String {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|value| serde_json::to_string_pretty(&value).ok())
        .unwrap_or_else(|| text.to_owned())
}

fn pretty_json_value(value: Option<&serde_json::Value>) -> String {
    match value {
        Some(serde_json::Value::String(text)) => pretty_json_str(text),
        Some(value) => serde_json::to_string_pretty(value).unwrap_or_default(),
        None => String::new(),
    }
}

fn duration_ms(started_at: &str, completed_at: &str) -> Option<i64> {
    let start = started_at.parse::<DateTime<Utc>>().ok()?;
    let end = completed_at.parse::<DateTime<Utc>>().ok()?;
    Some((end - start).num_milliseconds())
}

async fn read_json<T>(path: &Path) -> anyhow::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let bytes = fs::read(path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

async fn write_json_pretty<T>(path: PathBuf, value: &T) -> anyhow::Result<()>
where
    T: Serialize,
{
    let mut bytes = serde_json::to_vec_pretty(value).context("serialize testset JSON")?;
    bytes.push(b'\n');
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let mut file = fs::File::create(&path)
        .await
        .with_context(|| format!("create {}", path.display()))?;
    file.write_all(&bytes)
        .await
        .with_context(|| format!("write {}", path.display()))?;
    file.flush()
        .await
        .with_context(|| format!("flush {}", path.display()))
}

async fn preview_testset_inner(
    state: &GatewayState,
    profile: &str,
    session_id: &str,
    request: &SaveTestsetRequest,
) -> anyhow::Result<TestsetPreview> {
    let observed = session_inner(state, profile, session_id).await?;
    let plan = build_testset_export_plan(&observed, request)?;
    Ok(TestsetPreview {
        profile: profile.to_owned(),
        session_id: session_id.to_owned(),
        first_user_input: plan.first_user_input,
        user_inputs: plan.user_inputs,
        source_request_count: observed.requests.len(),
        selected_request_count: plan.requests.len(),
        removed_request_count: observed.requests.len().saturating_sub(plan.requests.len()),
        redact_sensitive_headers: request.redact_sensitive_headers,
        sensitive_value_count: request.sensitive_values().len(),
        remove: request.remove.clone(),
        requests: plan
            .requests
            .iter()
            .map(|selected| {
                let call = observed
                    .requests
                    .iter()
                    .find(|call| call.index == selected.source_index)
                    .expect("export plan only contains observed requests");
                TestsetPreviewRequest {
                    source_index: selected.source_index.clone(),
                    export_index: selected.export_index.clone(),
                    protocol: call.protocol.clone(),
                    method: call.method.clone(),
                    path: call.path.clone(),
                    prompt_block_types: prompt_blocks(&selected.request_body)
                        .into_iter()
                        .map(|block| block.block_type)
                        .collect(),
                    tool_definitions: tool_definitions(&selected.request_body).len(),
                    sse_events: call
                        .sse_events
                        .iter()
                        .filter(|event| !request.remove.tools || !is_tool_sse_event(&event.data))
                        .count(),
                    websocket_frames: call
                        .websocket_frames
                        .iter()
                        .filter(|frame| !request.remove.tools || !is_tool_websocket_frame(frame))
                        .count(),
                    request_body: selected.request_body.clone(),
                }
            })
            .collect(),
    })
}

fn build_testset_export_plan(
    observed: &ObservedSession,
    request: &SaveTestsetRequest,
) -> anyhow::Result<TestsetExportPlan> {
    let requested = request
        .selected_requests
        .as_ref()
        .map(|indices| indices.iter().map(String::as_str).collect::<HashSet<_>>());
    if requested.as_ref().is_some_and(HashSet::is_empty) {
        return Err(anyhow!(
            "at least one request/response pair must be selected"
        ));
    }
    if let Some(requested) = requested.as_ref() {
        let available = observed
            .requests
            .iter()
            .map(|call| call.index.as_str())
            .collect::<HashSet<_>>();
        let mut unknown = requested
            .difference(&available)
            .copied()
            .collect::<Vec<_>>();
        unknown.sort_unstable();
        if !unknown.is_empty() {
            return Err(anyhow!(
                "unknown selected request indices: {}",
                unknown.join(", ")
            ));
        }
    }

    let sensitive_values = request.sensitive_values();
    let mut requests = Vec::new();
    let mut user_inputs = Vec::new();
    for call in &observed.requests {
        if requested
            .as_ref()
            .is_some_and(|requested| !requested.contains(call.index.as_str()))
        {
            continue;
        }
        let mut request_body = call.request_body.clone();
        trim_request_body(&mut request_body, &request.remove);
        redact_json_value(&mut request_body, &sensitive_values);
        for input in visible_user_messages(&prompt_blocks(&request_body)) {
            let input = input.trim();
            if !input.is_empty() && !user_inputs.iter().any(|existing| existing == input) {
                user_inputs.push(input.to_owned());
            }
        }
        requests.push(TestsetExportRequest {
            source_index: call.index.clone(),
            export_index: format!("{:06}", requests.len()),
            request_body,
        });
    }
    if requests.is_empty() {
        return Err(anyhow!(
            "at least one request/response pair must be selected"
        ));
    }
    let first_user_input = user_inputs
        .first()
        .cloned()
        .unwrap_or_else(|| format!("session:{}", observed.session_id));
    Ok(TestsetExportPlan {
        requests,
        first_user_input,
        user_inputs,
    })
}

fn trim_request_body(value: &mut serde_json::Value, remove: &TestsetRemovalOptions) {
    let Some(body) = value.as_object_mut() else {
        return;
    };
    if remove.tools {
        body.remove("tools");
        body.remove("tool_choice");
        body.remove("parallel_tool_calls");
    }
    let remove_instructions = match body.get_mut("instructions") {
        Some(serde_json::Value::String(instructions)) => !trim_prompt_text(instructions, remove),
        _ => false,
    };
    if remove_instructions {
        body.remove("instructions");
    }
    let Some(input) = body
        .get_mut("input")
        .and_then(serde_json::Value::as_array_mut)
    else {
        return;
    };
    input.retain_mut(|item| {
        if remove.tools
            && item
                .get("type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(is_tool_artifact_type)
        {
            return false;
        }
        let Some(content) = item.get_mut("content") else {
            return true;
        };
        match content {
            serde_json::Value::String(text) => trim_prompt_text(text, remove),
            serde_json::Value::Array(parts) => {
                parts.retain_mut(|part| match part {
                    serde_json::Value::String(text) => trim_prompt_text(text, remove),
                    serde_json::Value::Object(fields) => {
                        let text = if fields.contains_key("text") {
                            fields.get_mut("text")
                        } else {
                            fields.get_mut("output_text")
                        };
                        match text {
                            Some(serde_json::Value::String(text)) => trim_prompt_text(text, remove),
                            _ => true,
                        }
                    }
                    _ => true,
                });
                !parts.is_empty()
            }
            _ => true,
        }
    });
}

fn trim_prompt_text(text: &mut String, remove: &TestsetRemovalOptions) -> bool {
    let tagged_sections = [
        (
            remove.skills,
            "<skills_instructions>",
            "</skills_instructions>",
        ),
        (remove.apps, "<apps_instructions>", "</apps_instructions>"),
        (
            remove.plugins,
            "<plugins_instructions>",
            "</plugins_instructions>",
        ),
        (
            remove.plugins,
            "<recommended_plugins>",
            "</recommended_plugins>",
        ),
        (
            remove.derived_prompt,
            "<environment_context>",
            "</environment_context>",
        ),
        (
            remove.derived_prompt,
            "<permissions instructions>",
            "</permissions instructions>",
        ),
    ];
    for (enabled, open, close) in tagged_sections {
        if enabled {
            remove_tagged_sections(text, open, close);
        }
    }
    !text.trim().is_empty()
}

fn remove_tagged_sections(text: &mut String, open: &str, close: &str) {
    while let Some(start) = text.find(open) {
        let content_start = start + open.len();
        let Some(relative_end) = text[content_start..].find(close) else {
            break;
        };
        let end = content_start + relative_end + close.len();
        text.replace_range(start..end, "");
    }
}

fn redact_json_value(value: &mut serde_json::Value, sensitive_values: &[&str]) {
    match value {
        serde_json::Value::String(text) => {
            *text = redact_text(text, sensitive_values);
        }
        serde_json::Value::Array(values) => {
            for value in values {
                redact_json_value(value, sensitive_values);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                redact_json_value(value, sensitive_values);
            }
        }
        _ => {}
    }
}

fn redact_text(text: &str, sensitive_values: &[&str]) -> String {
    sensitive_values
        .iter()
        .fold(text.to_owned(), |text, value| {
            text.replace(value, REDACTED_TESTSET_HEADER_VALUE)
        })
}

async fn export_testset_session(
    source: &Path,
    destination: &Path,
    plan: &TestsetExportPlan,
    request: &SaveTestsetRequest,
) -> anyhow::Result<()> {
    fs::create_dir_all(destination.join("requests"))
        .await
        .with_context(|| format!("create {}", destination.display()))?;
    let sensitive_values = request.sensitive_values();
    let source_manifest = source.join("manifest.json");
    if fs::try_exists(&source_manifest).await? {
        let mut manifest = read_json::<serde_json::Value>(&source_manifest).await?;
        if let Some(manifest) = manifest.as_object_mut() {
            manifest.insert(
                "request_count".to_owned(),
                serde_json::json!(plan.requests.len()),
            );
        }
        redact_json_value(&mut manifest, &sensitive_values);
        write_json_pretty(destination.join("manifest.json"), &manifest).await?;
    }
    for selected in &plan.requests {
        export_testset_request(
            &source.join("requests").join(&selected.source_index),
            &destination.join("requests").join(&selected.export_index),
            selected,
            request,
        )
        .await?;
    }
    Ok(())
}

async fn export_testset_request(
    source: &Path,
    destination: &Path,
    selected: &TestsetExportRequest,
    request: &SaveTestsetRequest,
) -> anyhow::Result<()> {
    fs::create_dir_all(destination)
        .await
        .with_context(|| format!("create {}", destination.display()))?;
    let sensitive_values = request.sensitive_values();
    let source_request_body = fs::read(source.join("request_body.raw"))
        .await
        .with_context(|| format!("read request body in {}", source.display()))?;
    let request_body = transform_request_body(&source_request_body, request)?;
    let response_sse = transform_response_sse(&source.join("response_sse.raw"), request).await?;
    let response_body = transform_response_body(&source.join("response_body.raw"), request).await?;
    let websocket_frames =
        transform_websocket_frames(&source.join("websocket_frames.jsonl"), request).await?;
    let response_bytes = response_sse
        .as_ref()
        .or(response_body.as_ref())
        .map(Vec::len);

    let mut entries = fs::read_dir(source)
        .await
        .with_context(|| format!("read {}", source.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        let destination_path = destination.join(&name);
        match name_str {
            "request_match.json" | "response_rewrite.json" => {}
            "request_body.raw" => write_bytes_file(&destination_path, &request_body).await?,
            "response_sse.raw" => {
                if let Some(bytes) = response_sse.as_ref() {
                    write_bytes_file(&destination_path, bytes).await?;
                }
            }
            "response_body.raw" => {
                if let Some(bytes) = response_body.as_ref() {
                    write_bytes_file(&destination_path, bytes).await?;
                }
            }
            "websocket_frames.jsonl" => {
                if let Some(bytes) = websocket_frames.as_ref() {
                    write_bytes_file(&destination_path, bytes).await?;
                }
            }
            "request_meta.json" => {
                let mut meta = read_json::<serde_json::Value>(&entry.path()).await?;
                if let Some(meta) = meta.as_object_mut() {
                    meta.insert(
                        "index".to_owned(),
                        serde_json::json!(selected.export_index.parse::<u64>()?),
                    );
                    meta.insert(
                        "request_body_bytes".to_owned(),
                        serde_json::json!(request_body.len()),
                    );
                }
                redact_json_value(&mut meta, &sensitive_values);
                write_json_pretty(destination_path, &meta).await?;
            }
            "response_meta.json" => {
                let mut meta = read_json::<serde_json::Value>(&entry.path()).await?;
                if let Some(meta) = meta.as_object_mut() {
                    if let Some(response_bytes) = response_bytes {
                        meta.insert(
                            "response_body_bytes".to_owned(),
                            serde_json::json!(response_bytes),
                        );
                    }
                    if let Some(bytes) = response_sse.as_ref() {
                        meta.insert(
                            "sse_event_count".to_owned(),
                            serde_json::json!(parse_response_sse(bytes).events.len()),
                        );
                    }
                }
                redact_json_value(&mut meta, &sensitive_values);
                write_json_pretty(destination_path, &meta).await?;
            }
            "request_headers.json" => {
                export_header_file(
                    &entry.path(),
                    &destination_path,
                    REQUEST_TESTSET_HEADER_ALLOWLIST,
                    request,
                    Some(request_body.len()),
                )
                .await?;
            }
            "response_headers.json" => {
                export_header_file(
                    &entry.path(),
                    &destination_path,
                    RESPONSE_TESTSET_HEADER_ALLOWLIST,
                    request,
                    response_bytes,
                )
                .await?;
            }
            "websocket_response_headers.json" => {
                export_header_file(
                    &entry.path(),
                    &destination_path,
                    RESPONSE_TESTSET_HEADER_ALLOWLIST,
                    request,
                    None,
                )
                .await?;
            }
            "websocket_meta.json" => {
                let mut meta = read_json::<serde_json::Value>(&entry.path()).await?;
                if let Some(frames) = websocket_frames.as_deref() {
                    let (client_to_upstream, upstream_to_client) = websocket_frame_counts(frames);
                    if let Some(meta) = meta.as_object_mut() {
                        meta.insert(
                            "client_to_upstream_frames".to_owned(),
                            serde_json::json!(client_to_upstream),
                        );
                        meta.insert(
                            "upstream_to_client_frames".to_owned(),
                            serde_json::json!(upstream_to_client),
                        );
                    }
                }
                redact_json_value(&mut meta, &sensitive_values);
                write_json_pretty(destination_path, &meta).await?;
            }
            _ => {
                let bytes = fs::read(entry.path()).await?;
                let bytes = redact_bytes(&bytes, &sensitive_values);
                write_bytes_file(&destination_path, &bytes).await?;
            }
        }
    }
    Ok(())
}

fn transform_request_body(bytes: &[u8], request: &SaveTestsetRequest) -> anyhow::Result<Vec<u8>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let sensitive_values = request.sensitive_values();
    if sensitive_values.is_empty() && !request.remove.any() {
        return Ok(bytes.to_vec());
    }
    let (mut value, compressed) = match serde_json::from_slice::<serde_json::Value>(bytes) {
        Ok(value) => (value, false),
        Err(json_err) => {
            let mut decoder = match zstd::stream::read::Decoder::new(std::io::Cursor::new(bytes)) {
                Ok(decoder) => decoder,
                Err(_) if !request.remove.any() => {
                    return Ok(redact_bytes(bytes, &sensitive_values));
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("decode request body for trimming after {json_err}")
                    })
                }
            };
            let mut decoded = Vec::new();
            decoder
                .read_to_end(&mut decoded)
                .context("decode zstd request body for trimming")?;
            (
                serde_json::from_slice(&decoded)
                    .with_context(|| format!("parse decoded request body after {json_err}"))?,
                true,
            )
        }
    };
    trim_request_body(&mut value, &request.remove);
    redact_json_value(&mut value, &sensitive_values);
    let json = serde_json::to_vec(&value).context("serialize trimmed request body")?;
    if compressed {
        zstd::encode_all(std::io::Cursor::new(json), 0)
            .context("compress trimmed request body as zstd")
    } else {
        Ok(json)
    }
}

async fn transform_response_body(
    path: &Path,
    request: &SaveTestsetRequest,
) -> anyhow::Result<Option<Vec<u8>>> {
    match fs::read(path).await {
        Ok(bytes) => Ok(Some(transform_protocol_payload(&bytes, request)?.0)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("read {}", path.display())),
    }
}

async fn transform_response_sse(
    path: &Path,
    request: &SaveTestsetRequest,
) -> anyhow::Result<Option<Vec<u8>>> {
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    let sensitive_values = request.sensitive_values();
    if !request.remove.tools {
        return Ok(Some(redact_bytes(&bytes, &sensitive_values)));
    }
    let mut parser = SseParser::default();
    let mut out = Vec::with_capacity(bytes.len());
    for event in parser.push(&bytes) {
        let data = event.data.join("\n");
        let Ok(mut value) = serde_json::from_str::<serde_json::Value>(&data) else {
            out.extend_from_slice(&redact_bytes(&event.raw, &sensitive_values));
            continue;
        };
        if is_tool_event_value(&value) {
            continue;
        }
        if remove_tool_artifacts(&mut value) {
            redact_json_value(&mut value, &sensitive_values);
            let data = serde_json::to_string(&value)?;
            write_sse_event(&mut out, &event, &data);
        } else {
            out.extend_from_slice(&redact_bytes(&event.raw, &sensitive_values));
        }
    }
    Ok(Some(out))
}

fn is_tool_sse_event(data: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(data).is_ok_and(|value| is_tool_event_value(&value))
}

async fn transform_websocket_frames(
    path: &Path,
    request: &SaveTestsetRequest,
) -> anyhow::Result<Option<Vec<u8>>> {
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    let sensitive_values = request.sensitive_values();
    if sensitive_values.is_empty() && !request.remove.tools {
        return Ok(Some(bytes));
    }
    let mut out = Vec::with_capacity(bytes.len());
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let body = line.strip_suffix(b"\n").unwrap_or(line);
        if body.is_empty() {
            out.extend_from_slice(line);
            continue;
        }
        let mut frame: serde_json::Value = serde_json::from_slice(body)
            .with_context(|| format!("parse WebSocket frame in {}", path.display()))?;
        if let Some(payload) = frame
            .get("payload_base64")
            .and_then(|value| value.as_str())
            .and_then(|payload| BASE64.decode(payload).ok())
        {
            let (transformed, is_tool_event) = transform_protocol_payload(&payload, request)?;
            if is_tool_event {
                continue;
            }
            if let Some(field) = frame.get_mut("payload_base64") {
                *field = serde_json::Value::String(BASE64.encode(&transformed));
            }
            if frame.get("text").is_some_and(serde_json::Value::is_string) {
                frame["text"] = serde_json::Value::String(
                    String::from_utf8(transformed).context("transform WebSocket text payload")?,
                );
            }
        } else if let Some(text) = frame.get("text").and_then(serde_json::Value::as_str) {
            let (transformed, is_tool_event) =
                transform_protocol_payload(text.as_bytes(), request)?;
            if is_tool_event {
                continue;
            }
            frame["text"] = serde_json::Value::String(
                String::from_utf8(transformed).context("transform WebSocket text payload")?,
            );
        }
        redact_json_value(&mut frame, &sensitive_values);
        serde_json::to_writer(&mut out, &frame)?;
        out.push(b'\n');
    }
    Ok(Some(out))
}

fn transform_protocol_payload(
    bytes: &[u8],
    request: &SaveTestsetRequest,
) -> anyhow::Result<(Vec<u8>, bool)> {
    let sensitive_values = request.sensitive_values();
    let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return Ok((redact_bytes(bytes, &sensitive_values), false));
    };
    if request.remove.tools && is_tool_event_value(&value) {
        return Ok((Vec::new(), true));
    }
    if request.remove.tools && remove_tool_artifacts(&mut value) {
        redact_json_value(&mut value, &sensitive_values);
        return Ok((serde_json::to_vec(&value)?, false));
    }
    Ok((redact_bytes(bytes, &sensitive_values), false))
}

fn write_sse_event(out: &mut Vec<u8>, event: &ParsedSseEventWithRaw, data: &str) {
    if let Some(event_type) = event.event.as_deref() {
        out.extend_from_slice(b"event: ");
        out.extend_from_slice(event_type.as_bytes());
        out.push(b'\n');
    }
    if let Some(id) = event.id.as_deref() {
        out.extend_from_slice(b"id: ");
        out.extend_from_slice(id.as_bytes());
        out.push(b'\n');
    }
    if let Some(retry) = event.retry.as_deref() {
        out.extend_from_slice(b"retry: ");
        out.extend_from_slice(retry.as_bytes());
        out.push(b'\n');
    }
    for line in data.lines() {
        out.extend_from_slice(b"data: ");
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
    }
    out.push(b'\n');
}

fn is_tool_artifact_type(item_type: &str) -> bool {
    item_type == "additional_tools"
        || item_type == "mcp_list_tools"
        || item_type.ends_with("_call")
        || item_type.ends_with("_call_output")
        || item_type.ends_with("_approval_request")
        || item_type.ends_with("_approval_response")
}

fn is_tool_artifact_value(value: &serde_json::Value) -> bool {
    value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .is_some_and(is_tool_artifact_type)
}

fn is_tool_event_value(value: &serde_json::Value) -> bool {
    let event_type = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    is_tool_artifact_type(event_type)
        || event_type.contains("_call")
        || value.get("item").is_some_and(is_tool_artifact_value)
}

fn remove_tool_artifacts(value: &mut serde_json::Value) -> bool {
    match value {
        serde_json::Value::Array(values) => {
            let original_len = values.len();
            values.retain(|value| !is_tool_artifact_value(value));
            let mut changed = values.len() != original_len;
            for value in values {
                changed |= remove_tool_artifacts(value);
            }
            changed
        }
        serde_json::Value::Object(values) => {
            let mut changed = false;
            for key in ["tools", "tool_choice", "parallel_tool_calls"] {
                changed |= values.remove(key).is_some();
            }
            let keys = values.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                let remove = values.get(&key).is_some_and(is_tool_artifact_value);
                if remove {
                    values.remove(&key);
                    changed = true;
                } else if let Some(value) = values.get_mut(&key) {
                    changed |= remove_tool_artifacts(value);
                }
            }
            changed
        }
        _ => false,
    }
}

fn is_tool_websocket_frame(frame: &ObservedWebSocketFrame) -> bool {
    let payload = frame
        .payload_base64
        .as_deref()
        .and_then(|payload| BASE64.decode(payload).ok())
        .or_else(|| frame.text.as_deref().map(|text| text.as_bytes().to_vec()));
    payload.as_deref().is_some_and(|payload| {
        serde_json::from_slice::<serde_json::Value>(payload)
            .is_ok_and(|value| is_tool_event_value(&value))
    })
}

fn websocket_frame_counts(bytes: &[u8]) -> (usize, usize) {
    let mut client_to_upstream = 0;
    let mut upstream_to_client = 0;
    for line in bytes.split(|byte| *byte == b'\n') {
        let direction = serde_json::from_slice::<serde_json::Value>(line)
            .ok()
            .and_then(|frame| frame.get("direction")?.as_str().map(ToOwned::to_owned));
        match direction.as_deref() {
            Some("client_to_upstream") => client_to_upstream += 1,
            Some("upstream_to_client") => upstream_to_client += 1,
            _ => {}
        }
    }
    (client_to_upstream, upstream_to_client)
}

fn redact_bytes(bytes: &[u8], sensitive_values: &[&str]) -> Vec<u8> {
    sensitive_values
        .iter()
        .fold(bytes.to_vec(), |bytes, value| {
            replace_bytes(
                &bytes,
                value.as_bytes(),
                REDACTED_TESTSET_HEADER_VALUE.as_bytes(),
            )
        })
}

fn replace_bytes(bytes: &[u8], needle: &[u8], replacement: &[u8]) -> Vec<u8> {
    if needle.is_empty() {
        return bytes.to_vec();
    }
    let mut out = Vec::with_capacity(bytes.len());
    let mut offset = 0;
    while let Some(relative) = bytes[offset..]
        .windows(needle.len())
        .position(|window| window == needle)
    {
        let found = offset + relative;
        out.extend_from_slice(&bytes[offset..found]);
        out.extend_from_slice(replacement);
        offset = found + needle.len();
    }
    out.extend_from_slice(&bytes[offset..]);
    out
}

async fn export_header_file(
    source: &Path,
    destination: &Path,
    allowlist: &[&str],
    request: &SaveTestsetRequest,
    content_length: Option<usize>,
) -> anyhow::Result<()> {
    let mut records = read_json::<Vec<HeaderRecord>>(source).await?;
    if request.redact_sensitive_headers {
        redact_testset_header_records(&mut records, allowlist);
    }
    let sensitive_values = request.sensitive_values();
    for record in &mut records {
        match &mut record.value {
            HeaderValueRecord::Text { value } | HeaderValueRecord::BinaryBase64 { value } => {
                *value = redact_text(value, &sensitive_values);
            }
        }
    }
    if let Some(content_length) = content_length {
        if let Some(record) = records
            .iter_mut()
            .find(|record| record.name.eq_ignore_ascii_case("content-length"))
        {
            record.value = HeaderValueRecord::Text {
                value: content_length.to_string(),
            };
        }
    }
    write_json_pretty(destination.to_path_buf(), &records).await
}

fn redact_testset_header_records(records: &mut [HeaderRecord], allowlist: &[&str]) {
    for record in records {
        if allowlist
            .iter()
            .any(|allowed| record.name.eq_ignore_ascii_case(allowed))
        {
            continue;
        }
        match &mut record.value {
            HeaderValueRecord::Text { value } | HeaderValueRecord::BinaryBase64 { value } => {
                REDACTED_TESTSET_HEADER_VALUE.clone_into(value);
            }
        }
    }
}

async fn write_bytes_file(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let mut output = fs::File::create(path)
        .await
        .with_context(|| format!("create {}", path.display()))?;
    output
        .write_all(bytes)
        .await
        .with_context(|| format!("write {}", path.display()))?;
    output
        .flush()
        .await
        .with_context(|| format!("flush {}", path.display()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

enum SaveTestsetError {
    Conflict(SaveTestsetConflict),
    Other(anyhow::Error),
}

impl From<anyhow::Error> for SaveTestsetError {
    fn from(err: anyhow::Error) -> Self {
        Self::Other(err)
    }
}

impl From<std::io::Error> for SaveTestsetError {
    fn from(err: std::io::Error) -> Self {
        Self::Other(err.into())
    }
}

#[derive(Clone, Deserialize)]
pub struct SaveTestsetRequest {
    #[serde(default)]
    replace: bool,
    #[serde(default)]
    selected_requests: Option<Vec<String>>,
    #[serde(default = "default_true")]
    redact_sensitive_headers: bool,
    #[serde(default)]
    sensitive_values: Vec<String>,
    #[serde(default)]
    remove: TestsetRemovalOptions,
}

impl Default for SaveTestsetRequest {
    fn default() -> Self {
        Self {
            replace: false,
            selected_requests: None,
            redact_sensitive_headers: true,
            sensitive_values: Vec::new(),
            remove: TestsetRemovalOptions::default(),
        }
    }
}

impl SaveTestsetRequest {
    fn sensitive_values(&self) -> Vec<&str> {
        let mut values = Vec::new();
        for value in &self.sensitive_values {
            let value = value.trim();
            if !value.is_empty() && !values.contains(&value) {
                values.push(value);
            }
        }
        values
    }
}

const fn default_true() -> bool {
    true
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct TestsetRemovalOptions {
    #[serde(default)]
    tools: bool,
    #[serde(default)]
    skills: bool,
    #[serde(default)]
    apps: bool,
    #[serde(default)]
    plugins: bool,
    #[serde(default)]
    derived_prompt: bool,
}

impl TestsetRemovalOptions {
    fn any(&self) -> bool {
        self.tools || self.skills || self.apps || self.plugins || self.derived_prompt
    }
}

#[derive(Serialize)]
struct SaveTestsetConflict {
    error: String,
    replace_required: bool,
    profile: String,
    session_id: String,
    first_user_input: String,
    user_input_sha256: String,
    testset_path: String,
}

#[derive(Serialize)]
struct SavedTestset {
    status: String,
    profile: String,
    session_id: String,
    first_user_input: String,
    user_input_sha256: String,
    testset_path: String,
    raw_path: String,
    selected_requests: usize,
}

#[derive(Serialize, Deserialize)]
struct TestsetManifest {
    version: u32,
    profile: String,
    source_session_id: String,
    first_user_input: String,
    #[serde(default)]
    user_inputs: Vec<String>,
    user_input_sha256: String,
    saved_at: String,
    source_recording_path: String,
    raw_recording_path: String,
    #[serde(default)]
    export: Option<TestsetExportManifest>,
}

#[derive(Clone, Serialize, Deserialize)]
struct TestsetExportManifest {
    selected_requests: Vec<String>,
    redact_sensitive_headers: bool,
    sensitive_value_count: usize,
    remove: TestsetRemovalOptions,
}

#[derive(Serialize)]
struct TestsetSummary {
    profile: String,
    id: String,
    source_session_id: String,
    first_user_input: String,
    user_inputs: Vec<String>,
    user_input_sha256: String,
    saved_at: String,
    source_recording_path: String,
    raw_recording_path: String,
    testset_path: String,
    export: Option<TestsetExportManifest>,
}

struct TestsetExportPlan {
    requests: Vec<TestsetExportRequest>,
    first_user_input: String,
    user_inputs: Vec<String>,
}

struct TestsetExportRequest {
    source_index: String,
    export_index: String,
    request_body: serde_json::Value,
}

#[derive(Serialize)]
struct TestsetPreview {
    profile: String,
    session_id: String,
    first_user_input: String,
    user_inputs: Vec<String>,
    source_request_count: usize,
    selected_request_count: usize,
    removed_request_count: usize,
    redact_sensitive_headers: bool,
    sensitive_value_count: usize,
    remove: TestsetRemovalOptions,
    requests: Vec<TestsetPreviewRequest>,
}

#[derive(Serialize)]
struct TestsetPreviewRequest {
    source_index: String,
    export_index: String,
    protocol: String,
    method: String,
    path: String,
    prompt_block_types: Vec<String>,
    tool_definitions: usize,
    sse_events: usize,
    websocket_frames: usize,
    request_body: serde_json::Value,
}

#[derive(Serialize)]
struct ObservedSessionSummary {
    session_id: String,
    profile: String,
    created_at: String,
    updated_at: String,
    request_count: u64,
}

#[derive(Serialize)]
struct ObservedSession {
    profile: String,
    session_id: String,
    raw_root: String,
    manifest: serde_json::Value,
    turns: Vec<ObservedTurn>,
    requests: Vec<ObservedCall>,
}

#[derive(Clone, Serialize)]
struct ObservedTurn {
    id: String,
    user: String,
    started_at: String,
    calls: Vec<ObservedCall>,
    assistant: String,
    tool_outputs: Vec<ToolOutput>,
}

#[derive(Clone, Serialize)]
struct ObservedCall {
    index: String,
    request_id: String,
    started_at: String,
    completed_at: String,
    duration_ms: Option<i64>,
    method: String,
    path: String,
    status: u16,
    protocol: String,
    recording_state: String,
    recording_warning: Option<String>,
    model: String,
    stream: bool,
    input_count: usize,
    tools_count: usize,
    tool_names: Vec<String>,
    tool_definitions: Vec<ToolDefinition>,
    prompt_blocks: Vec<PromptBlock>,
    visible_user_messages: Vec<String>,
    previous_tool_outputs: Vec<ToolOutput>,
    previous_function_calls: Vec<ObservedFunctionCall>,
    previous_assistant_messages: Vec<String>,
    function_calls: Vec<ObservedFunctionCall>,
    output_text: String,
    response_body: Option<ObservedPayload>,
    usage: Option<serde_json::Value>,
    event_counts: BTreeMap<String, usize>,
    sse_events: Vec<ObservedSseEvent>,
    websocket_frames: Vec<ObservedWebSocketFrame>,
    websocket_meta: Option<serde_json::Value>,
    request_meta: serde_json::Value,
    response_meta: Option<serde_json::Value>,
    request_body: serde_json::Value,
    timeline: Vec<ObservedTimelineEvent>,
    files: Vec<ObservedFile>,
    raw_dir: String,
    request_body_bytes: usize,
    response_body_bytes: usize,
}

#[derive(Clone, Serialize)]
struct PromptBlock {
    role: String,
    #[serde(rename = "type")]
    block_type: String,
    chars: usize,
    excerpt: String,
    text: String,
}

#[derive(Clone, Serialize)]
struct ObservedFunctionCall {
    id: String,
    call_id: String,
    name: String,
    status: String,
    arguments: String,
    result: Option<String>,
}

#[derive(Clone, Serialize)]
struct ToolDefinition {
    name: String,
    #[serde(rename = "type")]
    tool_type: String,
    description: String,
    definition: serde_json::Value,
}

#[derive(Clone, Serialize)]
struct ToolOutput {
    call_id: String,
    output: String,
}

#[derive(Clone, Serialize)]
struct ObservedFile {
    name: String,
    bytes: u64,
}

#[derive(Clone, Serialize)]
struct ObservedPayload {
    encoding: String,
    content: String,
    bytes: usize,
}

#[derive(Clone, Serialize)]
struct ObservedSseEvent {
    index: usize,
    event: Option<String>,
    id: Option<String>,
    retry: Option<String>,
    event_type: String,
    data: String,
    raw: String,
}

#[derive(Clone, Deserialize, Serialize)]
struct ObservedWebSocketFrame {
    index: usize,
    direction: String,
    timestamp: String,
    opcode: String,
    text: Option<String>,
    payload_base64: Option<String>,
    close: Option<serde_json::Value>,
}

#[derive(Clone, Serialize)]
struct ObservedTimelineEvent {
    sequence: usize,
    kind: String,
    timestamp: Option<String>,
    summary: String,
}

#[derive(Default)]
struct ParsedResponseSse {
    output_text: String,
    function_calls: Vec<ObservedFunctionCall>,
    completed_response: serde_json::Value,
    event_counts: BTreeMap<String, usize>,
    events: Vec<ObservedSseEvent>,
}

struct ClaudeToolInput {
    id: String,
    name: String,
    input: String,
}

const OBSERVABILITY_HTML: &str = include_str!("observability_ui.html");

#[cfg(test)]
mod tests {
    use super::*;

    fn text_header(name: &str, value: &str) -> HeaderRecord {
        HeaderRecord {
            name: name.to_owned(),
            value: HeaderValueRecord::Text {
                value: value.to_owned(),
            },
        }
    }

    fn assert_text_value(record: &HeaderRecord, expected: &str) {
        assert!(matches!(
            &record.value,
            HeaderValueRecord::Text { value } if value == expected
        ));
    }

    fn assert_redacted_value(record: &HeaderRecord) {
        let value = match &record.value {
            HeaderValueRecord::Text { value } | HeaderValueRecord::BinaryBase64 { value } => value,
        };
        assert_eq!(value, REDACTED_TESTSET_HEADER_VALUE);
    }

    #[test]
    fn redacts_unknown_request_headers_and_preserves_allowlisted_values() {
        let mut records = REQUEST_TESTSET_HEADER_ALLOWLIST
            .iter()
            .map(|name| text_header(name, &format!("original-{name}")))
            .collect::<Vec<_>>();
        records.push(text_header("Authorization", "Bearer secret"));
        records.push(HeaderRecord {
            name: "x-binary-secret".to_owned(),
            value: HeaderValueRecord::BinaryBase64 {
                value: "c2VjcmV0".to_owned(),
            },
        });
        redact_testset_header_records(&mut records, REQUEST_TESTSET_HEADER_ALLOWLIST);

        for (record, name) in records.iter().zip(REQUEST_TESTSET_HEADER_ALLOWLIST) {
            assert_eq!(record.name, *name);
            assert_text_value(record, &format!("original-{name}"));
        }
        assert_eq!(records[10].name, "Authorization");
        assert_redacted_value(&records[10]);
        assert!(matches!(
            &records[11].value,
            HeaderValueRecord::BinaryBase64 { value }
                if value == REDACTED_TESTSET_HEADER_VALUE
        ));
    }

    #[test]
    fn redacts_unknown_response_headers_and_preserves_allowlisted_values() {
        let mut records = RESPONSE_TESTSET_HEADER_ALLOWLIST
            .iter()
            .map(|name| text_header(name, &format!("original-{name}")))
            .collect::<Vec<_>>();
        records.push(text_header("set-cookie", "session=secret"));

        redact_testset_header_records(&mut records, RESPONSE_TESTSET_HEADER_ALLOWLIST);

        for (record, name) in records.iter().zip(RESPONSE_TESTSET_HEADER_ALLOWLIST) {
            assert_eq!(record.name, *name);
            assert_text_value(record, &format!("original-{name}"));
        }
        assert_redacted_value(&records[13]);
    }

    #[tokio::test]
    async fn testset_copy_redacts_headers_without_changing_source_or_other_files() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("recording");
        let destination = temp.path().join("testset");
        let request_dir = source.join("requests/000000");
        fs::create_dir_all(&request_dir).await.unwrap();
        let request_headers = vec![
            text_header("SESSION-ID", "session-original"),
            text_header("authorization", "Bearer secret"),
        ];
        let response_headers = vec![
            text_header("x-models-etag", "etag-original"),
            text_header("set-cookie", "session=secret"),
        ];
        write_json_pretty(request_dir.join("request_headers.json"), &request_headers)
            .await
            .unwrap();
        write_json_pretty(request_dir.join("response_headers.json"), &response_headers)
            .await
            .unwrap();
        let body = b"body copied byte for byte\n";
        fs::write(request_dir.join("request_body.raw"), body)
            .await
            .unwrap();
        fs::write(request_dir.join("request_match.json"), b"derived matcher")
            .await
            .unwrap();
        fs::write(
            request_dir.join("response_rewrite.json"),
            b"derived rewrite",
        )
        .await
        .unwrap();

        export_testset_request(
            &request_dir,
            &destination.join("requests/000000"),
            &TestsetExportRequest {
                source_index: "000000".to_owned(),
                export_index: "000000".to_owned(),
                request_body: serde_json::Value::Null,
            },
            &SaveTestsetRequest::default(),
        )
        .await
        .unwrap();

        let source_request =
            read_json::<Vec<HeaderRecord>>(&request_dir.join("request_headers.json"))
                .await
                .unwrap();
        assert_text_value(&source_request[0], "session-original");
        assert_text_value(&source_request[1], "Bearer secret");

        let copied_request = read_json::<Vec<HeaderRecord>>(
            &destination.join("requests/000000/request_headers.json"),
        )
        .await
        .unwrap();
        assert_eq!(copied_request[0].name, "SESSION-ID");
        assert_text_value(&copied_request[0], "session-original");
        assert_eq!(copied_request[1].name, "authorization");
        assert_redacted_value(&copied_request[1]);

        let copied_response = read_json::<Vec<HeaderRecord>>(
            &destination.join("requests/000000/response_headers.json"),
        )
        .await
        .unwrap();
        assert_text_value(&copied_response[0], "etag-original");
        assert_redacted_value(&copied_response[1]);
        assert_eq!(
            fs::read(destination.join("requests/000000/request_body.raw"))
                .await
                .unwrap(),
            body
        );
        assert!(!destination
            .join("requests/000000/request_match.json")
            .exists());
        assert!(!destination
            .join("requests/000000/response_rewrite.json")
            .exists());
        assert!(request_dir.join("request_match.json").exists());
        assert!(request_dir.join("response_rewrite.json").exists());
    }

    #[test]
    fn parses_custom_tool_call_from_sse() {
        let bytes = br#"data: {"type":"response.custom_tool_call_input.done","item_id":"ctc_1","input":"{\"cmd\":\"pwd\"}"}

data: {"type":"response.output_item.done","item":{"type":"custom_tool_call","id":"ctc_1","call_id":"call_1","name":"exec","status":"completed"}}

"#;

        let parsed = parse_response_sse(bytes);

        assert_eq!(parsed.function_calls.len(), 1);
        let call = &parsed.function_calls[0];
        assert_eq!(call.id, "ctc_1");
        assert_eq!(call.call_id, "call_1");
        assert_eq!(call.name, "exec");
        assert_eq!(call.status, "completed");
        assert_eq!(call.arguments, "{\n  \"cmd\": \"pwd\"\n}");
    }

    #[test]
    fn preserves_order_and_raw_content_for_every_sse_event() {
        let bytes = b"event: response.created\r\nid: first\r\ndata: {\"type\":\"response.created\",\"secret\":\"value\"}\r\n\r\nevent: note\ndata: plain text\n\n";

        let parsed = parse_response_sse(bytes);

        assert_eq!(parsed.events.len(), 2);
        assert_eq!(parsed.events[0].index, 0);
        assert_eq!(parsed.events[0].event_type, "response.created");
        assert_eq!(parsed.events[0].id.as_deref(), Some("first"));
        assert_eq!(
            parsed.events[0].raw,
            "event: response.created\r\nid: first\r\ndata: {\"type\":\"response.created\",\"secret\":\"value\"}\r\n\r\n"
        );
        assert_eq!(parsed.events[1].index, 1);
        assert_eq!(parsed.events[1].event_type, "note");
        assert_eq!(parsed.events[1].data, "plain text");
        assert_eq!(parsed.event_counts["response.created"], 1);
        assert_eq!(parsed.event_counts["note"], 1);
    }

    #[test]
    fn parses_claude_messages_prompt_and_tool_history() {
        let body = serde_json::json!({
            "system": [
                {"type": "text", "text": "You are Claude Code."},
                {"type": "text", "text": "Follow the project instructions."}
            ],
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "<system-reminder>derived context</system-reminder>"},
                    {"type": "text", "text": "ping"}
                ]},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "toolu_1", "name": "Read", "input": {"file_path": "/tmp/a"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_1", "content": "contents"}
                ]}
            ]
        });

        let blocks = prompt_blocks(&body);
        assert_eq!(blocks.len(), 4);
        assert_eq!(blocks[0].role, "system");
        assert_eq!(blocks[2].block_type, "system_reminder");
        assert_eq!(visible_user_messages(&blocks), vec!["ping"]);

        let calls = previous_function_calls(&body);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].call_id, "toolu_1");
        assert_eq!(calls[0].name, "Read");
        assert!(calls[0].arguments.contains("file_path"));

        let outputs = previous_tool_outputs(&body);
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].call_id, "toolu_1");
        assert_eq!(outputs[0].output, "contents");
    }

    #[test]
    fn parses_claude_messages_sse_text_tools_and_usage() {
        let bytes = br#"event: message_start
data: {"type":"message_start","message":{"id":"msg_1","model":"claude-test","usage":{"input_tokens":10,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: content_block_start
data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"Read","input":{}}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"file_path\":\"/tmp/a\"}"}}

event: content_block_stop
data: {"type":"content_block_stop","index":1}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":5}}

event: message_stop
data: {"type":"message_stop"}

"#;

        let parsed = parse_response_sse(bytes);

        assert_eq!(parsed.output_text, "hello");
        assert_eq!(parsed.completed_response["id"], "msg_1");
        assert_eq!(parsed.completed_response["model"], "claude-test");
        assert_eq!(parsed.completed_response["stop_reason"], "tool_use");
        assert_eq!(parsed.completed_response["usage"]["input_tokens"], 10);
        assert_eq!(parsed.completed_response["usage"]["output_tokens"], 5);
        assert_eq!(
            observed_usage(&parsed.completed_response).unwrap()["total_tokens"],
            15
        );
        assert_eq!(parsed.function_calls.len(), 1);
        assert_eq!(parsed.function_calls[0].call_id, "toolu_1");
        assert_eq!(parsed.function_calls[0].name, "Read");
        assert!(parsed.function_calls[0].arguments.contains("file_path"));
        assert_eq!(parsed.event_counts["content_block_delta"], 2);
    }

    #[tokio::test]
    async fn decodes_gzip_response_only_for_observability() {
        let temp = tempfile::tempdir().unwrap();
        let request_dir = temp.path();
        write_json_pretty(
            request_dir.join("response_headers.json"),
            &vec![text_header("content-encoding", "gzip")],
        )
        .await
        .unwrap();
        let original = b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, original).unwrap();
        let encoded = encoder.finish().unwrap();

        let decoded = decode_response_body_for_observability(request_dir, &encoded)
            .await
            .unwrap();

        assert_eq!(decoded, original);
        assert_ne!(encoded, original);
    }

    #[tokio::test]
    async fn ignores_false_incomplete_marker_when_content_length_was_fully_recorded() {
        let temp = tempfile::tempdir().unwrap();
        write_json_pretty(
            temp.path().join("request_headers.json"),
            &vec![text_header("content-length", "4")],
        )
        .await
        .unwrap();
        write_json_pretty(
            temp.path().join("recording_incomplete.json"),
            &serde_json::json!({
                "incomplete": true,
                "stage": "http_request_body_stream",
                "error": "upstream stopped consuming the request body before it completed"
            }),
        )
        .await
        .unwrap();

        assert!(load_recording_incomplete(
            &temp.path().join("recording_incomplete.json"),
            temp.path(),
            4,
        )
        .await
        .is_none());
        assert!(load_recording_incomplete(
            &temp.path().join("recording_incomplete.json"),
            temp.path(),
            3,
        )
        .await
        .is_some());
    }

    #[tokio::test]
    async fn loads_websocket_frames_in_recorded_order_and_reports_partial_lines() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("websocket_frames.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"index\":1,\"direction\":\"upstream_to_client\",\"timestamp\":\"2030-01-01T00:00:00.002Z\",\"opcode\":\"text\",\"text\":\"reply\",\"payload_base64\":\"cmVwbHk=\",\"close\":null}\n",
                "{\"index\":0,\"direction\":\"client_to_upstream\",\"timestamp\":\"2030-01-01T00:00:00.001Z\",\"opcode\":\"text\",\"text\":\"hello\",\"payload_base64\":\"aGVsbG8=\",\"close\":null}\n",
                "{\"index\":"
            ),
        )
        .await
        .unwrap();

        let (frames, warning) = load_websocket_frames(&path).await.unwrap();

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].index, 0);
        assert_eq!(frames[0].direction, "client_to_upstream");
        assert_eq!(frames[0].text.as_deref(), Some("hello"));
        assert_eq!(frames[1].index, 1);
        assert!(warning.unwrap().contains("1 WebSocket frame"));
    }

    #[test]
    fn trims_tools_and_derived_capability_blocks_from_request_copy() {
        let mut body = serde_json::json!({
            "model": "gpt-test",
            "tools": [{"type": "function", "name": "exec"}],
            "tool_choice": "auto",
            "input": [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "keep secret-value"}]},
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "<skills_instructions>remove</skills_instructions>"}]},
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "<apps_instructions>remove</apps_instructions>"}]},
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "<recommended_plugins>remove</recommended_plugins>"}]},
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "<environment_context>remove</environment_context>"}]},
                {"type": "additional_tools", "tools": [{"type": "custom", "name": "wait"}]},
                {"type": "custom_tool_call", "call_id": "call-1", "name": "exec", "input": "pwd"},
                {"type": "custom_tool_call_output", "call_id": "call-1", "output": "secret-value"}
            ]
        });
        let remove = TestsetRemovalOptions {
            tools: true,
            skills: true,
            apps: true,
            plugins: true,
            derived_prompt: true,
        };

        trim_request_body(&mut body, &remove);
        redact_json_value(&mut body, &["secret-value"]);

        assert!(body.get("tools").is_none());
        assert!(body.get("tool_choice").is_none());
        assert_eq!(body["input"].as_array().unwrap().len(), 1);
        assert_eq!(
            body.pointer("/input/0/content/0/text")
                .and_then(serde_json::Value::as_str),
            Some("keep ******")
        );
        assert!(tool_definitions(&body).is_empty());
    }

    #[test]
    fn trims_only_the_derived_fragment_when_user_text_shares_a_content_part() {
        let mut body = serde_json::json!({
            "input": [{
                "type": "message",
                "role": "user",
                "content": [{
                    "type": "input_text",
                    "text": "<recommended_plugins>generated list</recommended_plugins>\nKeep this user request"
                }]
            }]
        });
        let remove = TestsetRemovalOptions {
            plugins: true,
            ..TestsetRemovalOptions::default()
        };

        trim_request_body(&mut body, &remove);

        let text = body
            .pointer("/input/0/content/0/text")
            .and_then(serde_json::Value::as_str)
            .unwrap();
        assert_eq!(text.trim(), "Keep this user request");
        assert_eq!(
            visible_user_messages(&prompt_blocks(&body)),
            vec!["Keep this user request"]
        );
    }

    #[tokio::test]
    async fn tool_trimming_removes_nested_calls_from_sse_and_buffered_json() {
        let temp = tempfile::tempdir().unwrap();
        let sse_path = temp.path().join("response_sse.raw");
        let response_path = temp.path().join("response_body.raw");
        let response = serde_json::json!({
            "type": "response",
            "output": [
                {"type":"message","content":[{"type":"output_text","text":"keep"}]},
                {"type":"function_call","call_id":"call-1","name":"exec","arguments":"secret"}
            ]
        });
        let sse = format!(
            "data: {{\"type\":\"response.function_call_arguments.done\",\"arguments\":\"secret\"}}\n\ndata: {}\n\n",
            serde_json::json!({"type":"response.completed","response":response})
        );
        fs::write(&sse_path, &sse).await.unwrap();
        fs::write(&response_path, serde_json::to_vec(&response).unwrap())
            .await
            .unwrap();
        let request = SaveTestsetRequest {
            remove: TestsetRemovalOptions {
                tools: true,
                ..TestsetRemovalOptions::default()
            },
            ..SaveTestsetRequest::default()
        };

        let transformed_sse = transform_response_sse(&sse_path, &request)
            .await
            .unwrap()
            .unwrap();
        let transformed_body = transform_response_body(&response_path, &request)
            .await
            .unwrap()
            .unwrap();

        let sse_text = String::from_utf8(transformed_sse).unwrap();
        assert!(!sse_text.contains("function_call"));
        assert!(!sse_text.contains("secret"));
        assert!(sse_text.contains("response.completed"));
        assert!(sse_text.contains("keep"));
        let body: serde_json::Value = serde_json::from_slice(&transformed_body).unwrap();
        assert_eq!(body["output"].as_array().unwrap().len(), 1);
        assert_eq!(
            body.pointer("/output/0/type"),
            Some(&serde_json::json!("message"))
        );
    }

    #[tokio::test]
    async fn tool_trimming_filters_websocket_events_and_updates_payloads() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("websocket_frames.jsonl");
        let tool_event = serde_json::json!({
            "type": "response.function_call_arguments.done",
            "arguments": "secret"
        })
        .to_string();
        let completed_event = serde_json::json!({
            "type": "response.completed",
            "response": {
                "output": [
                    {"type":"message","content":[{"type":"output_text","text":"keep"}]},
                    {"type":"custom_tool_call","call_id":"call-1","name":"exec","input":"secret"}
                ]
            }
        })
        .to_string();
        let frames = [tool_event, completed_event]
            .into_iter()
            .enumerate()
            .map(|(index, text)| {
                serde_json::json!({
                    "index": index,
                    "direction": "upstream_to_client",
                    "timestamp": format!("2030-01-01T00:00:00.00{index}Z"),
                    "opcode": "text",
                    "text": text,
                    "payload_base64": BASE64.encode(text.as_bytes()),
                    "close": null
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&path, format!("{frames}\n")).await.unwrap();
        let request = SaveTestsetRequest {
            remove: TestsetRemovalOptions {
                tools: true,
                ..TestsetRemovalOptions::default()
            },
            ..SaveTestsetRequest::default()
        };

        let transformed = transform_websocket_frames(&path, &request)
            .await
            .unwrap()
            .unwrap();

        let lines = transformed
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>();
        assert_eq!(lines.len(), 1);
        let frame: serde_json::Value = serde_json::from_slice(lines[0]).unwrap();
        let text = frame["text"].as_str().unwrap();
        assert!(text.contains("response.completed"));
        assert!(text.contains("keep"));
        assert!(!text.contains("custom_tool_call"));
        assert!(!text.contains("secret"));
        assert_eq!(websocket_frame_counts(&transformed), (0, 1));
    }

    #[tokio::test]
    async fn trimmed_export_selects_and_reindexes_without_mutating_recording() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("recording");
        let destination = temp.path().join("testset");
        let selected_source = source.join("requests/000004");
        let omitted_source = source.join("requests/000005");
        fs::create_dir_all(&selected_source).await.unwrap();
        fs::create_dir_all(&omitted_source).await.unwrap();
        write_json_pretty(
            source.join("manifest.json"),
            &serde_json::json!({"session_id":"session-1","request_count":2}),
        )
        .await
        .unwrap();
        let body = serde_json::to_vec(&serde_json::json!({
            "model": "gpt-test",
            "tools": [{"type":"function","name":"exec"}],
            "input": [
                {"type":"message","role":"user","content":[{"type":"input_text","text":"keep secret-value"}]},
                {"type":"message","role":"user","content":[{"type":"input_text","text":"<skills_instructions>remove</skills_instructions>"}]}
            ]
        }))
        .unwrap();
        fs::write(selected_source.join("request_body.raw"), &body)
            .await
            .unwrap();
        fs::write(omitted_source.join("request_body.raw"), b"{}")
            .await
            .unwrap();
        write_json_pretty(
            selected_source.join("request_meta.json"),
            &serde_json::json!({"index":4,"request_body_bytes":body.len(),"upstream_url":"https://example.invalid"}),
        )
        .await
        .unwrap();
        write_json_pretty(
            selected_source.join("request_headers.json"),
            &vec![
                text_header("content-length", &body.len().to_string()),
                text_header("authorization", "Bearer secret-value"),
            ],
        )
        .await
        .unwrap();
        write_json_pretty(
            selected_source.join("response_headers.json"),
            &vec![text_header("set-cookie", "secret-value")],
        )
        .await
        .unwrap();
        let response = concat!(
            "data: {\"type\":\"response.custom_tool_call_input.done\",\"item_id\":\"tool-1\",\"input\":\"secret-value\"}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"secret-value\"}\n\n"
        )
        .as_bytes();
        fs::write(selected_source.join("response_sse.raw"), response)
            .await
            .unwrap();
        write_json_pretty(
            selected_source.join("response_meta.json"),
            &serde_json::json!({"status":200,"response_body_bytes":response.len(),"sse_event_count":1}),
        )
        .await
        .unwrap();
        let source_body_before = fs::read(selected_source.join("request_body.raw"))
            .await
            .unwrap();
        let request = SaveTestsetRequest {
            selected_requests: Some(vec!["000004".to_owned()]),
            sensitive_values: vec!["secret-value".to_owned()],
            remove: TestsetRemovalOptions {
                tools: true,
                skills: true,
                ..TestsetRemovalOptions::default()
            },
            ..SaveTestsetRequest::default()
        };
        let mut preview_body = serde_json::from_slice(&body).unwrap();
        trim_request_body(&mut preview_body, &request.remove);
        redact_json_value(&mut preview_body, &request.sensitive_values());
        let plan = TestsetExportPlan {
            requests: vec![TestsetExportRequest {
                source_index: "000004".to_owned(),
                export_index: "000000".to_owned(),
                request_body: preview_body,
            }],
            first_user_input: "keep ******".to_owned(),
            user_inputs: vec!["keep ******".to_owned()],
        };

        export_testset_session(&source, &destination, &plan, &request)
            .await
            .unwrap();

        assert_eq!(
            fs::read(selected_source.join("request_body.raw"))
                .await
                .unwrap(),
            source_body_before
        );
        assert!(!destination.join("requests/000004").exists());
        assert!(!destination.join("requests/000001").exists());
        let exported_dir = destination.join("requests/000000");
        let exported_body: serde_json::Value = serde_json::from_slice(
            &fs::read(exported_dir.join("request_body.raw"))
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(exported_body.get("tools").is_none());
        assert_eq!(exported_body["input"].as_array().unwrap().len(), 1);
        assert_eq!(
            exported_body.pointer("/input/0/content/0/text"),
            Some(&serde_json::json!("keep ******"))
        );
        let exported_meta: serde_json::Value = read_json(&exported_dir.join("request_meta.json"))
            .await
            .unwrap();
        assert_eq!(exported_meta["index"], 0);
        let exported_sse = fs::read_to_string(exported_dir.join("response_sse.raw"))
            .await
            .unwrap();
        assert!(exported_sse.contains("******"));
        assert!(!exported_sse.contains("secret-value"));
        assert!(!exported_sse.contains("custom_tool_call"));
        assert!(exported_sse.contains("response.output_text.delta"));
        let exported_manifest: serde_json::Value =
            read_json(&destination.join("manifest.json")).await.unwrap();
        assert_eq!(exported_manifest["request_count"], 1);
    }

    #[test]
    fn reads_additional_tool_definitions() {
        let body = serde_json::json!({
            "input": [{
                "type": "additional_tools",
                "role": "developer",
                "tools": [
                    {"type": "custom", "name": "exec", "description": "Run code"},
                    {"type": "function", "name": "wait", "description": "Wait"}
                ]
            }]
        });

        let definitions = tool_definitions(&body);

        assert_eq!(definitions.len(), 2);
        assert_eq!(definitions[0].name, "exec");
        assert_eq!(definitions[0].tool_type, "custom");
        assert_eq!(definitions[1].name, "wait");
    }

    #[test]
    fn reads_custom_tool_call_history() {
        let body = serde_json::json!({
            "input": [
                {
                    "type": "custom_tool_call",
                    "call_id": "call_1",
                    "name": "exec",
                    "status": "completed",
                    "input": "ls -la"
                },
                {
                    "type": "custom_tool_call_output",
                    "call_id": "call_1",
                    "output": [
                        {"type": "input_text", "text": "process exited 0"},
                        {"type": "input_text", "text": "5050"}
                    ]
                },
                {
                    "type": "function_call_output",
                    "call_id": "call_2",
                    "output": "legacy output"
                }
            ]
        });

        let calls = previous_function_calls(&body);
        let outputs = previous_tool_outputs(&body);

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "exec");
        assert_eq!(calls[0].arguments, "ls -la");
        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[0].call_id, "call_1");
        assert_eq!(outputs[0].output, "process exited 0\n5050");
        assert_eq!(outputs[1].call_id, "call_2");
        assert_eq!(outputs[1].output, "legacy output");
    }

    #[tokio::test]
    async fn empty_response_metadata_marks_call_incomplete_without_error() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("response_meta.json");
        std::fs::write(&path, b"").unwrap();

        let (meta, state, warning) = load_response_meta(&path).await;

        assert!(meta.is_none());
        assert_eq!(state, "incomplete");
        assert!(warning.unwrap().contains("empty"));
    }

    #[tokio::test]
    async fn invalid_response_metadata_marks_call_incomplete_without_error() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("response_meta.json");
        std::fs::write(&path, b"{").unwrap();

        let (meta, state, warning) = load_response_meta(&path).await;

        assert!(meta.is_none());
        assert_eq!(state, "incomplete");
        assert!(warning.unwrap().contains("invalid"));
    }

    #[tokio::test]
    async fn recording_failure_marker_exposes_incomplete_stage() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("recording_incomplete.json");
        std::fs::write(
            &path,
            br#"{"incomplete":true,"stage":"http_response_body_write","error":"disk full"}"#,
        )
        .unwrap();

        let warning = load_recording_incomplete(&path, temp.path(), 0)
            .await
            .unwrap();

        assert!(warning.contains("http_response_body_write"));
        assert!(warning.contains("disk full"));
    }
}
