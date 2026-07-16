use std::{
    collections::{BTreeMap, HashMap},
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
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::{
    constants::UNKNOWN_SESSION,
    sse::SseParser,
    types::{GatewayState, RequestMeta, ResponseMeta},
};

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

pub async fn testsets() -> Response {
    match testsets_inner(None).await {
        Ok(testsets) => Json(serde_json::json!({ "testsets": testsets })).into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

pub async fn profile_testsets(AxumPath(profile): AxumPath<String>) -> Response {
    match testsets_inner(Some(&profile)).await {
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
    match save_testset_inner(&state, &profile, &session_id, request.replace).await {
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
    replace: bool,
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
    let user_inputs = collect_user_inputs(&observed.turns);
    let first_user_input = user_inputs
        .first()
        .cloned()
        .with_context(|| format!("session {session_id} has no visible user input"))?;
    let user_input_sha256 = sha256_hex(first_user_input.as_bytes());
    let repo_root = std::env::current_dir().context("resolve current git repository root")?;
    let testset_dir = repo_root
        .join("testsets")
        .join(profile)
        .join(&user_input_sha256);
    let raw_dir = testset_dir.join("raw").join(session_id);

    if fs::try_exists(&testset_dir).await? && !replace {
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

    let temp_dir = repo_root
        .join("testsets")
        .join(profile)
        .join(format!(".{user_input_sha256}.tmp"));
    if fs::try_exists(&temp_dir).await? {
        fs::remove_dir_all(&temp_dir).await?;
    }
    fs::create_dir_all(&temp_dir).await?;
    copy_dir_all(&source_dir, &temp_dir.join("raw").join(session_id)).await?;

    let manifest = TestsetManifest {
        version: 1,
        profile: profile.to_owned(),
        source_session_id: session_id.to_owned(),
        first_user_input: first_user_input.clone(),
        user_inputs,
        user_input_sha256: user_input_sha256.clone(),
        saved_at: crate::util::now_rfc3339(),
        source_recording_path: source_dir.display().to_string(),
        raw_recording_path: format!("raw/{session_id}"),
    };
    write_json_pretty(temp_dir.join("testset.json"), &manifest).await?;

    if fs::try_exists(&testset_dir).await? {
        fs::remove_dir_all(&testset_dir).await?;
    }
    fs::rename(&temp_dir, &testset_dir).await?;

    Ok(SavedTestset {
        status: if replace { "replaced" } else { "saved" }.to_owned(),
        profile: profile.to_owned(),
        session_id: session_id.to_owned(),
        first_user_input,
        user_input_sha256,
        testset_path: testset_dir.display().to_string(),
        raw_path: raw_dir.display().to_string(),
    })
}

async fn testsets_inner(profile_filter: Option<&str>) -> anyhow::Result<Vec<TestsetSummary>> {
    let repo_root = std::env::current_dir().context("resolve current git repository root")?;
    let testsets_dir = repo_root.join("testsets");
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
    Ok(calls)
}

async fn load_observed_call(index: String, request_dir: PathBuf) -> anyhow::Result<ObservedCall> {
    let request_meta = read_json::<RequestMeta>(&request_dir.join("request_meta.json")).await?;
    let response_meta_path = request_dir.join("response_meta.json");
    let response_meta = if fs::try_exists(&response_meta_path).await? {
        Some(read_json::<ResponseMeta>(&response_meta_path).await?)
    } else {
        None
    };
    let request_body_bytes = fs::read(request_dir.join("request_body.raw"))
        .await
        .with_context(|| format!("read request body in {}", request_dir.display()))?;
    let request_body = decode_request_body_json(&request_body_bytes)?;
    let sse_path = request_dir.join("response_sse.raw");
    let (sse, response_preview) = if fs::try_exists(&sse_path).await? {
        let sse_bytes = fs::read(&sse_path)
            .await
            .with_context(|| format!("read response_sse.raw in {}", request_dir.display()))?;
        (parse_response_sse(&sse_bytes), None)
    } else {
        let body_path = request_dir.join("response_body.raw");
        if fs::try_exists(&body_path).await? {
            let body = fs::read(&body_path)
                .await
                .with_context(|| format!("read response_body.raw in {}", request_dir.display()))?;
            if looks_like_sse_response(&body) {
                (parse_response_sse(&body), None)
            } else {
                (
                    ParsedResponseSse::default(),
                    Some(response_body_preview(&body)),
                )
            }
        } else {
            let preview = match response_meta.as_ref() {
                Some(meta) if meta.upstream_error.is_some() => format!(
                    "<response stream failed: {}>",
                    meta.upstream_error
                        .as_deref()
                        .unwrap_or("unknown upstream error")
                ),
                Some(meta) if meta.response_body_bytes == 0 => "<empty response body>".to_owned(),
                Some(_) => "<response body recording unavailable>".to_owned(),
                None => "<response recording incomplete>".to_owned(),
            };
            (ParsedResponseSse::default(), Some(preview))
        }
    };
    let files = request_files(&request_dir).await?;

    let prompt_blocks = prompt_blocks(&request_body);
    let visible_user_messages = visible_user_messages(&prompt_blocks);
    let tool_definitions = tool_definitions(&request_body);
    let previous_tool_outputs = previous_tool_outputs(&request_body);
    let previous_function_calls = previous_function_calls(&request_body);
    let previous_assistant_messages = previous_assistant_messages(&prompt_blocks);

    let duration_ms = response_meta
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
        .and_then(serde_json::Value::as_array)
        .map_or(0, Vec::len);

    Ok(ObservedCall {
        index,
        request_id: if response_id.is_empty() {
            format!("request-{}", request_meta.index)
        } else {
            response_id
        },
        started_at: request_meta.started_at,
        completed_at: response_meta
            .as_ref()
            .map(|meta| meta.completed_at.clone())
            .unwrap_or_default(),
        duration_ms,
        method: request_meta.method,
        path: request_meta.path,
        status: response_meta.as_ref().map(|meta| meta.status).unwrap_or(0),
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
        output_text: if sse.output_text.trim().is_empty() {
            response_preview.unwrap_or_default()
        } else {
            sse.output_text.trim().to_owned()
        },
        usage: sse.completed_response.get("usage").cloned(),
        event_counts: sse.event_counts,
        files,
        raw_dir: request_dir.display().to_string(),
        request_body_bytes: request_meta.request_body_bytes,
        response_body_bytes: response_meta
            .as_ref()
            .map(|meta| meta.response_body_bytes)
            .unwrap_or(0),
    })
}

fn decode_request_body_json(bytes: &[u8]) -> anyhow::Result<serde_json::Value> {
    match serde_json::from_slice(bytes) {
        Ok(value) => return Ok(value),
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

fn response_body_preview(bytes: &[u8]) -> String {
    let text = match std::str::from_utf8(bytes) {
        Ok(text) => text.trim(),
        Err(_) => return format!("<{} bytes binary response>", bytes.len()),
    };
    if text.is_empty() {
        return String::new();
    }

    let preview = serde_json::from_str::<serde_json::Value>(text)
        .map(|value| value.to_string())
        .unwrap_or_else(|_| text.to_owned());
    truncate_chars(&preview, 2000)
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

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn parse_response_sse(bytes: &[u8]) -> ParsedResponseSse {
    let mut parser = SseParser::default();
    let mut event_counts = BTreeMap::new();
    let mut output_text = String::new();
    let mut function_calls = Vec::new();
    let mut tool_inputs = HashMap::new();
    let mut completed_response = serde_json::Value::Object(serde_json::Map::new());

    for event in parser.push(bytes) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&event.data.join("\n")) else {
            continue;
        };
        let event_type = value
            .get("type")
            .and_then(serde_json::Value::as_str)
            .or(event.event.as_deref())
            .unwrap_or("unknown")
            .to_owned();
        *event_counts.entry(event_type.clone()).or_insert(0) += 1;

        match event_type.as_str() {
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
    }
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

    let Some(input) = request_body
        .get("input")
        .and_then(serde_json::Value::as_array)
    else {
        return blocks;
    };
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
    blocks
}

fn visible_user_messages(blocks: &[PromptBlock]) -> Vec<String> {
    blocks
        .iter()
        .filter(|block| block.role == "user" && !is_derived_context_block(&block.block_type))
        .map(|block| block.text.clone())
        .collect()
}

fn previous_assistant_messages(blocks: &[PromptBlock]) -> Vec<String> {
    blocks
        .iter()
        .filter(|block| block.role == "assistant")
        .map(|block| block.excerpt.clone())
        .collect()
}

fn is_derived_context_block(block_type: &str) -> bool {
    matches!(
        block_type,
        "environment" | "permissions" | "skills" | "apps" | "plugins"
    )
}

fn classify_prompt_block(role: &str, text: &str) -> String {
    let trimmed = text.trim_start();
    if trimmed.starts_with("<environment_context>") {
        "environment"
    } else if trimmed.starts_with("<permissions instructions>") {
        "permissions"
    } else if trimmed.starts_with("<skills_instructions>") {
        "skills"
    } else if trimmed.starts_with("<apps_instructions>") {
        "apps"
    } else if trimmed.starts_with("<plugins_instructions>") {
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
                description: excerpt(description, 220),
            }
        })
        .collect()
}

fn previous_tool_outputs(request_body: &serde_json::Value) -> Vec<ToolOutput> {
    request_body
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
            output: excerpt(&tool_output_text(item.get("output")), 1200),
        })
        .collect()
}

fn previous_function_calls(request_body: &serde_json::Value) -> Vec<ObservedFunctionCall> {
    request_body
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
        })
        .collect()
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

fn collect_user_inputs(turns: &[ObservedTurn]) -> Vec<String> {
    let mut inputs = Vec::new();
    for turn in turns {
        let user = turn.user.trim();
        if user.is_empty() || user == "(no visible user input)" {
            continue;
        }
        if !inputs.iter().any(|existing| existing == user) {
            inputs.push(user.to_owned());
        }
    }
    inputs
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
    let mut bytes = serde_json::to_vec_pretty(value).context("serialize testset manifest")?;
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

async fn copy_dir_all(source: &Path, destination: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(destination)
        .await
        .with_context(|| format!("create {}", destination.display()))?;
    let mut stack = vec![(source.to_path_buf(), destination.to_path_buf())];
    while let Some((source_dir, destination_dir)) = stack.pop() {
        let mut entries = fs::read_dir(&source_dir)
            .await
            .with_context(|| format!("read {}", source_dir.display()))?;
        while let Some(entry) = entries.next_entry().await? {
            let source_path = entry.path();
            let destination_path = destination_dir.join(entry.file_name());
            let file_type = entry.file_type().await?;
            if file_type.is_dir() {
                fs::create_dir_all(&destination_path)
                    .await
                    .with_context(|| format!("create {}", destination_path.display()))?;
                stack.push((source_path, destination_path));
            } else if file_type.is_file() {
                copy_file_verbatim(&source_path, &destination_path).await?;
            }
        }
    }
    Ok(())
}

async fn copy_file_verbatim(source: &Path, destination: &Path) -> anyhow::Result<()> {
    let mut input = fs::File::open(source)
        .await
        .with_context(|| format!("open {}", source.display()))?;
    let mut output = fs::File::create(destination)
        .await
        .with_context(|| format!("create {}", destination.display()))?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = input
            .read(&mut buffer)
            .await
            .with_context(|| format!("read {}", source.display()))?;
        if read == 0 {
            break;
        }
        output
            .write_all(&buffer[..read])
            .await
            .with_context(|| format!("write {}", destination.display()))?;
    }
    output
        .flush()
        .await
        .with_context(|| format!("flush {}", destination.display()))
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

#[derive(Deserialize)]
pub struct SaveTestsetRequest {
    #[serde(default)]
    replace: bool,
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
    usage: Option<serde_json::Value>,
    event_counts: BTreeMap<String, usize>,
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
}

#[derive(Clone, Serialize)]
struct ToolDefinition {
    name: String,
    #[serde(rename = "type")]
    tool_type: String,
    description: String,
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

#[derive(Default)]
struct ParsedResponseSse {
    output_text: String,
    function_calls: Vec<ObservedFunctionCall>,
    completed_response: serde_json::Value,
    event_counts: BTreeMap<String, usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
}

const OBSERVABILITY_HTML: &str = include_str!("observability_ui.html");
