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

mod api;
mod claude;
mod codex;
mod prompt;
mod recording;
mod sse;
mod testset;
mod ui;

pub use api::{
    preview_testset, profile_testsets, profiles, save_testset, session, sessions, testsets,
};
pub use ui::ui;

use prompt::*;
use recording::*;
use sse::*;
use testset::*;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObservabilityProvider {
    Claude,
    Codex,
    Generic,
}

impl ObservabilityProvider {
    fn from_profile(profile: &str) -> Self {
        match profile {
            "claude" => Self::Claude,
            "codex-http" | "codex-websocket" => Self::Codex,
            _ => Self::Generic,
        }
    }

    fn classify_request_kind(self, request_body: &serde_json::Value) -> ObservedRequestKind {
        match self {
            Self::Claude => claude::classify_request_kind(request_body),
            Self::Codex => codex::classify_request_kind(request_body),
            Self::Generic => ObservedRequestKind::Conversation,
        }
    }

    fn prompt_blocks(self, request_body: &serde_json::Value) -> Vec<PromptBlock> {
        match self {
            Self::Claude => claude::prompt_blocks(request_body),
            Self::Codex => codex::prompt_blocks(request_body),
            Self::Generic => prompt_blocks(request_body),
        }
    }

    fn visible_user_messages(self, blocks: &[PromptBlock]) -> Vec<String> {
        match self {
            Self::Claude => claude::visible_user_messages(blocks),
            Self::Codex => codex::visible_user_messages(blocks),
            Self::Generic => visible_user_messages(blocks),
        }
    }

    fn build_turns(self, calls: &[ObservedCall]) -> Vec<ObservedTurn> {
        match self {
            Self::Claude => claude::build_turns(calls),
            Self::Codex => codex::build_turns(calls),
            Self::Generic => build_conversation_turns(calls),
        }
    }
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
    request_kind: ObservedRequestKind,
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
    request_kind: ObservedRequestKind,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ObservedRequestKind {
    Conversation,
    SessionTitle,
    SessionRecap,
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

#[cfg(test)]
mod tests;
