use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};

use axum::http::HeaderName;
use clap::Parser;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::constants::DEFAULT_SESSION_HEADER;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Transparent model-provider recorder for agent traffic"
)]
pub struct Args {
    #[arg(long, env = "RECORDER_LISTEN", default_value = "127.0.0.1:8787")]
    pub listen: SocketAddr,

    #[arg(
        long,
        env = "RECORDER_SESSION_HEADER",
        default_value = DEFAULT_SESSION_HEADER
    )]
    pub session_header: String,
}

#[derive(Clone)]
pub struct GatewayState {
    pub client: reqwest::Client,
    pub output_root: PathBuf,
    pub access_log_path: PathBuf,
    pub profiles: Arc<HashMap<String, ProfileConfig>>,
    pub session_header: HeaderName,
    pub counters: Arc<Mutex<HashMap<String, u64>>>,
    pub replay_sessions: Arc<Mutex<HashMap<String, ReplaySession>>>,
}

#[derive(Clone)]
pub struct AppState {
    pub client: reqwest::Client,
    pub profile: ProfileConfig,
    pub output_dir: PathBuf,
    pub mock_derived_dir: PathBuf,
    pub session_header: HeaderName,
    pub counters: Arc<Mutex<HashMap<String, u64>>>,
    pub replay_sessions: Arc<Mutex<HashMap<String, ReplaySession>>>,
}

#[derive(Clone)]
pub struct ProfileConfig {
    pub name: String,
    pub upstream: Url,
    pub supports_websocket: bool,
    pub home_dir: PathBuf,
}

#[derive(Serialize)]
pub struct Manifest {
    pub session_id: String,
    pub created_at: String,
    pub updated_at: String,
    pub request_count: u64,
    pub recorder: RecorderManifest,
}

#[derive(Serialize)]
pub struct RecorderManifest {
    pub version: &'static str,
    pub profile: String,
    pub session_header: String,
    pub upstream_base_url: String,
}

#[derive(Serialize, Deserialize)]
pub struct RequestMeta {
    pub index: u64,
    pub session_id: String,
    pub session_source: SessionSource,
    pub started_at: String,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub upstream_url: String,
    pub request_body_bytes: usize,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RequestMatch {
    pub version: u32,
    pub hash: String,
    pub route: MatchRoute,
    pub canonical: serde_json::Value,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MatchRoute {
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub model: Option<String>,
    pub stream: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct ReplaySession {
    pub recorded_session_id: String,
    pub next_index: u64,
}

#[derive(Debug, Deserialize)]
pub struct ResponseRewriteSpec {
    pub replacements: Vec<ResponseRewriteReplacement>,
}

#[derive(Debug, Deserialize)]
pub struct ResponseRewriteReplacement {
    pub pointer: String,
    pub value: serde_json::Value,
}

pub struct RecordedMatch {
    pub session_id: String,
    pub index: u64,
    pub request_dir: PathBuf,
    pub derived_dir: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionSource {
    Header { name: String },
    Unknown,
}

#[derive(Serialize, Deserialize)]
pub struct ResponseMeta {
    pub status: u16,
    pub started_at: String,
    pub completed_at: String,
    pub response_body_bytes: usize,
    pub sse_event_count: usize,
    pub upstream_error: Option<String>,
}

#[derive(Serialize)]
pub struct WebSocketMeta {
    pub status: &'static str,
    pub started_at: String,
    pub completed_at: String,
    pub upstream_url: String,
    pub client_to_upstream_frames: usize,
    pub upstream_to_client_frames: usize,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct HeaderRecord {
    pub name: String,
    pub value: HeaderValueRecord,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HeaderValueRecord {
    Text { value: String },
    BinaryBase64 { value: String },
}

#[derive(Serialize)]
pub struct WebSocketFrameRecord {
    pub index: usize,
    pub direction: WebSocketDirection,
    pub timestamp: String,
    pub opcode: &'static str,
    pub text: Option<String>,
    pub payload_base64: Option<String>,
    pub close: Option<WebSocketCloseRecord>,
}

#[derive(Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum WebSocketDirection {
    ClientToUpstream,
    UpstreamToClient,
}

#[derive(Serialize)]
pub struct WebSocketCloseRecord {
    pub code: u16,
    pub reason: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ParsedSseEvent {
    pub event: Option<String>,
    pub id: Option<String>,
    pub retry: Option<String>,
    pub data: Vec<String>,
}

#[derive(Deserialize)]
pub struct ProfileFile {
    pub upstream: String,
    pub supports_websocket: bool,
}
