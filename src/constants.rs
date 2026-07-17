pub const UNKNOWN_SESSION: &str = "unknown";
pub const DEFAULT_SESSION_HEADER: &str = "x-codex-session-id";
pub const FALLBACK_SESSION_HEADERS: &[&str] = &["thread-id", "session-id"];
pub const CODEX_TURN_METADATA_HEADER: &str = "x-codex-turn-metadata";
pub const CODEX_TURN_METADATA_FIELDS: &[&str] = &["thread_id", "session_id"];
pub const HOP_BY_HOP_RESPONSE_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];
pub const MATCHER_VERSION: u32 = 2;
pub const IGNORED_INPUT_TEXT_PREFIXES: &[&str] = &[
    "<skills_instructions>",
    "<apps_instructions>",
    "<plugins_instructions>",
];
pub const UPSTREAM_POOL_IDLE_TIMEOUT_SECS: u64 = 5 * 60;
pub const UPSTREAM_TCP_KEEPALIVE_SECS: u64 = 60;
