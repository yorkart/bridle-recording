use anyhow::Context;
use axum::{
    extract::{Path as AxumPath, State, ws::WebSocketUpgrade},
    http::{HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{any, get},
    Router,
};
use bytes::Bytes;
use clap::Parser;
use tokio::{fs, net::TcpListener};
use tracing::{error, info, warn};

use crate::{
    proxy::{
        http::handle_proxy,
        replay::handle_mock_proxy,
        websocket::{prepare_websocket_proxy, run_websocket_proxy},
    },
    recording::append_access_log_line,
    types::{AppState, Args, GatewayState, ProfileConfig},
};

pub async fn run() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "bridle_recording=info,tower_http=info".into()),
        )
        .init();

    let args = Args::parse();
    let session_header = axum::http::HeaderName::from_bytes(args.session_header.as_bytes())
        .with_context(|| format!("invalid session header: {}", args.session_header))?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("build upstream HTTP client")?;
    let profile_root = default_profile_root();
    let access_log_path = profile_root.join("access.log");
    let profiles = load_profiles(&profile_root).await?;
    if args.strip_responses_lite {
        anyhow::bail!(
            "RECORDER_STRIP_RESPONSES_LITE/--strip-responses-lite is no longer supported: transparent proxy mode cannot modify forwarded traffic"
        );
    }
    let proxy_mode = args.proxy_mode;

    fs::create_dir_all(&profile_root)
        .await
        .with_context(|| format!("create profile root {}", profile_root.display()))?;

    let state = GatewayState {
        client,
        output_root: profile_root,
        access_log_path,
        profiles: std::sync::Arc::new(profiles),
        session_header,
        unsafe_record_secrets: args.unsafe_record_secrets,
        proxy_mode,
        counters: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        replay_sessions: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/:profile/mock/*path", any(mock_proxy))
        .route("/:profile/*path", any(proxy))
        .with_state(state.clone());

    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    info!(
        listen = %args.listen,
        profiles = ?state.profiles.keys().collect::<Vec<_>>(),
        profile_root = %state.output_root.display(),
        session_header = %state.session_header,
        proxy_mode = ?state.proxy_mode,
        http_proxy = ?std::env::var("HTTP_PROXY").ok(),
        https_proxy = ?std::env::var("HTTPS_PROXY").ok(),
        all_proxy = ?std::env::var("ALL_PROXY").ok(),
        "recorder listening"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve recorder")
}

async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        warn!(?err, "failed to install ctrl-c handler");
    }
}

async fn health() -> &'static str {
    "ok"
}

fn default_profile_root() -> std::path::PathBuf {
    std::env::var_os("BRIDLE_HOME_ROOT")
        .or_else(|| {
            std::env::var_os("BRIDLE_AGENT_HOME").and_then(|path| {
                std::path::PathBuf::from(path)
                    .parent()
                    .map(|parent| parent.to_path_buf())
                    .map(|parent| parent.into_os_string())
            })
        })
        .or_else(|| {
            std::env::var_os("CODEX_HOME").and_then(|path| {
                std::path::PathBuf::from(path)
                    .parent()
                    .map(|parent| parent.to_path_buf())
                    .map(|parent| parent.into_os_string())
            })
        })
        .or_else(|| std::env::var_os("HOME").map(|home| std::path::PathBuf::from(home).join(".bridle-recording").into_os_string()))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(".bridle-recording"))
}

async fn load_profiles(
    profile_root: &std::path::Path,
) -> anyhow::Result<std::collections::HashMap<String, ProfileConfig>> {
    let mut profiles = std::collections::HashMap::new();
    let mut entries = match fs::read_dir(profile_root).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(profiles),
        Err(err) => {
            return Err(err).with_context(|| format!("read profile root {}", profile_root.display()))
        }
    };

    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("read entry in {}", profile_root.display()))?
    {
        if !entry
            .file_type()
            .await
            .with_context(|| format!("read file type for {}", entry.path().display()))?
            .is_dir()
        {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        let home_dir = entry.path();
        let meta_path = home_dir.join("bridle-profile.toml");
        if !fs::try_exists(&meta_path).await? {
            continue;
        }
        let raw = fs::read_to_string(&meta_path)
            .await
            .with_context(|| format!("read {}", meta_path.display()))?;
        let file: crate::types::ProfileFile =
            toml::from_str(&raw).with_context(|| format!("parse {}", meta_path.display()))?;
        profiles.insert(
            name.clone(),
            ProfileConfig {
                name,
                upstream: reqwest::Url::parse(&file.upstream)
                    .with_context(|| format!("parse upstream in {}", meta_path.display()))?,
                supports_websocket: file.supports_websocket,
                home_dir,
            },
        );
    }

    Ok(profiles)
}

fn resolve_profile(state: &GatewayState, profile: &str) -> anyhow::Result<AppState> {
    let profile_config = state
        .profiles
        .get(profile)
        .cloned()
        .with_context(|| format!("unknown profile: {profile}"))?;
    Ok(AppState {
        client: state.client.clone(),
        profile: profile_config.clone(),
        output_dir: profile_config.home_dir.join("recordings"),
        session_header: state.session_header.clone(),
        unsafe_record_secrets: state.unsafe_record_secrets,
        proxy_mode: state.proxy_mode,
        counters: state.counters.clone(),
        replay_sessions: state.replay_sessions.clone(),
    })
}

pub async fn proxy(
    State(state): State<GatewayState>,
    ws: Option<WebSocketUpgrade>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    AxumPath((profile, path)): AxumPath<(String, String)>,
    body: Bytes,
) -> Response {
    let user_agent = headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-");
    let access_line = format!(
        "{} method={} uri={} profile={} path=/{path} ua={user_agent}\n",
        crate::util::now_rfc3339(),
        method,
        uri,
        profile,
    );
    if let Err(err) = append_access_log_line(&state.access_log_path, &access_line).await {
        warn!(?err, path = %state.access_log_path.display(), "failed to append access log");
    }

    let state = match resolve_profile(&state, &profile) {
        Ok(state) => state,
        Err(err) => return (StatusCode::NOT_FOUND, err.to_string()).into_response(),
    };

    if let Some(ws) = ws {
        if !state.profile.supports_websocket {
            return (
                StatusCode::BAD_REQUEST,
                format!("profile '{}' does not support websocket proxying", state.profile.name),
            )
                .into_response();
        }
        let protocols = crate::util::websocket_protocols(&headers);
        let ws = if protocols.is_empty() {
            ws
        } else {
            ws.protocols(protocols)
        };
        return match prepare_websocket_proxy(state, method, uri, headers, path).await {
            Ok(prepared) => ws.on_upgrade(move |client| run_websocket_proxy(client, prepared)),
            Err(err) => {
                error!(?err, "websocket proxy setup failed");
                (
                    StatusCode::BAD_GATEWAY,
                    format!("recorder websocket proxy error: {err:#}"),
                )
                    .into_response()
            }
        };
    }

    match handle_proxy(state, method, uri, headers, path, body).await {
        Ok(response) => response,
        Err(err) => {
            error!(?err, "proxy request failed");
            (
                StatusCode::BAD_GATEWAY,
                format!("recorder proxy error: {err:#}"),
            )
                .into_response()
        }
    }
}

pub async fn mock_proxy(
    State(state): State<GatewayState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    AxumPath((profile, path)): AxumPath<(String, String)>,
    body: Bytes,
) -> Response {
    let state = match resolve_profile(&state, &profile) {
        Ok(state) => state,
        Err(err) => return (StatusCode::NOT_FOUND, err.to_string()).into_response(),
    };

    match handle_mock_proxy(state, method, uri, headers, path, body).await {
        Ok(response) => response,
        Err(err) => {
            warn!(?err, profile, "mock replay failed");
            (
                StatusCode::NOT_FOUND,
                [("content-type", "application/json")],
                serde_json::json!({
                    "error": "mock replay failed",
                    "detail": err.to_string()
                })
                .to_string(),
            )
                .into_response()
        }
    }
}
