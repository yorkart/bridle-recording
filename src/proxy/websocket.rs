use std::{path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context};
use axum::{
    extract::ws::{CloseFrame, Message as AxumWsMessage, WebSocket},
    http::{HeaderMap, Method, Uri},
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use futures_util::{SinkExt, StreamExt};
use reqwest::Url;
use tokio::{
    fs::{self, File},
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};
use tokio_socks::tcp::Socks5Stream;
use tokio_tungstenite::{
    client_async_tls_with_config,
    tungstenite::{
        client::IntoClientRequest,
        protocol::{
            frame::coding::CloseCode, CloseFrame as TungsteniteCloseFrame,
            Message as TungsteniteMessage,
        },
    },
};

use crate::{
    recording::{
        headers_to_records, write_bytes_file, write_json_file, write_manifest, write_websocket_meta,
    },
    types::{
        AppState, RequestMeta, WebSocketCloseRecord, WebSocketDirection, WebSocketFrameRecord,
        WebSocketMeta,
    },
    util::{
        build_upstream_websocket_url, next_request_index, now_rfc3339, request_dir,
        session_from_headers, should_forward_websocket_header,
    },
};

pub struct PreparedWebSocketProxy {
    pub request_dir: Option<PathBuf>,
    pub started_at: String,
    pub upstream_url: Url,
    pub headers: HeaderMap,
    pub unsafe_record_secrets: bool,
    pub proxy_mode: crate::types::ProxyMode,
}

pub async fn prepare_websocket_proxy(
    state: AppState,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    path: String,
) -> anyhow::Result<PreparedWebSocketProxy> {
    let started_at = now_rfc3339();
    let (session_id, session_source) = session_from_headers(&headers, &state.session_header);
    let index = next_request_index(&state, &session_id).await?;
    let upstream_url = build_upstream_websocket_url(&state.profile.upstream, &path, uri.query())?;
    let request_dir = request_dir(&state.output_dir, &session_id, index);
    let request_dir = match create_websocket_recording(
        &request_dir,
        &state,
        RequestMeta {
            index,
            session_id: session_id.clone(),
            session_source,
            started_at: started_at.clone(),
            method: method.to_string(),
            path: format!("/{path}"),
            query: uri.query().map(ToOwned::to_owned),
            upstream_url: upstream_url.to_string(),
            request_body_bytes: 0,
        },
        &session_id,
        &headers,
    )
    .await
    {
        Ok(request_dir) => request_dir,
        Err(err) => {
            tracing::warn!(
                ?err,
                "websocket recording setup failed; continuing without recording"
            );
            None
        }
    };

    Ok(PreparedWebSocketProxy {
        request_dir,
        started_at,
        upstream_url,
        headers,
        unsafe_record_secrets: state.unsafe_record_secrets,
        proxy_mode: state.proxy_mode,
    })
}

pub async fn run_websocket_proxy(client: WebSocket, prepared: PreparedWebSocketProxy) {
    if let Err(err) = run_websocket_proxy_inner(client, prepared).await {
        tracing::error!(?err, "websocket proxy failed");
    }
}

async fn run_websocket_proxy_inner(
    client: WebSocket,
    prepared: PreparedWebSocketProxy,
) -> anyhow::Result<()> {
    let mut upstream_request = prepared
        .upstream_url
        .as_str()
        .into_client_request()
        .with_context(|| format!("build websocket request {}", prepared.upstream_url))?;
    for (name, value) in prepared.headers.iter() {
        if should_forward_websocket_header(name, prepared.proxy_mode) {
            upstream_request
                .headers_mut()
                .insert(name.clone(), value.clone());
        }
    }

    let upstream_socket = connect_upstream_socket(&prepared.upstream_url).await?;

    let (upstream, upstream_response) =
        match client_async_tls_with_config(upstream_request, upstream_socket, None, None).await {
            Ok(result) => result,
            Err(err) => {
                maybe_write_websocket_meta(
                    prepared.request_dir.as_deref(),
                    WebSocketMeta {
                        status: "connect_failed",
                        started_at: prepared.started_at,
                        completed_at: now_rfc3339(),
                        upstream_url: prepared.upstream_url.to_string(),
                        client_to_upstream_frames: 0,
                        upstream_to_client_frames: 0,
                        error: Some(err.to_string()),
                    },
                )
                .await;
                return Err(anyhow!("connect upstream websocket failed: {err}"));
            }
        };

    if let Some(request_dir) = prepared.request_dir.as_ref() {
        if let Err(err) = write_json_file(
            request_dir.join("websocket_response_headers.json"),
            &headers_to_records(upstream_response.headers(), prepared.unsafe_record_secrets),
        )
        .await
        {
            tracing::warn!(?err, "failed to record websocket response headers");
        }
    }

    let recorder = match prepared.request_dir.as_ref() {
        Some(request_dir) => match File::create(request_dir.join("websocket_frames.jsonl")).await {
            Ok(file) => Some(WebSocketRecorder::new(file)),
            Err(err) => {
                tracing::warn!(?err, "failed to create websocket recording file");
                None
            }
        },
        None => None,
    };

    let (mut client_sender, mut client_receiver) = client.split();
    let (mut upstream_sender, mut upstream_receiver) = upstream.split();

    let client_to_upstream = async {
        while let Some(message) = client_receiver.next().await {
            let message = message.context("read client websocket frame")?;
            let is_close = matches!(message, AxumWsMessage::Close(_));
            if let Some(recorder) = recorder.as_ref() {
                recorder
                    .record_axum(WebSocketDirection::ClientToUpstream, &message)
                    .await;
            }
            upstream_sender
                .send(axum_to_tungstenite_message(message))
                .await
                .context("send websocket frame to upstream")?;
            if is_close {
                break;
            }
        }
        anyhow::Ok(())
    };

    let upstream_to_client = async {
        while let Some(message) = upstream_receiver.next().await {
            let message = message.context("read upstream websocket frame")?;
            let is_close = matches!(message, TungsteniteMessage::Close(_));
            if let Some(recorder) = recorder.as_ref() {
                recorder
                    .record_tungstenite(WebSocketDirection::UpstreamToClient, &message)
                    .await;
            }
            client_sender
                .send(tungstenite_to_axum_message(message))
                .await
                .context("send websocket frame to client")?;
            if is_close {
                break;
            }
        }
        anyhow::Ok(())
    };

    let transfer_error = tokio::select! {
        result = client_to_upstream => result.err().map(|err| err.to_string()),
        result = upstream_to_client => result.err().map(|err| err.to_string()),
    };

    let counts = match recorder.as_ref() {
        Some(recorder) => recorder.counts().await,
        None => WebSocketRecorderCounts::default(),
    };
    maybe_write_websocket_meta(
        prepared.request_dir.as_deref(),
        WebSocketMeta {
            status: if transfer_error.is_some() {
                "transfer_error"
            } else {
                "completed"
            },
            started_at: prepared.started_at,
            completed_at: now_rfc3339(),
            upstream_url: prepared.upstream_url.to_string(),
            client_to_upstream_frames: counts.client_to_upstream,
            upstream_to_client_frames: counts.upstream_to_client,
            error: transfer_error,
        },
    )
    .await;

    Ok(())
}

enum UpstreamSocket {
    Tcp(TcpStream),
    Socks5(Socks5Stream<TcpStream>),
}

impl AsyncRead for UpstreamSocket {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            UpstreamSocket::Tcp(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            UpstreamSocket::Socks5(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for UpstreamSocket {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match &mut *self {
            UpstreamSocket::Tcp(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            UpstreamSocket::Socks5(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            UpstreamSocket::Tcp(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            UpstreamSocket::Socks5(stream) => std::pin::Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            UpstreamSocket::Tcp(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            UpstreamSocket::Socks5(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
        }
    }
}

async fn connect_upstream_socket(upstream_url: &Url) -> anyhow::Result<UpstreamSocket> {
    let scheme = upstream_url.scheme();
    let Some(host) = upstream_url.host_str() else {
        return Err(anyhow!("upstream websocket URL missing host"));
    };
    let port = upstream_url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("upstream websocket URL missing port"))?;

    if let Some(proxy_url) = websocket_proxy_url_for_scheme(scheme) {
        tracing::info!(proxy = %proxy_url, upstream = %upstream_url, "connecting websocket upstream via proxy");
        return connect_via_proxy(&proxy_url, host, port).await;
    }

    tracing::info!(upstream = %upstream_url, "connecting websocket upstream directly");
    let stream = TcpStream::connect((host, port))
        .await
        .with_context(|| format!("connect websocket upstream {}:{}", host, port))?;
    Ok(UpstreamSocket::Tcp(stream))
}

fn websocket_proxy_url_for_scheme(scheme: &str) -> Option<String> {
    match scheme {
        "wss" | "https" => env_proxy(["HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"]),
        "ws" | "http" => env_proxy(["HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"]),
        _ => env_proxy(["ALL_PROXY", "all_proxy"]),
    }
}

fn env_proxy<const N: usize>(keys: [&str; N]) -> Option<String> {
    keys.into_iter()
        .find_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

async fn connect_via_proxy(
    proxy_url: &str,
    host: &str,
    port: u16,
) -> anyhow::Result<UpstreamSocket> {
    let proxy = Url::parse(proxy_url).with_context(|| format!("parse proxy URL {proxy_url}"))?;
    let proxy_host = proxy
        .host_str()
        .ok_or_else(|| anyhow!("proxy URL missing host: {proxy_url}"))?;
    let proxy_port = proxy
        .port_or_known_default()
        .ok_or_else(|| anyhow!("proxy URL missing port: {proxy_url}"))?;

    match proxy.scheme() {
        "socks5" | "socks5h" => {
            let stream = Socks5Stream::connect((proxy_host, proxy_port), (host, port))
                .await
                .with_context(|| {
                    format!("connect websocket via SOCKS5 proxy {proxy_host}:{proxy_port}")
                })?;
            Ok(UpstreamSocket::Socks5(stream))
        }
        "http" | "https" => {
            let mut stream = TcpStream::connect((proxy_host, proxy_port))
                .await
                .with_context(|| {
                    format!("connect websocket via HTTP proxy {proxy_host}:{proxy_port}")
                })?;

            let connect_request = format!(
                "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Connection: Keep-Alive\r\nConnection: Keep-Alive\r\n\r\n"
            );
            stream
                .write_all(connect_request.as_bytes())
                .await
                .context("write HTTP CONNECT request")?;

            let mut response = Vec::with_capacity(1024);
            let mut buf = [0_u8; 1024];
            loop {
                let read = stream
                    .read(&mut buf)
                    .await
                    .context("read HTTP CONNECT response")?;
                if read == 0 {
                    return Err(anyhow!(
                        "HTTP proxy closed while establishing CONNECT tunnel"
                    ));
                }
                response.extend_from_slice(&buf[..read]);
                if response.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
                if response.len() > 16 * 1024 {
                    return Err(anyhow!("HTTP CONNECT response too large"));
                }
            }

            let header_end = response
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|idx| idx + 4)
                .ok_or_else(|| anyhow!("invalid HTTP CONNECT response"))?;
            let header_text = String::from_utf8_lossy(&response[..header_end]);
            let Some(status_line) = header_text.lines().next() else {
                return Err(anyhow!("missing HTTP CONNECT status line"));
            };
            if !(status_line.starts_with("HTTP/1.1 200") || status_line.starts_with("HTTP/1.0 200"))
            {
                return Err(anyhow!("HTTP proxy CONNECT failed: {status_line}"));
            }
            Ok(UpstreamSocket::Tcp(stream))
        }
        scheme => Err(anyhow!(
            "unsupported proxy scheme for websocket upstream: {scheme}"
        )),
    }
}

#[derive(Clone)]
struct WebSocketRecorder {
    file: Arc<Mutex<File>>,
    state: Arc<Mutex<WebSocketRecorderState>>,
}

#[derive(Default, Clone, Copy)]
struct WebSocketRecorderCounts {
    client_to_upstream: usize,
    upstream_to_client: usize,
}

#[derive(Default)]
struct WebSocketRecorderState {
    next_index: usize,
    counts: WebSocketRecorderCounts,
}

impl WebSocketRecorder {
    fn new(file: File) -> Self {
        Self {
            file: Arc::new(Mutex::new(file)),
            state: Arc::new(Mutex::new(WebSocketRecorderState::default())),
        }
    }

    async fn record_axum(&self, direction: WebSocketDirection, message: &AxumWsMessage) {
        let record = self.next_record(direction, axum_ws_opcode(message)).await;
        let record = match message {
            AxumWsMessage::Text(text) => WebSocketFrameRecord {
                text: Some(text.to_string()),
                payload_base64: Some(BASE64.encode(text.as_bytes())),
                close: None,
                ..record
            },
            AxumWsMessage::Binary(bytes) => WebSocketFrameRecord {
                payload_base64: Some(BASE64.encode(bytes)),
                ..record
            },
            AxumWsMessage::Ping(bytes) => WebSocketFrameRecord {
                payload_base64: Some(BASE64.encode(bytes)),
                ..record
            },
            AxumWsMessage::Pong(bytes) => WebSocketFrameRecord {
                payload_base64: Some(BASE64.encode(bytes)),
                ..record
            },
            AxumWsMessage::Close(close) => WebSocketFrameRecord {
                close: close.as_ref().map(|close| WebSocketCloseRecord {
                    code: close.code,
                    reason: close.reason.to_string(),
                }),
                ..record
            },
        };
        if let Err(err) = self.append_record(&record).await {
            tracing::warn!(?err, "failed to append websocket client frame record");
        }
    }

    async fn record_tungstenite(
        &self,
        direction: WebSocketDirection,
        message: &TungsteniteMessage,
    ) {
        let record = self
            .next_record(direction, tungstenite_ws_opcode(message))
            .await;
        let record = match message {
            TungsteniteMessage::Text(text) => WebSocketFrameRecord {
                text: Some(text.to_string()),
                payload_base64: Some(BASE64.encode(text.as_bytes())),
                close: None,
                ..record
            },
            TungsteniteMessage::Binary(bytes) => WebSocketFrameRecord {
                payload_base64: Some(BASE64.encode(bytes)),
                ..record
            },
            TungsteniteMessage::Ping(bytes) => WebSocketFrameRecord {
                payload_base64: Some(BASE64.encode(bytes)),
                ..record
            },
            TungsteniteMessage::Pong(bytes) => WebSocketFrameRecord {
                payload_base64: Some(BASE64.encode(bytes)),
                ..record
            },
            TungsteniteMessage::Close(close) => WebSocketFrameRecord {
                close: close.as_ref().map(|close| WebSocketCloseRecord {
                    code: u16::from(close.code),
                    reason: close.reason.to_string(),
                }),
                ..record
            },
            TungsteniteMessage::Frame(frame) => WebSocketFrameRecord {
                payload_base64: Some(BASE64.encode(frame.payload())),
                ..record
            },
        };
        if let Err(err) = self.append_record(&record).await {
            tracing::warn!(?err, "failed to append websocket upstream frame record");
        }
    }

    async fn next_record(
        &self,
        direction: WebSocketDirection,
        opcode: &'static str,
    ) -> WebSocketFrameRecord {
        let mut state = self.state.lock().await;
        let index = state.next_index;
        state.next_index += 1;
        match direction {
            WebSocketDirection::ClientToUpstream => state.counts.client_to_upstream += 1,
            WebSocketDirection::UpstreamToClient => state.counts.upstream_to_client += 1,
        }
        WebSocketFrameRecord {
            index,
            direction,
            timestamp: now_rfc3339(),
            opcode,
            text: None,
            payload_base64: None,
            close: None,
        }
    }

    async fn append_record(&self, record: &WebSocketFrameRecord) -> anyhow::Result<()> {
        let mut line = serde_json::to_vec(record).context("serialize websocket frame record")?;
        line.push(b'\n');
        let mut file = self.file.lock().await;
        file.write_all(&line)
            .await
            .context("write websocket frame record")?;
        file.flush().await.context("flush websocket frame record")
    }

    async fn counts(&self) -> WebSocketRecorderCounts {
        self.state.lock().await.counts
    }
}

async fn create_websocket_recording(
    request_dir: &PathBuf,
    state: &AppState,
    request_meta: RequestMeta,
    session_id: &str,
    headers: &HeaderMap,
) -> anyhow::Result<Option<PathBuf>> {
    fs::create_dir_all(request_dir)
        .await
        .with_context(|| format!("create request dir {}", request_dir.display()))?;
    write_json_file(request_dir.join("request_meta.json"), &request_meta).await?;
    write_json_file(
        request_dir.join("request_headers.json"),
        &headers_to_records(headers, state.unsafe_record_secrets),
    )
    .await?;
    write_bytes_file(request_dir.join("request_body.raw"), b"").await?;
    write_manifest(state, session_id).await?;
    Ok(Some(request_dir.clone()))
}

async fn maybe_write_websocket_meta(request_dir: Option<&std::path::Path>, meta: WebSocketMeta) {
    let Some(request_dir) = request_dir else {
        return;
    };
    if let Err(err) = write_websocket_meta(request_dir, meta).await {
        tracing::warn!(?err, "failed to write websocket metadata");
    }
}

fn axum_to_tungstenite_message(message: AxumWsMessage) -> TungsteniteMessage {
    match message {
        AxumWsMessage::Text(text) => TungsteniteMessage::Text(text.to_string().into()),
        AxumWsMessage::Binary(bytes) => TungsteniteMessage::Binary(bytes.into()),
        AxumWsMessage::Ping(bytes) => TungsteniteMessage::Ping(bytes.into()),
        AxumWsMessage::Pong(bytes) => TungsteniteMessage::Pong(bytes.into()),
        AxumWsMessage::Close(close) => {
            TungsteniteMessage::Close(close.map(|close| TungsteniteCloseFrame {
                code: CloseCode::from(close.code),
                reason: close.reason.to_string().into(),
            }))
        }
    }
}

fn tungstenite_to_axum_message(message: TungsteniteMessage) -> AxumWsMessage {
    match message {
        TungsteniteMessage::Text(text) => AxumWsMessage::Text(text.to_string().into()),
        TungsteniteMessage::Binary(bytes) => AxumWsMessage::Binary(bytes.to_vec()),
        TungsteniteMessage::Ping(bytes) => AxumWsMessage::Ping(bytes.to_vec()),
        TungsteniteMessage::Pong(bytes) => AxumWsMessage::Pong(bytes.to_vec()),
        TungsteniteMessage::Close(close) => AxumWsMessage::Close(close.map(|close| CloseFrame {
            code: u16::from(close.code),
            reason: close.reason.to_string().into(),
        })),
        TungsteniteMessage::Frame(frame) => AxumWsMessage::Binary(frame.payload().to_vec().into()),
    }
}

fn axum_ws_opcode(message: &AxumWsMessage) -> &'static str {
    match message {
        AxumWsMessage::Text(_) => "text",
        AxumWsMessage::Binary(_) => "binary",
        AxumWsMessage::Ping(_) => "ping",
        AxumWsMessage::Pong(_) => "pong",
        AxumWsMessage::Close(_) => "close",
    }
}

fn tungstenite_ws_opcode(message: &TungsteniteMessage) -> &'static str {
    match message {
        TungsteniteMessage::Text(_) => "text",
        TungsteniteMessage::Binary(_) => "binary",
        TungsteniteMessage::Ping(_) => "ping",
        TungsteniteMessage::Pong(_) => "pong",
        TungsteniteMessage::Close(_) => "close",
        TungsteniteMessage::Frame(_) => "frame",
    }
}
