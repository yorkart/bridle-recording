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
    io::AsyncWriteExt,
    sync::Mutex,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest,
        protocol::{
            frame::coding::CloseCode, CloseFrame as TungsteniteCloseFrame,
            Message as TungsteniteMessage,
        },
    },
};

use crate::{
    types::{
        AppState, RequestMeta, WebSocketCloseRecord, WebSocketDirection, WebSocketFrameRecord,
        WebSocketMeta,
    },
    util::{
        build_upstream_websocket_url, headers_to_records, next_request_index, now_rfc3339,
        request_dir, session_from_headers, should_forward_websocket_header, strip_responses_lite_from_ws_text,
        write_bytes_file, write_json_file, write_manifest, write_websocket_meta,
    },
};

pub struct PreparedWebSocketProxy {
    pub request_dir: PathBuf,
    pub started_at: String,
    pub upstream_url: Url,
    pub headers: HeaderMap,
    pub unsafe_record_secrets: bool,
    pub strip_responses_lite: bool,
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
    let request_dir = request_dir(&state.output_dir, &session_id, index);
    fs::create_dir_all(&request_dir)
        .await
        .with_context(|| format!("create request dir {}", request_dir.display()))?;

    let upstream_url = build_upstream_websocket_url(&state.profile.upstream, &path, uri.query())?;
    let request_meta = RequestMeta {
        index,
        session_id: session_id.clone(),
        session_source,
        started_at: started_at.clone(),
        method: method.to_string(),
        path: format!("/{path}"),
        query: uri.query().map(ToOwned::to_owned),
        upstream_url: upstream_url.to_string(),
        request_body_bytes: 0,
    };

    write_json_file(request_dir.join("request_meta.json"), &request_meta).await?;
    write_json_file(
        request_dir.join("request_headers.json"),
        &headers_to_records(&headers, state.unsafe_record_secrets),
    )
    .await?;
    write_bytes_file(request_dir.join("request_body.raw"), b"").await?;
    write_manifest(&state, &session_id).await?;

    Ok(PreparedWebSocketProxy {
        request_dir,
        started_at,
        upstream_url,
        headers,
        unsafe_record_secrets: state.unsafe_record_secrets,
        strip_responses_lite: state.strip_responses_lite,
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
        if should_forward_websocket_header(name, prepared.strip_responses_lite) {
            upstream_request
                .headers_mut()
                .insert(name.clone(), value.clone());
        }
    }

    let (upstream, upstream_response) = match connect_async(upstream_request).await {
        Ok(result) => result,
        Err(err) => {
            write_websocket_meta(
                &prepared.request_dir,
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
            .await?;
            return Err(anyhow!("connect upstream websocket failed: {err}"));
        }
    };

    write_json_file(
        prepared.request_dir.join("websocket_response_headers.json"),
        &headers_to_records(upstream_response.headers(), prepared.unsafe_record_secrets),
    )
    .await?;

    let recorder = WebSocketRecorder::new(
        File::create(prepared.request_dir.join("websocket_frames.jsonl"))
            .await
            .with_context(|| {
                format!(
                    "create {}",
                    prepared
                        .request_dir
                        .join("websocket_frames.jsonl")
                        .display()
                )
            })?,
    );

    let (mut client_sender, mut client_receiver) = client.split();
    let (mut upstream_sender, mut upstream_receiver) = upstream.split();

    let client_to_upstream = async {
        while let Some(message) = client_receiver.next().await {
            let message = message.context("read client websocket frame")?;
            let is_close = matches!(message, AxumWsMessage::Close(_));
            recorder
                .record_axum(WebSocketDirection::ClientToUpstream, &message)
                .await?;
            let message = if prepared.strip_responses_lite {
                strip_responses_lite_from_axum_ws_message(message)
            } else {
                message
            };
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
            recorder
                .record_tungstenite(WebSocketDirection::UpstreamToClient, &message)
                .await?;
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

    let counts = recorder.counts().await;
    write_websocket_meta(
        &prepared.request_dir,
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
    .await?;

    Ok(())
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

    async fn record_axum(
        &self,
        direction: WebSocketDirection,
        message: &AxumWsMessage,
    ) -> anyhow::Result<()> {
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
        self.append_record(&record).await
    }

    async fn record_tungstenite(
        &self,
        direction: WebSocketDirection,
        message: &TungsteniteMessage,
    ) -> anyhow::Result<()> {
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
        self.append_record(&record).await
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

fn strip_responses_lite_from_axum_ws_message(message: AxumWsMessage) -> AxumWsMessage {
    match message {
        AxumWsMessage::Text(text) => {
            let original = text.to_string();
            match strip_responses_lite_from_ws_text(&original) {
                Some(stripped) => AxumWsMessage::Text(stripped.into()),
                None => AxumWsMessage::Text(text),
            }
        }
        other => other,
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
