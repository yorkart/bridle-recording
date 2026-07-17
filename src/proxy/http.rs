use std::path::PathBuf;

use anyhow::{anyhow, Context};
use async_stream::stream;
use axum::{
    body::Body,
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, Method, Uri},
    response::Response,
};
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    sync::mpsc,
};
use tracing::warn;

use crate::{
    constants::UPSTREAM_MAX_ATTEMPTS,
    recording::{
        headers_to_records, recording_failure, write_bytes_file, write_error_response_meta,
        write_json_file, write_manifest, RecordingContext, RECORDING_QUEUE_CAPACITY,
    },
    types::{AppState, RequestMeta, ResponseMeta},
    util::{
        build_upstream_url, expects_sse, is_sse_content_type, next_request_index, now_rfc3339,
        request_dir, reqwest_method, session_from_headers, should_forward_http_header,
        should_forward_response_header,
    },
};

pub async fn handle_proxy(
    state: AppState,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    path: String,
    body: Bytes,
) -> anyhow::Result<Response> {
    let started_at = now_rfc3339();
    let (session_id, session_source) = session_from_headers(&headers, &state.session_header);
    let user_agent = headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-");
    tracing::info!(
        profile = %state.profile.name,
        method = %method,
        path = %format!("/{path}"),
        session_id = %session_id,
        user_agent = %user_agent,
        "received http proxy request"
    );
    let upstream_url = build_upstream_url(&state.profile.upstream, &path, uri.query())?;
    let recording = start_http_recording(
        state.clone(),
        RequestMeta {
            index: 0,
            session_id: session_id.clone(),
            session_source,
            started_at: started_at.clone(),
            method: method.to_string(),
            path: format!("/{path}"),
            query: uri.query().map(ToOwned::to_owned),
            upstream_url: upstream_url.to_string(),
            request_body_bytes: body.len(),
        },
        headers.clone(),
        body.clone(),
        session_id,
    );

    let upstream_response =
        match send_upstream_with_retry(&state, &method, &headers, &body, upstream_url).await {
            Ok(response) => response,
            Err(err) => {
                record_error_response_in_background(recording, started_at.clone(), err.to_string());
                return Err(anyhow!("upstream request failed: {err}"));
            }
        };

    let status = upstream_response.status();
    let response_headers = upstream_response.headers().clone();
    record_json_in_background(
        recording.clone(),
        "response_headers.json",
        headers_to_records(&response_headers, state.unsafe_record_secrets),
        "http_response_headers",
    );

    let is_sse = expects_sse(&headers) || is_sse_content_type(&response_headers);

    let mut response_builder = Response::builder().status(status.as_u16());
    for (name, value) in response_headers.iter() {
        if !should_forward_response_header(name) {
            continue;
        }
        response_builder = response_builder.header(name, value);
    }
    if is_sse && !response_headers.contains_key(CONTENT_TYPE) {
        response_builder =
            response_builder.header(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    }

    let recording_name = if is_sse {
        "response_sse.raw"
    } else {
        "response_body.raw"
    };
    let body_stream =
        record_streaming_response(upstream_response, recording, started_at, recording_name);
    response_builder
        .body(Body::from_stream(body_stream))
        .context("build streaming response")
}

async fn send_upstream_with_retry(
    state: &AppState,
    method: &Method,
    headers: &HeaderMap,
    body: &Bytes,
    upstream_url: reqwest::Url,
) -> anyhow::Result<reqwest::Response> {
    let method = reqwest_method(method)?;
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 1..=UPSTREAM_MAX_ATTEMPTS {
        let mut upstream_request = state.client.request(method.clone(), upstream_url.clone());
        for (name, value) in headers.iter() {
            if !should_forward_http_header(name, state.proxy_mode) {
                continue;
            }
            upstream_request = upstream_request.header(name.as_str(), value.as_bytes());
        }
        let upstream_request = upstream_request.body(body.clone());

        match upstream_request.send().await {
            Ok(response) => {
                if should_retry_status(response.status()) && attempt < UPSTREAM_MAX_ATTEMPTS {
                    warn!(
                        attempt,
                        max_attempts = UPSTREAM_MAX_ATTEMPTS,
                        status = %response.status(),
                        profile = %state.profile.name,
                        "retrying upstream request after retryable status"
                    );
                    tokio::time::sleep(retry_delay(attempt)).await;
                    continue;
                }
                return Ok(response);
            }
            Err(err) => {
                if should_retry_error(&err) && attempt < UPSTREAM_MAX_ATTEMPTS {
                    warn!(
                        attempt,
                        max_attempts = UPSTREAM_MAX_ATTEMPTS,
                        error = %err,
                        error_debug = ?err,
                        is_timeout = err.is_timeout(),
                        is_connect = err.is_connect(),
                        is_request = err.is_request(),
                        is_status = err.is_status(),
                        url = ?err.url(),
                        profile = %state.profile.name,
                        "retrying upstream request after transport error"
                    );
                    last_error = Some(anyhow!(err));
                    tokio::time::sleep(retry_delay(attempt)).await;
                    continue;
                }
                warn!(
                    attempt,
                    max_attempts = UPSTREAM_MAX_ATTEMPTS,
                    error = %err,
                    error_debug = ?err,
                    is_timeout = err.is_timeout(),
                    is_connect = err.is_connect(),
                    is_request = err.is_request(),
                    is_status = err.is_status(),
                    url = ?err.url(),
                    profile = %state.profile.name,
                    "upstream request failed after transport error"
                );
                return Err(anyhow!(err));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("upstream request retries exhausted")))
}

pub(crate) fn should_retry_status(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::BAD_GATEWAY
            | reqwest::StatusCode::SERVICE_UNAVAILABLE
            | reqwest::StatusCode::GATEWAY_TIMEOUT
            | reqwest::StatusCode::TOO_MANY_REQUESTS
    )
}

pub(crate) fn should_retry_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request()
}

pub(crate) fn retry_delay(attempt: usize) -> std::time::Duration {
    match attempt {
        1 => std::time::Duration::from_millis(200),
        2 => std::time::Duration::from_millis(500),
        _ => std::time::Duration::from_secs(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn response_recording_finalizes_after_downstream_body_is_dropped() {
        let app = Router::new().route(
            "/",
            get(|| async {
                let body = Body::from_stream(stream! {
                    yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
                        b"event: response.completed\ndata: {\"status\":\"completed\"}\n\n",
                    ));
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                    yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"data: trailing-bytes\n\n"));
                });
                axum::response::Response::builder()
                    .header(CONTENT_TYPE, "text/event-stream")
                    .body(body)
                    .unwrap()
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let upstream_response = reqwest::get(format!("http://{addr}/")).await.unwrap();
        let temp = tempfile::tempdir().unwrap();
        let mut downstream = Box::pin(record_streaming_response(
            upstream_response,
            RecordingContext::spawn({
                let request_dir = temp.path().to_path_buf();
                async move { Some(request_dir) }
            }),
            now_rfc3339(),
            "response_sse.raw",
        ));

        let first = downstream.next().await.unwrap().unwrap();
        assert!(first.starts_with(b"event: response.completed"));
        drop(downstream);

        let meta_path = temp.path().join("response_meta.json");
        for _ in 0..50 {
            if meta_path.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let raw = std::fs::read_to_string(temp.path().join("response_sse.raw")).unwrap();
        assert!(raw.contains("event: response.completed"));
        assert!(raw.contains("data: trailing-bytes"));

        let meta: ResponseMeta =
            serde_json::from_slice(&std::fs::read(meta_path).unwrap()).unwrap();
        assert_eq!(meta.status, 200);
        assert_eq!(meta.response_body_bytes, raw.len());
        assert!(meta.upstream_error.is_none());
    }

    #[tokio::test]
    async fn slow_response_recording_does_not_delay_downstream_body() {
        let app = Router::new().route(
            "/",
            get(|| async {
                Body::from_stream(stream! {
                    yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"first-"));
                    yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"second"));
                })
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let upstream_response = reqwest::get(format!("http://{addr}/")).await.unwrap();
        let temp = tempfile::tempdir().unwrap();
        let recording = RecordingContext::spawn({
            let request_dir = temp.path().to_path_buf();
            async move { Some(request_dir) }
        })
        .with_stream_write_delay(std::time::Duration::from_millis(500));
        let mut downstream = Box::pin(record_streaming_response(
            upstream_response,
            recording,
            now_rfc3339(),
            "response_body.raw",
        ));

        let forwarded = tokio::time::timeout(std::time::Duration::from_millis(300), async {
            let mut forwarded = Vec::new();
            while let Some(chunk) = downstream.next().await {
                forwarded.extend_from_slice(&chunk.unwrap());
            }
            forwarded
        })
        .await
        .expect("slow recording must not delay downstream response");
        assert_eq!(forwarded, b"first-second");

        let meta_path = temp.path().join("response_meta.json");
        for _ in 0..200 {
            if meta_path.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            std::fs::read(temp.path().join("response_body.raw")).unwrap(),
            b"first-second"
        );
        assert!(meta_path.exists());
    }

    #[tokio::test]
    async fn response_recording_failure_marks_incomplete_without_changing_body() {
        let app = Router::new().route(
            "/",
            get(|| async { Body::from(Bytes::from_static(b"verbatim-response")) }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let upstream_response = reqwest::get(format!("http://{addr}/")).await.unwrap();
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir(temp.path().join("response_body.raw")).unwrap();
        let recording = RecordingContext::spawn({
            let request_dir = temp.path().to_path_buf();
            async move { Some(request_dir) }
        });
        let mut downstream = Box::pin(record_streaming_response(
            upstream_response,
            recording,
            now_rfc3339(),
            "response_body.raw",
        ));

        let mut forwarded = Vec::new();
        while let Some(chunk) = downstream.next().await {
            forwarded.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(forwarded, b"verbatim-response");

        let marker_path = temp.path().join("recording_incomplete.json");
        for _ in 0..100 {
            if marker_path.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let marker: serde_json::Value =
            serde_json::from_slice(&std::fs::read(marker_path).unwrap()).unwrap();
        assert_eq!(marker["incomplete"], true);
        assert_eq!(marker["stage"], "http_response_body_create");
    }
}

fn record_streaming_response(
    upstream_response: reqwest::Response,
    recording: RecordingContext,
    started_at: String,
    recording_name: &'static str,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> {
    let (downstream_sender, mut downstream_receiver) = mpsc::channel(1);
    let (recording_sender, recording_receiver) = mpsc::channel(RECORDING_QUEUE_CAPACITY);
    let (completion_sender, completion_receiver) = tokio::sync::oneshot::channel();
    tokio::spawn(forward_response_in_background(
        upstream_response,
        downstream_sender,
        recording_sender,
        completion_sender,
    ));
    tokio::spawn(record_response_in_background(
        recording,
        recording_name,
        started_at,
        recording_receiver,
        completion_receiver,
    ));

    stream! {
        while let Some(bytes) = downstream_receiver.recv().await {
            yield Ok(bytes);
        }
    }
}

struct ResponseRecordingCompletion {
    status: u16,
    response_body_bytes: usize,
    upstream_error: Option<String>,
    queue_error: Option<String>,
}

async fn forward_response_in_background(
    upstream_response: reqwest::Response,
    downstream_sender: mpsc::Sender<Bytes>,
    recording_sender: mpsc::Sender<Bytes>,
    completion_sender: tokio::sync::oneshot::Sender<ResponseRecordingCompletion>,
) {
    let status = upstream_response.status().as_u16();
    let mut stream = upstream_response.bytes_stream();
    let mut response_body_bytes = 0usize;
    let mut upstream_error = None;
    let mut downstream_open = true;
    let mut recording_sender = Some(recording_sender);
    let mut queue_error = None;

    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                response_body_bytes += bytes.len();
                if downstream_open {
                    downstream_open = downstream_sender.send(bytes.clone()).await.is_ok();
                }
                if let Some(sender) = recording_sender.as_ref() {
                    if let Err(err) = sender.try_send(bytes) {
                        let error = match err {
                            mpsc::error::TrySendError::Full(_) => {
                                "HTTP response recording queue filled while storage was slow"
                            }
                            mpsc::error::TrySendError::Closed(_) => {
                                "HTTP response recording worker stopped before response completion"
                            }
                        };
                        warn!(
                            error,
                            "HTTP forwarding continues without further body recording"
                        );
                        queue_error = Some(error.to_owned());
                        recording_sender = None;
                    }
                }
            }
            Err(err) => {
                upstream_error = Some(err.to_string());
                break;
            }
        }
    }

    drop(recording_sender);
    let _ = completion_sender.send(ResponseRecordingCompletion {
        status,
        response_body_bytes,
        upstream_error,
        queue_error,
    });
}

async fn record_response_in_background(
    recording: RecordingContext,
    recording_name: &'static str,
    started_at: String,
    mut chunks: mpsc::Receiver<Bytes>,
    completion: tokio::sync::oneshot::Receiver<ResponseRecordingCompletion>,
) {
    let Some(request_dir) = recording.request_dir().await else {
        return;
    };
    let raw_path = request_dir.join(recording_name);
    let mut raw_file = match File::create(&raw_path).await {
        Ok(file) => Some(file),
        Err(err) => {
            recording_failure(Some(&request_dir), "http_response_body_create", &err).await;
            None
        }
    };
    while let Some(bytes) = chunks.recv().await {
        let Some(file) = raw_file.as_mut() else {
            continue;
        };
        recording.before_stream_write().await;
        if let Err(err) = file.write_all(&bytes).await {
            recording_failure(Some(&request_dir), "http_response_body_write", &err).await;
            raw_file = None;
        }
    }

    if let Some(raw_file) = raw_file.as_mut() {
        recording.before_stream_write().await;
        if let Err(err) = raw_file.flush().await {
            recording_failure(Some(&request_dir), "http_response_body_flush", &err).await;
        }
    }
    let completion = match completion.await {
        Ok(completion) => completion,
        Err(err) => {
            recording_failure(Some(&request_dir), "http_response_body_channel", &err).await;
            return;
        }
    };
    if let Some(error) = completion.queue_error.as_ref() {
        recording_failure(Some(&request_dir), "http_response_body_queue", error).await;
    }
    let response_meta = ResponseMeta {
        status: completion.status,
        started_at,
        completed_at: now_rfc3339(),
        response_body_bytes: completion.response_body_bytes,
        sse_event_count: 0,
        upstream_error: completion.upstream_error,
    };
    if let Err(err) = write_json_file(request_dir.join("response_meta.json"), &response_meta).await
    {
        recording_failure(Some(&request_dir), "http_response_metadata", &err).await;
    }
}

fn start_http_recording(
    state: AppState,
    mut request_meta: RequestMeta,
    headers: HeaderMap,
    body: Bytes,
    session_id: String,
) -> RecordingContext {
    RecordingContext::spawn(async move {
        let index = match next_request_index(&state, &session_id).await {
            Ok(index) => index,
            Err(err) => {
                recording_failure(None, "http_request_index", &err).await;
                return None;
            }
        };
        request_meta.index = index;
        let request_dir = request_dir(&state.output_dir, &session_id, index);
        match create_http_recording(
            &state,
            &request_dir,
            request_meta,
            &headers,
            &body,
            &session_id,
        )
        .await
        {
            Ok(request_dir) => request_dir,
            Err(err) => {
                recording_failure(Some(&request_dir), "http_request_setup", &err).await;
                None
            }
        }
    })
}

fn record_json_in_background<T>(
    recording: RecordingContext,
    file_name: &'static str,
    value: T,
    stage: &'static str,
) where
    T: serde::Serialize + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let Some(request_dir) = recording.request_dir().await else {
            return;
        };
        if let Err(err) = write_json_file(request_dir.join(file_name), &value).await {
            recording_failure(Some(&request_dir), stage, &err).await;
        }
    });
}

fn record_error_response_in_background(
    recording: RecordingContext,
    started_at: String,
    error: String,
) {
    tokio::spawn(async move {
        let Some(request_dir) = recording.request_dir().await else {
            return;
        };
        if let Err(err) = write_error_response_meta(&request_dir, started_at, error).await {
            recording_failure(Some(&request_dir), "http_error_response_metadata", &err).await;
        }
    });
}

async fn create_http_recording(
    state: &AppState,
    request_dir: &PathBuf,
    request_meta: RequestMeta,
    headers: &HeaderMap,
    body: &Bytes,
    session_id: &str,
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
    write_bytes_file(request_dir.join("request_body.raw"), body).await?;
    write_manifest(state, session_id).await?;
    Ok(Some(request_dir.clone()))
}
