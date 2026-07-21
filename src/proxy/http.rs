use std::{
    path::PathBuf,
    pin::Pin,
    sync::Mutex as StdMutex,
    task::{Context as TaskContext, Poll},
};

use anyhow::{anyhow, Context};
use async_stream::stream;
use axum::{
    body::Body,
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, Method, Uri},
    response::Response,
};
use bytes::Bytes;
use futures_util::StreamExt;
use http_body::{Body as HttpBody, Frame, SizeHint};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    sync::mpsc,
};
use tracing::warn;

use crate::{
    recording::{
        headers_to_records, recording_failure, write_error_response_meta, write_json_file,
        write_manifest, RecordingContext, RECORDING_QUEUE_CAPACITY,
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
    body: Body,
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
    let request_meta = RequestMeta {
        index: 0,
        session_id: session_id.clone(),
        session_source,
        started_at: started_at.clone(),
        method: method.to_string(),
        path: format!("/{path}"),
        query: uri.query().map(ToOwned::to_owned),
        upstream_url: upstream_url.to_string(),
        request_body_bytes: 0,
    };
    let recording = start_http_recording(state.clone(), request_meta, headers.clone(), session_id);
    let body = record_streaming_request(body, recording.clone());

    let upstream_response = match send_upstream(&state, &method, &headers, body, upstream_url).await
    {
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
        headers_to_records(&response_headers),
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

async fn send_upstream(
    state: &AppState,
    method: &Method,
    headers: &HeaderMap,
    body: reqwest::Body,
    upstream_url: reqwest::Url,
) -> anyhow::Result<reqwest::Response> {
    let method = reqwest_method(method)?;
    let mut upstream_request = state.client.request(method, upstream_url);
    for (name, value) in headers.iter() {
        if !should_forward_http_header(name) {
            continue;
        }
        upstream_request = upstream_request.header(name.as_str(), value.as_bytes());
    }

    upstream_request.body(body).send().await.map_err(Into::into)
}

fn record_streaming_request(body: Body, recording: RecordingContext) -> reqwest::Body {
    let (recording_sender, recording_receiver) = mpsc::unbounded_channel();
    let (completion_sender, completion_receiver) = tokio::sync::oneshot::channel();
    tokio::spawn(record_request_in_background(
        recording,
        recording_receiver,
        completion_receiver,
    ));

    let initially_complete = body.is_end_stream();
    let expected_body_bytes = body.size_hint().exact();
    let mut body = RequestRecordingBody {
        inner: StdMutex::new(Box::pin(body)),
        recording_sender: Some(recording_sender),
        completion_sender: Some(completion_sender),
        request_body_bytes: 0,
        expected_body_bytes,
        queue_error: None,
    };
    if initially_complete {
        body.finish(None);
    }
    reqwest::Body::wrap(body)
}

struct RequestRecordingBody {
    inner: StdMutex<Pin<Box<Body>>>,
    recording_sender: Option<mpsc::UnboundedSender<Bytes>>,
    completion_sender: Option<tokio::sync::oneshot::Sender<RequestRecordingCompletion>>,
    request_body_bytes: usize,
    expected_body_bytes: Option<u64>,
    queue_error: Option<String>,
}

impl RequestRecordingBody {
    fn finish(&mut self, body_error: Option<String>) {
        let Some(completion_sender) = self.completion_sender.take() else {
            return;
        };
        self.recording_sender.take();
        let _ = completion_sender.send(RequestRecordingCompletion {
            request_body_bytes: self.request_body_bytes,
            body_error,
            queue_error: self.queue_error.take(),
        });
    }
}

impl Drop for RequestRecordingBody {
    fn drop(&mut self) {
        let body_error = (self.expected_body_bytes != Some(self.request_body_bytes as u64))
            .then(|| "upstream stopped consuming the request body before it completed".to_owned());
        self.finish(body_error);
    }
}

impl HttpBody for RequestRecordingBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let result = this
            .inner
            .get_mut()
            .expect("request body mutex poisoned")
            .as_mut()
            .poll_frame(cx);

        match result {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(bytes) = frame.data_ref() {
                    this.request_body_bytes += bytes.len();
                    if let Some(sender) = this.recording_sender.as_ref() {
                        if sender.send(bytes.clone()).is_err() {
                            this.queue_error = Some(
                                "HTTP request recording worker stopped before request completion"
                                    .to_owned(),
                            );
                            this.recording_sender = None;
                        }
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(err))) => {
                this.finish(Some(err.to_string()));
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                this.finish(None);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner
            .lock()
            .expect("request body mutex poisoned")
            .as_ref()
            .is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner
            .lock()
            .expect("request body mutex poisoned")
            .as_ref()
            .size_hint()
    }
}

struct RequestRecordingCompletion {
    request_body_bytes: usize,
    body_error: Option<String>,
    queue_error: Option<String>,
}

async fn record_request_in_background(
    recording: RecordingContext,
    mut chunks: mpsc::UnboundedReceiver<Bytes>,
    completion: tokio::sync::oneshot::Receiver<RequestRecordingCompletion>,
) {
    let Some(request_dir) = recording.request_dir().await else {
        return;
    };
    let raw_path = request_dir.join("request_body.raw");
    let mut raw_file = match File::create(&raw_path).await {
        Ok(file) => Some(file),
        Err(err) => {
            recording_failure(Some(&request_dir), "http_request_body_create", &err).await;
            None
        }
    };
    while let Some(bytes) = chunks.recv().await {
        let Some(file) = raw_file.as_mut() else {
            continue;
        };
        recording.before_stream_write().await;
        if let Err(err) = file.write_all(&bytes).await {
            recording_failure(Some(&request_dir), "http_request_body_write", &err).await;
            raw_file = None;
        }
    }

    if let Some(raw_file) = raw_file.as_mut() {
        recording.before_stream_write().await;
        if let Err(err) = raw_file.flush().await {
            recording_failure(Some(&request_dir), "http_request_body_flush", &err).await;
        }
    }
    let completion = match completion.await {
        Ok(completion) => completion,
        Err(err) => {
            recording_failure(Some(&request_dir), "http_request_body_channel", &err).await;
            return;
        }
    };
    if let Some(error) = completion.queue_error.as_ref() {
        recording_failure(Some(&request_dir), "http_request_body_queue", error).await;
    }
    if let Some(error) = completion.body_error.as_ref() {
        recording_failure(Some(&request_dir), "http_request_body_stream", error).await;
    }
    if let Err(err) = update_request_body_bytes(&request_dir, completion.request_body_bytes).await {
        recording_failure(Some(&request_dir), "http_request_metadata", &err).await;
    }
}

async fn update_request_body_bytes(
    request_dir: &std::path::Path,
    request_body_bytes: usize,
) -> anyhow::Result<()> {
    let meta_path = request_dir.join("request_meta.json");
    let raw = fs::read(&meta_path)
        .await
        .with_context(|| format!("read {}", meta_path.display()))?;
    let mut request_meta: RequestMeta =
        serde_json::from_slice(&raw).with_context(|| format!("parse {}", meta_path.display()))?;
    request_meta.request_body_bytes = request_body_bytes;
    write_json_file(meta_path, &request_meta).await
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn dropping_after_exact_size_does_not_mark_request_body_incomplete() {
        let (recording_sender, _recording_receiver) = mpsc::unbounded_channel();
        let (completion_sender, completion_receiver) = tokio::sync::oneshot::channel();
        let body = RequestRecordingBody {
            inner: StdMutex::new(Box::pin(Body::empty())),
            recording_sender: Some(recording_sender),
            completion_sender: Some(completion_sender),
            request_body_bytes: 4,
            expected_body_bytes: Some(4),
            queue_error: None,
        };

        drop(body);

        let completion = completion_receiver.await.unwrap();
        assert!(completion.body_error.is_none());
        assert_eq!(completion.request_body_bytes, 4);
    }

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
    async fn slow_request_recording_does_not_delay_upstream_body() {
        let app = Router::new().route(
            "/",
            axum::routing::post(|body: Body| async move {
                axum::body::to_bytes(body, usize::MAX).await.unwrap()
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let temp = tempfile::tempdir().unwrap();
        write_json_file(
            temp.path().join("request_meta.json"),
            &RequestMeta {
                index: 0,
                session_id: "test".to_owned(),
                session_source: crate::types::SessionSource::Unknown,
                started_at: now_rfc3339(),
                method: "POST".to_owned(),
                path: "/".to_owned(),
                query: None,
                upstream_url: format!("http://{addr}/"),
                request_body_bytes: 0,
            },
        )
        .await
        .unwrap();
        let recording = RecordingContext::spawn({
            let request_dir = temp.path().to_path_buf();
            async move { Some(request_dir) }
        })
        .with_stream_write_delay(std::time::Duration::from_millis(500));
        let body = Body::from_stream(stream! {
            yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"first-"));
            yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"second"));
        });
        let body = record_streaming_request(body, recording);

        let forwarded = tokio::time::timeout(std::time::Duration::from_millis(300), async {
            reqwest::Client::new()
                .post(format!("http://{addr}/"))
                .body(body)
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        })
        .await
        .expect("slow recording must not delay the upstream request");
        assert_eq!(forwarded, b"first-second".as_slice());

        let meta_path = temp.path().join("request_meta.json");
        for _ in 0..300 {
            let complete = std::fs::read(&meta_path)
                .ok()
                .and_then(|raw| serde_json::from_slice::<RequestMeta>(&raw).ok())
                .map(|meta| meta.request_body_bytes == forwarded.len())
                .unwrap_or(false);
            if complete {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            std::fs::read(temp.path().join("request_body.raw")).unwrap(),
            forwarded
        );
        let meta: RequestMeta = serde_json::from_slice(&std::fs::read(meta_path).unwrap()).unwrap();
        assert_eq!(meta.request_body_bytes, forwarded.len());
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
        match create_http_recording(&state, &request_dir, request_meta, &headers, &session_id).await
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
    session_id: &str,
) -> anyhow::Result<Option<PathBuf>> {
    fs::create_dir_all(request_dir)
        .await
        .with_context(|| format!("create request dir {}", request_dir.display()))?;
    write_json_file(request_dir.join("request_meta.json"), &request_meta).await?;
    write_json_file(
        request_dir.join("request_headers.json"),
        &headers_to_records(headers),
    )
    .await?;
    write_manifest(state, session_id).await?;
    Ok(Some(request_dir.clone()))
}
