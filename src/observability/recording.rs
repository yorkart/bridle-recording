use super::*;

pub(super) async fn sessions_inner(
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

pub(super) async fn session_inner(
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
    let provider = ObservabilityProvider::from_profile(profile);
    let requests = load_observed_calls(&session_dir, provider).await?;
    let turns = provider.build_turns(&requests);
    let flows = provider.build_flows(&requests, &turns);
    Ok(ObservedSession {
        profile: profile.to_owned(),
        session_id: session_id.to_owned(),
        raw_root: session_dir.display().to_string(),
        manifest,
        flows,
        turns,
        requests,
    })
}

pub(super) async fn load_observed_calls(
    session_dir: &Path,
    provider: ObservabilityProvider,
) -> anyhow::Result<Vec<ObservedCall>> {
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
        calls.push(load_observed_call(index, request_dir, provider).await?);
    }
    attach_tool_results(&mut calls);
    Ok(calls)
}

pub(super) async fn load_observed_call(
    index: String,
    request_dir: PathBuf,
    provider: ObservabilityProvider,
) -> anyhow::Result<ObservedCall> {
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

    let request_kind = provider.classify_request_kind(&request_body);
    let prompt_blocks = provider.prompt_blocks(&request_body);
    let visible_user_messages = provider.visible_user_messages(&prompt_blocks);
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
        request_kind,
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

pub(super) async fn load_response_meta(
    path: &Path,
) -> (Option<ResponseMeta>, String, Option<String>) {
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

pub(super) async fn load_recording_incomplete(
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

pub(super) async fn request_body_matches_content_length(
    request_dir: &Path,
    recorded_bytes: usize,
) -> bool {
    let Ok(headers) =
        read_json::<Vec<HeaderRecord>>(&request_dir.join("request_headers.json")).await
    else {
        return false;
    };
    recorded_header_text(&headers, "content-length")
        .and_then(|value| value.trim().parse::<usize>().ok())
        == Some(recorded_bytes)
}

pub(super) async fn decode_response_body_for_observability(
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

pub(super) fn recorded_header_text<'a>(headers: &'a [HeaderRecord], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case(name))
        .and_then(|header| match &header.value {
            HeaderValueRecord::Text { value } => Some(value.as_str()),
            HeaderValueRecord::BinaryBase64 { .. } => None,
        })
}

pub(super) fn decode_request_body_json(bytes: &[u8]) -> anyhow::Result<serde_json::Value> {
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

pub(super) fn observed_payload(bytes: &[u8]) -> ObservedPayload {
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

pub(super) fn looks_like_sse_response(bytes: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return false;
    };
    text.lines().any(|line| {
        let line = line.trim_start();
        line.starts_with("event:") || line.starts_with("data:")
    })
}

pub(super) async fn load_websocket_frames(
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

pub(super) fn build_call_timeline(
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

pub(super) fn attach_tool_results(calls: &mut [ObservedCall]) {
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

pub(super) async fn request_files(request_dir: &Path) -> anyhow::Result<Vec<ObservedFile>> {
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

pub(super) fn duration_ms(started_at: &str, completed_at: &str) -> Option<i64> {
    let start = started_at.parse::<DateTime<Utc>>().ok()?;
    let end = completed_at.parse::<DateTime<Utc>>().ok()?;
    Some((end - start).num_milliseconds())
}

pub(super) async fn read_json<T>(path: &Path) -> anyhow::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let bytes = fs::read(path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

pub(super) async fn write_json_pretty<T>(path: PathBuf, value: &T) -> anyhow::Result<()>
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
