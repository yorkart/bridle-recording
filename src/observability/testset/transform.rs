use super::*;

pub(in crate::observability) fn trim_request_body(
    value: &mut serde_json::Value,
    remove: &TestsetRemovalOptions,
) {
    let Some(body) = value.as_object_mut() else {
        return;
    };
    if remove.tools {
        body.remove("tools");
        body.remove("tool_choice");
        body.remove("parallel_tool_calls");
    }
    let remove_instructions = match body.get_mut("instructions") {
        Some(serde_json::Value::String(instructions)) => !trim_prompt_text(instructions, remove),
        _ => false,
    };
    if remove_instructions {
        body.remove("instructions");
    }
    let Some(input) = body
        .get_mut("input")
        .and_then(serde_json::Value::as_array_mut)
    else {
        return;
    };
    input.retain_mut(|item| {
        if remove.tools
            && item
                .get("type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(is_tool_artifact_type)
        {
            return false;
        }
        let Some(content) = item.get_mut("content") else {
            return true;
        };
        match content {
            serde_json::Value::String(text) => trim_prompt_text(text, remove),
            serde_json::Value::Array(parts) => {
                parts.retain_mut(|part| match part {
                    serde_json::Value::String(text) => trim_prompt_text(text, remove),
                    serde_json::Value::Object(fields) => {
                        let text = if fields.contains_key("text") {
                            fields.get_mut("text")
                        } else {
                            fields.get_mut("output_text")
                        };
                        match text {
                            Some(serde_json::Value::String(text)) => trim_prompt_text(text, remove),
                            _ => true,
                        }
                    }
                    _ => true,
                });
                !parts.is_empty()
            }
            _ => true,
        }
    });
}

pub(in crate::observability) fn trim_prompt_text(
    text: &mut String,
    remove: &TestsetRemovalOptions,
) -> bool {
    let tagged_sections = [
        (
            remove.skills,
            "<skills_instructions>",
            "</skills_instructions>",
        ),
        (remove.apps, "<apps_instructions>", "</apps_instructions>"),
        (
            remove.plugins,
            "<plugins_instructions>",
            "</plugins_instructions>",
        ),
        (
            remove.plugins,
            "<recommended_plugins>",
            "</recommended_plugins>",
        ),
        (
            remove.derived_prompt,
            "<environment_context>",
            "</environment_context>",
        ),
        (
            remove.derived_prompt,
            "<permissions instructions>",
            "</permissions instructions>",
        ),
    ];
    for (enabled, open, close) in tagged_sections {
        if enabled {
            remove_tagged_sections(text, open, close);
        }
    }
    !text.trim().is_empty()
}

pub(in crate::observability) fn remove_tagged_sections(text: &mut String, open: &str, close: &str) {
    while let Some(start) = text.find(open) {
        let content_start = start + open.len();
        let Some(relative_end) = text[content_start..].find(close) else {
            break;
        };
        let end = content_start + relative_end + close.len();
        text.replace_range(start..end, "");
    }
}

pub(in crate::observability) fn redact_json_value(
    value: &mut serde_json::Value,
    sensitive_values: &[&str],
) {
    match value {
        serde_json::Value::String(text) => {
            *text = redact_text(text, sensitive_values);
        }
        serde_json::Value::Array(values) => {
            for value in values {
                redact_json_value(value, sensitive_values);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                redact_json_value(value, sensitive_values);
            }
        }
        _ => {}
    }
}

pub(in crate::observability) fn redact_text(text: &str, sensitive_values: &[&str]) -> String {
    sensitive_values
        .iter()
        .fold(text.to_owned(), |text, value| {
            text.replace(value, REDACTED_TESTSET_HEADER_VALUE)
        })
}

pub(in crate::observability) fn transform_request_body(
    bytes: &[u8],
    request: &SaveTestsetRequest,
) -> anyhow::Result<Vec<u8>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let sensitive_values = request.sensitive_values();
    if sensitive_values.is_empty() && !request.remove.any() {
        return Ok(bytes.to_vec());
    }
    let (mut value, compressed) = match serde_json::from_slice::<serde_json::Value>(bytes) {
        Ok(value) => (value, false),
        Err(json_err) => {
            let mut decoder = match zstd::stream::read::Decoder::new(std::io::Cursor::new(bytes)) {
                Ok(decoder) => decoder,
                Err(_) if !request.remove.any() => {
                    return Ok(redact_bytes(bytes, &sensitive_values));
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("decode request body for trimming after {json_err}")
                    })
                }
            };
            let mut decoded = Vec::new();
            decoder
                .read_to_end(&mut decoded)
                .context("decode zstd request body for trimming")?;
            (
                serde_json::from_slice(&decoded)
                    .with_context(|| format!("parse decoded request body after {json_err}"))?,
                true,
            )
        }
    };
    trim_request_body(&mut value, &request.remove);
    redact_json_value(&mut value, &sensitive_values);
    let json = serde_json::to_vec(&value).context("serialize trimmed request body")?;
    if compressed {
        zstd::encode_all(std::io::Cursor::new(json), 0)
            .context("compress trimmed request body as zstd")
    } else {
        Ok(json)
    }
}

pub(in crate::observability) async fn transform_response_body(
    path: &Path,
    request: &SaveTestsetRequest,
) -> anyhow::Result<Option<Vec<u8>>> {
    match fs::read(path).await {
        Ok(bytes) => Ok(Some(transform_protocol_payload(&bytes, request)?.0)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("read {}", path.display())),
    }
}

pub(in crate::observability) async fn transform_response_sse(
    path: &Path,
    request: &SaveTestsetRequest,
) -> anyhow::Result<Option<Vec<u8>>> {
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    let sensitive_values = request.sensitive_values();
    if !request.remove.tools {
        return Ok(Some(redact_bytes(&bytes, &sensitive_values)));
    }
    let mut parser = SseParser::default();
    let mut out = Vec::with_capacity(bytes.len());
    for event in parser.push(&bytes) {
        let data = event.data.join("\n");
        let Ok(mut value) = serde_json::from_str::<serde_json::Value>(&data) else {
            out.extend_from_slice(&redact_bytes(&event.raw, &sensitive_values));
            continue;
        };
        if is_tool_event_value(&value) {
            continue;
        }
        if remove_tool_artifacts(&mut value) {
            redact_json_value(&mut value, &sensitive_values);
            let data = serde_json::to_string(&value)?;
            write_sse_event(&mut out, &event, &data);
        } else {
            out.extend_from_slice(&redact_bytes(&event.raw, &sensitive_values));
        }
    }
    Ok(Some(out))
}

pub(in crate::observability) fn is_tool_sse_event(data: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(data).is_ok_and(|value| is_tool_event_value(&value))
}

pub(in crate::observability) async fn transform_websocket_frames(
    path: &Path,
    request: &SaveTestsetRequest,
) -> anyhow::Result<Option<Vec<u8>>> {
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    let sensitive_values = request.sensitive_values();
    if sensitive_values.is_empty() && !request.remove.tools {
        return Ok(Some(bytes));
    }
    let mut out = Vec::with_capacity(bytes.len());
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let body = line.strip_suffix(b"\n").unwrap_or(line);
        if body.is_empty() {
            out.extend_from_slice(line);
            continue;
        }
        let mut frame: serde_json::Value = serde_json::from_slice(body)
            .with_context(|| format!("parse WebSocket frame in {}", path.display()))?;
        if let Some(payload) = frame
            .get("payload_base64")
            .and_then(|value| value.as_str())
            .and_then(|payload| BASE64.decode(payload).ok())
        {
            let (transformed, is_tool_event) = transform_protocol_payload(&payload, request)?;
            if is_tool_event {
                continue;
            }
            if let Some(field) = frame.get_mut("payload_base64") {
                *field = serde_json::Value::String(BASE64.encode(&transformed));
            }
            if frame.get("text").is_some_and(serde_json::Value::is_string) {
                frame["text"] = serde_json::Value::String(
                    String::from_utf8(transformed).context("transform WebSocket text payload")?,
                );
            }
        } else if let Some(text) = frame.get("text").and_then(serde_json::Value::as_str) {
            let (transformed, is_tool_event) =
                transform_protocol_payload(text.as_bytes(), request)?;
            if is_tool_event {
                continue;
            }
            frame["text"] = serde_json::Value::String(
                String::from_utf8(transformed).context("transform WebSocket text payload")?,
            );
        }
        redact_json_value(&mut frame, &sensitive_values);
        serde_json::to_writer(&mut out, &frame)?;
        out.push(b'\n');
    }
    Ok(Some(out))
}

pub(in crate::observability) fn transform_protocol_payload(
    bytes: &[u8],
    request: &SaveTestsetRequest,
) -> anyhow::Result<(Vec<u8>, bool)> {
    let sensitive_values = request.sensitive_values();
    let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return Ok((redact_bytes(bytes, &sensitive_values), false));
    };
    if request.remove.tools && is_tool_event_value(&value) {
        return Ok((Vec::new(), true));
    }
    if request.remove.tools && remove_tool_artifacts(&mut value) {
        redact_json_value(&mut value, &sensitive_values);
        return Ok((serde_json::to_vec(&value)?, false));
    }
    Ok((redact_bytes(bytes, &sensitive_values), false))
}

pub(in crate::observability) fn write_sse_event(
    out: &mut Vec<u8>,
    event: &ParsedSseEventWithRaw,
    data: &str,
) {
    if let Some(event_type) = event.event.as_deref() {
        out.extend_from_slice(b"event: ");
        out.extend_from_slice(event_type.as_bytes());
        out.push(b'\n');
    }
    if let Some(id) = event.id.as_deref() {
        out.extend_from_slice(b"id: ");
        out.extend_from_slice(id.as_bytes());
        out.push(b'\n');
    }
    if let Some(retry) = event.retry.as_deref() {
        out.extend_from_slice(b"retry: ");
        out.extend_from_slice(retry.as_bytes());
        out.push(b'\n');
    }
    for line in data.lines() {
        out.extend_from_slice(b"data: ");
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
    }
    out.push(b'\n');
}

pub(in crate::observability) fn is_tool_artifact_type(item_type: &str) -> bool {
    item_type == "additional_tools"
        || item_type == "mcp_list_tools"
        || item_type.ends_with("_call")
        || item_type.ends_with("_call_output")
        || item_type.ends_with("_approval_request")
        || item_type.ends_with("_approval_response")
}

pub(in crate::observability) fn is_tool_artifact_value(value: &serde_json::Value) -> bool {
    value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .is_some_and(is_tool_artifact_type)
}

pub(in crate::observability) fn is_tool_event_value(value: &serde_json::Value) -> bool {
    let event_type = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    is_tool_artifact_type(event_type)
        || event_type.contains("_call")
        || value.get("item").is_some_and(is_tool_artifact_value)
}

pub(in crate::observability) fn remove_tool_artifacts(value: &mut serde_json::Value) -> bool {
    match value {
        serde_json::Value::Array(values) => {
            let original_len = values.len();
            values.retain(|value| !is_tool_artifact_value(value));
            let mut changed = values.len() != original_len;
            for value in values {
                changed |= remove_tool_artifacts(value);
            }
            changed
        }
        serde_json::Value::Object(values) => {
            let mut changed = false;
            for key in ["tools", "tool_choice", "parallel_tool_calls"] {
                changed |= values.remove(key).is_some();
            }
            let keys = values.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                let remove = values.get(&key).is_some_and(is_tool_artifact_value);
                if remove {
                    values.remove(&key);
                    changed = true;
                } else if let Some(value) = values.get_mut(&key) {
                    changed |= remove_tool_artifacts(value);
                }
            }
            changed
        }
        _ => false,
    }
}

pub(in crate::observability) fn is_tool_websocket_frame(frame: &ObservedWebSocketFrame) -> bool {
    let payload = frame
        .payload_base64
        .as_deref()
        .and_then(|payload| BASE64.decode(payload).ok())
        .or_else(|| frame.text.as_deref().map(|text| text.as_bytes().to_vec()));
    payload.as_deref().is_some_and(|payload| {
        serde_json::from_slice::<serde_json::Value>(payload)
            .is_ok_and(|value| is_tool_event_value(&value))
    })
}

pub(in crate::observability) fn websocket_frame_counts(bytes: &[u8]) -> (usize, usize) {
    let mut client_to_upstream = 0;
    let mut upstream_to_client = 0;
    for line in bytes.split(|byte| *byte == b'\n') {
        let direction = serde_json::from_slice::<serde_json::Value>(line)
            .ok()
            .and_then(|frame| frame.get("direction")?.as_str().map(ToOwned::to_owned));
        match direction.as_deref() {
            Some("client_to_upstream") => client_to_upstream += 1,
            Some("upstream_to_client") => upstream_to_client += 1,
            _ => {}
        }
    }
    (client_to_upstream, upstream_to_client)
}

pub(in crate::observability) fn redact_bytes(bytes: &[u8], sensitive_values: &[&str]) -> Vec<u8> {
    sensitive_values
        .iter()
        .fold(bytes.to_vec(), |bytes, value| {
            replace_bytes(
                &bytes,
                value.as_bytes(),
                REDACTED_TESTSET_HEADER_VALUE.as_bytes(),
            )
        })
}

pub(in crate::observability) fn replace_bytes(
    bytes: &[u8],
    needle: &[u8],
    replacement: &[u8],
) -> Vec<u8> {
    if needle.is_empty() {
        return bytes.to_vec();
    }
    let mut out = Vec::with_capacity(bytes.len());
    let mut offset = 0;
    while let Some(relative) = bytes[offset..]
        .windows(needle.len())
        .position(|window| window == needle)
    {
        let found = offset + relative;
        out.extend_from_slice(&bytes[offset..found]);
        out.extend_from_slice(replacement);
        offset = found + needle.len();
    }
    out.extend_from_slice(&bytes[offset..]);
    out
}
