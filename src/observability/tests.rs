use super::*;

fn text_header(name: &str, value: &str) -> HeaderRecord {
    HeaderRecord {
        name: name.to_owned(),
        value: HeaderValueRecord::Text {
            value: value.to_owned(),
        },
    }
}

fn assert_text_value(record: &HeaderRecord, expected: &str) {
    assert!(matches!(
        &record.value,
        HeaderValueRecord::Text { value } if value == expected
    ));
}

fn assert_redacted_value(record: &HeaderRecord) {
    let value = match &record.value {
        HeaderValueRecord::Text { value } | HeaderValueRecord::BinaryBase64 { value } => value,
    };
    assert_eq!(value, REDACTED_TESTSET_HEADER_VALUE);
}

#[test]
fn redacts_unknown_request_headers_and_preserves_allowlisted_values() {
    let mut records = REQUEST_TESTSET_HEADER_ALLOWLIST
        .iter()
        .map(|name| text_header(name, &format!("original-{name}")))
        .collect::<Vec<_>>();
    records.push(text_header("Authorization", "Bearer secret"));
    records.push(HeaderRecord {
        name: "x-binary-secret".to_owned(),
        value: HeaderValueRecord::BinaryBase64 {
            value: "c2VjcmV0".to_owned(),
        },
    });
    redact_testset_header_records(&mut records, REQUEST_TESTSET_HEADER_ALLOWLIST);

    for (record, name) in records.iter().zip(REQUEST_TESTSET_HEADER_ALLOWLIST) {
        assert_eq!(record.name, *name);
        assert_text_value(record, &format!("original-{name}"));
    }
    assert_eq!(records[10].name, "Authorization");
    assert_redacted_value(&records[10]);
    assert!(matches!(
        &records[11].value,
        HeaderValueRecord::BinaryBase64 { value }
            if value == REDACTED_TESTSET_HEADER_VALUE
    ));
}

#[test]
fn redacts_unknown_response_headers_and_preserves_allowlisted_values() {
    let mut records = RESPONSE_TESTSET_HEADER_ALLOWLIST
        .iter()
        .map(|name| text_header(name, &format!("original-{name}")))
        .collect::<Vec<_>>();
    records.push(text_header("set-cookie", "session=secret"));

    redact_testset_header_records(&mut records, RESPONSE_TESTSET_HEADER_ALLOWLIST);

    for (record, name) in records.iter().zip(RESPONSE_TESTSET_HEADER_ALLOWLIST) {
        assert_eq!(record.name, *name);
        assert_text_value(record, &format!("original-{name}"));
    }
    assert_redacted_value(&records[13]);
}

#[tokio::test]
async fn testset_copy_redacts_headers_without_changing_source_or_other_files() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("recording");
    let destination = temp.path().join("testset");
    let request_dir = source.join("requests/000000");
    fs::create_dir_all(&request_dir).await.unwrap();
    let request_headers = vec![
        text_header("SESSION-ID", "session-original"),
        text_header("authorization", "Bearer secret"),
    ];
    let response_headers = vec![
        text_header("x-models-etag", "etag-original"),
        text_header("set-cookie", "session=secret"),
    ];
    write_json_pretty(request_dir.join("request_headers.json"), &request_headers)
        .await
        .unwrap();
    write_json_pretty(request_dir.join("response_headers.json"), &response_headers)
        .await
        .unwrap();
    let body = b"body copied byte for byte\n";
    fs::write(request_dir.join("request_body.raw"), body)
        .await
        .unwrap();
    fs::write(request_dir.join("request_match.json"), b"derived matcher")
        .await
        .unwrap();
    fs::write(
        request_dir.join("response_rewrite.json"),
        b"derived rewrite",
    )
    .await
    .unwrap();

    export_testset_request(
        &request_dir,
        &destination.join("requests/000000"),
        &TestsetExportRequest {
            source_index: "000000".to_owned(),
            export_index: "000000".to_owned(),
            request_body: serde_json::Value::Null,
        },
        &SaveTestsetRequest::default(),
    )
    .await
    .unwrap();

    let source_request = read_json::<Vec<HeaderRecord>>(&request_dir.join("request_headers.json"))
        .await
        .unwrap();
    assert_text_value(&source_request[0], "session-original");
    assert_text_value(&source_request[1], "Bearer secret");

    let copied_request =
        read_json::<Vec<HeaderRecord>>(&destination.join("requests/000000/request_headers.json"))
            .await
            .unwrap();
    assert_eq!(copied_request[0].name, "SESSION-ID");
    assert_text_value(&copied_request[0], "session-original");
    assert_eq!(copied_request[1].name, "authorization");
    assert_redacted_value(&copied_request[1]);

    let copied_response =
        read_json::<Vec<HeaderRecord>>(&destination.join("requests/000000/response_headers.json"))
            .await
            .unwrap();
    assert_text_value(&copied_response[0], "etag-original");
    assert_redacted_value(&copied_response[1]);
    assert_eq!(
        fs::read(destination.join("requests/000000/request_body.raw"))
            .await
            .unwrap(),
        body
    );
    assert!(!destination
        .join("requests/000000/request_match.json")
        .exists());
    assert!(!destination
        .join("requests/000000/response_rewrite.json")
        .exists());
    assert!(request_dir.join("request_match.json").exists());
    assert!(request_dir.join("response_rewrite.json").exists());
}

#[test]
fn parses_custom_tool_call_from_sse() {
    let bytes = br#"data: {"type":"response.custom_tool_call_input.done","item_id":"ctc_1","input":"{\"cmd\":\"pwd\"}"}

data: {"type":"response.output_item.done","item":{"type":"custom_tool_call","id":"ctc_1","call_id":"call_1","name":"exec","status":"completed"}}

"#;

    let parsed = parse_response_sse(bytes);

    assert_eq!(parsed.function_calls.len(), 1);
    let call = &parsed.function_calls[0];
    assert_eq!(call.id, "ctc_1");
    assert_eq!(call.call_id, "call_1");
    assert_eq!(call.name, "exec");
    assert_eq!(call.status, "completed");
    assert_eq!(call.arguments, "{\n  \"cmd\": \"pwd\"\n}");
}

#[test]
fn preserves_order_and_raw_content_for_every_sse_event() {
    let bytes = b"event: response.created\r\nid: first\r\ndata: {\"type\":\"response.created\",\"secret\":\"value\"}\r\n\r\nevent: note\ndata: plain text\n\n";

    let parsed = parse_response_sse(bytes);

    assert_eq!(parsed.events.len(), 2);
    assert_eq!(parsed.events[0].index, 0);
    assert_eq!(parsed.events[0].event_type, "response.created");
    assert_eq!(parsed.events[0].id.as_deref(), Some("first"));
    assert_eq!(
            parsed.events[0].raw,
            "event: response.created\r\nid: first\r\ndata: {\"type\":\"response.created\",\"secret\":\"value\"}\r\n\r\n"
        );
    assert_eq!(parsed.events[1].index, 1);
    assert_eq!(parsed.events[1].event_type, "note");
    assert_eq!(parsed.events[1].data, "plain text");
    assert_eq!(parsed.event_counts["response.created"], 1);
    assert_eq!(parsed.event_counts["note"], 1);
}

#[test]
fn parses_claude_messages_prompt_and_tool_history() {
    let body = serde_json::json!({
        "system": [
            {"type": "text", "text": "You are Claude Code."},
            {"type": "text", "text": "Follow the project instructions."}
        ],
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "<system-reminder>derived context</system-reminder>"},
                {"type": "text", "text": "ping"}
            ]},
            {"role": "assistant", "content": [
                {"type": "tool_use", "id": "toolu_1", "name": "Read", "input": {"file_path": "/tmp/a"}}
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_1", "content": "contents"}
            ]}
        ]
    });

    let blocks = prompt_blocks(&body);
    assert_eq!(blocks.len(), 4);
    assert_eq!(blocks[0].role, "system");
    assert_eq!(blocks[2].block_type, "system_reminder");
    assert_eq!(visible_user_messages(&blocks), vec!["ping"]);

    let calls = previous_function_calls(&body);
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].call_id, "toolu_1");
    assert_eq!(calls[0].name, "Read");
    assert!(calls[0].arguments.contains("file_path"));

    let outputs = previous_tool_outputs(&body);
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].call_id, "toolu_1");
    assert_eq!(outputs[0].output, "contents");
}

#[test]
fn provider_dispatch_keeps_claude_derivation_out_of_codex() {
    let title = serde_json::json!({
        "system": [
            {"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude."},
            {
                "type": "text",
                "text": "Generate a concise, sentence-case title (3-7 words). Return JSON with a single \"title\" field."
            }
        ],
        "messages": [{
            "role": "user",
            "content": [{"type": "text", "text": "<session>\nlist files\n</session>"}]
        }]
    });
    let conversation = serde_json::json!({
        "messages": [{"role": "user", "content": "ping"}]
    });

    assert_eq!(
        ObservabilityProvider::from_profile("claude"),
        ObservabilityProvider::Claude
    );
    assert_eq!(
        ObservabilityProvider::from_profile("codex-http"),
        ObservabilityProvider::Codex
    );
    assert_eq!(
        ObservabilityProvider::from_profile("codex-websocket"),
        ObservabilityProvider::Codex
    );
    assert_eq!(
        ObservabilityProvider::Claude.classify_request_kind(&title),
        ObservedRequestKind::SessionTitle
    );
    assert_eq!(
        ObservabilityProvider::Codex.classify_request_kind(&title),
        ObservedRequestKind::Conversation
    );
    assert_eq!(
        ObservabilityProvider::Claude.classify_request_kind(&conversation),
        ObservedRequestKind::Conversation
    );

    let mut inputs = Vec::new();
    append_conversation_user_inputs(
        &mut inputs,
        ObservabilityProvider::Claude,
        ObservabilityProvider::Claude.classify_request_kind(&title),
        &title,
    );
    append_conversation_user_inputs(
        &mut inputs,
        ObservabilityProvider::Claude,
        ObservabilityProvider::Claude.classify_request_kind(&conversation),
        &conversation,
    );
    assert_eq!(inputs, vec!["ping"]);
}

#[test]
fn parses_claude_messages_sse_text_tools_and_usage() {
    let bytes = br#"event: message_start
data: {"type":"message_start","message":{"id":"msg_1","model":"claude-test","usage":{"input_tokens":10,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: content_block_start
data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"Read","input":{}}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"file_path\":\"/tmp/a\"}"}}

event: content_block_stop
data: {"type":"content_block_stop","index":1}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":5}}

event: message_stop
data: {"type":"message_stop"}

"#;

    let parsed = parse_response_sse(bytes);

    assert_eq!(parsed.output_text, "hello");
    assert_eq!(parsed.completed_response["id"], "msg_1");
    assert_eq!(parsed.completed_response["model"], "claude-test");
    assert_eq!(parsed.completed_response["stop_reason"], "tool_use");
    assert_eq!(parsed.completed_response["usage"]["input_tokens"], 10);
    assert_eq!(parsed.completed_response["usage"]["output_tokens"], 5);
    assert_eq!(
        observed_usage(&parsed.completed_response).unwrap()["total_tokens"],
        15
    );
    assert_eq!(parsed.function_calls.len(), 1);
    assert_eq!(parsed.function_calls[0].call_id, "toolu_1");
    assert_eq!(parsed.function_calls[0].name, "Read");
    assert!(parsed.function_calls[0].arguments.contains("file_path"));
    assert_eq!(parsed.event_counts["content_block_delta"], 2);
}

#[tokio::test]
async fn decodes_gzip_response_only_for_observability() {
    let temp = tempfile::tempdir().unwrap();
    let request_dir = temp.path();
    write_json_pretty(
        request_dir.join("response_headers.json"),
        &vec![text_header("content-encoding", "gzip")],
    )
    .await
    .unwrap();
    let original = b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut encoder, original).unwrap();
    let encoded = encoder.finish().unwrap();

    let decoded = decode_response_body_for_observability(request_dir, &encoded)
        .await
        .unwrap();

    assert_eq!(decoded, original);
    assert_ne!(encoded, original);
}

#[tokio::test]
async fn ignores_false_incomplete_marker_when_content_length_was_fully_recorded() {
    let temp = tempfile::tempdir().unwrap();
    write_json_pretty(
        temp.path().join("request_headers.json"),
        &vec![text_header("content-length", "4")],
    )
    .await
    .unwrap();
    write_json_pretty(
        temp.path().join("recording_incomplete.json"),
        &serde_json::json!({
            "incomplete": true,
            "stage": "http_request_body_stream",
            "error": "upstream stopped consuming the request body before it completed"
        }),
    )
    .await
    .unwrap();

    assert!(load_recording_incomplete(
        &temp.path().join("recording_incomplete.json"),
        temp.path(),
        4,
    )
    .await
    .is_none());
    assert!(load_recording_incomplete(
        &temp.path().join("recording_incomplete.json"),
        temp.path(),
        3,
    )
    .await
    .is_some());
}

#[tokio::test]
async fn loads_websocket_frames_in_recorded_order_and_reports_partial_lines() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("websocket_frames.jsonl");
    fs::write(
            &path,
            concat!(
                "{\"index\":1,\"direction\":\"upstream_to_client\",\"timestamp\":\"2030-01-01T00:00:00.002Z\",\"opcode\":\"text\",\"text\":\"reply\",\"payload_base64\":\"cmVwbHk=\",\"close\":null}\n",
                "{\"index\":0,\"direction\":\"client_to_upstream\",\"timestamp\":\"2030-01-01T00:00:00.001Z\",\"opcode\":\"text\",\"text\":\"hello\",\"payload_base64\":\"aGVsbG8=\",\"close\":null}\n",
                "{\"index\":"
            ),
        )
        .await
        .unwrap();

    let (frames, warning) = load_websocket_frames(&path).await.unwrap();

    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].index, 0);
    assert_eq!(frames[0].direction, "client_to_upstream");
    assert_eq!(frames[0].text.as_deref(), Some("hello"));
    assert_eq!(frames[1].index, 1);
    assert!(warning.unwrap().contains("1 WebSocket frame"));
}

#[test]
fn trims_tools_and_derived_capability_blocks_from_request_copy() {
    let mut body = serde_json::json!({
        "model": "gpt-test",
        "tools": [{"type": "function", "name": "exec"}],
        "tool_choice": "auto",
        "input": [
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "keep secret-value"}]},
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "<skills_instructions>remove</skills_instructions>"}]},
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "<apps_instructions>remove</apps_instructions>"}]},
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "<recommended_plugins>remove</recommended_plugins>"}]},
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "<environment_context>remove</environment_context>"}]},
            {"type": "additional_tools", "tools": [{"type": "custom", "name": "wait"}]},
            {"type": "custom_tool_call", "call_id": "call-1", "name": "exec", "input": "pwd"},
            {"type": "custom_tool_call_output", "call_id": "call-1", "output": "secret-value"}
        ]
    });
    let remove = TestsetRemovalOptions {
        tools: true,
        skills: true,
        apps: true,
        plugins: true,
        derived_prompt: true,
    };

    trim_request_body(&mut body, &remove);
    redact_json_value(&mut body, &["secret-value"]);

    assert!(body.get("tools").is_none());
    assert!(body.get("tool_choice").is_none());
    assert_eq!(body["input"].as_array().unwrap().len(), 1);
    assert_eq!(
        body.pointer("/input/0/content/0/text")
            .and_then(serde_json::Value::as_str),
        Some("keep ******")
    );
    assert!(tool_definitions(&body).is_empty());
}

#[test]
fn trims_only_the_derived_fragment_when_user_text_shares_a_content_part() {
    let mut body = serde_json::json!({
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "<recommended_plugins>generated list</recommended_plugins>\nKeep this user request"
            }]
        }]
    });
    let remove = TestsetRemovalOptions {
        plugins: true,
        ..TestsetRemovalOptions::default()
    };

    trim_request_body(&mut body, &remove);

    let text = body
        .pointer("/input/0/content/0/text")
        .and_then(serde_json::Value::as_str)
        .unwrap();
    assert_eq!(text.trim(), "Keep this user request");
    assert_eq!(
        visible_user_messages(&prompt_blocks(&body)),
        vec!["Keep this user request"]
    );
}

#[tokio::test]
async fn tool_trimming_removes_nested_calls_from_sse_and_buffered_json() {
    let temp = tempfile::tempdir().unwrap();
    let sse_path = temp.path().join("response_sse.raw");
    let response_path = temp.path().join("response_body.raw");
    let response = serde_json::json!({
        "type": "response",
        "output": [
            {"type":"message","content":[{"type":"output_text","text":"keep"}]},
            {"type":"function_call","call_id":"call-1","name":"exec","arguments":"secret"}
        ]
    });
    let sse = format!(
            "data: {{\"type\":\"response.function_call_arguments.done\",\"arguments\":\"secret\"}}\n\ndata: {}\n\n",
            serde_json::json!({"type":"response.completed","response":response})
        );
    fs::write(&sse_path, &sse).await.unwrap();
    fs::write(&response_path, serde_json::to_vec(&response).unwrap())
        .await
        .unwrap();
    let request = SaveTestsetRequest {
        remove: TestsetRemovalOptions {
            tools: true,
            ..TestsetRemovalOptions::default()
        },
        ..SaveTestsetRequest::default()
    };

    let transformed_sse = transform_response_sse(&sse_path, &request)
        .await
        .unwrap()
        .unwrap();
    let transformed_body = transform_response_body(&response_path, &request)
        .await
        .unwrap()
        .unwrap();

    let sse_text = String::from_utf8(transformed_sse).unwrap();
    assert!(!sse_text.contains("function_call"));
    assert!(!sse_text.contains("secret"));
    assert!(sse_text.contains("response.completed"));
    assert!(sse_text.contains("keep"));
    let body: serde_json::Value = serde_json::from_slice(&transformed_body).unwrap();
    assert_eq!(body["output"].as_array().unwrap().len(), 1);
    assert_eq!(
        body.pointer("/output/0/type"),
        Some(&serde_json::json!("message"))
    );
}

#[tokio::test]
async fn tool_trimming_filters_websocket_events_and_updates_payloads() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("websocket_frames.jsonl");
    let tool_event = serde_json::json!({
        "type": "response.function_call_arguments.done",
        "arguments": "secret"
    })
    .to_string();
    let completed_event = serde_json::json!({
        "type": "response.completed",
        "response": {
            "output": [
                {"type":"message","content":[{"type":"output_text","text":"keep"}]},
                {"type":"custom_tool_call","call_id":"call-1","name":"exec","input":"secret"}
            ]
        }
    })
    .to_string();
    let frames = [tool_event, completed_event]
        .into_iter()
        .enumerate()
        .map(|(index, text)| {
            serde_json::json!({
                "index": index,
                "direction": "upstream_to_client",
                "timestamp": format!("2030-01-01T00:00:00.00{index}Z"),
                "opcode": "text",
                "text": text,
                "payload_base64": BASE64.encode(text.as_bytes()),
                "close": null
            })
            .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&path, format!("{frames}\n")).await.unwrap();
    let request = SaveTestsetRequest {
        remove: TestsetRemovalOptions {
            tools: true,
            ..TestsetRemovalOptions::default()
        },
        ..SaveTestsetRequest::default()
    };

    let transformed = transform_websocket_frames(&path, &request)
        .await
        .unwrap()
        .unwrap();

    let lines = transformed
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(lines.len(), 1);
    let frame: serde_json::Value = serde_json::from_slice(lines[0]).unwrap();
    let text = frame["text"].as_str().unwrap();
    assert!(text.contains("response.completed"));
    assert!(text.contains("keep"));
    assert!(!text.contains("custom_tool_call"));
    assert!(!text.contains("secret"));
    assert_eq!(websocket_frame_counts(&transformed), (0, 1));
}

#[tokio::test]
async fn trimmed_export_selects_and_reindexes_without_mutating_recording() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("recording");
    let destination = temp.path().join("testset");
    let selected_source = source.join("requests/000004");
    let omitted_source = source.join("requests/000005");
    fs::create_dir_all(&selected_source).await.unwrap();
    fs::create_dir_all(&omitted_source).await.unwrap();
    write_json_pretty(
        source.join("manifest.json"),
        &serde_json::json!({"session_id":"session-1","request_count":2}),
    )
    .await
    .unwrap();
    let body = serde_json::to_vec(&serde_json::json!({
            "model": "gpt-test",
            "tools": [{"type":"function","name":"exec"}],
            "input": [
                {"type":"message","role":"user","content":[{"type":"input_text","text":"keep secret-value"}]},
                {"type":"message","role":"user","content":[{"type":"input_text","text":"<skills_instructions>remove</skills_instructions>"}]}
            ]
        }))
        .unwrap();
    fs::write(selected_source.join("request_body.raw"), &body)
        .await
        .unwrap();
    fs::write(omitted_source.join("request_body.raw"), b"{}")
        .await
        .unwrap();
    write_json_pretty(
            selected_source.join("request_meta.json"),
            &serde_json::json!({"index":4,"request_body_bytes":body.len(),"upstream_url":"https://example.invalid"}),
        )
        .await
        .unwrap();
    write_json_pretty(
        selected_source.join("request_headers.json"),
        &vec![
            text_header("content-length", &body.len().to_string()),
            text_header("authorization", "Bearer secret-value"),
        ],
    )
    .await
    .unwrap();
    write_json_pretty(
        selected_source.join("response_headers.json"),
        &vec![text_header("set-cookie", "secret-value")],
    )
    .await
    .unwrap();
    let response = concat!(
            "data: {\"type\":\"response.custom_tool_call_input.done\",\"item_id\":\"tool-1\",\"input\":\"secret-value\"}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"secret-value\"}\n\n"
        )
        .as_bytes();
    fs::write(selected_source.join("response_sse.raw"), response)
        .await
        .unwrap();
    write_json_pretty(
        selected_source.join("response_meta.json"),
        &serde_json::json!({"status":200,"response_body_bytes":response.len(),"sse_event_count":1}),
    )
    .await
    .unwrap();
    let source_body_before = fs::read(selected_source.join("request_body.raw"))
        .await
        .unwrap();
    let request = SaveTestsetRequest {
        selected_requests: Some(vec!["000004".to_owned()]),
        sensitive_values: vec!["secret-value".to_owned()],
        remove: TestsetRemovalOptions {
            tools: true,
            skills: true,
            ..TestsetRemovalOptions::default()
        },
        ..SaveTestsetRequest::default()
    };
    let mut preview_body = serde_json::from_slice(&body).unwrap();
    trim_request_body(&mut preview_body, &request.remove);
    redact_json_value(&mut preview_body, &request.sensitive_values());
    let plan = TestsetExportPlan {
        requests: vec![TestsetExportRequest {
            source_index: "000004".to_owned(),
            export_index: "000000".to_owned(),
            request_body: preview_body,
        }],
        first_user_input: "keep ******".to_owned(),
        user_inputs: vec!["keep ******".to_owned()],
    };

    export_testset_session(&source, &destination, &plan, &request)
        .await
        .unwrap();

    assert_eq!(
        fs::read(selected_source.join("request_body.raw"))
            .await
            .unwrap(),
        source_body_before
    );
    assert!(!destination.join("requests/000004").exists());
    assert!(!destination.join("requests/000001").exists());
    let exported_dir = destination.join("requests/000000");
    let exported_body: serde_json::Value = serde_json::from_slice(
        &fs::read(exported_dir.join("request_body.raw"))
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(exported_body.get("tools").is_none());
    assert_eq!(exported_body["input"].as_array().unwrap().len(), 1);
    assert_eq!(
        exported_body.pointer("/input/0/content/0/text"),
        Some(&serde_json::json!("keep ******"))
    );
    let exported_meta: serde_json::Value = read_json(&exported_dir.join("request_meta.json"))
        .await
        .unwrap();
    assert_eq!(exported_meta["index"], 0);
    let exported_sse = fs::read_to_string(exported_dir.join("response_sse.raw"))
        .await
        .unwrap();
    assert!(exported_sse.contains("******"));
    assert!(!exported_sse.contains("secret-value"));
    assert!(!exported_sse.contains("custom_tool_call"));
    assert!(exported_sse.contains("response.output_text.delta"));
    let exported_manifest: serde_json::Value =
        read_json(&destination.join("manifest.json")).await.unwrap();
    assert_eq!(exported_manifest["request_count"], 1);
}

#[test]
fn reads_additional_tool_definitions() {
    let body = serde_json::json!({
        "input": [{
            "type": "additional_tools",
            "role": "developer",
            "tools": [
                {"type": "custom", "name": "exec", "description": "Run code"},
                {"type": "function", "name": "wait", "description": "Wait"}
            ]
        }]
    });

    let definitions = tool_definitions(&body);

    assert_eq!(definitions.len(), 2);
    assert_eq!(definitions[0].name, "exec");
    assert_eq!(definitions[0].tool_type, "custom");
    assert_eq!(definitions[1].name, "wait");
}

#[test]
fn reads_custom_tool_call_history() {
    let body = serde_json::json!({
        "input": [
            {
                "type": "custom_tool_call",
                "call_id": "call_1",
                "name": "exec",
                "status": "completed",
                "input": "ls -la"
            },
            {
                "type": "custom_tool_call_output",
                "call_id": "call_1",
                "output": [
                    {"type": "input_text", "text": "process exited 0"},
                    {"type": "input_text", "text": "5050"}
                ]
            },
            {
                "type": "function_call_output",
                "call_id": "call_2",
                "output": "legacy output"
            }
        ]
    });

    let calls = previous_function_calls(&body);
    let outputs = previous_tool_outputs(&body);

    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].name, "exec");
    assert_eq!(calls[0].arguments, "ls -la");
    assert_eq!(outputs.len(), 2);
    assert_eq!(outputs[0].call_id, "call_1");
    assert_eq!(outputs[0].output, "process exited 0\n5050");
    assert_eq!(outputs[1].call_id, "call_2");
    assert_eq!(outputs[1].output, "legacy output");
}

#[tokio::test]
async fn empty_response_metadata_marks_call_incomplete_without_error() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("response_meta.json");
    std::fs::write(&path, b"").unwrap();

    let (meta, state, warning) = load_response_meta(&path).await;

    assert!(meta.is_none());
    assert_eq!(state, "incomplete");
    assert!(warning.unwrap().contains("empty"));
}

#[tokio::test]
async fn invalid_response_metadata_marks_call_incomplete_without_error() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("response_meta.json");
    std::fs::write(&path, b"{").unwrap();

    let (meta, state, warning) = load_response_meta(&path).await;

    assert!(meta.is_none());
    assert_eq!(state, "incomplete");
    assert!(warning.unwrap().contains("invalid"));
}

#[tokio::test]
async fn recording_failure_marker_exposes_incomplete_stage() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("recording_incomplete.json");
    std::fs::write(
        &path,
        br#"{"incomplete":true,"stage":"http_response_body_write","error":"disk full"}"#,
    )
    .unwrap();

    let warning = load_recording_incomplete(&path, temp.path(), 0)
        .await
        .unwrap();

    assert!(warning.contains("http_response_body_write"));
    assert!(warning.contains("disk full"));
}

fn summary_test_call(index: &str) -> ObservedCall {
    ObservedCall {
        index: index.to_owned(),
        request_id: format!("request-{index}"),
        started_at: "2026-01-01T00:00:00Z".to_owned(),
        completed_at: "2026-01-01T00:00:01Z".to_owned(),
        duration_ms: Some(1000),
        method: "POST".to_owned(),
        path: "/v1/messages".to_owned(),
        status: 200,
        protocol: "http".to_owned(),
        request_kind: ObservedRequestKind::Conversation,
        recording_state: "complete".to_owned(),
        recording_warning: None,
        model: "model-x".to_owned(),
        stream: true,
        input_count: 3,
        tools_count: 2,
        tool_names: vec!["Read".to_owned(), "Edit".to_owned()],
        tool_definitions: Vec::new(),
        prompt_blocks: vec![PromptBlock {
            role: "user".to_owned(),
            block_type: "text".to_owned(),
            chars: 5,
            excerpt: "hello".to_owned(),
            text: "hello".to_owned(),
        }],
        visible_user_messages: vec!["hello".to_owned()],
        previous_tool_outputs: vec![ToolOutput {
            call_id: "tool_1".to_owned(),
            output: "file contents".to_owned(),
        }],
        previous_function_calls: Vec::new(),
        previous_assistant_messages: Vec::new(),
        function_calls: vec![ObservedFunctionCall {
            id: "call_1".to_owned(),
            call_id: "tool_1".to_owned(),
            name: "Read".to_owned(),
            status: "completed".to_owned(),
            arguments: "{}".to_owned(),
            result: Some("file contents".to_owned()),
        }],
        output_text: "done".to_owned(),
        response_body: None,
        usage: Some(serde_json::json!({ "total_tokens": 10 })),
        event_counts: BTreeMap::from([("message_start".to_owned(), 1)]),
        sse_events: vec![ObservedSseEvent {
            index: 0,
            event: None,
            id: None,
            retry: None,
            event_type: "message_start".to_owned(),
            data: "{}".to_owned(),
            raw: "data: {}\n\n".to_owned(),
        }],
        websocket_frames: Vec::new(),
        websocket_meta: None,
        request_meta: serde_json::Value::Null,
        response_meta: None,
        request_body: serde_json::Value::Null,
        timeline: Vec::new(),
        files: vec![ObservedFile {
            name: "response_sse.raw".to_owned(),
            bytes: 9,
        }],
        raw_dir: "/tmp/rec".to_owned(),
        request_body_bytes: 12,
        response_body_bytes: 9,
    }
}

#[test]
fn observed_call_summary_keeps_light_fields_and_counts() {
    let call = summary_test_call("000003");
    let summary = ObservedCallSummary::from(&call);

    assert_eq!(summary.index, "000003");
    assert_eq!(summary.request_id, "request-000003");
    assert_eq!(summary.sse_event_count, 1);
    assert_eq!(summary.function_call_count, 1);
    assert_eq!(summary.prompt_block_count, 1);
    assert_eq!(summary.function_calls[0].call_id, "tool_1");
    assert_eq!(summary.function_calls[0].name, "Read");
    assert!(summary.function_calls[0].has_result);
    assert_eq!(summary.usage.as_ref().unwrap()["total_tokens"], 10);
    assert_eq!(summary.request_body_bytes, 12);
    assert_eq!(summary.files.len(), 1);
}

#[test]
fn overview_keeps_main_turns_out_of_flows_and_preserves_agent_flow_turns() {
    let call = summary_test_call("000000");
    let turn = ObservedTurn {
        id: "turn-000000".to_owned(),
        user: "hello".to_owned(),
        started_at: "2026-01-01T00:00:00Z".to_owned(),
        calls: vec![call],
        assistant: "done".to_owned(),
        tool_outputs: vec![ToolOutput {
            call_id: "tool_1".to_owned(),
            output: "file contents".to_owned(),
        }],
    };
    let main_flow = ObservedFlow {
        id: "main".to_owned(),
        role: ObservedFlowRole::Main,
        kind: ObservedRequestKind::Conversation,
        label: "User conversation".to_owned(),
        started_at: "2026-01-01T00:00:00Z".to_owned(),
        completed_at: "2026-01-01T00:00:01Z".to_owned(),
        request_count: 1,
        relation: None,
        turns: vec![turn.clone()],
    };
    let agent_flow = ObservedFlow {
        id: "agent-000000".to_owned(),
        role: ObservedFlowRole::Agent,
        kind: ObservedRequestKind::SessionTitle,
        label: "Session title".to_owned(),
        started_at: "2026-01-01T00:00:00Z".to_owned(),
        completed_at: "2026-01-01T00:00:01Z".to_owned(),
        request_count: 1,
        relation: None,
        turns: vec![turn],
    };

    let main_summary = ObservedFlowSummary::from(&main_flow);
    let agent_summary = ObservedFlowSummary::from(&agent_flow);
    let turn_summary = ObservedTurnSummary::from(&main_flow.turns[0]);

    assert!(main_summary.turns.is_empty());
    assert_eq!(main_summary.request_count, 1);
    assert_eq!(agent_summary.turns.len(), 1);
    assert_eq!(agent_summary.turns[0].user, "hello");
    assert_eq!(agent_summary.turns[0].calls[0].index, "000000");
    assert_eq!(agent_summary.turns[0].assistant, "done");
    assert_eq!(agent_summary.turns[0].tool_outputs[0].call_id, "tool_1");
    assert_eq!(turn_summary.calls[0].sse_event_count, 1);
}

#[test]
fn request_detail_rejects_non_numeric_indexes() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let state = GatewayState {
        client: reqwest::Client::new(),
        output_root: std::path::PathBuf::from("/nonexistent"),
        testsets_root: std::path::PathBuf::from("/nonexistent"),
        access_log_path: std::path::PathBuf::from("/nonexistent/access.log"),
        profiles: std::sync::Arc::new(HashMap::new()),
        session_header: axum::http::HeaderName::from_static("x-codex-session-id"),
        counters: std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        replay_sessions: std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new())),
    };
    let result = runtime.block_on(request_detail_inner(&state, "codex-http", "session", ".."));
    assert!(result.is_err());
    let result = runtime.block_on(request_detail_inner(&state, "codex-http", "session", "abc"));
    assert!(result.is_err());
    let result = runtime.block_on(request_detail_inner(&state, "codex-http", "session", ""));
    assert!(result.is_err());
}

#[test]
fn relation_from_headers_extracts_codex_guardian_relationship() {
    let headers = vec![
        text_header("thread-id", "child-1"),
        text_header(
            "x-codex-turn-metadata",
            r#"{"session_id":"parent-1","thread_id":"child-1","parent_thread_id":"parent-1","subagent_kind":"guardian","thread_source":"subagent"}"#,
        ),
    ];
    let relation = relation_from_headers(&headers).expect("relation");
    assert_eq!(relation.parent_session_id.as_deref(), Some("parent-1"));
    assert_eq!(relation.subagent_kind.as_deref(), Some("guardian"));
    assert_eq!(relation.thread_source.as_deref(), Some("subagent"));
}

#[test]
fn relation_from_headers_falls_back_to_parent_thread_header() {
    let headers = vec![
        text_header("thread-id", "child-1"),
        text_header("x-codex-parent-thread-id", "parent-1"),
        text_header("x-openai-subagent", "guardian"),
    ];
    let relation = relation_from_headers(&headers).expect("relation");
    assert_eq!(relation.parent_session_id.as_deref(), Some("parent-1"));
    assert_eq!(relation.subagent_kind.as_deref(), Some("guardian"));
    assert_eq!(relation.thread_source, None);
}

#[test]
fn relation_from_headers_returns_none_without_relationship_headers() {
    let parent_metadata = vec![text_header(
        "x-codex-turn-metadata",
        r#"{"session_id":"parent-1","thread_id":"parent-1","thread_source":"user"}"#,
    )];
    assert_eq!(relation_from_headers(&parent_metadata), None);
    assert_eq!(relation_from_headers(&[]), None);
}

#[tokio::test]
async fn session_relations_links_guardian_sessions_to_parent() {
    let temp = tempfile::tempdir().unwrap();
    let recordings = temp.path().join("recordings");
    let parent_dir = recordings.join("parent-1");
    let child_dir = recordings.join("child-1");
    fs::create_dir_all(parent_dir.join("requests/000000"))
        .await
        .unwrap();
    fs::create_dir_all(child_dir.join("requests/000000"))
        .await
        .unwrap();
    write_json_pretty(
        parent_dir.join("requests/000000/request_headers.json"),
        &vec![text_header("thread-id", "parent-1")],
    )
    .await
    .unwrap();
    write_json_pretty(
        child_dir.join("requests/000000/request_headers.json"),
        &vec![
            text_header("thread-id", "child-1"),
            text_header("x-codex-parent-thread-id", "parent-1"),
            text_header("x-openai-subagent", "guardian"),
        ],
    )
    .await
    .unwrap();

    let relations = session_relations(&recordings).await.unwrap();
    let child = relations.get("child-1").expect("child relation");
    assert_eq!(child.parent_session_id.as_deref(), Some("parent-1"));
    assert_eq!(child.subagent_kind.as_deref(), Some("guardian"));
    assert!(!relations.contains_key("parent-1"));
}

#[test]
fn child_session_relation_anchors_to_main_call_in_progress() {
    let call = summary_test_call("000000");
    let turn = ObservedTurn {
        id: "turn-000000".to_owned(),
        user: "hello".to_owned(),
        started_at: "2026-01-01T00:00:00Z".to_owned(),
        calls: vec![call],
        assistant: "done".to_owned(),
        tool_outputs: Vec::new(),
    };
    let relation =
        child_session_relation(&[turn], "2026-01-01T00:00:00.500Z", "2026-01-01T00:00:02Z")
            .expect("relation");
    assert_eq!(relation.main_turn_id, "turn-000000");
    assert_eq!(relation.timing, ObservedFlowTiming::DuringCall);
    assert_eq!(relation.anchor_call_index.as_deref(), Some("000000"));
    assert!(relation.overlaps_main);
}

#[test]
fn child_session_relation_after_main_turn_completes() {
    let call = summary_test_call("000000");
    let turn = ObservedTurn {
        id: "turn-000000".to_owned(),
        user: "hello".to_owned(),
        started_at: "2026-01-01T00:00:00Z".to_owned(),
        calls: vec![call],
        assistant: "done".to_owned(),
        tool_outputs: Vec::new(),
    };
    let relation = child_session_relation(&[turn], "2026-01-01T00:00:10Z", "2026-01-01T00:00:11Z")
        .expect("relation");
    assert_eq!(relation.main_turn_id, "turn-000000");
    assert_eq!(relation.timing, ObservedFlowTiming::AfterTurn);
}

async fn write_session_request(
    request_dir: &std::path::Path,
    session_id: &str,
    index: u64,
    started_at: &str,
    completed_at: &str,
    headers: Vec<HeaderRecord>,
    model: &str,
    user_text: &str,
) {
    fs::create_dir_all(request_dir).await.unwrap();
    let body = serde_json::json!({
        "model": model,
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": user_text}]
        }],
        "stream": true
    });
    let body_bytes = serde_json::to_vec(&body).unwrap();
    write_json_pretty(
        request_dir.join("request_meta.json"),
        &RequestMeta {
            index,
            session_id: session_id.to_owned(),
            session_source: crate::types::SessionSource::Header {
                name: "thread-id".to_owned(),
            },
            started_at: started_at.to_owned(),
            method: "POST".to_owned(),
            path: "/responses".to_owned(),
            query: None,
            upstream_url: "https://apipool.dev/responses".to_owned(),
            request_body_bytes: body_bytes.len(),
        },
    )
    .await
    .unwrap();
    fs::write(request_dir.join("request_body.raw"), body_bytes)
        .await
        .unwrap();
    write_json_pretty(request_dir.join("request_headers.json"), &headers)
        .await
        .unwrap();
    write_json_pretty(
        request_dir.join("response_meta.json"),
        &ResponseMeta {
            status: 200,
            started_at: started_at.to_owned(),
            completed_at: completed_at.to_owned(),
            response_body_bytes: 0,
            sse_event_count: 0,
            upstream_error: None,
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn session_overview_links_guardian_child_sessions() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let recordings = home.join("recordings");
    let parent_dir = recordings.join("parent-1");
    let child_dir = recordings.join("child-1");

    write_json_pretty(
        parent_dir.join("manifest.json"),
        &serde_json::json!({
            "session_id": "parent-1",
            "created_at": "2026-08-08T06:00:00Z",
            "updated_at": "2026-08-08T06:05:00Z",
            "request_count": 2,
            "recorder": {}
        }),
    )
    .await
    .unwrap();
    write_session_request(
        &parent_dir.join("requests/000000"),
        "parent-1",
        0,
        "2026-08-08T06:00:01Z",
        "2026-08-08T06:00:05Z",
        vec![
            text_header("thread-id", "parent-1"),
            text_header(
                "x-codex-turn-metadata",
                r#"{"session_id":"parent-1","thread_id":"parent-1","thread_source":"user"}"#,
            ),
        ],
        "model-x",
        "hello",
    )
    .await;
    write_session_request(
        &parent_dir.join("requests/000001"),
        "parent-1",
        1,
        "2026-08-08T06:04:00Z",
        "2026-08-08T06:04:20Z",
        vec![text_header("thread-id", "parent-1")],
        "model-x",
        "hello",
    )
    .await;

    write_json_pretty(
        child_dir.join("manifest.json"),
        &serde_json::json!({
            "session_id": "child-1",
            "created_at": "2026-08-08T06:04:10Z",
            "updated_at": "2026-08-08T06:04:42Z",
            "request_count": 1,
            "recorder": {}
        }),
    )
    .await
    .unwrap();
    write_session_request(
        &child_dir.join("requests/000000"),
        "child-1",
        0,
        "2026-08-08T06:04:10Z",
        "2026-08-08T06:04:12Z",
        vec![
            text_header("thread-id", "child-1"),
            text_header(
                "x-codex-turn-metadata",
                r#"{"session_id":"parent-1","thread_id":"child-1","parent_thread_id":"parent-1","subagent_kind":"guardian","thread_source":"subagent"}"#,
            ),
        ],
        "codex-auto-review",
        "approval",
    )
    .await;

    let state = GatewayState {
        client: reqwest::Client::new(),
        output_root: temp.path().to_path_buf(),
        testsets_root: temp.path().join("testsets"),
        access_log_path: temp.path().join("access.log"),
        profiles: std::sync::Arc::new(HashMap::from([(
            "codex-apipool-http".to_owned(),
            crate::types::ProfileConfig {
                name: "codex-apipool-http".to_owned(),
                upstream: reqwest::Url::parse("https://apipool.dev/").unwrap(),
                supports_websocket: false,
                home_dir: home,
            },
        )])),
        session_header: axum::http::HeaderName::from_static("x-codex-session-id"),
        counters: std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        replay_sessions: std::sync::Arc::new(tokio::sync::Mutex::new(HashMap::new())),
    };

    let overview = session_overview_inner(&state, "codex-apipool-http", "parent-1")
        .await
        .unwrap();
    assert!(overview.parent_session_id.is_none());
    assert_eq!(overview.child_sessions.len(), 1);
    let child = &overview.child_sessions[0];
    assert_eq!(child.session_id, "child-1");
    assert_eq!(child.subagent_kind.as_deref(), Some("guardian"));
    assert_eq!(child.request_count, 1);
    let relation = child.relation.as_ref().expect("child relation");
    assert_eq!(relation.main_turn_id, "turn-000000");
    assert_eq!(relation.timing, ObservedFlowTiming::DuringCall);
    assert_eq!(relation.anchor_call_index.as_deref(), Some("000001"));
    assert!(relation.overlaps_main);

    let child_overview = session_overview_inner(&state, "codex-apipool-http", "child-1")
        .await
        .unwrap();
    assert_eq!(
        child_overview.parent_session_id.as_deref(),
        Some("parent-1")
    );
    assert_eq!(child_overview.subagent_kind.as_deref(), Some("guardian"));
    assert!(child_overview.child_sessions.is_empty());

    let sessions = sessions_inner(&state, "codex-apipool-http").await.unwrap();
    assert_eq!(sessions.len(), 2);
    let child_summary = sessions
        .iter()
        .find(|session| session.session_id == "child-1")
        .expect("child summary");
    assert_eq!(child_summary.parent_session_id.as_deref(), Some("parent-1"));
    assert_eq!(child_summary.subagent_kind.as_deref(), Some("guardian"));
}
