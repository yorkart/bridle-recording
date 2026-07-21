use std::{collections::HashMap, path::Path, sync::Arc};

use axum::{
    body::Body,
    extract::ws::WebSocketUpgrade,
    http::{header::CONTENT_ENCODING, HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri},
    routing::any,
    Router,
};
use bytes::Bytes;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use reqwest::Url;
use tokio::{fs, net::TcpListener, sync::Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message as TestWsMessage};
use tower::ServiceExt;

use crate::{
    app::{load_profiles_with_claude_settings, proxy},
    constants::{CODEX_TURN_METADATA_HEADER, DEFAULT_SESSION_HEADER, UNKNOWN_SESSION},
    matcher::build_request_match,
    proxy::{
        http::handle_proxy,
        replay::{build_replay_response, handle_mock_proxy},
    },
    recording::{headers_to_records, write_bytes_file, write_json_file, write_manifest},
    sse::SseParser,
    types::{
        AppState, Args, GatewayState, HeaderRecord, HeaderValueRecord, ProfileConfig, RequestMeta,
        ResponseMeta, SessionSource,
    },
    util::{
        build_upstream_url, expects_sse, next_existing_request_index, now_rfc3339, request_dir,
        sanitize_session_id, session_from_headers, should_forward_http_header,
        should_forward_response_header,
    },
};

#[test]
fn sanitizes_session_id_for_directory_names() {
    assert_eq!(sanitize_session_id(" abc/def:ghi "), "abc_def_ghi");
    assert_eq!(sanitize_session_id("session-1_2.3"), "session-1_2.3");
    assert_eq!(sanitize_session_id("   "), UNKNOWN_SESSION);
}

#[test]
fn uses_unknown_when_session_header_is_missing() {
    let headers = HeaderMap::new();
    let header = HeaderName::from_static(DEFAULT_SESSION_HEADER);
    let (session, source) = session_from_headers(&headers, &header);
    assert_eq!(session, UNKNOWN_SESSION);
    assert!(matches!(source, SessionSource::Unknown));
}

#[test]
fn uses_configured_session_header_when_present() {
    let mut headers = HeaderMap::new();
    let header = HeaderName::from_static(DEFAULT_SESSION_HEADER);
    headers.insert(header.clone(), HeaderValue::from_static("thread/123"));
    let (session, source) = session_from_headers(&headers, &header);
    assert_eq!(session, "thread_123");
    assert!(matches!(source, SessionSource::Header { .. }));
}

#[test]
fn uses_codex_thread_id_header_as_fallback() {
    let mut headers = HeaderMap::new();
    let header = HeaderName::from_static(DEFAULT_SESSION_HEADER);
    headers.insert(
        HeaderName::from_static("thread-id"),
        HeaderValue::from_static("019f05ff-967b-70b2-b3ed-910823418893"),
    );

    let (session, source) = session_from_headers(&headers, &header);

    assert_eq!(session, "019f05ff-967b-70b2-b3ed-910823418893");
    assert!(matches!(
        source,
        SessionSource::Header { name } if name == "thread-id"
    ));
}

#[test]
fn uses_claude_code_session_id_header_as_fallback() {
    let mut headers = HeaderMap::new();
    let header = HeaderName::from_static(DEFAULT_SESSION_HEADER);
    headers.insert(
        HeaderName::from_static("x-claude-code-session-id"),
        HeaderValue::from_static("019f8505-6d2f-7603-91c3-23beae5c9267"),
    );

    let (session, source) = session_from_headers(&headers, &header);

    assert_eq!(session, "019f8505-6d2f-7603-91c3-23beae5c9267");
    assert!(matches!(
        source,
        SessionSource::Header { name } if name == "x-claude-code-session-id"
    ));
}

#[test]
fn uses_codex_turn_metadata_as_fallback() {
    let mut headers = HeaderMap::new();
    let header = HeaderName::from_static(DEFAULT_SESSION_HEADER);
    headers.insert(
        HeaderName::from_static(CODEX_TURN_METADATA_HEADER),
        HeaderValue::from_static(
            r#"{"thread_id":"thread/abc","session_id":"session/def","turn_id":"turn/ghi"}"#,
        ),
    );

    let (session, source) = session_from_headers(&headers, &header);

    assert_eq!(session, "thread_abc");
    assert!(matches!(
        source,
        SessionSource::Header { name }
            if name == "x-codex-turn-metadata.thread_id"
    ));
}

#[test]
fn joins_upstream_base_path_and_request_path() {
    let upstream = Url::parse("https://example.test/base/").unwrap();
    let url = build_upstream_url(&upstream, "v1/chat/completions", Some("a=1")).unwrap();
    assert_eq!(
        url.as_str(),
        "https://example.test/base/v1/chat/completions?a=1"
    );
}

#[tokio::test]
async fn claude_profile_reads_upstream_from_user_settings() {
    let temp = tempfile::tempdir().unwrap();
    let profile_root = temp.path().join("profiles");
    let profile_dir = profile_root.join("claude");
    let claude_settings = temp.path().join("claude-settings.json");
    fs::create_dir_all(&profile_dir).await.unwrap();
    write_bytes_file(
        profile_dir.join("bridle-profile.toml"),
        b"upstream_from = \"claude-settings\"\nsupports_websocket = false\n",
    )
    .await
    .unwrap();
    write_bytes_file(
        claude_settings.clone(),
        br#"{"env":{"ANTHROPIC_BASE_URL":"https://relay.example/anthropic","ANTHROPIC_AUTH_TOKEN":"not-read-as-profile-data"}}"#,
    )
    .await
    .unwrap();

    let profiles = load_profiles_with_claude_settings(&profile_root, &claude_settings)
        .await
        .unwrap();
    let claude = profiles.get("claude").unwrap();

    assert_eq!(claude.upstream.as_str(), "https://relay.example/anthropic");
    assert!(!claude.supports_websocket);
    assert_eq!(claude.home_dir, profile_dir);
}

#[tokio::test]
async fn auto_discovers_claude_profile_without_runtime_template() {
    let temp = tempfile::tempdir().unwrap();
    let profile_root = temp.path().join("profiles");
    let claude_settings = temp.path().join("claude-settings.json");
    fs::create_dir_all(&profile_root).await.unwrap();
    write_bytes_file(
        claude_settings.clone(),
        br#"{"env":{"ANTHROPIC_BASE_URL":"https://relay.example/anthropic"}}"#,
    )
    .await
    .unwrap();

    let profiles = load_profiles_with_claude_settings(&profile_root, &claude_settings)
        .await
        .unwrap();
    let claude = profiles.get("claude").unwrap();

    assert_eq!(claude.upstream.as_str(), "https://relay.example/anthropic");
    assert_eq!(claude.home_dir, profile_root.join("claude"));
}

#[test]
fn forwards_responses_lite_http_header_without_mutation() {
    let header = HeaderName::from_static("x-openai-internal-codex-responses-lite");
    assert!(should_forward_http_header(&header));
}

#[test]
fn removed_compatibility_options_are_not_accepted_by_cli() {
    assert!(Args::try_parse_from(["bridle-recording", "--unsafe-record-secrets"]).is_err());
    assert!(Args::try_parse_from(["bridle-recording", "--proxy-mode", "passthrough"]).is_err());
    assert!(Args::try_parse_from(["bridle-recording", "--strip-responses-lite"]).is_err());
}

#[test]
fn detects_sse_from_request_accept_when_response_content_type_is_missing() {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::ACCEPT,
        HeaderValue::from_static("text/event-stream"),
    );

    assert!(expects_sse(&headers));
}

#[test]
fn does_not_forward_hop_by_hop_response_headers() {
    assert!(!should_forward_response_header(&HeaderName::from_static(
        "transfer-encoding"
    )));
    assert!(!should_forward_response_header(&HeaderName::from_static(
        "connection"
    )));
    assert!(should_forward_response_header(&HeaderName::from_static(
        "x-codex-plan-type"
    )));
}

#[test]
fn whitelist_match_ignores_dynamic_codex_metadata() {
    let method = Method::POST;
    let body_a = Bytes::from_static(
        br#"{
            "model":"gpt-5.5",
            "stream":true,
            "tool_choice":"auto",
            "reasoning":{"effort":"xhigh","context":"all_turns"},
            "text":{"verbosity":"low"},
            "input":[{"role":"user","type":"message","content":[{"type":"input_text","text":"good morning"}],"internal_chat_message_metadata_passthrough":{"turn_id":"a"}}],
            "tools":[{"type":"function","name":"wait","description":"waits","parameters":{"type":"object"}}],
            "prompt_cache_key":"thread-a",
            "client_metadata":{"session_id":"thread-a","turn_id":"turn-a"}
        }"#,
    );
    let body_b = Bytes::from_static(
        br#"{
            "model":"gpt-5.5",
            "stream":true,
            "tool_choice":"auto",
            "reasoning":{"context":"all_turns","effort":"xhigh"},
            "text":{"verbosity":"low"},
            "input":[{"type":"message","role":"user","content":[{"text":"good morning","type":"input_text"}],"internal_chat_message_metadata_passthrough":{"turn_id":"b"}}],
            "tools":[{"name":"wait","type":"function","parameters":{"type":"object"},"description":"waits"}],
            "prompt_cache_key":"thread-b",
            "client_metadata":{"session_id":"thread-b","turn_id":"turn-b"}
        }"#,
    );

    let hash_a = build_request_match(&method, "responses", None, &HeaderMap::new(), &body_a)
        .unwrap()
        .hash;
    let hash_b = build_request_match(&method, "responses", None, &HeaderMap::new(), &body_b)
        .unwrap()
        .hash;

    assert_eq!(hash_a, hash_b);
}

#[test]
fn whitelist_match_ignores_skill_capability_surface() {
    let recorded_with_skills = Bytes::from_static(
        br#"{
            "model":"gpt-5.5",
            "stream":true,
            "input":[{
                "role":"developer",
                "type":"message",
                "content":[
                    {"type":"input_text","text":"<skills_instructions>\n- skill-a\n</skills_instructions>"},
                    {"type":"input_text","text":"<apps_instructions>\napp instructions\n</apps_instructions>"},
                    {"type":"input_text","text":"stable developer instruction"}
                ]
            },{
                "role":"user",
                "type":"message",
                "content":[{"type":"input_text","text":"good morning"}]
            }],
            "tools":[{"type":"function","name":"skill_tool","description":"from skill","parameters":{"type":"object"}}]
        }"#,
    );
    let first_agent_without_skills = Bytes::from_static(
        br#"{
            "model":"gpt-5.5",
            "stream":true,
            "input":[{
                "role":"developer",
                "type":"message",
                "content":[{"type":"input_text","text":"stable developer instruction"}]
            },{
                "role":"user",
                "type":"message",
                "content":[{"type":"input_text","text":"good morning"}]
            }]
        }"#,
    );

    let recorded_match = build_request_match(
        &Method::POST,
        "responses",
        None,
        &HeaderMap::new(),
        &recorded_with_skills,
    )
    .unwrap();
    let first_agent_match = build_request_match(
        &Method::POST,
        "responses",
        None,
        &HeaderMap::new(),
        &first_agent_without_skills,
    )
    .unwrap();

    assert_eq!(recorded_match.hash, first_agent_match.hash);
    assert!(recorded_match.canonical.pointer("/body/tools").is_none());
    let content = recorded_match
        .canonical
        .pointer("/body/input/0/content")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(content.len(), 1);
    assert_eq!(
        content[0]
            .pointer("/text")
            .and_then(serde_json::Value::as_str),
        Some("stable developer instruction")
    );
}

#[test]
fn whitelist_match_ignores_environment_date_and_timezone() {
    let method = Method::POST;
    let body_a = Bytes::from_static(
        br#"{
            "model":"gpt-5.5",
            "stream":true,
            "input":[{
                "role":"user",
                "type":"message",
                "content":[{"type":"input_text","text":"<environment_context>\n  <cwd>/workspace</cwd>\n  <shell>bash</shell>\n  <current_date>2026-06-27</current_date>\n  <timezone>PRC</timezone>\n  <filesystem>same</filesystem>\n</environment_context>"}]
            }]
        }"#,
    );
    let body_b = Bytes::from_static(
        br#"{
            "model":"gpt-5.5",
            "stream":true,
            "input":[{
                "role":"user",
                "type":"message",
                "content":[{"type":"input_text","text":"<environment_context>\n  <cwd>/workspace</cwd>\n  <shell>bash</shell>\n  <filesystem>same</filesystem>\n</environment_context>"}]
            }]
        }"#,
    );

    let recorded_match =
        build_request_match(&method, "responses", None, &HeaderMap::new(), &body_a).unwrap();
    let live_match =
        build_request_match(&method, "responses", None, &HeaderMap::new(), &body_b).unwrap();

    assert_eq!(recorded_match.hash, live_match.hash);
    let text = recorded_match
        .canonical
        .pointer("/body/input/0/content/0/text")
        .and_then(serde_json::Value::as_str)
        .unwrap();
    assert!(!text.contains("<current_date>"));
    assert!(!text.contains("<timezone>"));
    assert!(text.contains("<cwd>/workspace</cwd>"));
    assert!(text.contains("<filesystem>same</filesystem>"));
}

#[test]
fn whitelist_match_decodes_zstd_request_body() {
    let body = br#"{"model":"gpt-5.5","stream":true,"input":[{"role":"user","type":"message","content":[{"type":"input_text","text":"good morning"}]}]}"#;
    let compressed = zstd::encode_all(std::io::Cursor::new(body), 0).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_ENCODING, HeaderValue::from_static("zstd"));

    let request_match = build_request_match(
        &Method::POST,
        "responses",
        None,
        &headers,
        &Bytes::from(compressed),
    )
    .unwrap();

    assert_eq!(request_match.route.model.as_deref(), Some("gpt-5.5"));
    assert_eq!(request_match.route.stream, Some(true));
}

#[tokio::test]
async fn replay_reads_response_rewrite_only_from_derived_dir() {
    let temp = tempfile::tempdir().unwrap();
    let request_dir = temp.path().join("recording/requests/000000");
    let derived_dir = temp.path().join("derived/requests/000000");
    fs::create_dir_all(&request_dir).await.unwrap();
    fs::create_dir_all(&derived_dir).await.unwrap();
    write_json_file(
        request_dir.join("response_headers.json"),
        &Vec::<HeaderRecord>::new(),
    )
    .await
    .unwrap();
    write_json_file(
        request_dir.join("response_meta.json"),
        &ResponseMeta {
            status: 200,
            started_at: now_rfc3339(),
            completed_at: now_rfc3339(),
            response_body_bytes: 35,
            sse_event_count: 0,
            upstream_error: None,
        },
    )
    .await
    .unwrap();
    write_bytes_file(
        request_dir.join("response_body.raw"),
        br#"{"response":{"id":"resp_recorded"}}"#,
    )
    .await
    .unwrap();
    let rewrite = br#"{"replacements":[{"pointer":"/response/id","value":"resp_derived"}]}"#;
    write_bytes_file(request_dir.join("response_rewrite.json"), rewrite)
        .await
        .unwrap();

    let original = build_replay_response(&request_dir, &derived_dir)
        .await
        .unwrap();
    let original = axum::body::to_bytes(original.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(original, br#"{"response":{"id":"resp_recorded"}}"#[..]);

    write_bytes_file(derived_dir.join("response_rewrite.json"), rewrite)
        .await
        .unwrap();
    let rewritten = build_replay_response(&request_dir, &derived_dir)
        .await
        .unwrap();
    let rewritten = axum::body::to_bytes(rewritten.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&rewritten).unwrap(),
        serde_json::json!({"response": {"id": "resp_derived"}})
    );
}

#[tokio::test]
async fn next_existing_request_index_continues_after_existing_dirs() {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir_all(temp.path().join("unknown/requests/000000"))
        .await
        .unwrap();
    fs::create_dir_all(temp.path().join("unknown/requests/000003"))
        .await
        .unwrap();
    write_bytes_file(temp.path().join("unknown/requests/not-a-dir"), b"x")
        .await
        .unwrap();

    let next = next_existing_request_index(temp.path(), "unknown")
        .await
        .unwrap();
    assert_eq!(next, 4);
}

#[tokio::test]
async fn mock_replay_binds_session_and_requires_recorded_order() {
    let output_dir = tempfile::tempdir().unwrap();
    write_recorded_http_request(
        output_dir.path(),
        "recorded-session",
        0,
        "first",
        b"data: first\n\n",
    )
    .await;
    write_recorded_http_request(
        output_dir.path(),
        "recorded-session",
        1,
        "second",
        b"data: second\n\n",
    )
    .await;

    let state = test_state(output_dir.path());
    let raw_request_dir = request_dir(output_dir.path(), "recorded-session", 0);
    let derived_match_path =
        request_dir(&state.mock_derived_dir, "recorded-session", 0).join("request_match.json");
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("thread-id"),
        HeaderValue::from_static("live-session"),
    );
    headers.insert(
        axum::http::header::ACCEPT,
        HeaderValue::from_static("text/event-stream"),
    );

    let first = handle_mock_proxy(
        state.clone(),
        Method::POST,
        Uri::from_static("/mock/responses"),
        headers.clone(),
        "responses".to_owned(),
        test_body("first"),
    )
    .await
    .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert!(!raw_request_dir.join("request_match.json").exists());
    assert!(derived_match_path.exists());

    let second = handle_mock_proxy(
        state.clone(),
        Method::POST,
        Uri::from_static("/mock/responses"),
        headers.clone(),
        "responses".to_owned(),
        test_body("second"),
    )
    .await
    .unwrap();
    assert_eq!(second.status(), StatusCode::OK);

    let mismatch = handle_mock_proxy(
        state,
        Method::POST,
        Uri::from_static("/mock/responses"),
        headers,
        "responses".to_owned(),
        test_body("first"),
    )
    .await
    .unwrap_err();
    assert!(mismatch.to_string().contains("read next recorded request"));
}

#[tokio::test]
async fn mock_replay_prefers_repository_testset_over_stale_local_recording() {
    let output_dir = tempfile::tempdir().unwrap();
    let state = test_state(output_dir.path());
    write_recorded_http_request(
        output_dir.path(),
        "stale-local-session",
        0,
        "list files",
        b"data: stale-local-recording\n\n",
    )
    .await;
    let testset_dir = state.testsets_dir.join("list-files");
    let testset_raw_dir = testset_dir.join("raw");
    write_json_file(
        testset_dir.join("testset.json"),
        &serde_json::json!({"first_user_input": "list files"}),
    )
    .await
    .unwrap();
    write_recorded_http_request(
        &testset_raw_dir,
        "exported-session",
        0,
        "list files",
        b"data: from-testset\n\n",
    )
    .await;
    write_recorded_http_request(
        &testset_raw_dir,
        "exported-session",
        1,
        "hi",
        b"data: second-turn\n\n",
    )
    .await;

    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("thread-id"),
        HeaderValue::from_static("live-testset-session"),
    );
    headers.insert(
        axum::http::header::ACCEPT,
        HeaderValue::from_static("text/event-stream"),
    );

    let first = handle_mock_proxy(
        state.clone(),
        Method::POST,
        Uri::from_static("/mock/responses"),
        headers.clone(),
        "responses".to_owned(),
        test_body("list files"),
    )
    .await
    .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = handle_mock_proxy(
        state.clone(),
        Method::POST,
        Uri::from_static("/mock/responses"),
        headers,
        "responses".to_owned(),
        test_body("hi"),
    )
    .await
    .unwrap();
    assert_eq!(second.status(), StatusCode::OK);

    assert!(!testset_raw_dir
        .join("exported-session/requests/000000/request_match.json")
        .exists());
    assert!(state
        .mock_derived_dir
        .join("testsets/list-files/exported-session/requests/000000/request_match.json")
        .exists());
}

#[test]
fn parses_sse_events_across_chunks() {
    let mut parser = SseParser::default();
    assert!(parser.push(b"event: delta\ndata: hel").is_empty());
    let events = parser.push(b"lo\nid: 1\n\ndata: done\n\n");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event.as_deref(), Some("delta"));
    assert_eq!(events[0].id.as_deref(), Some("1"));
    assert_eq!(events[0].data, vec!["hello"]);
    assert_eq!(events[1].data, vec!["done"]);
}

#[test]
fn records_secret_headers_verbatim() {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", HeaderValue::from_static("Bearer secret"));
    let records = headers_to_records(&headers);
    assert!(matches!(
        &records[0].value,
        HeaderValueRecord::Text { value } if value == "Bearer secret"
    ));
}

#[tokio::test]
async fn manifest_omits_removed_recording_options() {
    let temp = tempfile::tempdir().unwrap();
    let state = test_state(temp.path());

    write_manifest(&state, "manifest-session").await.unwrap();

    let manifest = fs::read_to_string(temp.path().join("manifest-session/manifest.json"))
        .await
        .unwrap();
    let manifest: serde_json::Value = serde_json::from_str(&manifest).unwrap();
    let recorder = manifest["recorder"].as_object().unwrap();
    assert!(!recorder.contains_key("unsafe_record_secrets"));
    assert!(!recorder.contains_key("proxy_mode"));
}

#[tokio::test]
async fn http_forwarding_succeeds_when_recording_path_is_unusable() {
    let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let app = Router::new().route(
            "/*path",
            any(|headers: HeaderMap, body: Bytes| async move {
                assert_eq!(headers["x-verbatim-request"], "preserved");
                (
                    StatusCode::CREATED,
                    [("x-verbatim-response", "preserved")],
                    body,
                )
            }),
        );
        axum::serve(upstream_listener, app).await.unwrap();
    });

    let temp = tempfile::tempdir().unwrap();
    let unusable_output = temp.path().join("recordings-is-a-file");
    std::fs::write(&unusable_output, b"not a directory").unwrap();
    let mut state = test_state(&unusable_output);
    state.profile.upstream = Url::parse(&format!("http://{upstream_addr}")).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-verbatim-request", HeaderValue::from_static("preserved"));
    let request_body = Bytes::from_static(b"verbatim-request-body");

    let response = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        handle_proxy(
            state,
            Method::POST,
            Uri::from_static("/echo?case=recording-failure"),
            headers,
            "echo".to_owned(),
            Body::from(request_body.clone()),
        ),
    )
    .await
    .expect("recording failure must not delay the upstream request")
    .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers()["x-verbatim-response"], "preserved");
    let response_body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(response_body, request_body);
    assert!(unusable_output.is_file());
}

#[tokio::test]
async fn records_an_empty_request_body_as_complete() {
    let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let app = Router::new().route("/*path", any(|| async { StatusCode::NO_CONTENT }));
        axum::serve(upstream_listener, app).await.unwrap();
    });

    let temp = tempfile::tempdir().unwrap();
    let mut state = test_state(temp.path());
    state.profile.upstream = Url::parse(&format!("http://{upstream_addr}")).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        DEFAULT_SESSION_HEADER,
        HeaderValue::from_static("empty-request"),
    );

    let response = handle_proxy(
        state,
        Method::POST,
        Uri::from_static("/empty"),
        headers,
        "empty".to_owned(),
        Body::empty(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let request_dir = temp.path().join("empty-request/requests/000000");
    wait_for_recorded_request(&request_dir, 0).await;
    assert_eq!(
        fs::read(request_dir.join("request_body.raw"))
            .await
            .unwrap(),
        b""
    );
    assert!(!request_dir.join("recording_incomplete.json").exists());
}

#[tokio::test]
async fn proxies_and_records_request_larger_than_two_mebibytes() {
    let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();
    let (received_sender, mut received_receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let app = Router::new().route(
            "/*path",
            any(move |body: Body| {
                let received_sender = received_sender.clone();
                async move {
                    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
                    received_sender.send(bytes).unwrap();
                    StatusCode::NO_CONTENT
                }
            }),
        );
        axum::serve(upstream_listener, app).await.unwrap();
    });

    let temp = tempfile::tempdir().unwrap();
    let state = gateway_test_state(
        temp.path(),
        Url::parse(&format!("http://{upstream_addr}")).unwrap(),
    );
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let app = Router::new()
            .route("/:profile/*path", any(proxy))
            .with_state(state);
        axum::serve(proxy_listener, app).await.unwrap();
    });

    let request_body = Bytes::from(
        (0..(2 * 1024 * 1024 + 17_321))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let response = reqwest::Client::new()
        .post(format!("http://{proxy_addr}/codex-http/upload"))
        .header(DEFAULT_SESSION_HEADER, "large-request")
        .body(request_body.clone())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(received_receiver.recv().await.unwrap(), request_body);

    let request_dir = temp
        .path()
        .join("codex-http/recordings/large-request/requests/000000");
    wait_for_recorded_request(&request_dir, request_body.len()).await;
    assert_eq!(
        fs::read(request_dir.join("request_body.raw"))
            .await
            .unwrap(),
        request_body
    );
}

#[tokio::test]
async fn forwards_and_records_request_chunks_as_they_arrive() {
    let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();
    let (first_chunk_sender, first_chunk_receiver) = tokio::sync::oneshot::channel();
    let first_chunk_sender = Arc::new(Mutex::new(Some(first_chunk_sender)));
    let (received_sender, mut received_receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let app = Router::new().route(
            "/*path",
            any(move |body: Body| {
                let first_chunk_sender = first_chunk_sender.clone();
                let received_sender = received_sender.clone();
                async move {
                    let mut body = body.into_data_stream();
                    let mut received = Vec::new();
                    if let Some(chunk) = body.next().await {
                        received.extend_from_slice(&chunk.unwrap());
                        if let Some(sender) = first_chunk_sender.lock().await.take() {
                            let _ = sender.send(());
                        }
                    }
                    while let Some(chunk) = body.next().await {
                        received.extend_from_slice(&chunk.unwrap());
                    }
                    received_sender.send(Bytes::from(received)).unwrap();
                    StatusCode::NO_CONTENT
                }
            }),
        );
        axum::serve(upstream_listener, app).await.unwrap();
    });

    let temp = tempfile::tempdir().unwrap();
    let state = gateway_test_state(
        temp.path(),
        Url::parse(&format!("http://{upstream_addr}")).unwrap(),
    );
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let app = Router::new()
            .route("/:profile/*path", any(proxy))
            .with_state(state);
        axum::serve(proxy_listener, app).await.unwrap();
    });

    let first = Bytes::from_static(b"first-streamed-chunk-");
    let second = Bytes::from_static(b"second-streamed-chunk");
    let expected = [first.as_ref(), second.as_ref()].concat();
    let (release_sender, release_receiver) = tokio::sync::oneshot::channel();
    let request_stream = async_stream::stream! {
        yield Ok::<Bytes, std::io::Error>(first);
        let _ = release_receiver.await;
        yield Ok::<Bytes, std::io::Error>(second);
    };
    let request = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("http://{proxy_addr}/codex-http/stream"))
            .header(DEFAULT_SESSION_HEADER, "streamed-request")
            .body(reqwest::Body::wrap_stream(request_stream))
            .send()
            .await
            .unwrap()
    });

    tokio::time::timeout(std::time::Duration::from_secs(1), first_chunk_receiver)
        .await
        .expect("the first chunk must reach upstream before the request body completes")
        .unwrap();
    assert!(!request.is_finished());
    release_sender.send(()).unwrap();

    let response = request.await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(received_receiver.recv().await.unwrap(), expected);

    let request_dir = temp
        .path()
        .join("codex-http/recordings/streamed-request/requests/000000");
    wait_for_recorded_request(&request_dir, expected.len()).await;
    assert_eq!(
        fs::read(request_dir.join("request_body.raw"))
            .await
            .unwrap(),
        expected
    );
}

#[tokio::test]
async fn proxies_and_records_websocket_frames() {
    let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let app = Router::new().route(
            "/*path",
            any(|ws: WebSocketUpgrade| async {
                ws.on_upgrade(|mut socket| async move {
                    while let Some(Ok(message)) = socket.next().await {
                        match message {
                            axum::extract::ws::Message::Text(text) => {
                                let _ = socket
                                    .send(axum::extract::ws::Message::Text(format!("echo:{text}")))
                                    .await;
                            }
                            axum::extract::ws::Message::Close(close) => {
                                let _ = socket.send(axum::extract::ws::Message::Close(close)).await;
                                break;
                            }
                            other => {
                                let _ = socket.send(other).await;
                            }
                        }
                    }
                })
            }),
        );
        axum::serve(upstream_listener, app).await.unwrap();
    });

    let output_dir = tempfile::tempdir().unwrap();
    let recorder_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let recorder_addr = recorder_listener.local_addr().unwrap();
    let state = gateway_test_state(
        output_dir.path(),
        Url::parse(&format!("http://{upstream_addr}")).unwrap(),
    );
    tokio::spawn(async move {
        let app = Router::new()
            .route("/:profile/*path", any(proxy))
            .with_state(state);
        axum::serve(recorder_listener, app).await.unwrap();
    });

    let (mut ws, _) =
        connect_async(format!("ws://{recorder_addr}/codex-websocket/ws?case=record").as_str())
            .await
            .unwrap();
    ws.send(TestWsMessage::Text("hello".into())).await.unwrap();
    let echoed = ws.next().await.unwrap().unwrap();
    assert_eq!(echoed.into_text().unwrap(), "echo:hello");
    drop(ws);

    let frames_path = output_dir
        .path()
        .join("codex-websocket/recordings/unknown/requests/000000/websocket_frames.jsonl");
    let meta_path = output_dir
        .path()
        .join("codex-websocket/recordings/unknown/requests/000000/websocket_meta.json");
    for _ in 0..50 {
        if frames_path.exists() && meta_path.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let frames = fs::read_to_string(&frames_path).await.unwrap();
    assert!(frames.contains("\"client_to_upstream\""));
    assert!(frames.contains("\"upstream_to_client\""));
    assert!(frames.contains("\"hello\""));
    assert!(frames.contains("\"echo:hello\""));

    let meta = fs::read_to_string(&meta_path).await.unwrap();
    assert!(meta.contains("\"completed\"") || meta.contains("\"transfer_error\""));
}

#[tokio::test]
async fn returns_404_for_unknown_profile() {
    let state = gateway_test_state(
        tempfile::tempdir().unwrap().path(),
        Url::parse("https://example.test").unwrap(),
    );
    let app = Router::new()
        .route("/:profile/mock/*path", any(crate::app::mock_proxy))
        .route("/:profile/*path", any(proxy))
        .with_state(state);

    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/missing-profile/responses")
                .method("POST")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rejects_websocket_on_http_only_profile() {
    let state = gateway_test_state(
        tempfile::tempdir().unwrap().path(),
        Url::parse("https://example.test").unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let app = Router::new()
            .route("/:profile/*path", any(proxy))
            .with_state(state);
        axum::serve(listener, app).await.unwrap();
    });

    let err = connect_async(format!("ws://{addr}/codex-http/ws").as_str())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("400"));
}

fn test_state(output_dir: &Path) -> AppState {
    AppState {
        client: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
        profile: ProfileConfig {
            name: "codex-http".to_owned(),
            upstream: Url::parse("https://example.test").unwrap(),
            supports_websocket: false,
            home_dir: output_dir.to_path_buf(),
        },
        output_dir: output_dir.to_path_buf(),
        testsets_dir: output_dir.join("testsets/codex-http"),
        mock_derived_dir: output_dir.join(".mock-derived"),
        session_header: HeaderName::from_static(DEFAULT_SESSION_HEADER),
        counters: Arc::new(Mutex::new(HashMap::new())),
        replay_sessions: Arc::new(Mutex::new(HashMap::new())),
    }
}

fn gateway_test_state(output_root: &Path, upstream: Url) -> GatewayState {
    GatewayState {
        client: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
        output_root: output_root.to_path_buf(),
        testsets_root: output_root.join("testsets"),
        access_log_path: output_root.join("access.log"),
        profiles: Arc::new(HashMap::from([
            (
                "codex-http".to_owned(),
                ProfileConfig {
                    name: "codex-http".to_owned(),
                    upstream: upstream.clone(),
                    supports_websocket: false,
                    home_dir: output_root.join("codex-http"),
                },
            ),
            (
                "codex-websocket".to_owned(),
                ProfileConfig {
                    name: "codex-websocket".to_owned(),
                    upstream,
                    supports_websocket: true,
                    home_dir: output_root.join("codex-websocket"),
                },
            ),
        ])),
        session_header: HeaderName::from_static(DEFAULT_SESSION_HEADER),
        counters: Arc::new(Mutex::new(HashMap::new())),
        replay_sessions: Arc::new(Mutex::new(HashMap::new())),
    }
}

fn test_body(text: &str) -> Bytes {
    Bytes::from(
        serde_json::json!({
            "model": "gpt-5.5",
            "stream": true,
            "input": [{
                "role": "user",
                "type": "message",
                "content": [{ "type": "input_text", "text": text }]
            }]
        })
        .to_string(),
    )
}

async fn wait_for_recorded_request(request_dir: &Path, expected_bytes: usize) {
    let meta_path = request_dir.join("request_meta.json");
    for _ in 0..300 {
        if let Ok(raw) = fs::read(&meta_path).await {
            if let Ok(meta) = serde_json::from_slice::<crate::types::RequestMeta>(&raw) {
                if meta.request_body_bytes == expected_bytes
                    && fs::metadata(request_dir.join("request_body.raw"))
                        .await
                        .map(|metadata| metadata.len() == expected_bytes as u64)
                        .unwrap_or(false)
                {
                    return;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!(
        "request recording did not finalize with {expected_bytes} bytes at {}",
        request_dir.display()
    );
}

async fn write_recorded_http_request(
    output_dir: &Path,
    session_id: &str,
    index: u64,
    text: &str,
    response: &[u8],
) {
    let request_dir = request_dir(output_dir, session_id, index);
    fs::create_dir_all(&request_dir).await.unwrap();
    let body = test_body(text);
    write_json_file(
        request_dir.join("request_meta.json"),
        &RequestMeta {
            index,
            session_id: session_id.to_owned(),
            session_source: SessionSource::Unknown,
            started_at: now_rfc3339(),
            method: Method::POST.to_string(),
            path: "/responses".to_owned(),
            query: None,
            upstream_url: "https://example.test/responses".to_owned(),
            request_body_bytes: body.len(),
        },
    )
    .await
    .unwrap();
    write_json_file(
        request_dir.join("request_headers.json"),
        &Vec::<HeaderRecord>::new(),
    )
    .await
    .unwrap();
    write_bytes_file(request_dir.join("request_body.raw"), &body)
        .await
        .unwrap();
    write_json_file(
        request_dir.join("response_headers.json"),
        &vec![HeaderRecord {
            name: "content-type".to_owned(),
            value: HeaderValueRecord::Text {
                value: "text/event-stream".to_owned(),
            },
        }],
    )
    .await
    .unwrap();
    write_json_file(
        request_dir.join("response_meta.json"),
        &ResponseMeta {
            status: 200,
            started_at: now_rfc3339(),
            completed_at: now_rfc3339(),
            response_body_bytes: response.len(),
            sse_event_count: 1,
            upstream_error: None,
        },
    )
    .await
    .unwrap();
    write_bytes_file(request_dir.join("response_sse.raw"), response)
        .await
        .unwrap();
}
