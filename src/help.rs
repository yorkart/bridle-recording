use axum::{extract::State, Json};

use crate::{
    constants::FALLBACK_SESSION_HEADERS,
    types::{GatewayState, ProfileConfig},
};

pub async fn help(State(state): State<GatewayState>) -> Json<serde_json::Value> {
    Json(help_document(&state))
}

pub(crate) fn help_document(state: &GatewayState) -> serde_json::Value {
    let mut profiles = state.profiles.values().collect::<Vec<_>>();
    profiles.sort_by(|left, right| left.name.cmp(&right.name));
    let profile_names = profiles
        .iter()
        .map(|profile| profile.name.clone())
        .collect::<Vec<_>>();
    let profile_descriptions = profiles
        .into_iter()
        .map(profile_document)
        .collect::<Vec<_>>();
    let mut accepted_session_headers = vec![state.session_header.to_string()];
    for header in FALLBACK_SESSION_HEADERS {
        if !accepted_session_headers.iter().any(|name| name == header) {
            accepted_session_headers.push((*header).to_owned());
        }
    }

    serde_json::json!({
        "schema_version": "1",
        "service": {
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Transparent model-provider traffic recorder and mock replay service",
            "help_semantics": "Machine-readable equivalent of CLI --help; paths are relative to this service origin"
        },
        "endpoints": {
            "help": {"method": "GET", "path": "/help"},
            "health": {"method": "GET", "path": "/health"},
            "ui": {"method": "GET", "path": "/ui"},
            "profiles": {"method": "GET", "path": "/api/profiles"},
            "sessions": {"method": "GET", "path_template": "/api/profiles/{profile}/sessions"},
            "session": {"method": "GET", "path_template": "/api/profiles/{profile}/sessions/{session_id}"},
            "testsets": {"method": "GET", "path": "/api/testsets"},
            "testsets_by_profile": {"method": "GET", "path_template": "/api/testsets/{profile}"},
            "testset_preview": {"method": "POST", "path_template": "/api/profiles/{profile}/sessions/{session_id}/testset/preview"},
            "testset_save": {"method": "POST", "path_template": "/api/profiles/{profile}/sessions/{session_id}/testset"}
        },
        "active_profiles": profile_names,
        "profiles": profile_descriptions,
        "session_identification": {
            "accepted_headers_in_precedence_order": accepted_session_headers,
            "missing_session_behavior": "Requests without a usable session identifier are recorded under the unknown session"
        },
        "operations": [
            record_proxy_operation(&profile_names),
            list_testsets_operation(&profile_names),
            mock_replay_operation(&profile_names)
        ],
        "behavior": {
            "transparent_proxy": true,
            "request_headers_recorded_verbatim": true,
            "request_body_recorded_verbatim": true,
            "response_headers_recorded_verbatim": true,
            "response_body_recorded_verbatim": true,
            "authentication": "Client-supplied authentication headers are forwarded and recorded verbatim",
            "recording_failure_affects_forwarding": false,
            "derived_features": [
                "observability views",
                "testset export",
                "request matching",
                "mock replay"
            ],
            "security_notice": "Raw recordings contain sensitive headers and bodies; protect the recording directory"
        }
    })
}

fn profile_document(profile: &ProfileConfig) -> serde_json::Value {
    let mut protocols = vec!["http", "sse"];
    if profile.supports_websocket {
        protocols.push("websocket");
    }
    let preferred_session_header = if profile.name == "claude" {
        "x-claude-code-session-id"
    } else {
        "x-codex-session-id"
    };
    let examples = match profile.name.as_str() {
        "claude" => vec![serde_json::json!({
            "description": "Record a Claude Messages API request",
            "method": "POST",
            "path": format!("/{}/v1/messages?beta=true", profile.name)
        })],
        "codex-http" | "codex-websocket" => vec![serde_json::json!({
            "description": "Record an OpenAI Responses API request",
            "method": "POST",
            "path": format!("/{}/responses", profile.name)
        })],
        _ => vec![serde_json::json!({
            "description": "Forward an upstream API request through this profile",
            "method": "<original-method>",
            "path": format!("/{}/{{upstream_path}}", profile.name)
        })],
    };

    serde_json::json!({
        "name": profile.name,
        "recording_base_path": format!("/{}", profile.name),
        "recording_path_template": format!("/{}/{{upstream_path}}", profile.name),
        "mock_base_path": format!("/{}/mock", profile.name),
        "mock_path_template": format!("/{}/mock/{{upstream_path}}", profile.name),
        "protocols": protocols,
        "supports_websocket": profile.supports_websocket,
        "preferred_session_header": preferred_session_header,
        "path_mapping": "The profile prefix is removed and the remaining path and query are forwarded to the configured upstream",
        "examples": examples
    })
}

fn profile_property(profile_names: &[String]) -> serde_json::Value {
    serde_json::json!({
        "type": "string",
        "description": "An active recorder profile",
        "enum": profile_names
    })
}

fn record_proxy_operation(profile_names: &[String]) -> serde_json::Value {
    serde_json::json!({
        "name": "record_proxy_request",
        "description": "Forward and record one model-provider request without changing its protocol content",
        "http": {
            "method": "<original-method>",
            "path_template": "/{profile}/{upstream_path}",
            "query": "Forward the original query string unchanged"
        },
        "input_schema": {
            "type": "object",
            "required": ["profile", "method", "upstream_path"],
            "properties": {
                "profile": profile_property(profile_names),
                "method": {"type": "string", "description": "Original HTTP method"},
                "upstream_path": {"type": "string", "description": "Original provider API path without the recorder profile prefix"},
                "query": {"type": "string", "description": "Optional original query string"},
                "headers": {
                    "type": "object",
                    "description": "Original request headers, including authentication and session headers",
                    "additionalProperties": {"type": "string"}
                },
                "body": {"description": "Original request body; JSON and non-JSON bodies are accepted"}
            }
        },
        "result": "The upstream response is streamed to the client while raw request and response traffic is recorded on a side path"
    })
}

fn list_testsets_operation(profile_names: &[String]) -> serde_json::Value {
    serde_json::json!({
        "name": "list_testsets",
        "description": "List exported testsets. Each returned testset already includes first_user_input and all user_inputs",
        "http": {
            "variants": [
                {
                    "method": "GET",
                    "path": "/api/testsets",
                    "description": "List testsets across all profiles"
                },
                {
                    "method": "GET",
                    "path_template": "/api/testsets/{profile}",
                    "description": "List testsets for one profile"
                }
            ]
        },
        "input_schema": {
            "type": "object",
            "properties": {
                "profile": profile_property(profile_names)
            }
        },
        "output_schema": {
            "type": "object",
            "required": ["testsets"],
            "properties": {
                "testsets": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": [
                            "profile",
                            "id",
                            "source_session_id",
                            "first_user_input",
                            "user_inputs",
                            "user_input_sha256",
                            "saved_at"
                        ],
                        "properties": {
                            "profile": {"type": "string"},
                            "id": {"type": "string", "description": "Testset identifier"},
                            "source_session_id": {"type": "string"},
                            "first_user_input": {"type": "string"},
                            "user_inputs": {
                                "type": "array",
                                "description": "All user inputs retained in this testset, in recorded order",
                                "items": {"type": "string"}
                            },
                            "user_input_sha256": {"type": "string"},
                            "saved_at": {"type": "string", "format": "date-time"},
                            "source_recording_path": {"type": "string"},
                            "raw_recording_path": {"type": "string"},
                            "testset_path": {"type": "string"},
                            "export": {"type": ["object", "null"]}
                        }
                    }
                }
            }
        },
        "examples": [
            {"method": "GET", "path": "/api/testsets"},
            {"method": "GET", "path": "/api/testsets/claude"}
        ]
    })
}

fn mock_replay_operation(profile_names: &[String]) -> serde_json::Value {
    serde_json::json!({
        "name": "mock_replay_request",
        "description": "Replay a matching exported testset or local recording through a provider-compatible endpoint",
        "http": {
            "method": "<original-method>",
            "path_template": "/{profile}/mock/{upstream_path}"
        },
        "input_schema": {
            "type": "object",
            "required": ["profile", "method", "upstream_path", "body"],
            "properties": {
                "profile": profile_property(profile_names),
                "method": {"type": "string"},
                "upstream_path": {"type": "string"},
                "headers": {"type": "object", "additionalProperties": {"type": "string"}},
                "body": {"description": "Provider-compatible request body used for testset matching"}
            }
        },
        "behavior": {
            "testset_priority": "Repository testsets are matched before local recordings",
            "session_binding": "After the first match, later requests remain bound to the same recorded session and order",
            "authentication_headers_used_for_matching": false
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf, sync::Arc};

    use axum::http::HeaderName;
    use reqwest::Url;
    use tokio::sync::Mutex;

    use super::*;

    fn test_state() -> GatewayState {
        let mut profiles = HashMap::new();
        for (name, supports_websocket) in [("codex-http", false), ("claude", false)] {
            profiles.insert(
                name.to_owned(),
                ProfileConfig {
                    name: name.to_owned(),
                    upstream: Url::parse("https://example.test").unwrap(),
                    supports_websocket,
                    home_dir: PathBuf::from(format!("/tmp/{name}")),
                },
            );
        }
        GatewayState {
            client: reqwest::Client::new(),
            output_root: PathBuf::from("/tmp/bridle-help"),
            testsets_root: PathBuf::from("/tmp/testsets"),
            access_log_path: PathBuf::from("/tmp/bridle-help/access.log"),
            profiles: Arc::new(profiles),
            session_header: HeaderName::from_static("x-codex-session-id"),
            counters: Arc::new(Mutex::new(HashMap::new())),
            replay_sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[test]
    fn help_is_dynamic_and_documents_testset_user_inputs() {
        let document = help_document(&test_state());

        assert_eq!(
            document["active_profiles"],
            serde_json::json!(["claude", "codex-http"])
        );
        let operations = document["operations"].as_array().unwrap();
        let list_testsets = operations
            .iter()
            .find(|operation| operation["name"] == "list_testsets")
            .unwrap();
        assert_eq!(
            list_testsets
                .pointer("/output_schema/properties/testsets/items/properties/user_inputs/type"),
            Some(&serde_json::json!("array"))
        );
        assert_eq!(
            list_testsets.pointer("/http/variants/1/path_template"),
            Some(&serde_json::json!("/api/testsets/{profile}"))
        );
    }

    #[test]
    fn help_exposes_profile_paths_without_upstream_credentials() {
        let document = help_document(&test_state());
        let profiles = document["profiles"].as_array().unwrap();
        let claude = profiles
            .iter()
            .find(|profile| profile["name"] == "claude")
            .unwrap();

        assert_eq!(claude["recording_base_path"], "/claude");
        assert_eq!(claude["mock_base_path"], "/claude/mock");
        assert_eq!(
            claude["preferred_session_header"],
            "x-claude-code-session-id"
        );
        assert!(!document.to_string().contains("https://example.test"));
    }
}
