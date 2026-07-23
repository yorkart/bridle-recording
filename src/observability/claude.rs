use chrono::{DateTime, Utc};

use super::{
    append_prompt_content_blocks, build_conversation_turns, build_main_flow, content_text,
    trim_prompt_text, ObservedCall, ObservedFlow, ObservedFlowRelation, ObservedFlowRole,
    ObservedFlowTiming, ObservedRequestKind, ObservedTurn, PromptBlock, TestsetRemovalOptions,
};

pub(super) fn prompt_blocks(request_body: &serde_json::Value) -> Vec<PromptBlock> {
    let mut blocks = Vec::new();
    append_prompt_content_blocks(&mut blocks, "system", request_body.get("system"));
    if let Some(messages) = request_body
        .get("messages")
        .and_then(serde_json::Value::as_array)
    {
        for message in messages {
            let role = message
                .get("role")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("message");
            append_prompt_content_blocks(&mut blocks, role, message.get("content"));
        }
    }
    blocks
}

pub(super) fn visible_user_messages(blocks: &[PromptBlock]) -> Vec<String> {
    let remove_derived = TestsetRemovalOptions {
        skills: true,
        apps: true,
        plugins: true,
        derived_prompt: true,
        ..TestsetRemovalOptions::default()
    };
    blocks
        .iter()
        .filter(|block| block.role == "user")
        .filter(|block| block.block_type != "system_reminder")
        .filter_map(|block| {
            let mut text = block.text.clone();
            trim_prompt_text(&mut text, &remove_derived).then(|| text.trim().to_owned())
        })
        .collect()
}

pub(super) fn classify_request_kind(request_body: &serde_json::Value) -> ObservedRequestKind {
    let system_text = content_text(request_body.get("system"));
    let is_claude_code =
        system_text.contains("You are Claude Code, Anthropic's official CLI for Claude.");
    let messages = request_body
        .get("messages")
        .and_then(serde_json::Value::as_array);
    let last_user_text = messages
        .and_then(|messages| messages.last())
        .filter(|message| message.get("role").and_then(serde_json::Value::as_str) == Some("user"))
        .map(|message| content_text(message.get("content")))
        .unwrap_or_default();

    let is_session_title = is_claude_code
        && messages.is_some_and(|messages| messages.len() == 1)
        && system_text.contains("Generate a concise, sentence-case title")
        && system_text.contains("Return JSON with a single \"title\" field")
        && last_user_text.trim_start().starts_with("<session>");
    if is_session_title {
        return ObservedRequestKind::SessionTitle;
    }

    if is_claude_code
        && last_user_text
            .trim_start()
            .starts_with("The user stepped away and is coming back. Recap in under 40 words")
    {
        return ObservedRequestKind::SessionRecap;
    }

    ObservedRequestKind::Conversation
}

pub(super) fn build_turns(calls: &[ObservedCall]) -> Vec<ObservedTurn> {
    build_conversation_turns(calls)
}

pub(super) fn build_flows(
    calls: &[ObservedCall],
    main_turns: &[ObservedTurn],
) -> Vec<ObservedFlow> {
    let mut flows = vec![build_main_flow(main_turns)];
    flows.extend(
        calls
            .iter()
            .filter(|call| call.request_kind != ObservedRequestKind::Conversation)
            .map(|call| build_agent_flow(call, main_turns)),
    );
    flows
}

fn build_agent_flow(call: &ObservedCall, main_turns: &[ObservedTurn]) -> ObservedFlow {
    let id = format!("agent-{}", call.index);
    let label = agent_flow_label(call.request_kind);
    let user = call
        .visible_user_messages
        .last()
        .cloned()
        .unwrap_or_else(|| label.clone());
    let turn = ObservedTurn {
        id: format!("{id}-turn-000000"),
        user,
        started_at: call.started_at.clone(),
        calls: vec![call.clone()],
        assistant: call.output_text.clone(),
        tool_outputs: Vec::new(),
    };
    ObservedFlow {
        id,
        role: ObservedFlowRole::Agent,
        kind: call.request_kind,
        label,
        started_at: call.started_at.clone(),
        completed_at: call.completed_at.clone(),
        request_count: 1,
        relation: agent_operation_relation(main_turns, call),
        turns: vec![turn],
    }
}

fn agent_flow_label(request_kind: ObservedRequestKind) -> String {
    match request_kind {
        ObservedRequestKind::Conversation => "Conversation".to_owned(),
        ObservedRequestKind::SessionTitle => "Session title".to_owned(),
        ObservedRequestKind::SessionRecap => "Session recap".to_owned(),
    }
}

fn agent_operation_relation(
    turns: &[ObservedTurn],
    call: &ObservedCall,
) -> Option<ObservedFlowRelation> {
    let turn_index = agent_operation_turn_index(
        turns,
        call.request_kind,
        &call.request_body,
        &call.started_at,
    )?;
    let turn = &turns[turn_index];
    let operation_started = parse_timestamp(&call.started_at);
    let operation_completed = parse_timestamp(&call.completed_at);
    let during_call = operation_started.and_then(|started| {
        turn.calls
            .iter()
            .find(|main_call| call_is_active_at(main_call, started))
    });
    let overlaps_main = operation_started.is_some_and(|started| {
        turn.calls.iter().any(|main_call| {
            intervals_overlap(
                started,
                operation_completed,
                parse_timestamp(&main_call.started_at),
                parse_timestamp(&main_call.completed_at),
            )
        })
    });

    let (timing, anchor_call_index) = if let Some(main_call) = during_call {
        (
            ObservedFlowTiming::DuringCall,
            Some(main_call.index.clone()),
        )
    } else if timestamp_is_at_or_before(&call.started_at, &turn.started_at) {
        (
            ObservedFlowTiming::TurnStart,
            turn.calls.first().map(|main_call| main_call.index.clone()),
        )
    } else if let Some(last_call) = turn.calls.last().filter(|last_call| {
        !last_call.completed_at.is_empty()
            && timestamp_is_after(&call.started_at, &last_call.completed_at)
    }) {
        (ObservedFlowTiming::AfterTurn, Some(last_call.index.clone()))
    } else {
        let previous_call = turn
            .calls
            .iter()
            .filter(|main_call| timestamp_is_at_or_before(&main_call.started_at, &call.started_at))
            .max_by(|left, right| left.started_at.cmp(&right.started_at));
        (
            ObservedFlowTiming::BetweenCalls,
            previous_call.map(|main_call| main_call.index.clone()),
        )
    };

    Some(ObservedFlowRelation {
        main_turn_id: turn.id.clone(),
        timing,
        anchor_call_index,
        overlaps_main,
    })
}

fn call_is_active_at(call: &ObservedCall, timestamp: DateTime<Utc>) -> bool {
    let Some(started_at) = parse_timestamp(&call.started_at) else {
        return false;
    };
    started_at <= timestamp
        && parse_timestamp(&call.completed_at).is_none_or(|completed_at| timestamp <= completed_at)
}

fn intervals_overlap(
    left_started: DateTime<Utc>,
    left_completed: Option<DateTime<Utc>>,
    right_started: Option<DateTime<Utc>>,
    right_completed: Option<DateTime<Utc>>,
) -> bool {
    let Some(right_started) = right_started else {
        return false;
    };
    left_completed.is_none_or(|completed| right_started <= completed)
        && right_completed.is_none_or(|completed| left_started <= completed)
}

fn timestamp_is_at_or_before(left: &str, right: &str) -> bool {
    match (parse_timestamp(left), parse_timestamp(right)) {
        (Some(left), Some(right)) => left <= right,
        _ => left <= right,
    }
}

fn timestamp_is_after(left: &str, right: &str) -> bool {
    match (parse_timestamp(left), parse_timestamp(right)) {
        (Some(left), Some(right)) => left > right,
        _ => left > right,
    }
}

fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    value.parse().ok()
}

fn agent_operation_turn_index(
    turns: &[ObservedTurn],
    request_kind: ObservedRequestKind,
    request_body: &serde_json::Value,
    started_at: &str,
) -> Option<usize> {
    if request_kind == ObservedRequestKind::SessionTitle {
        if let Some(subject) = session_title_subject(request_body) {
            if let Some((index, _)) = turns
                .iter()
                .enumerate()
                .filter(|(_, turn)| turn.user.trim() == subject)
                .min_by_key(|(_, turn)| timestamp_distance_ms(&turn.started_at, started_at))
            {
                return Some(index);
            }
        }
    }

    turns
        .iter()
        .enumerate()
        .filter(|(_, turn)| turn.started_at.as_str() <= started_at)
        .max_by(|(_, left), (_, right)| left.started_at.cmp(&right.started_at))
        .map(|(index, _)| index)
        .or_else(|| {
            turns
                .iter()
                .enumerate()
                .min_by_key(|(_, turn)| timestamp_distance_ms(&turn.started_at, started_at))
                .map(|(index, _)| index)
        })
}

fn session_title_subject(request_body: &serde_json::Value) -> Option<&str> {
    let message = request_body
        .get("messages")
        .and_then(serde_json::Value::as_array)?
        .first()?;
    let text = message
        .get("content")
        .and_then(serde_json::Value::as_array)?
        .iter()
        .filter_map(|part| part.get("text").and_then(serde_json::Value::as_str))
        .find(|text| text.contains("<session>") && text.contains("</session>"))?;
    let start = text.find("<session>")? + "<session>".len();
    let end = start + text[start..].find("</session>")?;
    Some(text[start..end].trim())
}

fn timestamp_distance_ms(left: &str, right: &str) -> i64 {
    let Some(left) = left.parse::<DateTime<Utc>>().ok() else {
        return i64::MAX;
    };
    let Some(right) = right.parse::<DateTime<Utc>>().ok() else {
        return i64::MAX;
    };
    (left - right).num_milliseconds().abs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observed_call(
        index: &str,
        request_kind: ObservedRequestKind,
        started_at: &str,
        completed_at: &str,
        request_body: serde_json::Value,
    ) -> ObservedCall {
        ObservedCall {
            index: index.to_owned(),
            request_id: format!("request-{index}"),
            started_at: started_at.to_owned(),
            completed_at: completed_at.to_owned(),
            duration_ms: None,
            method: "POST".to_owned(),
            path: "/v1/messages".to_owned(),
            status: 200,
            protocol: "http".to_owned(),
            request_kind,
            recording_state: "complete".to_owned(),
            recording_warning: None,
            model: "claude".to_owned(),
            stream: true,
            input_count: 1,
            tools_count: 0,
            tool_names: Vec::new(),
            tool_definitions: Vec::new(),
            prompt_blocks: Vec::new(),
            visible_user_messages: vec!["derived input".to_owned()],
            previous_tool_outputs: Vec::new(),
            previous_function_calls: Vec::new(),
            previous_assistant_messages: Vec::new(),
            function_calls: Vec::new(),
            output_text: "derived output".to_owned(),
            response_body: None,
            usage: None,
            event_counts: std::collections::BTreeMap::new(),
            sse_events: Vec::new(),
            websocket_frames: Vec::new(),
            websocket_meta: None,
            request_meta: serde_json::Value::Null,
            response_meta: None,
            request_body,
            timeline: Vec::new(),
            files: Vec::new(),
            raw_dir: String::new(),
            request_body_bytes: 0,
            response_body_bytes: 0,
        }
    }

    fn title_request() -> serde_json::Value {
        serde_json::json!({
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
        })
    }

    fn recap_request() -> serde_json::Value {
        serde_json::json!({
            "system": "You are Claude Code, Anthropic's official CLI for Claude.",
            "messages": [
                {"role": "user", "content": "list files"},
                {"role": "assistant", "content": "done"},
                {
                    "role": "user",
                    "content": "The user stepped away and is coming back. Recap in under 40 words, 1-2 plain sentences, no markdown."
                }
            ]
        })
    }

    #[test]
    fn classifies_known_claude_agent_operations() {
        assert_eq!(
            classify_request_kind(&title_request()),
            ObservedRequestKind::SessionTitle
        );
        assert_eq!(
            classify_request_kind(&recap_request()),
            ObservedRequestKind::SessionRecap
        );
        assert_eq!(
            classify_request_kind(&serde_json::json!({
                "messages": [{"role": "user", "content": "ping"}]
            })),
            ObservedRequestKind::Conversation
        );
    }

    #[test]
    fn prompt_derivation_reads_only_claude_messages_shape() {
        let body = serde_json::json!({
            "messages": [{"role": "user", "content": "claude user"}],
            "input": [{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "codex user"}]
            }]
        });

        let blocks = prompt_blocks(&body);
        assert_eq!(visible_user_messages(&blocks), vec!["claude user"]);
    }

    #[test]
    fn associates_claude_agent_operations_with_the_related_turn() {
        let turns = vec![
            ObservedTurn {
                id: "turn-000000".to_owned(),
                user: "hi".to_owned(),
                started_at: "2026-07-21T23:22:00.720Z".to_owned(),
                calls: Vec::new(),
                assistant: String::new(),
                tool_outputs: Vec::new(),
            },
            ObservedTurn {
                id: "turn-000001".to_owned(),
                user: "list files".to_owned(),
                started_at: "2026-07-21T23:22:05.591Z".to_owned(),
                calls: Vec::new(),
                assistant: String::new(),
                tool_outputs: Vec::new(),
            },
        ];
        let title = title_request();
        let recap = recap_request();

        assert_eq!(session_title_subject(&title), Some("list files"));
        assert_eq!(
            agent_operation_turn_index(
                &turns,
                ObservedRequestKind::SessionTitle,
                &title,
                "2026-07-21T23:22:05.589Z",
            ),
            Some(1)
        );
        assert_eq!(
            agent_operation_turn_index(
                &turns,
                ObservedRequestKind::SessionRecap,
                &recap,
                "2026-07-21T23:25:13.269Z",
            ),
            Some(1)
        );
    }

    #[test]
    fn builds_agent_flows_parallel_to_the_main_flow_with_timing_relations() {
        let main_calls = vec![
            observed_call(
                "000003",
                ObservedRequestKind::Conversation,
                "2026-07-21T23:22:05.591Z",
                "2026-07-21T23:22:08.100Z",
                serde_json::Value::Null,
            ),
            observed_call(
                "000004",
                ObservedRequestKind::Conversation,
                "2026-07-21T23:22:09.400Z",
                "2026-07-21T23:22:13.630Z",
                serde_json::Value::Null,
            ),
        ];
        let main_turns = vec![ObservedTurn {
            id: "turn-000002".to_owned(),
            user: "list files".to_owned(),
            started_at: "2026-07-21T23:22:05.591Z".to_owned(),
            calls: main_calls,
            assistant: "done".to_owned(),
            tool_outputs: Vec::new(),
        }];
        let agent_calls = vec![
            observed_call(
                "000002",
                ObservedRequestKind::SessionTitle,
                "2026-07-21T23:22:05.589Z",
                "2026-07-21T23:22:09.394Z",
                title_request(),
            ),
            observed_call(
                "000005",
                ObservedRequestKind::SessionTitle,
                "2026-07-21T23:22:09.407Z",
                "2026-07-21T23:22:29.949Z",
                title_request(),
            ),
            observed_call(
                "000006",
                ObservedRequestKind::SessionRecap,
                "2026-07-21T23:25:13.269Z",
                "2026-07-21T23:25:16.832Z",
                recap_request(),
            ),
        ];

        let flows = build_flows(&agent_calls, &main_turns);

        assert_eq!(flows.len(), 4);
        assert!(matches!(flows[0].role, ObservedFlowRole::Main));
        assert_eq!(flows[0].request_count, 2);
        assert_eq!(flows[0].turns[0].calls.len(), 2);
        let relations = flows[1..]
            .iter()
            .map(|flow| flow.relation.as_ref().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(relations[0].timing, ObservedFlowTiming::TurnStart);
        assert_eq!(relations[0].anchor_call_index.as_deref(), Some("000003"));
        assert!(relations[0].overlaps_main);
        assert_eq!(relations[1].timing, ObservedFlowTiming::DuringCall);
        assert_eq!(relations[1].anchor_call_index.as_deref(), Some("000004"));
        assert!(relations[1].overlaps_main);
        assert_eq!(relations[2].timing, ObservedFlowTiming::AfterTurn);
        assert_eq!(relations[2].anchor_call_index.as_deref(), Some("000004"));
        assert!(!relations[2].overlaps_main);
        assert_eq!(flows[1].turns[0].user, "derived input");
    }
}
