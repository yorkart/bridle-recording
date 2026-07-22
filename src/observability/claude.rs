use chrono::{DateTime, Utc};

use super::{
    append_prompt_content_blocks, build_conversation_turns, content_text, trim_prompt_text,
    ObservedCall, ObservedRequestKind, ObservedTurn, PromptBlock, TestsetRemovalOptions,
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
    let mut turns = build_conversation_turns(calls);

    for call in calls {
        if call.request_kind == ObservedRequestKind::Conversation {
            continue;
        }
        let Some(turn_index) = agent_operation_turn_index(
            &turns,
            call.request_kind,
            &call.request_body,
            &call.started_at,
        ) else {
            continue;
        };
        turns[turn_index].calls.push(call.clone());
    }

    for turn in &mut turns {
        turn.calls.sort_by(|left, right| {
            left.started_at
                .cmp(&right.started_at)
                .then_with(|| left.index.cmp(&right.index))
        });
    }
    turns
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
}
