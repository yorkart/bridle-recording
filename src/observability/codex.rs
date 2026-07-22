use super::{
    build_conversation_turns, classify_prompt_block, content_text, excerpt, trim_prompt_text,
    ObservedCall, ObservedRequestKind, ObservedTurn, PromptBlock, TestsetRemovalOptions,
};

pub(super) fn prompt_blocks(request_body: &serde_json::Value) -> Vec<PromptBlock> {
    let mut blocks = Vec::new();
    if let Some(instructions) = request_body
        .get("instructions")
        .and_then(serde_json::Value::as_str)
        .filter(|instructions| !instructions.trim().is_empty())
    {
        blocks.push(PromptBlock {
            role: "system".to_owned(),
            block_type: "system".to_owned(),
            chars: instructions.chars().count(),
            excerpt: excerpt(instructions, 760),
            text: instructions.to_owned(),
        });
    }

    if let Some(input) = request_body
        .get("input")
        .and_then(serde_json::Value::as_array)
    {
        blocks.extend(input.iter().filter_map(|item| {
            if item.get("type").and_then(serde_json::Value::as_str) != Some("message") {
                return None;
            }
            let role = item
                .get("role")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("message")
                .to_owned();
            let text = content_text(item.get("content"));
            let block_type = classify_prompt_block(&role, &text);
            Some(PromptBlock {
                role,
                block_type,
                chars: text.chars().count(),
                excerpt: excerpt(&text, 760),
                text,
            })
        }));
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

pub(super) fn classify_request_kind(_request_body: &serde_json::Value) -> ObservedRequestKind {
    ObservedRequestKind::Conversation
}

pub(super) fn build_turns(calls: &[ObservedCall]) -> Vec<ObservedTurn> {
    build_conversation_turns(calls)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_prompt_signatures_do_not_change_codex_request_semantics() {
        let claude_title_shaped_body = serde_json::json!({
            "system": [
                {"type": "text", "text": "You are Claude Code, Anthropic's official CLI for Claude."},
                {
                    "type": "text",
                    "text": "Generate a concise, sentence-case title. Return JSON with a single \"title\" field."
                }
            ],
            "messages": [{
                "role": "user",
                "content": [{"type": "text", "text": "<session>\nlist files\n</session>"}]
            }]
        });

        assert_eq!(
            classify_request_kind(&claude_title_shaped_body),
            ObservedRequestKind::Conversation
        );
    }

    #[test]
    fn prompt_derivation_reads_only_codex_input_shape() {
        let body = serde_json::json!({
            "input": [{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "codex user"}]
            }],
            "messages": [{"role": "user", "content": "claude user"}]
        });

        let blocks = prompt_blocks(&body);
        assert_eq!(visible_user_messages(&blocks), vec!["codex user"]);
    }
}
