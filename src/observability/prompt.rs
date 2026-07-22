use super::*;

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

    append_prompt_content_blocks(&mut blocks, "system", request_body.get("system"));

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

pub(super) fn append_prompt_content_blocks(
    blocks: &mut Vec<PromptBlock>,
    role: &str,
    content: Option<&serde_json::Value>,
) {
    let texts = match content {
        Some(serde_json::Value::String(text)) => vec![text.as_str()],
        Some(serde_json::Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(serde_json::Value::as_str))
            .collect(),
        _ => Vec::new(),
    };
    for text in texts.into_iter().filter(|text| !text.trim().is_empty()) {
        blocks.push(PromptBlock {
            role: role.to_owned(),
            block_type: classify_prompt_block(role, text),
            chars: text.chars().count(),
            excerpt: excerpt(text, 760),
            text: text.to_owned(),
        });
    }
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

pub(super) fn previous_assistant_messages(blocks: &[PromptBlock]) -> Vec<String> {
    blocks
        .iter()
        .filter(|block| block.role == "assistant")
        .map(|block| block.excerpt.clone())
        .collect()
}

pub(super) fn classify_prompt_block(role: &str, text: &str) -> String {
    let trimmed = text.trim_start();
    if trimmed.starts_with("<environment_context>") {
        "environment"
    } else if trimmed.starts_with("<system-reminder>") {
        "system_reminder"
    } else if trimmed.starts_with("<permissions instructions>") {
        "permissions"
    } else if trimmed.starts_with("<skills_instructions>") {
        "skills"
    } else if trimmed.starts_with("<apps_instructions>") {
        "apps"
    } else if trimmed.starts_with("<plugins_instructions>")
        || trimmed.starts_with("<recommended_plugins>")
    {
        "plugins"
    } else {
        role
    }
    .to_owned()
}

pub(super) fn tool_definitions(request_body: &serde_json::Value) -> Vec<ToolDefinition> {
    let top_level = request_body
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten();
    let additional = request_body
        .get("input")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| {
            item.get("type").and_then(serde_json::Value::as_str) == Some("additional_tools")
        })
        .flat_map(|item| {
            item.get("tools")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
        });

    top_level
        .chain(additional)
        .map(|tool| {
            let function = tool.get("function");
            let name = tool
                .get("name")
                .or_else(|| function.and_then(|function| function.get("name")))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_owned();
            let tool_type = tool
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("function")
                .to_owned();
            let description = tool
                .get("description")
                .or_else(|| function.and_then(|function| function.get("description")))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            ToolDefinition {
                name,
                tool_type,
                description: description.to_owned(),
                definition: tool.clone(),
            }
        })
        .collect()
}

pub(super) fn previous_tool_outputs(request_body: &serde_json::Value) -> Vec<ToolOutput> {
    let mut outputs = request_body
        .get("input")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| {
            matches!(
                item.get("type").and_then(serde_json::Value::as_str),
                Some("function_call_output" | "custom_tool_call_output")
            )
        })
        .map(|item| ToolOutput {
            call_id: item
                .get("call_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            output: tool_output_text(item.get("output")),
        })
        .collect::<Vec<_>>();
    outputs.extend(
        request_body
            .get("messages")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .flat_map(|message| {
                message
                    .get("content")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .filter(|part| {
                part.get("type").and_then(serde_json::Value::as_str) == Some("tool_result")
            })
            .map(|part| ToolOutput {
                call_id: part
                    .get("tool_use_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                output: tool_output_text(part.get("content")),
            }),
    );
    outputs
}

pub(super) fn previous_function_calls(
    request_body: &serde_json::Value,
) -> Vec<ObservedFunctionCall> {
    let mut calls = request_body
        .get("input")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| is_tool_call_type(item.get("type").and_then(serde_json::Value::as_str)))
        .map(|item| ObservedFunctionCall {
            id: item
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            call_id: item
                .get("call_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            name: item
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
            status: item
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            arguments: pretty_json_str(
                item.get("arguments")
                    .or_else(|| item.get("input"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default(),
            ),
            result: None,
        })
        .collect::<Vec<_>>();
    calls.extend(
        request_body
            .get("messages")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter(|message| {
                message.get("role").and_then(serde_json::Value::as_str) == Some("assistant")
            })
            .flat_map(|message| {
                message
                    .get("content")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .filter(|part| part.get("type").and_then(serde_json::Value::as_str) == Some("tool_use"))
            .map(|part| {
                let id = part
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                ObservedFunctionCall {
                    id: id.clone(),
                    call_id: id,
                    name: part
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("unknown")
                        .to_owned(),
                    status: "completed".to_owned(),
                    arguments: pretty_json_value(part.get("input")),
                    result: None,
                }
            }),
    );
    calls
}

pub(super) fn is_tool_call_type(item_type: Option<&str>) -> bool {
    matches!(item_type, Some("function_call" | "custom_tool_call"))
}

pub(super) fn build_conversation_turns(calls: &[ObservedCall]) -> Vec<ObservedTurn> {
    let mut turns: Vec<ObservedTurn> = Vec::new();
    for call in calls {
        if call.request_kind != ObservedRequestKind::Conversation {
            continue;
        }
        let user = call
            .visible_user_messages
            .last()
            .cloned()
            .unwrap_or_else(|| "(no visible user input)".to_owned());
        let should_start = turns.last().map(|turn| turn.user != user).unwrap_or(true);
        if should_start {
            turns.push(ObservedTurn {
                id: format!("turn-{:06}", turns.len()),
                user,
                started_at: call.started_at.clone(),
                calls: Vec::new(),
                assistant: String::new(),
                tool_outputs: Vec::new(),
            });
        }
        let turn = turns
            .last_mut()
            .expect("turn inserted before attaching observed call");
        if !call.output_text.is_empty() {
            turn.assistant = call.output_text.clone();
        }
        for output in &call.previous_tool_outputs {
            if !turn
                .tool_outputs
                .iter()
                .any(|existing| existing.call_id == output.call_id)
            {
                turn.tool_outputs.push(output.clone());
            }
        }
        turn.calls.push(call.clone());
    }
    turns
}

pub(super) fn content_text(value: Option<&serde_json::Value>) -> String {
    match value {
        Some(serde_json::Value::String(text)) => text.clone(),
        Some(serde_json::Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| {
                part.get("text")
                    .or_else(|| part.get("output_text"))
                    .and_then(serde_json::Value::as_str)
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

pub(super) fn tool_output_text(value: Option<&serde_json::Value>) -> String {
    let text = content_text(value);
    if !text.is_empty() {
        return text;
    }
    match value {
        None | Some(serde_json::Value::Null) => String::new(),
        Some(value) => serde_json::to_string_pretty(value).unwrap_or_default(),
    }
}

pub(super) fn excerpt(text: &str, max_chars: usize) -> String {
    let text = text.trim();
    if text.chars().count() <= max_chars {
        return text.to_owned();
    }
    let mut out = text.chars().take(max_chars).collect::<String>();
    out.push_str("...");
    out
}

pub(super) fn pretty_json_str(text: &str) -> String {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|value| serde_json::to_string_pretty(&value).ok())
        .unwrap_or_else(|| text.to_owned())
}

pub(super) fn pretty_json_value(value: Option<&serde_json::Value>) -> String {
    match value {
        Some(serde_json::Value::String(text)) => pretty_json_str(text),
        Some(value) => serde_json::to_string_pretty(value).unwrap_or_default(),
        None => String::new(),
    }
}
