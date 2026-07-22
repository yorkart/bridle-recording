use super::*;

pub(super) fn parse_response_sse(bytes: &[u8]) -> ParsedResponseSse {
    let mut parser = SseParser::default();
    let mut event_counts = BTreeMap::new();
    let mut events = Vec::new();
    let mut output_text = String::new();
    let mut function_calls = Vec::new();
    let mut tool_inputs = HashMap::new();
    let mut claude_tool_inputs = BTreeMap::<usize, ClaudeToolInput>::new();
    let mut completed_response = serde_json::Value::Object(serde_json::Map::new());

    for event in parser.push(bytes) {
        let data = event.data.join("\n");
        let value = serde_json::from_str::<serde_json::Value>(&data).ok();
        let event_type = value
            .as_ref()
            .and_then(|value| value.get("type"))
            .and_then(serde_json::Value::as_str)
            .or(event.event.as_deref())
            .unwrap_or("unknown")
            .to_owned();
        *event_counts.entry(event_type.clone()).or_insert(0) += 1;
        events.push(ObservedSseEvent {
            index: events.len(),
            event: event.event,
            id: event.id,
            retry: event.retry,
            event_type: event_type.clone(),
            data,
            raw: String::from_utf8_lossy(&event.raw).into_owned(),
        });

        let Some(value) = value else {
            continue;
        };

        match event_type.as_str() {
            "message_start" => {
                completed_response = value
                    .get("message")
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
            }
            "content_block_start" => {
                let index = value
                    .get("index")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|index| usize::try_from(index).ok());
                let block = value.get("content_block");
                match block
                    .and_then(|block| block.get("type"))
                    .and_then(serde_json::Value::as_str)
                {
                    Some("text") => {
                        if let Some(text) = block
                            .and_then(|block| block.get("text"))
                            .and_then(serde_json::Value::as_str)
                        {
                            output_text.push_str(text);
                        }
                    }
                    Some("tool_use") => {
                        if let (Some(index), Some(block)) = (index, block) {
                            let initial_input = block
                                .get("input")
                                .filter(|input| {
                                    !input.as_object().is_some_and(serde_json::Map::is_empty)
                                })
                                .and_then(|input| serde_json::to_string(input).ok())
                                .unwrap_or_default();
                            claude_tool_inputs.insert(
                                index,
                                ClaudeToolInput {
                                    id: block
                                        .get("id")
                                        .and_then(serde_json::Value::as_str)
                                        .unwrap_or_default()
                                        .to_owned(),
                                    name: block
                                        .get("name")
                                        .and_then(serde_json::Value::as_str)
                                        .unwrap_or("unknown")
                                        .to_owned(),
                                    input: initial_input,
                                },
                            );
                        }
                    }
                    _ => {}
                }
            }
            "content_block_delta" => {
                let delta = value.get("delta");
                match delta
                    .and_then(|delta| delta.get("type"))
                    .and_then(serde_json::Value::as_str)
                {
                    Some("text_delta") => {
                        if let Some(text) = delta
                            .and_then(|delta| delta.get("text"))
                            .and_then(serde_json::Value::as_str)
                        {
                            output_text.push_str(text);
                        }
                    }
                    Some("input_json_delta") => {
                        if let (Some(index), Some(partial_json)) = (
                            value
                                .get("index")
                                .and_then(serde_json::Value::as_u64)
                                .and_then(|index| usize::try_from(index).ok()),
                            delta
                                .and_then(|delta| delta.get("partial_json"))
                                .and_then(serde_json::Value::as_str),
                        ) {
                            if let Some(tool) = claude_tool_inputs.get_mut(&index) {
                                tool.input.push_str(partial_json);
                            }
                        }
                    }
                    _ => {}
                }
            }
            "content_block_stop" => {
                if let Some(tool) = value
                    .get("index")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|index| usize::try_from(index).ok())
                    .and_then(|index| claude_tool_inputs.remove(&index))
                {
                    function_calls.push(ObservedFunctionCall {
                        id: tool.id.clone(),
                        call_id: tool.id,
                        name: tool.name,
                        status: "completed".to_owned(),
                        arguments: pretty_json_str(&tool.input),
                        result: None,
                    });
                }
            }
            "message_delta" => {
                merge_json_object(
                    &mut completed_response,
                    value.get("delta").unwrap_or(&serde_json::Value::Null),
                );
                if let Some(usage) = value.get("usage") {
                    let completed_usage = completed_response.as_object_mut().and_then(|response| {
                        response
                            .entry("usage")
                            .or_insert_with(|| serde_json::json!({}))
                            .as_object_mut()
                    });
                    if let (Some(completed_usage), Some(usage)) =
                        (completed_usage, usage.as_object())
                    {
                        completed_usage.extend(usage.clone());
                    }
                }
            }
            "response.output_text.delta" => {
                if let Some(delta) = value.get("delta").and_then(serde_json::Value::as_str) {
                    output_text.push_str(delta);
                }
            }
            "response.function_call_arguments.done" | "response.custom_tool_call_input.done" => {
                if let (Some(item_id), Some(input)) = (
                    value.get("item_id").and_then(serde_json::Value::as_str),
                    value
                        .get("arguments")
                        .or_else(|| value.get("input"))
                        .and_then(serde_json::Value::as_str),
                ) {
                    tool_inputs.insert(item_id.to_owned(), input.to_owned());
                }
            }
            "response.output_item.done" => {
                if let Some(item) = value.get("item") {
                    if item.get("type").and_then(serde_json::Value::as_str) == Some("message") {
                        if output_text.is_empty() {
                            output_text.push_str(&content_text(item.get("content")));
                        }
                    } else if is_tool_call_type(
                        item.get("type").and_then(serde_json::Value::as_str),
                    ) {
                        let item_id = item
                            .get("id")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default();
                        let arguments = item
                            .get("arguments")
                            .or_else(|| item.get("input"))
                            .and_then(serde_json::Value::as_str)
                            .map(ToOwned::to_owned)
                            .or_else(|| tool_inputs.get(item_id).cloned())
                            .unwrap_or_default();
                        function_calls.push(ObservedFunctionCall {
                            id: item_id.to_owned(),
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
                            arguments: pretty_json_str(&arguments),
                            result: None,
                        });
                    }
                }
            }
            "response.completed" => {
                completed_response = value
                    .get("response")
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
            }
            _ => {}
        }
    }

    ParsedResponseSse {
        output_text,
        function_calls,
        completed_response,
        event_counts,
        events,
    }
}

pub(super) fn merge_json_object(target: &mut serde_json::Value, source: &serde_json::Value) {
    let (Some(target), Some(source)) = (target.as_object_mut(), source.as_object()) else {
        return;
    };
    target.extend(source.clone());
}

pub(super) fn observed_usage(response: &serde_json::Value) -> Option<serde_json::Value> {
    let mut usage = response.get("usage")?.clone();
    let usage_object = usage.as_object_mut()?;
    if !usage_object.contains_key("total_tokens") {
        let total_tokens = [
            "input_tokens",
            "output_tokens",
            "cache_creation_input_tokens",
            "cache_read_input_tokens",
        ]
        .into_iter()
        .filter_map(|key| usage_object.get(key).and_then(serde_json::Value::as_u64))
        .sum::<u64>();
        usage_object.insert("total_tokens".to_owned(), total_tokens.into());
    }
    Some(usage)
}
