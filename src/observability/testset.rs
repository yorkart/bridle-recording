use super::*;

mod export;
mod transform;

pub(super) use export::*;
pub(super) use transform::*;

pub(super) async fn save_testset_inner(
    state: &GatewayState,
    profile: &str,
    session_id: &str,
    request: SaveTestsetRequest,
) -> Result<SavedTestset, SaveTestsetError> {
    let profile_config = state
        .profiles
        .get(profile)
        .with_context(|| format!("unknown profile: {profile}"))?;
    let source_dir = profile_config.home_dir.join("recordings").join(session_id);
    if !fs::try_exists(&source_dir).await? {
        return Err(anyhow!("recording session not found: {}", source_dir.display()).into());
    }

    let observed = session_inner(state, profile, session_id).await?;
    let plan = build_testset_export_plan(&observed, &request)?;
    let first_user_input = plan.first_user_input.clone();
    let user_inputs = plan.user_inputs.clone();
    let user_input_sha256 = sha256_hex(first_user_input.as_bytes());
    let testset_dir = state.testsets_root.join(profile).join(&user_input_sha256);
    let raw_dir = testset_dir.join("raw").join(session_id);

    if fs::try_exists(&testset_dir).await? && !request.replace {
        return Err(SaveTestsetError::Conflict(SaveTestsetConflict {
            error: "testset already exists".to_owned(),
            replace_required: true,
            profile: profile.to_owned(),
            session_id: session_id.to_owned(),
            first_user_input,
            user_input_sha256,
            testset_path: testset_dir.display().to_string(),
        }));
    }

    let temp_dir = state
        .testsets_root
        .join(profile)
        .join(format!(".{user_input_sha256}.tmp"));
    if fs::try_exists(&temp_dir).await? {
        fs::remove_dir_all(&temp_dir).await?;
    }
    fs::create_dir_all(&temp_dir).await?;
    export_testset_session(
        &source_dir,
        &temp_dir.join("raw").join(session_id),
        &plan,
        &request,
    )
    .await?;

    let manifest = TestsetManifest {
        version: 2,
        profile: profile.to_owned(),
        source_session_id: session_id.to_owned(),
        first_user_input: first_user_input.clone(),
        user_inputs,
        user_input_sha256: user_input_sha256.clone(),
        saved_at: crate::util::now_rfc3339(),
        source_recording_path: source_dir.display().to_string(),
        raw_recording_path: format!("raw/{session_id}"),
        export: Some(TestsetExportManifest {
            selected_requests: plan
                .requests
                .iter()
                .map(|selected| selected.source_index.clone())
                .collect(),
            redact_sensitive_headers: request.redact_sensitive_headers,
            sensitive_value_count: request.sensitive_values().len(),
            remove: request.remove.clone(),
        }),
    };
    write_json_pretty(temp_dir.join("testset.json"), &manifest).await?;

    if fs::try_exists(&testset_dir).await? {
        fs::remove_dir_all(&testset_dir).await?;
    }
    fs::rename(&temp_dir, &testset_dir).await?;

    Ok(SavedTestset {
        status: if request.replace { "replaced" } else { "saved" }.to_owned(),
        profile: profile.to_owned(),
        session_id: session_id.to_owned(),
        first_user_input,
        user_input_sha256,
        testset_path: testset_dir.display().to_string(),
        raw_path: raw_dir.display().to_string(),
        selected_requests: plan.requests.len(),
    })
}

pub(super) async fn testsets_inner(
    testsets_dir: &Path,
    profile_filter: Option<&str>,
) -> anyhow::Result<Vec<TestsetSummary>> {
    let mut out = Vec::new();
    let mut profile_entries = match fs::read_dir(&testsets_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(err) => return Err(err).with_context(|| format!("read {}", testsets_dir.display())),
    };

    while let Some(profile_entry) = profile_entries.next_entry().await? {
        if !profile_entry.file_type().await?.is_dir() {
            continue;
        }
        let Some(profile) = profile_entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        if profile_filter.is_some_and(|filter| filter != profile) {
            continue;
        }

        let mut testset_entries = fs::read_dir(profile_entry.path())
            .await
            .with_context(|| format!("read testsets for profile {profile}"))?;
        while let Some(testset_entry) = testset_entries.next_entry().await? {
            if !testset_entry.file_type().await?.is_dir() {
                continue;
            }
            let Some(id) = testset_entry.file_name().to_str().map(ToOwned::to_owned) else {
                continue;
            };
            if id.starts_with('.') {
                continue;
            }
            let manifest_path = testset_entry.path().join("testset.json");
            let manifest = read_json::<TestsetManifest>(&manifest_path)
                .await
                .with_context(|| format!("load testset manifest {}", manifest_path.display()))?;
            let user_inputs = if manifest.user_inputs.is_empty() {
                vec![manifest.first_user_input.clone()]
            } else {
                manifest.user_inputs.clone()
            };
            out.push(TestsetSummary {
                profile: manifest.profile,
                id,
                source_session_id: manifest.source_session_id,
                first_user_input: manifest.first_user_input,
                user_inputs,
                user_input_sha256: manifest.user_input_sha256,
                saved_at: manifest.saved_at,
                source_recording_path: manifest.source_recording_path,
                raw_recording_path: manifest.raw_recording_path,
                testset_path: testset_entry.path().display().to_string(),
                export: manifest.export,
            });
        }
    }

    out.sort_by(|left, right| {
        left.profile
            .cmp(&right.profile)
            .then_with(|| left.first_user_input.cmp(&right.first_user_input))
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(out)
}

pub(super) async fn preview_testset_inner(
    state: &GatewayState,
    profile: &str,
    session_id: &str,
    request: &SaveTestsetRequest,
) -> anyhow::Result<TestsetPreview> {
    let observed = session_inner(state, profile, session_id).await?;
    let plan = build_testset_export_plan(&observed, request)?;
    let provider = ObservabilityProvider::from_profile(profile);
    Ok(TestsetPreview {
        profile: profile.to_owned(),
        session_id: session_id.to_owned(),
        first_user_input: plan.first_user_input,
        user_inputs: plan.user_inputs,
        source_request_count: observed.requests.len(),
        selected_request_count: plan.requests.len(),
        removed_request_count: observed.requests.len().saturating_sub(plan.requests.len()),
        redact_sensitive_headers: request.redact_sensitive_headers,
        sensitive_value_count: request.sensitive_values().len(),
        remove: request.remove.clone(),
        requests: plan
            .requests
            .iter()
            .map(|selected| {
                let call = observed
                    .requests
                    .iter()
                    .find(|call| call.index == selected.source_index)
                    .expect("export plan only contains observed requests");
                TestsetPreviewRequest {
                    source_index: selected.source_index.clone(),
                    export_index: selected.export_index.clone(),
                    request_kind: call.request_kind,
                    protocol: call.protocol.clone(),
                    method: call.method.clone(),
                    path: call.path.clone(),
                    prompt_block_types: provider
                        .prompt_blocks(&selected.request_body)
                        .into_iter()
                        .map(|block| block.block_type)
                        .collect(),
                    tool_definitions: tool_definitions(&selected.request_body).len(),
                    sse_events: call
                        .sse_events
                        .iter()
                        .filter(|event| !request.remove.tools || !is_tool_sse_event(&event.data))
                        .count(),
                    websocket_frames: call
                        .websocket_frames
                        .iter()
                        .filter(|frame| !request.remove.tools || !is_tool_websocket_frame(frame))
                        .count(),
                    request_body: selected.request_body.clone(),
                }
            })
            .collect(),
    })
}

pub(super) fn build_testset_export_plan(
    observed: &ObservedSession,
    request: &SaveTestsetRequest,
) -> anyhow::Result<TestsetExportPlan> {
    let requested = request
        .selected_requests
        .as_ref()
        .map(|indices| indices.iter().map(String::as_str).collect::<HashSet<_>>());
    if requested.as_ref().is_some_and(HashSet::is_empty) {
        return Err(anyhow!(
            "at least one request/response pair must be selected"
        ));
    }
    if let Some(requested) = requested.as_ref() {
        let available = observed
            .requests
            .iter()
            .map(|call| call.index.as_str())
            .collect::<HashSet<_>>();
        let mut unknown = requested
            .difference(&available)
            .copied()
            .collect::<Vec<_>>();
        unknown.sort_unstable();
        if !unknown.is_empty() {
            return Err(anyhow!(
                "unknown selected request indices: {}",
                unknown.join(", ")
            ));
        }
    }

    let sensitive_values = request.sensitive_values();
    let provider = ObservabilityProvider::from_profile(&observed.profile);
    let mut requests = Vec::new();
    let mut user_inputs = Vec::new();
    for call in &observed.requests {
        if requested
            .as_ref()
            .is_some_and(|requested| !requested.contains(call.index.as_str()))
        {
            continue;
        }
        let mut request_body = call.request_body.clone();
        trim_request_body(&mut request_body, &request.remove);
        redact_json_value(&mut request_body, &sensitive_values);
        append_conversation_user_inputs(
            &mut user_inputs,
            provider,
            call.request_kind,
            &request_body,
        );
        requests.push(TestsetExportRequest {
            source_index: call.index.clone(),
            export_index: format!("{:06}", requests.len()),
            request_body,
        });
    }
    if requests.is_empty() {
        return Err(anyhow!(
            "at least one request/response pair must be selected"
        ));
    }
    let first_user_input = user_inputs
        .first()
        .cloned()
        .unwrap_or_else(|| format!("session:{}", observed.session_id));
    Ok(TestsetExportPlan {
        requests,
        first_user_input,
        user_inputs,
    })
}

pub(super) fn append_conversation_user_inputs(
    user_inputs: &mut Vec<String>,
    provider: ObservabilityProvider,
    request_kind: ObservedRequestKind,
    request_body: &serde_json::Value,
) {
    if request_kind != ObservedRequestKind::Conversation {
        return;
    }
    for input in provider.visible_user_messages(&provider.prompt_blocks(request_body)) {
        let input = input.trim();
        if !input.is_empty() && !user_inputs.iter().any(|existing| existing == input) {
            user_inputs.push(input.to_owned());
        }
    }
}
