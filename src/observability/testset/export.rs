use super::*;

pub(in crate::observability) async fn export_testset_session(
    source: &Path,
    destination: &Path,
    plan: &TestsetExportPlan,
    request: &SaveTestsetRequest,
) -> anyhow::Result<()> {
    fs::create_dir_all(destination.join("requests"))
        .await
        .with_context(|| format!("create {}", destination.display()))?;
    let sensitive_values = request.sensitive_values();
    let source_manifest = source.join("manifest.json");
    if fs::try_exists(&source_manifest).await? {
        let mut manifest = read_json::<serde_json::Value>(&source_manifest).await?;
        if let Some(manifest) = manifest.as_object_mut() {
            manifest.insert(
                "request_count".to_owned(),
                serde_json::json!(plan.requests.len()),
            );
        }
        redact_json_value(&mut manifest, &sensitive_values);
        write_json_pretty(destination.join("manifest.json"), &manifest).await?;
    }
    for selected in &plan.requests {
        export_testset_request(
            &source.join("requests").join(&selected.source_index),
            &destination.join("requests").join(&selected.export_index),
            selected,
            request,
        )
        .await?;
    }
    Ok(())
}

pub(in crate::observability) async fn export_testset_request(
    source: &Path,
    destination: &Path,
    selected: &TestsetExportRequest,
    request: &SaveTestsetRequest,
) -> anyhow::Result<()> {
    fs::create_dir_all(destination)
        .await
        .with_context(|| format!("create {}", destination.display()))?;
    let sensitive_values = request.sensitive_values();
    let source_request_body = fs::read(source.join("request_body.raw"))
        .await
        .with_context(|| format!("read request body in {}", source.display()))?;
    let request_body = transform_request_body(&source_request_body, request)?;
    let response_sse = transform_response_sse(&source.join("response_sse.raw"), request).await?;
    let response_body = transform_response_body(&source.join("response_body.raw"), request).await?;
    let websocket_frames =
        transform_websocket_frames(&source.join("websocket_frames.jsonl"), request).await?;
    let response_bytes = response_sse
        .as_ref()
        .or(response_body.as_ref())
        .map(Vec::len);

    let mut entries = fs::read_dir(source)
        .await
        .with_context(|| format!("read {}", source.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        let destination_path = destination.join(&name);
        match name_str {
            "request_match.json" | "response_rewrite.json" => {}
            "request_body.raw" => write_bytes_file(&destination_path, &request_body).await?,
            "response_sse.raw" => {
                if let Some(bytes) = response_sse.as_ref() {
                    write_bytes_file(&destination_path, bytes).await?;
                }
            }
            "response_body.raw" => {
                if let Some(bytes) = response_body.as_ref() {
                    write_bytes_file(&destination_path, bytes).await?;
                }
            }
            "websocket_frames.jsonl" => {
                if let Some(bytes) = websocket_frames.as_ref() {
                    write_bytes_file(&destination_path, bytes).await?;
                }
            }
            "request_meta.json" => {
                let mut meta = read_json::<serde_json::Value>(&entry.path()).await?;
                if let Some(meta) = meta.as_object_mut() {
                    meta.insert(
                        "index".to_owned(),
                        serde_json::json!(selected.export_index.parse::<u64>()?),
                    );
                    meta.insert(
                        "request_body_bytes".to_owned(),
                        serde_json::json!(request_body.len()),
                    );
                }
                redact_json_value(&mut meta, &sensitive_values);
                write_json_pretty(destination_path, &meta).await?;
            }
            "response_meta.json" => {
                let mut meta = read_json::<serde_json::Value>(&entry.path()).await?;
                if let Some(meta) = meta.as_object_mut() {
                    if let Some(response_bytes) = response_bytes {
                        meta.insert(
                            "response_body_bytes".to_owned(),
                            serde_json::json!(response_bytes),
                        );
                    }
                    if let Some(bytes) = response_sse.as_ref() {
                        meta.insert(
                            "sse_event_count".to_owned(),
                            serde_json::json!(parse_response_sse(bytes).events.len()),
                        );
                    }
                }
                redact_json_value(&mut meta, &sensitive_values);
                write_json_pretty(destination_path, &meta).await?;
            }
            "request_headers.json" => {
                export_header_file(
                    &entry.path(),
                    &destination_path,
                    REQUEST_TESTSET_HEADER_ALLOWLIST,
                    request,
                    Some(request_body.len()),
                )
                .await?;
            }
            "response_headers.json" => {
                export_header_file(
                    &entry.path(),
                    &destination_path,
                    RESPONSE_TESTSET_HEADER_ALLOWLIST,
                    request,
                    response_bytes,
                )
                .await?;
            }
            "websocket_response_headers.json" => {
                export_header_file(
                    &entry.path(),
                    &destination_path,
                    RESPONSE_TESTSET_HEADER_ALLOWLIST,
                    request,
                    None,
                )
                .await?;
            }
            "websocket_meta.json" => {
                let mut meta = read_json::<serde_json::Value>(&entry.path()).await?;
                if let Some(frames) = websocket_frames.as_deref() {
                    let (client_to_upstream, upstream_to_client) = websocket_frame_counts(frames);
                    if let Some(meta) = meta.as_object_mut() {
                        meta.insert(
                            "client_to_upstream_frames".to_owned(),
                            serde_json::json!(client_to_upstream),
                        );
                        meta.insert(
                            "upstream_to_client_frames".to_owned(),
                            serde_json::json!(upstream_to_client),
                        );
                    }
                }
                redact_json_value(&mut meta, &sensitive_values);
                write_json_pretty(destination_path, &meta).await?;
            }
            _ => {
                let bytes = fs::read(entry.path()).await?;
                let bytes = redact_bytes(&bytes, &sensitive_values);
                write_bytes_file(&destination_path, &bytes).await?;
            }
        }
    }
    Ok(())
}

pub(in crate::observability) async fn export_header_file(
    source: &Path,
    destination: &Path,
    allowlist: &[&str],
    request: &SaveTestsetRequest,
    content_length: Option<usize>,
) -> anyhow::Result<()> {
    let mut records = read_json::<Vec<HeaderRecord>>(source).await?;
    if request.redact_sensitive_headers {
        redact_testset_header_records(&mut records, allowlist);
    }
    let sensitive_values = request.sensitive_values();
    for record in &mut records {
        match &mut record.value {
            HeaderValueRecord::Text { value } | HeaderValueRecord::BinaryBase64 { value } => {
                *value = redact_text(value, &sensitive_values);
            }
        }
    }
    if let Some(content_length) = content_length {
        if let Some(record) = records
            .iter_mut()
            .find(|record| record.name.eq_ignore_ascii_case("content-length"))
        {
            record.value = HeaderValueRecord::Text {
                value: content_length.to_string(),
            };
        }
    }
    write_json_pretty(destination.to_path_buf(), &records).await
}

pub(in crate::observability) fn redact_testset_header_records(
    records: &mut [HeaderRecord],
    allowlist: &[&str],
) {
    for record in records {
        if allowlist
            .iter()
            .any(|allowed| record.name.eq_ignore_ascii_case(allowed))
        {
            continue;
        }
        match &mut record.value {
            HeaderValueRecord::Text { value } | HeaderValueRecord::BinaryBase64 { value } => {
                REDACTED_TESTSET_HEADER_VALUE.clone_into(value);
            }
        }
    }
}

pub(in crate::observability) async fn write_bytes_file(
    path: &Path,
    bytes: &[u8],
) -> anyhow::Result<()> {
    let mut output = fs::File::create(path)
        .await
        .with_context(|| format!("create {}", path.display()))?;
    output
        .write_all(bytes)
        .await
        .with_context(|| format!("write {}", path.display()))?;
    output
        .flush()
        .await
        .with_context(|| format!("flush {}", path.display()))
}

pub(in crate::observability) fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
