use super::*;

pub async fn profiles(State(state): State<GatewayState>) -> Response {
    let mut profiles = state.profiles.keys().cloned().collect::<Vec<_>>();
    profiles.sort();
    Json(serde_json::json!({ "profiles": profiles })).into_response()
}

pub async fn testsets(State(state): State<GatewayState>) -> Response {
    match testsets_inner(&state.testsets_root, None).await {
        Ok(testsets) => Json(serde_json::json!({ "testsets": testsets })).into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

pub async fn profile_testsets(
    State(state): State<GatewayState>,
    AxumPath(profile): AxumPath<String>,
) -> Response {
    match testsets_inner(&state.testsets_root, Some(&profile)).await {
        Ok(testsets) => Json(serde_json::json!({ "testsets": testsets })).into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, err),
    }
}

pub async fn sessions(
    State(state): State<GatewayState>,
    AxumPath(profile): AxumPath<String>,
) -> Response {
    match sessions_inner(&state, &profile).await {
        Ok(sessions) => Json(serde_json::json!({ "sessions": sessions })).into_response(),
        Err(err) => api_error(StatusCode::NOT_FOUND, err),
    }
}

pub async fn session(
    State(state): State<GatewayState>,
    AxumPath((profile, session_id)): AxumPath<(String, String)>,
) -> Response {
    match session_inner(&state, &profile, &session_id).await {
        Ok(session) => Json(session).into_response(),
        Err(err) => api_error(StatusCode::NOT_FOUND, err),
    }
}

pub async fn save_testset(
    State(state): State<GatewayState>,
    AxumPath((profile, session_id)): AxumPath<(String, String)>,
    Json(request): Json<SaveTestsetRequest>,
) -> Response {
    match save_testset_inner(&state, &profile, &session_id, request).await {
        Ok(saved) => Json(saved).into_response(),
        Err(SaveTestsetError::Conflict(conflict)) => (
            StatusCode::CONFLICT,
            [(CONTENT_TYPE, "application/json")],
            serde_json::to_string(&conflict).unwrap_or_else(|_| "{}".to_owned()),
        )
            .into_response(),
        Err(SaveTestsetError::Other(err)) => api_error(StatusCode::BAD_REQUEST, err),
    }
}

pub async fn preview_testset(
    State(state): State<GatewayState>,
    AxumPath((profile, session_id)): AxumPath<(String, String)>,
    Json(request): Json<SaveTestsetRequest>,
) -> Response {
    match preview_testset_inner(&state, &profile, &session_id, &request).await {
        Ok(preview) => Json(preview).into_response(),
        Err(err) => api_error(StatusCode::BAD_REQUEST, err),
    }
}

fn api_error(status: StatusCode, err: anyhow::Error) -> Response {
    (
        status,
        [(CONTENT_TYPE, "application/json")],
        serde_json::json!({
            "error": "observability request failed",
            "detail": err.to_string()
        })
        .to_string(),
    )
        .into_response()
}
