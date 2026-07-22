use super::*;

const OBSERVABILITY_HTML: &str = include_str!("ui.html");

pub async fn ui() -> Response {
    (
        [(CONTENT_TYPE, "text/html; charset=utf-8")],
        OBSERVABILITY_HTML,
    )
        .into_response()
}
