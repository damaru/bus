//! `GET`/`HEAD /file/:id` attachment download handler.
//!
//! Deliberately has **no ACL check**: matches upstream ntfy, where an
//! attachment download is protected only by the unguessable 12-character
//! id, not by re-checking the topic's read permission (the download URL
//! carries no topic context at all). This is a documented, deliberate
//! parity choice inherited from ntfy, not a bus-specific weakening — see
//! docs/API.md.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::IntoResponse;

use crate::error::AppError;
use crate::http::AppState;
use crate::model;

/// Handles both `GET` and `HEAD /file/:id` (optionally with a trailing
/// `.ext` the client appended for display purposes — ignored for lookup,
/// matching ntfy's `fileRegex` behavior).
pub async fn download(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
    method: Method,
) -> Result<impl IntoResponse, AppError> {
    let id = raw_id.split('.').next().unwrap_or(&raw_id);

    if !model::is_valid_message_id(id) {
        return Err(AppError::NotFound("attachment not found".to_string()));
    }

    let attachments = state
        .attachments
        .as_ref()
        .ok_or_else(|| AppError::NotFound("attachment not found".to_string()))?;

    let meta = attachments
        .get_meta(id)
        .map_err(|e| AppError::Internal(format!("attachment metadata lookup failed: {e}")))?
        .ok_or_else(|| AppError::NotFound("attachment not found".to_string()))?;

    if meta.expires.is_some_and(|e| e < model::now_unix()) {
        return Err(AppError::NotFound("attachment not found".to_string()));
    }

    let mut headers = HeaderMap::new();

    let content_type = meta.r#type.clone().unwrap_or_else(|| "application/octet-stream".to_string());
    if let Ok(v) = HeaderValue::from_str(&content_type) {
        headers.insert(axum::http::header::CONTENT_TYPE, v);
    }

    let disposition = format!("attachment; filename=\"{}\"", meta.name.replace('"', "'"));
    if let Ok(v) = HeaderValue::from_str(&disposition) {
        headers.insert(axum::http::header::CONTENT_DISPOSITION, v);
    }

    if method == Method::HEAD {
        if let Some(size) = meta.size {
            if let Ok(v) = HeaderValue::from_str(&size.to_string()) {
                headers.insert(axum::http::header::CONTENT_LENGTH, v);
            }
        }
        return Ok((StatusCode::OK, headers, axum::body::Bytes::new()));
    }

    let bytes = match attachments.read(id).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(AppError::NotFound("attachment not found".to_string()))
        }
        Err(e) => return Err(AppError::Internal(format!("reading attachment: {e}"))),
    };

    if let Ok(v) = HeaderValue::from_str(&bytes.len().to_string()) {
        headers.insert(axum::http::header::CONTENT_LENGTH, v);
    }

    Ok((StatusCode::OK, headers, axum::body::Bytes::from(bytes)))
}
