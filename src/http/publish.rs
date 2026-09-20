//! `PUT`/`POST /{topic}` publish handler.

use std::net::SocketAddr;

use axum::body::Bytes;
use axum::extract::{ConnectInfo, Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, Uri};
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;

use crate::error::AppError;
use crate::http::params;
use crate::http::AppState;
use crate::model;
use crate::model::{Enc, Envelope};
use crate::store::acl::Permission;

/// Matches ntfy's `emptyMessageBody` placeholder used when a published
/// message has no text at all.
const EMPTY_MESSAGE_PLACEHOLDER: &str = "triggered";

/// Subset of ntfy's `publishMessage` JSON shape, accepted when
/// `Content-Type: application/json` is sent to `/{topic}` (see PLAN.md 5.1;
/// this is a superset of upstream ntfy, which only accepts JSON bodies at
/// `POST /`, not `/{topic}` — documented milestone decision). `encoding`/
/// `enc` are just `Envelope` fields (PLAN.md 4.1/7, M5), so they round-trip
/// through this struct for free — no special-casing needed for the e2e
/// envelope shape beyond the shape validation applied below.
#[derive(Debug, Default, Deserialize)]
struct JsonPublishBody {
    title: Option<String>,
    message: Option<String>,
    priority: Option<serde_json::Value>,
    tags: Option<Vec<String>>,
    click: Option<String>,
    encoding: Option<String>,
    enc: Option<Enc>,
}

/// Subset of ntfy's `publishMessage` JSON shape, accepted when publishing
/// to the server root (`PUT`/`POST /`) instead of `/{topic}` — ntfy's
/// "publish as JSON" API, used by the ntfy web app itself (`Api.js`
/// `publish()`), which always PUTs to the base URL with `topic` embedded
/// in the body rather than the path. Mirrors upstream ntfy's
/// `transformBodyJSON` (server.go): the JSON `message` field becomes the
/// downstream request body (mattering only for the local-attachment-upload
/// branch, where that text doubles as file content), and everything else
/// is resolved directly from the parsed fields rather than round-tripped
/// through synthetic headers.
#[derive(Debug, Default, Deserialize)]
struct RootPublishBody {
    topic: Option<String>,
    title: Option<String>,
    message: Option<String>,
    priority: Option<serde_json::Value>,
    tags: Option<Vec<String>>,
    click: Option<String>,
    attach: Option<String>,
    filename: Option<String>,
    markdown: Option<bool>,
    encoding: Option<String>,
    enc: Option<Enc>,
}

/// Handles `PUT`/`POST /`: ntfy's "publish as JSON" endpoint. Reads
/// `topic` (required) and the rest of `RootPublishBody` from the JSON
/// body, then funnels into the same [`finish_publish`] tail as the
/// `/{topic}` handler below. See [`RootPublishBody`] for why this is a
/// separate entry point rather than a `Path`-less variant of [`publish`].
pub async fn publish_root(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
    body: Bytes,
) -> Result<impl IntoResponse, AppError> {
    let parsed: RootPublishBody =
        serde_json::from_slice(&body).map_err(|e| AppError::BadRequest(format!("invalid JSON body: {e}")))?;
    let topic = parsed
        .topic
        .filter(|t| !t.is_empty())
        .ok_or_else(|| AppError::BadRequest("topic missing from JSON body".to_string()))?;
    params::validate_topic_name(&topic)?;
    let query = params::parse_query(&uri);

    let visitor = crate::auth::authenticate(&headers, &query, addr.ip(), &state.users, &state.auth_limiter)?;
    crate::http::require_permission(&state, &visitor, &topic, Permission::Write)?;

    let rate_key = crate::auth::RateKey::for_visitor(&visitor, addr.ip());
    if !state.publish_limiter.check(rate_key) {
        return Err(AppError::TooManyRequests(
            "publish rate limit exceeded, slow down".to_string(),
        ));
    }

    let mut priority: Option<u8> = None;
    if let Some(v) = parsed.priority {
        priority = Some(parse_priority_value(&v)?);
    }
    let filename = parsed.filename.filter(|s| !s.is_empty());
    let attach_url = parsed.attach.filter(|s| !s.is_empty());
    let content_type = if parsed.markdown.unwrap_or(false) {
        Some("text/markdown".to_string())
    } else {
        None
    };
    // ntfy transforms the JSON body into a plain-text request body equal
    // to `message` before delegating to the shared publish path — this
    // matters only for the local-upload branch, where that text doubles
    // as the uploaded file's content.
    let message_text = parsed.message.filter(|s| !s.is_empty());
    let body_for_attachment = Bytes::from(message_text.clone().unwrap_or_default());

    finish_publish(
        state,
        topic,
        parsed.title,
        parsed.click,
        parsed.tags.unwrap_or_default(),
        priority,
        parsed.encoding,
        parsed.enc,
        filename,
        attach_url,
        content_type,
        message_text,
        body_for_attachment,
    )
    .await
}

/// Handles `PUT`/`POST /{topic}`: publishes a message and returns the
/// stored envelope as JSON (ntfy-style publish ack).
pub async fn publish(
    State(state): State<AppState>,
    Path(topic): Path<String>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
    body: Bytes,
) -> Result<impl IntoResponse, AppError> {
    params::validate_topic_name(&topic)?;
    let query = params::parse_query(&uri);

    let visitor = crate::auth::authenticate(&headers, &query, addr.ip(), &state.users, &state.auth_limiter)?;
    crate::http::require_permission(&state, &visitor, &topic, Permission::Write)?;

    // M6: per-visitor publish rate limit (separate from the M3 auth-failure
    // limiter above) — throttles legitimate, frequent publish traffic.
    // Checked after auth/ACL so a request that would be rejected anyway
    // doesn't consume budget, but before any body parsing.
    let rate_key = crate::auth::RateKey::for_visitor(&visitor, addr.ip());
    if !state.publish_limiter.check(rate_key) {
        return Err(AppError::TooManyRequests(
            "publish rate limit exceeded, slow down".to_string(),
        ));
    }

    let content_type_header = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let is_json = content_type_header
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .eq_ignore_ascii_case("application/json");

    let mut title = params::read_param(&headers, &query, &["x-title", "title", "t"]);
    let mut click = params::read_param(&headers, &query, &["x-click", "click"]);
    let mut tags = params::read_comma_separated_param(&headers, &query, &["x-tags", "tags", "tag", "ta"]);
    let mut priority: Option<u8> = None;
    let mut message_text: Option<String>;
    let (mut encoding, mut enc) = params::read_enc_params(&headers, &query);
    let filename = params::read_param(&headers, &query, &["x-filename", "filename", "file", "f"]);
    let attach_url = params::read_param(&headers, &query, &["x-attach", "attach", "a"]);

    if filename.is_some() {
        // Local file upload: the raw body bytes ARE the attachment
        // content, not message text — skip UTF-8/JSON decoding entirely.
        message_text = None;
    } else if is_json {
        let parsed: JsonPublishBody =
            serde_json::from_slice(&body).map_err(|e| AppError::BadRequest(format!("invalid JSON body: {e}")))?;
        if title.is_none() {
            title = parsed.title;
        }
        if click.is_none() {
            click = parsed.click;
        }
        if tags.is_empty() {
            tags = parsed.tags.unwrap_or_default();
        }
        if let Some(v) = parsed.priority {
            priority = Some(parse_priority_value(&v)?);
        }
        if encoding.is_none() {
            encoding = parsed.encoding;
        }
        if enc.is_none() {
            enc = parsed.enc;
        }
        message_text = parsed.message;
    } else {
        let text = String::from_utf8(body.to_vec())
            .map_err(|_| AppError::BadRequest("message body must be valid UTF-8".to_string()))?;
        let trimmed = text.trim();
        message_text = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        };
    }

    // X-Message/Message/M param: used when the body carries something else,
    // or as a fallback when the body was empty. Matches ntfy's alias set.
    // (For e2e, the raw plain-text body IS the ciphertext — see the
    // `else` branch above — so this fallback rarely fires for e2e in
    // practice, but is left in place for the "headers + no body" pattern.)
    if message_text.as_deref().map(str::is_empty).unwrap_or(true) {
        if let Some(m) = params::read_param(&headers, &query, &["x-message", "message", "m"]) {
            message_text = Some(m.replace("\\n", "\n"));
        }
    }

    if let Some(p) = params::read_param(&headers, &query, &["x-priority", "priority", "prio", "p"]) {
        priority = Some(params::parse_priority(&p)?);
    }

    let content_type_param = params::read_param(&headers, &query, &["content-type", "content_type"]);
    let markdown = params::read_bool_param(false, &headers, &query, &["x-markdown", "markdown", "md"]);
    let content_type = if markdown
        || content_type_param
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case("text/markdown"))
            .unwrap_or(false)
    {
        Some("text/markdown".to_string())
    } else {
        None
    };

    let message_text = message_text.filter(|s| !s.is_empty());

    finish_publish(
        state,
        topic,
        title,
        click,
        tags,
        priority,
        encoding,
        enc,
        filename,
        attach_url,
        content_type,
        message_text,
        body,
    )
    .await
}

/// Shared tail of both publish entry points ([`publish`] and
/// [`publish_root`]): e2e shape validation, attachment resolution
/// (remote-URL vs local-upload vs none), envelope construction, and
/// persistence. `body` is the raw bytes used as attachment content when
/// `filename` is set (the real HTTP request body for [`publish`]; the
/// JSON `message` field re-encoded as bytes for [`publish_root`], per
/// ntfy's own `transformBodyJSON` semantics).
#[allow(clippy::too_many_arguments)]
async fn finish_publish(
    state: AppState,
    topic: String,
    title: Option<String>,
    click: Option<String>,
    tags: Vec<String>,
    priority: Option<u8>,
    encoding: Option<String>,
    enc: Option<Enc>,
    filename: Option<String>,
    attach_url: Option<String>,
    content_type: Option<String>,
    message_text: Option<String>,
    body: Bytes,
) -> Result<Json<Envelope>, AppError> {
    // Opt-in shape validation, only when encoding == "e2e" (PLAN.md 7).
    // Never decodes/inspects message/title content beyond a size cap.
    params::validate_e2e(encoding.as_deref(), enc.as_ref(), message_text.as_deref(), title.as_deref())?;

    // Attachment handling (remote URL vs local upload vs none) — see
    // docs/plans/file-attachments.md section 5. Both branches are gated
    // identically behind `state.attachments.is_some()` for a consistent
    // mental model (recommendation adopted from the plan's open decision
    // #1), even though the remote-URL branch itself writes no bytes to
    // disk.
    let attachment: Option<model::Attachment> = if let Some(url) = attach_url {
        if state.attachments.is_none() {
            return Err(AppError::BadRequest("attachments are not enabled on this server".to_string()));
        }
        Some(model::Attachment {
            name: filename_from_url(&url),
            r#type: None,
            size: None,
            expires: None,
            url,
        })
    } else {
        None
    };

    let topic_ref = state.topics.get_or_create(&topic)?;
    let topic_name = topic.clone();
    let envelope = if let (Some(filename), None) = (&filename, &attachment) {

        // Local file upload: filename set, no attach_url (attach_url
        // takes precedence if somehow both are set — matches ntfy's
        // dispatch order in handlePublishBody).
        let attachments = state
            .attachments
            .as_ref()
            .ok_or_else(|| AppError::BadRequest("attachments are not enabled on this server".to_string()))?;

        if body.len() as u64 > attachments.file_size_limit() {
            return Err(AppError::PayloadTooLarge(
                "attachment exceeds the server's file size limit".to_string(),
            ));
        }
        if body.len() as u64 > attachments.remaining() {
            return Err(AppError::PayloadTooLarge(
                "attachment exceeds the server's remaining storage budget".to_string(),
            ));
        }

        let id = model::generate_message_id();
        let mime = crate::store::attachments::Attachments::mime_for_filename(filename);
        let expires = model::now_unix() + state.attachment_expiry.as_secs() as i64;
        let base_url = state.base_url.clone().unwrap_or_default();
        let attachment_meta = model::Attachment {
            name: filename.clone(),
            r#type: Some(mime),
            size: Some(body.len() as u64),
            expires: Some(expires),
            url: format!("{}/file/{}", base_url.trim_end_matches('/'), id),
        };

        attachments
            .write(&id, &body, attachment_meta.clone())
            .await
            .map_err(|e| AppError::Internal(format!("failed to store attachment: {e}")))?;

        let message_text = message_text.unwrap_or_else(|| format!("You received a file: {filename}"));

        topic_ref
            .publish_with_id(id, move |seq, id, time| {
                let mut env = Envelope::new_message(
                    topic_name,
                    seq,
                    id,
                    time,
                    title,
                    Some(message_text),
                    priority,
                    tags,
                    click,
                    content_type,
                    encoding,
                    enc,
                    None,
                );
                env.attachment = Some(attachment_meta);
                env
            })
            .map_err(|e| AppError::Internal(format!("failed to persist message: {e}")))?
    } else {
        let message_text = message_text.unwrap_or_else(|| EMPTY_MESSAGE_PLACEHOLDER.to_string());

        topic_ref
            .publish(move |seq, id, time| {
                let mut env = Envelope::new_message(
                    topic_name,
                    seq,
                    id,
                    time,
                    title,
                    Some(message_text),
                    priority,
                    tags,
                    click,
                    content_type,
                    encoding,
                    enc,
                    None,
                );
                env.attachment = attachment;
                env
            })
            .map_err(|e| AppError::Internal(format!("failed to persist message: {e}")))?
    };

    Ok(Json(envelope))
}

/// Best-effort filename from the last path segment of a URL, falling back
/// to a generic name — used to label remote (`attach=`) attachments for
/// display, matching ntfy's own fallback behavior.
fn filename_from_url(url: &str) -> String {
    url.rsplit('/').next().filter(|s| !s.is_empty()).unwrap_or("attachment").to_string()
}

/// Accepts either a JSON number (`1`..`5`) or a priority alias string, per
/// the same rules as the header/query `priority` param.
fn parse_priority_value(v: &serde_json::Value) -> Result<u8, AppError> {
    match v {
        serde_json::Value::Number(n) => {
            let p = n
                .as_u64()
                .ok_or_else(|| AppError::BadRequest("invalid priority".to_string()))?;

            if (1..=5).contains(&p) {
                Ok(p as u8)
            } else {
                Err(AppError::BadRequest("invalid priority".to_string()))
            }
        }
        serde_json::Value::String(s) => params::parse_priority(s),
        _ => Err(AppError::BadRequest("invalid priority".to_string())),
    }
}
