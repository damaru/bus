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

    if is_json {
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

    let message_text = message_text
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| EMPTY_MESSAGE_PLACEHOLDER.to_string());

    // Opt-in shape validation, only when encoding == "e2e" (PLAN.md 7).
    // Never decodes/inspects message/title content beyond a size cap.
    params::validate_e2e(encoding.as_deref(), enc.as_ref(), Some(&message_text), title.as_deref())?;

    let topic_ref = state.topics.get_or_create(&topic)?;
    let topic_name = topic.clone();
    let envelope = topic_ref
        .publish(move |seq, id, time| {
            Envelope::new_message(
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
            )
        })
        .map_err(|e| AppError::Internal(format!("failed to persist message: {e}")))?;

    Ok(Json(envelope))
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
