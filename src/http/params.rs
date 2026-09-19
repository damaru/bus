//! Query/header parsing + aliasing, and content filters for subscribe
//! endpoints. Ports the alias tables from `refs/ntfy/server/server.go`
//! (`parsePublishParams`, `parseQueryFilters`, `parseSince`) and the
//! `readParam`/`readBoolParam`/`readCommaSeparatedParam` helpers from
//! `refs/ntfy/server/util.go`.

use std::collections::HashMap;

use axum::http::{HeaderMap, Uri};
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;

use crate::error::AppError;
use crate::model::{self, Enc, Envelope, Event, SinceMarker};

/// Parses a request's raw query string into a lowercased-key map. Matches
/// ntfy's `readQueryParam`, which looks up `r.URL.Query().Get(strings.ToLower(name))` —
/// i.e. alias names are matched case-insensitively against lowercase keys.
/// When a key repeats, the first occurrence wins (matches `url.Values.Get`).
pub fn parse_query(uri: &Uri) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(q) = uri.query() {
        for (k, v) in form_urlencoded::parse(q.as_bytes()) {
            map.entry(k.to_lowercase()).or_insert_with(|| v.into_owned());
        }
    }
    map
}

/// Reads the first non-empty value among `names`, checking headers before
/// query params (matches ntfy's `readParam`).
pub fn read_param(headers: &HeaderMap, query: &HashMap<String, String>, names: &[&str]) -> Option<String> {
    for name in names {
        if let Some(v) = headers.get(*name).and_then(|v| v.to_str().ok()) {
            let v = v.trim();
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    for name in names {
        if let Some(v) = query.get(&name.to_lowercase()) {
            let v = v.trim();
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// Matches ntfy's `toBool`/`isBoolValue`: only `1`/`yes`/`true` are truthy.
fn to_bool(value: &str) -> bool {
    matches!(value.to_lowercase().as_str(), "1" | "yes" | "true")
}

/// Reads a boolean param, defaulting to `default` when absent (matches
/// ntfy's `readBoolParam`).
pub fn read_bool_param(default: bool, headers: &HeaderMap, query: &HashMap<String, String>, names: &[&str]) -> bool {
    match read_param(headers, query, names) {
        Some(v) if !v.is_empty() => to_bool(&v),
        _ => default,
    }
}

/// Reads a comma-separated param, trimming each entry and dropping empties
/// (matches ntfy's `readCommaSeparatedParam`).
pub fn read_comma_separated_param(headers: &HeaderMap, query: &HashMap<String, String>, names: &[&str]) -> Vec<String> {
    match read_param(headers, query, names) {
        Some(v) => v
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        None => Vec::new(),
    }
}

/// Parses a priority alias value (`1`..`5` or `min`/`low`/`default`/`high`/
/// `max`/`urgent`), matching ntfy's `util.ParsePriority`.
pub fn parse_priority(s: &str) -> Result<u8, AppError> {
    match s.trim().to_lowercase().as_str() {
        "1" | "min" => Ok(1),
        "2" | "low" => Ok(2),
        "3" | "default" => Ok(3),
        "4" | "high" => Ok(4),
        "5" | "max" | "urgent" => Ok(5),
        other => Err(AppError::BadRequest(format!("invalid priority: {other}"))),
    }
}

/// Validates a single topic name: `[-_A-Za-z0-9]{1,64}` (matches ntfy's
/// `topicPathRegex`, sans the leading slash).
pub fn validate_topic_name(name: &str) -> Result<(), AppError> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if valid {
        Ok(())
    } else {
        Err(AppError::BadRequest(format!("invalid topic name: {name}")))
    }
}

/// Splits a (possibly comma-separated) path segment into validated topic
/// names, supporting the multi-topic subscribe syntax from PLAN.md 5.1.
pub fn split_topics(path: &str) -> Result<Vec<String>, AppError> {
    let names: Vec<String> = path.split(',').map(|s| s.trim().to_string()).collect();
    if names.is_empty() {
        return Err(AppError::BadRequest("missing topic".to_string()));
    }
    for name in &names {
        validate_topic_name(name)?;
    }
    Ok(names)
}

/// Parses `since=` into a [`SinceMarker`]. Matches ntfy's `parseSince`:
/// `all`, `latest`, `none`, a message id, a unix timestamp, or a simple
/// duration string (`12h`/`30m`/`45s`) meaning "that far back from now".
pub fn parse_since(headers: &HeaderMap, query: &HashMap<String, String>, poll: bool) -> Result<SinceMarker, AppError> {
    let since = read_param(headers, query, &["x-since", "since", "si"]);
    let since = match since {
        None => return Ok(if poll { SinceMarker::All } else { SinceMarker::None }),
        Some(s) => s,
    };
    match since.as_str() {
        "all" => return Ok(SinceMarker::All),
        "latest" => return Ok(SinceMarker::Latest),
        "none" => return Ok(SinceMarker::None),
        _ => {}
    }
    if model::is_valid_message_id(&since) {
        return Ok(SinceMarker::Id(since));
    }
    if let Ok(ts) = since.parse::<i64>() {
        return Ok(SinceMarker::Time(ts));
    }
    if let Some(secs_ago) = parse_duration_secs(&since) {
        return Ok(SinceMarker::Time(model::now_unix() - secs_ago));
    }
    Err(AppError::BadRequest(format!("invalid since parameter: {since}")))
}

/// Minimal `<n>h`/`<n>m`/`<n>s` duration parser for `since=`.
fn parse_duration_secs(s: &str) -> Option<i64> {
    let (num, mult) = if let Some(n) = s.strip_suffix('h') {
        (n, 3600)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1)
    } else {
        return None;
    };
    num.parse::<i64>().ok().map(|n| n * mult)
}

/// Content filters applied to replayed and live envelopes on subscribe
/// endpoints (`id=`, `message=`, `title=`, `priority=`, `tags=`). Only
/// `message`/`message_delete`/`message_clear` events are filtered — control
/// and lifecycle events (open/keepalive) always pass, matching ntfy's
/// `queryFilter.Pass`.
///
/// **E2E note (PLAN.md section 7):** `message=`/`title=` are plain string
/// equality checks against whatever is in the envelope's `message`/`title`
/// fields. When `encoding == "e2e"`, those fields hold base64 ciphertext,
/// so a plaintext filter value will (almost) never equal the ciphertext —
/// these filters are simply **not meaningful** on e2e-encoded messages.
/// This isn't a bug to fix: the server can't know the plaintext (it's a
/// blind relay), so "no match" is the only honest outcome. No special-case
/// code is needed here; it falls out of the existing plain-string-compare
/// logic below.
#[derive(Debug, Default, Clone)]
pub struct QueryFilter {
    pub id: Option<String>,
    pub message: Option<String>,
    pub title: Option<String>,
    pub tags: Vec<String>,
    pub priority: Vec<u8>,
}

impl QueryFilter {
    pub fn pass(&self, env: &Envelope) -> bool {
        if !matches!(env.event, Event::Message | Event::MessageDelete | Event::MessageClear) {
            return true;
        }
        if let Some(id) = &self.id {
            if &env.id != id {
                return false;
            }
        }
        if let Some(message) = &self.message {
            if env.message.as_deref() != Some(message.as_str()) {
                return false;
            }
        }
        if let Some(title) = &self.title {
            if env.title.as_deref() != Some(title.as_str()) {
                return false;
            }
        }
        // Unset priority (None) is equivalent to "default" (3) for filtering,
        // matching ntfy's queryFilter.Pass.
        let effective_priority = env.priority.unwrap_or(3);
        if !self.priority.is_empty() && !self.priority.contains(&effective_priority) {
            return false;
        }
        if !self.tags.is_empty() && !self.tags.iter().all(|t| env.tags.contains(t)) {
            return false;
        }
        true
    }
}

/// Parses the `id=`/`message=`/`title=`/`tags=`/`priority=` content filters
/// (matches ntfy's `parseQueryFilters`).
pub fn parse_query_filters(headers: &HeaderMap, query: &HashMap<String, String>) -> Result<QueryFilter, AppError> {
    let id = read_param(headers, query, &["x-id", "id"]);
    let message = read_param(headers, query, &["x-message", "message", "m"]);
    let title = read_param(headers, query, &["x-title", "title", "t"]);
    let tags = read_comma_separated_param(headers, query, &["x-tags", "tags", "tag", "ta"]);
    let mut priority = Vec::new();
    for p in read_comma_separated_param(headers, query, &["x-priority", "priority", "prio", "p"]) {
        priority.push(parse_priority(&p)?);
    }
    Ok(QueryFilter {
        id,
        message,
        title,
        tags,
        priority,
    })
}

/// Reads the `encoding`/`enc.{alg,kid,nonce}` header/query aliases for a
/// publish request (PLAN.md section 4.1). Returns `(encoding, enc)` — `enc`
/// is only `Some` if at least one of alg/kid/nonce was supplied via
/// header/query (an incomplete triple is still returned as `Some` here so
/// [`validate_e2e`] can produce a precise "which field is missing" error;
/// this function itself does no validation).
pub fn read_enc_params(headers: &HeaderMap, query: &HashMap<String, String>) -> (Option<String>, Option<Enc>) {
    let encoding = read_param(headers, query, &["x-encoding", "encoding"]);
    let alg = read_param(headers, query, &["x-enc-alg", "enc-alg"]);
    let kid = read_param(headers, query, &["x-enc-kid", "enc-kid", "x-enc-key-id", "enc-key-id"]);
    let nonce = read_param(headers, query, &["x-enc-nonce", "enc-nonce"]);
    let enc = if alg.is_some() || kid.is_some() || nonce.is_some() {
        Some(Enc {
            alg: alg.unwrap_or_default(),
            kid: kid.unwrap_or_default(),
            nonce: nonce.unwrap_or_default(),
        })
    } else {
        None
    };
    (encoding, enc)
}

/// Max length (bytes) of `enc.alg`/`enc.kid`. These are short, opaque,
/// client-defined identifiers (e.g. `"xchacha20poly1305"`, `"topic-key-v1"`)
/// — 128 bytes is generous headroom for any real algorithm name or key-id
/// scheme while still bounding abuse. PLAN.md section 7 asks for "length
/// limits" but doesn't pin an exact number, so this is a documented
/// milestone choice.
pub const E2E_ALG_MAX_LEN: usize = 128;
pub const E2E_KID_MAX_LEN: usize = 128;

/// Max *decoded* length (bytes) of `enc.nonce`. Real AEAD nonces are tiny
/// (12 bytes for AES-GCM, 24 for XChaCha20-Poly1305); 256 bytes is over 10x
/// headroom for any conceivable algorithm's nonce while still rejecting
/// abuse (e.g. someone stuffing large data into the "nonce" field).
pub const E2E_NONCE_MAX_DECODED_LEN: usize = 256;

/// Max length (bytes, as transmitted — i.e. base64 text length, not
/// decoded) of an e2e-encoded `message`/`title` field. Chosen per the
/// milestone's own suggestion (documented in PLAN.md M5 guidance as "e.g.
/// 256KB"): comfortably larger than ntfy's plain-message default of 4096
/// bytes (`refs/ntfy/server/config.go`'s `DefaultMessageSizeLimit`, chosen
/// there to fit FCM/APNS push payloads — a constraint this project doesn't
/// have, since it has no push-forwarding), while still well under the
/// outer 1 MiB `DefaultBodyLimit` request-body cap from `http::mod`. This
/// is a per-message content cap, not a request-size cap.
pub const E2E_PAYLOAD_MAX_LEN: usize = 256 * 1024;

/// Validates the **shape** of an e2e envelope — PLAN.md section 7: "Server
/// validates only shape (base64-decodable, nonce/kid length limits, overall
/// payload size cap) and treats the ciphertext as an opaque blob". Never
/// decodes or otherwise inspects `message`/`title`'s ciphertext content.
///
/// A complete no-op (returns `Ok(())` immediately) unless `encoding` is
/// (case-insensitively) `"e2e"` — this is opt-in per-message, not a
/// server-wide mode, so plain-text publishes are never affected.
///
/// Shared by both the HTTP publish path (`http::publish`) and the bus
/// extension's client message-frame path (`http::bus`), per PLAN.md's
/// implication that e2e isn't HTTP-only.
pub fn validate_e2e(encoding: Option<&str>, enc: Option<&Enc>, message: Option<&str>, title: Option<&str>) -> Result<(), AppError> {
    let is_e2e = encoding.map(|e| e.eq_ignore_ascii_case("e2e")).unwrap_or(false);
    if !is_e2e {
        return Ok(());
    }

    let enc = enc.ok_or_else(|| {
        AppError::BadRequest("encoding=e2e requires an 'enc' object with non-empty alg/kid/nonce".to_string())
    })?;
    if enc.alg.is_empty() || enc.kid.is_empty() || enc.nonce.is_empty() {
        return Err(AppError::BadRequest(
            "encoding=e2e requires enc.alg, enc.kid, and enc.nonce to all be non-empty".to_string(),
        ));
    }
    if enc.alg.len() > E2E_ALG_MAX_LEN {
        return Err(AppError::BadRequest(format!("enc.alg exceeds {E2E_ALG_MAX_LEN} bytes")));
    }
    if enc.kid.len() > E2E_KID_MAX_LEN {
        return Err(AppError::BadRequest(format!("enc.kid exceeds {E2E_KID_MAX_LEN} bytes")));
    }

    // Only shape-checked: must decode as *some* common base64 variant
    // (standard or URL-safe, padded or not — the client's choice, PLAN.md
    // doesn't mandate one), and the decoded byte length must be sane. The
    // decoded bytes themselves are discarded immediately; the server never
    // uses the nonce for anything.
    let decoded_len = STANDARD
        .decode(enc.nonce.as_bytes())
        .or_else(|_| URL_SAFE.decode(enc.nonce.as_bytes()))
        .or_else(|_| STANDARD_NO_PAD.decode(enc.nonce.as_bytes()))
        .or_else(|_| URL_SAFE_NO_PAD.decode(enc.nonce.as_bytes()))
        .map_err(|_| AppError::BadRequest("enc.nonce is not valid base64".to_string()))?
        .len();
    if decoded_len > E2E_NONCE_MAX_DECODED_LEN {
        return Err(AppError::BadRequest(format!(
            "enc.nonce decodes to more than {E2E_NONCE_MAX_DECODED_LEN} bytes"
        )));
    }

    // Overall payload size cap only — content is never decoded/validated
    // (PLAN.md 7: "treats the ciphertext as an opaque blob").
    if let Some(msg) = message {
        if msg.len() > E2E_PAYLOAD_MAX_LEN {
            return Err(AppError::BadRequest(format!("e2e message exceeds {E2E_PAYLOAD_MAX_LEN} bytes")));
        }
    }
    if let Some(t) = title {
        if t.len() > E2E_PAYLOAD_MAX_LEN {
            return Err(AppError::BadRequest(format!("e2e title exceeds {E2E_PAYLOAD_MAX_LEN} bytes")));
        }
    }

    Ok(())
}
