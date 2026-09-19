//! Request authentication (`Authorization: Basic|Bearer ...` / `?auth=`)
//! and the resulting [`Visitor`]. ACL *authorization* (permission lookup)
//! lives in `store::acl`; this module only resolves *who* is making the
//! request. Ports `refs/ntfy/server/server_auth.go`'s `readAuthHeader`/
//! `authenticate`/`maybeAuthenticate` family.

use std::net::IpAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::http::HeaderMap;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use dashmap::DashMap;

use crate::error::AppError;
use crate::store::users::{User, Users};

/// The authenticated (or anonymous) requester for a single request. Always
/// resolvable — there is no "authentication required" state at this layer;
/// permission enforcement happens separately via `store::acl::Acl::resolve`.
/// Matches PLAN.md section 6's "always resolves to *some* visitor"
/// principle (ported from ntfy's `maybeAuthenticate`).
#[derive(Debug, Clone, Default)]
pub struct Visitor {
    pub username: Option<String>,
    pub is_admin: bool,
}

impl Visitor {
    fn anonymous() -> Self {
        Self::default()
    }

    fn from_user(user: User) -> Self {
        Self {
            is_admin: user.is_admin(),
            username: Some(user.username),
        }
    }
}

/// Per-IP auth-failure rate limiter: a fixed-window counter (reset once
/// `WINDOW` elapses since the window started), checked *before* a
/// credential-verification attempt so repeated bad guesses don't keep
/// hitting the argon2 hasher. This is a deliberately simplified stand-in
/// for ntfy's token-bucket (`rate.Limiter`) — window-based rather than
/// continuously refilling — but the numbers are chosen to match ntfy's
/// defaults closely: `DefaultVisitorAuthFailureLimitBurst = 30` failures,
/// `DefaultVisitorAuthFailureLimitReplenish = 1 minute`.
pub struct AuthLimiter {
    failures: DashMap<IpAddr, (AtomicU32, Mutex<Instant>)>,
}

const AUTH_FAILURE_WINDOW: Duration = Duration::from_secs(60);
const AUTH_FAILURE_MAX: u32 = 30;

impl Default for AuthLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthLimiter {
    pub fn new() -> Self {
        Self { failures: DashMap::new() }
    }

    /// True if `ip` is still allowed to attempt authentication.
    fn allowed(&self, ip: IpAddr) -> bool {
        match self.failures.get(&ip) {
            None => true,
            Some(entry) => {
                let window_start = *entry.1.lock().unwrap();
                if window_start.elapsed() > AUTH_FAILURE_WINDOW {
                    true
                } else {
                    entry.0.load(Ordering::Relaxed) < AUTH_FAILURE_MAX
                }
            }
        }
    }

    /// Records a failed authentication attempt from `ip`, resetting the
    /// window if it has expired.
    fn record_failure(&self, ip: IpAddr) {
        let entry = self
            .failures
            .entry(ip)
            .or_insert_with(|| (AtomicU32::new(0), Mutex::new(Instant::now())));
        let mut window_start = entry.1.lock().unwrap();
        if window_start.elapsed() > AUTH_FAILURE_WINDOW {
            *window_start = Instant::now();
            entry.0.store(1, Ordering::Relaxed);
        } else {
            entry.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Reads the raw `Authorization`-style value for a request: the
/// `Authorization` header by default, overridden by the `?auth=`/
/// `?authorization=` query param when present (doubly-encoded:
/// `base64url(no padding)("Basic " + base64(user:pass))` or
/// `base64url("Bearer " + token)`) — this exists only so browser
/// WebSocket clients that can't set headers on the upgrade request can
/// still authenticate. Ports `readAuthHeader` verbatim, including the
/// query-overrides-header precedence.
fn read_raw_auth_value(headers: &HeaderMap, query: &std::collections::HashMap<String, String>) -> Result<String, AppError> {
    let mut value = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim()
        .to_string();

    let query_param = query.get("authorization").or_else(|| query.get("auth"));
    if let Some(qp) = query_param {
        if !qp.is_empty() {
            let decoded = URL_SAFE_NO_PAD
                .decode(qp.as_bytes())
                .map_err(|_| AppError::Unauthorized("invalid ?auth= encoding".to_string()))?;
            value = String::from_utf8(decoded)
                .map_err(|_| AppError::Unauthorized("invalid ?auth= encoding".to_string()))?
                .trim()
                .to_string();
        }
    }
    Ok(value)
}

/// True only if `value` (case-insensitively) starts with `"basic "` or
/// `"bearer "` — an empty value or an unrelated scheme (e.g. `"WebPush"`)
/// is not supported and is treated as anonymous, matching ntfy's
/// `supportedAuthHeader`.
fn supported_auth_header(value: &str) -> bool {
    let lower = value.to_lowercase();
    lower.starts_with("basic ") || lower.starts_with("bearer ")
}

/// Decodes a `Basic base64(user:pass)` value into `(username, password)`.
fn decode_basic(value: &str) -> Option<(String, String)> {
    let decoded = STANDARD.decode(value.trim()).ok()?;
    let text = String::from_utf8(decoded).ok()?;
    let (user, pass) = text.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

/// Authenticates a single request: parses Basic/Bearer credentials (from
/// the header or `?auth=`), rate-limits repeated failures per IP, and
/// resolves a [`Visitor`]. No credentials (or an unsupported scheme)
/// resolves to the anonymous visitor rather than an error — matching
/// ntfy's "always return *some* visitor" behavior; only outright malformed
/// or wrong credentials, or a rate-limited IP, produce an `Err`.
pub fn authenticate(
    headers: &HeaderMap,
    query: &std::collections::HashMap<String, String>,
    ip: IpAddr,
    users: &Users,
    limiter: &AuthLimiter,
) -> Result<Visitor, AppError> {
    let raw = read_raw_auth_value(headers, query)?;
    if raw.is_empty() || !supported_auth_header(&raw) {
        return Ok(Visitor::anonymous());
    }

    if !limiter.allowed(ip) {
        return Err(AppError::TooManyRequests(
            "too many authentication failures, try again later".to_string(),
        ));
    }

    let (scheme, value) = raw.split_once(' ').unwrap_or((raw.as_str(), ""));
    let scheme = scheme.to_lowercase();
    let value = value.trim();

    let verified = if scheme == "bearer" {
        verify_bearer(users, value)
    } else {
        // Basic base64(user:pass); an empty username means "the password
        // slot actually carries a bearer token", matching ntfy's
        // authenticateBasicAuth fallback (some clients send tokens this
        // way when they can only set one credential field).
        match decode_basic(value) {
            Some((username, password)) if username.is_empty() => verify_bearer(users, &password),
            Some((username, password)) => verify_password(users, &username, &password),
            None => Err(AppError::Unauthorized("malformed Basic auth value".to_string())),
        }
    };

    match verified {
        Ok(Some(user)) => Ok(Visitor::from_user(user)),
        Ok(None) => {
            limiter.record_failure(ip);
            Err(AppError::Unauthorized("invalid credentials".to_string()))
        }
        Err(e) => {
            limiter.record_failure(ip);
            Err(e)
        }
    }
}

fn verify_password(users: &Users, username: &str, password: &str) -> Result<Option<User>, AppError> {
    users
        .verify_password(username, password)
        .map_err(|e| AppError::Internal(format!("password verification failed: {e}")))
}

fn verify_bearer(users: &Users, token: &str) -> Result<Option<User>, AppError> {
    users
        .verify_token(token)
        .map_err(|e| AppError::Internal(format!("token verification failed: {e}")))
}
