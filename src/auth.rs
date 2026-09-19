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
///
/// Built on [`WindowCounter`], the same fixed-window primitive [`PublishLimiter`]
/// (M6) uses for the separate publish-rate limit — this only *records* on
/// an actual auth failure (unlike `PublishLimiter`, which records every
/// attempt), so successful logins never count against the budget.
pub struct AuthLimiter {
    counter: WindowCounter<IpAddr>,
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
        Self { counter: WindowCounter::new(AUTH_FAILURE_WINDOW) }
    }

    /// True if `ip` is still allowed to attempt authentication.
    fn allowed(&self, ip: IpAddr) -> bool {
        self.counter.peek(&ip) < AUTH_FAILURE_MAX
    }

    /// Records a failed authentication attempt from `ip`, resetting the
    /// window if it has expired.
    fn record_failure(&self, ip: IpAddr) {
        self.counter.increment(&ip);
    }
}

/// A rate-limit key: per-authenticated-user when available, falling back
/// to per-IP for anonymous requests (M6 publish rate limiter). Distinct
/// user accounts sharing an IP (e.g. behind NAT) don't share a budget;
/// anonymous requests from different IPs don't share a budget either.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RateKey {
    User(String),
    Ip(IpAddr),
}

impl RateKey {
    /// Builds the rate-limit key for a request: the authenticated
    /// username if there is one, else the client's IP.
    pub fn for_visitor(visitor: &Visitor, ip: IpAddr) -> Self {
        match &visitor.username {
            Some(username) => RateKey::User(username.clone()),
            None => RateKey::Ip(ip),
        }
    }
}

/// Per-visitor publish rate limiter (M6) — separate from [`AuthLimiter`]
/// (which only throttles bad-credential *attempts*): this throttles
/// legitimate, frequent publish requests, so a single chatty client can't
/// starve a topic or the retention sweep. Fixed 60-second window; `max` is
/// `Config::publish_rate_limit` (messages per window), configurable via
/// `bus serve --publish-rate-limit`.
pub struct PublishLimiter {
    counter: WindowCounter<RateKey>,
    max: u32,
}

const PUBLISH_RATE_WINDOW: Duration = Duration::from_secs(60);

impl PublishLimiter {
    pub fn new(max: u32) -> Self {
        Self { counter: WindowCounter::new(PUBLISH_RATE_WINDOW), max }
    }

    /// Records one publish attempt for `key` and returns `true` if it's
    /// still within budget (every attempt counts, unlike `AuthLimiter`
    /// which only counts failures — a successful publish is exactly the
    /// kind of traffic this limiter exists to throttle).
    pub fn check(&self, key: RateKey) -> bool {
        self.counter.increment(&key) <= self.max
    }
}

/// Generic fixed-window request counter, keyed by `K` (an IP address, a
/// username, ...). Shared building block for [`AuthLimiter`] and
/// [`PublishLimiter`]: each key gets its own window that resets once
/// `window` has elapsed since it was last (re)started. This is a
/// deliberately simple stand-in for a true token bucket — see
/// [`AuthLimiter`]'s doc comment for the tradeoff — reused here rather than
/// duplicated per PLAN.md M6's "reuse/extend that pattern" guidance.
struct WindowCounter<K: Eq + std::hash::Hash + Clone> {
    entries: DashMap<K, (AtomicU32, Mutex<Instant>)>,
    window: Duration,
}

impl<K: Eq + std::hash::Hash + Clone> WindowCounter<K> {
    fn new(window: Duration) -> Self {
        Self { entries: DashMap::new(), window }
    }

    /// Current count within `key`'s window, without incrementing (`0` if
    /// the window has expired or `key` was never seen).
    fn peek(&self, key: &K) -> u32 {
        match self.entries.get(key) {
            None => 0,
            Some(entry) => {
                if entry.1.lock().unwrap().elapsed() > self.window {
                    0
                } else {
                    entry.0.load(Ordering::Relaxed)
                }
            }
        }
    }

    /// Increments `key`'s count (resetting the window first if it has
    /// expired) and returns the count *after* incrementing.
    fn increment(&self, key: &K) -> u32 {
        let entry = self
            .entries
            .entry(key.clone())
            .or_insert_with(|| (AtomicU32::new(0), Mutex::new(Instant::now())));
        let mut window_start = entry.1.lock().unwrap();
        if window_start.elapsed() > self.window {
            *window_start = Instant::now();
            entry.0.store(1, Ordering::Relaxed);
            1
        } else {
            entry.0.fetch_add(1, Ordering::Relaxed) + 1
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
