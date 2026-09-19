//! Axum `Router` assembly: routes, tracing middleware, body size limits.

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::get;
use axum::Router;
use tower_http::trace::TraceLayer;

use crate::auth::AuthLimiter;
use crate::config::DefaultAccess;
use crate::store::acl::Acl;
use crate::store::users::Users;
use crate::store::Store;
use crate::topic::TopicRegistry;

pub mod bus;
pub mod params;
pub mod publish;
pub mod subscribe;

/// Shared state handed to all axum handlers.
#[derive(Clone)]
pub struct AppState {
    #[allow(dead_code)]
    pub store: Arc<Store>,
    pub topics: Arc<TopicRegistry>,
    pub users: Arc<Users>,
    pub acl: Arc<Acl>,
    pub auth_limiter: Arc<AuthLimiter>,
    /// Server-wide fallback permission applied when no explicit ACL entry
    /// matches a (principal, topic) pair (M0's `Config::default_access`,
    /// threaded through so `Acl::resolve` calls don't need to rebuild it).
    pub default_access: DefaultAccess,
}

/// Maximum request body size accepted by any handler (1 MiB for M0/M1).
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// Builds the top-level axum `Router`: `/health`, the ntfy-compatible
/// publish/subscribe surface (PLAN.md 5.1), plus tracing and
/// body-size-limit middleware. The `/bus` extension is wired in during M4.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route(
            "/:topic",
            axum::routing::post(publish::publish).put(publish::publish),
        )
        .route("/:topic/json", get(subscribe::subscribe_json))
        .route("/:topic/sse", get(subscribe::subscribe_sse))
        .route("/:topic/raw", get(subscribe::subscribe_raw))
        .route("/:topic/ws", get(subscribe::subscribe_ws))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

/// Enforces that `visitor` has (at least) `need` access to `topic`,
/// consulting the ACL (with `state.default_access` as the fallback) unless
/// the visitor is an admin, who bypasses ACL checks entirely (matches
/// ntfy's `Authorize`: `if user.Role == RoleAdmin { return nil }`).
/// Returns `403 Forbidden` on denial — bad/malformed credentials are
/// rejected earlier, in `auth::authenticate`, with `401 Unauthorized`.
pub fn require_permission(
    state: &AppState,
    visitor: &crate::auth::Visitor,
    topic: &str,
    need: crate::store::acl::Permission,
) -> Result<(), crate::error::AppError> {
    use crate::error::AppError;
    use crate::store::acl::Permission;

    if visitor.is_admin {
        return Ok(());
    }
    let granted = state
        .acl
        .resolve(visitor.username.as_deref(), topic, state.default_access)
        .map_err(|e| AppError::Internal(format!("acl lookup failed: {e}")))?;
    let allowed = match need {
        Permission::Read => granted.is_read(),
        Permission::Write => granted.is_write(),
        Permission::ReadWrite => granted.is_read() && granted.is_write(),
        Permission::DenyAll => true,
    };
    if allowed {
        Ok(())
    } else {
        Err(AppError::Forbidden(format!("no {need:?} access to topic '{topic}'")))
    }
}
