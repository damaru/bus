//! Axum `Router` assembly: routes, tracing middleware, body size limits.

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::get;
use axum::Router;
use tower_http::trace::TraceLayer;

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
