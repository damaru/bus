//! Axum `Router` assembly: routes, tracing middleware, body size limits.

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::get;
use axum::Router;
use tower_http::trace::TraceLayer;

use crate::store::Store;

pub mod bus;
pub mod params;
pub mod publish;
pub mod subscribe;

/// Shared state handed to all axum handlers.
#[derive(Clone)]
pub struct AppState {
    #[allow(dead_code)]
    pub store: Arc<Store>,
}

/// Maximum request body size accepted by any handler (1 MiB for M0).
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// Builds the top-level axum `Router` with the `/health` endpoint plus
/// tracing and body-size-limit middleware. Publish/subscribe/bus routes
/// are wired in during M1/M4.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}
