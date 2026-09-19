//! Library crate for `bus` — exposes every module publicly so both the
//! `bus` binary (`src/main.rs`) and the integration test suite
//! (`tests/*.rs`) share exactly one source of truth for how the server is
//! assembled, rather than each reimplementing the wiring in
//! `main.rs::serve()`.

pub mod admin;
pub mod auth;
pub mod config;
pub mod error;
pub mod http;
pub mod model;
pub mod store;
pub mod topic;

use std::sync::Arc;

use config::Config;
use store::Store;

/// Builds the full [`http::AppState`] (sled store, topic registry,
/// users/ACL, rate limiters) from a [`Config`] — the single canonical
/// "wire everything together" function used by both `bus serve` and the
/// integration tests, so there's no risk of the two drifting apart.
pub fn build_state(config: &Config) -> anyhow::Result<http::AppState> {
    let store = Arc::new(Store::open(&config.data_dir)?);
    let cache = Arc::new(store.cache());
    let topic_limits = topic::TopicLimits {
        max_topics: config.max_topics,
        max_subscribers_per_topic: config.max_subscribers_per_topic,
        max_subscribers_total: config.max_subscribers_total,
        max_bus_participants_per_topic: config.max_bus_participants_per_topic,
    };
    let topics = Arc::new(topic::TopicRegistry::new(config.cache_count as usize, cache, topic_limits));
    let users = Arc::new(store.users());
    let acl = Arc::new(store.acl());
    let auth_limiter = Arc::new(auth::AuthLimiter::new());
    let publish_limiter = Arc::new(auth::PublishLimiter::new(config.publish_rate_limit));

    Ok(http::AppState {
        store,
        topics,
        users,
        acl,
        auth_limiter,
        publish_limiter,
        default_access: config.default_access,
        max_message_bytes: config.max_message_bytes,
    })
}

/// Builds the top-level axum [`axum::Router`] directly from a [`Config`] —
/// a thin convenience wrapper around [`build_state`] + [`http::router`].
/// This is the function both `bus serve` and every integration test under
/// `tests/` call to get a real, fully-wired router.
pub fn build_router(config: &Config) -> anyhow::Result<axum::Router> {
    Ok(http::router(build_state(config)?))
}
