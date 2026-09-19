//! Shared helpers for the integration test suite. Included via `mod
//! common;` in each `tests/*.rs` file — `tests/common/mod.rs` (rather than
//! `tests/common.rs`) is the standard Cargo convention for a test helper
//! module that ISN'T itself compiled as a separate test binary.

#![allow(dead_code)] // not every test file uses every helper

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use axum::extract::connect_info::MockConnectInfo;
use axum::Router;
use bus::config::Config;
use tempfile::TempDir;

/// Builds a `Config` pointed at a fresh temp sled dir, with every
/// connection/rate/topic cap turned up high so ordinary test traffic never
/// trips them (those caps get their own dedicated, deliberately-low-cap
/// tests elsewhere) — this file's helpers are for the "everything just
/// works" integration tests.
pub fn test_config(dir: &TempDir) -> Config {
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.publish_rate_limit = 10_000;
    config.max_topics = 10_000;
    config.max_subscribers_per_topic = 10_000;
    config.max_subscribers_total = 10_000;
    config.max_bus_participants_per_topic = 10_000;
    config
}

/// Builds a real axum `Router` from `config`, wrapped with
/// [`MockConnectInfo`] so `ConnectInfo<SocketAddr>`-based extraction (used
/// by auth/rate-limiting) works under `tower::ServiceExt::oneshot` without
/// a real accepted connection. Give each caller that needs a distinct
/// "client" a different `client_addr` (e.g. to test per-IP rate limiting
/// in isolation).
pub fn test_router(config: &Config, client_addr: SocketAddr) -> Router {
    let state = bus::build_state(config).expect("failed to build AppState for test");
    bus::http::router(state).layer(MockConnectInfo(client_addr))
}

/// Default mock client address for tests that don't care about the exact
/// IP (most of them) — port varies per call so independent `test_router`
/// instances in the same test don't accidentally share a rate-limit/auth
/// bucket keyed by IP+port... note: rate limiting/auth keying in this
/// project is by IP only, not IP+port, so tests that DO care about sharing
/// or not sharing a rate-limit bucket should pick explicit, deliberately
/// same/different IPs instead of relying on this helper's port to differ.
pub fn mock_addr(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

/// Binds a real OS-assigned ephemeral port (`:0`) and spawns `router` on
/// it via `axum::serve(...).into_make_service_with_connect_info()` (so
/// `ConnectInfo<SocketAddr>` works for real, unlike the oneshot/mock path
/// above) — used by WS-based tests (`/ws`, `/bus`) that need a real
/// `TcpListener` for `tokio-tungstenite` to connect to. Returns the actual
/// bound address; the spawned server task is aborted when the returned
/// `ServerHandle` is dropped.
pub async fn spawn_server(router: Router) -> ServerHandle {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port");
    let addr = listener.local_addr().expect("failed to read bound addr");
    let task = tokio::spawn(async move {
        axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .expect("test server exited with an error");
    });
    ServerHandle { addr, task }
}

pub struct ServerHandle {
    pub addr: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl ServerHandle {
    pub fn ws_url(&self, path: &str) -> String {
        format!("ws://{}{}", self.addr, path)
    }

    pub fn http_url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}
