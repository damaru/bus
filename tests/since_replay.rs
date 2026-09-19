//! Integration test: persistence + `since=`/`poll=` across a simulated
//! process restart. A real OS process restart is awkward inside `cargo
//! test`, so instead we drop the old `Router`/`AppState`/`Store` (releasing
//! sled's file lock) and construct a brand new set pointed at the SAME
//! `tempfile::TempDir` path — this exercises the exact sled-reopen code
//! path (`Store::open` -> `Cache::last_seq` -> `Topic::new`) a real restart
//! would. Mirrors PLAN.md section 11's "restart-and-replay via a temp sled
//! dir".

mod common;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

async fn publish(config: &bus::config::Config, addr: std::net::SocketAddr, topic: &str, message: &str) -> Value {
    let router = common::test_router(config, addr);
    let req = Request::builder()
        .method("POST")
        .uri(format!("/{topic}"))
        .body(Body::from(message.to_string()))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "publish failed");
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn poll_since(config: &bus::config::Config, addr: std::net::SocketAddr, topic: &str, since: &str) -> Vec<Value> {
    let router = common::test_router(config, addr);
    let req = Request::builder()
        .method("GET")
        .uri(format!("/{topic}/json?poll=true&since={since}"))
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    text.lines().map(|l| serde_json::from_str(l).unwrap()).collect()
}

#[tokio::test]
async fn persistence_and_seq_continuity_across_simulated_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let addr = common::mock_addr(40010);

    // --- "process 1": publish 2 messages ---
    let m1 = publish(&config, addr, "restarttopic", "message one").await;
    let m2 = publish(&config, addr, "restarttopic", "message two").await;
    assert_eq!(m1["seq"], 1);
    assert_eq!(m2["seq"], 2);
    let m1_id = m1["id"].as_str().unwrap().to_string();
    let m1_time = m1["time"].as_i64().unwrap();

    // Every `test_router`/`build_state` call above already opened and
    // dropped its own `Store` (each `router.oneshot(...)` call used a
    // fresh `AppState` built from the same `config.data_dir`), so sled's
    // file lock is already released between calls — this loop already
    // implicitly exercises "close and reopen" 3 times. Do one more
    // explicit, clearly-commented "restart" for the actual since=/seq
    // assertions below.

    // --- "process 2": fresh Store/TopicRegistry/Router, same data_dir ---
    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("GET")
        .uri("/restarttopic/json?poll=true&since=all")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let envelopes: Vec<Value> = text.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    assert_eq!(envelopes.len(), 2, "expected both messages to survive the simulated restart");
    assert_eq!(envelopes[0]["seq"], 1);
    assert_eq!(envelopes[0]["message"], "message one");
    assert_eq!(envelopes[1]["seq"], 2);
    assert_eq!(envelopes[1]["message"], "message two");

    // --- publish a 3rd message against the "restarted" instance: seq must continue at 3, not reset ---
    let m3 = publish(&config, addr, "restarttopic", "message three").await;
    assert_eq!(m3["seq"], 3, "seq must continue across the simulated restart, not reset to 1");

    // --- since=<message_id>: resolves against sled-backed history, not just the in-memory ring ---
    let since_id = poll_since(&config, addr, "restarttopic", &m1_id).await;
    assert_eq!(since_id.len(), 2, "since=<id of message 1> should return messages 2 and 3");
    assert_eq!(since_id[0]["message"], "message two");
    assert_eq!(since_id[1]["message"], "message three");

    // --- since=<unix_timestamp>: resolves against sled-backed history too ---
    let since_time = poll_since(&config, addr, "restarttopic", &m1_time.to_string()).await;
    assert_eq!(since_time.len(), 3, "since=<time of message 1> should include all 3 messages (time >= t)");

    // A timestamp strictly after all 3 messages should return nothing.
    let since_future = poll_since(&config, addr, "restarttopic", &(m1_time + 3600).to_string()).await;
    assert!(since_future.is_empty(), "since=<far future> should return no messages");
}
