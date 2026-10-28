mod common;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};
use tower::ServiceExt;

async fn publish(config: &bus::config::Config, addr: std::net::SocketAddr, topic: &str, msg: &str) -> Value {
    let router = common::test_router(config, addr);
    let req = Request::builder()
        .method("POST")
        .uri(format!("/{topic}"))
        .body(Body::from(msg.to_string()))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn poll_since(config: &bus::config::Config, addr: std::net::SocketAddr, topic: &str) -> Vec<Value> {
    let router = common::test_router(config, addr);
    let req = Request::builder()
        .method("GET")
        .uri(format!("/{topic}/json?poll=true&since=all"))
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(body.to_vec())
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[tokio::test]
async fn history_off_keeps_sequence_but_replays_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = common::test_config(&dir);
    config.history = false;
    let addr = common::mock_addr(40031);

    let m1 = publish(&config, addr, "nohist", "one").await;
    let m2 = publish(&config, addr, "nohist", "two").await;
    assert_eq!(m1["seq"], 1);
    assert_eq!(m2["seq"], 2);

    let replay = poll_since(&config, addr, "nohist").await;
    assert!(replay.is_empty(), "history off must not replay: {replay:?}");
}

#[test]
fn unix_socket_publish_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("bus.sock");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_bus"))
        .args(["serve", "--unix"])
        .arg(&sock)
        .arg("--data-dir")
        .arg(dir.path().join("data"))
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    while !sock.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(sock.exists(), "socket not created");

    let mode = std::fs::metadata(&sock).unwrap().permissions();
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(mode.mode() & 0o777, 0o600);

    let body = "hello over unix";
    let mut s = UnixStream::connect(&sock).unwrap();
    write!(
        s,
        "POST /unixtopic HTTP/1.1\r\nHost: bus\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
    .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    child.kill().ok();
    child.wait().ok();

    assert!(out.starts_with("HTTP/1.1 200"), "{out}");
    assert!(out.contains("\"seq\":1") || out.contains("\"seq\": 1"), "{out}");
}
