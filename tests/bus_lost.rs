mod common;

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::StreamExt;
use serde_json::Value;
use tower::ServiceExt;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn publish(cfg: &bus::config::Config, topic: &str, msg: &str) {
    let router = common::test_router(cfg, common::mock_addr(40300));
    let req = Request::builder()
        .method("POST")
        .uri(format!("/{topic}"))
        .body(Body::from(msg.to_string()))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "publish failed");
}

async fn next(ws: &mut Ws, d: Duration) -> Option<Value> {
    let m = tokio::time::timeout(d, ws.next()).await.ok()?.expect("stream ended").expect("ws error");
    let t = m.into_text().expect("expected text frame");
    Some(serde_json::from_str(&t).expect("invalid JSON"))
}

fn is_lost(v: &Value) -> bool {
    v["event"] == "control" && v["control"]["type"] == "lost"
}

#[tokio::test]
async fn history_off_sends_lost_and_no_backlog() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::test_config(&dir);
    cfg.history = false;
    publish(&cfg, "lostoff", "gone").await;

    let router = common::test_router(&cfg, common::mock_addr(40301));
    let srv = common::spawn_server(router).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(srv.ws_url("/lostoff/bus?since=all"))
        .await
        .expect("connect failed");

    let mut seen = Vec::new();
    let mut found = false;
    while let Some(v) = next(&mut ws, Duration::from_secs(3)).await {
        assert_ne!(v["event"], "message", "message arrived before lost frame: {seen:?}");
        if is_lost(&v) {
            found = true;
            break;
        }
        seen.push(v);
    }
    assert!(found, "no lost frame arrived; saw {seen:?}");

    let end = tokio::time::Instant::now() + Duration::from_secs(1);
    while let Some(left) = end.checked_duration_since(tokio::time::Instant::now()) {
        match next(&mut ws, left).await {
            Some(v) => assert_ne!(v["event"], "message", "unexpected backlog message: {v}"),
            None => break,
        }
    }
}

#[tokio::test]
async fn history_on_sends_no_lost() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = common::test_config(&dir);
    publish(&cfg, "loston", "kept").await;

    let router = common::test_router(&cfg, common::mock_addr(40302));
    let srv = common::spawn_server(router).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(srv.ws_url("/loston/bus?since=all"))
        .await
        .expect("connect failed");

    let mut got = false;
    let end = tokio::time::Instant::now() + Duration::from_secs(1);
    while let Some(left) = end.checked_duration_since(tokio::time::Instant::now()) {
        match next(&mut ws, left).await {
            Some(v) => {
                assert!(!is_lost(&v), "unexpected lost frame: {v}");
                if v["event"] == "message" {
                    assert_eq!(v["message"], "kept");
                    got = true;
                }
            }
            None => break,
        }
    }
    assert!(got, "backlog message was not received");
}
