//! Integration test: publish via `PUT`/`POST /{topic}`, then read the
//! message back through each of the four ntfy-compatible read paths
//! (`/json`, `/sse`, `/raw`, `/ws`, all with `poll=true`) and confirm the
//! content matches. Mirrors PLAN.md section 11's "roundtrip across
//! json/sse/raw/ws".

mod common;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use futures::StreamExt;
use tower::ServiceExt;

#[tokio::test]
async fn publish_then_read_back_via_all_four_formats() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let client_addr = common::mock_addr(40001);

    // --- publish ---
    let router = common::test_router(&config, client_addr);
    let publish_req = Request::builder()
        .method("POST")
        .uri("/roundtrip")
        .body(Body::from("hello roundtrip"))
        .unwrap();
    let resp = router.oneshot(publish_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let published: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(published["message"], "hello roundtrip");
    assert_eq!(published["seq"], 1);
    let expected_id = published["id"].as_str().unwrap().to_string();

    // --- /json?poll=true ---
    let router = common::test_router(&config, client_addr);
    let req = Request::builder()
        .method("GET")
        .uri("/roundtrip/json?poll=true")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let line = text.lines().next().expect("expected at least one ndjson line");
    let env: serde_json::Value = serde_json::from_str(line).unwrap();
    assert_eq!(env["message"], "hello roundtrip");
    assert_eq!(env["id"], expected_id);

    // --- /sse?poll=true ---
    let router = common::test_router(&config, client_addr);
    let req = Request::builder()
        .method("GET")
        .uri("/roundtrip/sse?poll=true")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    // message events are framed as "data: {json}\n\n" (no "event:" line,
    // per ntfy's EventSource-friendly convention).
    let data_line = text
        .lines()
        .find(|l| l.starts_with("data: "))
        .expect("expected a data: line in the SSE stream");
    let env: serde_json::Value = serde_json::from_str(data_line.trim_start_matches("data: ")).unwrap();
    assert_eq!(env["message"], "hello roundtrip");

    // --- /raw?poll=true ---
    let router = common::test_router(&config, client_addr);
    let req = Request::builder()
        .method("GET")
        .uri("/roundtrip/raw?poll=true")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert_eq!(text.trim(), "hello roundtrip");

    // --- /ws?poll=true : needs a real bound listener + WS client ---
    let ws_router = common::test_router(&config, client_addr);
    let server = common::spawn_server(ws_router).await;
    let url = server.ws_url("/roundtrip/ws?poll=true");
    let (mut ws, _resp) = tokio_tungstenite::connect_async(url).await.expect("ws connect failed");

    let msg = tokio::time::timeout(std::time::Duration::from_secs(3), ws.next())
        .await
        .expect("timed out waiting for ws poll backlog")
        .expect("stream ended unexpectedly")
        .expect("ws error");
    let text = msg.into_text().expect("expected a text frame");
    let env: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(env["message"], "hello roundtrip");

    // poll mode: the server closes right after the backlog — drain until
    // the stream ends (either a Close frame or the connection dropping).
    let _ = ws.close(None).await;
}
