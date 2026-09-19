//! Integration test: two clients chat + presence join/leave over `/bus`.
//! Mirrors PLAN.md section 11's "two-participant bus chat with presence
//! events" and the exact scenario manually smoke-tested in M4. Wire format
//! matches `src/http/bus.rs`'s `IncomingFrame`/`RawControlFrame` exactly.

mod common;

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

type WsStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn recv_json(ws: &mut WsStream, label: &str) -> Value {
    let msg = tokio::time::timeout(Duration::from_secs(3), ws.next())
        .await
        .unwrap_or_else(|_| panic!("[{label}] timed out waiting for a frame"))
        .unwrap_or_else(|| panic!("[{label}] stream ended unexpectedly"))
        .unwrap_or_else(|e| panic!("[{label}] ws error: {e}"));
    let text = msg.into_text().unwrap_or_else(|_| panic!("[{label}] expected a text frame"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("[{label}] invalid JSON ({e}): {text}"))
}

/// Asserts no frame arrives within `timeout` — used to prove e.g. that a
/// `pong` reply is NOT broadcast to other participants.
async fn assert_no_frame(ws: &mut WsStream, label: &str, timeout: Duration) {
    match tokio::time::timeout(timeout, ws.next()).await {
        Ok(Some(Ok(msg))) => panic!("[{label}] expected no frame, but got: {msg:?}"),
        Ok(Some(Err(e))) => panic!("[{label}] expected no frame, but got a ws error: {e}"),
        Ok(None) => panic!("[{label}] expected no frame, but the stream ended"),
        Err(_) => {} // timed out waiting -- correct, nothing arrived
    }
}

async fn send_json(ws: &mut WsStream, value: Value) {
    ws.send(Message::Text(value.to_string())).await.unwrap();
}

#[tokio::test]
async fn two_clients_chat_with_presence_join_leave() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let router = common::test_router(&config, common::mock_addr(40200));
    let server = common::spawn_server(router).await;

    // --- client 1 connects alone ---
    let (mut ws1, _) = tokio_tungstenite::connect_async(server.ws_url("/duplextopic/bus"))
        .await
        .expect("client 1 connect failed");
    let open1 = recv_json(&mut ws1, "c1 open").await;
    assert_eq!(open1["event"], "open");

    let presence1 = recv_json(&mut ws1, "c1 presence (alone)").await;
    assert_eq!(presence1["event"], "control");
    assert_eq!(presence1["control"]["type"], "presence");
    assert_eq!(presence1["control"]["data"], json!([]), "roster should be empty while alone");

    // no join yet -- nobody else has connected
    assert_no_frame(&mut ws1, "c1 (should be idle while alone)", Duration::from_millis(300)).await;

    // --- client 2 connects ---
    let (mut ws2, _) = tokio_tungstenite::connect_async(server.ws_url("/duplextopic/bus?sender=client2"))
        .await
        .expect("client 2 connect failed");

    // client 1 sees client 2 join
    let join_seen_by_c1 = recv_json(&mut ws1, "c1 sees join").await;
    assert_eq!(join_seen_by_c1["event"], "control");
    assert_eq!(join_seen_by_c1["control"]["type"], "join");
    assert_eq!(join_seen_by_c1["control"]["from"], "client2");

    // client 2 gets open + presence listing client 1
    let open2 = recv_json(&mut ws2, "c2 open").await;
    assert_eq!(open2["event"], "open");
    let presence2 = recv_json(&mut ws2, "c2 presence").await;
    assert_eq!(presence2["control"]["type"], "presence");
    let roster = presence2["control"]["data"].as_array().unwrap();
    assert_eq!(roster.len(), 1, "presence roster should list exactly client 1");

    // --- client 1 sends a message (minimal shape: no "event" key) ---
    send_json(&mut ws1, json!({ "message": "hello from client 1" })).await;

    let msg_seen_by_c2 = recv_json(&mut ws2, "c2 receives message").await;
    assert_eq!(msg_seen_by_c2["event"], "message");
    assert_eq!(msg_seen_by_c2["message"], "hello from client 1");
    assert!(msg_seen_by_c2["sender"].is_string(), "message should carry a sender label");

    // client 1 gets its own message echoed back (same shared fanout as
    // every other subscriber -- documented M4 behavior).
    let echo = recv_json(&mut ws1, "c1 echo of own message").await;
    assert_eq!(echo["message"], "hello from client 1");

    // --- client 2 sends a typing control frame ---
    send_json(&mut ws2, json!({ "event": "control", "control": { "type": "typing" } })).await;

    let typing_seen_by_c1 = recv_json(&mut ws1, "c1 sees typing").await;
    assert_eq!(typing_seen_by_c1["event"], "control");
    assert_eq!(typing_seen_by_c1["control"]["type"], "typing");
    assert_eq!(typing_seen_by_c1["control"]["from"], "client2");

    // --- client 1 sends ping; only client 1 should get pong back ---
    send_json(&mut ws1, json!({ "event": "control", "control": { "type": "ping" } })).await;

    let pong = recv_json(&mut ws1, "c1 receives pong").await;
    assert_eq!(pong["event"], "control");
    assert_eq!(pong["control"]["type"], "pong");
    assert_eq!(pong["control"]["from"], "server");

    // client 2 must NOT receive the pong (it's a direct reply, not broadcast).
    assert_no_frame(&mut ws2, "c2 (must not see pong)", Duration::from_millis(400)).await;

    // --- disconnect client 1; client 2 should see leave ---
    ws1.close(None).await.ok();
    drop(ws1);

    let leave_seen_by_c2 = recv_json(&mut ws2, "c2 sees leave").await;
    assert_eq!(leave_seen_by_c2["event"], "control");
    assert_eq!(leave_seen_by_c2["control"]["type"], "leave");
}
