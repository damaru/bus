//! Integration test: e2e envelope ciphertext passthrough byte-for-byte,
//! shape validation rejections, and content filters correctly ignored on
//! ciphertext. Mirrors PLAN.md section 11's "encrypted envelope passed
//! through byte-identical with filters correctly ignored" and PLAN.md
//! section 7's e2e design.

mod common;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::{json, Value};
use tower::ServiceExt;

async fn post_json(
    config: &bus::config::Config,
    addr: std::net::SocketAddr,
    topic: &str,
    body: &Value,
) -> (StatusCode, Value) {
    let router = common::test_router(config, addr);
    let req = Request::builder()
        .method("POST")
        .uri(format!("/{topic}"))
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn poll_with_query(
    config: &bus::config::Config,
    addr: std::net::SocketAddr,
    topic: &str,
    query_suffix: &str,
) -> Vec<Value> {
    let router = common::test_router(config, addr);
    let req = Request::builder()
        .method("GET")
        .uri(format!("/{topic}/json?poll=true&since=all{query_suffix}"))
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    text.lines().filter(|l| !l.is_empty()).map(|l| serde_json::from_str(l).unwrap()).collect()
}

#[tokio::test]
async fn ciphertext_and_enc_metadata_pass_through_byte_identical() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let addr = common::mock_addr(40300);

    let ciphertext = STANDARD.encode(b"totally opaque ciphertext bytes, server never looks inside");
    let nonce = STANDARD.encode(b"random-nonce-bytes-here");

    let publish_body = json!({
        "message": ciphertext,
        "encoding": "e2e",
        "enc": { "alg": "xchacha20poly1305", "kid": "topic-key-v1", "nonce": nonce },
    });
    let (status, published) = post_json(&config, addr, "e2etopic", &publish_body).await;
    assert_eq!(status, StatusCode::OK, "valid e2e publish should succeed: {published}");
    assert_eq!(published["message"], ciphertext, "ciphertext must round-trip byte-identical in the publish ack");
    assert_eq!(published["encoding"], "e2e");
    assert_eq!(published["enc"]["alg"], "xchacha20poly1305");
    assert_eq!(published["enc"]["kid"], "topic-key-v1");
    assert_eq!(published["enc"]["nonce"], nonce, "nonce must round-trip byte-identical");

    // Read it back via poll and confirm byte-identical persistence round-trip.
    let envelopes = poll_with_query(&config, addr, "e2etopic", "").await;
    assert_eq!(envelopes.len(), 1);
    assert_eq!(envelopes[0]["message"], ciphertext);
    assert_eq!(envelopes[0]["enc"]["nonce"], nonce);
    assert_eq!(envelopes[0]["enc"]["alg"], "xchacha20poly1305");
    assert_eq!(envelopes[0]["enc"]["kid"], "topic-key-v1");
}

#[tokio::test]
async fn missing_enc_object_with_e2e_encoding_is_bad_request() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let addr = common::mock_addr(40301);

    let body = json!({ "message": "some ciphertext", "encoding": "e2e" });
    let (status, resp) = post_json(&config, addr, "e2etopic2", &body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "encoding=e2e with no enc object must be rejected: {resp}");
}

#[tokio::test]
async fn invalid_base64_nonce_is_bad_request() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let addr = common::mock_addr(40302);

    let body = json!({
        "message": "some ciphertext",
        "encoding": "e2e",
        "enc": { "alg": "xchacha20poly1305", "kid": "k1", "nonce": "not-valid-base64!!!***" },
    });
    let (status, resp) = post_json(&config, addr, "e2etopic3", &body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "non-base64 nonce must be rejected: {resp}");
}

#[tokio::test]
async fn message_filter_is_not_meaningful_on_ciphertext_but_is_plain_string_compare() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let addr = common::mock_addr(40303);

    let ciphertext = STANDARD.encode(b"another opaque ciphertext blob");
    let nonce = STANDARD.encode(b"another-nonce-value");
    let publish_body = json!({
        "message": ciphertext,
        "encoding": "e2e",
        "enc": { "alg": "xchacha20poly1305", "kid": "k1", "nonce": nonce },
    });
    let (status, _) = post_json(&config, addr, "e2efiltertopic", &publish_body).await;
    assert_eq!(status, StatusCode::OK);

    // An unrelated plaintext filter value should NOT match the ciphertext
    // (proves filters are non-meaningful on e2e content, per PLAN.md 7).
    let unrelated = poll_with_query(&config, addr, "e2efiltertopic", "&message=totally-unrelated-plaintext").await;
    assert!(unrelated.is_empty(), "an unrelated message= filter must not match e2e ciphertext");

    // Filtering with the EXACT ciphertext string must still match --
    // proving the filter itself is a correctly-working plain string
    // comparison, not silently broken/always-false.
    let encoded_ct = urlencoding_lite(&ciphertext);
    let exact = poll_with_query(&config, addr, "e2efiltertopic", &format!("&message={encoded_ct}")).await;
    assert_eq!(exact.len(), 1, "filtering with the exact ciphertext string must match");
    assert_eq!(exact[0]["message"], ciphertext);
}

/// Minimal query-string percent-encoding for the handful of characters
/// base64 (standard alphabet) can produce that aren't URL-safe as-is
/// (`+`, `/`, `=`) — avoids pulling in a whole URL-encoding crate just for
/// this one test.
fn urlencoding_lite(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '+' => "%2B".to_string(),
            '/' => "%2F".to_string(),
            '=' => "%3D".to_string(),
            other => other.to_string(),
        })
        .collect()
}
