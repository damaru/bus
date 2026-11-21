//! Integration test: admin-only `POST /admin/purge`, which drops the stored
//! history of named topics. Covers the durable sled path and the in-memory
//! ring, sequence continuity after a purge, and the 403/400/415 rejections.

mod common;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use bus::store::users::Role;
use bus::store::Store;
use serde_json::{json, Value};
use tower::ServiceExt;

fn seed_token(dir: &tempfile::TempDir, user: &str, role: Role) -> String {
    let store = Store::open(dir.path()).expect("failed to open sled store for seeding");
    let users = store.users();
    users.create(user, "secret123", role).unwrap();
    users.issue_token(user, None).unwrap()
}

async fn send(router: &Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

async fn publish(router: &Router, topic: &str, message: &str) -> Value {
    let req = Request::builder()
        .method("POST")
        .uri(format!("/{topic}"))
        .body(Body::from(message.to_string()))
        .unwrap();
    let (status, body) = send(router, req).await;
    assert_eq!(status, StatusCode::OK, "publish failed: {body}");
    body
}

async fn replay(router: &Router, topic: &str) -> Vec<Value> {
    let req = Request::builder()
        .method("GET")
        .uri(format!("/{topic}/json?poll=true&since=all"))
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    text.lines().map(|l| serde_json::from_str(l).unwrap()).collect()
}

fn purge_request(auth: Option<&str>, content_type: Option<&str>, body: &str) -> Request<Body> {
    let mut builder = Request::builder().method("POST").uri("/admin/purge");
    if let Some(a) = auth {
        builder = builder.header("Authorization", a);
    }
    if let Some(ct) = content_type {
        builder = builder.header("Content-Type", ct);
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

#[tokio::test]
async fn purge_empties_named_topic_and_keeps_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let token = seed_token(&dir, "root", Role::Admin);
    let router = common::test_router(&config, common::mock_addr(40300));
    publish(&router, "stale", "one").await;
    publish(&router, "stale", "two").await;
    publish(&router, "keep", "kept").await;
    assert_eq!(replay(&router, "stale").await.len(), 2);

    let auth = format!("Bearer {token}");
    let req = purge_request(Some(&auth), Some("application/json"), r#"{"topics":["stale"]}"#);
    let (status, body) = send(&router, req).await;
    assert_eq!(status, StatusCode::OK, "admin purge failed: {body}");
    assert_eq!(body, json!({"purged": 2}));

    assert!(replay(&router, "stale").await.is_empty(), "since=all must return nothing after purge");
    assert_eq!(replay(&router, "keep").await.len(), 1, "unnamed topics must be left alone");

    let next = publish(&router, "stale", "three").await;
    assert_eq!(next["seq"], 3, "sequence must keep counting after a purge");
    assert_eq!(replay(&router, "stale").await.len(), 1);
}

#[tokio::test]
async fn purge_reaches_persisted_topics_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let token = seed_token(&dir, "root", Role::Admin);
    {
        let router = common::test_router(&config, common::mock_addr(40301));
        publish(&router, "stale", "one").await;
        publish(&router, "stale", "two").await;
    }

    let router = common::test_router(&config, common::mock_addr(40301));
    let auth = format!("Bearer {token}");
    let req = purge_request(Some(&auth), Some("application/json"), r#"{"topics":["stale"]}"#);
    let (status, body) = send(&router, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"purged": 2}), "topics not yet loaded in memory must still be purged");
    assert!(replay(&router, "stale").await.is_empty());
}

#[tokio::test]
async fn purge_unknown_topic_is_a_noop() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let token = seed_token(&dir, "root", Role::Admin);
    let router = common::test_router(&config, common::mock_addr(40302));

    let auth = format!("Bearer {token}");
    let req = purge_request(Some(&auth), Some("application/json"), r#"{"topics":["nosuchtopic"]}"#);
    let (status, body) = send(&router, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"purged": 0}));
}

#[tokio::test]
async fn non_admin_and_anonymous_callers_get_403() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let user_token = seed_token(&dir, "alice", Role::User);
    let router = common::test_router(&config, common::mock_addr(40303));
    publish(&router, "stale", "one").await;

    let user_auth = format!("Bearer {user_token}");
    for auth in [Some(user_auth.as_str()), None] {
        let req = purge_request(auth, Some("application/json"), r#"{"topics":["stale"]}"#);
        let (status, body) = send(&router, req).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["error"], "admin only");
    }
    assert_eq!(replay(&router, "stale").await.len(), 1, "a rejected purge must not drop anything");
}

#[tokio::test]
async fn invalid_topic_name_is_bad_request_and_drops_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let token = seed_token(&dir, "root", Role::Admin);
    let router = common::test_router(&config, common::mock_addr(40304));
    publish(&router, "stale", "one").await;

    let auth = format!("Bearer {token}");
    let req = purge_request(Some(&auth), Some("application/json"), r#"{"topics":["stale","bad name!"]}"#);
    let (status, _) = send(&router, req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(replay(&router, "stale").await.len(), 1, "validation must run before any topic is purged");
}

#[tokio::test]
async fn malformed_requests_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let token = seed_token(&dir, "root", Role::Admin);
    let router = common::test_router(&config, common::mock_addr(40305));
    let auth = format!("Bearer {token}");

    let (status, _) = send(&router, purge_request(Some(&auth), Some("text/plain"), r#"{"topics":[]}"#)).await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);

    let (status, _) = send(&router, purge_request(Some(&auth), None, r#"{"topics":[]}"#)).await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);

    let (status, _) = send(&router, purge_request(Some(&auth), Some("application/json"), "not json")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let names: Vec<String> = (0..1001).map(|i| format!("t{i}")).collect();
    let too_many = json!({ "topics": names }).to_string();
    let (status, _) = send(&router, purge_request(Some(&auth), Some("application/json"), &too_many)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn admin_named_topic_still_routes_to_subscribe() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let router = common::test_router(&config, common::mock_addr(40306));
    publish(&router, "admin", "hello").await;
    assert_eq!(replay(&router, "admin").await.len(), 1, "GET /admin/json must still subscribe to topic 'admin'");
}
