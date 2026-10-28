mod common;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt;

fn seed(config: &bus::config::Config) -> (String, String) {
    let store = bus::store::Store::open(&config.data_dir).unwrap();
    let users = store.users();
    users.create("root", "rootpass", bus::store::users::Role::Admin).unwrap();
    users.create("bob", "bobpass", bus::store::users::Role::User).unwrap();
    let root = users.issue_token("root", None).unwrap();
    let bob = users.issue_token("bob", None).unwrap();
    (root, bob)
}

#[tokio::test]
async fn mint_requires_admin_and_returns_scoped_token() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let (root, bob) = seed(&config);
    let app = common::test_router(&config, common::mock_addr(40041));

    let mint = |tok: &str| {
        Request::builder()
            .method("POST")
            .uri("/admin/child-token")
            .header("authorization", format!("Bearer {tok}"))
            .header("content-type", "application/json")
            .body(Body::from(json!({"session": "s42", "ttl_secs": 3600}).to_string()))
            .unwrap()
    };

    let resp = app.clone().oneshot(mint(&bob)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    let resp = app.clone().oneshot(mint(&root)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["topic_pattern"], "session.s42.*");
    let child = v["token"].as_str().unwrap().to_string();

    let get = |topic: &str| {
        Request::builder()
            .method("GET")
            .uri(format!("/{topic}/json?poll=true&since=none"))
            .header("authorization", format!("Bearer {child}"))
            .body(Body::empty())
            .unwrap()
    };
    let resp = app.clone().oneshot(get("session.s42.inbox")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = app.clone().oneshot(get("session.other.inbox")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}
