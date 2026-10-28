mod common;

use std::os::unix::fs::PermissionsExt;

use axum::body::Body;
use axum::extract::connect_info::MockConnectInfo;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;

fn mint(tok: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/admin/child-token")
        .header("authorization", format!("Bearer {tok}"))
        .header("content-type", "application/json")
        .body(Body::from(json!({"session": "b41", "ttl_secs": 60}).to_string()))
        .unwrap()
}

fn read_tok(path: &std::path::Path) -> String {
    let raw = std::fs::read_to_string(path).unwrap();
    assert!(raw.ends_with('\n'));
    let tok = raw[..raw.len() - 1].to_string();
    assert!(tok.len() >= 32);
    assert!(!tok.chars().any(|c| c.is_whitespace()));
    let mode = std::fs::metadata(path).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
    tok
}

fn router(config: &bus::config::Config) -> axum::Router {
    bus::build_router(config)
        .unwrap()
        .layer(MockConnectInfo(common::mock_addr(40042)))
}

#[tokio::test]
async fn admin_token_file_is_recreated_each_start() {
    let dir = tempfile::tempdir().unwrap();
    let tdir = tempfile::tempdir().unwrap();
    let path = tdir.path().join("sub").join("admin.token");
    let mut config = common::test_config(&dir);
    config.admin_token_file = Some(path.clone());

    let app = router(&config);
    let old = read_tok(&path);
    let resp = app.clone().oneshot(mint(&old)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    drop(app);

    let app = router(&config);
    let new = read_tok(&path);
    assert_ne!(old, new);
    let resp = app.clone().oneshot(mint(&old)).await.unwrap();
    assert!(matches!(
        resp.status(),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
    ));
    let resp = app.clone().oneshot(mint(&new)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
