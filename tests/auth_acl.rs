//! Integration test: ACL allow/deny matrix + `default_access` modes.
//! Seeds users/ACL entries directly via `store::users::Users`/
//! `store::acl::Acl` (no need to shell out to the `bus admin` CLI) before
//! driving requests through a real router. Mirrors PLAN.md section 11's
//! "ACL allow/deny matrix".

mod common;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use bus::config::DefaultAccess;
use bus::store::acl::Permission;
use bus::store::users::Role;
use bus::store::Store;
use tempfile::TempDir;
use tower::ServiceExt;

/// Opens a throwaway `Store` handle against `dir`, runs `seed` against its
/// `Users`/`Acl` handles, then drops the `Store` before returning — so its
/// sled lock is released before the test builds a router (which opens its
/// own fresh `Store` for the same directory via `bus::build_state`).
fn seed(dir: &TempDir, seed: impl FnOnce(&bus::store::users::Users, &bus::store::acl::Acl)) {
    let store = Store::open(dir.path()).expect("failed to open sled store for seeding");
    let users = store.users();
    let acl = store.acl();
    seed(&users, &acl);
}

fn basic_auth_header(user: &str, pass: &str) -> String {
    format!("Basic {}", STANDARD.encode(format!("{user}:{pass}")))
}

async fn publish_status(
    config: &bus::config::Config,
    addr: std::net::SocketAddr,
    topic: &str,
    auth_header: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let router = common::test_router(config, addr);
    let mut builder = Request::builder().method("POST").uri(format!("/{topic}"));
    if let Some(h) = auth_header {
        builder = builder.header("Authorization", h);
    }
    let req = builder.body(Body::from("payload")).unwrap();
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test]
async fn default_read_write_with_no_acl_allows_anonymous() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir); // DefaultAccess::ReadWrite by default
    let addr = common::mock_addr(40100);

    let (status, body) = publish_status(&config, addr, "opentopic", None).await;
    assert_eq!(status, StatusCode::OK, "anonymous publish should succeed with no ACL entries: {body}");
}

#[tokio::test]
async fn explicit_everyone_deny_blocks_anonymous() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let addr = common::mock_addr(40101);

    seed(&dir, |_users, acl| {
        acl.grant("*", "secrettopic", Permission::DenyAll).unwrap();
    });

    let (status, _) = publish_status(&config, addr, "secrettopic", None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "explicit '*' deny should block anonymous access");
}

#[tokio::test]
async fn user_specific_grant_wins_over_everyone_deny() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let addr = common::mock_addr(40102);

    seed(&dir, |users, acl| {
        users.create("alice", "secret123", Role::User).unwrap();
        acl.grant("*", "secrettopic", Permission::DenyAll).unwrap();
        acl.grant("alice", "secrettopic", Permission::ReadWrite).unwrap();
    });

    // anonymous still denied
    let (anon_status, _) = publish_status(&config, addr, "secrettopic", None).await;
    assert_eq!(anon_status, StatusCode::FORBIDDEN);

    // alice, authenticated via Basic, gets through despite the '*' deny
    let auth = basic_auth_header("alice", "secret123");
    let (alice_status, body) = publish_status(&config, addr, "secrettopic", Some(&auth)).await;
    assert_eq!(alice_status, StatusCode::OK, "alice's user-specific grant should win over '*' deny: {body}");
    assert_eq!(body["message"], "payload");
}

#[tokio::test]
async fn wrong_password_is_unauthorized() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let addr = common::mock_addr(40103);

    seed(&dir, |users, acl| {
        users.create("alice", "secret123", Role::User).unwrap();
        acl.grant("alice", "secrettopic", Permission::ReadWrite).unwrap();
    });

    let bad_auth = basic_auth_header("alice", "wrongpassword");
    let (status, _) = publish_status(&config, addr, "secrettopic", Some(&bad_auth)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "wrong password must be 401, not 403");
}

#[tokio::test]
async fn bearer_token_authenticates_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let config = common::test_config(&dir);
    let addr = common::mock_addr(40104);

    let token = {
        let store = Store::open(dir.path()).unwrap();
        let users = store.users();
        let acl = store.acl();
        users.create("bob", "hunter2", Role::User).unwrap();
        acl.grant("bob", "tokentopic", Permission::ReadWrite).unwrap();
        acl.grant("*", "tokentopic", Permission::DenyAll).unwrap();
        users.issue_token("bob", None).unwrap()
    };

    // anonymous still denied
    let (anon_status, _) = publish_status(&config, addr, "tokentopic", None).await;
    assert_eq!(anon_status, StatusCode::FORBIDDEN);

    // bob's bearer token gets through
    let auth = format!("Bearer {token}");
    let (status, body) = publish_status(&config, addr, "tokentopic", Some(&auth)).await;
    assert_eq!(status, StatusCode::OK, "valid bearer token should authenticate: {body}");
}

#[tokio::test]
async fn deny_all_default_access_blocks_everyone_with_no_grants() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = common::test_config(&dir);
    config.default_access = DefaultAccess::DenyAll;
    let addr = common::mock_addr(40105);

    // no ACL entries at all -- default_access=deny-all should block anonymous...
    let (anon_status, _) = publish_status(&config, addr, "lockedtopic", None).await;
    assert_eq!(anon_status, StatusCode::FORBIDDEN, "deny-all default access should block anonymous");

    // ...and also block an authenticated-but-ungranted user.
    seed(&dir, |users, _acl| {
        users.create("carol", "pw", Role::User).unwrap();
    });
    let auth = basic_auth_header("carol", "pw");
    let (carol_status, _) = publish_status(&config, addr, "lockedtopic", Some(&auth)).await;
    assert_eq!(
        carol_status,
        StatusCode::FORBIDDEN,
        "deny-all default access should block an authenticated user with no explicit grant too"
    );
}
