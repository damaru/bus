//! Integration tests for the file-attachments feature (see
//! docs/plans/file-attachments.md section 10): local file upload + `GET`/
//! `HEAD /file/:id` download, remote-URL passthrough, both size-limit
//! rejections, the attachments-disabled path, lazy expiry-on-download, and
//! malformed/unknown id handling. Mirrors the patterns in
//! `tests/publish_subscribe.rs` and `tests/since_replay.rs`: build a fresh
//! `Router` per request (each `oneshot()` consumes it), read JSON publish
//! acks via `serde_json::Value`.

mod common;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

/// Builds a `Config` with attachments enabled: `attachment_dir` pointed at
/// a second, separate temp dir (kept alive by the caller) and a fake
/// (never dereferenced) `base_url`.
fn attachment_config(dir: &tempfile::TempDir, blob_dir: &tempfile::TempDir) -> bus::config::Config {
    let mut config = common::test_config(dir);
    config.attachment_dir = Some(blob_dir.path().to_path_buf());
    config.base_url = Some("http://localhost:8080".to_string());
    config
}

#[tokio::test]
async fn upload_then_download_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = tempfile::tempdir().unwrap();
    let config = attachment_config(&dir, &blob_dir);
    let addr = common::mock_addr(41001);

    let file_bytes = b"not really a jpeg but good enough for a test".to_vec();

    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("POST")
        .uri("/attachtopic")
        .header("X-Filename", "photo.jpg")
        .body(Body::from(file_bytes.clone()))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let published: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let id = published["id"].as_str().unwrap().to_string();
    let url = published["attachment"]["url"].as_str().unwrap().to_string();
    assert_eq!(published["attachment"]["name"], "photo.jpg");
    assert!(
        url.ends_with(&format!("/file/{id}")),
        "expected attachment.url to end with /file/{id}, got {url}"
    );

    // GET /file/{id}
    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("GET")
        .uri(format!("/file/{id}"))
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let headers = resp.headers().clone();
    let content_type = headers.get("content-type").expect("expected a Content-Type header");
    assert!(
        content_type.to_str().unwrap().starts_with("image/jpeg"),
        "expected image/jpeg content-type for a .jpg filename, got {content_type:?}"
    );
    let disposition = headers
        .get("content-disposition")
        .expect("expected a Content-Disposition header");
    assert!(
        disposition.to_str().unwrap().contains("photo.jpg"),
        "expected Content-Disposition to mention photo.jpg, got {disposition:?}"
    );

    let downloaded = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(downloaded.to_vec(), file_bytes, "downloaded bytes must match the uploaded bytes exactly");
}

#[tokio::test]
async fn oversized_file_rejected_with_413() {
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = tempfile::tempdir().unwrap();
    let mut config = attachment_config(&dir, &blob_dir);
    config.attachment_file_size_limit = 10;
    let addr = common::mock_addr(41002);

    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("POST")
        .uri("/attachtopic")
        .header("X-Filename", "too-big.bin")
        .body(Body::from(vec![b'x'; 1000]))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn publish_exceeding_total_budget_rejected_with_413() {
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = tempfile::tempdir().unwrap();
    let mut config = attachment_config(&dir, &blob_dir);
    config.attachment_total_size_limit = 150;
    config.attachment_file_size_limit = 10_000; // generous, so only the total budget is being tested
    let addr = common::mock_addr(41003);

    // First upload: 100 bytes, well within the 150-byte total budget.
    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("POST")
        .uri("/budgettopic")
        .header("X-Filename", "first.bin")
        .body(Body::from(vec![b'a'; 100]))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "first upload should fit comfortably within the total budget");

    // Second upload: another 100 bytes would push the running total to
    // 200, over the 150-byte budget -- must be rejected.
    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("POST")
        .uri("/budgettopic")
        .header("X-Filename", "second.bin")
        .body(Body::from(vec![b'b'; 100]))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn attachments_disabled_returns_400_for_filename_publish() {
    let dir = tempfile::tempdir().unwrap();
    // attachment_dir/base_url deliberately left unset.
    let config = common::test_config(&dir);
    let addr = common::mock_addr(41004);

    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("POST")
        .uri("/disabledtopic")
        .header("X-Filename", "photo.jpg")
        .body(Body::from(b"some bytes".to_vec()))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn remote_attachment_url_passthrough() {
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = tempfile::tempdir().unwrap();
    let mut config = attachment_config(&dir, &blob_dir);
    // Deliberately tiny -- a remote-URL attachment writes no local bytes,
    // so this must not trigger a 413.
    config.attachment_total_size_limit = 1;
    config.attachment_file_size_limit = 1;
    let addr = common::mock_addr(41005);

    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("POST")
        .uri("/remotetopic")
        .header("X-Attach", "http://example.com/some/remote/file.pdf")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let published: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        published["attachment"]["url"], "http://example.com/some/remote/file.pdf",
        "remote attachment url must pass through verbatim, unchanged"
    );
}

#[tokio::test]
async fn attachment_expiry_sweep_makes_file_404_while_message_survives() {
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = tempfile::tempdir().unwrap();
    let mut config = attachment_config(&dir, &blob_dir);
    // `expires` is stored/compared at whole-second (unix-seconds)
    // granularity throughout this codebase -- see `model::now_unix` and
    // the `expires = now_unix() + attachment_expiry.as_secs() as i64`
    // computation in `src/http/publish.rs`, matching `Envelope.time`'s own
    // second-level precision. A sub-second `attachment_expiry` (e.g.
    // 200ms) truncates to 0 via `Duration::as_secs()` and is therefore
    // indistinguishable from "expires right now", which is too flaky to
    // assert on reliably. Use the smallest granularity the system actually
    // supports (1s) and sleep past a full second boundary instead.
    config.attachment_expiry = std::time::Duration::from_secs(1);
    config.cache_duration = std::time::Duration::from_secs(3600); // long enough not to interfere
    let addr = common::mock_addr(41006);

    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("POST")
        .uri("/expirytopic")
        .header("X-Filename", "soon-gone.bin")
        .body(Body::from(vec![b'z'; 50]))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let published: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let id = published["id"].as_str().unwrap().to_string();

    // No background sweep runs in this test process (that's wired only in
    // main.rs::serve) -- rely on the lazy expiry check baked into the
    // download handler itself. Sleep past 2 whole-second boundaries (not
    // just 1s) so this isn't flaky depending on where within the current
    // second the upload happened to land.
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;

    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("GET")
        .uri(format!("/file/{id}"))
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "expired attachment must 404");

    // The message-level cache is independent of attachment expiry -- the
    // envelope itself must still be readable.
    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("GET")
        .uri("/expirytopic/json?poll=true&since=all")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let line = text.lines().next().expect("expected the message to still be replayable");
    let env: serde_json::Value = serde_json::from_str(line).unwrap();
    assert_eq!(env["id"], id, "the message envelope must survive even though its attachment expired");
}

#[tokio::test]
async fn head_request_returns_headers_no_body() {
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = tempfile::tempdir().unwrap();
    let config = attachment_config(&dir, &blob_dir);
    let addr = common::mock_addr(41007);

    let file_bytes = vec![b'h'; 321];

    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("POST")
        .uri("/headtopic")
        .header("X-Filename", "data.bin")
        .body(Body::from(file_bytes.clone()))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let published: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let id = published["id"].as_str().unwrap().to_string();

    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("HEAD")
        .uri(format!("/file/{id}"))
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let content_length = resp
        .headers()
        .get("content-length")
        .expect("expected a Content-Length header on HEAD response")
        .to_str()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    assert_eq!(content_length, file_bytes.len());

    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 0, "HEAD response must not include a body");
}

#[tokio::test]
async fn unknown_or_malformed_attachment_id_returns_404_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    let blob_dir = tempfile::tempdir().unwrap();
    let config = attachment_config(&dir, &blob_dir);
    let addr = common::mock_addr(41008);

    // Wrong shape entirely (way shorter than MESSAGE_ID_LEN = 12).
    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("GET")
        .uri("/file/not-a-real-id-at-all")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Right length/shape (12 lowercase alphanumeric chars) but never issued.
    let router = common::test_router(&config, addr);
    let req = Request::builder()
        .method("GET")
        .uri("/file/aaaaaaaaaaaa")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
