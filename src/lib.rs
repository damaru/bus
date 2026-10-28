//! Library crate for `bus` — exposes every module publicly so both the
//! `bus` binary (`src/main.rs`) and the integration test suite
//! (`tests/*.rs`) share exactly one source of truth for how the server is
//! assembled, rather than each reimplementing the wiring in
//! `main.rs::serve()`.

pub mod admin;
pub mod auth;
pub mod config;
pub mod error;
pub mod http;
pub mod model;
pub mod store;
pub mod topic;

use std::sync::Arc;

use config::Config;
use store::Store;

/// Builds the full [`http::AppState`] (sled store, topic registry,
/// users/ACL, rate limiters) from a [`Config`] — the single canonical
/// "wire everything together" function used by both `bus serve` and the
/// integration tests, so there's no risk of the two drifting apart.
pub fn build_state(config: &Config) -> anyhow::Result<http::AppState> {
    let store = Arc::new(Store::open(&config.data_dir)?);
    let cache = Arc::new(store.cache());
    let topic_limits = topic::TopicLimits {
        max_topics: config.max_topics,
        max_subscribers_per_topic: config.max_subscribers_per_topic,
        max_subscribers_total: config.max_subscribers_total,
        max_bus_participants_per_topic: config.max_bus_participants_per_topic,
    };
    let topics = Arc::new(topic::TopicRegistry::new(config.cache_count as usize, cache, topic_limits).with_history(config.history));
    let users = Arc::new(store.users());
    let acl = Arc::new(store.acl());
    if let Some(path) = &config.admin_token_file {
        bootstrap_admin(&users, path)?;
    }
    let auth_limiter = Arc::new(auth::AuthLimiter::new());
    let publish_limiter = Arc::new(auth::PublishLimiter::new(config.publish_rate_limit));

    let attachments = match &config.attachment_dir {
        Some(dir) if config.base_url.is_some() => Some(Arc::new(store.attachments(
            dir.clone(),
            config.attachment_file_size_limit,
            config.attachment_total_size_limit,
        )?)),
        _ => None,
    };

    Ok(http::AppState {
        store,
        topics,
        users,
        acl,
        auth_limiter,
        publish_limiter,
        default_access: config.default_access,
        max_message_bytes: config.max_message_bytes,
        attachments,
        base_url: config.base_url.clone(),
        attachment_expiry: config.attachment_expiry,
    })
}

fn bootstrap_admin(users: &store::users::Users, path: &std::path::Path) -> anyhow::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    if users.get("root")?.is_some() {
        users.delete("root")?;
    }
    let pw = format!("{:032x}{:032x}", rand::random::<u128>(), rand::random::<u128>());
    users.create("root", &pw, store::users::Role::Admin)?;
    let tok = users.issue_token("root", None)?;

    let dir = match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    };
    std::fs::create_dir_all(&dir)?;
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    let tmp = dir.join(name);
    let _ = std::fs::remove_file(&tmp);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)?;
    f.write_all(format!("{tok}\n").as_bytes())?;
    f.sync_all()?;
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Builds the top-level axum [`axum::Router`] directly from a [`Config`] —
/// a thin convenience wrapper around [`build_state`] + [`http::router`].
/// This is the function both `bus serve` and every integration test under
/// `tests/` call to get a real, fully-wired router.
pub fn build_router(config: &Config) -> anyhow::Result<axum::Router> {
    Ok(http::router(build_state(config)?))
}

/// Serves `app` on a Unix domain socket at `path` with owner-only mode.
/// Each connection is run with a loopback `ConnectInfo`, since local
/// clients are trusted the way loopback TCP clients are. With `idle`
/// set, returns once no connection has been open for that long.
pub async fn serve_unix(
    path: &std::path::Path,
    app: axum::Router,
    idle: Option<std::time::Duration>,
) -> anyhow::Result<()> {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder;
    use hyper_util::service::TowerToHyperService;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt;

    let _ = std::fs::remove_file(path);
    let listener = tokio::net::UnixListener::bind(path)?;
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    let active = std::sync::Arc::new(AtomicUsize::new(0));
    let idle_watch = {
        let active = active.clone();
        async move {
            let Some(d) = idle else {
                return std::future::pending::<()>().await;
            };
            let mut since = tokio::time::Instant::now();
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if active.load(Ordering::SeqCst) > 0 {
                    since = tokio::time::Instant::now();
                } else if since.elapsed() >= d {
                    return;
                }
            }
        }
    };
    tokio::pin!(idle_watch);
    loop {
        let (stream, _) = tokio::select! {
            r = listener.accept() => r?,
            _ = &mut idle_watch => {
                tracing::info!("idle timeout reached, exiting");
                return Ok(());
            }
        };
        let svc = TowerToHyperService::new(app.clone().map_request(
            |r: axum::http::Request<hyper::body::Incoming>| {
                let mut r = r.map(axum::body::Body::new);
                r.extensions_mut().insert(axum::extract::ConnectInfo(std::net::SocketAddr::from((
                    [127, 0, 0, 1],
                    0,
                ))));
                r
            },
        ));
        active.fetch_add(1, Ordering::SeqCst);
        let guard_active = active.clone();
        tokio::spawn(async move {
            if let Err(e) = Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(TokioIo::new(stream), svc)
                .await
            {
                tracing::debug!(error = %e, "unix connection closed");
            }
            guard_active.fetch_sub(1, Ordering::SeqCst);
        });
    }
}
