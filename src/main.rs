//! CLI entry point: `bus serve`, `bus admin ...`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};

use bus::admin;
use bus::config::Config;

/// Cadence shared by both background sweeps: the message-cache retention
/// pass (`spawn_retention_sweep`) and the attachment expiry pass
/// (`spawn_attachment_expiry_sweep`). Only the expiry *threshold* differs
/// per sweep (`cache_duration`/`cache_count`/`cache_size` vs
/// `Config::attachment_expiry`) — there's no separate configurable
/// interval for attachments.
const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Debug, Parser)]
#[command(name = "bus", about = "Minimal ntfy-compatible pub/sub server with a bidirectional bus extension")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the HTTP/WS server.
    Serve(Config),
    /// Administrative subcommands (users, tokens, ACLs). Operates directly
    /// on the sled DB at `--data-dir`; stop `bus serve` first if it's
    /// running against the same directory (sled allows only one process).
    Admin {
        #[command(subcommand)]
        cmd: admin::AdminCommand,
        /// Directory of the sled embedded database (same as `bus serve --data-dir`).
        #[arg(long, global = true, default_value = "./data")]
        data_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Serve(config) => serve(config).await?,
        Command::Admin { cmd, data_dir } => admin::run(cmd, data_dir)?,
    }

    Ok(())
}

async fn serve(config: Config) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    config.validate()?;

    tracing::info!(bind = %config.bind, data_dir = ?config.data_dir, "starting bus server");

    // Single source of truth for wiring store/topics/auth/etc together —
    // shared with the integration test suite via `bus::build_state`.
    let state = bus::build_state(&config)?;
    let store = state.store.clone();
    let topics = state.topics.clone();
    let attachments = state.attachments.clone();
    // Cache is cheap to re-derive from the store (just clones the sled::Db
    // handle) — used here only by the retention sweep, which isn't part of
    // `AppState` itself.
    let cache = Arc::new(store.cache());

    spawn_retention_sweep(
        cache,
        config.cache_duration,
        config.cache_count,
        config.cache_size,
        attachments.clone(),
    );
    if let Some(a) = attachments.clone() {
        spawn_attachment_expiry_sweep(a);
    }

    let app = bus::http::router(state);

    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");

    let shutdown_grace = std::time::Duration::from_secs(config.shutdown_grace_secs);

    // Graceful shutdown (M6) is split into two distinct waits, since they
    // have very different expected durations:
    //   1. Waiting for SIGINT/SIGTERM is *unbounded* — that's just the
    //      server running normally, for however long the operator wants.
    //   2. Waiting for in-flight connections to drain, *after* the signal
    //      arrives, must be *bounded* (a "reasonable grace period"), or a
    //      client that never closes its stream would hang the process
    //      forever.
    // A single `tokio::time::timeout` around the whole `axum::serve(...)`
    // future would incorrectly start counting from process start, not
    // from when a shutdown was actually requested — so instead, the serve
    // future is driven by a separate task, and the bounded timeout only
    // wraps waiting on *that task's completion*, starting only after the
    // signal has already been received.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let mut serve_task = tokio::spawn(async move {
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    wait_for_shutdown_signal().await;
    tracing::info!("shutdown signal received, broadcasting close to active /bus participants");
    topics.broadcast_shutdown_close();
    // Tell axum to stop accepting new connections and start draining
    // existing ones; ignore the error if the serve task already exited.
    let _ = shutdown_tx.send(());

    // `tokio::select!` (rather than `tokio::time::timeout`) so that on a
    // timeout we still hold the `JoinHandle` and can `.abort()` it —
    // `timeout()` would drop the handle without aborting the task,
    // leaving the listener/connections to be torn down only implicitly
    // (and only eventually) by process exit.
    tokio::select! {
        result = &mut serve_task => {
            match result {
                Ok(Ok(())) => tracing::info!("server shut down gracefully"),
                Ok(Err(e)) => tracing::error!(error = %e, "server error during shutdown"),
                Err(join_err) => tracing::error!(error = %join_err, "server task panicked during shutdown"),
            }
        }
        _ = tokio::time::sleep(shutdown_grace) => {
            tracing::warn!(
                grace_secs = config.shutdown_grace_secs,
                "graceful shutdown grace period elapsed, forcing remaining connections closed"
            );
            serve_task.abort();
        }
    }

    // Flush sled before exiting so any buffered writes are durable on
    // disk, not just in the shared page cache — cheap and quick even if
    // periodic/Drop-time flushing would eventually do the same.
    if let Err(e) = tokio::task::spawn_blocking(move || store.db.flush()).await {
        tracing::warn!(error = %e, "sled flush task panicked during shutdown");
    }

    Ok(())
}

/// Resolves once SIGINT (Ctrl+C) or SIGTERM is received. Deliberately just
/// the signal wait — broadcasting the `close` control message and
/// triggering axum's graceful shutdown both happen in `serve()`, right
/// after this returns, so the *bounded* grace-period timeout in `serve()`
/// only starts counting from here, not from process start.
async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received SIGINT, starting graceful shutdown"),
        _ = terminate => tracing::info!("received SIGTERM, starting graceful shutdown"),
    }
}

/// Periodically prunes every known topic's persisted message tree against
/// the configured `cache-duration`/`cache-count`/`cache-size` limits (best-
/// effort for size; see `store::cache::Cache::prune`). Runs on a dedicated
/// blocking task since a full retention pass scans whole topic trees. Any
/// pruned entries that carried an attachment have their now-orphaned blob
/// reclaimed too (via `attachments`, if attachments are enabled), since
/// otherwise it would linger on disk until its own independent expiry.
fn spawn_retention_sweep(
    cache: Arc<bus::store::cache::Cache>,
    max_age: std::time::Duration,
    max_count: u64,
    max_size_bytes: u64,
    attachments: Option<Arc<bus::store::attachments::Attachments>>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(SWEEP_INTERVAL);
        loop {
            interval.tick().await;
            let cache = cache.clone();
            let result = tokio::task::spawn_blocking(move || {
                let mut total_pruned = 0usize;
                let mut all_orphaned_ids: Vec<String> = Vec::new();
                for topic in cache.topics() {
                    match cache.prune(&topic, max_age, max_count, max_size_bytes) {
                        Ok(res) => {
                            total_pruned += res.removed;
                            all_orphaned_ids.extend(res.orphaned_attachment_ids);
                        }
                        Err(e) => tracing::warn!(topic = %topic, error = %e, "retention sweep failed for topic"),
                    }
                }
                (total_pruned, all_orphaned_ids)
            })
            .await;
            match result {
                Ok((n, orphaned_ids)) => {
                    if n > 0 {
                        tracing::debug!(pruned = n, "retention sweep pruned expired/excess messages");
                    }
                    if let Some(a) = &attachments {
                        for id in orphaned_ids {
                            if let Err(e) = a.delete(&id).await {
                                tracing::warn!(id = %id, error = %e, "failed to delete orphaned attachment blob");
                            }
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, "retention sweep task panicked"),
            }
        }
    });
}

/// Periodically reclaims attachment blobs whose `expires` timestamp has
/// passed (independent of, and typically shorter than, the message cache
/// retention duration — see `Config::attachment_expiry`). Pure async I/O
/// (sled + filesystem, both already behind async fns), so this runs on
/// the normal tokio runtime rather than `spawn_blocking`.
fn spawn_attachment_expiry_sweep(attachments: Arc<bus::store::attachments::Attachments>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(SWEEP_INTERVAL);
        loop {
            interval.tick().await;
            let expired = attachments.list_expired(bus::model::now_unix());
            let mut deleted = 0usize;
            for id in expired {
                match attachments.delete(&id).await {
                    Ok(()) => deleted += 1,
                    Err(e) => tracing::warn!(id = %id, error = %e, "failed to delete expired attachment"),
                }
            }
            if deleted > 0 {
                tracing::debug!(deleted, "attachment expiry sweep deleted expired attachments");
            }
        }
    });
}
