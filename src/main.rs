//! CLI entry point: `bus serve`, `bus admin ...`.

mod admin;
mod auth;
mod config;
mod error;
mod http;
mod model;
mod store;
mod topic;

use std::sync::Arc;

use clap::{Parser, Subcommand};

use config::Config;
use store::Store;

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
    /// Administrative subcommands (users, tokens, ACLs).
    Admin {
        #[command(subcommand)]
        cmd: admin::AdminCommand,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Serve(config) => serve(config).await?,
        Command::Admin { cmd } => admin::run(cmd),
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

    tracing::info!(bind = %config.bind, data_dir = ?config.data_dir, "starting bus server");

    let store = Arc::new(Store::open(&config.data_dir)?);
    let cache = Arc::new(store.cache());
    let topics = Arc::new(topic::TopicRegistry::new(config.cache_count as usize, cache.clone()));

    spawn_retention_sweep(cache.clone(), config.cache_duration, config.cache_count, config.cache_size);

    let state = http::AppState { store, topics };
    let app = http::router(state);

    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");
    axum::serve(listener, app).await?;

    Ok(())
}

/// Periodically prunes every known topic's persisted message tree against
/// the configured `cache-duration`/`cache-count`/`cache-size` limits (best-
/// effort for size; see `store::cache::Cache::prune`). Runs on a dedicated
/// blocking task since a full retention pass scans whole topic trees.
fn spawn_retention_sweep(cache: Arc<store::cache::Cache>, max_age: std::time::Duration, max_count: u64, max_size_bytes: u64) {
    const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(SWEEP_INTERVAL);
        loop {
            interval.tick().await;
            let cache = cache.clone();
            let result = tokio::task::spawn_blocking(move || {
                let mut total_pruned = 0usize;
                for topic in cache.topics() {
                    match cache.prune(&topic, max_age, max_count, max_size_bytes) {
                        Ok(n) => total_pruned += n,
                        Err(e) => tracing::warn!(topic = %topic, error = %e, "retention sweep failed for topic"),
                    }
                }
                total_pruned
            })
            .await;
            match result {
                Ok(n) if n > 0 => tracing::debug!(pruned = n, "retention sweep pruned expired/excess messages"),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "retention sweep task panicked"),
            }
        }
    });
}
