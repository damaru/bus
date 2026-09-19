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
    let topics = Arc::new(topic::TopicRegistry::new(config.cache_count as usize));
    let state = http::AppState { store, topics };
    let app = http::router(state);

    let listener = tokio::net::TcpListener::bind(&config.bind).await?;
    tracing::info!(addr = %listener.local_addr()?, "listening");
    axum::serve(listener, app).await?;

    Ok(())
}
