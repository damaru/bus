//! Server configuration: bind address, data directory, cache limits, and
//! default-access policy. Parsed via `clap` when running `bus serve`.

use std::path::PathBuf;

use clap::Args;

/// Default-access policy applied when no explicit ACL entry matches a
/// (principal, topic) pair. Full enforcement lands in M3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum DefaultAccess {
    #[default]
    ReadWrite,
    ReadOnly,
    DenyAll,
}

/// Server configuration, populated from CLI flags on `bus serve`.
#[derive(Debug, Clone, Args)]
pub struct Config {
    /// Address (and port) to bind the HTTP server to.
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub bind: String,

    /// Directory for the sled embedded database.
    #[arg(long, default_value = "./data")]
    pub data_dir: PathBuf,

    /// How long cached messages are retained before being pruned.
    #[arg(long, default_value = "12h", value_parser = parse_duration_str)]
    pub cache_duration: std::time::Duration,

    /// Maximum total size (bytes) of the message cache.
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    pub cache_size: u64,

    /// Maximum number of cached messages per topic.
    #[arg(long, default_value_t = 10_000)]
    pub cache_count: u64,

    /// Default access policy when no ACL entry matches.
    #[arg(long, value_enum, default_value_t = DefaultAccess::ReadWrite)]
    pub default_access: DefaultAccess,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:8080".to_string(),
            data_dir: PathBuf::from("./data"),
            cache_duration: std::time::Duration::from_secs(12 * 3600),
            cache_size: 1024 * 1024 * 1024,
            cache_count: 10_000,
            default_access: DefaultAccess::ReadWrite,
        }
    }
}

/// Parses simple duration strings like `12h`, `30m`, `45s`, or a bare
/// number of seconds. Minimal parser, good enough for config defaults.
fn parse_duration_str(s: &str) -> Result<std::time::Duration, String> {
    let s = s.trim();
    if let Some(num) = s.strip_suffix('h') {
        let n: u64 = num.parse().map_err(|_| format!("invalid duration: {s}"))?;
        return Ok(std::time::Duration::from_secs(n * 3600));
    }
    if let Some(num) = s.strip_suffix('m') {
        let n: u64 = num.parse().map_err(|_| format!("invalid duration: {s}"))?;
        return Ok(std::time::Duration::from_secs(n * 60));
    }
    if let Some(num) = s.strip_suffix('s') {
        let n: u64 = num.parse().map_err(|_| format!("invalid duration: {s}"))?;
        return Ok(std::time::Duration::from_secs(n));
    }
    let n: u64 = s.parse().map_err(|_| format!("invalid duration: {s}"))?;
    Ok(std::time::Duration::from_secs(n))
}
