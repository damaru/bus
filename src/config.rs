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

/// Resolves this app's base cache directory: `$XDG_CACHE_HOME/bus` if
/// `XDG_CACHE_HOME` is set and non-empty, else `$HOME/.cache/bus`. Falls
/// back to `./.bus-cache` if neither environment variable is usable (e.g.
/// a minimal container with `$HOME` unset) rather than failing outright.
pub fn cache_dir() -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_CACHE_HOME") {
        if !xdg.is_empty() {
            return PathBuf::from(xdg).join("bus");
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        if !home.is_empty() {
            return PathBuf::from(home).join(".cache").join("bus");
        }
    }
    PathBuf::from("./.bus-cache")
}

/// Default for `--data-dir`: `~/.cache/bus/data` (see [`cache_dir`]).
pub fn default_data_dir() -> PathBuf {
    cache_dir().join("data")
}

/// Default for `--attachment-dir`: `~/.cache/bus/attachments`.
fn default_attachment_dir() -> PathBuf {
    cache_dir().join("attachments")
}

/// Derives a default `--base-url` from `--bind` (e.g. `127.0.0.1:8080` ->
/// `http://127.0.0.1:8080`), used only when `--base-url` isn't given
/// explicitly. A wildcard bind host (`0.0.0.0`, `::`, `[::]`, or empty)
/// isn't reachable by clients as-is, so it's rewritten to `127.0.0.1` —
/// fine for local/single-node use; reverse-proxied or multi-host
/// deployments should still pass `--base-url` explicitly.
fn derive_base_url(bind: &str) -> String {
    let host_for_url = match bind.rsplit_once(':') {
        Some((host, port)) => {
            let host = match host {
                "0.0.0.0" | "" | "::" | "[::]" => "127.0.0.1",
                other => other,
            };
            format!("{host}:{port}")
        }
        None => bind.to_string(),
    };
    format!("http://{host_for_url}")
}

/// Server configuration, populated from CLI flags on `bus serve`.
#[derive(Debug, Clone, Args)]
pub struct Config {
    /// Address (and port) to bind the HTTP server to.
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub bind: String,

    /// Directory for the sled embedded database. Defaults to
    /// `~/.cache/bus/data` (`$XDG_CACHE_HOME/bus/data` if set).
    #[arg(long, default_value_os_t = default_data_dir())]
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

    /// Max publish requests allowed per 60-second window, per authenticated
    /// user (falling back to per-IP for anonymous publishers). Separate
    /// from the auth-failure limiter (M3): this one throttles legitimate,
    /// frequent publish traffic, not bad credentials. Inspired by ntfy's
    /// `DefaultVisitorRequestLimitBurst = 60` general per-visitor request
    /// budget (`refs/ntfy/server/config.go`); reused directly as a
    /// reasonable "messages per minute" default for a single-node deploy.
    #[arg(long, default_value_t = 60)]
    pub publish_rate_limit: u32,

    /// Max size (bytes) of a single published message: enforced both on
    /// `PUT`/`POST /{topic}` request bodies (via axum's `DefaultBodyLimit`)
    /// and on incoming `/bus` WebSocket text frames (M4/M6), so publishing
    /// the same oversized content is rejected consistently regardless of
    /// transport. Default matches M0's original hardcoded HTTP body limit
    /// (1 MiB) — well above ntfy's `DefaultMessageSizeLimit` of 4096 bytes
    /// (which exists to fit FCM/APNS push payloads, a constraint this
    /// project doesn't have) and above M5's 256 KiB e2e-ciphertext cap.
    #[arg(long, default_value_t = 1024 * 1024)]
    pub max_message_bytes: u64,

    /// Max distinct topics the server will create via `get_or_create`
    /// before refusing new ones (existing topics are never blocked).
    /// Protects against unbounded topic-name abuse. Scaled down from
    /// ntfy.sh's own `DefaultTotalTopicLimit = 15000` (a multi-tenant
    /// public-instance number) to a more conservative default for a
    /// typical single-node/self-hosted deployment of this project.
    #[arg(long, default_value_t = 1000)]
    pub max_topics: usize,

    /// Max concurrent subscriber connections (any of `/json`/`/sse`/`/raw`/
    /// `/ws`/`/bus`) on a single topic.
    #[arg(long, default_value_t = 1000)]
    pub max_subscribers_per_topic: u64,

    /// Max concurrent subscriber connections across the whole server (all
    /// topics combined) — a global soft cap layered on top of the
    /// per-topic cap, per PLAN.md M6 guidance ("a per-topic cap composed
    /// into a global soft cap is reasonable").
    #[arg(long, default_value_t = 10_000)]
    pub max_subscribers_total: u64,

    /// Max concurrent `/bus` (full-duplex chat) participants on a single
    /// topic — tighter than the generic subscriber cap since a "chat room"
    /// with thousands of full-duplex participants is a different (and
    /// much rarer/heavier) use case than thousands of passive readers.
    #[arg(long, default_value_t = 200)]
    pub max_bus_participants_per_topic: u64,

    /// Bounded grace period for graceful shutdown: after SIGINT/SIGTERM,
    /// the server stops accepting new connections and gives existing
    /// streams/WS connections this long to drain before forcing exit.
    #[arg(long, default_value_t = 10)]
    pub shutdown_grace_secs: u64,

    /// Directory to store uploaded file attachments. Defaults to
    /// `~/.cache/bus/attachments` unless `--no-attachments` is set; either
    /// `--attachment-dir` or `--base-url` alone is enough to enable
    /// attachments (the other gets its own default filled in). Populated by
    /// [`Config::finalize`] after CLI parsing — stays unset (attachments
    /// disabled) via `Config::default()`, which the test suite uses.
    #[arg(long)]
    pub attachment_dir: Option<PathBuf>,

    /// Public base URL clients use to reach this server (e.g.
    /// "https://bus.example.com"), used to build attachment download URLs.
    /// Defaults to a URL derived from `--bind` unless `--no-attachments` is
    /// set. See `attachment_dir` for how the two combine.
    #[arg(long)]
    pub base_url: Option<String>,

    /// Disables file attachments outright, even if `--attachment-dir`
    /// and/or `--base-url` are also given. The only way to opt out now that
    /// both have defaults.
    #[arg(long, default_value_t = false)]
    pub no_attachments: bool,

    /// Max size (bytes) of a single uploaded attachment.
    #[arg(long, default_value_t = 15 * 1024 * 1024)]
    pub attachment_file_size_limit: u64,

    /// Max total size (bytes) of all stored attachments combined.
    #[arg(long, default_value_t = 5 * 1024 * 1024 * 1024)]
    pub attachment_total_size_limit: u64,

    /// How long an uploaded attachment is retained before being reclaimed
    /// (independent of, and typically shorter than, the message cache
    /// retention duration).
    #[arg(long, default_value = "3h", value_parser = parse_duration_str)]
    pub attachment_expiry: std::time::Duration,
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
            publish_rate_limit: 60,
            max_message_bytes: 1024 * 1024,
            max_topics: 1000,
            max_subscribers_per_topic: 1000,
            max_subscribers_total: 10_000,
            max_bus_participants_per_topic: 200,
            shutdown_grace_secs: 10,
            attachment_dir: None,
            base_url: None,
            no_attachments: false,
            attachment_file_size_limit: 15 * 1024 * 1024,
            attachment_total_size_limit: 5 * 1024 * 1024 * 1024,
            attachment_expiry: std::time::Duration::from_secs(3 * 3600),
        }
    }
}

impl Config {
    /// Fills in environment-derived defaults that a static `clap`
    /// `default_value` can't express: `attachment_dir` depends on the home
    /// directory, and `base_url` is derived from the (possibly
    /// user-overridden) `--bind` value. Called once after CLI parsing, in
    /// `main.rs`, before `validate()`/`build_state()`. A no-op when
    /// `--no-attachments` is set — attachments stay off in that case
    /// regardless of what else was passed. Not called by `Config::default()`
    /// (used by the test suite), which leaves attachments off unless a test
    /// opts in explicitly.
    pub fn finalize(&mut self) {
        if self.no_attachments {
            self.attachment_dir = None;
            self.base_url = None;
            return;
        }
        if self.attachment_dir.is_none() {
            self.attachment_dir = Some(default_attachment_dir());
        }
        if self.base_url.is_none() {
            self.base_url = Some(derive_base_url(&self.bind));
        }
    }

    /// Fails fast if exactly one of `attachment_dir`/`base_url` is set
    /// without the other — attachments require both or neither. Normally
    /// unreachable in `bus serve` (call `finalize()` first, which always
    /// leaves both set or both unset); kept as a defensive check for
    /// library callers that build `Config` by hand and skip `finalize()`.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.attachment_dir.is_some() != self.base_url.is_some() {
            anyhow::bail!(
                "--attachment-dir and --base-url must both be set to enable attachments, or both left unset to disable them"
            );
        }
        Ok(())
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
