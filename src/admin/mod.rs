//! `bus admin` subcommands: `user add|passwd|del`, `token issue|revoke`,
//! `acl grant|revoke`. Operate directly on the sled DB at the configured
//! `--data-dir` — PLAN.md section 6's "no HTTP admin API in v1" design.
//!
//! Since sled locks its data directory to a single process, the server
//! must not be running against the same `--data-dir` while an admin
//! command runs (stop the server, run the command, restart it).

use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use clap::Subcommand;

use crate::store::acl::{Acl, Permission};
use crate::store::users::Role;
use crate::store::Store;

/// `bus admin` subcommands.
#[derive(Debug, Subcommand)]
pub enum AdminCommand {
    /// User management (add, passwd, del).
    User {
        #[command(subcommand)]
        cmd: UserCommand,
    },
    /// Bearer token management (issue, revoke).
    Token {
        #[command(subcommand)]
        cmd: TokenCommand,
    },
    /// Per-topic ACL management (grant, revoke).
    Acl {
        #[command(subcommand)]
        cmd: AclCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum UserCommand {
    /// Create a new user.
    Add {
        username: String,
        /// Password; prompted on stdin if omitted.
        #[arg(long)]
        password: Option<String>,
        /// Role for the new user.
        #[arg(long, value_enum, default_value = "user")]
        role: Role,
    },
    /// Change an existing user's password.
    Passwd {
        username: String,
        /// New password; prompted on stdin if omitted.
        #[arg(long)]
        password: Option<String>,
    },
    /// Delete a user and revoke all of their tokens.
    Del { username: String },
}

#[derive(Debug, Subcommand)]
pub enum TokenCommand {
    /// Issue a new bearer token for a user; prints the token to stdout.
    Issue {
        username: String,
        /// Time-to-live, e.g. `30d`/`12h`/`45m`/`90s`. Never expires if omitted.
        #[arg(long)]
        ttl: Option<String>,
    },
    /// Revoke a bearer token outright.
    Revoke { token: String },
}

#[derive(Debug, Subcommand)]
pub enum AclCommand {
    /// Grant a permission for a principal (username, or `*` for everyone)
    /// on a topic pattern (`*` wildcards allowed, e.g. `myapp-*`).
    Grant {
        principal: String,
        topic_pattern: String,
        #[arg(value_enum)]
        permission: Permission,
    },
    /// Revoke a principal's ACL entry for a topic pattern.
    Revoke { principal: String, topic_pattern: String },
}

/// Entry point for `bus admin ...`, operating on the sled DB at `data_dir`.
pub fn run(cmd: AdminCommand, data_dir: PathBuf) -> anyhow::Result<()> {
    let store = Store::open(&data_dir)?;
    match cmd {
        AdminCommand::User { cmd } => run_user(&store, cmd),
        AdminCommand::Token { cmd } => run_token(&store, cmd),
        AdminCommand::Acl { cmd } => run_acl(&store, cmd),
    }
}

fn run_user(store: &Store, cmd: UserCommand) -> anyhow::Result<()> {
    let users = store.users();
    match cmd {
        UserCommand::Add { username, password, role } => {
            let password = password.unwrap_or_else(|| prompt_password("Password: "));
            users.create(&username, &password, role)?;
            println!("created user '{username}' (role: {role:?})");
        }
        UserCommand::Passwd { username, password } => {
            let password = password.unwrap_or_else(|| prompt_password("New password: "));
            users.set_password(&username, &password)?;
            println!("updated password for user '{username}'");
        }
        UserCommand::Del { username } => {
            users.delete(&username)?;
            println!("deleted user '{username}'");
        }
    }
    Ok(())
}

fn run_token(store: &Store, cmd: TokenCommand) -> anyhow::Result<()> {
    let users = store.users();
    match cmd {
        TokenCommand::Issue { username, ttl } => {
            let ttl = ttl.as_deref().map(parse_ttl).transpose()?;
            let token = users.issue_token(&username, ttl)?;
            println!("{token}");
        }
        TokenCommand::Revoke { token } => {
            users.revoke_token(&token)?;
            println!("revoked token");
        }
    }
    Ok(())
}

fn run_acl(store: &Store, cmd: AclCommand) -> anyhow::Result<()> {
    let acl = store.acl();
    match cmd {
        AclCommand::Grant { principal, topic_pattern, permission } => {
            grant(&acl, &principal, &topic_pattern, permission)?;
            println!("granted {permission:?} to '{principal}' on '{topic_pattern}'");
        }
        AclCommand::Revoke { principal, topic_pattern } => {
            acl.revoke(&principal, &topic_pattern)?;
            println!("revoked ACL entry for '{principal}' on '{topic_pattern}'");
        }
    }
    Ok(())
}

fn grant(acl: &Acl, principal: &str, topic_pattern: &str, permission: Permission) -> anyhow::Result<()> {
    acl.grant(principal, topic_pattern, permission)
}

/// Prompts for a password on stdin. Not hidden/no-echo (would require an
/// extra terminal-raw-mode dependency such as `rpassword`, deliberately
/// skipped to keep the dependency list matching PLAN.md section 9)
/// — documented simplification; prefer `--password` for scripted use.
fn prompt_password(prompt: &str) -> String {
    print!("{prompt}");
    std::io::stdout().flush().ok();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).expect("reading password from stdin");
    line.trim_end_matches(['\r', '\n']).to_string()
}

/// Parses a simple duration string (`<n>d`/`<n>h`/`<n>m`/`<n>s`) for
/// `token issue --ttl`.
fn parse_ttl(s: &str) -> anyhow::Result<Duration> {
    let s = s.trim();
    let (num, mult) = if let Some(n) = s.strip_suffix('d') {
        (n, 86_400)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3_600)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1)
    } else {
        (s, 1)
    };
    let n: u64 = num.parse().map_err(|_| anyhow::anyhow!("invalid ttl: {s}"))?;
    Ok(Duration::from_secs(n * mult))
}
