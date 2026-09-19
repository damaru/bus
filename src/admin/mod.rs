//! `bus admin` subcommands: `user add|passwd|del`, `token issue|revoke`,
//! `acl grant|revoke`.
//!
//! Placeholder for M0 — full implementation lands in M3.

use clap::Subcommand;

/// `bus admin` subcommands. Stubbed out for M0; each variant prints
/// "not yet implemented" until M3 wires it up against the sled store.
#[derive(Debug, Subcommand)]
pub enum AdminCommand {
    /// User management (add, passwd, del) — stub until M3.
    User,
    /// Token management (issue, revoke) — stub until M3.
    Token,
    /// ACL management (grant, revoke) — stub until M3.
    Acl,
}

/// Entry point for `bus admin ...`. Currently a stub for all subcommands.
pub fn run(cmd: AdminCommand) {
    match cmd {
        AdminCommand::User => println!("bus admin user: not yet implemented"),
        AdminCommand::Token => println!("bus admin token: not yet implemented"),
        AdminCommand::Acl => println!("bus admin acl: not yet implemented"),
    }
}
