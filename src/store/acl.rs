//! Per-topic ACL storage + resolution, mirroring PLAN.md section 6 and
//! `refs/ntfy/user::Manager`'s `Authorize`/`authorizeTopicAccess` precedence
//! rules (ported from Go, minus the sync-topic/tier special cases which
//! don't exist in this project).

use anyhow::{bail, Context, Result};

use crate::config::DefaultAccess;

/// Wildcard principal representing "any user, including anonymous" — ntfy
/// calls this `Everyone` (`*`).
pub const EVERYONE: &str = "*";

const ACL_TREE: &str = "acl";
/// Separates `principal` from `topic_pattern` within an ACL key. Chosen
/// because neither usernames nor topic patterns may legally contain a NUL
/// byte (enforced in `grant`), so the split is always unambiguous.
const KEY_SEP: u8 = 0;

/// Effective access level for a (principal, topic) pair. Numeric values
/// intentionally match ntfy's `user.Permission` bitmask (`DenyAll=0,
/// Read=1, Write=2, ReadWrite=3`), so the on-disk byte is self-describing
/// and the read/write bit tests are trivial ANDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[repr(u8)]
pub enum Permission {
    #[value(name = "deny")]
    DenyAll = 0,
    #[value(name = "read")]
    Read = 1,
    #[value(name = "write")]
    Write = 2,
    #[value(name = "read-write")]
    ReadWrite = 3,
}

impl Permission {
    pub fn is_read(self) -> bool {
        (self as u8) & (Permission::Read as u8) != 0
    }

    pub fn is_write(self) -> bool {
        (self as u8) & (Permission::Write as u8) != 0
    }

    fn from_byte(b: u8) -> Self {
        match b {
            1 => Permission::Read,
            2 => Permission::Write,
            3 => Permission::ReadWrite,
            _ => Permission::DenyAll,
        }
    }
}

impl From<DefaultAccess> for Permission {
    fn from(d: DefaultAccess) -> Self {
        match d {
            DefaultAccess::DenyAll => Permission::DenyAll,
            DefaultAccess::ReadOnly => Permission::Read,
            DefaultAccess::ReadWrite => Permission::ReadWrite,
        }
    }
}

/// ACL store backed by the `acl` sled tree (key `"{principal}\0{pattern}"`
/// -> single permission byte).
pub struct Acl {
    db: sled::Db,
}

impl Acl {
    pub fn new(db: sled::Db) -> Self {
        Self { db }
    }

    fn tree(&self) -> Result<sled::Tree> {
        self.db.open_tree(ACL_TREE).context("opening acl tree")
    }

    fn key(principal: &str, topic_pattern: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(principal.len() + topic_pattern.len() + 1);
        key.extend_from_slice(principal.as_bytes());
        key.push(KEY_SEP);
        key.extend_from_slice(topic_pattern.as_bytes());
        key
    }

    fn validate(principal: &str, topic_pattern: &str) -> Result<()> {
        if principal.is_empty() || principal.as_bytes().contains(&KEY_SEP) {
            bail!("invalid principal '{principal}'");
        }
        if topic_pattern.is_empty()
            || topic_pattern.len() > 64
            || topic_pattern
                .chars()
                .any(|c| !(c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '*'))
        {
            bail!("invalid topic pattern '{topic_pattern}'");
        }
        Ok(())
    }

    /// Grants (or overwrites) `permission` for `principal` on `topic_pattern`.
    /// `principal` is either a username or [`EVERYONE`] (`"*"`).
    /// `topic_pattern` may contain `*` wildcards anywhere (see
    /// [`topic_matches`]).
    pub fn grant(&self, principal: &str, topic_pattern: &str, permission: Permission) -> Result<()> {
        Self::validate(principal, topic_pattern)?;
        let tree = self.tree()?;
        tree.insert(Self::key(principal, topic_pattern), &[permission as u8])
            .context("inserting acl entry")?;
        Ok(())
    }

    /// Removes a single (principal, topic_pattern) ACL entry, if present.
    pub fn revoke(&self, principal: &str, topic_pattern: &str) -> Result<()> {
        let tree = self.tree()?;
        tree.remove(Self::key(principal, topic_pattern))
            .context("removing acl entry")?;
        Ok(())
    }

    fn all_entries(&self) -> Result<Vec<(String, String, Permission)>> {
        let tree = self.tree()?;
        let mut out = Vec::new();
        for item in tree.iter() {
            let (key, value) = item.context("scanning acl tree")?;
            if let Some(sep) = key.iter().position(|&b| b == KEY_SEP) {
                let principal = String::from_utf8_lossy(&key[..sep]).into_owned();
                let pattern = String::from_utf8_lossy(&key[sep + 1..]).into_owned();
                let perm = Permission::from_byte(value.first().copied().unwrap_or(0));
                out.push((principal, pattern, perm));
            }
        }
        Ok(out)
    }

    /// Resolves the effective permission for `username` (`None` = anonymous)
    /// on `topic`.
    ///
    /// Precedence (mirrors `refs/ntfy/user/manager.go`'s
    /// `authorizeTopicAccess` doc comment):
    ///   1. A specific username beats the `*` (everyone) wildcard,
    ///      regardless of pattern specificity — i.e. any matching
    ///      user-specific entry always outranks any `*` entry.
    ///   2. Among entries for the same principal rank, a longer topic
    ///      pattern beats a shorter one (more specific wins), so
    ///      `"myapp-*"` beats `"*"` for topic `"myapp-prod"`, and an exact
    ///      match (pattern == topic) beats any wildcard pattern.
    ///   3. If nothing matches at all, fall back to `default_access`.
    pub fn resolve(&self, username: Option<&str>, topic: &str, default_access: DefaultAccess) -> Result<Permission> {
        let entries = self.all_entries()?;
        // (principal_specificity, pattern_len, permission) — compared
        // lexicographically so rule 1 always dominates rule 2.
        let mut best: Option<(u8, usize, Permission)> = None;

        for (principal, pattern, perm) in &entries {
            if !topic_matches(pattern, topic) {
                continue;
            }
            let principal_rank: u8 = if Some(principal.as_str()) == username {
                1
            } else if principal == EVERYONE {
                0
            } else {
                continue; // entry belongs to a different specific user
            };
            let candidate = (principal_rank, pattern.len(), *perm);
            best = Some(match best {
                Some(cur) if (cur.0, cur.1) >= (candidate.0, candidate.1) => cur,
                _ => candidate,
            });
        }

        Ok(match best {
            Some((_, _, perm)) => perm,
            None => Permission::from(default_access),
        })
    }
}

/// Minimal glob matcher: `*` matches zero or more characters, anywhere in
/// the pattern (mirrors ntfy's SQL `LIKE`-based topic pattern matching,
/// where `*` is translated to `%`). Covers (at minimum) exact match and a
/// trailing wildcard, e.g. `"myapp-*"` matches `"myapp-prod"`; patterns are
/// ASCII-only (enforced by `Acl::validate`) so all byte-offset slicing here
/// is safe.
fn topic_matches(pattern: &str, topic: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == topic;
    }
    let parts: Vec<&str> = pattern.split('*').collect();
    let last = parts.len() - 1;
    let mut pos = 0usize;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            if !topic[pos..].starts_with(part) {
                return false;
            }
            pos += part.len();
        } else if i == last {
            return topic[pos..].ends_with(part) && topic.len() >= pos + part.len();
        } else {
            match topic[pos..].find(part) {
                Some(found) => pos += found + part.len(),
                None => return false,
            }
        }
    }
    true
}
