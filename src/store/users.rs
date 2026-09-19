//! User + token CRUD, argon2 password hashing/verification. Mirrors
//! PLAN.md section 8's `users`/`tokens` trees and (a trimmed-down port of)
//! `refs/ntfy/user`'s shape, minus tiers/billing/email.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::model::now_unix;

const USERS_TREE: &str = "users";
const TOKENS_TREE: &str = "tokens";

/// Matches ntfy's `tk_` token convention: a short fixed prefix plus a
/// random lowercase-alphanumeric body, 32 characters total.
const TOKEN_PREFIX: &str = "tk_";
const TOKEN_RANDOM_LEN: usize = 29;
const TOKEN_CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

/// A user's role. Admins bypass ACL checks entirely (see `store::acl`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    #[value(name = "user")]
    User,
    #[value(name = "admin")]
    Admin,
}

/// On-disk shape of the `users` tree value.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredUser {
    password_hash: String,
    role: Role,
}

/// A resolved user, returned by password/token verification and lookups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub username: String,
    pub role: Role,
}

impl User {
    pub fn is_admin(&self) -> bool {
        self.role == Role::Admin
    }
}

/// On-disk shape of the `tokens` tree value.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredToken {
    username: String,
    expires: Option<i64>,
    last_access: i64,
    #[allow(dead_code)]
    last_origin: Option<String>,
}

/// User/token store backed by the `users`/`tokens` sled trees.
pub struct Users {
    db: sled::Db,
}

impl Users {
    pub fn new(db: sled::Db) -> Self {
        Self { db }
    }

    fn users_tree(&self) -> Result<sled::Tree> {
        self.db.open_tree(USERS_TREE).context("opening users tree")
    }

    fn tokens_tree(&self) -> Result<sled::Tree> {
        self.db.open_tree(TOKENS_TREE).context("opening tokens tree")
    }

    /// Creates a new user with an argon2-hashed password. Fails if the
    /// username already exists.
    pub fn create(&self, username: &str, password: &str, role: Role) -> Result<()> {
        let tree = self.users_tree()?;
        if tree.contains_key(username.as_bytes()).context("checking existing user")? {
            bail!("user '{username}' already exists");
        }
        let stored = StoredUser {
            password_hash: Self::hash_password(password)?,
            role,
        };
        tree.insert(username.as_bytes(), serde_json::to_vec(&stored)?)
            .context("inserting user")?;
        Ok(())
    }

    /// Looks up a user by username without checking any credentials.
    pub fn get(&self, username: &str) -> Result<Option<User>> {
        let tree = self.users_tree()?;
        match tree.get(username.as_bytes()).context("reading user")? {
            Some(bytes) => {
                let stored: StoredUser = serde_json::from_slice(&bytes).context("decoding user")?;
                Ok(Some(User {
                    username: username.to_string(),
                    role: stored.role,
                }))
            }
            None => Ok(None),
        }
    }

    /// Verifies `password` against the stored argon2 hash for `username`.
    /// Returns `Ok(None)` (not an error) for either an unknown user or a
    /// wrong password — callers should treat both identically to avoid
    /// leaking which case occurred.
    pub fn verify_password(&self, username: &str, password: &str) -> Result<Option<User>> {
        let tree = self.users_tree()?;
        let Some(bytes) = tree.get(username.as_bytes()).context("reading user")? else {
            return Ok(None);
        };
        let stored: StoredUser = serde_json::from_slice(&bytes).context("decoding user")?;
        let parsed = PasswordHash::new(&stored.password_hash)
            .map_err(|e| anyhow::anyhow!("corrupt password hash for '{username}': {e}"))?;
        if Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok() {
            Ok(Some(User {
                username: username.to_string(),
                role: stored.role,
            }))
        } else {
            Ok(None)
        }
    }

    /// Re-hashes and stores a new password for an existing user.
    pub fn set_password(&self, username: &str, new_password: &str) -> Result<()> {
        let tree = self.users_tree()?;
        let existing = tree
            .get(username.as_bytes())
            .context("reading user")?
            .with_context(|| format!("user '{username}' not found"))?;
        let mut stored: StoredUser = serde_json::from_slice(&existing).context("decoding user")?;
        stored.password_hash = Self::hash_password(new_password)?;
        tree.insert(username.as_bytes(), serde_json::to_vec(&stored)?)
            .context("updating password")?;
        Ok(())
    }

    /// Deletes a user and revokes every token issued to them.
    pub fn delete(&self, username: &str) -> Result<()> {
        let tree = self.users_tree()?;
        tree.remove(username.as_bytes()).context("deleting user")?;

        let tokens = self.tokens_tree()?;
        let mut stale = Vec::new();
        for item in tokens.iter() {
            let (key, value) = item.context("scanning tokens")?;
            let stored: StoredToken = serde_json::from_slice(&value).context("decoding token")?;
            if stored.username == username {
                stale.push(key);
            }
        }
        for key in stale {
            tokens.remove(key).context("revoking token during user delete")?;
        }
        Ok(())
    }

    /// Issues a new random bearer token for `username`, optionally expiring
    /// after `ttl`. Fails if the user doesn't exist.
    pub fn issue_token(&self, username: &str, ttl: Option<Duration>) -> Result<String> {
        if self.get(username)?.is_none() {
            bail!("user '{username}' not found");
        }
        let token = Self::generate_token();
        let now = now_unix();
        let stored = StoredToken {
            username: username.to_string(),
            expires: ttl.map(|d| now + d.as_secs() as i64),
            last_access: now,
            last_origin: None,
        };
        let tree = self.tokens_tree()?;
        tree.insert(token.as_bytes(), serde_json::to_vec(&stored)?)
            .context("inserting token")?;
        Ok(token)
    }

    /// Verifies a bearer token: checks expiry (and garbage-collects it if
    /// expired), bumps `last_access`, and resolves the owning user.
    pub fn verify_token(&self, token: &str) -> Result<Option<User>> {
        let tokens = self.tokens_tree()?;
        let Some(bytes) = tokens.get(token.as_bytes()).context("reading token")? else {
            return Ok(None);
        };
        let mut stored: StoredToken = serde_json::from_slice(&bytes).context("decoding token")?;
        let now = now_unix();
        if let Some(expires) = stored.expires {
            if now >= expires {
                tokens.remove(token.as_bytes()).context("removing expired token")?;
                return Ok(None);
            }
        }
        stored.last_access = now;
        tokens
            .insert(token.as_bytes(), serde_json::to_vec(&stored)?)
            .context("updating token last_access")?;
        self.get(&stored.username)
    }

    /// Revokes (deletes) a token outright.
    pub fn revoke_token(&self, token: &str) -> Result<()> {
        let tokens = self.tokens_tree()?;
        tokens.remove(token.as_bytes()).context("revoking token")?;
        Ok(())
    }

    fn hash_password(password: &str) -> Result<String> {
        let salt = SaltString::generate(&mut OsRng);
        let hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| anyhow::anyhow!("hashing password: {e}"))?;
        Ok(hash.to_string())
    }

    fn generate_token() -> String {
        let mut rng = rand::thread_rng();
        let random: String = (0..TOKEN_RANDOM_LEN)
            .map(|_| TOKEN_CHARSET[rng.gen_range(0..TOKEN_CHARSET.len())] as char)
            .collect();
        format!("{TOKEN_PREFIX}{random}")
    }
}
