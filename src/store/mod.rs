//! `sled::Db` handle wrapper + tree helpers.

use std::path::Path;

pub mod acl;
pub mod attachments;
pub mod cache;
pub mod users;

use acl::Acl;
use attachments::Attachments;
use cache::Cache;
use users::Users;

/// Thin wrapper around the embedded `sled` database handle.
pub struct Store {
    pub db: sled::Db,
}

impl Store {
    /// Opens (or creates) the sled database at `data_dir`.
    pub fn open(data_dir: impl AsRef<Path>) -> sled::Result<Self> {
        let db = sled::open(data_dir)?;
        Ok(Self { db })
    }

    /// Builds a [`Cache`] handle over this store's database. `sled::Db` is
    /// cheaply `Clone` (internally reference-counted), so this is just a
    /// handle copy, not a reopen.
    pub fn cache(&self) -> Cache {
        Cache::new(self.db.clone())
    }

    /// Builds a [`Users`] handle over this store's database.
    pub fn users(&self) -> Users {
        Users::new(self.db.clone())
    }

    /// Builds an [`Acl`] handle over this store's database.
    pub fn acl(&self) -> Acl {
        Acl::new(self.db.clone())
    }

    /// Builds an [`Attachments`] handle over this store's database,
    /// rooted at `dir` on the filesystem. Returns an error if the
    /// directory can't be created or the sled tree can't be opened.
    pub fn attachments(&self, dir: std::path::PathBuf, file_size_limit: u64, total_size_limit: u64) -> anyhow::Result<Attachments> {
        Attachments::open(self.db.clone(), dir, file_size_limit, total_size_limit)
    }
}
