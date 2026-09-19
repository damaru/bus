//! `sled::Db` handle wrapper + tree helpers.

use std::path::Path;

pub mod acl;
pub mod cache;
pub mod users;

use cache::Cache;

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
}
