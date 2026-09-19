//! `sled::Db` handle wrapper + tree helpers.

use std::path::Path;

pub mod acl;
pub mod cache;
pub mod users;

/// Thin wrapper around the embedded `sled` database handle.
pub struct Store {
    #[allow(dead_code)]
    pub db: sled::Db,
}

impl Store {
    /// Opens (or creates) the sled database at `data_dir`.
    pub fn open(data_dir: impl AsRef<Path>) -> sled::Result<Self> {
        let db = sled::open(data_dir)?;
        Ok(Self { db })
    }
}
