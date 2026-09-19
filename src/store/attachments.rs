//! Attachment blob storage: filesystem blobs + a sled tree of metadata for
//! `GET /file/:id` lookups and the expiry sweep. Mirrors the
//! `store/acl.rs`/`store/users.rs` module pattern.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};

use crate::model::Attachment;

const ATTACHMENTS_TREE: &str = "attachments";

/// Local-filesystem-backed attachment store: blobs live at `{dir}/{id}`
/// (no extension, matching upstream ntfy's `attachment/backend_file.go`),
/// metadata lives in a dedicated sled tree keyed by the same `id`.
pub struct Attachments {
    tree: sled::Tree,
    dir: PathBuf,
    total_bytes: AtomicU64,
    file_size_limit: u64,
    total_size_limit: u64,
}

impl Attachments {
    /// Opens (creating if needed) the blob directory and the sled
    /// metadata tree, seeding the running `total_bytes` counter by
    /// summing the metadata tree's recorded sizes (so a restart doesn't
    /// leak the total-size budget).
    pub fn open(db: sled::Db, dir: PathBuf, file_size_limit: u64, total_size_limit: u64) -> Result<Self> {
        std::fs::create_dir_all(&dir).with_context(|| format!("creating attachment dir {}", dir.display()))?;
        let tree = db.open_tree(ATTACHMENTS_TREE).context("opening attachments tree")?;

        let mut total = 0u64;
        for item in tree.iter() {
            let (_, value) = item.context("iterating attachments tree")?;
            if let Ok(meta) = serde_json::from_slice::<Attachment>(&value) {
                total += meta.size.unwrap_or(0);
            }
        }

        Ok(Self {
            tree,
            dir,
            total_bytes: AtomicU64::new(total),
            file_size_limit,
            total_size_limit,
        })
    }

    pub fn file_size_limit(&self) -> u64 {
        self.file_size_limit
    }

    /// Bytes of total-size budget still available.
    pub fn remaining(&self) -> u64 {
        self.total_size_limit.saturating_sub(self.total_bytes.load(Ordering::Relaxed))
    }

    /// Writes a new blob + its metadata. Callers should already have
    /// checked `file_size_limit`/`remaining()` themselves, but this is
    /// re-checked here as the source of truth.
    pub async fn write(&self, id: &str, bytes: &[u8], meta: Attachment) -> Result<()> {
        let len = bytes.len() as u64;
        if len > self.file_size_limit {
            anyhow::bail!("attachment exceeds file size limit");
        }
        if len > self.remaining() {
            anyhow::bail!("attachment exceeds remaining total size budget");
        }

        let path = self.dir.join(id);
        tokio::fs::write(&path, bytes)
            .await
            .with_context(|| format!("writing attachment blob {}", path.display()))?;

        let value = serde_json::to_vec(&meta).context("serializing attachment metadata")?;
        self.tree.insert(id.as_bytes(), value).context("inserting attachment metadata")?;

        self.total_bytes.fetch_add(len, Ordering::Relaxed);
        Ok(())
    }

    /// Reads a blob's raw bytes.
    pub async fn read(&self, id: &str) -> std::io::Result<Vec<u8>> {
        tokio::fs::read(self.dir.join(id)).await
    }

    /// Looks up an attachment's metadata without reading the blob.
    pub fn get_meta(&self, id: &str) -> Result<Option<Attachment>> {
        match self.tree.get(id.as_bytes()).context("reading attachment metadata")? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes).context("decoding attachment metadata")?)),
            None => Ok(None),
        }
    }

    /// Deletes a blob (ignoring a not-found error) + its metadata,
    /// decrementing the running total-size counter.
    pub async fn delete(&self, id: &str) -> Result<()> {
        let size = self.get_meta(id)?.and_then(|m| m.size).unwrap_or(0);

        match tokio::fs::remove_file(self.dir.join(id)).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).context("removing attachment blob"),
        }

        self.tree.remove(id.as_bytes()).context("removing attachment metadata")?;

        if size > 0 {
            self.total_bytes.fetch_sub(size, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Ids of every attachment whose `expires` is in the past, for the
    /// periodic expiry sweep.
    pub fn list_expired(&self, now: i64) -> Vec<String> {
        let mut out = Vec::new();
        for item in self.tree.iter() {
            let Ok((key, value)) = item else { continue };
            let Ok(meta) = serde_json::from_slice::<Attachment>(&value) else { continue };
            if meta.expires.is_some_and(|e| e < now) {
                if let Ok(id) = std::str::from_utf8(&key) {
                    out.push(id.to_string());
                }
            }
        }
        out
    }

    /// Small static extension -> MIME-type table (no `mime_guess` crate
    /// dependency for this short list). Falls back to
    /// `application/octet-stream` for unrecognized/missing extensions.
    pub fn mime_for_filename(name: &str) -> String {
        let ext = name.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
        match ext.as_str() {
            "jpg" | "jpeg" => "image/jpeg",
            "png" => "image/png",
            "gif" => "image/gif",
            "webp" => "image/webp",
            "bmp" => "image/bmp",
            "svg" => "image/svg+xml",
            "pdf" => "application/pdf",
            "txt" => "text/plain",
            "json" => "application/json",
            "zip" => "application/zip",
            "gz" => "application/gzip",
            "mp3" => "audio/mpeg",
            "mp4" => "video/mp4",
            "mov" => "video/quicktime",
            "webm" => "video/webm",
            "doc" => "application/msword",
            "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "xls" => "application/vnd.ms-excel",
            "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            _ => "application/octet-stream",
        }
        .to_string()
    }
}
