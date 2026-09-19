//! Message persistence, `since=` resolution, and retention sweep, backed by
//! the `sled::Db` opened in `store/mod.rs`. Ports PLAN.md section 8's
//! storage design.
//!
//! ## Layout decisions (documented, since PLAN.md leaves some latitude)
//!
//! - **One sled tree per topic**, named `msgs::{topic}`, keyed by
//!   `{seq:016x}` (zero-padded lowercase hex of the `u64` sequence number,
//!   as ASCII bytes). Fixed-width zero-padded hex sorts identically to
//!   numeric order under sled's byte-lexicographic key ordering, so
//!   `Tree::iter()`/`Tree::range()` naturally yield messages in publish
//!   order. A per-topic tree (rather than one shared tree with
//!   topic-prefixed keys) was chosen because it makes full-topic scans and
//!   the retention sweep trivial (`tree.iter()` never needs prefix
//!   filtering), and topic enumeration for the background sweep is just
//!   `Db::tree_names()` filtered by the `msgs::` prefix.
//! - **`topic_meta` tree**: one shared tree, keyed by the raw topic name,
//!   value is JSON-encoded [`TopicMeta`] (`{last_seq, last_access}`). Seq
//!   allocation goes through `sled::Tree::update_and_fetch`, which does an
//!   internal compare-and-swap retry loop, so it's safe under concurrent
//!   publishers without any additional locking.
//! - **Encoding**: `serde_json` for both trees. Slower and larger on disk
//!   than `bincode`, but keeps the database contents human-inspectable
//!   (`sled` has no built-in browser, so this matters for debugging), and
//!   at this project's scale the overhead is irrelevant.

use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::model::{self, Envelope, SinceMarker};

const MSGS_TREE_PREFIX: &str = "msgs::";
const TOPIC_META_TREE: &str = "topic_meta";

/// Per-topic bookkeeping persisted in the `topic_meta` tree.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
struct TopicMeta {
    last_seq: u64,
    last_access: i64,
}

/// sled-backed message cache: persistence, `since=` replay, and retention.
pub struct Cache {
    db: sled::Db,
}

impl Cache {
    pub fn new(db: sled::Db) -> Self {
        Self { db }
    }

    fn msgs_tree(&self, topic: &str) -> Result<sled::Tree> {
        self.db
            .open_tree(format!("{MSGS_TREE_PREFIX}{topic}"))
            .with_context(|| format!("opening msgs tree for topic {topic}"))
    }

    fn meta_tree(&self) -> Result<sled::Tree> {
        self.db.open_tree(TOPIC_META_TREE).context("opening topic_meta tree")
    }

    /// Zero-padded fixed-width hex key so byte-lexicographic order == seq order.
    fn seq_key(seq: u64) -> [u8; 16] {
        let mut key = [0u8; 16];
        key.copy_from_slice(format!("{seq:016x}").as_bytes());
        key
    }

    /// Persists `envelope` under its own `seq` in the topic's message tree.
    /// Called synchronously from the publish path, before fan-out, so a
    /// message is never acknowledged to the publisher unless it is durable.
    pub fn store_message(&self, topic: &str, envelope: &Envelope) -> Result<()> {
        let tree = self.msgs_tree(topic)?;
        let key = Self::seq_key(envelope.seq);
        let value = serde_json::to_vec(envelope).context("serializing envelope")?;
        tree.insert(key, value).context("inserting message into sled")?;
        Ok(())
    }

    /// Atomically allocates and persists the next sequence number for
    /// `topic`, so sequence numbers stay monotonic across process restarts
    /// even under concurrent publishers (uses sled's compare-and-swap based
    /// `update_and_fetch`, no external locking needed).
    pub fn next_seq(&self, topic: &str) -> Result<u64> {
        let tree = self.meta_tree()?;
        let now = model::now_unix();
        let updated = tree
            .update_and_fetch(topic.as_bytes(), move |old: Option<&[u8]>| {
                let mut meta: TopicMeta = old
                    .and_then(|bytes| serde_json::from_slice(bytes).ok())
                    .unwrap_or_default();
                meta.last_seq += 1;
                meta.last_access = now;
                serde_json::to_vec(&meta).ok()
            })
            .context("allocating next seq")?;
        let bytes = updated.context("update_and_fetch returned no value")?;
        let meta: TopicMeta = serde_json::from_slice(&bytes).context("decoding topic_meta")?;
        Ok(meta.last_seq)
    }

    /// Reads the last allocated sequence number for `topic` without
    /// allocating a new one (0 if the topic has never been published to).
    /// Used to initialize a freshly created [`crate::topic::Topic`]'s
    /// in-memory seq mirror after a restart.
    pub fn last_seq(&self, topic: &str) -> Result<u64> {
        let tree = self.meta_tree()?;
        match tree.get(topic.as_bytes()).context("reading topic_meta")? {
            Some(bytes) => {
                let meta: TopicMeta = serde_json::from_slice(&bytes).context("decoding topic_meta")?;
                Ok(meta.last_seq)
            }
            None => Ok(0),
        }
    }

    /// Reads every message currently persisted for `topic`, oldest first.
    fn all_messages(&self, tree: &sled::Tree) -> Result<Vec<Envelope>> {
        let mut out = Vec::with_capacity(tree.len());
        for item in tree.iter() {
            let (_, value) = item.context("iterating msgs tree")?;
            let envelope: Envelope = serde_json::from_slice(&value).context("decoding envelope")?;
            out.push(envelope);
        }
        Ok(out)
    }

    /// Resolves `since=` against the persisted message tree for `topic`.
    /// Same semantics as [`crate::topic::Topic::replay_since`] (in-memory
    /// version): `all`/`none`/`latest`/`<unix_ts>`/`<message_id>`.
    pub fn messages_since(&self, topic: &str, since: &SinceMarker) -> Result<Vec<Envelope>> {
        let tree = self.msgs_tree(topic)?;
        match since {
            SinceMarker::None => Ok(Vec::new()),
            SinceMarker::All => self.all_messages(&tree),
            SinceMarker::Latest => match tree.iter().values().next_back() {
                Some(value) => {
                    let value = value.context("reading latest message")?;
                    let envelope: Envelope = serde_json::from_slice(&value).context("decoding envelope")?;
                    Ok(vec![envelope])
                }
                None => Ok(Vec::new()),
            },
            SinceMarker::Time(t) => {
                let all = self.all_messages(&tree)?;
                Ok(all.into_iter().filter(|e| e.time >= *t).collect())
            }
            SinceMarker::Id(id) => {
                let all = self.all_messages(&tree)?;
                match all.iter().position(|e| &e.id == id) {
                    Some(pos) => Ok(all.into_iter().skip(pos + 1).collect()),
                    None => Ok(Vec::new()),
                }
            }
        }
    }

    /// Deletes the oldest entries in `topic`'s message tree once the
    /// `max_age`/`max_count`/`max_size_bytes` limits are exceeded, in that
    /// order (age first, then count, then approximate size). Returns the
    /// number of entries removed.
    ///
    /// `max_size_bytes` enforcement is best-effort per PLAN.md section 8:
    /// after age/count pruning it trims further, oldest-first, until the
    /// summed serialized size of the topic's remaining messages is under
    /// the cap. This is exact (not averaged) since we already have every
    /// entry's byte length in hand from the scan, but it's still a
    /// secondary limit — cache-duration and cache-count are primary.
    pub fn prune(&self, topic: &str, max_age: Duration, max_count: u64, max_size_bytes: u64) -> Result<usize> {
        let tree = self.msgs_tree(topic)?;
        let cutoff = model::now_unix() - max_age.as_secs() as i64;

        let mut entries: Vec<(sled::IVec, Envelope, usize)> = Vec::with_capacity(tree.len());
        for item in tree.iter() {
            let (key, value) = item.context("iterating msgs tree for prune")?;
            let envelope: Envelope = serde_json::from_slice(&value).context("decoding envelope for prune")?;
            entries.push((key, envelope, value.len()));
        }

        let mut removed = 0usize;
        let mut kept: Vec<(sled::IVec, usize)> = Vec::with_capacity(entries.len());
        for (key, envelope, size) in entries {
            if envelope.time < cutoff {
                tree.remove(&key).context("removing expired message")?;
                removed += 1;
            } else {
                kept.push((key, size));
            }
        }

        if kept.len() as u64 > max_count {
            let excess = kept.len() as u64 - max_count;
            for (key, _) in kept.drain(..excess as usize) {
                tree.remove(&key).context("removing excess message")?;
                removed += 1;
            }
        }

        let mut total_size: u64 = kept.iter().map(|(_, size)| *size as u64).sum();
        let mut idx = 0;
        while total_size > max_size_bytes && idx < kept.len() {
            let (key, size) = &kept[idx];
            tree.remove(key).context("removing oversized-cache message")?;
            total_size = total_size.saturating_sub(*size as u64);
            removed += 1;
            idx += 1;
        }

        Ok(removed)
    }

    /// Lists every topic that has at least one persisted message tree
    /// (used by the background retention sweep to enumerate what to prune).
    pub fn topics(&self) -> Vec<String> {
        self.db
            .tree_names()
            .into_iter()
            .filter_map(|name| {
                std::str::from_utf8(name.as_ref())
                    .ok()
                    .and_then(|s| s.strip_prefix(MSGS_TREE_PREFIX))
                    .map(|topic| topic.to_string())
            })
            .collect()
    }
}
