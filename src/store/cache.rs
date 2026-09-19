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

/// Outcome of a single [`Cache::prune`] call: how many entries were
/// removed, and the message ids of any removed entries that carried an
/// attachment (so the caller — `main.rs`'s sweep — can also reclaim the
/// now-orphaned attachment blob, which would otherwise linger until its
/// own independent expiry).
pub struct PruneResult {
    pub removed: usize,
    pub orphaned_attachment_ids: Vec<String>,
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
    /// order (age first, then count, then approximate size). Returns a
    /// [`PruneResult`]: the number of entries removed, plus the message
    /// ids of any removed entries that carried an attachment.
    ///
    /// `max_size_bytes` enforcement is best-effort per PLAN.md section 8:
    /// after age/count pruning it trims further, oldest-first, until the
    /// summed serialized size of the topic's remaining messages is under
    /// the cap. This is exact (not averaged) since we already have every
    /// entry's byte length in hand from the scan, but it's still a
    /// secondary limit — cache-duration and cache-count are primary.
    pub fn prune(&self, topic: &str, max_age: Duration, max_count: u64, max_size_bytes: u64) -> Result<PruneResult> {
        let tree = self.msgs_tree(topic)?;
        let cutoff = model::now_unix() - max_age.as_secs() as i64;

        let mut entries: Vec<(sled::IVec, Envelope, usize)> = Vec::with_capacity(tree.len());
        for item in tree.iter() {
            let (key, value) = item.context("iterating msgs tree for prune")?;
            let envelope: Envelope = serde_json::from_slice(&value).context("decoding envelope for prune")?;
            entries.push((key, envelope, value.len()));
        }

        let mut removed = 0usize;
        let mut orphaned_attachment_ids: Vec<String> = Vec::new();
        let mut kept: Vec<(sled::IVec, usize, Option<String>)> = Vec::with_capacity(entries.len());
        for (key, envelope, size) in entries {
            if envelope.time < cutoff {
                tree.remove(&key).context("removing expired message")?;
                removed += 1;
                if envelope.attachment.is_some() {
                    orphaned_attachment_ids.push(envelope.id.clone());
                }
            } else {
                let attachment_id = envelope.attachment.is_some().then(|| envelope.id.clone());
                kept.push((key, size, attachment_id));
            }
        }

        if kept.len() as u64 > max_count {
            let excess = kept.len() as u64 - max_count;
            for (key, _, attachment_id) in kept.drain(..excess as usize) {
                tree.remove(&key).context("removing excess message")?;
                removed += 1;
                if let Some(id) = attachment_id {
                    orphaned_attachment_ids.push(id);
                }
            }
        }

        let mut total_size: u64 = kept.iter().map(|(_, size, _)| *size as u64).sum();
        let mut idx = 0;
        while total_size > max_size_bytes && idx < kept.len() {
            let (key, size, attachment_id) = &kept[idx];
            tree.remove(key).context("removing oversized-cache message")?;
            total_size = total_size.saturating_sub(*size as u64);
            removed += 1;
            if let Some(id) = attachment_id {
                orphaned_attachment_ids.push(id.clone());
            }
            idx += 1;
        }

        Ok(PruneResult {
            removed,
            orphaned_attachment_ids,
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::generate_message_id;

    fn temp_cache() -> Cache {
        let db = sled::Config::new().temporary(true).open().expect("failed to open temporary sled db");
        Cache::new(db)
    }

    fn envelope_at(topic: &str, seq: u64, time: i64) -> Envelope {
        Envelope::new_message(
            topic.to_string(),
            seq,
            generate_message_id(),
            time,
            None,
            Some(format!("message {seq}")),
            None,
            Vec::new(),
            None,
            None,
            None,
            None,
            None,
        )
    }

    #[test]
    fn next_seq_is_monotonic_and_persists_across_cache_instances() {
        let db = sled::Config::new().temporary(true).open().unwrap();
        let cache1 = Cache::new(db.clone());
        assert_eq!(cache1.next_seq("t").unwrap(), 1);
        assert_eq!(cache1.next_seq("t").unwrap(), 2);

        // Simulate a restart: a fresh `Cache` wrapping the same underlying
        // `sled::Db` handle must continue from the persisted counter.
        let cache2 = Cache::new(db);
        assert_eq!(cache2.last_seq("t").unwrap(), 2);
        assert_eq!(cache2.next_seq("t").unwrap(), 3);
    }

    #[test]
    fn prune_removes_only_entries_older_than_max_age() {
        let cache = temp_cache();
        let now = model::now_unix();
        let old = envelope_at("agetopic", 1, now - 7200); // 2h old
        let recent = envelope_at("agetopic", 2, now - 1800); // 30m old
        let fresh = envelope_at("agetopic", 3, now);
        cache.store_message("agetopic", &old).unwrap();
        cache.store_message("agetopic", &recent).unwrap();
        cache.store_message("agetopic", &fresh).unwrap();

        let removed = cache
            .prune("agetopic", Duration::from_secs(3600), u64::MAX, u64::MAX)
            .unwrap()
            .removed;
        assert_eq!(removed, 1, "only the 2h-old message should be pruned by a 1h max_age");

        let remaining = cache.messages_since("agetopic", &SinceMarker::All).unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].seq, 2);
        assert_eq!(remaining[1].seq, 3);
    }

    #[test]
    fn prune_keeps_only_the_newest_max_count_entries() {
        let cache = temp_cache();
        let now = model::now_unix();
        for seq in 1..=5u64 {
            cache.store_message("counttopic", &envelope_at("counttopic", seq, now)).unwrap();
        }

        let removed = cache.prune("counttopic", Duration::from_secs(86_400), 3, u64::MAX).unwrap().removed;
        assert_eq!(removed, 2, "5 entries capped to max_count=3 should remove the oldest 2");

        let remaining = cache.messages_since("counttopic", &SinceMarker::All).unwrap();
        let seqs: Vec<u64> = remaining.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![3, 4, 5], "the 3 newest entries (highest seq) must survive");
    }

    #[test]
    fn prune_trims_oldest_first_once_over_the_size_cap() {
        let cache = temp_cache();
        let now = model::now_unix();
        let mut entry_size = 0usize;
        for seq in 1..=5u64 {
            let env = envelope_at("sizetopic", seq, now);
            entry_size = serde_json::to_vec(&env).unwrap().len();
            cache.store_message("sizetopic", &env).unwrap();
        }

        // Cap sized for roughly 2 entries -- age/count limits left wide
        // open so only the size cap is exercised.
        let cap = (entry_size * 2 + entry_size / 2) as u64;
        let removed = cache.prune("sizetopic", Duration::from_secs(86_400), u64::MAX, cap).unwrap().removed;
        assert_eq!(removed, 3, "should trim oldest-first until under the size cap");

        let remaining = cache.messages_since("sizetopic", &SinceMarker::All).unwrap();
        let seqs: Vec<u64> = remaining.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![4, 5], "the newest entries must survive size-based trimming");
    }

    #[test]
    fn prune_is_a_noop_when_nothing_exceeds_any_limit() {
        let cache = temp_cache();
        let now = model::now_unix();
        cache.store_message("quiettopic", &envelope_at("quiettopic", 1, now)).unwrap();
        let removed = cache.prune("quiettopic", Duration::from_secs(86_400), 100, u64::MAX).unwrap().removed;
        assert_eq!(removed, 0);
    }
}
