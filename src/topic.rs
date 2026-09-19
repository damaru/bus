//! `TopicRegistry`, `Topic`, in-memory broadcast fanout (ports
//! `server/topic.go`), now backed by the sled-persisted [`Cache`] for
//! durability and restart-safe sequence numbers (PLAN.md section 8). Bus
//! participant/presence tracking lands in M4.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::model::{generate_message_id, now_unix, Envelope, SinceMarker};
use crate::store::cache::Cache;

/// Identifies a single subscriber's fanout channel within a [`Topic`].
pub type SubscriberId = u64;

/// Bounded channel capacity for each subscriber's fanout `mpsc`. If a
/// subscriber can't keep up and the channel fills, it is dropped rather than
/// blocking the topic (slow-consumer policy per PLAN.md section 12) —
/// clients are expected to reconnect with `since=` to catch up.
const SUBSCRIBER_CHANNEL_CAPACITY: usize = 256;

/// Upper bound on the in-memory ring buffer size, regardless of configured
/// `cache-count`. The ring buffer is now just a fast-path cache in front of
/// the sled-backed [`Cache`] (source of truth), so this just keeps M2's
/// per-topic memory use bounded, independent of how large the persisted
/// history is allowed to grow.
const MAX_RING_CAPACITY: usize = 10_000;

/// A single topic: recent-message ring buffer (fast path), a mirror of the
/// persisted sequence counter, live subscriber fanout channels, and a
/// handle to the sled-backed cache for durability + backlog fallback.
pub struct Topic {
    name: String,
    /// Mirrors the persisted `topic_meta.last_seq`. Initialized from
    /// [`Cache::last_seq`] when the topic is first touched (so publishing
    /// continues the sequence correctly after a restart), but the actual
    /// allocation authority for each new message is [`Cache::next_seq`] —
    /// this field is a fast local cache for introspection, not the source
    /// of truth.
    seq: AtomicU64,
    ring: Mutex<VecDeque<Envelope>>,
    ring_cap: usize,
    subscribers: Mutex<HashMap<SubscriberId, mpsc::Sender<Envelope>>>,
    next_subscriber_id: AtomicU64,
    cache: Arc<Cache>,
}

impl Topic {
    fn new(name: String, ring_cap: usize, cache: Arc<Cache>) -> Self {
        let last_seq = cache.last_seq(&name).unwrap_or_else(|e| {
            tracing::warn!(topic = %name, error = %e, "failed to read persisted last_seq, starting from 0");
            0
        });
        Self {
            name,
            seq: AtomicU64::new(last_seq),
            ring: Mutex::new(VecDeque::with_capacity(ring_cap.min(256))),
            ring_cap,
            subscribers: Mutex::new(HashMap::new()),
            next_subscriber_id: AtomicU64::new(0),
            cache,
        }
    }

    /// Topic name this instance was created for.
    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The last sequence number known to this in-memory mirror (0 if the
    /// topic has never been published to in this process or a prior one).
    #[allow(dead_code)]
    pub fn current_seq(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    /// Allocates a persisted seq/id/time, builds the envelope via `build`,
    /// durably stores it in sled, updates the ring buffer, and fans it out
    /// to all live subscribers. Returns the stored envelope.
    ///
    /// Ordering (durability before ack, per PLAN.md M2 guidance): seq is
    /// allocated and the envelope is written to sled *before* it is pushed
    /// into the ring buffer or handed to any subscriber, so a client is
    /// never told a message was published unless it is already durable.
    /// sled's writes are fast local KV operations, so this runs inline on
    /// the calling (async handler) task rather than via `spawn_blocking` —
    /// only the periodic retention sweep, which scans whole topic trees,
    /// is moved onto a blocking task (see `main.rs`).
    pub fn publish<F>(&self, build: F) -> Result<Envelope>
    where
        F: FnOnce(u64, String, i64) -> Envelope,
    {
        let seq = self.cache.next_seq(&self.name)?;
        self.seq.store(seq, Ordering::SeqCst);
        let id = generate_message_id();
        let time = now_unix();
        let envelope = build(seq, id, time);

        self.cache.store_message(&self.name, &envelope)?;

        {
            let mut ring = self.ring.lock().unwrap();
            ring.push_back(envelope.clone());
            while ring.len() > self.ring_cap {
                ring.pop_front();
            }
        }

        self.fan_out(&envelope);
        Ok(envelope)
    }

    /// Sends `envelope` to every live subscriber's channel. Subscribers
    /// whose channel is full or closed are dropped (see
    /// `SUBSCRIBER_CHANNEL_CAPACITY` doc comment).
    fn fan_out(&self, envelope: &Envelope) {
        let mut dead = Vec::new();
        {
            let subs = self.subscribers.lock().unwrap();
            for (id, tx) in subs.iter() {
                if tx.try_send(envelope.clone()).is_err() {
                    dead.push(*id);
                }
            }
        }
        if !dead.is_empty() {
            let mut subs = self.subscribers.lock().unwrap();
            for id in dead {
                subs.remove(&id);
            }
        }
    }

    /// Registers a new live subscriber, returning its id (for
    /// [`Topic::unsubscribe`]) and the receiving end of its fanout channel.
    pub fn subscribe(&self) -> (SubscriberId, mpsc::Receiver<Envelope>) {
        let (tx, rx) = mpsc::channel(SUBSCRIBER_CHANNEL_CAPACITY);
        let id = self.next_subscriber_id.fetch_add(1, Ordering::SeqCst);
        self.subscribers.lock().unwrap().insert(id, tx);
        (id, rx)
    }

    /// Removes a subscriber, e.g. on client disconnect.
    pub fn unsubscribe(&self, id: SubscriberId) {
        self.subscribers.lock().unwrap().remove(&id);
    }

    /// Returns entries matching `since`, oldest first, using the in-memory
    /// ring buffer as a fast path and falling back to the sled-backed
    /// [`Cache`] whenever the ring buffer cannot be proven to hold the
    /// complete answer (notably: right after a process restart, when the
    /// ring is empty or has not yet re-accumulated the requested range).
    ///
    /// The ring buffer is a contiguous, append-only window (only the front
    /// is ever evicted), so each case below has a cheap, exact test for
    /// "does the ring alone already contain everything the caller asked
    /// for":
    /// - `All`: sufficient iff the oldest ring entry is seq 1 (nothing has
    ///   ever been evicted, in this process or before it).
    /// - `Id(id)`: sufficient iff `id` is found in the ring (everything
    ///   after it in the ring is then guaranteed contiguous).
    /// - `Time(t)`: sufficient iff the oldest ring entry's time is already
    ///   `<= t` (so nothing older that could still be `>= t` was evicted).
    /// - `Latest`: sufficient iff the ring is non-empty (its last entry is
    ///   always the newest, since `publish` appends before fan-out).
    /// - `None`: always empty, never needs sled.
    pub fn replay_since(&self, since: &SinceMarker) -> Vec<Envelope> {
        let ring_result = {
            let ring = self.ring.lock().unwrap();
            match since {
                SinceMarker::None => return Vec::new(),
                SinceMarker::All => match ring.front() {
                    Some(first) if first.seq == 1 => Some(ring.iter().cloned().collect()),
                    _ => None,
                },
                SinceMarker::Latest => ring.back().map(|e| vec![e.clone()]),
                SinceMarker::Time(t) => match ring.front() {
                    Some(first) if first.time <= *t => {
                        Some(ring.iter().filter(|e| e.time >= *t).cloned().collect())
                    }
                    _ => None,
                },
                SinceMarker::Id(id) => match ring.iter().position(|e| &e.id == id) {
                    Some(pos) => Some(ring.iter().skip(pos + 1).cloned().collect()),
                    None => None,
                },
            }
        };

        if let Some(envelopes) = ring_result {
            return envelopes;
        }

        // Ring buffer couldn't prove completeness (likely just after a
        // restart, or since= reaches further back than ring_cap) — fall
        // back to the durable, authoritative sled store.
        match self.cache.messages_since(&self.name, since) {
            Ok(envelopes) => envelopes,
            Err(e) => {
                tracing::warn!(topic = %self.name, error = %e, "sled backlog lookup failed, returning ring-only results");
                let ring = self.ring.lock().unwrap();
                match since {
                    SinceMarker::All => ring.iter().cloned().collect(),
                    SinceMarker::Latest => ring.back().cloned().into_iter().collect(),
                    SinceMarker::Time(t) => ring.iter().filter(|e| e.time >= *t).cloned().collect(),
                    SinceMarker::Id(id) => match ring.iter().position(|e| &e.id == id) {
                        Some(pos) => ring.iter().skip(pos + 1).cloned().collect(),
                        None => Vec::new(),
                    },
                    SinceMarker::None => Vec::new(),
                }
            }
        }
    }
}

/// In-memory registry of all known topics, created lazily on first
/// publish/subscribe. The registry itself is never persisted (per PLAN.md
/// section 8), but each [`Topic`] is now backed by the sled [`Cache`] for
/// durable message storage and restart-safe sequence numbers.
pub struct TopicRegistry {
    topics: DashMap<String, Arc<Topic>>,
    ring_cap: usize,
    cache: Arc<Cache>,
}

impl TopicRegistry {
    /// `ring_cap` bounds each topic's in-memory backlog (clamped to
    /// [`MAX_RING_CAPACITY`]); `cache` is the shared sled-backed store all
    /// topics persist through.
    pub fn new(ring_cap: usize, cache: Arc<Cache>) -> Self {
        Self {
            topics: DashMap::new(),
            ring_cap: ring_cap.clamp(1, MAX_RING_CAPACITY),
            cache,
        }
    }

    /// Returns the topic, creating it if it doesn't exist yet. A freshly
    /// created topic has its in-memory seq mirror initialized from
    /// [`Cache::last_seq`], so publishing continues the sequence correctly
    /// after a restart instead of starting over at 0/1.
    pub fn get_or_create(&self, name: &str) -> Arc<Topic> {
        self.topics
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Topic::new(name.to_string(), self.ring_cap, self.cache.clone())))
            .clone()
    }
}
