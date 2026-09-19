//! `TopicRegistry`, `Topic`, in-memory broadcast fanout (ports
//! `server/topic.go`). In-memory only per PLAN.md section 8 — sled-backed
//! persistence lands in M2, bus participant/presence tracking in M4.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::model::{now_unix, Envelope, SinceMarker};

/// Identifies a single subscriber's fanout channel within a [`Topic`].
pub type SubscriberId = u64;

/// Bounded channel capacity for each subscriber's fanout `mpsc`. If a
/// subscriber can't keep up and the channel fills, it is dropped rather than
/// blocking the topic (slow-consumer policy per PLAN.md section 12) —
/// clients are expected to reconnect with `since=` to catch up.
const SUBSCRIBER_CHANNEL_CAPACITY: usize = 256;

/// Upper bound on the in-memory ring buffer size, regardless of configured
/// `cache-count`. Persistence (and the real retention sweep) lands in M2;
/// this just keeps M1 memory use bounded.
const MAX_RING_CAPACITY: usize = 10_000;

/// A single topic: recent-message ring buffer, monotonic sequence counter,
/// and live subscriber fanout channels.
pub struct Topic {
    name: String,
    seq: AtomicU64,
    ring: Mutex<VecDeque<Envelope>>,
    ring_cap: usize,
    subscribers: Mutex<HashMap<SubscriberId, mpsc::Sender<Envelope>>>,
    next_subscriber_id: AtomicU64,
}

impl Topic {
    fn new(name: String, ring_cap: usize) -> Self {
        Self {
            name,
            seq: AtomicU64::new(0),
            ring: Mutex::new(VecDeque::with_capacity(ring_cap.min(256))),
            ring_cap,
            subscribers: Mutex::new(HashMap::new()),
            next_subscriber_id: AtomicU64::new(0),
        }
    }

    /// Topic name this instance was created for.
    #[allow(dead_code)]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Allocates seq/id/time, builds the envelope via `build`, stores it in
    /// the ring buffer, and fans it out to all live subscribers. Returns the
    /// stored envelope.
    pub fn publish<F>(&self, build: F) -> Envelope
    where
        F: FnOnce(u64, String, i64) -> Envelope,
    {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let id = crate::model::generate_message_id();
        let time = now_unix();
        let envelope = build(seq, id, time);

        {
            let mut ring = self.ring.lock().unwrap();
            ring.push_back(envelope.clone());
            while ring.len() > self.ring_cap {
                ring.pop_front();
            }
        }

        self.fan_out(&envelope);
        envelope
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

    /// Returns ring-buffer entries matching `since`, oldest first. See
    /// [`SinceMarker`] for the supported semantics.
    pub fn replay_since(&self, since: &SinceMarker) -> Vec<Envelope> {
        let ring = self.ring.lock().unwrap();
        match since {
            SinceMarker::None => Vec::new(),
            SinceMarker::All => ring.iter().cloned().collect(),
            SinceMarker::Latest => ring.back().cloned().into_iter().collect(),
            SinceMarker::Time(t) => ring.iter().filter(|e| e.time >= *t).cloned().collect(),
            SinceMarker::Id(id) => match ring.iter().position(|e| &e.id == id) {
                // Unknown id (already pruned, wrong topic, etc): nothing to
                // replay "after" it, so return an empty backlog rather than
                // guessing. Matches conservative ntfy behavior for stale ids.
                Some(pos) => ring.iter().skip(pos + 1).cloned().collect(),
                None => Vec::new(),
            },
        }
    }
}

/// In-memory registry of all known topics, created lazily on first
/// publish/subscribe (never persisted, per PLAN.md section 8).
pub struct TopicRegistry {
    topics: DashMap<String, Arc<Topic>>,
    ring_cap: usize,
}

impl TopicRegistry {
    /// `ring_cap` bounds each topic's in-memory backlog (clamped to
    /// [`MAX_RING_CAPACITY`] until sled persistence lands in M2).
    pub fn new(ring_cap: usize) -> Self {
        Self {
            topics: DashMap::new(),
            ring_cap: ring_cap.clamp(1, MAX_RING_CAPACITY),
        }
    }

    /// Returns the topic, creating it (with an empty backlog) if it doesn't
    /// exist yet.
    pub fn get_or_create(&self, name: &str) -> Arc<Topic> {
        self.topics
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Topic::new(name.to_string(), self.ring_cap)))
            .clone()
    }
}
