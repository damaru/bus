//! `TopicRegistry`, `Topic`, in-memory broadcast fanout (ports
//! `server/topic.go`), backed by the sled-persisted [`Cache`] for
//! durability and restart-safe sequence numbers (PLAN.md section 8), plus
//! the bus extension's participant/presence tracking (PLAN.md section 5.2,
//! M4).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use dashmap::DashMap;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::model::{generate_message_id, now_unix, Control, ControlType, Enc, Envelope, SinceMarker};
use crate::store::cache::Cache;

/// Identifies a single subscriber's fanout channel within a [`Topic`].
/// Every live consumer of a topic — plain `/json`/`/sse`/`/raw`/`/ws`
/// subscribers *and* `/bus` participants alike — gets one of these, so a
/// message published from any source (HTTP publish or a bus participant)
/// reaches everyone through the same fanout path.
pub type SubscriberId = u64;

/// Identifies a single `/bus` participant (a chat-style identity layered on
/// top of a [`SubscriberId`]). Reuses the same random-id shape as message
/// ids (`model::generate_message_id`).
pub type SessionId = String;

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

/// A `/bus` participant: a chat-style identity (PLAN.md section 8's
/// `Participant`). Not constructed for plain `/json`/`/sse`/`/raw`/`/ws`
/// subscribers — those only ever touch `Topic::subscribers`, never
/// `Topic::participants`.
#[derive(Debug, Clone)]
struct Participant {
    session_id: SessionId,
    /// Client-supplied "sender" label (PLAN.md 4.1), e.g. a device/user name.
    sender_label: String,
    /// Whether this participant may publish `message`/relay-eligible
    /// `control` frames (checked both by the `/bus` handler up front and
    /// again here as defense-in-depth).
    can_write: bool,
    joined_at: i64,
    /// The underlying fanout channel this participant shares with every
    /// other subscriber on the topic.
    subscriber_id: SubscriberId,
}

/// Public-facing roster entry, used to build the `presence` control
/// message's `data` payload.
#[derive(Debug, Clone, Serialize)]
pub struct ParticipantInfo {
    pub session_id: SessionId,
    pub sender: String,
    pub can_write: bool,
    pub joined_at: i64,
}

impl From<&Participant> for ParticipantInfo {
    fn from(p: &Participant) -> Self {
        Self {
            session_id: p.session_id.clone(),
            sender: p.sender_label.clone(),
            can_write: p.can_write,
            joined_at: p.joined_at,
        }
    }
}

/// A single topic: recent-message ring buffer (fast path), a mirror of the
/// persisted sequence counter, live subscriber fanout channels, bus
/// participant/presence tracking, and a handle to the sled-backed cache
/// for durability + backlog fallback.
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
    /// Bus participants currently joined (PLAN.md section 8's
    /// `participants: HashMap<SessionId, Participant>`), separate from
    /// `subscribers` since not every subscriber is a bus participant.
    participants: Mutex<HashMap<SessionId, Participant>>,
    /// Max concurrent entries in `subscribers` for *this* topic (M6).
    max_subscribers_per_topic: u64,
    /// Max concurrent entries in `participants` for *this* topic (M6) —
    /// tighter than `max_subscribers_per_topic`, see `Config`'s doc
    /// comment on `max_bus_participants_per_topic`.
    max_bus_per_topic: u64,
    /// Shared across every `Topic` in the registry (M6): total live
    /// subscriber count server-wide, checked against
    /// `max_subscribers_total` as a global soft cap layered on top of the
    /// per-topic cap (PLAN.md M6: "a per-topic cap composed into a global
    /// soft cap is reasonable").
    total_subscribers: Arc<AtomicU64>,
    max_subscribers_total: u64,
}

impl Topic {
    #[allow(clippy::too_many_arguments)]
    fn new(
        name: String,
        ring_cap: usize,
        cache: Arc<Cache>,
        max_subscribers_per_topic: u64,
        max_bus_per_topic: u64,
        total_subscribers: Arc<AtomicU64>,
        max_subscribers_total: u64,
    ) -> Self {
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
            participants: Mutex::new(HashMap::new()),
            max_subscribers_per_topic,
            max_bus_per_topic,
            total_subscribers,
            max_subscribers_total,
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
    pub fn publish_with_id<F>(&self, id: String, build: F) -> Result<Envelope>
    where
        F: FnOnce(u64, String, i64) -> Envelope,
    {
        let seq = self.cache.next_seq(&self.name)?;
        self.seq.store(seq, Ordering::SeqCst);
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

    /// Same as [`Topic::publish_with_id`] but generates a fresh message id
    /// internally.
    pub fn publish<F>(&self, build: F) -> Result<Envelope>
    where
        F: FnOnce(u64, String, i64) -> Envelope,
    {
        self.publish_with_id(generate_message_id(), build)
    }

    /// Sends `envelope` to every live subscriber's channel except
    /// (optionally) `exclude`. Subscribers whose channel is full or closed
    /// are dropped (see `SUBSCRIBER_CHANNEL_CAPACITY` doc comment).
    fn fan_out_filtered(&self, envelope: &Envelope, exclude: Option<SubscriberId>) {
        let mut dead = Vec::new();
        {
            let subs = self.subscribers.lock().unwrap();
            for (id, tx) in subs.iter() {
                if Some(*id) == exclude {
                    continue;
                }
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

    fn fan_out(&self, envelope: &Envelope) {
        self.fan_out_filtered(envelope, None);
    }

    fn fan_out_except(&self, envelope: &Envelope, exclude: SubscriberId) {
        self.fan_out_filtered(envelope, Some(exclude));
    }

    /// Sends `envelope` to exactly one subscriber's channel (used for
    /// direct, non-broadcast replies like `ping` -> `pong`). Silently
    /// no-ops if the target has disconnected.
    fn send_to(&self, target: SubscriberId, envelope: Envelope) {
        let tx = self.subscribers.lock().unwrap().get(&target).cloned();
        if let Some(tx) = tx {
            let _ = tx.try_send(envelope);
        }
    }

    /// True if this topic currently has room for one more subscriber,
    /// under *both* the per-topic and the global soft caps (M6). Checked
    /// by callers (`http::subscribe::prepare`, `http::bus::bus_ws`)
    /// *before* committing to a streaming response/WS upgrade, so a
    /// capacity rejection can still be a clean HTTP 429 rather than an
    /// upgrade that immediately gets dropped. `Topic::subscribe` itself
    /// stays infallible and doesn't re-check — see its doc comment for
    /// the accepted TOCTOU tradeoff.
    pub fn subscriber_capacity_available(&self) -> bool {
        let per_topic = self.subscribers.lock().unwrap().len() as u64;
        per_topic < self.max_subscribers_per_topic
            && self.total_subscribers.load(Ordering::Relaxed) < self.max_subscribers_total
    }

    /// True if this topic currently has room for one more `/bus`
    /// participant, under the per-topic bus cap (M6). Bus participants are
    /// also subscribers, so this is checked *in addition to*
    /// [`Topic::subscriber_capacity_available`], not instead of it.
    pub fn bus_capacity_available(&self) -> bool {
        (self.participants.lock().unwrap().len() as u64) < self.max_bus_per_topic
    }

    /// Registers a new live subscriber, returning its id (for
    /// [`Topic::unsubscribe`]) and the receiving end of its fanout channel.
    /// Used directly by the plain `/json`/`/sse`/`/raw`/`/ws` endpoints;
    /// `/bus` participants get the same channel via [`Topic::bus_join`].
    ///
    /// Infallible and unconditional: capacity is enforced earlier, by
    /// [`Topic::subscriber_capacity_available`], at the point where the
    /// caller can still return a clean HTTP error instead of an upgrade
    /// that would have to be torn back down. Since that check and this
    /// call aren't atomic together, concurrent requests can in rare cases
    /// push a topic slightly past its cap — an accepted soft-cap tradeoff
    /// (PLAN.md M6 explicitly calls the global limit a "soft cap").
    pub fn subscribe(&self) -> (SubscriberId, mpsc::Receiver<Envelope>) {
        let (tx, rx) = mpsc::channel(SUBSCRIBER_CHANNEL_CAPACITY);
        let id = self.next_subscriber_id.fetch_add(1, Ordering::SeqCst);
        self.subscribers.lock().unwrap().insert(id, tx);
        self.total_subscribers.fetch_add(1, Ordering::Relaxed);
        (id, rx)
    }

    /// Removes a subscriber, e.g. on client disconnect.
    pub fn unsubscribe(&self, id: SubscriberId) {
        if self.subscribers.lock().unwrap().remove(&id).is_some() {
            self.total_subscribers.fetch_sub(1, Ordering::Relaxed);
        }
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
                SinceMarker::Id(id) => ring.iter().position(|e| &e.id == id).map(|pos| ring.iter().skip(pos + 1).cloned().collect()),
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

    // ---- Bus extension (PLAN.md 5.2 / M4) ----

    /// Registers a new `/bus` participant: allocates a session id, shares
    /// the same fanout channel every subscriber uses (so bus participants
    /// see regular HTTP-published messages, and vice versa — PLAN.md 5.2's
    /// full interop requirement falls out of reusing `Topic::subscribe`'s
    /// machinery here rather than a parallel structure), and broadcasts a
    /// `join` control message to every *other* current subscriber. Returns
    /// the new session id and the receiving end of its fanout channel.
    ///
    /// Callers should snapshot [`Topic::roster`] *before* calling this (to
    /// build a `presence` message that reflects "everyone but me"); once
    /// `bus_join` returns, the new participant is already part of the
    /// roster and of the join broadcast's audience exclusion.
    pub fn bus_join(&self, sender_label: String, can_write: bool) -> (SessionId, mpsc::Receiver<Envelope>) {
        let (subscriber_id, rx) = self.subscribe();
        let session_id = generate_message_id();

        let participant = Participant {
            session_id: session_id.clone(),
            sender_label: sender_label.clone(),
            can_write,
            joined_at: now_unix(),
            subscriber_id,
        };
        self.participants.lock().unwrap().insert(session_id.clone(), participant);

        let join = Envelope::control(self.name.clone(), Control::new(ControlType::Join, sender_label));
        self.fan_out_except(&join, subscriber_id);

        (session_id, rx)
    }

    /// Removes a `/bus` participant (on WS close/error) and broadcasts a
    /// `leave` control message to the remaining participants/subscribers.
    /// A no-op if `session_id` is unknown (already left, or never joined).
    pub fn bus_leave(&self, session_id: &str) {
        let participant = self.participants.lock().unwrap().remove(session_id);
        if let Some(p) = participant {
            self.unsubscribe(p.subscriber_id);
            let leave = Envelope::control(self.name.clone(), Control::new(ControlType::Leave, p.sender_label));
            // The leaving participant's own channel is already removed
            // above, so a plain (unfiltered) fan-out is equivalent to
            // "all other participants" here.
            self.fan_out(&leave);
        }
    }

    /// Sends a `close` control message (PLAN.md section 4.2: "server is
    /// terminating the connection") directly to every currently-joined
    /// `/bus` participant on this topic — used during graceful shutdown
    /// (M6) so bus clients get an explicit heads-up before the process
    /// exits, rather than just seeing the socket drop. Sent only to bus
    /// participants (not every plain `/json`/`/sse`/`/raw`/`/ws`
    /// subscriber), matching `close`'s documented direction
    /// ("server -> client") in the bus control-message table.
    pub fn broadcast_close(&self) {
        let targets: Vec<SubscriberId> = {
            let participants = self.participants.lock().unwrap();
            participants.values().map(|p| p.subscriber_id).collect()
        };
        if targets.is_empty() {
            return;
        }
        let close = Envelope::control(self.name.clone(), Control::new(ControlType::Close, "server"));
        for subscriber_id in targets {
            self.send_to(subscriber_id, close.clone());
        }
    }

    /// Snapshot of every currently-joined `/bus` participant, used to
    /// build the `presence` control message sent to a newly-connecting
    /// client.
    pub fn roster(&self) -> Vec<ParticipantInfo> {
        self.participants.lock().unwrap().values().map(ParticipantInfo::from).collect()
    }

    /// Publishes a `message` event on behalf of a `/bus` participant,
    /// tagged with their `sender` label — otherwise identical to the HTTP
    /// publish path (seq allocation, durable sled write, ring buffer,
    /// fan-out to every subscriber). Looks up `can_write` from the
    /// participant record itself (defense-in-depth: the `/bus` handler
    /// should already have checked this before calling, but a session
    /// that was write-capable at connect time and got revoked mid-session
    /// would still be caught here). Accepts `encoding`/`enc` (M5): PLAN.md
    /// doesn't restrict e2e to HTTP-only publish, and shape validation is
    /// the caller's job (`http::params::validate_e2e`, same function used
    /// by the HTTP publish path) so it isn't duplicated here.
    #[allow(clippy::too_many_arguments)]
    pub fn bus_publish_message(
        &self,
        session_id: &str,
        title: Option<String>,
        message: Option<String>,
        priority: Option<u8>,
        tags: Vec<String>,
        click: Option<String>,
        encoding: Option<String>,
        enc: Option<Enc>,
    ) -> Result<Envelope> {
        let sender_label = {
            let participants = self.participants.lock().unwrap();
            let p = participants.get(session_id).context("unknown bus session")?;
            if !p.can_write {
                anyhow::bail!("permission denied: read-only participant");
            }
            p.sender_label.clone()
        };
        let topic_name = self.name.clone();
        self.publish(move |seq, id, time| {
            Envelope::new_bus_message(
                topic_name,
                seq,
                id,
                time,
                sender_label,
                title,
                message,
                priority,
                tags,
                click,
                encoding,
                enc,
            )
        })
    }

    /// Handles a client-originated `control` frame (`typing`/`ack`/`ping`
    /// only — any other type from a client is rejected by the caller
    /// before this is reached). `typing`/`ack` are relayed to every *other*
    /// participant/subscriber and never persisted (PLAN.md 4.2: ephemeral
    /// only). `ping` is answered directly to the sender with a `pong` —
    /// never broadcast.
    pub fn bus_relay_control(&self, session_id: &str, control_type: ControlType, data: Option<serde_json::Value>) -> Result<()> {
        let (sender_label, subscriber_id, can_write) = {
            let participants = self.participants.lock().unwrap();
            let p = participants.get(session_id).context("unknown bus session")?;
            (p.sender_label.clone(), p.subscriber_id, p.can_write)
        };
        if !can_write {
            anyhow::bail!("permission denied: read-only participant");
        }

        match control_type {
            ControlType::Ping => {
                let pong = Envelope::control(self.name.clone(), Control::new(ControlType::Pong, "server"));
                self.send_to(subscriber_id, pong);
            }
            ControlType::Typing | ControlType::Ack => {
                let mut control = Control::new(control_type, sender_label);
                if let Some(d) = data {
                    control = control.with_data(d);
                }
                let envelope = Envelope::control(self.name.clone(), control);
                self.fan_out_except(&envelope, subscriber_id);
            }
            other => anyhow::bail!("clients may not send control type '{other:?}'"),
        }
        Ok(())
    }
}

/// Connection/topic caps for the whole registry (M6 hardening), mirroring
/// `Config`'s `max_topics`/`max_subscribers_per_topic`/
/// `max_subscribers_total`/`max_bus_participants_per_topic` CLI flags.
#[derive(Debug, Clone, Copy)]
pub struct TopicLimits {
    pub max_topics: usize,
    pub max_subscribers_per_topic: u64,
    pub max_subscribers_total: u64,
    pub max_bus_participants_per_topic: u64,
}

/// In-memory registry of all known topics, created lazily on first
/// publish/subscribe. The registry itself is never persisted (per PLAN.md
/// section 8), but each [`Topic`] is now backed by the sled [`Cache`] for
/// durable message storage and restart-safe sequence numbers.
pub struct TopicRegistry {
    topics: DashMap<String, Arc<Topic>>,
    ring_cap: usize,
    cache: Arc<Cache>,
    limits: TopicLimits,
    /// Shared with every `Topic` this registry creates — see
    /// `Topic::total_subscribers`'s doc comment.
    total_subscribers: Arc<AtomicU64>,
}

impl TopicRegistry {
    /// `ring_cap` bounds each topic's in-memory backlog (clamped to
    /// [`MAX_RING_CAPACITY`]); `cache` is the shared sled-backed store all
    /// topics persist through; `limits` are the M6 connection/topic caps.
    pub fn new(ring_cap: usize, cache: Arc<Cache>, limits: TopicLimits) -> Self {
        Self {
            topics: DashMap::new(),
            ring_cap: ring_cap.clamp(1, MAX_RING_CAPACITY),
            cache,
            limits,
            total_subscribers: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns the topic, creating it if it doesn't exist yet. A freshly
    /// created topic has its in-memory seq mirror initialized from
    /// [`Cache::last_seq`], so publishing continues the sequence correctly
    /// after a restart instead of starting over at 0/1.
    ///
    /// Fails with `429 Too Many Requests` (M6) only when `name` doesn't
    /// exist yet *and* the registry is already at `max_topics` — an
    /// already-existing topic is never blocked, no matter how many topics
    /// exist. `429` (rather than `503`) was chosen to match the other M6
    /// capacity rejections (rate limit, connection caps): these are all
    /// "you've hit a limit, back off/try elsewhere" conditions specific to
    /// the request, not a server-wide outage, so `429` fits better than
    /// `503` (documented milestone decision).
    pub fn get_or_create(&self, name: &str) -> Result<Arc<Topic>, crate::error::AppError> {
        if let Some(existing) = self.topics.get(name) {
            return Ok(existing.clone());
        }
        if self.topics.len() >= self.limits.max_topics {
            return Err(crate::error::AppError::TooManyRequests(format!(
                "server has reached its max-topics limit ({})",
                self.limits.max_topics
            )));
        }
        // Note: the len() check above and this insert aren't atomic
        // together, so concurrent creation of several brand-new distinct
        // topic names can in rare cases overshoot max_topics by a small
        // amount — the same accepted soft-cap tradeoff as
        // `Topic::subscribe` (see its doc comment).
        let topic = self.topics.entry(name.to_string()).or_insert_with(|| {
            Arc::new(Topic::new(
                name.to_string(),
                self.ring_cap,
                self.cache.clone(),
                self.limits.max_subscribers_per_topic,
                self.limits.max_bus_participants_per_topic,
                self.total_subscribers.clone(),
                self.limits.max_subscribers_total,
            ))
        });
        Ok(topic.clone())
    }

    /// Broadcasts a `close` control message to every `/bus` participant on
    /// every topic — called once at the start of graceful shutdown (M6),
    /// see `Topic::broadcast_close`.
    pub fn broadcast_shutdown_close(&self) {
        for entry in self.topics.iter() {
            entry.value().broadcast_close();
        }
    }
}
