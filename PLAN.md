# bus — minimal ntfy-compatible pub/sub server with bidirectional bus + E2E extension

Reference: `./refs/ntfy` (ntfy.sh Go server source, read for protocol shape, model, auth/ACL design).

## 1. Goals

- Implement a **minimal** subset of the ntfy HTTP pub/sub protocol (publish, subscribe as
  json/sse/raw/ws, since/poll replay, filters, Basic/Bearer auth) — wire-compatible enough
  that plain `curl`, `websocat`, and simple ntfy-style clients work unmodified for the
  read-only paths.
- Add a **bidirectional "bus" extension**: on a single topic, any authenticated participant
  with write access can both publish and receive over one persistent WebSocket connection
  (full-duplex group channel, like a group chat), with server-relayed **control messages**
  (join/leave/presence/typing/ack/ping-pong/error/close).
- Add **end-to-end encryption** support as an opaque envelope: the server is a blind relay.
  Clients derive a shared topic key out-of-band (pre-shared passphrase via
  Argon2/HKDF) — no key-exchange protocol in v1. The server only stores/relays ciphertext
  bytes plus algorithm/key-id/nonce metadata, never plaintext.
- **Auth + ACL**: users, tokens, per-topic read/write permissions, default-access policy —
  modeled directly on ntfy's `user/` package, minus tiers/billing.

## 2. Non-goals (explicitly out of scope for v1)

Firebase/APNs push, email/SMTP publishing, phone calls, file attachments/uploads, web UI,
payments/tiers, Matrix bridge, iOS instant-push polling, UnifiedPush-specific handling,
message templates, server-side key exchange (defer to v2).

## 3. Decisions locked in

| Question | Decision |
|---|---|
| Bus semantics | Full duplex group bus: any participant with write perm can publish, any with read perm receives; server is relay + control-message router |
| E2E key mgmt | Out-of-band pre-shared key (client-side Argon2/HKDF); server only relays opaque ciphertext envelopes |
| Persistence | Embedded KV via `sled` (no SQL); in-memory topic/broadcast registry on top |
| Deliverable scope | Server only (no client lib/CLI beyond an admin sub-command) |
| Web framework | `axum` + `tokio` |

## 4. Wire format

### 4.1 Message envelope (JSON, superset of ntfy's `model.Message`)

```jsonc
{
  "id": "hwQ2YpKdmg",          // random message id
  "seq": 42,                    // NEW: monotonic per-topic sequence number
  "time": 1635528741,
  "expires": 1635536741,        // omitted if caching disabled for this message
  "event": "message",           // open | keepalive | message | message_delete | message_clear | poll_request | control (NEW)
  "topic": "mytopic",
  "sender": "device-abc",       // NEW: opaque client-supplied participant/session label
  "title": "...", "message": "...", "priority": 3, "tags": [...],
  "click": "...", "content_type": "text/plain",
  "encoding": "base64|e2e",     // "e2e" NEW: message field carries ciphertext (base64)
  "enc": {                      // NEW: present only when encoding == "e2e"
    "alg": "xchacha20poly1305", // client-defined; server does not validate the algorithm, only shape
    "kid": "topic-key-v1",      // key id/version, client-managed
    "nonce": "base64..."
  },
  "control": {                  // NEW: present only when event == "control"
    "type": "join|leave|presence|typing|ack|ping|pong|error|close",
    "from": "device-abc",
    "data": { ... }             // free-form, type-specific payload
  }
}
```

Backward compatible: any client that only understands the original ntfy fields can ignore
`seq`, `sender`, `enc`, and `control`.

### 4.2 Control message types (v1)

| type | direction | purpose |
|---|---|---|
| `join` | server -> participants | emitted when a bus participant connects |
| `leave` | server -> participants | emitted when a bus participant disconnects |
| `presence` | server -> new participant | full roster snapshot on connect |
| `typing` | client -> relayed to others | ephemeral, never cached |
| `ack` | client -> relayed to others | client-defined delivery/read acknowledgement referencing a message `id` |
| `ping`/`pong` | client <-> server | app-level heartbeat (in addition to WS ping frames) |
| `error` | server -> client | e.g. malformed frame, rate-limited, permission denied |
| `close` | server -> client | server is terminating the connection (banned, shutting down) |

Control messages are **never persisted** to the message cache (ephemeral only), except
`join`/`leave` may optionally update the in-memory presence roster.

## 5. HTTP/WS API surface

### 5.1 ntfy-compatible subset (read side unchanged from upstream semantics)

- `PUT|POST /{topic}` — publish; plain-text body or JSON body; `X-Title`, `X-Priority`,
  `X-Tags`, `X-Click`, `Content-Type: text/markdown` headers/aliases per `docs/publish.md`.
  Returns the stored message as JSON (ntfy-style ack).
- `GET /{topic}/json` — NDJSON stream (`since=`, `poll=`, filters: `id`, `message`, `title`,
  `priority`, `tags`).
- `GET /{topic}/sse` — Server-Sent Events stream, same filters.
- `GET /{topic}/raw` — raw text, one line per message body, keepalives as blank lines.
- `GET /{topic}/ws` — WebSocket, **read-only** subscribe (ntfy-compatible; a plain ntfy
  client can use this and never notice the extension exists).
- Multi-topic: comma-separated topic list on all of the above.
- Auth: `Authorization: Basic|Bearer ...` header, or `?auth=` query param
  (`base64url(Basic base64(user:pass))`) for browser WebSocket clients that cannot set
  headers — copied verbatim from ntfy's `readAuthHeader`.

### 5.2 Bus extension (new)

- `GET /{topic}/bus` — WebSocket upgrade. Requires **read** permission to connect at all;
  requires **write** permission to send `message`/`control` frames (read-only participants
  receive but any send attempt gets an `error` control frame).
  - On connect: server assigns a `session_id`, sends `open`, replays cache per `since=`
    (default `since=none`, i.e. no backlog unless requested), then sends a `presence`
    control message with the current roster, then broadcasts `join` to other participants.
  - Client sends `message` events to publish, `control` events (`typing`, `ack`, `ping`) to
    interact; server assigns `id`/`seq`/`time`, applies rate limits, persists if cacheable,
    and fans out to all connected bus/subscribe/ws participants on the topic.
  - On disconnect: server broadcasts `leave`, removes from roster.

## 6. Auth & ACL model (mirrors `refs/ntfy/user/`)

- **Users**: username, argon2 password hash, role (user/admin).
- **Tokens**: opaque bearer token -> username, expiry, last-access/origin.
- **ACL**: `(username_or_*, topic_pattern) -> permission` where permission is
  `deny | read | write | read-write` (same bitmask idea as `user.Permission`).
- **Default access**: server-wide fallback (`deny-all | read-only | read-write`) applied
  when no explicit ACL entry matches, mirroring `auth-default-access` in ntfy.
- No HTTP admin API in v1 (keeps attack surface minimal); instead the same binary exposes
  `bus admin user add|passwd|del`, `bus admin token issue|revoke`, `bus admin acl grant|revoke`
  subcommands that operate directly on the sled DB.
- `maybeAuthenticate`-equivalent: always resolves to *some* visitor (IP-based anonymous or
  user-based); rate-limits auth failures per IP before hitting the password hasher.

## 7. E2E encryption envelope (server stays blind)

- Client responsibility only: derive a per-topic symmetric key from a shared passphrase via
  Argon2id -> HKDF, encrypt `message` (and optionally `title`) client-side, set
  `encoding: "e2e"` and populate `enc.alg`/`enc.kid`/`enc.nonce`; `message` becomes
  base64 ciphertext.
- Server validates only **shape** (base64-decodable, `nonce`/`kid` length limits, overall
  payload size cap) and treats the ciphertext as an opaque blob for storage, replay, and
  fan-out. Content-based filters (`message=`, `title=` query filters) are documented as
  **not meaningful** on `e2e`-encoded messages (ciphertext won't match plaintext filters).
- No key-exchange control message in v1 — documented as a v2 extension point
  (`control.type == "key_offer"/"key_request"`, still opaque to the server).

## 8. Storage design (sled)

Trees:
- `users` — `username -> {password_hash, role}`
- `tokens` — `token -> {username, expires, last_access, last_origin}`
- `acl` — `"{principal}\0{topic_pattern}" -> permission_byte`
- `msgs::{topic}` — `{seq:016x} -> envelope_bytes` (per-topic subtree via key prefix), used
  for `since=`/`poll=` replay; pruned by a background sweep against
  `cache-duration`/`cache-size`/`cache-count` config limits (mirrors ntfy's
  `message/cache*.go` retention logic, simplified to one embedded store).
- `topic_meta` — `topic -> {last_seq, last_access}` for id allocation and staleness checks.

In-memory only (never persisted, matches `server/topic.go`):
- `TopicRegistry: DashMap<String, Arc<Topic>>`
- `Topic { subscribers: HashMap<SubscriberId, mpsc::Sender<Envelope>>, participants: HashMap<SessionId, Participant>, rate_visitor, last_access }`
- Publish path: allocate id/seq -> optionally persist to sled -> fan out to all live
  subscriber channels concurrently (bounded mpsc per subscriber; slow consumers get
  disconnected rather than blocking the topic, same trade-off as ntfy's per-subscriber
  goroutines).

## 9. Crate / module layout

```
bus/
  Cargo.toml                 # axum, tokio, tokio-tungstenite (via axum ws), serde(+json),
                              # sled, argon2, rand, base64, dashmap, clap, tracing(+subscriber)
  src/
    main.rs                  # CLI entry: `bus serve`, `bus admin ...`
    config.rs                # Config struct, clap parsing, env/file overrides
    model.rs                 # Envelope, Event, Control, SinceMarker (ports model/model.go)
    error.rs                 # AppError -> ntfy-style JSON error responses
    store/
      mod.rs                 # sled::Db handle, tree helpers
      users.rs                # user/token CRUD + argon2 verify
      acl.rs                   # permission lookup + default-access fallback
      cache.rs                 # message persistence, since= resolution, retention sweep
    topic.rs                  # TopicRegistry, Topic, broadcast fanout (ports server/topic.go)
    auth.rs                   # Authorization header/query-param extraction + verification
    http/
      mod.rs                  # axum Router assembly, middleware (tracing, size limits)
      publish.rs                # PUT/POST handler
      subscribe.rs               # /json /sse /raw /ws (read-only, ntfy-compatible)
      bus.rs                      # /bus (new, full duplex + control messages)
      params.rs                    # query/header parsing + aliasing (X-Title, since=, filters)
    admin/
      mod.rs                  # `bus admin` subcommands
  tests/
    publish_subscribe.rs      # roundtrip across json/sse/raw/ws
    since_replay.rs           # persistence + since=/poll= across process restart
    auth_acl.rs                # allow/deny matrix, default-access modes
    bus_duplex.rs                # two clients chat + presence join/leave over /bus
    e2e_envelope.rs                # ciphertext passthrough byte-for-byte, filters no-op
```

## 10. Milestones

1. **M0 — Scaffold**: Cargo project, axum skeleton, `bus serve` CLI, tracing, `/health`.
2. **M1 — Core protocol (in-memory only)**: publish (`PUT`/`POST`), subscribe
   `/json`/`/sse`/`/raw`/`/ws`, in-memory ring buffer for `since=`/`poll=`. Validate with
   curl/websocat against `refs/ntfy/docs` examples.
3. **M2 — Persistence**: sled-backed message cache with retention limits; since= resolves
   against sled; cache survives restart.
4. **M3 — Auth & ACL**: users/tokens/argon2 in sled; Basic/Bearer + `?auth=` query param;
   per-topic ACL + default-access; `bus admin` subcommands.
5. **M4 — Bus extension**: `/bus` WS endpoint, client-originated publish, control envelope
   (join/leave/presence/typing/ack/ping-pong/error/close), presence roster, write-gated send.
6. **M5 — E2E envelope**: shape validation for `encoding: "e2e"` + `enc{alg,kid,nonce}`,
   size caps, documentation of client-side key derivation; confirm filters/cache treat
   ciphertext as opaque.
7. **M6 — Hardening**: per-IP/per-topic rate limiting, payload size limits, connection/topic
   caps, graceful shutdown, slow-consumer disconnect policy, integration test suite,
   Dockerfile.

## 11. Testing strategy

- Unit tests per module: `since=` parsing, ACL default-access fallback, cache retention
  pruning, control-frame validation.
- Integration tests driving the real `axum::Router` in-process (`tower::ServiceExt::oneshot`
  for HTTP, a bound `TcpListener` + `tokio-tungstenite` client for WS/bus): publish then
  read back via each of json/sse/raw/ws; restart-and-replay via a temp sled dir; ACL
  allow/deny matrix; two-participant bus chat with presence events; encrypted envelope
  passed through byte-identical with filters correctly ignored.

## 12. Known risks / open items (non-blocking)

- `sled` is in maintenance mode upstream but adequate for an embedded single-node KV store
  at this scale; revisit if multi-node/HA is ever needed.
- `tokio::sync::broadcast`-style fanout needs an explicit slow-consumer policy (drop +
  force-reconnect-with-`since=`, matching ntfy's behavior) rather than unbounded buffering.
- Key exchange (control-message-based) explicitly deferred to v2 per current scope.
