# bus

A minimal, ntfy-compatible pub/sub server written in Rust (axum + tokio + sled), with two
extensions layered on top of the standard ntfy HTTP/WebSocket protocol: a **bidirectional
"bus" channel** (`/{topic}/bus`) for full-duplex group chat with server-relayed control
messages (join/leave/presence/typing/ack/ping-pong), and an optional **end-to-end
encryption envelope** (`encoding: "e2e"`) where the server acts as a pure blind relay and
never decrypts or inspects message content. Plain `curl`, `websocat`, and existing
ntfy-style clients work unmodified against the read-only endpoints (`/json`, `/sse`,
`/raw`, `/ws`).

## Features

- ntfy-compatible `PUT`/`POST /{topic}` publish, with plain-text or JSON bodies and the
  usual `X-Title`/`X-Priority`/`X-Tags`/`X-Click` header aliases.
- Four read-only subscribe formats: NDJSON (`/json`), Server-Sent Events (`/sse`), raw text
  (`/raw`), and read-only WebSocket (`/ws`) — all support `since=`/`poll=` replay and
  `id=`/`message=`/`title=`/`priority=`/`tags=` content filters.
- sled-backed message persistence with configurable retention (age/count/size) that
  survives process restarts; `since=` resolves against the persisted history, not just an
  in-memory ring buffer.
- Full-duplex `/{topic}/bus` WebSocket extension: any participant with write access can
  publish and receive on one connection, with server-relayed control messages (`join`,
  `leave`, `presence`, `typing`, `ack`, `ping`/`pong`, `error`, `close`).
- Users, argon2-hashed passwords, bearer tokens, and a per-topic ACL (`deny`/`read`/
  `write`/`read-write`) with wildcard principals/topic patterns and a configurable
  server-wide default-access fallback.
- `Authorization: Basic`/`Bearer` header auth, plus a `?auth=` query-param encoding for
  browser WebSocket clients that can't set headers.
- Opt-in, per-message E2E envelope (`encoding: "e2e"` + `enc{alg,kid,nonce}`): the server
  validates only shape (base64-decodability, length caps), never plaintext — it's a blind
  relay.
- Per-visitor publish rate limiting, a configurable max message size (enforced on both HTTP
  bodies and `/bus` WebSocket frames), and configurable connection/topic caps.
- Graceful shutdown on SIGINT/SIGTERM: stops accepting new connections, sends a `close`
  control message to active `/bus` participants, and gives in-flight streams a bounded
  grace period to drain before forcing exit.
- `bus admin` CLI for user/token/ACL management directly against the sled database (no HTTP
  admin API).
- Optional file attachments (local upload or remote URL) via `filename=`/`attach=` publish
  params, served back at `GET /file/{id}` with independent expiry from the message cache.

## Quick start

```bash
cargo build --release
./target/release/bus serve --bind 127.0.0.1:8080 --data-dir ./data
```

Publish a message and read it back:

```bash
curl -X POST http://127.0.0.1:8080/mytopic -d "hello world"
curl 'http://127.0.0.1:8080/mytopic/json?poll=true&since=all'
```

With no users or ACL entries configured, the server defaults to `read-write` access for
everyone (`--default-access read-write`), so anonymous publish/subscribe works out of the
box — see [docs/API.md](docs/API.md#auth--acl) for how to lock topics down.

### Docker

```bash
docker build -t bus .
docker run -d --name bus -p 8080:8080 -v bus-data:/data bus
```

The image's `ENTRYPOINT` is `bus serve`, with a default `CMD` of
`--bind 0.0.0.0:8080 --data-dir /data`; the container exposes port `8080` and declares a
`VOLUME` at `/data` for the sled database. Pass extra flags after the image name to
override the default `CMD`, e.g.:

```bash
docker run -d -p 8080:8080 -v bus-data:/data bus --bind 0.0.0.0:8080 --data-dir /data \
  --default-access deny-all
```

## Configuration

All flags apply to `bus serve`. `bus admin ...` only takes `--data-dir` (see
[docs/API.md](docs/API.md#admin-cli)).

| Flag | Default | Description |
|---|---|---|
| `--bind` | `127.0.0.1:8080` | Address (and port) to bind the HTTP server to. |
| `--data-dir` | `./data` | Directory for the sled embedded database. |
| `--cache-duration` | `12h` | How long cached messages are retained before being pruned. |
| `--cache-size` | `1073741824` (1 GiB) | Maximum total size (bytes) of the message cache (best-effort). |
| `--cache-count` | `10000` | Maximum number of cached messages per topic. |
| `--default-access` | `read-write` | Fallback permission when no ACL entry matches: `read-write`, `read-only`, or `deny-all`. |
| `--publish-rate-limit` | `60` | Max publish requests per 60-second window, per authenticated user (or per-IP for anonymous). |
| `--max-message-bytes` | `1048576` (1 MiB) | Max size of a single published message — enforced on both HTTP request bodies and `/bus` WebSocket text frames. |
| `--max-topics` | `1000` | Max distinct topics the server will create before refusing new ones (existing topics are never blocked). |
| `--max-subscribers-per-topic` | `1000` | Max concurrent subscriber connections (`/json`/`/sse`/`/raw`/`/ws`/`/bus`) on a single topic. |
| `--max-subscribers-total` | `10000` | Max concurrent subscriber connections across the whole server (a global soft cap on top of the per-topic cap). |
| `--max-bus-participants-per-topic` | `200` | Max concurrent `/bus` participants on a single topic. |
| `--shutdown-grace-secs` | `10` | Bounded grace period after SIGINT/SIGTERM before forcing remaining connections closed. |
| `--attachment-dir` | unset (disabled) | Directory to store uploaded file attachments. Must be set together with `--base-url` (or both left unset) to enable/disable the feature — setting only one is a startup error. |
| `--base-url` | unset (disabled) | Public base URL clients use to reach this server (e.g. `https://bus.example.com`), used to build attachment download URLs. Must be set together with `--attachment-dir`. |
| `--attachment-file-size-limit` | `15728640` (15 MiB) | Max size (bytes) of a single uploaded attachment. |
| `--attachment-total-size-limit` | `5368709120` (5 GiB) | Max total size (bytes) of all stored attachments combined. |
| `--attachment-expiry` | `3h` | How long an uploaded attachment is retained before being reclaimed (independent of, and typically shorter than, `--cache-duration`). |

See [docs/API.md](docs/API.md) for the full HTTP/WebSocket API reference, including the
message envelope format, every endpoint, auth/ACL semantics, the bus extension's control
messages, the E2E envelope, and exact error responses.

## Development

```bash
cargo test      # unit tests (in-crate) + 6 integration test files under tests/
cargo clippy
```

Project layout:

```
src/
  main.rs        CLI entry point: `bus serve`, `bus admin ...`
  lib.rs         library crate root — build_state()/build_router(), shared by main.rs and tests/
  config.rs      Config struct + clap CLI flags (see table above)
  model.rs       Envelope, Event, Control, ControlType, Enc, SinceMarker
  error.rs       AppError -> JSON error responses
  auth.rs        Authorization header/?auth= extraction, per-IP/per-visitor rate limiters
  topic.rs       TopicRegistry, Topic (in-memory fanout, ring buffer, bus participants/roster)
  store/
    mod.rs       sled::Db handle wrapper
    users.rs     user/token CRUD + argon2 verification
    acl.rs       permission lookup + default-access fallback + topic-pattern globbing
    cache.rs     message persistence, since= resolution, retention pruning
    attachments.rs  attachment blob storage, size accounting, expiry listing
  http/
    mod.rs       axum Router assembly, shared AppState, permission enforcement
    publish.rs   PUT/POST /{topic}
    subscribe.rs /json, /sse, /raw, /ws (read-only)
    bus.rs       /{topic}/bus (full duplex + control messages)
    attachment.rs  GET/HEAD /file/:id download handler
    params.rs    query/header parsing + aliasing, since= parsing, e2e shape validation
  admin/
    mod.rs       `bus admin user|token|acl ...` subcommands
tests/
  common/mod.rs        shared test helpers (temp sled dirs, test router/server builders)
  publish_subscribe.rs roundtrip across json/sse/raw/ws
  since_replay.rs      persistence + since=/poll= across a simulated restart
  auth_acl.rs          allow/deny matrix, default-access modes
  bus_duplex.rs        two clients chat + presence join/leave over /bus
  e2e_envelope.rs      ciphertext passthrough byte-for-byte, filters no-op
  attachments.rs       upload/download roundtrip, size limits, expiry, remote-URL passthrough
```
