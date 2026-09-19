# bus API reference

Full reference for `bus`'s HTTP and WebSocket surface. See the [README](../README.md)
for installation/configuration; this document covers wire formats and behavior only.

- [Message envelope](#message-envelope)
- [Publishing](#publishing)
- [Subscribing](#subscribing)
- [Auth & ACL](#auth--acl)
- [Admin CLI](#admin-cli)
- [Bus extension (`/bus`)](#bus-extension-bus)
- [E2E envelope](#e2e-envelope)
- [Errors](#errors)
- [Rate limits & caps](#rate-limits--caps)
- [Graceful shutdown](#graceful-shutdown)

## Message envelope

Every message, control frame, `open`, and `keepalive` sent by the server (and every
`message`/`control` frame accepted from a `/bus` client) is one JSON object with this
shape:

| Field | Type | Presence | Example |
|---|---|---|---|
| `id` | string | always | `"iMKDOcSvCSZ6"` — 12-character mixed-case alphanumeric, randomly generated for every envelope (including ephemeral `open`/`keepalive`/`control` frames, which still get a fresh random `id` even though their `seq` is always `0`). |
| `seq` | integer (u64) | always | `42` — monotonic per-topic sequence number for real published messages; always `0` for `open`/`keepalive`/`control` frames (never persisted, never counted). |
| `time` | integer (unix seconds) | always | `1700000000` |
| `expires` | integer (unix seconds) | **defined in the wire format but never populated by this server** — no code path currently sets it, so it is always omitted from every real response (see [Notes](#notes) below) | — |
| `event` | string enum | always | `"message"` — see [Event values](#event-values) below |
| `topic` | string | always | `"mytopic"` (or the comma-joined multi-topic path segment, for `open`/`keepalive`) |
| `sender` | string | only on `message`/`control` envelopes originated by a `/bus` participant; absent for HTTP-published messages | `"alice"` |
| `title` | string | only if supplied at publish time | `"Disk space warning"` |
| `message` | string | omitted only for control/open/keepalive frames; for real messages, always present — a publish with no body/text at all is stored as the literal string `"triggered"` rather than an empty/absent field | `"disk usage above 90%"` |
| `priority` | integer 1–5 | only if supplied at publish time | `4` |
| `tags` | array of strings | omitted when empty | `["warning","disk"]` |
| `click` | string (URL, not validated) | only if supplied at publish time | `"https://example.com/dashboard"` |
| `content_type` | string | the **only** value this server ever sets is `"text/markdown"` (via the markdown alias/flag); omitted otherwise, which implies plain text | `"text/markdown"` |
| `encoding` | string | only if supplied at publish time; the value `"e2e"` (case-insensitive) triggers shape validation (see [E2E envelope](#e2e-envelope)) — any other value is stored/relayed as opaque text with no validation at all | `"e2e"` |
| `enc` | object `{alg, kid, nonce}` (all strings) | only present when supplied at publish time, normally alongside `encoding: "e2e"` | `{"alg":"xchacha20poly1305","kid":"topic-key-v1","nonce":"Ey2Md4djKesJXxf+NKvRBrRsqj6tHjmF"}` |
| `control` | object `{type, from, data?}` | only when `event == "control"` | `{"type":"join","from":"alice"}` |

### Event values

`event` is a snake_case string. Values actually emitted by this server: `open`,
`keepalive`, `message`, `control`. Three additional values — `message_delete`,
`message_clear`, `poll_request` — are defined in the model for ntfy wire-format
compatibility (and are recognized by the content-filter logic) but this server never
emits them; there is no message-delete/clear or poll-request feature implemented.

### Notes

- `expires` exists in the struct and in the JSON serialization rules (it would be omitted
  when absent, matching every other optional field) purely for ntfy wire-format
  compatibility. No publish path in this server ever sets it — retention is handled
  entirely server-side via `--cache-duration`/`--cache-count`/`--cache-size` (see
  [Rate limits & caps](#rate-limits--caps)), not per-message expiry.
- A client that only understands the original ntfy fields can safely ignore `seq`,
  `sender`, `enc`, and `control` — they're additive.

## Publishing

```
PUT /{topic}
POST /{topic}
```

`{topic}` must match `[-_A-Za-z0-9]{1,64}` (a single topic — multi-topic comma lists are
only accepted on the four subscribe endpoints below, not here). Requires **write**
permission (see [Auth & ACL](#auth--acl)).

### Body modes

- **Plain text** (any `Content-Type` other than `application/json`): the whole request
  body, UTF-8 decoded and trimmed, becomes `message`. An empty body falls back to the
  `message`/`X-Message`/`m` alias below, and if that's empty too, `message` is stored as
  the literal string `"triggered"`.
- **JSON body** (`Content-Type: application/json`): the body is parsed as
  `{"title"?, "message"?, "priority"?, "tags"?, "click"?, "encoding"?, "enc"?}`. This is a
  superset of upstream ntfy, which only accepts a JSON body at `POST /` (with a `topic`
  field inside) — here it's accepted directly at `/{topic}`.

Header/query aliases (headers win over query params when both are set; first match in
each list wins):

| Field | Header/query names |
|---|---|
| Title | `X-Title`, `Title`, `T` |
| Priority | `X-Priority`, `Priority`, `Prio`, `P` — accepts `1`–`5` or `min`/`low`/`default`/`high`/`max`/`urgent` (also accepted as a JSON number or string in the JSON body) |
| Tags | `X-Tags`, `Tags`, `Tag`, `Ta` — comma-separated |
| Click | `X-Click`, `Click` |
| Message override | `X-Message`, `Message`, `M` — used as a fallback when the body-derived text is empty (plain-text mode) or the JSON body's `message` field is absent/empty; `\n` sequences are unescaped to real newlines |
| Markdown | the `Content-Type` header value `text/markdown`, or `X-Markdown`/`Markdown`/`Md` (boolean: `1`/`yes`/`true`) |
| Encoding | `X-Encoding`, `Encoding` — set to `e2e` for the [E2E envelope](#e2e-envelope) |
| Enc algorithm | `X-Enc-Alg`, `Enc-Alg` |
| Enc key id | `X-Enc-Kid`, `Enc-Kid`, `X-Enc-Key-Id`, `Enc-Key-Id` |
| Enc nonce | `X-Enc-Nonce`, `Enc-Nonce` |

### Examples

Plain text:

```bash
curl -X POST http://localhost:8080/alerts -d "disk usage above 90%"
```

```json
{"id":"EiU8z1I5xd54","seq":1,"time":1700000000,"event":"message","topic":"alerts","message":"disk usage above 90%"}
```

With header aliases:

```bash
curl -X POST http://localhost:8080/alerts \
  -H "X-Title: Disk space warning" \
  -H "X-Priority: 4" \
  -H "X-Tags: warning,disk" \
  -H "X-Click: https://example.com/dashboard" \
  -d "disk usage above 90% on db-1"
```

```json
{"id":"X70jZcaQM3jd","seq":2,"time":1700000000,"event":"message","topic":"alerts","title":"Disk space warning","message":"disk usage above 90% on db-1","priority":4,"tags":["warning","disk"],"click":"https://example.com/dashboard"}
```

JSON body:

```bash
curl -X POST http://localhost:8080/alerts \
  -H "Content-Type: application/json" \
  -d '{"title":"Deploy finished","message":"v1.2.3 deployed to prod","priority":3,"tags":["deploy"]}'
```

```json
{"id":"cHn6BFIcqZmW","seq":3,"time":1700000000,"event":"message","topic":"alerts","title":"Deploy finished","message":"v1.2.3 deployed to prod","priority":3,"tags":["deploy"]}
```

### Response

`200 OK` with the stored [envelope](#message-envelope) as the JSON body (an ntfy-style
publish ack) — exactly what gets fanned out to subscribers.

## Subscribing

All four endpoints accept a comma-separated topic list (e.g. `/topicA,topicB/json`) and
share the same query-param/header table below. All require **read** permission on
*every* named topic (a single denied topic 403s the whole request). Multi-topic backlog
replay is time-sorted across topics but preserves publish order within each topic.

### Shared query params

| Param | Header/query names | Meaning |
|---|---|---|
| `since` | `X-Since`, `Since`, `Si` | See [`since=` syntax](#since-syntax) below. |
| `poll` | `X-Poll`, `Poll`, `Po` | Boolean (`1`/`yes`/`true`). `poll=true` returns only the matching backlog then ends the response/closes the WS — no `open`, no live tail, no `keepalive`. Default `false`. |
| `id` | `X-Id`, `Id` | Only pass messages with this exact `id`. |
| `message` | `X-Message`, `Message`, `M` | Only pass messages whose `message` field exactly equals this string. |
| `title` | `X-Title`, `Title`, `T` | Only pass messages whose `title` field exactly equals this string. |
| `priority` | `X-Priority`, `Priority`, `Prio`, `P` | Comma-separated list of `1`–`5` (or aliases); a message with no priority set counts as `3` for filtering. |
| `tags` | `X-Tags`, `Tags`, `Tag`, `Ta` | Comma-separated; message must have **all** listed tags. |

Filters only apply to `message`/`message_delete`/`message_clear` events — `open`,
`keepalive`, and `control` frames always pass through regardless of filters.

### `since=` syntax

| Value | Meaning |
|---|---|
| *(absent)* | `none` for a live (non-poll) stream; `all` for `poll=true`. |
| `all` | Every message currently in the cache. |
| `none` | No backlog at all. |
| `latest` | Only the single most recently cached message, if any. |
| `<message_id>` | A 12-character alphanumeric id: messages strictly *after* that id, in publish order. An id not found in the cache resolves to an empty backlog (not an error). |
| `<unix_timestamp>` | An integer: messages with `time >= <unix_timestamp>`. |
| `<n>h` / `<n>m` / `<n>s` | Shorthand for "that far back from now" (e.g. `12h`, `30m`, `45s`) — resolved to a unix timestamp at request time. |

Anything else is `400 Bad Request`.

### `GET /{topic}/json`

Content-Type: `application/x-ndjson; charset=utf-8`. One JSON [envelope](#message-envelope)
per line, newline-terminated.

```bash
curl 'http://localhost:8080/alerts/json?poll=true&since=all'
```

```
{"id":"EiU8z1I5xd54","seq":1,"time":1700000000,"event":"message","topic":"alerts","message":"disk usage above 90%"}
{"id":"X70jZcaQM3jd","seq":2,"time":1700000000,"event":"message","topic":"alerts","title":"Disk space warning","message":"disk usage above 90% on db-1","priority":4,"tags":["warning","disk"],"click":"https://example.com/dashboard"}
```

### `GET /{topic}/sse`

Content-Type: `text/event-stream; charset=utf-8`. `message`/`message_delete`/
`message_clear` events are framed as a plain `data:` block (so a browser
`EventSource.onmessage` fires); every other event (`open`, `keepalive`, `control`) gets an
explicit `event: <type>` line first:

```bash
curl -N 'http://localhost:8080/alerts/sse?poll=true'
```

```
data: {"id":"EiU8z1I5xd54","seq":1,"time":1700000000,"event":"message","topic":"alerts","message":"disk usage above 90%"}

```

For a live (non-poll) stream, the leading `open` frame looks like:

```
event: open
data: {"id":"N3TUB1xu06ml","seq":0,"time":1700000000,"event":"open","topic":"alerts"}

```

### `GET /{topic}/raw`

Content-Type: `text/plain; charset=utf-8`. One line per `message` event — just the
`message` field text, with any embedded newlines flattened to spaces. Every non-`message`
event (`open`, `keepalive`, `control`) is rendered as a single blank line.

```bash
curl 'http://localhost:8080/alerts/raw?poll=true'
```

```
disk usage above 90%
disk usage above 90% on db-1
```

### `GET /{topic}/ws`

WebSocket upgrade, **read-only** — a plain ntfy client can use this and never notice the
bus extension exists. Any frame the client sends is ignored (beyond detecting `Close`).
The server sends one text frame per JSON [envelope](#message-envelope), in the same
sequence a live `/json` stream would produce (`open`, backlog, then live `message`s
interleaved with `keepalive`s every 45s — or, in `poll=true` mode, just the backlog
followed by a WS `Close` frame).

```bash
websocat "ws://localhost:8080/alerts/ws?poll=true&since=all"
```

```json
{"id":"N3TUB1xu06ml","seq":0,"time":1700000000,"event":"open","topic":"alerts"}
```
*(only sent when `poll` is not set — omitted here since the example uses `poll=true`)*

## Auth & ACL

Every request resolves to *some* visitor — anonymous if no credentials are given —
before permission is checked. There is no way to require authentication outright; access
control is entirely ACL/default-access driven.

### Credentials

- **Header**: `Authorization: Basic <base64(username:password)>` or
  `Authorization: Bearer <token>`.
- **Query param** (for WebSocket clients that can't set headers on the upgrade request):
  `?auth=<value>` or `?authorization=<value>`, where `<value>` is
  `base64url(no padding)` of the *entire* header value above (`"Basic ..."` or
  `"Bearer ..."`), i.e. **doubly encoded**. When both a header and `?auth=` are present,
  the query param wins.

Worked example — Basic auth for `alice:secret123` via `?auth=`:

```bash
# 1. Build the inner Basic value:
python3 -c "import base64; print('Basic ' + base64.b64encode(b'alice:secret123').decode())"
# -> Basic YWxpY2U6c2VjcmV0MTIz

# 2. base64url-encode that whole string, no padding:
python3 -c "
import base64
inner = b'Basic YWxpY2U6c2VjcmV0MTIz'
print(base64.urlsafe_b64encode(inner).decode().rstrip('='))
"
# -> QmFzaWMgWVd4cFkyVTZjMlZqY21WME1USXo

curl -X POST "http://localhost:8080/secrettopic?auth=QmFzaWMgWVd4cFkyVTZjMlZqY21WME1USXo" \
  -d "via auth param"
```

An empty username with a Basic value (`Basic base64(":sometoken")`) is treated as a
bearer token in the password slot, for clients that can only set one credential field.

### Permission / DefaultAccess

`Permission` (per-grant, bitmask-style): `deny`, `read`, `write`, `read-write`.
`DefaultAccess` (server-wide fallback, `--default-access`): `read-write` (default),
`read-only`, `deny-all`.

### ACL precedence

1. A specific username's matching grant always outranks a `*` (everyone) grant,
   **regardless of pattern specificity**.
2. Among grants for the same principal rank, a longer/more-specific topic pattern beats a
   shorter one (`"myapp-*"` beats `"*"` for topic `"myapp-prod"`; an exact match beats any
   wildcard).
3. If nothing matches, fall back to `--default-access`.
4. Admins (`bus admin user add --role admin`) bypass ACL checks entirely.

Topic patterns may contain `*` anywhere (`myapp-*`, `*-prod`, `a*c`, or bare `*` for
"every topic") — not just as a trailing wildcard.

### 401 vs 403

- **`401 Unauthorized`**: credentials were supplied but are wrong/malformed (bad password,
  unknown bearer token, unparseable Basic value, bad `?auth=` encoding), or the
  per-IP auth-failure limit has been hit (see [Rate limits & caps](#rate-limits--caps)).
- **`403 Forbidden`**: credentials (or anonymity) were accepted, but the resolved ACL
  permission doesn't include the required `read`/`write` bit.

```json
// wrong password
{"code":40100,"http":401,"error":"invalid credentials"}
```

```json
// anonymous publish to a topic with an explicit "*" -> deny grant
{"code":40300,"http":403,"error":"no Write access to topic 'secrettopic'"}
```

## Admin CLI

`bus admin <user|token|acl> ... [--data-dir <dir>]` (default `--data-dir ./data`, same as
`bus serve`). Operates directly on the sled database — **stop `bus serve` first** if it's
running against the same `--data-dir` (sled locks the directory to one process).

| Command | Arguments | Example |
|---|---|---|
| `bus admin user add <username>` | `--password <pw>` (prompted if omitted), `--role user\|admin` (default `user`) | `bus admin user add alice --password secret123 --role user` |
| `bus admin user passwd <username>` | `--password <pw>` (prompted if omitted) | `bus admin user passwd alice --password newpass456` |
| `bus admin user del <username>` | — | `bus admin user del alice` |
| `bus admin token issue <username>` | `--ttl <duration>` (e.g. `30d`/`12h`/`45m`/`90s`; never expires if omitted) | `bus admin token issue alice --ttl 30d` |
| `bus admin token revoke <token>` | — | `bus admin token revoke tk_ij17je0x3rqd240gv41rloz3o6u1o` |
| `bus admin acl grant <principal> <topic_pattern> <permission>` | `<permission>` is one of `deny`/`read`/`write`/`read-write`; `<principal>` is a username or `*` | `bus admin acl grant alice secrettopic read-write` |
| `bus admin acl revoke <principal> <topic_pattern>` | — | `bus admin acl revoke alice secrettopic` |

`token issue` prints the raw token (`tk_` + 29 random lowercase-alphanumeric characters)
to stdout — capture it, it's shown only once.

## Bus extension (`/bus`)

```
GET /{topic}/bus
```

WebSocket upgrade. Requires **read** permission to connect at all; **write** permission
gates sending `message`/relay-eligible `control` frames — a read-only participant still
connects and receives everything, but any send attempt gets an `error` control frame back
(the connection is never dropped for this). `{topic}` is a single topic name (no
comma-separated multi-topic support here).

The `sender` label shown to other participants comes from (in order): a `?sender=` /
`X-Sender` query/header value, then the authenticated username, then a random
`anon-XXXXXX` label.

### Connection lifecycle

1. Server sends `open`.
2. Server replays the `since=` backlog (default `none` — no backlog at all, unlike
   `/json`'s poll-mode default of `all`; pass `?since=all`/`?since=<id>`/etc. explicitly to
   get one). No content filters are available on `/bus` (only `since=`).
3. Server sends a `presence` control frame with the roster of participants already
   connected (*not* including the connecting client itself).
4. Server registers the client as a participant and broadcasts a `join` control frame to
   every *other* current participant/subscriber on the topic.
5. Ongoing: the client may send `message` frames (if it has write access) and `typing`/
   `ack`/`ping` control frames; the server relays/acks them and fans out everything
   (including HTTP-published messages on the same topic — bus participants and plain
   `/json`/`/sse`/`/raw`/`/ws` subscribers all share one fan-out path).
6. On disconnect, the server broadcasts a `leave` control frame to the remaining
   participants.

### Client → server message frame

Either the full envelope shape:

```json
{"event": "message", "title": "optional", "message": "hello", "priority": 5, "tags": ["a","b"], "click": "https://x", "encoding": "e2e", "enc": {"alg": "...", "kid": "...", "nonce": "..."}}
```

or the minimal shape — omit `"event"` entirely and it's treated as an implicit `message`:

```json
{"message": "hello from client 1"}
```

`id`/`seq`/`time`/`topic`/`sender` are always server-assigned; anything the client sends
for those is ignored. `encoding`/`enc` work exactly like the HTTP publish path (see
[E2E envelope](#e2e-envelope)) — same validation, same opaque passthrough.

### Client → server control frame

```json
{"event": "control", "control": {"type": "typing"}}
{"event": "control", "control": {"type": "ack", "data": {"id": "abc123"}}}
{"event": "control", "control": {"type": "ping"}}
```

Only `typing`, `ack`, and `ping` are accepted from a client; sending any other `type`
(the server-only ones below) gets an `error` control frame back.

### Control types

| `type` | Direction | Meaning |
|---|---|---|
| `join` | server → other participants | A new participant connected. |
| `leave` | server → other participants | A participant disconnected. |
| `presence` | server → the newly-connecting client only | Roster snapshot: `data` is an array of `{session_id, sender, can_write, joined_at}` for every *other* current participant. |
| `typing` | client → relayed to every other participant | Ephemeral; never persisted. |
| `ack` | client → relayed to every other participant | Client-defined delivery/read acknowledgement; `data` is free-form (e.g. `{"id": "<message id>"}`). |
| `ping` | client → server | App-level heartbeat request. |
| `pong` | server → **only the requesting client** | Direct reply to `ping` — never broadcast to other participants. |
| `error` | server → the client that sent the bad/denied/oversized frame | `data` is `{"error": "<description>"}`. Never disconnects the client. |
| `close` | server → every connected `/bus` participant | Sent once, right before graceful shutdown begins (see [Graceful shutdown](#graceful-shutdown)). |

A control envelope always looks like:

```json
{"id":"...", "seq":0, "time":1700000000, "event":"control", "topic":"mytopic", "control":{"type":"join","from":"alice"}}
```

(`from` is `"server"` for `presence`/`join`/`leave`/`error`/`close`, or the originating
participant's `sender` label for `typing`/`ack`.)

### Worked two-client example

Frames exactly as asserted by `tests/bus_duplex.rs`. Client 1 connects to
`ws://localhost:8080/duplextopic/bus`, then client 2 connects to
`ws://localhost:8080/duplextopic/bus?sender=client2`:

```jsonc
// client 1 receives, immediately on connect:
{"event":"open", "topic":"duplextopic", ...}
{"event":"control","control":{"type":"presence","from":"server","data":[]}}   // alone so far

// client 2 connects; client 1 then receives:
{"event":"control","control":{"type":"join","from":"client2"}}

// client 2 receives, on its own connect:
{"event":"open", "topic":"duplextopic", ...}
{"event":"control","control":{"type":"presence","from":"server","data":[{"session_id":"...","sender":"<client1's sender>","can_write":true,"joined_at":1700000000}]}}

// client 1 sends: {"message": "hello from client 1"}
// client 2 receives:
{"event":"message","topic":"duplextopic","sender":"<client1's sender>","message":"hello from client 1", ...}
// client 1 ALSO receives its own message back (same shared fan-out as any subscriber):
{"event":"message","topic":"duplextopic","sender":"<client1's sender>","message":"hello from client 1", ...}

// client 2 sends: {"event":"control","control":{"type":"typing"}}
// client 1 receives:
{"event":"control","control":{"type":"typing","from":"client2"}}

// client 1 sends: {"event":"control","control":{"type":"ping"}}
// ONLY client 1 receives:
{"event":"control","control":{"type":"pong","from":"server"}}

// client 1 disconnects; client 2 receives:
{"event":"control","control":{"type":"leave","from":"<client1's sender>"}}
```

### Write-gating

A read-only participant (read but not write ACL permission) that sends a `message` or a
`typing`/`ack`/`ping` control frame gets back:

```json
{"event":"control","control":{"type":"error","from":"server","data":{"error":"permission denied: read-only participant"}}}
```

The connection stays open.

## E2E envelope

The server is a **blind relay** for E2E-encrypted messages: it validates only the *shape*
of the envelope, never decrypts or otherwise interprets `message`/`title` content. Set
`encoding: "e2e"` and populate `enc.{alg,kid,nonce}` (all strings; `alg`/`kid` are
client-defined and never interpreted server-side); `message` (and optionally `title`)
becomes base64 ciphertext.

Validation (only triggered when `encoding` case-insensitively equals `"e2e"` — this is
opt-in per message, not a server-wide mode):

| Rule | Limit |
|---|---|
| `enc` object present, `alg`/`kid`/`nonce` all non-empty | — |
| `enc.alg` / `enc.kid` length | ≤ 128 bytes each |
| `enc.nonce` must decode as base64 (standard or URL-safe, padded or not) | decoded length ≤ 256 bytes |
| `message` / `title` length (as transmitted, i.e. base64 text, not decoded) | ≤ 262144 bytes (256 KiB) each |

The nonce's decoded bytes are discarded immediately after measuring their length — never
used for anything. `message`'s ciphertext content itself is never decoded or inspected
beyond that length cap.

### Example

```bash
CIPHERTEXT=$(head -c 64 /dev/urandom | base64 -w0)
NONCE=$(head -c 24 /dev/urandom | base64 -w0)

curl -X POST http://localhost:8080/e2etopic \
  -H "Content-Type: application/json" \
  -d "{\"message\":\"$CIPHERTEXT\",\"encoding\":\"e2e\",\"enc\":{\"alg\":\"xchacha20poly1305\",\"kid\":\"topic-key-v1\",\"nonce\":\"$NONCE\"}}"
```

```json
{"id":"AWFBKgMd01UR","seq":1,"time":1700000000,"event":"message","topic":"e2etopic","message":"u+ozBOWdmYxPdmTeOc+TVfxYFtkULy1zHSYG70nOHY6KBxoTg/6bI9U0HgE+QTK9K/Dj57suBkA77pRNF7dqYw==","encoding":"e2e","enc":{"alg":"xchacha20poly1305","kid":"topic-key-v1","nonce":"ct5jfIcffM5QCykniAA5QOOhqBm5dh0z"}}
```

The same shape is also accepted as the raw plain-text body (`--data-binary "$CIPHERTEXT"`)
combined with the `X-Encoding`/`X-Enc-Alg`/`X-Enc-Kid`/`X-Enc-Nonce` headers, and as a
`/bus` client message frame (see [Bus extension](#bus-extension-bus)) — all three paths
share the same validation function and produce byte-identical stored/replayed ciphertext.

**Content filters (`message=`, `title=`) are not meaningful against e2e-encoded
messages**: they're plain string-equality checks against whatever text is in those
fields, and ciphertext essentially never equals a plaintext filter value. This is
expected — the server genuinely cannot know the plaintext — not a bug. (Filtering with
the *exact* ciphertext string does still match, which just proves the filter itself
works correctly as a string comparison.)

## Errors

Every error response constructed by this server's handlers is a JSON body:

```json
{"code": <5-digit int>, "http": <http status>, "error": "<message>"}
```

| `AppError` variant | HTTP status | `code` |
|---|---|---|
| `BadRequest` | 400 | 40000 |
| `Unauthorized` | 401 | 40100 |
| `Forbidden` | 403 | 40300 |
| `NotFound` | 404 | 40400 *(defined but never constructed by any handler in this server today — see below)* |
| `TooManyRequests` | 429 | 42900 |
| `Internal` | 500 | 50000 |

```json
// 400 — malformed since= value
{"code":40000,"http":400,"error":"invalid since parameter: not-a-real-value"}
```
```json
// 401 — wrong password
{"code":40100,"http":401,"error":"invalid credentials"}
```
```json
// 403 — ACL denies write access
{"code":40300,"http":403,"error":"no Write access to topic 'secrettopic'"}
```
```json
// 429 — publish rate limit exceeded
{"code":42900,"http":429,"error":"publish rate limit exceeded, slow down"}
```

**413 is not this server's own JSON shape.** An oversized request body is rejected by
axum's `DefaultBodyLimit` middleware *before* any handler runs, so the body is axum's own
plain-text response, not `{"code",...}`:

```
HTTP/1.1 413 Payload Too Large

Failed to buffer the request body: length limit exceeded
```

**404 is also not always this server's shape.** A path that doesn't match any route at
all gets axum's default empty-body `404`; a path that matches a route's shape but the
wrong HTTP method (e.g. `GET /{topic}`, which only has `PUT`/`POST` registered) gets
axum's default empty-body `405 Method Not Allowed`. Neither goes through `AppError`,
since the request never reaches a handler.

## Rate limits & caps

| Limit | Flag | Response when exceeded |
|---|---|---|
| Publish rate (per authenticated user, else per-IP; 60s window, shared across topics and between HTTP publish and `/bus` message frames) | `--publish-rate-limit` | `429` `{"error":"publish rate limit exceeded, slow down"}` (HTTP) or an `error` control frame with the same text (`/bus`) |
| Auth-failure rate (per IP, 60s window; separate limiter from publish rate) | not configurable via a flag | `429` `{"error":"too many authentication failures, try again later"}` |
| Message size (HTTP body and `/bus` WS text frames) | `--max-message-bytes` | `413` (HTTP, axum's own body) or an `error` control frame `"message exceeds max size of <n> bytes"` (`/bus`) |
| Distinct topics | `--max-topics` | `429` `{"error":"server has reached its max-topics limit (<n>)"}` — existing topics are never blocked |
| Subscribers per topic / server-wide | `--max-subscribers-per-topic` / `--max-subscribers-total` | `429` `{"error":"topic '<name>' has reached its subscriber capacity"}` |
| `/bus` participants per topic | `--max-bus-participants-per-topic` | `429` `{"error":"topic '<name>' has reached its /bus participant capacity"}` |

See the [README's configuration table](../README.md#configuration) for every flag's
default value.

## Graceful shutdown

On SIGINT (Ctrl+C) or SIGTERM, the server: broadcasts a `close` control frame to every
currently-connected `/bus` participant, stops accepting new connections, and gives
existing streams/WebSocket connections up to `--shutdown-grace-secs` (default `10`) to
finish on their own before forcibly closing whatever's left and exiting. The sled database
is flushed to disk before the process exits.
