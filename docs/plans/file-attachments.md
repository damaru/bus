# Plan: File Attachments (ntfy parity gap)

## Problem

`bus` implements ntfy's core pub/sub wire protocol (verified compatible with
the real `ntfy` CLI/mobile clients — see session notes), but has no way to
attach a binary file to a message. Real ntfy clients (Android/iOS/web app,
CLI `ntfy publish -f`) can send images/files alongside a notification and
expect an `attachment` object in the envelope with a downloadable `url`.
This is a known, explicitly scoped-in gap (Firebase push and the bundled web
UI are explicitly OUT of scope per user decision).

## Goal

A client can `PUT`/`POST` a file to a topic (via `filename=`/`X-Filename`
header, ntfy-compatible) and:
1. The file is stored durably on the server's local filesystem.
2. The resulting `Envelope` carries an `attachment: {name, type, size,
   expires, url}` object, delivered to all subscribers exactly like any
   other message (JSON/SSE/raw/WS/bus — no special-casing needed there,
   it's just a new optional field).
3. `GET /file/{id}` downloads the file with correct `Content-Type` and
   `Content-Disposition`.
4. Attachments expire and are reclaimed independently of (and typically
   sooner than) the message cache retention sweep, with global and
   per-file size caps enforceable via config.
5. A client can also attach a **remote** URL (`attach=`/`X-Attach` header)
   without uploading bytes through the server at all (ntfy's "Case 3").

## Non-goals (this plan)

- Firebase/FCM push delivery of attachments — explicitly deferred.
- Bundled web UI for browsing/uploading attachments — explicitly deferred.
- Per-visitor/per-user attachment bandwidth or quota accounting (ntfy's
  `visitor.BandwidthLimiter`) — only global total-size + per-file caps.
- S3/remote object storage backend — local filesystem only (matches
  ntfy's `fileBackend`, the simpler of ntfy's two backends).
- Image thumbnailing/preview generation.

## Reference (ntfy's actual behavior, `refs/ntfy`)

- `attachment/backend_file.go`: blob stored at `{dir}/{id}` — **no
  extension in the filename on disk**. `Put`/`Get`/`Delete`/`List` are
  trivial `os` calls.
- `server/server.go`:
  - `handlePublishBody` (~line 1290): dispatches on whether `m.Attachment`
    was pre-populated by `x-attach` (remote URL, "Case 3") vs `x-filename`
    ("Case 4", `handleBodyAsAttachment`).
  - Header aliases: filename = `x-filename`, `filename`, `file`, `f`
    (line 1155); remote URL = `x-attach`, `attach`, `a` (line 1156).
  - Placeholder message when body/message empty:
    `"You received a file: %s"` (line 160).
  - `handleBodyAsAttachment` (~line 1329): enforces file-size-limit +
    remaining total-size-limit *before* writing, sets
    `Attachment.Expires` (capped to never outlive the message's own
    `expires`), builds `URL = fmt.Sprintf("%s/file/%s%s", BaseURL, ID,
    ext)`, requires `BaseURL != ""` else `errHTTPBadRequestAttachmentsDisallowed`
    (400).
  - `handleFile` (~line 751): `GET`/`HEAD /file/{id}(.ext)?`, extension
    is parsed but ignored for lookup (`fileRegex` at line 139:
    `^/file/([-_A-Za-z0-9]{1,64})(?:\.[A-Za-z0-9]{1,16})?$`); serves via
    `Content-Disposition: attachment; filename="..."` + sniffed
    `Content-Type`; 404 if file/message not found.
  - Config defaults (`server/config.go` ~line 71): file-size-limit 15 MB,
    total-size-limit 5 GB, expiry 3h.
  - `errHTTPEntityTooLargeAttachment` -> HTTP 413.

## Design (bus-specific, grounded in current code)

### 1. `model.rs` — new `Attachment` struct + `Envelope.attachment` field

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attachment {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,   // MIME type, e.g. "image/png"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,        // bytes; None for remote-URL attachments
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires: Option<i64>,     // unix seconds; None for remote-URL
    pub url: String,
}
```

Add `pub attachment: Option<Attachment>` to `Envelope` (after `enc`, before
`control`), `#[serde(skip_serializing_if = "Option::is_none")]`. **Do not**
change `Envelope::new_message`'s positional-arg signature (already
`#[allow(clippy::too_many_arguments)]` and has 3 call sites) — instead set
`env.attachment = Some(...)` post-construction, exactly like
`new_bus_message` already does for `env.sender` (model.rs ~line 227).

### 2. `config.rs` — new fields (all optional; feature disabled unless both
`attachment_dir` and `base_url` are set — matches ntfy's `BaseURL != ""`
gate)

```rust
pub attachment_dir: Option<PathBuf>,       // default: None (disabled)
pub base_url: Option<String>,              // default: None (disabled); e.g. "https://bus.example.com"
pub attachment_file_size_limit: u64,       // default 15 * 1024 * 1024
pub attachment_total_size_limit: u64,      // default 5 * 1024 * 1024 * 1024
pub attachment_expiry: Duration,           // default 3h, reuse parse_duration_str
```

Reject at startup (in `main.rs`, alongside existing config validation if
any) if exactly one of `attachment_dir`/`base_url` is set without the
other — fail fast with a clear error rather than silently disabling.

### 3. `src/store/attachments.rs` (new module, mirrors `store/acl.rs` /
`store/users.rs` pattern) — `Attachments` struct

Wraps a dedicated sled tree (`db.open_tree("attachments")`, id -> bincode/
JSON-serialized `Attachment` metadata — reuse whatever (de)serialization
convention `store/cache.rs` already uses for envelopes) **plus** the
filesystem blob directory and a running size counter:

```rust
pub struct Attachments {
    tree: sled::Tree,           // id -> Attachment metadata (small, for GET /file lookup + sweep)
    dir: PathBuf,                // blob storage: {dir}/{id}, no extension (matches ntfy)
    total_bytes: AtomicU64,      // running total; initialized by summing dir on open()
    file_size_limit: u64,
    total_size_limit: u64,
}
```

Methods:
- `open(dir, file_size_limit, total_size_limit) -> io::Result<Self>` —
  `std::fs::create_dir_all(&dir)`, scan existing entries to seed
  `total_bytes` (supports restart without leaking the budget).
- `remaining(&self) -> u64`
- `async fn write(&self, id: &str, bytes: &[u8]) -> io::Result<()>` —
  bounds-check against `file_size_limit`/`remaining()` *before* writing
  (caller already enforces this too, but keep it here as the source of
  truth), `tokio::fs::write(dir.join(id), bytes).await`, update
  `total_bytes`, then `tree.insert(id, metadata)`.
- `async fn read(&self, id: &str) -> io::Result<Vec<u8>>` —
  `tokio::fs::read(dir.join(id)).await`.
- `fn get_meta(&self, id: &str) -> sled::Result<Option<Attachment>>`
- `async fn delete(&self, id: &str) -> io::Result<()>` — remove file
  (ignore `NotFound`), `tree.remove(id)`, decrement `total_bytes`.
- `fn list_expired(&self, now: i64) -> Vec<String>` — scan tree for
  `expires.is_some_and(|e| e < now)`; used by the retention sweep.
- `fn mime_for_filename(name: &str) -> Option<String>` — small static
  extension->MIME table (jpg/jpeg, png, gif, webp, bmp, svg, pdf, txt,
  json, zip, gz, mp3, mp4, mov, webm, doc(x), xls(x)); default
  `application/octet-stream` if unrecognized. (No new crate — matches
  "don't add abstractions beyond what's needed"; `mime_guess` would be
  overkill for this list.)

Wire into `Store` (`src/store/mod.rs`) as
`Store::attachments(&self, dir, file_size_limit, total_size_limit) ->
io::Result<Attachments>`, constructed once in `main.rs` when config has
both `attachment_dir` and `base_url` set, and added to `AppState` as
`pub attachments: Option<Arc<Attachments>>`.

### 4. `Topic::publish_with_id` (small refactor, `src/topic.rs`)

Extract the body of the existing `publish` (topic.rs line 177) into a new
method that accepts a pre-generated id, so the attachment write (which
needs the id to name the blob file) can happen *before* the envelope is
constructed and durably stored — without adding filesystem I/O inside the
`build` closure (which topic.rs's own doc comment says should stay cheap):

```rust
pub fn publish_with_id<F>(&self, id: String, build: F) -> Result<Envelope>
where F: FnOnce(u64, String, i64) -> Envelope
{
    let seq = self.cache.next_seq(&self.name)?;
    self.seq.store(seq, Ordering::SeqCst);
    let time = now_unix();
    let envelope = build(seq, id, time);
    self.cache.store_message(&self.name, &envelope)?;
    { /* ring buffer push, same as today */ }
    self.fan_out(&envelope);
    Ok(envelope)
}

pub fn publish<F>(&self, build: F) -> Result<Envelope>
where F: FnOnce(u64, String, i64) -> Envelope
{
    self.publish_with_id(generate_message_id(), build)
}
```

No existing call site (`http/publish.rs`, `http/bus.rs`) changes.

### 5. `src/http/publish.rs` — new branch before the existing text/UTF-8
body handling

Order of checks (mirrors ntfy's `handlePublishBody` dispatch):

1. Read `filename` via `read_param(headers, query, &["x-filename",
   "filename", "file", "f"])`.
2. Read `attach_url` via `read_param(headers, query, &["x-attach",
   "attach", "a"])`.
3. **If `attach_url` is set** (remote attachment, no upload): build
   `Attachment { name: <derive from URL path segment, or "attachment">,
   type: None, size: None, expires: <now + attachment_expiry, capped to
   message `expires` if scheduled delivery is ever added>, url: attach_url
   }`. Body (if any) is parsed as the normal text message (existing path,
   unchanged) — this is the *only* branch that needs no attachments-enabled
   check, since bus stores nothing (matches ntfy Case 3: still gated by
   `BaseURL` in ntfy but bus can allow it even without local storage
   configured, since there's no disk write — **decision needed**: gate it
   the same as local uploads for consistency, or allow always. Recommend
   gating identically — simpler mental model, one `state.attachments.is_none()`
   check covers both branches).
4. **Else if `filename` is set** (local upload):
   - `400 attachments not allowed` (`AppError::BadRequest`) if
     `state.attachments.is_none()`.
   - Read full request body as bytes (already done for the text path via
     `axum::body::Bytes` extractor — reuse).
   - `413` (new `AppError::PayloadTooLarge`) if `body.len() as u64 >
     attachment_file_size_limit` or `> attachments.remaining()`.
   - `id = generate_message_id()`.
   - `attachments.write(&id, &body).await?` (maps `io::Error` ->
     `AppError::Internal`).
   - `mime = Attachments::mime_for_filename(&filename)`.
   - `expires = now_unix() + attachment_expiry.as_secs() as i64`.
   - message text: existing header-derived message, or if empty,
     `format!("You received a file: {filename}")` (matches ntfy's
     `defaultAttachmentMessage`).
   - `topic_ref.publish_with_id(id, move |seq, id, time| { let mut env =
     Envelope::new_message(...); env.attachment = Some(Attachment { name:
     filename, r#type: Some(mime), size: Some(body.len() as u64), expires:
     Some(expires), url: format!("{base_url}/file/{id}") }); env })`.
5. Else: existing behavior, unchanged.

### 6. `src/http/attachment.rs` (new handler module) — `GET`/`HEAD
/file/:id`

- Strip to the first `model::MESSAGE_ID_LEN` (12) chars of the path param
  (ignore any trailing `.ext` the client appended for display purposes,
  matching ntfy's regex behavior) and validate with
  `model::is_valid_message_id`; `404` on shape mismatch.
- `404` if `state.attachments.is_none()` or `get_meta(id)` returns `None`.
- `404` if `meta.expires.is_some_and(|e| e < now_unix())` (lazy-expiry
  fallback in case the sweep hasn't run yet — cheap, no reason not to).
- `HEAD`: return headers only (`Content-Length`, `Content-Type`), no body
  read.
- `GET`: `attachments.read(id).await` -> `404` on `NotFound`, `500`
  (`AppError::Internal`) on other IO errors; respond with
  `Content-Type: {meta.type or "application/octet-stream"}`,
  `Content-Disposition: attachment; filename="{meta.name}"` (quote/escape
  the filename — reuse or add a tiny helper, no need for a crate for one
  header), body bytes.
- No ACL/auth check beyond what the topic already required at publish
  time — **decision needed**: ntfy's real server does NOT gate `/file/`
  downloads by topic ACL (anyone with the unguessable 12-char ID can
  download); matching that is simplest and consistent with ntfy. Note this
  explicitly in docs since it's a deliberate "security by obscurity of the
  ID" tradeoff inherited from upstream ntfy, not a bus-specific weakening.

Route registration in `http/mod.rs`: add
`.route("/file/:id", get(attachment::download).head(attachment::download))`
above or alongside the existing routes (no conflict — `/file` won't collide
with `/:topic` since axum matches literal segments before wildcards... but
`/:topic` at the *root* level also matches `/file` as a topic name! Need
`.route("/file/:id", ...)` registered so axum's router correctly
disambiguates a literal `/file/...` two-segment path from `/:topic`
one-segment — this works fine since they have different segment counts;
just double check with a quick `cargo test` that publishing to a topic
literally named `file` still round-trips as `/file` (one segment, still
routes to `publish`/`subscribe`), only `/file/{something}` two-segment
paths route to the new handler. No topic name collision risk since ntfy's
own topic-name regex already excludes `/`.)

### 7. Retention sweep (`main.rs` `spawn_retention_sweep`, ~line 168)

Add a second periodic pass (same interval, same `spawn_blocking` task) that
calls `attachments.list_expired(now)` and deletes each (blob + tree entry)
via `attachments.delete(id)`. Independent of message-cache pruning: an
attachment can expire (3h default) while its message is still within the
12h message-cache retention — the message stays, `attachment.url` just
starts 404ing, matching ntfy.

Also: when `Cache::prune` (existing, `store/cache.rs`) removes a message
that has an attachment (message aged out of the 12h/size/count cache
limits before the 3h attachment expiry hit it — rare but possible with a
short `--cache-duration`), it should also clean up the orphaned attachment
via `attachments.delete(id)`. Needs threading `Option<Arc<Attachments>>`
into `Cache::prune`'s call site in `main.rs` (both sweeps already share
`Arc<Cache>`; give the closure access to `Option<Arc<Attachments>>` too).

### 8. `error.rs` — new variant

```rust
AppError::PayloadTooLarge(String), // (StatusCode::PAYLOAD_TOO_LARGE, 41301, msg) — reuses ntfy's own error code 41301
```

### 9. Docs

- `docs/API.md`: new "Attachments" section — publish-with-file example
  (`curl -T photo.jpg "http://host/topic?filename=photo.jpg"` and the
  `ntfy publish -f photo.jpg <url>` CLI equivalent, both verified against
  the real header names), envelope shape with `attachment`, `GET
  /file/{id}` reference, remote-attachment-by-URL example, error codes
  (400 disallowed, 413 too large), and the "no ACL check on /file/{id}"
  security note above.
- `README.md`: add the 4 new config flags to the existing config table;
  one-line mention in Features.
- Also fix the previously-found undocumented gap while touching these
  files: note that `bus admin` cannot run while `bus serve` holds the
  sled lock on the same `--data-dir` (stop the server first). Small,
  unrelated-but-cheap fix, bundle into this same docs pass.

### 10. Tests — `tests/attachments.rs` (new, follow existing
`tests/*.rs` conventions/`tests/common`)

- Publish with `filename=` header + binary body -> envelope has
  `attachment.url`; `GET` that URL returns byte-identical content and
  correct `Content-Type`/`Content-Disposition`.
- Oversized file (> `attachment_file_size_limit`) -> `413`.
- Publish exceeding remaining `attachment_total_size_limit` -> `413`.
- Attachments disabled (no `--attachment-dir`/`--base-url` configured) ->
  `filename=` publish returns `400`.
- Remote attachment (`attach=<url>`) -> envelope's `attachment.url` equals
  the given URL verbatim, no local file written, no size limit applied.
- Expiry sweep: publish with a short `--attachment-expiry`, wait past it,
  assert `GET /file/{id}` -> `404`, while the message itself is still
  retrievable via `since=all` (cache duration left long).
- `HEAD /file/{id}` returns headers with no body / correct
  `Content-Length`.
- Unknown/malformed id -> `404` (not a panic on the id-shape strip).

## Rollout / sequencing (for `implement` skill / `task_run_prd`)

1. `model.rs`: `Attachment` struct + `Envelope.attachment` field.
2. `config.rs`: 4 new fields + CLI flags + startup validation
   (both-or-neither `attachment_dir`/`base_url`).
3. `error.rs`: `PayloadTooLarge` variant.
4. `topic.rs`: `publish_with_id` extraction (pure refactor, verify
   existing tests still pass before moving on).
5. `store/attachments.rs`: new module + `Store::attachments()`.
6. `http/attachment.rs`: `GET`/`HEAD /file/:id` handler + route wiring in
   `http/mod.rs` + `AppState.attachments` field + `main.rs` construction.
7. `http/publish.rs`: filename/attach-url branches.
8. `main.rs`: attachment-expiry sweep pass + orphan cleanup hook in the
   existing message-cache sweep.
9. `tests/attachments.rs`.
10. `docs/API.md` + `README.md` updates (include the `bus admin`/sled-lock
    caveat fix).

Each step should `cargo build` clean and the full existing test suite
(`cargo test`) should stay green before moving to the next step — same
discipline used for M0-M6.

## Open decisions (flag for the user before/while implementing)

1. Gate remote-URL attachments (`attach=`) behind the same
   `attachment_dir`/`base_url` config as local uploads, or allow always
   since no disk I/O is involved? **Recommendation: gate identically**
   (simpler mental model; also `base_url` is still needed to make the
   feature meaningful/consistent for clients rendering the two attachment
   kinds the same way).
2. `/file/{id}` has no ACL check (matches upstream ntfy: unguessable ID is
   the only protection). Confirm this is acceptable, or require the same
   topic-read permission as subscribing (would need a `?topic=` hint or a
   topic->id index, since the download URL alone doesn't carry topic
   context) — **recommendation: match upstream ntfy behavior (no ACL
   check)** for parity and simplicity; document the tradeoff clearly.
