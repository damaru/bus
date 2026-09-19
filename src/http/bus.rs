//! `GET /{topic}/bus` — full-duplex WebSocket bus extension (PLAN.md
//! section 5.2). Requires **read** permission to connect at all; **write**
//! permission gates sending `message`/relay-eligible `control` frames
//! (read-only participants still connect and receive, per PLAN.md).
//!
//! ## Client wire format (documented choice — PLAN.md 4.1/5.2 don't fully
//! pin this down)
//!
//! Incoming WS **text** frames are parsed as one JSON object:
//! - `{"event": "message", "title"?, "message", "priority"?, "tags"?, "click"?}`
//!   — a full envelope-shaped publish. `id`/`seq`/`time`/`topic`/`sender`
//!   are always server-assigned and ignored if sent.
//! - `{"title"?, "message", "priority"?, "tags"?, "click"?}` with **no**
//!   `"event"` key at all — the same shape, treated as an implicit
//!   `message` event. This is the minimal/simple form for clients that
//!   don't want to think about the envelope wrapper.
//! - `{"event": "control", "control": {"type": "typing"|"ack"|"ping", "data"?}}`
//!   — a control frame. Only `typing`/`ack`/`ping` are accepted from
//!   clients; any other `type` (the server-only ones: `join`/`leave`/
//!   `presence`/`pong`/`error`/`close`) gets rejected with an `error`
//!   control frame.
//!
//! Any other shape, or unparseable JSON, gets an `error` control frame
//! back — the connection is never dropped for a malformed frame.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, Uri};
use axum::response::Response;
use futures::StreamExt;
use serde::Deserialize;
use tokio_stream::wrappers::ReceiverStream;

use crate::auth::Visitor;
use crate::error::AppError;
use crate::http::params;
use crate::http::AppState;
use crate::model::{Control, ControlType, Envelope, SinceMarker};
use crate::store::acl::Permission;
use crate::topic::Topic;

/// `GET /{topic}/bus` upgrade handler: authenticates, resolves read/write
/// access, then hands off to [`run_bus`] for the connection's lifetime.
pub async fn bus_ws(
    State(state): State<AppState>,
    Path(topic_name): Path<String>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    params::validate_topic_name(&topic_name)?;
    let query = params::parse_query(&uri);

    let visitor = crate::auth::authenticate(&headers, &query, addr.ip(), &state.users, &state.auth_limiter)?;
    // Read is required just to connect; write is optional and only flips
    // `can_write` (checked, not fatal) — read-only participants can join
    // and receive but any send attempt gets an `error` control frame.
    crate::http::require_permission(&state, &visitor, &topic_name, Permission::Read)?;
    let can_write = crate::http::require_permission(&state, &visitor, &topic_name, Permission::Write).is_ok();

    // Default `since=none` (no backlog) per PLAN.md 5.2, unless the client
    // explicitly asks for one — `parse_since(.., poll=false)` already
    // defaults to `SinceMarker::None` when `since=` is absent.
    let since = params::parse_since(&headers, &query, false)?;
    let sender_label = resolve_sender_label(&headers, &query, &visitor);

    let topic = state.topics.get_or_create(&topic_name);

    Ok(ws.on_upgrade(move |socket| run_bus(socket, topic, topic_name, since, sender_label, can_write)))
}

/// Resolves the client's "sender" label (PLAN.md 4.1's opaque
/// participant/session label): an explicit `sender`/`x-sender`
/// header-or-query value wins, then the authenticated username, then a
/// short random anonymous label.
fn resolve_sender_label(headers: &HeaderMap, query: &HashMap<String, String>, visitor: &Visitor) -> String {
    if let Some(s) = params::read_param(headers, query, &["x-sender", "sender"]) {
        return s;
    }
    if let Some(username) = &visitor.username {
        return username.clone();
    }
    format!("anon-{}", &crate::model::generate_message_id()[..6])
}

/// Envelope-or-minimal client frame shape — see the module doc comment for
/// the documented wire format.
#[derive(Debug, Default, Deserialize)]
struct IncomingFrame {
    event: Option<String>,
    title: Option<String>,
    message: Option<String>,
    priority: Option<serde_json::Value>,
    tags: Option<Vec<String>>,
    click: Option<String>,
    control: Option<RawControlFrame>,
}

#[derive(Debug, Deserialize)]
struct RawControlFrame {
    #[serde(rename = "type")]
    r#type: ControlType,
    #[serde(default)]
    data: Option<serde_json::Value>,
}

/// Drives one `/bus` connection end to end: `open` -> backlog replay ->
/// `presence` -> `bus_join` (which broadcasts `join`) -> read/write loop
/// -> `bus_leave` (which broadcasts `leave`) on disconnect.
async fn run_bus(
    mut socket: WebSocket,
    topic: Arc<Topic>,
    topic_name: String,
    since: SinceMarker,
    sender_label: String,
    can_write: bool,
) {
    if send_envelope(&mut socket, &Envelope::open(topic_name.clone())).await.is_err() {
        return;
    }

    for env in topic.replay_since(&since) {
        if send_envelope(&mut socket, &env).await.is_err() {
            return;
        }
    }

    // Roster snapshot BEFORE this client joins, so `presence` reflects
    // "everyone already here" (symmetric with `join` only going to
    // *other* participants).
    let roster = topic.roster();
    let roster_data = serde_json::to_value(&roster).unwrap_or(serde_json::Value::Array(vec![]));
    let presence = Envelope::control(
        topic_name.clone(),
        Control::new(ControlType::Presence, "server").with_data(roster_data),
    );
    if send_envelope(&mut socket, &presence).await.is_err() {
        return;
    }

    let (session_id, rx) = topic.bus_join(sender_label, can_write);
    let mut live = ReceiverStream::new(rx);

    loop {
        tokio::select! {
            maybe_env = live.next() => {
                match maybe_env {
                    Some(env) => {
                        if send_envelope(&mut socket, &env).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        handle_incoming_text(&mut socket, &topic, &topic_name, &session_id, can_write, &text).await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => continue, // binary/ping/pong WS frames: app-level ping is a control frame, not a WS ping
                    Some(Err(_)) => break,
                }
            }
        }
    }

    topic.bus_leave(&session_id);
}

/// Parses and dispatches one incoming client text frame. Never causes the
/// connection to drop — malformed/unsupported/permission-denied frames all
/// just get an `error` control frame written back.
async fn handle_incoming_text(
    socket: &mut WebSocket,
    topic: &Arc<Topic>,
    topic_name: &str,
    session_id: &str,
    can_write: bool,
    text: &str,
) {
    let frame: IncomingFrame = match serde_json::from_str(text) {
        Ok(f) => f,
        Err(e) => {
            send_error(socket, topic_name, &format!("malformed JSON: {e}")).await;
            return;
        }
    };

    match frame.event.as_deref() {
        Some("control") => {
            let Some(control) = frame.control else {
                send_error(socket, topic_name, "control event missing 'control' field").await;
                return;
            };
            if !matches!(control.r#type, ControlType::Typing | ControlType::Ack | ControlType::Ping) {
                send_error(
                    socket,
                    topic_name,
                    &format!("clients may not send control type '{:?}'", control.r#type),
                )
                .await;
                return;
            }
            if !can_write {
                send_error(socket, topic_name, "permission denied: read-only participant").await;
                return;
            }
            if let Err(e) = topic.bus_relay_control(session_id, control.r#type, control.data) {
                send_error(socket, topic_name, &e.to_string()).await;
            }
        }
        Some("message") | None => {
            if !can_write {
                send_error(socket, topic_name, "permission denied: read-only participant").await;
                return;
            }
            let priority = match frame.priority {
                Some(v) => match parse_priority_value(&v) {
                    Ok(p) => Some(p),
                    Err(e) => {
                        send_error(socket, topic_name, &e).await;
                        return;
                    }
                },
                None => None,
            };
            let tags = frame.tags.unwrap_or_default();
            let result = topic.bus_publish_message(session_id, frame.title, frame.message, priority, tags, frame.click);
            if let Err(e) = result {
                send_error(socket, topic_name, &e.to_string()).await;
            }
        }
        Some(other) => {
            send_error(socket, topic_name, &format!("unsupported event type '{other}'")).await;
        }
    }
}

/// Accepts either a JSON number (`1`..`5`) or a priority alias string, same
/// rules as the HTTP publish path's `priority` param/field.
fn parse_priority_value(v: &serde_json::Value) -> Result<u8, String> {
    match v {
        serde_json::Value::Number(n) => {
            let p = n.as_u64().ok_or_else(|| "invalid priority".to_string())?;
            if (1..=5).contains(&p) {
                Ok(p as u8)
            } else {
                Err("invalid priority".to_string())
            }
        }
        serde_json::Value::String(s) => params::parse_priority(s).map_err(|e| e.to_string()),
        _ => Err("invalid priority".to_string()),
    }
}

/// Sends an `error` control frame directly to `socket` (not broadcast).
async fn send_error(socket: &mut WebSocket, topic_name: &str, message: &str) {
    let control = Control::new(ControlType::Error, "server").with_data(serde_json::json!({ "error": message }));
    let envelope = Envelope::control(topic_name.to_string(), control);
    let _ = send_envelope(socket, &envelope).await;
}

async fn send_envelope(socket: &mut WebSocket, envelope: &Envelope) -> Result<(), axum::Error> {
    let text = serde_json::to_string(envelope).unwrap_or_default();
    socket.send(Message::Text(text)).await
}
