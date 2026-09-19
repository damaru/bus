//! `/json`, `/sse`, `/raw`, `/ws` read-only subscribe handlers
//! (ntfy-compatible). Ports `handleSubscribeJSON/SSE/Raw/WS` +
//! `handleSubscribeHTTP` from `refs/ntfy/server/server.go`.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::Response;
use futures::stream::{select_all, Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;

use crate::error::AppError;
use crate::http::params::{self, QueryFilter};
use crate::http::AppState;
use crate::model::{Envelope, Event, SinceMarker};
use crate::store::acl::Permission;
use crate::topic::{SubscriberId, Topic};

/// App-level heartbeat interval for live subscribe streams, matching ntfy's
/// `DefaultKeepaliveInterval`.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(45);

struct PreparedSubscribe {
    topics: Vec<Arc<Topic>>,
    poll: bool,
    since: SinceMarker,
    filters: QueryFilter,
}

/// Shared setup for all four subscribe endpoints: validates/splits the
/// (possibly comma-separated) topic path segment, authenticates the
/// requester and requires **read** access to every named topic (denying
/// the whole multi-topic request with 403 if any one topic is denied,
/// matching ntfy's `authorizeTopic` middleware), resolves each topic in
/// the registry, and parses `poll=`/`since=`/content filters.
fn prepare(
    state: &AppState,
    topic_path: &str,
    headers: &HeaderMap,
    uri: &Uri,
    ip: std::net::IpAddr,
) -> Result<PreparedSubscribe, AppError> {
    let names = params::split_topics(topic_path)?;
    let query = params::parse_query(uri);

    let visitor = crate::auth::authenticate(headers, &query, ip, &state.users, &state.auth_limiter)?;
    for name in &names {
        crate::http::require_permission(state, &visitor, name, Permission::Read)?;
    }

    let poll = params::read_bool_param(false, headers, &query, &["x-poll", "poll", "po"]);
    let since = params::parse_since(headers, &query, poll)?;
    let filters = params::parse_query_filters(headers, &query)?;
    let topics: Vec<Arc<Topic>> = names
        .iter()
        .map(|n| state.topics.get_or_create(n))
        .collect::<Result<Vec<_>, AppError>>()?;

    // M6: reject (429) before committing to a streaming response/WS
    // upgrade if any requested topic is already at its subscriber
    // capacity — see `Topic::subscriber_capacity_available`'s doc comment
    // for why this check happens here rather than inside `subscribe()`.
    for t in &topics {
        if !t.subscriber_capacity_available() {
            return Err(AppError::TooManyRequests(format!(
                "topic '{}' has reached its subscriber capacity",
                t.name()
            )));
        }
    }

    Ok(PreparedSubscribe { topics, poll, since, filters })
}


/// Drops a topic subscription when the holding stream frame is dropped
/// (client disconnect, poll-mode completion, etc).
struct SubscriptionGuard {
    topic: Arc<Topic>,
    id: SubscriberId,
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        self.topic.unsubscribe(self.id);
    }
}

fn collect_backlog(topics: &[Arc<Topic>], since: &SinceMarker) -> Vec<Envelope> {
    let mut backlog: Vec<Envelope> = Vec::new();
    for t in topics {
        backlog.extend(t.replay_since(since));
    }
    // Stable sort: topics interleave by time, but within a topic publish
    // order must be preserved (matches ntfy's sendOldMessages).
    backlog.sort_by_key(|e| e.time);
    backlog
}

/// Produces the full envelope sequence for a subscribe request: in poll
/// mode just the filtered backlog (then ends); otherwise `open`, backlog,
/// then live fanout interleaved with periodic `keepalive`s until the
/// consumer drops the stream.
///
/// Every `filters.pass(&env)` call below applies the `id=`/`message=`/
/// `title=`/`priority=`/`tags=` content filters uniformly to every
/// message, e2e-encoded or not — see [`QueryFilter`]'s doc comment
/// (`http::params`) for why `message=`/`title=` simply don't match
/// e2e-encoded ciphertext (PLAN.md section 7), and why that's expected
/// rather than a bug to special-case here.
fn envelope_stream(
    topics: Vec<Arc<Topic>>,
    poll: bool,
    since: SinceMarker,
    filters: QueryFilter,
    topics_label: String,
) -> impl Stream<Item = Envelope> {
    async_stream::stream! {
        if poll {
            for env in collect_backlog(&topics, &since) {
                if filters.pass(&env) {
                    yield env;
                }
            }
            return;
        }

        // Subscribe before replaying the backlog so we don't miss messages
        // published in the gap between the two (a message could in theory
        // be delivered twice across that boundary; clients are expected to
        // dedupe by `id`, matching ntfy's own best-effort guarantee here).
        let mut guards = Vec::with_capacity(topics.len());
        let mut receivers = Vec::with_capacity(topics.len());
        for t in &topics {
            let (id, rx) = t.subscribe();
            guards.push(SubscriptionGuard { topic: t.clone(), id });
            receivers.push(ReceiverStream::new(rx));
        }
        let mut live = select_all(receivers);

        yield Envelope::open(topics_label.clone());

        for env in collect_backlog(&topics, &since) {
            if filters.pass(&env) {
                yield env;
            }
        }

        let mut interval = tokio::time::interval(KEEPALIVE_INTERVAL);
        interval.tick().await; // first tick fires immediately; we just sent `open`

        loop {
            tokio::select! {
                maybe_env = live.next() => {
                    match maybe_env {
                        Some(env) => {
                            if filters.pass(&env) {
                                yield env;
                            }
                        }
                        None => break,
                    }
                }
                _ = interval.tick() => {
                    yield Envelope::keepalive(topics_label.clone());
                }
            }
        }

        let _ = guards; // keep subscriptions alive until here
    }
}

fn event_name(event: Event) -> &'static str {
    match event {
        Event::Open => "open",
        Event::Keepalive => "keepalive",
        Event::Message => "message",
        Event::MessageDelete => "message_delete",
        Event::MessageClear => "message_clear",
        Event::PollRequest => "poll_request",
        Event::Control => "control",
    }
}

/// `GET /{topic}/json` — newline-delimited JSON stream.
pub async fn subscribe_json(
    State(state): State<AppState>,
    Path(topic_path): Path<String>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, AppError> {
    let p = prepare(&state, &topic_path, &headers, &uri, addr.ip())?;
    let stream = envelope_stream(p.topics, p.poll, p.since, p.filters, topic_path).map(|env| {
        let mut line = serde_json::to_string(&env).unwrap_or_default();
        line.push('\n');
        Ok::<_, Infallible>(Bytes::from(line))
    });
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/x-ndjson; charset=utf-8")
        .body(Body::from_stream(stream))
        .expect("valid response"))
}

/// `GET /{topic}/sse` — Server-Sent Events stream.
pub async fn subscribe_sse(
    State(state): State<AppState>,
    Path(topic_path): Path<String>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, AppError> {
    let p = prepare(&state, &topic_path, &headers, &uri, addr.ip())?;
    let stream = envelope_stream(p.topics, p.poll, p.since, p.filters, topic_path).map(|env| {
        let json = serde_json::to_string(&env).unwrap_or_default();
        let frame = if matches!(env.event, Event::Message | Event::MessageDelete | Event::MessageClear) {
            // Plain "data:" frames fire a browser EventSource's .onmessage().
            format!("data: {json}\n\n")
        } else {
            format!("event: {}\ndata: {json}\n\n", event_name(env.event))
        };
        Ok::<_, Infallible>(Bytes::from(frame))
    });
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream; charset=utf-8")
        .body(Body::from_stream(stream))
        .expect("valid response"))
}

/// `GET /{topic}/raw` — one line per message body; blank lines for
/// open/keepalive/other events.
pub async fn subscribe_raw(
    State(state): State<AppState>,
    Path(topic_path): Path<String>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, AppError> {
    let p = prepare(&state, &topic_path, &headers, &uri, addr.ip())?;
    let stream = envelope_stream(p.topics, p.poll, p.since, p.filters, topic_path).map(|env| {
        let line = if env.event == Event::Message {
            let msg = env.message.unwrap_or_default().replace('\n', " ");
            format!("{msg}\n")
        } else {
            "\n".to_string()
        };
        Ok::<_, Infallible>(Bytes::from(line))
    });
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from_stream(stream))
        .expect("valid response"))
}

/// `GET /{topic}/ws` — read-only WebSocket subscribe (ntfy-compatible; a
/// plain ntfy client can use this and never notice the bus extension
/// exists). Incoming client frames are ignored beyond detecting close.
pub async fn subscribe_ws(
    State(state): State<AppState>,
    Path(topic_path): Path<String>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    let p = prepare(&state, &topic_path, &headers, &uri, addr.ip())?;
    Ok(ws.on_upgrade(move |socket| run_ws(socket, p.topics, p.poll, p.since, p.filters, topic_path)))
}

async fn run_ws(
    mut socket: WebSocket,
    topics: Vec<Arc<Topic>>,
    poll: bool,
    since: SinceMarker,
    filters: QueryFilter,
    topics_label: String,
) {
    let stream = envelope_stream(topics, poll, since, filters, topics_label);
    tokio::pin!(stream);
    loop {
        tokio::select! {
            maybe_env = stream.next() => {
                match maybe_env {
                    Some(env) => {
                        let text = serde_json::to_string(&env).unwrap_or_default();
                        if socket.send(Message::Text(text)).await.is_err() {
                            return;
                        }
                    }
                    None => {
                        // Poll mode: backlog exhausted, close cleanly.
                        let _ = socket.send(Message::Close(None)).await;
                        return;
                    }
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => return,
                    Some(Ok(_)) => continue, // read-only endpoint: ignore client frames
                    Some(Err(_)) => return,
                }
            }
        }
    }
}
