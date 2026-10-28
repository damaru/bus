use std::net::SocketAddr;
use std::time::Duration;

use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, Uri};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::AppError;
use crate::http::{params, AppState};
use crate::store::acl::Permission;
use crate::store::users::Role;

#[derive(Deserialize)]
pub struct MintReq {
    pub session: String,
    pub ttl_secs: u64,
}

#[derive(Serialize)]
pub struct MintResp {
    pub username: String,
    pub token: String,
    pub topic_pattern: String,
    pub ttl_secs: u64,
}

pub async fn mint_child_token(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
    Json(req): Json<MintReq>,
) -> Result<Json<MintResp>, AppError> {
    let query = params::parse_query(&uri);
    let visitor = crate::auth::authenticate(&headers, &query, addr.ip(), &state.users, &state.auth_limiter)?;
    if !visitor.is_admin {
        return Err(AppError::Forbidden("admin only".into()));
    }
    if req.session.is_empty()
        || req.session.len() > 40
        || !req.session.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(AppError::BadRequest(format!("invalid session id: {}", req.session)));
    }
    if req.ttl_secs == 0 {
        return Err(AppError::BadRequest("ttl_secs must be positive".into()));
    }

    let suffix = format!("{:016x}", rand::random::<u64>());
    let username = format!("child-{}-{}", req.session, suffix);
    let password = format!("{:032x}{:032x}", rand::random::<u128>(), rand::random::<u128>());
    let topic_pattern = format!("session.{}.*", req.session);

    state
        .users
        .create(&username, &password, Role::User)
        .map_err(|e| AppError::Internal(format!("creating child user: {e}")))?;
    state
        .acl
        .grant(&username, "*", Permission::DenyAll)
        .map_err(|e| AppError::Internal(format!("denying child access: {e}")))?;
    state
        .acl
        .grant(&username, &topic_pattern, Permission::ReadWrite)
        .map_err(|e| AppError::Internal(format!("granting child access: {e}")))?;
    let token = state
        .users
        .issue_token(&username, Some(Duration::from_secs(req.ttl_secs)))
        .map_err(|e| AppError::Internal(format!("issuing child token: {e}")))?;

    Ok(Json(MintResp {
        username,
        token,
        topic_pattern,
        ttl_secs: req.ttl_secs,
    }))
}
