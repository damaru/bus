//! `AppError` -> ntfy-style JSON error responses.
//!
//! Body shape mirrors ntfy's error responses: `{"code", "http", "error"}`.
//!
//! Variants beyond `Internal` aren't constructed yet in M0; they exist for
//! later milestones to use once handlers are added.
#![allow(dead_code)]

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

/// Application-level error type, convertible into an axum HTTP response.
#[derive(Debug)]
pub enum AppError {
    NotFound(String),
    BadRequest(String),
    Unauthorized(String),
    Forbidden(String),
    TooManyRequests(String),
    Internal(String),
}

impl AppError {
    fn parts(&self) -> (StatusCode, u16, &str) {
        match self {
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, 40400, msg.as_str()),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, 40000, msg.as_str()),
            AppError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, 40100, msg.as_str()),
            AppError::Forbidden(msg) => (StatusCode::FORBIDDEN, 40300, msg.as_str()),
            AppError::TooManyRequests(msg) => (StatusCode::TOO_MANY_REQUESTS, 42900, msg.as_str()),
            AppError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, 50000, msg.as_str()),
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    code: u16,
    http: u16,
    error: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, msg) = self.parts();
        let body = ErrorBody {
            code,
            http: status.as_u16(),
            error: msg.to_string(),
        };
        (status, Json(body)).into_response()
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (_, _, msg) = self.parts();
        write!(f, "{msg}")
    }
}

impl std::error::Error for AppError {}
