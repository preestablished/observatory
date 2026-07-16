#![forbid(unsafe_code)]
//! HTTP surface (axum): `/healthz` and `/metrics` in M0; the REST API
//! grows here in later milestones.

use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use prometheus::{Encoder, IntGauge, Registry, TextEncoder};

/// Observatory's own metrics registry plus the gauges M0 exports; later
/// packages register their counters here.
#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub db_size_bytes: IntGauge,
    pub ingest_channel_depth: IntGauge,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let db_size_bytes =
            IntGauge::new("obs_db_size_bytes", "SQLite database size in bytes").expect("gauge");
        let ingest_channel_depth = IntGauge::new(
            "obs_ingest_channel_depth",
            "Writer channel queue depth (bounded at 4096)",
        )
        .expect("gauge");
        registry
            .register(Box::new(db_size_bytes.clone()))
            .expect("register obs_db_size_bytes");
        registry
            .register(Box::new(ingest_channel_depth.clone()))
            .expect("register obs_ingest_channel_depth");
        Self {
            registry,
            db_size_bytes,
            ingest_channel_depth,
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct AppState {
    /// Database path for the fresh-open health probe (deliberately NOT a
    /// pooled handle: revoked permissions don't affect open FDs, so only a
    /// per-request open makes /healthz honest about DB availability).
    pub db_path: PathBuf,
    pub metrics: Arc<Metrics>,
}

/// Builds the M0 router: `/healthz` + `/metrics`.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn healthz(State(state): State<AppState>) -> Response {
    let path = state.db_path.clone();
    let probe = tokio::task::spawn_blocking(move || obs_store::probe(&path)).await;
    match probe {
        Ok(Ok(())) => (
            StatusCode::OK,
            axum::Json(serde_json::json!({
                "status": "ok",
                "db": "ok",
                "ingest_lag_s": 0.0,
            })),
        )
            .into_response(),
        Ok(Err(error)) => {
            tracing::warn!(error = %error, "health probe failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(serde_json::json!({
                    "status": "degraded",
                    "db": "unavailable",
                })),
            )
                .into_response()
        }
        Err(join_error) => {
            tracing::error!(error = %join_error, "health probe panicked");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(serde_json::json!({
                    "status": "degraded",
                    "db": "unavailable",
                })),
            )
                .into_response()
        }
    }
}

async fn metrics(State(state): State<AppState>) -> Response {
    let families = state.metrics.registry.gather();
    let mut buffer = Vec::new();
    if let Err(error) = TextEncoder::new().encode(&families, &mut buffer) {
        tracing::error!(error = %error, "metrics encoding failed");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        buffer,
    )
        .into_response()
}
