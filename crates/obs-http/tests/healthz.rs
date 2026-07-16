use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use obs_http::{AppState, Metrics};
use obs_store::{Store, StoreConfig};
use tower::ServiceExt;

fn state_for(db_path: std::path::PathBuf) -> AppState {
    AppState {
        db_path,
        metrics: Arc::new(Metrics::new()),
    }
}

#[tokio::test]
async fn healthz_ok_when_db_readable() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("h.db");
    drop(Store::open(&StoreConfig::new(&db_path)).unwrap());

    let router = obs_http::router(state_for(db_path));
    let response = router
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn healthz_503_when_db_unreadable() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("h.db");
    drop(Store::open(&StoreConfig::new(&db_path)).unwrap());

    // The IMPLEMENTATION-PLAN M0 chmod test: revoking permissions must
    // degrade /healthz — which holds only because the probe opens a NEW
    // connection per request (open FDs survive chmod).
    std::fs::set_permissions(&db_path, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::File::open(&db_path).is_ok() {
        // Running as root (permissions not enforced): the probe cannot be
        // exercised this way; skip rather than assert a lie.
        eprintln!("skipping: chmod 000 does not revoke read access for this user");
        return;
    }

    let router = obs_http::router(state_for(db_path.clone()));
    let response = router
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    // Restore so the tempdir can be cleaned up.
    std::fs::set_permissions(&db_path, std::fs::Permissions::from_mode(0o644)).unwrap();
}

#[tokio::test]
async fn metrics_exports_m0_gauges() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("h.db");
    drop(Store::open(&StoreConfig::new(&db_path)).unwrap());

    let state = state_for(db_path);
    state.metrics.db_size_bytes.set(1234);
    let router = obs_http::router(state);
    let response = router
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("obs_db_size_bytes 1234"), "{text}");
    assert!(text.contains("obs_ingest_channel_depth"), "{text}");
}
