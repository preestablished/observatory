//! REST shape + grain-selection tests (API.md §3.2): score-curve and
//! timeseries, including the ≤1500-points guarantee across grains.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use obs_http::{AppState, Metrics};
use obs_store::{spawn_writer, ProjectionContext, Store, StoreConfig};
use obs_types::{FixedClock, Grain, MetricSample, RollupRow, SeriesKey, WriteBatch};
use tower::ServiceExt;

const S: i64 = 1_000_000_000;

struct Api {
    state: AppState,
    writer: obs_store::WriterHandle,
    db_path: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

async fn api(now_ns: i64) -> Api {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("api.db");
    let store = Store::open(&StoreConfig::new(&db_path)).unwrap();
    let pool = store.read_pool();
    let (conn, _) = store.into_parts();
    let (writer, _join) = spawn_writer(conn, ProjectionContext::default());
    let state = AppState {
        db_path: db_path.clone(),
        metrics: Arc::new(Metrics::new()),
        pool,
        clock: Arc::new(FixedClock(now_ns)),
    };
    Api {
        state,
        writer,
        db_path,
        _dir: dir,
    }
}

async fn get_json(state: &AppState, uri: &str) -> (StatusCode, serde_json::Value) {
    let response = obs_http::router(state.clone())
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1 << 22)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn score_curve_returns_api_shape() {
    let api = api(100 * S).await;
    {
        let conn = rusqlite::Connection::open(&api.db_path).unwrap();
        conn.execute_batch(
            "INSERT INTO score_points (run_id, expansion_idx, ts_wall_ns, score, node_id) VALUES
               ('run-x', 10, 1000, 0.2, 'n-1'),
               ('run-x', 25, 2000, 0.5, 'n-2');",
        )
        .unwrap();
    }
    let (status, body) = get_json(&api.state, "/api/v1/runs/run-x/score-curve").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        serde_json::json!({ "points": [[10, 0.2, "n-1"], [25, 0.5, "n-2"]] })
    );

    let (status, body) = get_json(&api.state, "/api/v1/runs/run-x/score-curve?by=wall").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["points"][0][0], 1000);

    let (status, body) = get_json(&api.state, "/api/v1/runs/run-x/score-curve?by=nope").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "bad-request");
}

#[tokio::test]
async fn timeseries_serves_derived_and_svc_series() {
    let now = 1_000 * S;
    let api = api(now).await;
    let labels = r#"{"run_id":"run-x"}"#;
    let mut samples = vec![];
    for i in 0..10 {
        samples.push(MetricSample {
            key: SeriesKey::new("derived", "search_best_score", labels),
            ts_ns: now - (10 - i) * S,
            value: 0.1 * i as f64,
        });
        samples.push(MetricSample {
            key: SeriesKey::new("state-scorer", "sc_archive_cells", "{}"),
            ts_ns: now - (10 - i) * S,
            value: (100 + i) as f64,
        });
    }
    api.writer
        .write(WriteBatch::MetricSamples(samples))
        .await
        .unwrap();

    let (status, body) = get_json(
        &api.state,
        "/api/v1/runs/run-x/timeseries?metrics=search_best_score,svc:state-scorer:sc_archive_cells&step=auto",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let series = body["series"].as_array().unwrap();
    assert_eq!(series.len(), 2);
    assert_eq!(series[0]["metric"], "search_best_score");
    assert_eq!(series[0]["labels"]["run_id"], "run-x");
    assert_eq!(series[0]["points"].as_array().unwrap().len(), 10);
    assert_eq!(series[1]["metric"], "svc:state-scorer:sc_archive_cells");
    // Raw grain: values verbatim.
    assert_eq!(series[1]["points"][9][1], 109.0);
}

#[tokio::test]
async fn timeseries_rate_from_rollups_has_no_negative_points() {
    let now = 10_000 * S;
    let api = api(now).await;
    // Interned series + synthetic 5s rollup rows with a counter reset.
    {
        let conn = rusqlite::Connection::open(&api.db_path).unwrap();
        conn.execute(
            "INSERT INTO metric_series (service, metric, labels)
             VALUES ('state-scorer', 'sc_score_requests_total', '{}')",
            [],
        )
        .unwrap();
    }
    let series_id: i64 = api
        .state
        .pool
        .with_read(|conn| {
            conn.query_row("SELECT series_id FROM metric_series", [], |row| row.get(0))
        })
        .unwrap();
    // Force rollup grain by exceeding 1500 raw points... instead, insert
    // >1500 raw samples cheaply is slow; rely on rollup rows + raw counts:
    // put 2000 raw samples over ~3 hours so raw > 1500 and grain 1m fits.
    let mut samples = Vec::new();
    let mut value = 0.0;
    for i in 0..2_000 {
        // Counter reset mid-window.
        value = if i == 1_000 { 5.0 } else { value + 10.0 };
        samples.push(MetricSample {
            key: SeriesKey::new("state-scorer", "sc_score_requests_total", "{}"),
            ts_ns: now - (2_000 - i) * 5 * S,
            value,
        });
    }
    api.writer
        .write(WriteBatch::MetricSamples(samples))
        .await
        .unwrap();
    // Fold into 1m rollups directly (the ticker path is covered in
    // obs-derive tests): fold_samples over the raw set.
    let raw: Vec<(i64, i64, f64)> = api
        .state
        .pool
        .with_read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT series_id, ts_ns, value FROM metrics_raw ORDER BY series_id, ts_ns",
            )?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
            rows.collect()
        })
        .unwrap();
    assert_eq!(raw.len(), 2_000);
    let mut rows: Vec<RollupRow> = Vec::new();
    for (sid, ts, v) in raw {
        let bucket = ts - ts.rem_euclid(Grain::M1.width_ns());
        match rows.last_mut() {
            Some(r) if r.series_id == sid && r.bucket_ns == bucket => {
                r.n += 1;
                r.sum += v;
                r.min = r.min.min(v);
                r.max = r.max.max(v);
                r.last = v;
            }
            _ => rows.push(RollupRow {
                series_id: sid,
                bucket_ns: bucket,
                n: 1,
                sum: v,
                min: v,
                max: v,
                first: v,
                last: v,
            }),
        }
    }
    api.writer
        .write(WriteBatch::RollupFold {
            grain: Grain::M1,
            rows,
            high_water_ns: now,
        })
        .await
        .unwrap();

    let from = now - 2_000 * 5 * S;
    let (status, body) = get_json(
        &api.state,
        &format!(
            "/api/v1/runs/run-x/timeseries?metrics=svc:state-scorer:sc_score_requests_total:rate&from_ns={from}&to_ns={now}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let points = body["series"][0]["points"].as_array().unwrap();
    assert!(
        points.len() <= 1_500,
        "grain selection failed: {}",
        points.len()
    );
    assert!(!points.is_empty());
    for point in points {
        let rate = point[1].as_f64().unwrap();
        assert!(rate >= 0.0, "negative rate at {point:?}");
    }
    assert_eq!(series_id, 1);
}

#[tokio::test]
async fn timeseries_stays_under_1500_points_across_grains() {
    // Windows sized to force raw, 5s, 1m, and 10m grains; the series has
    // dense raw data so raw would blow the budget on large windows.
    let now = 4_000_000 * S;
    let api = api(now).await;
    // 3000 raw samples, one per second, ending at `now`.
    let samples: Vec<MetricSample> = (0..3_000)
        .map(|i| MetricSample {
            key: SeriesKey::new("stub", "g", "{}"),
            ts_ns: now - (3_000 - i) * S,
            value: i as f64,
        })
        .collect();
    api.writer
        .write(WriteBatch::MetricSamples(samples))
        .await
        .unwrap();
    // Populate every rollup grain from the raw data.
    let raw: Vec<(i64, i64, f64)> = api
        .state
        .pool
        .with_read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT series_id, ts_ns, value FROM metrics_raw ORDER BY series_id, ts_ns",
            )?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
            rows.collect()
        })
        .unwrap();
    for grain in [Grain::S5, Grain::M1, Grain::M10] {
        let mut rows: Vec<RollupRow> = Vec::new();
        for &(sid, ts, v) in &raw {
            let bucket = ts - ts.rem_euclid(grain.width_ns());
            match rows.last_mut() {
                Some(r) if r.series_id == sid && r.bucket_ns == bucket => {
                    r.n += 1;
                    r.sum += v;
                    r.min = r.min.min(v);
                    r.max = r.max.max(v);
                    r.last = v;
                }
                _ => rows.push(RollupRow {
                    series_id: sid,
                    bucket_ns: bucket,
                    n: 1,
                    sum: v,
                    min: v,
                    max: v,
                    first: v,
                    last: v,
                }),
            }
        }
        api.writer
            .write(WriteBatch::RollupFold {
                grain,
                rows,
                high_water_ns: now,
            })
            .await
            .unwrap();
    }

    // raw window (≤1500 raw points), then windows forcing 5s, 1m, 10m.
    for (window_s, label) in [
        (1_000i64, "raw"),
        (3_000, "5s"),
        (7_000 * 5, "1m"),
        (1_500 * 600 + 600, "10m"),
    ] {
        let from = now - window_s * S;
        let (status, body) = get_json(
            &api.state,
            &format!("/api/v1/runs/r/timeseries?metrics=svc:stub:g&from_ns={from}&to_ns={now}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{label}");
        let points = body["series"][0]["points"].as_array().unwrap();
        assert!(
            points.len() <= 1_500,
            "{label}: {} points exceed the budget",
            points.len()
        );
    }
}

#[tokio::test]
async fn timeseries_error_shapes() {
    let api = api(100 * S).await;
    let (status, body) = get_json(&api.state, "/api/v1/runs/r/timeseries").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "bad-request");

    let (status, body) = get_json(
        &api.state,
        "/api/v1/runs/r/timeseries?metrics=svc:too:many:parts:here",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "bad-metric");

    // Unknown metric: empty series list, not an error.
    let (status, body) = get_json(&api.state, "/api/v1/runs/r/timeseries?metrics=nope").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["series"], serde_json::json!([]));
}
