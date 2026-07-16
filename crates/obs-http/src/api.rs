//! M2 REST endpoints (API.md §3.2 shapes): score-curve and timeseries
//! with automatic grain selection (≤ 1500 points per series). Errors are
//! `{"error":{"code":..,"message":..}}` with an appropriate HTTP status.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use obs_store::ReadPool;
use obs_types::Grain;

use crate::AppState;

const MAX_POINTS: i64 = 1_500;

fn error_response(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        axum::Json(json!({ "error": { "code": code, "message": message } })),
    )
        .into_response()
}

fn internal(error: impl std::fmt::Display) -> Response {
    tracing::error!(error = %error, "REST query failed");
    error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal",
        "query failed",
    )
}

/// `GET /api/v1/runs/{run_id}/score-curve?by=expansions|wall`
/// → `{"points":[[x, score, node_id], ...]}` straight from score_points
/// (the full curve; it is small).
pub async fn score_curve(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let by = params.get("by").map(String::as_str).unwrap_or("expansions");
    let x_column = match by {
        "expansions" => "expansion_idx",
        "wall" => "ts_wall_ns",
        other => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "bad-request",
                &format!("unknown by={other:?} (expansions|wall)"),
            )
        }
    };
    let pool = state.pool.clone();
    let sql = format!(
        "SELECT {x_column}, score, node_id FROM score_points
         WHERE run_id = ?1 ORDER BY expansion_idx"
    );
    let result = tokio::task::spawn_blocking(move || {
        pool.with_read(|conn| {
            let mut stmt = conn.prepare_cached(&sql)?;
            let rows = stmt.query_map([&run_id], |row| {
                Ok(json!([
                    row.get::<_, i64>(0)?,
                    row.get::<_, f64>(1)?,
                    row.get::<_, String>(2)?
                ]))
            })?;
            rows.collect::<Result<Vec<_>, _>>()
        })
    })
    .await;
    match result {
        Ok(Ok(points)) => axum::Json(json!({ "points": points })).into_response(),
        Ok(Err(error)) => internal(error),
        Err(join_error) => internal(join_error),
    }
}

/// One resolved series request.
struct SeriesSpec {
    /// Name echoed back in the response.
    metric: String,
    service: String,
    prom_metric: String,
    labels: Option<String>,
    mode: Mode,
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Plain,
    Rate,
    P50,
    P99,
}

fn parse_metric(name: &str, run_id: &str) -> Result<SeriesSpec, String> {
    if let Some(rest) = name.strip_prefix("svc:") {
        let parts: Vec<&str> = rest.split(':').collect();
        let (service, prom_metric, mode) = match parts.as_slice() {
            [service, metric] => (service, metric, Mode::Plain),
            [service, metric, "rate"] => (service, metric, Mode::Rate),
            [service, metric, "p50"] => (service, metric, Mode::P50),
            [service, metric, "p99"] => (service, metric, Mode::P99),
            _ => return Err(format!("malformed metric {name:?}")),
        };
        Ok(SeriesSpec {
            metric: name.to_owned(),
            service: (*service).to_owned(),
            prom_metric: (*prom_metric).to_owned(),
            labels: None,
            mode,
        })
    } else {
        // Derived metric: series keyed by this run's label.
        Ok(SeriesSpec {
            metric: name.to_owned(),
            service: "derived".to_owned(),
            prom_metric: name.to_owned(),
            labels: Some(json!({ "run_id": run_id }).to_string()),
            mode: Mode::Plain,
        })
    }
}

/// `GET /api/v1/runs/{run_id}/timeseries?metrics=m1,m2&from_ns=&to_ns=&step=auto`
pub async fn timeseries(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let Some(metric_names) = params.get("metrics") else {
        return error_response(StatusCode::BAD_REQUEST, "bad-request", "missing metrics=");
    };
    let now = state.clock.now_ns();
    let to_ns: i64 = match params.get("to_ns").map(|value| value.parse()) {
        None => now,
        Some(Ok(value)) => value,
        Some(Err(_)) => return error_response(StatusCode::BAD_REQUEST, "bad-request", "bad to_ns"),
    };
    let from_ns: i64 = match params.get("from_ns").map(|value| value.parse()) {
        None => to_ns - 3_600_000_000_000, // default window: 1 h
        Some(Ok(value)) => value,
        Some(Err(_)) => {
            return error_response(StatusCode::BAD_REQUEST, "bad-request", "bad from_ns")
        }
    };
    if to_ns <= from_ns {
        return error_response(StatusCode::BAD_REQUEST, "bad-request", "empty window");
    }

    let mut specs = Vec::new();
    for name in metric_names.split(',').filter(|name| !name.is_empty()) {
        match parse_metric(name, &run_id) {
            Ok(spec) => specs.push(spec),
            Err(message) => return error_response(StatusCode::BAD_REQUEST, "bad-metric", &message),
        }
    }

    let pool = state.pool.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut series_out = Vec::new();
        for spec in &specs {
            query_series(&pool, spec, from_ns, to_ns, &mut series_out)?;
        }
        Ok::<_, obs_types::StoreError>(series_out)
    })
    .await;
    match result {
        Ok(Ok(series)) => axum::Json(json!({ "series": series })).into_response(),
        Ok(Err(error)) => internal(error),
        Err(join_error) => internal(join_error),
    }
}

/// Grain auto-selection: raw when the window's raw point count fits,
/// else the smallest rollup grain whose bucket count fits — decided
/// BEFORE the data query.
fn select_grain(
    pool: &ReadPool,
    series_ids: &[i64],
    from_ns: i64,
    to_ns: i64,
) -> Result<Option<Grain>, obs_types::StoreError> {
    let max_raw: i64 = pool.with_read(|conn| {
        let mut worst = 0i64;
        let mut stmt = conn.prepare_cached(
            "SELECT count(*) FROM metrics_raw
             WHERE series_id = ?1 AND ts_ns >= ?2 AND ts_ns <= ?3",
        )?;
        for series_id in series_ids {
            let count: i64 = stmt
                .query_row(rusqlite::params![series_id, from_ns, to_ns], |row| {
                    row.get(0)
                })?;
            worst = worst.max(count);
        }
        Ok(worst)
    })?;
    if max_raw <= MAX_POINTS {
        return Ok(None); // raw
    }
    let window = to_ns - from_ns;
    for grain in [Grain::S5, Grain::M1, Grain::M10] {
        if window / grain.width_ns() <= MAX_POINTS {
            return Ok(Some(grain));
        }
    }
    Ok(Some(Grain::M10))
}

fn query_series(
    pool: &ReadPool,
    spec: &SeriesSpec,
    from_ns: i64,
    to_ns: i64,
    out: &mut Vec<serde_json::Value>,
) -> Result<(), obs_types::StoreError> {
    // Resolve matching series ids (+labels for the response).
    let matches: Vec<(i64, String)> = pool.with_read(|conn| {
        match &spec.labels {
            Some(labels) => {
                let mut stmt = conn.prepare_cached(
                    "SELECT series_id, labels FROM metric_series
                     WHERE service = ?1 AND metric = ?2 AND labels = ?3",
                )?;
                let rows = stmt.query_map(
                    rusqlite::params![spec.service, spec.prom_metric, labels],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                rows.collect()
            }
            None => {
                // Histogram quantiles read the _bucket series family.
                let metric = match spec.mode {
                    Mode::P50 | Mode::P99 => format!("{}_bucket", spec.prom_metric),
                    _ => spec.prom_metric.clone(),
                };
                let mut stmt = conn.prepare_cached(
                    "SELECT series_id, labels FROM metric_series
                     WHERE service = ?1 AND metric = ?2 ORDER BY series_id",
                )?;
                let rows = stmt.query_map(rusqlite::params![spec.service, metric], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?;
                rows.collect()
            }
        }
    })?;
    if matches.is_empty() {
        return Ok(());
    }
    let series_ids: Vec<i64> = matches.iter().map(|(id, _)| *id).collect();
    let grain = select_grain(pool, &series_ids, from_ns, to_ns)?;

    match spec.mode {
        Mode::P50 | Mode::P99 => {
            let quantile = if spec.mode == Mode::P50 { 0.5 } else { 0.99 };
            let points =
                histogram_quantile_points(pool, &matches, grain, from_ns, to_ns, quantile)?;
            out.push(json!({
                "metric": spec.metric,
                "labels": {},
                "points": points,
            }));
        }
        _ => {
            for (series_id, labels) in &matches {
                let points = match grain {
                    None => raw_points(pool, *series_id, from_ns, to_ns, spec.mode)?,
                    Some(grain) => {
                        rollup_points(pool, *series_id, grain, from_ns, to_ns, spec.mode)?
                    }
                };
                let labels_value: serde_json::Value =
                    serde_json::from_str(labels).unwrap_or_else(|_| json!({}));
                out.push(json!({
                    "metric": spec.metric,
                    "labels": labels_value,
                    "points": points,
                }));
            }
        }
    }
    Ok(())
}

fn raw_points(
    pool: &ReadPool,
    series_id: i64,
    from_ns: i64,
    to_ns: i64,
    mode: Mode,
) -> Result<Vec<serde_json::Value>, obs_types::StoreError> {
    let samples: Vec<(i64, f64)> = pool.with_read(|conn| {
        let mut stmt = conn.prepare_cached(
            "SELECT ts_ns, value FROM metrics_raw
             WHERE series_id = ?1 AND ts_ns >= ?2 AND ts_ns <= ?3 ORDER BY ts_ns",
        )?;
        let rows = stmt.query_map(rusqlite::params![series_id, from_ns, to_ns], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
        rows.collect()
    })?;
    Ok(match mode {
        Mode::Rate => {
            // Raw counter rate: successive deltas over elapsed time.
            let mut points = Vec::new();
            for pair in samples.windows(2) {
                let (t0, v0) = pair[0];
                let (t1, v1) = pair[1];
                if t1 > t0 {
                    let delta = if v1 >= v0 { v1 - v0 } else { v1.max(0.0) };
                    points.push(json!([t1, delta / ((t1 - t0) as f64 / 1e9)]));
                }
            }
            points
        }
        _ => samples
            .into_iter()
            .map(|(ts, value)| json!([ts, value]))
            .collect(),
    })
}

fn rollup_points(
    pool: &ReadPool,
    series_id: i64,
    grain: Grain,
    from_ns: i64,
    to_ns: i64,
    mode: Mode,
) -> Result<Vec<serde_json::Value>, obs_types::StoreError> {
    let sql = format!(
        "SELECT bucket_ns, n, sum, first, last FROM {table}
         WHERE series_id = ?1 AND bucket_ns >= ?2 AND bucket_ns <= ?3 ORDER BY bucket_ns",
        table = grain.table()
    );
    let rows: Vec<(i64, i64, f64, f64, f64)> = pool.with_read(|conn| {
        let mut stmt = conn.prepare_cached(&sql)?;
        let rows = stmt.query_map(rusqlite::params![series_id, from_ns, to_ns], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?;
        rows.collect()
    })?;
    Ok(match mode {
        Mode::Rate => {
            let first_last: Vec<(i64, f64, f64)> = rows
                .iter()
                .map(|(bucket, _, _, first, last)| (*bucket, *first, *last))
                .collect();
            obs_derive_rate(&first_last, grain.width_ns())
                .into_iter()
                .map(|(ts, rate)| json!([ts, rate]))
                .collect()
        }
        _ => rows
            .into_iter()
            .map(|(bucket, n, sum, _, _)| json!([bucket, sum / n.max(1) as f64]))
            .collect(),
    })
}

/// Per-bucket counter rate with reset clamping — kept here (duplicated
/// from obs-derive's pure rate module) so obs-http does not depend on
/// obs-derive just for ~10 lines; both are covered by tests.
fn obs_derive_rate(rows: &[(i64, f64, f64)], width_ns: i64) -> Vec<(i64, f64)> {
    let width_s = width_ns as f64 / 1e9;
    let mut out = Vec::with_capacity(rows.len());
    let mut prev_last: Option<f64> = None;
    for &(bucket_ns, first, last) in rows {
        let base = prev_last.unwrap_or(first);
        let delta = if last >= base {
            last - base
        } else {
            last.max(0.0)
        };
        out.push((bucket_ns, delta / width_s));
        prev_last = Some(last);
    }
    out
}

/// Histogram quantile per output step from `_bucket` series (cumulative
/// counters): per-`le` windowed deltas per step, linear interpolation.
fn histogram_quantile_points(
    pool: &ReadPool,
    bucket_series: &[(i64, String)],
    grain: Option<Grain>,
    from_ns: i64,
    to_ns: i64,
    quantile: f64,
) -> Result<Vec<serde_json::Value>, obs_types::StoreError> {
    // Step width: the selected grain, or 5s over raw.
    let width_ns = grain.map(Grain::width_ns).unwrap_or(5_000_000_000);
    // Collect per-le per-step (first, last).
    struct LeSteps {
        upper: f64,
        steps: std::collections::BTreeMap<i64, (f64, f64)>,
    }
    let mut families: Vec<LeSteps> = Vec::new();
    for (series_id, labels) in bucket_series {
        let le = serde_json::from_str::<serde_json::Value>(labels)
            .ok()
            .and_then(|value| {
                value
                    .get("le")
                    .and_then(|le| le.as_str().map(str::to_owned))
            });
        let Some(le) = le else { continue };
        let upper = if le == "+Inf" {
            f64::INFINITY
        } else {
            match le.parse::<f64>() {
                Ok(value) => value,
                Err(_) => continue,
            }
        };
        let samples: Vec<(i64, f64)> = pool.with_read(|conn| {
            let mut stmt = conn.prepare_cached(
                "SELECT ts_ns, value FROM metrics_raw
                 WHERE series_id = ?1 AND ts_ns >= ?2 AND ts_ns <= ?3 ORDER BY ts_ns",
            )?;
            let rows = stmt.query_map(rusqlite::params![series_id, from_ns, to_ns], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;
            rows.collect()
        })?;
        let mut steps: std::collections::BTreeMap<i64, (f64, f64)> = Default::default();
        for (ts, value) in samples {
            let step = ts - ts.rem_euclid(width_ns);
            steps
                .entry(step)
                .and_modify(|(_, last)| *last = value)
                .or_insert((value, value));
        }
        families.push(LeSteps { upper, steps });
    }
    families.sort_by(|a, b| {
        a.upper
            .partial_cmp(&b.upper)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let all_steps: std::collections::BTreeSet<i64> = families
        .iter()
        .flat_map(|family| family.steps.keys().copied())
        .collect();
    let mut points = Vec::new();
    for step in all_steps {
        let mut cumulative: Vec<(f64, f64)> = Vec::new();
        for family in &families {
            if let Some((first, last)) = family.steps.get(&step) {
                let delta = if last >= first {
                    last - first
                } else {
                    last.max(0.0)
                };
                cumulative.push((family.upper, delta));
            }
        }
        let total = cumulative.last().map(|(_, count)| *count).unwrap_or(0.0);
        if total <= 0.0 {
            continue;
        }
        let target = quantile * total;
        let mut previous_upper = 0.0;
        let mut previous_count = 0.0;
        let mut value = None;
        for &(upper, count) in &cumulative {
            if count >= target {
                value = Some(if upper.is_infinite() {
                    previous_upper
                } else {
                    let span = count - previous_count;
                    let fraction = if span > 0.0 {
                        (target - previous_count) / span
                    } else {
                        1.0
                    };
                    previous_upper + (upper - previous_upper) * fraction
                });
                break;
            }
            previous_upper = upper;
            previous_count = count;
        }
        if let Some(value) = value.or(Some(previous_upper)) {
            points.push(json!([step, value]));
        }
    }
    Ok(points)
}
