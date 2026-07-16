//! Derived search-health metrics (ARCHITECTURE §3.3): computed every 10 s
//! per running run, written as `service='derived'` series (label
//! `run_id`) through the normal metric path. Timestamps are
//! observatory-minted via the injectable Clock (D6).
//!
//! D3 rule throughout: when an input is absent (no samples in the
//! window, missing scrape series), the metric is ABSENT for that tick —
//! never zero. Zero is a lie the M4 alert engine would act on.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use obs_store::{ReadPool, WriterHandle};
use obs_types::{Clock, MetricSample, SeriesKey, StoreError, WriteBatch};

/// Rate/delta window for event- and scrape-derived rates.
const WINDOW_NS: i64 = 30_000_000_000;
/// Least-squares window for the disk-burn slope (30 min of 5s rollups).
const BURN_WINDOW_NS: i64 = 1_800_000_000_000;

pub struct DerivedTicker {
    pool: ReadPool,
    writer: WriterHandle,
    clock: Arc<dyn Clock>,
    /// Per-run (ts_ns, expansions) history for the 30 s expansion rate.
    expansions: Mutex<HashMap<String, VecDeque<(i64, i64)>>>,
}

fn labels_for(run_id: &str) -> String {
    serde_json::json!({ "run_id": run_id }).to_string()
}

impl DerivedTicker {
    pub fn new(pool: ReadPool, writer: WriterHandle, clock: Arc<dyn Clock>) -> Self {
        Self {
            pool,
            writer,
            clock,
            expansions: Mutex::new(HashMap::new()),
        }
    }

    /// One derive pass. Returns the number of samples written.
    pub async fn tick(&self) -> Result<usize, StoreError> {
        let now = self.clock.now_ns();
        let runs: Vec<(String, i64, Option<f64>)> = self.pool.with_read(|conn| {
            let mut stmt = conn.prepare_cached(
                "SELECT run_id, expansions, best_score FROM runs WHERE status = 'running'",
            )?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
            rows.collect()
        })?;

        let mut samples: Vec<MetricSample> = Vec::new();
        let mut push = |metric: &str, run_id: &str, value: f64| {
            samples.push(MetricSample {
                key: SeriesKey::new("derived", metric, labels_for(run_id)),
                ts_ns: now,
                value,
            });
        };

        for (run_id, expansions, best_score) in &runs {
            if let Some(best) = best_score {
                push("search_best_score", run_id, *best);
            }
            if let Some(rate) = self.expansion_rate(run_id, now, *expansions) {
                push("search_expansions_per_sec", run_id, rate);
            }
            if let Some(rate) = self.novel_state_rate(run_id, now)? {
                push("search_novel_state_rate", run_id, rate);
            }
            if let Some(frontier) = self.frontier_size(run_id, now)? {
                push("search_frontier_size", run_id, frontier);
            }
            if let Some(rate) = self.dedup_hit_rate(now)? {
                push("search_dedup_hit_rate", run_id, rate);
            }
            if let Some(utilization) = self.slot_utilization(now)? {
                push("worker_slot_utilization", run_id, utilization);
            }
            if let Some(ratio) = self.snapshot_dedup_ratio(now)? {
                push("snapshot_dedup_ratio", run_id, ratio);
            }
            if let Some(burn) = self.disk_burn(now)? {
                push("disk_burn_bytes_per_hour", run_id, burn);
            }
            if let Some((p50, p99)) = self.gpu_latency_ms(now)? {
                push("gpu_batch_latency_ms_p50", run_id, p50);
                push("gpu_batch_latency_ms_p99", run_id, p99);
            }
        }

        let count = samples.len();
        if count > 0 {
            self.writer
                .write(WriteBatch::MetricSamples(samples))
                .await?;
        }
        Ok(count)
    }

    /// Rate of `runs.expansions` over the last 30 s (in-memory history —
    /// max orchestrator ts_logical is a monotone gauge, not a counter
    /// series).
    fn expansion_rate(&self, run_id: &str, now: i64, expansions: i64) -> Option<f64> {
        let mut map = self
            .expansions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let history = map.entry(run_id.to_owned()).or_default();
        history.push_back((now, expansions));
        while history.front().is_some_and(|(ts, _)| now - *ts > WINDOW_NS) {
            // Keep one point older than the window as the rate base.
            if history.len() > 2 {
                history.pop_front();
            } else {
                break;
            }
        }
        let (old_ts, old_expansions) = *history.front()?;
        if history.len() < 2 || now == old_ts {
            return None;
        }
        Some((expansions - old_expansions) as f64 / ((now - old_ts) as f64 / 1e9))
    }

    /// Σkept / (Σkept + Σdups) from the event-derived series over the
    /// window. Absent kept samples → unavailable (skip the tick).
    fn novel_state_rate(&self, run_id: &str, now: i64) -> Result<Option<f64>, StoreError> {
        let labels = labels_for(run_id);
        let window_sum = |metric: &str| -> Result<Option<(f64, i64)>, StoreError> {
            self.pool.with_read(|conn| {
                conn.query_row(
                    "SELECT coalesce(sum(value), 0), count(*) FROM metrics_raw
                     WHERE series_id = (SELECT series_id FROM metric_series
                                        WHERE service='event' AND metric=?1 AND labels=?2)
                       AND ts_ns > ?3",
                    rusqlite::params![metric, labels, now - WINDOW_NS],
                    |row| Ok(Some((row.get(0)?, row.get(1)?))),
                )
            })
        };
        let Some((kept, kept_count)) = window_sum("kept")? else {
            return Ok(None);
        };
        if kept_count == 0 {
            return Ok(None);
        }
        let dups = window_sum("dups")?.map(|(sum, _)| sum).unwrap_or(0.0);
        let denominator = kept + dups;
        if denominator <= 0.0 {
            return Ok(None);
        }
        Ok(Some(kept / denominator))
    }

    /// Latest checkpoint frontier size, refined by a fresher scraped
    /// `eo_frontier_size` sample when present.
    fn frontier_size(&self, run_id: &str, now: i64) -> Result<Option<f64>, StoreError> {
        let checkpoint: Option<i64> = self.pool.with_read(|conn| {
            conn.query_row(
                "SELECT frontier_size FROM checkpoints
                 WHERE run_id = ?1 AND frontier_size IS NOT NULL
                 ORDER BY ts_wall_ns DESC LIMIT 1",
                [run_id],
                |row| row.get(0),
            )
            .map(Some)
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(other),
            })
        })?;
        let scraped = self.latest_scraped("eo_frontier_size", now)?;
        Ok(scraped.or(checkpoint.map(|v| v as f64)))
    }

    /// Latest in-window sample of a scraped metric (any target), by name.
    fn latest_scraped(&self, metric: &str, now: i64) -> Result<Option<f64>, StoreError> {
        self.pool.with_read(|conn| {
            conn.query_row(
                "SELECT r.value FROM metrics_raw r
                 JOIN metric_series s ON s.series_id = r.series_id
                 WHERE s.metric = ?1 AND s.service NOT IN ('event', 'derived')
                   AND r.ts_ns > ?2
                 ORDER BY r.ts_ns DESC LIMIT 1",
                rusqlite::params![metric, now - WINDOW_NS],
                |row| row.get(0),
            )
            .map(Some)
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(other),
            })
        })
    }

    /// Windowed counter delta (first/last inside the window, reset-safe).
    fn window_delta(&self, metric: &str, now: i64) -> Result<Option<f64>, StoreError> {
        let points: Vec<f64> = self.pool.with_read(|conn| {
            let mut stmt = conn.prepare_cached(
                "SELECT r.value FROM metrics_raw r
                 JOIN metric_series s ON s.series_id = r.series_id
                 WHERE s.metric = ?1 AND s.service NOT IN ('event', 'derived')
                   AND r.ts_ns > ?2
                 ORDER BY r.ts_ns",
            )?;
            let rows =
                stmt.query_map(rusqlite::params![metric, now - WINDOW_NS], |row| row.get(0))?;
            rows.collect()
        })?;
        if points.len() < 2 {
            return Ok(None);
        }
        let (first, last) = (points[0], points[points.len() - 1]);
        Ok(Some(if last >= first {
            last - first
        } else {
            last.max(0.0)
        }))
    }

    fn dedup_hit_rate(&self, now: i64) -> Result<Option<f64>, StoreError> {
        let hits = self.window_delta("sc_dedup_hits_total", now)?;
        let requests = self.window_delta("sc_score_requests_total", now)?;
        match (hits, requests) {
            (Some(hits), Some(requests)) if requests > 0.0 => Ok(Some(hits / requests)),
            _ => Ok(None),
        }
    }

    fn slot_utilization(&self, now: i64) -> Result<Option<f64>, StoreError> {
        let busy = self.latest_scraped("dh_slots_busy", now)?;
        let total = self.latest_scraped("dh_slots_total", now)?;
        match (busy, total) {
            (Some(busy), Some(total)) if total > 0.0 => Ok(Some(busy / total)),
            _ => Ok(None),
        }
    }

    fn snapshot_dedup_ratio(&self, now: i64) -> Result<Option<f64>, StoreError> {
        let physical = self.latest_scraped("ss_bytes_physical_total", now)?;
        let logical = self.latest_scraped("ss_bytes_logical_total", now)?;
        match (physical, logical) {
            (Some(physical), Some(logical)) if logical > 0.0 => Ok(Some(physical / logical)),
            _ => Ok(None),
        }
    }

    /// Least-squares slope of `ss_disk_used_bytes` over the last 30 min of
    /// 5s rollups, scaled to bytes/hour.
    fn disk_burn(&self, now: i64) -> Result<Option<f64>, StoreError> {
        let points: Vec<(i64, f64)> = self.pool.with_read(|conn| {
            let mut stmt = conn.prepare_cached(
                "SELECT b.bucket_ns, b.last FROM rollup_5s b
                 JOIN metric_series s ON s.series_id = b.series_id
                 WHERE s.metric = 'ss_disk_used_bytes'
                   AND s.service NOT IN ('event', 'derived')
                   AND b.bucket_ns > ?1
                 ORDER BY b.bucket_ns",
            )?;
            let rows =
                stmt.query_map([now - BURN_WINDOW_NS], |row| Ok((row.get(0)?, row.get(1)?)))?;
            rows.collect()
        })?;
        if points.len() < 2 {
            return Ok(None);
        }
        // Least squares over (hours-since-first, bytes).
        let t0 = points[0].0;
        let n = points.len() as f64;
        let (mut sx, mut sy, mut sxx, mut sxy) = (0.0, 0.0, 0.0, 0.0);
        for (ts, value) in &points {
            let x = (ts - t0) as f64 / 3.6e12; // hours
            sx += x;
            sy += value;
            sxx += x * x;
            sxy += x * value;
        }
        let denominator = n * sxx - sx * sx;
        if denominator.abs() < f64::EPSILON {
            return Ok(None);
        }
        Ok(Some((n * sxy - sx * sy) / denominator))
    }

    /// p50/p99 in ms from the `sc_gpu_batch_latency_seconds` histogram:
    /// windowed per-`le` count deltas, linear interpolation within the
    /// matched bucket (the Prometheus quantile estimate).
    fn gpu_latency_ms(&self, now: i64) -> Result<Option<(f64, f64)>, StoreError> {
        let buckets: Vec<(String, f64)> = self.pool.with_read(|conn| {
            let mut stmt = conn.prepare_cached(
                "SELECT s.labels, max(r.value) - min(r.value)
                 FROM metrics_raw r
                 JOIN metric_series s ON s.series_id = r.series_id
                 WHERE s.metric = 'sc_gpu_batch_latency_seconds_bucket'
                   AND s.service NOT IN ('event', 'derived')
                   AND r.ts_ns > ?1
                 GROUP BY s.series_id
                 HAVING count(*) >= 2",
            )?;
            let rows = stmt.query_map([now - WINDOW_NS], |row| Ok((row.get(0)?, row.get(1)?)))?;
            rows.collect()
        })?;
        if buckets.is_empty() {
            return Ok(None);
        }
        let mut parsed: Vec<(f64, f64)> = Vec::with_capacity(buckets.len());
        for (labels, delta) in &buckets {
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
            parsed.push((upper, delta.max(0.0)));
        }
        parsed.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let total = parsed.last().map(|(_, count)| *count).unwrap_or(0.0);
        if total <= 0.0 {
            return Ok(None);
        }
        let quantile = |q: f64| -> f64 {
            let target = q * total;
            let mut previous_upper = 0.0;
            let mut previous_count = 0.0;
            for &(upper, count) in &parsed {
                if count >= target {
                    if upper.is_infinite() {
                        return previous_upper * 1_000.0;
                    }
                    let span = count - previous_count;
                    let fraction = if span > 0.0 {
                        (target - previous_count) / span
                    } else {
                        1.0
                    };
                    return (previous_upper + (upper - previous_upper) * fraction) * 1_000.0;
                }
                previous_upper = upper;
                previous_count = count;
            }
            previous_upper * 1_000.0
        };
        Ok(Some((quantile(0.5), quantile(0.99))))
    }

    /// Production loop (10 s per spec; injectable for tests).
    pub async fn run(self, every: Duration) {
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if let Err(error) = self.tick().await {
                tracing::warn!(%error, "derived-metrics tick failed");
            }
        }
    }
}
