//! Same-transaction projections (ARCHITECTURE §2 table, as amended by
//! reconciliation decisions D3/D4/D8). Applied by the writer ONLY for
//! events rows actually inserted — duplicate replays must be
//! byte-invisible, which is what makes the determinism gate hold.
//!
//! D3 tolerance: absent/mistyped payload fields never reject an event;
//! projections fall back to per-column defaults and mark the event
//! partial (`obs_projection_partial_total{event_type}`). There are
//! deliberately NO alias shims for the orchestrator's as-built field
//! names — drift stays visible; the fix belongs on the producer.

use rusqlite::Transaction;
use serde_json::Value;

use obs_types::catalog;
use obs_types::{EventRecord, SourceService};

use crate::metrics::IngestMetrics;

/// Grid discretization hint parsed from the canonical feature-map schema
/// (reference-workload API.md §1); armed per daemon from `[standalone]`.
#[derive(Clone, Debug, PartialEq)]
pub struct GridHint {
    pub x: String,
    pub y: String,
    pub room: Option<String>,
    pub cell_w: f64,
    pub cell_h: f64,
}

/// Standalone-mode caches loaded at startup (INTEGRATION §3): stand-ins
/// for the control-plane experiment-config fetch.
#[derive(Clone, Debug, Default)]
pub struct StandaloneData {
    pub experiment_json: Option<String>,
    pub feature_map_json: Option<String>,
    pub grid_hint: Option<GridHint>,
}

/// Everything the writer needs to project events.
#[derive(Clone, Default)]
pub struct ProjectionContext {
    pub standalone: StandaloneData,
    pub metrics: IngestMetrics,
}

/// Tracks whether any wanted payload field fell back to a default.
struct FieldReader<'a> {
    payload: &'a Value,
    partial: bool,
}

impl<'a> FieldReader<'a> {
    fn new(payload: &'a Value) -> Self {
        Self {
            payload,
            partial: false,
        }
    }

    fn str_field(&mut self, key: &str) -> Option<&'a str> {
        match self.payload.get(key).and_then(Value::as_str) {
            Some(value) => Some(value),
            None => {
                self.partial = true;
                None
            }
        }
    }

    /// Optional-by-contract string: absence is legal, wrong type is partial.
    fn opt_str_field(&mut self, key: &str) -> Option<&'a str> {
        match self.payload.get(key) {
            None | Some(Value::Null) => None,
            Some(value) => match value.as_str() {
                Some(text) => Some(text),
                None => {
                    self.partial = true;
                    None
                }
            },
        }
    }

    fn f64_field(&mut self, key: &str) -> Option<f64> {
        match self.payload.get(key).and_then(Value::as_f64) {
            Some(value) => Some(value),
            None => {
                self.partial = true;
                None
            }
        }
    }

    fn i64_field(&mut self, key: &str) -> Option<i64> {
        match self.payload.get(key).and_then(Value::as_i64) {
            Some(value) => Some(value),
            None => {
                self.partial = true;
                None
            }
        }
    }

    /// Present-only numeric field: absence is fine (D3 present-only rules).
    fn present_f64(&self, key: &str) -> Option<f64> {
        self.payload.get(key).and_then(Value::as_f64)
    }
}

/// Applies every projection for one inserted event. Returns rusqlite
/// errors only for genuine storage failures — payload shape issues are
/// absorbed per D3.
pub fn apply(
    tx: &Transaction<'_>,
    record: &EventRecord,
    ctx: &ProjectionContext,
) -> Result<(), rusqlite::Error> {
    if record.unknown {
        // Unknown event_type: stored + flagged, no projection (D3).
        return Ok(());
    }
    let payload: Value = serde_json::from_str(&record.payload).unwrap_or(Value::Null);
    let mut reader = FieldReader::new(&payload);

    if !record.run_id.is_empty() {
        project_runs(tx, record, ctx)?;
    }

    match record.event_type.as_str() {
        "node-added" => project_node_added(tx, record, &mut reader, ctx)?,
        "node-pruned" => project_node_pruned(tx, record, &mut reader)?,
        "best-score-improved" => project_best_score(tx, record, &mut reader)?,
        "stall-detected" | "goal-reached" | "assertion-violated" | "reachability-hit" => {
            project_finding(tx, record, &mut reader)?;
            if record.event_type == "goal-reached" {
                tx.execute(
                    "UPDATE runs SET status = 'goal_reached', goal_reached = 1 WHERE run_id = ?1",
                    [&record.run_id],
                )?;
            }
        }
        "escalation-changed" => {
            if let Some(level) = reader.i64_field("to_level") {
                ctx.metrics
                    .escalation_level
                    .with_label_values(&[record.run_id.as_str()])
                    .set(level);
            }
        }
        "batch-completed" => project_batch_completed(tx, record, &reader)?,
        "checkpoint" => project_checkpoint(tx, record, &mut reader)?,
        "replay-job-progress" | "replay-job-completed" | "replay-artifact-registered" => {
            project_replay(tx, record, &mut reader)?
        }
        _ => {}
    }

    if reader.partial {
        ctx.metrics
            .projection_partial_total
            .with_label_values(&[record.event_type.as_str()])
            .inc();
    }
    Ok(())
}

fn project_runs(
    tx: &Transaction<'_>,
    record: &EventRecord,
    ctx: &ProjectionContext,
) -> Result<(), rusqlite::Error> {
    let inserted = tx.execute(
        "INSERT OR IGNORE INTO runs (run_id, status, first_seen_ns, last_event_ns)
         VALUES (?1, 'running', ?2, ?2)",
        rusqlite::params![record.run_id, record.ts_wall_ns],
    )?;
    if inserted == 1 {
        // First event for an unknown run: standalone experiment-config
        // load (the control-plane GetExperiment fetch path is M3).
        let standalone = &ctx.standalone;
        if standalone.experiment_json.is_some() || standalone.feature_map_json.is_some() {
            tx.execute(
                "UPDATE runs SET experiment_json = ?2, feature_map_json = ?3 WHERE run_id = ?1",
                rusqlite::params![
                    record.run_id,
                    standalone.experiment_json,
                    standalone.feature_map_json
                ],
            )?;
        }
    } else {
        tx.execute(
            "UPDATE runs SET last_event_ns = max(last_event_ns, ?2) WHERE run_id = ?1",
            rusqlite::params![record.run_id, record.ts_wall_ns],
        )?;
    }
    if record.source_service == SourceService::ExplorationOrchestrator {
        // Source-scoped: keys off the decoded enum, never the stored
        // string (decision D9).
        tx.execute(
            "UPDATE runs SET expansions = max(expansions, ?2) WHERE run_id = ?1",
            rusqlite::params![record.run_id, record.ts_logical],
        )?;
    }
    match record.event_type.as_str() {
        "node-added" => {
            tx.execute(
                "UPDATE runs SET nodes_added = nodes_added + 1 WHERE run_id = ?1",
                [&record.run_id],
            )?;
        }
        "node-pruned" => {
            tx.execute(
                "UPDATE runs SET nodes_pruned = nodes_pruned + 1 WHERE run_id = ?1",
                [&record.run_id],
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn project_node_added(
    tx: &Transaction<'_>,
    record: &EventRecord,
    reader: &mut FieldReader<'_>,
    ctx: &ProjectionContext,
) -> Result<(), rusqlite::Error> {
    let Some(node_id) = reader.str_field("node_id").map(str::to_owned) else {
        return Ok(()); // No identity: nothing to insert; partial already set.
    };
    let parent_id = reader.opt_str_field("parent_id").map(str::to_owned);
    let depth = reader.i64_field("depth").unwrap_or(0);
    let progress_score = reader.f64_field("progress_score").unwrap_or(0.0);
    let novelty_score = reader.f64_field("novelty_score").unwrap_or(0.0);
    let stage = reader.i64_field("stage").unwrap_or(0);
    let cell_key = reader.opt_str_field("cell_key").map(str::to_owned);
    let snapshot_ref = reader.str_field("snapshot_ref").unwrap_or("").to_owned();
    let guest_time_ns = reader.i64_field("guest_time_ns").unwrap_or(0);
    let expansion_idx = reader
        .i64_field("expansion_idx")
        .unwrap_or(record.ts_logical);
    let features = reader.payload.get("features").filter(|v| v.is_object());
    let features_json = features.map(|value| value.to_string());

    tx.execute(
        "INSERT OR IGNORE INTO tree_nodes
           (run_id, node_id, parent_id, depth, progress_score, novelty_score, stage,
            cell_key, snapshot_ref, guest_time_ns, expansion_idx, created_ns, features_json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        rusqlite::params![
            record.run_id,
            node_id,
            parent_id,
            depth,
            progress_score,
            novelty_score,
            stage,
            cell_key,
            snapshot_ref,
            guest_time_ns,
            expansion_idx,
            record.ts_wall_ns,
            features_json,
        ],
    )?;

    tx.execute(
        "UPDATE runs SET best_score = ?2
         WHERE run_id = ?1 AND (best_score IS NULL OR best_score < ?2)",
        rusqlite::params![record.run_id, progress_score],
    )?;

    if let Some(hint) = &ctx.standalone.grid_hint {
        project_coverage(tx, record, features, &node_id, progress_score, hint, ctx)?;
    }
    Ok(())
}

/// Formats a feature value used as a `map_id` (room ids are integers in
/// practice; keep integer text for integral values).
fn feature_value_text(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 9e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

#[allow(clippy::too_many_arguments)]
fn project_coverage(
    tx: &Transaction<'_>,
    record: &EventRecord,
    features: Option<&Value>,
    node_id: &str,
    progress_score: f64,
    hint: &GridHint,
    ctx: &ProjectionContext,
) -> Result<(), rusqlite::Error> {
    let feature = |name: &str| -> Option<f64> { features?.get(name)?.as_f64() };
    let x = feature(&hint.x);
    let y = feature(&hint.y);
    let room = hint.room.as_deref().map(feature);
    let (x, y, map_id) = match (x, y, room) {
        (Some(x), Some(y), None) => (x, y, String::new()),
        (Some(x), Some(y), Some(Some(room_value))) => (x, y, feature_value_text(room_value)),
        _ => {
            // Sample doesn't cover the grid's features: skip + meter
            // (ARCHITECTURE §6 — a skip flood means the experiment's
            // decoded-feature subset doesn't cover the grid).
            ctx.metrics.coverage_skipped_total.inc();
            return Ok(());
        }
    };
    if hint.cell_w <= 0.0 || hint.cell_h <= 0.0 {
        ctx.metrics.coverage_skipped_total.inc();
        return Ok(());
    }
    let cx = (x / hint.cell_w).floor() as i64;
    let cy = (y / hint.cell_h).floor() as i64;
    tx.execute(
        "INSERT INTO coverage_cells
           (run_id, map_id, cx, cy, visits, best_score, best_node_id, first_ns, last_ns)
         VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, ?7, ?7)
         ON CONFLICT (run_id, map_id, cx, cy) DO UPDATE SET
           visits = visits + 1,
           first_ns = min(first_ns, excluded.first_ns),
           last_ns = max(last_ns, excluded.last_ns),
           best_node_id = CASE WHEN excluded.best_score > best_score
                               THEN excluded.best_node_id ELSE best_node_id END,
           best_score = max(best_score, excluded.best_score)",
        rusqlite::params![
            record.run_id,
            map_id,
            cx,
            cy,
            progress_score,
            node_id,
            record.ts_wall_ns
        ],
    )?;
    Ok(())
}

fn project_node_pruned(
    tx: &Transaction<'_>,
    record: &EventRecord,
    reader: &mut FieldReader<'_>,
) -> Result<(), rusqlite::Error> {
    let reason = reader.str_field("reason").map(str::to_owned);
    // D4: node_id is optional — absent means a never-committed candidate;
    // only the runs.nodes_pruned counter (already bumped) applies.
    if let Some(node_id) = reader.opt_str_field("node_id") {
        tx.execute(
            "UPDATE tree_nodes SET pruned = 1, prune_reason = ?3
             WHERE run_id = ?1 AND node_id = ?2",
            rusqlite::params![record.run_id, node_id, reason],
        )?;
    }
    Ok(())
}

fn project_best_score(
    tx: &Transaction<'_>,
    record: &EventRecord,
    reader: &mut FieldReader<'_>,
) -> Result<(), rusqlite::Error> {
    let Some(score) = reader.f64_field("score") else {
        return Ok(());
    };
    let node_id = reader.str_field("node_id").unwrap_or("").to_owned();
    let expansion_idx = reader
        .i64_field("expansion_idx")
        .unwrap_or(record.ts_logical);
    tx.execute(
        "UPDATE runs SET best_score = ?2, best_node_id = ?3
         WHERE run_id = ?1 AND (best_score IS NULL OR best_score <= ?2)",
        rusqlite::params![record.run_id, score, node_id],
    )?;
    tx.execute(
        "INSERT OR IGNORE INTO score_points (run_id, expansion_idx, ts_wall_ns, score, node_id)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![
            record.run_id,
            expansion_idx,
            record.ts_wall_ns,
            score,
            node_id
        ],
    )?;
    Ok(())
}

fn project_finding(
    tx: &Transaction<'_>,
    record: &EventRecord,
    reader: &mut FieldReader<'_>,
) -> Result<(), rusqlite::Error> {
    let severity = catalog::finding_severity(&record.event_type).unwrap_or("info");
    let (node_id, summary) = finding_summary(&record.event_type, reader);
    tx.execute(
        "INSERT INTO findings (run_id, kind, severity, node_id, ts_wall_ns, ts_logical, summary, payload)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            record.run_id,
            record.event_type,
            severity,
            node_id,
            record.ts_wall_ns,
            record.ts_logical,
            summary,
            record.payload,
        ],
    )?;
    Ok(())
}

fn finding_summary(event_type: &str, reader: &mut FieldReader<'_>) -> (Option<String>, String) {
    let node_id = reader.opt_str_field("node_id").map(str::to_owned);
    let summary = match event_type {
        "goal-reached" => match (reader.str_field("goal_id"), reader.present_f64("score")) {
            (Some(goal), Some(score)) => format!("goal {goal} reached (score {score})"),
            _ => "goal reached".to_owned(),
        },
        "stall-detected" => match reader.i64_field("window_expansions") {
            Some(window) => format!("search stalled ({window} expansions without improvement)"),
            None => "search stalled".to_owned(),
        },
        "assertion-violated" => match (
            reader.str_field("assertion_id"),
            reader.str_field("message"),
        ) {
            (Some(id), Some(message)) => format!("assertion {id} violated: {message}"),
            // D8: as-built producers relay raw SDK bytes without the typed
            // fields — store the finding, mark it undecoded.
            _ => "undecoded guest-sdk relay".to_owned(),
        },
        "reachability-hit" => match reader.str_field("reachability_id") {
            Some(id) => format!("reachability {id} hit"),
            _ => "undecoded guest-sdk relay".to_owned(),
        },
        other => other.to_owned(),
    };
    (node_id, summary)
}

/// `batch-completed` → synthetic `metrics_raw` series (service='event').
/// Sample ts is the ENVELOPE's `ts_wall_ns` (producer time,
/// generator-deterministic) — NOT the injectable Clock: under FixedClock
/// a Clock-minted ts would collide every sample on the (series_id, ts_ns)
/// primary key. Fields are present-only (D3); inserts are OR IGNORE so a
/// nonconforming producer with equal ts drops the duplicate sample rather
/// than aborting the transaction.
fn project_batch_completed(
    tx: &Transaction<'_>,
    record: &EventRecord,
    reader: &FieldReader<'_>,
) -> Result<(), rusqlite::Error> {
    const FIELDS: [&str; 5] = [
        "kept",
        "dups",
        "regressions",
        "failed_jobs",
        "batch_wall_ms",
    ];
    let labels = serde_json::json!({ "run_id": record.run_id }).to_string();
    for field in FIELDS {
        let Some(value) = reader.present_f64(field) else {
            continue;
        };
        tx.execute(
            "INSERT OR IGNORE INTO metric_series (service, metric, labels) VALUES ('event', ?1, ?2)",
            rusqlite::params![field, labels],
        )?;
        let series_id: i64 = tx.query_row(
            "SELECT series_id FROM metric_series WHERE service = 'event' AND metric = ?1 AND labels = ?2",
            rusqlite::params![field, labels],
            |row| row.get(0),
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO metrics_raw (series_id, ts_ns, value) VALUES (?1, ?2, ?3)",
            rusqlite::params![series_id, record.ts_wall_ns, value],
        )?;
    }
    Ok(())
}

fn project_checkpoint(
    tx: &Transaction<'_>,
    record: &EventRecord,
    reader: &mut FieldReader<'_>,
) -> Result<(), rusqlite::Error> {
    let checkpoint_id = reader
        .str_field("checkpoint_id")
        .map(str::to_owned)
        .unwrap_or_else(|| format!("ckpt-{}", record.ts_logical));
    let expansion_idx = reader
        .i64_field("expansion_idx")
        .unwrap_or(record.ts_logical);
    tx.execute(
        "INSERT OR IGNORE INTO checkpoints
           (run_id, checkpoint_id, expansion_idx, ts_wall_ns,
            frontier_size, tree_nodes, archive_cells, seen_set_size)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            record.run_id,
            checkpoint_id,
            expansion_idx,
            record.ts_wall_ns,
            reader.payload.get("frontier_size").and_then(Value::as_i64),
            reader.payload.get("tree_nodes").and_then(Value::as_i64),
            reader.payload.get("archive_cells").and_then(Value::as_i64),
            reader.payload.get("seen_set_size").and_then(Value::as_i64),
        ],
    )?;
    Ok(())
}

/// Catalog-defined Phase 7 producers (cheap forward compat): upsert the
/// `replays` cache.
fn project_replay(
    tx: &Transaction<'_>,
    record: &EventRecord,
    reader: &mut FieldReader<'_>,
) -> Result<(), rusqlite::Error> {
    let Some(artifact_id) = reader.str_field("artifact_id").map(str::to_owned) else {
        return Ok(());
    };
    let node_id = reader.str_field("node_id").unwrap_or("").to_owned();
    match record.event_type.as_str() {
        "replay-job-progress" => {
            let phase = reader.str_field("phase").unwrap_or("queued").to_owned();
            let pct = reader.present_f64("pct").unwrap_or(0.0);
            tx.execute(
                "INSERT INTO replays (artifact_id, run_id, node_id, status, pct, updated_ns)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT (artifact_id) DO UPDATE SET
                   status = excluded.status, pct = excluded.pct,
                   updated_ns = excluded.updated_ns
                 WHERE excluded.updated_ns >= updated_ns",
                rusqlite::params![
                    artifact_id,
                    record.run_id,
                    node_id,
                    phase,
                    pct,
                    record.ts_wall_ns
                ],
            )?;
        }
        "replay-job-completed" => {
            let status = reader.str_field("status").unwrap_or("done").to_owned();
            let verified = reader.payload.get("verified").and_then(Value::as_bool);
            let video_uri = reader.opt_str_field("video_uri").map(str::to_owned);
            let timeline_uri = reader.opt_str_field("timeline_uri").map(str::to_owned);
            tx.execute(
                "INSERT INTO replays
                   (artifact_id, run_id, node_id, status, pct, verified, video_uri, timeline_uri, updated_ns)
                 VALUES (?1, ?2, ?3, ?4, 100.0, ?5, ?6, ?7, ?8)
                 ON CONFLICT (artifact_id) DO UPDATE SET
                   status = excluded.status, pct = 100.0,
                   verified = excluded.verified,
                   video_uri = coalesce(excluded.video_uri, video_uri),
                   timeline_uri = coalesce(excluded.timeline_uri, timeline_uri),
                   updated_ns = excluded.updated_ns
                 WHERE excluded.updated_ns >= updated_ns",
                rusqlite::params![
                    artifact_id,
                    record.run_id,
                    node_id,
                    status,
                    verified,
                    video_uri,
                    timeline_uri,
                    record.ts_wall_ns
                ],
            )?;
        }
        "replay-artifact-registered" => {
            let kind = reader.str_field("kind").unwrap_or("").to_owned();
            let uri = reader.str_field("uri").map(str::to_owned);
            let (video_uri, timeline_uri) = match kind.as_str() {
                "timeline" => (None, uri),
                _ => (uri, None),
            };
            tx.execute(
                "INSERT INTO replays
                   (artifact_id, run_id, node_id, status, video_uri, timeline_uri, updated_ns)
                 VALUES (?1, ?2, ?3, 'queued', ?4, ?5, ?6)
                 ON CONFLICT (artifact_id) DO UPDATE SET
                   video_uri = coalesce(excluded.video_uri, video_uri),
                   timeline_uri = coalesce(excluded.timeline_uri, timeline_uri),
                   updated_ns = max(updated_ns, excluded.updated_ns)",
                rusqlite::params![
                    artifact_id,
                    record.run_id,
                    node_id,
                    video_uri,
                    timeline_uri,
                    record.ts_wall_ns
                ],
            )?;
        }
        _ => {}
    }
    Ok(())
}
