//! Migration v1: the full schema of ARCHITECTURE §3.1 verbatim, plus the
//! `rollup_state` high-water table §3.2 names. Every persisted format is
//! versioned via `schema_meta.version`.

/// The schema version this build reads and writes.
pub const SCHEMA_VERSION: i64 = 1;

pub const MIGRATION_V1: &str = r#"
CREATE TABLE schema_meta (version INTEGER NOT NULL);

-- Raw canonical event log (the ingest target; retention-pruned)
CREATE TABLE events (
  id              INTEGER PRIMARY KEY,            -- rowid
  run_id          TEXT    NOT NULL,
  source_service  TEXT    NOT NULL,               -- folded "<service>/<producer_id>" (decision D9)
  event_type      TEXT    NOT NULL,
  ts_logical      INTEGER NOT NULL,
  ts_wall_ns      INTEGER NOT NULL,               -- producer wall clock, advisory
  seq             INTEGER NOT NULL,               -- producer session sequence
  payload_version INTEGER NOT NULL,
  payload         TEXT    NOT NULL,               -- canonical JSON
  unknown         INTEGER NOT NULL DEFAULT 0,     -- 1 = event_type not in catalog
  ingested_at_ns  INTEGER NOT NULL,
  UNIQUE (run_id, source_service, seq)
);
CREATE INDEX events_run_type_ts ON events (run_id, event_type, ts_logical);
CREATE INDEX events_ingested   ON events (ingested_at_ns);

-- Run registry (projection)
CREATE TABLE runs (
  run_id           TEXT PRIMARY KEY,
  status           TEXT NOT NULL DEFAULT 'unknown', -- unknown|running|paused|stopped|goal_reached|failed
  first_seen_ns    INTEGER NOT NULL,
  last_event_ns    INTEGER NOT NULL,
  best_score       REAL,
  best_node_id     TEXT,
  goal_reached     INTEGER NOT NULL DEFAULT 0,
  expansions       INTEGER NOT NULL DEFAULT 0,    -- max ts_logical seen from orchestrator
  nodes_added      INTEGER NOT NULL DEFAULT 0,
  nodes_pruned     INTEGER NOT NULL DEFAULT 0,
  experiment_json  TEXT,
  feature_map_json TEXT
);

-- Tree topology projection (durable form of the tree; outlives raw events)
CREATE TABLE tree_nodes (
  run_id          TEXT    NOT NULL,
  node_id         TEXT    NOT NULL,
  parent_id       TEXT,                            -- NULL = root
  depth           INTEGER NOT NULL,
  progress_score  REAL    NOT NULL,
  novelty_score   REAL    NOT NULL,
  stage           INTEGER NOT NULL DEFAULT 0,
  cell_key        TEXT,
  snapshot_ref    TEXT    NOT NULL DEFAULT '',     -- D3 tolerance: v1 producer may omit
  guest_time_ns   INTEGER NOT NULL DEFAULT 0,      -- D3 tolerance: v1 producer may omit
  expansion_idx   INTEGER NOT NULL,                -- ts_logical at creation
  created_ns      INTEGER NOT NULL,
  pruned          INTEGER NOT NULL DEFAULT 0,
  prune_reason    TEXT,
  features_json   TEXT,
  PRIMARY KEY (run_id, node_id)
) WITHOUT ROWID;
CREATE INDEX tree_parent ON tree_nodes (run_id, parent_id);
CREATE INDEX tree_score  ON tree_nodes (run_id, progress_score DESC);

-- Best-score curve (small, kept forever): one row per improvement
CREATE TABLE score_points (
  run_id        TEXT NOT NULL,
  expansion_idx INTEGER NOT NULL,
  ts_wall_ns    INTEGER NOT NULL,
  score         REAL NOT NULL,
  node_id       TEXT NOT NULL,
  PRIMARY KEY (run_id, expansion_idx)
) WITHOUT ROWID;

CREATE TABLE checkpoints (
  run_id TEXT NOT NULL, checkpoint_id TEXT NOT NULL, expansion_idx INTEGER NOT NULL,
  ts_wall_ns INTEGER NOT NULL, frontier_size INTEGER, tree_nodes INTEGER,
  archive_cells INTEGER, seen_set_size INTEGER,
  PRIMARY KEY (run_id, checkpoint_id)
) WITHOUT ROWID;

-- Findings feed (projection; kept forever)
CREATE TABLE findings (
  id            INTEGER PRIMARY KEY,
  run_id        TEXT NOT NULL,
  kind          TEXT NOT NULL,        -- event_type of the source event
  severity      TEXT NOT NULL,        -- info|warning|critical (mapping in API.md §2.4)
  node_id       TEXT,
  ts_wall_ns    INTEGER NOT NULL,
  ts_logical    INTEGER NOT NULL,
  summary       TEXT NOT NULL,
  payload       TEXT NOT NULL
);
CREATE INDEX findings_run ON findings (run_id, id DESC);

-- Scraped + event-derived metric series
CREATE TABLE metric_series (
  series_id  INTEGER PRIMARY KEY,
  service    TEXT NOT NULL,           -- scrape target name or 'derived' or 'event'
  metric     TEXT NOT NULL,
  labels     TEXT NOT NULL DEFAULT '{}',  -- canonical sorted-key JSON
  UNIQUE (service, metric, labels)
);
CREATE TABLE metrics_raw (            -- retention: 24 h (sweeper is M8)
  series_id INTEGER NOT NULL,
  ts_ns     INTEGER NOT NULL,
  value     REAL NOT NULL,
  PRIMARY KEY (series_id, ts_ns)
) WITHOUT ROWID;

-- Rollups: identical shape at 3 grains. bucket_ns = ts truncated to width.
-- counters are stored as raw samples; rate() is computed at query time from
-- first/last within buckets (counter resets handled: negative delta -> use last).
CREATE TABLE rollup_5s  (series_id INTEGER NOT NULL, bucket_ns INTEGER NOT NULL,
  n INTEGER NOT NULL, sum REAL NOT NULL, min REAL NOT NULL, max REAL NOT NULL,
  first REAL NOT NULL, last REAL NOT NULL,
  PRIMARY KEY (series_id, bucket_ns)) WITHOUT ROWID;       -- retention 7 d
CREATE TABLE rollup_1m  (series_id INTEGER NOT NULL, bucket_ns INTEGER NOT NULL,
  n INTEGER NOT NULL, sum REAL NOT NULL, min REAL NOT NULL, max REAL NOT NULL,
  first REAL NOT NULL, last REAL NOT NULL,
  PRIMARY KEY (series_id, bucket_ns)) WITHOUT ROWID;       -- retention 30 d
CREATE TABLE rollup_10m (series_id INTEGER NOT NULL, bucket_ns INTEGER NOT NULL,
  n INTEGER NOT NULL, sum REAL NOT NULL, min REAL NOT NULL, max REAL NOT NULL,
  first REAL NOT NULL, last REAL NOT NULL,
  PRIMARY KEY (series_id, bucket_ns)) WITHOUT ROWID;       -- retention 1 y

-- Rollup high-water marks (ARCHITECTURE §3.2)
CREATE TABLE rollup_state (
  grain         TEXT PRIMARY KEY,
  high_water_ns INTEGER NOT NULL
);

-- Spatial coverage projection
CREATE TABLE coverage_cells (
  run_id   TEXT NOT NULL,
  map_id   TEXT NOT NULL,             -- region feature value as text, '' if none
  cx       INTEGER NOT NULL,
  cy       INTEGER NOT NULL,
  visits   INTEGER NOT NULL,
  best_score REAL NOT NULL,
  best_node_id TEXT NOT NULL,
  first_ns INTEGER NOT NULL,
  last_ns  INTEGER NOT NULL,
  PRIMARY KEY (run_id, map_id, cx, cy)
) WITHOUT ROWID;

-- Replay artifact cache (source of truth: control-plane registry)
CREATE TABLE replays (
  artifact_id  TEXT PRIMARY KEY,
  run_id       TEXT NOT NULL,
  node_id      TEXT NOT NULL,
  status       TEXT NOT NULL,         -- queued|reconstruct|reexec|verify|encode|done|failed
  pct          REAL NOT NULL DEFAULT 0,
  verified     INTEGER,
  video_uri    TEXT,
  timeline_uri TEXT,
  meta_json    TEXT,
  updated_ns   INTEGER NOT NULL
);
CREATE INDEX replays_run ON replays (run_id, updated_ns DESC);

-- Alert engine state
CREATE TABLE alert_instances (
  id           INTEGER PRIMARY KEY,
  rule_id      TEXT NOT NULL,
  run_id       TEXT,
  state        TEXT NOT NULL,         -- pending|firing|resolved
  opened_ns    INTEGER NOT NULL,
  fired_ns     INTEGER,
  resolved_ns  INTEGER,
  last_notified_ns INTEGER,
  context_json TEXT NOT NULL
);
CREATE INDEX alerts_open ON alert_instances (state) WHERE state != 'resolved';

INSERT INTO schema_meta (version) VALUES (1);
"#;
