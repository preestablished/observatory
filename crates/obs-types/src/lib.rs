#![forbid(unsafe_code)]
//! Shared observatory types: newtypes, the storage-ready event record, the
//! writer batch vocabulary, the injectable clock (reconciliation decision
//! D6), and error types. No I/O — everything here is pure data.
//!
//! Naming deviation from ARCHITECTURE §1: that doc calls this crate
//! `obs-core`; the Phase 0 skeleton named it `obs-types` and the name is
//! kept (recorded in the repo README).

use std::fmt;

pub use determinism_proto::observatory::v1;
pub use determinism_proto::observatory::v1::{
    event_ingest_client, event_ingest_server, EventBatch, EventEnvelope, PublishAck, Rejection,
    SourceService,
};

pub mod catalog;

/// Idempotency key of an envelope: `(run_id, source_service, seq)` with the
/// enum still in wire form (folding into the stored identity happens at
/// validation, decision D9).
pub fn event_key(event: &EventEnvelope) -> (&str, i32, u64) {
    (&event.run_id, event.source_service, event.seq)
}

macro_rules! string_newtype {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }
    };
}

string_newtype!(
    /// Run identity. In standalone mode this equals the experiment id.
    RunId
);
string_newtype!(
    /// Node identity within a run (decimal string in v1 payloads).
    NodeId
);

/// Identity of a metric series: `(service, metric, canonical labels JSON)`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SeriesKey {
    /// Scrape target name, `"derived"`, or `"event"`.
    pub service: String,
    pub metric: String,
    /// Canonical sorted-key JSON object (`{}` when unlabeled).
    pub labels_json: String,
}

impl SeriesKey {
    pub fn new(
        service: impl Into<String>,
        metric: impl Into<String>,
        labels_json: impl Into<String>,
    ) -> Self {
        Self {
            service: service.into(),
            metric: metric.into(),
            labels_json: labels_json.into(),
        }
    }
}

/// The validated, storage-ready form of an envelope. Field semantics match
/// the `events` table columns (ARCHITECTURE §3.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventRecord {
    pub run_id: String,
    /// ALWAYS the folded form `"<service>/<producer_id>"` per decision D9 —
    /// there is no plain-service branch.
    pub source_service_stored: String,
    pub event_type: String,
    pub ts_logical: i64,
    pub ts_wall_ns: i64,
    pub seq: i64,
    pub payload_version: i64,
    /// Canonical JSON — the exact received bytes when they were already a
    /// canonical UTF-8 JSON object (never re-serialized; byte determinism
    /// of the `events` table must not depend on serde map ordering).
    pub payload: String,
    /// True when `event_type` is not in the v1 catalog (stored, flagged,
    /// never rejected — forward compatibility).
    pub unknown: bool,
    /// Observatory-minted, from the injectable [`Clock`] (decision D6).
    pub ingested_at_ns: i64,
    /// Decoded source enum for source-scoped projection logic (decision
    /// D9: projections key off this, never off the stored string).
    pub source_service: SourceService,
}

/// One raw metric sample destined for `metrics_raw`.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricSample {
    pub key: SeriesKey,
    pub ts_ns: i64,
    pub value: f64,
}

/// Rollup grains (ARCHITECTURE §3.2). Width in ns; one table per grain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Grain {
    S5,
    M1,
    M10,
}

impl Grain {
    #[must_use]
    pub fn width_ns(self) -> i64 {
        match self {
            Grain::S5 => 5_000_000_000,
            Grain::M1 => 60_000_000_000,
            Grain::M10 => 600_000_000_000,
        }
    }

    /// Table name (static, never interpolated from input).
    #[must_use]
    pub fn table(self) -> &'static str {
        match self {
            Grain::S5 => "rollup_5s",
            Grain::M1 => "rollup_1m",
            Grain::M10 => "rollup_10m",
        }
    }

    /// `rollup_state.grain` key.
    #[must_use]
    pub fn key(self) -> &'static str {
        match self {
            Grain::S5 => "5s",
            Grain::M1 => "1m",
            Grain::M10 => "10m",
        }
    }
}

/// One folded rollup row (identical shape at every grain).
#[derive(Clone, Debug, PartialEq)]
pub struct RollupRow {
    pub series_id: i64,
    pub bucket_ns: i64,
    pub n: i64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub first: f64,
    pub last: f64,
}

/// Typed batches the single writer task consumes.
#[derive(Clone, Debug)]
pub enum WriteBatch {
    /// Raw events; the writer applies `INSERT OR IGNORE` plus
    /// same-transaction projections for inserted rows only.
    Events(Vec<EventRecord>),
    /// Scraped or derived samples for `metric_series`/`metrics_raw`.
    MetricSamples(Vec<MetricSample>),
    /// A rollup fold: rows merged into the grain table AND the grain's
    /// high-water advanced in the SAME transaction (crash-idempotency
    /// rides on that atomicity).
    RollupFold {
        grain: Grain,
        rows: Vec<RollupRow>,
        high_water_ns: i64,
    },
}

impl WriteBatch {
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            WriteBatch::Events(records) => records.len(),
            WriteBatch::MetricSamples(samples) => samples.len(),
            WriteBatch::RollupFold { rows, .. } => rows.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One committed event with its `events.rowid` (the SSE `Last-Event-ID`
/// replay key for M3 consumers).
#[derive(Clone, Debug)]
pub struct CommittedEvent {
    pub rowid: i64,
    pub record: EventRecord,
}

/// Post-commit broadcast payload: the rows actually inserted by one write
/// transaction plus the run ids they touched. Consumers in this phase:
/// metrics and tests; the SSE hub / obs-tree / obs-alert attach in M3/M4.
#[derive(Clone, Debug, Default)]
pub struct CommittedBatch {
    pub events: Vec<CommittedEvent>,
    pub run_ids: Vec<String>,
}

/// Injectable time source (decision D6): every timestamp observatory itself
/// mints flows through this so the determinism gate can pin time.
pub trait Clock: Send + Sync + 'static {
    /// Nanoseconds since the Unix epoch.
    fn now_ns(&self) -> i64;
}

/// Production clock backed by the system wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    }
}

/// Test clock pinned to a fixed instant.
#[derive(Clone, Copy, Debug)]
pub struct FixedClock(pub i64);

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(String),
    #[error("on-disk schema version {found} is newer than supported version {supported}; refusing to start")]
    SchemaTooNew { found: i64, supported: i64 },
    #[error("store writer is shut down")]
    WriterClosed,
    #[error("io: {0}")]
    Io(String),
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("transport: {0}")]
    Transport(String),
}
