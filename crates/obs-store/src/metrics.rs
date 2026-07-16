//! Ingest-path metrics, defined here because the writer increments the
//! projection counters in-transaction context; observatoryd registers them
//! into the shared registry.

use prometheus::{
    Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry,
};

#[derive(Clone)]
pub struct IngestMetrics {
    pub events_ingested_total: IntCounter,
    pub events_rejected_total: IntCounterVec,
    pub events_unknown_type_total: IntCounter,
    pub projection_partial_total: IntCounterVec,
    pub coverage_skipped_total: IntCounter,
    pub batch_flush_seconds: Histogram,
    /// Per-run escalation level (the durable trail is the raw event log).
    pub escalation_level: IntGaugeVec,
}

impl IngestMetrics {
    pub fn new() -> Self {
        Self {
            events_ingested_total: IntCounter::new(
                "obs_events_ingested_total",
                "Events durably committed",
            )
            .expect("counter"),
            events_rejected_total: IntCounterVec::new(
                Opts::new("obs_events_rejected_total", "Envelopes rejected per-seq"),
                &["reason"],
            )
            .expect("counter vec"),
            events_unknown_type_total: IntCounter::new(
                "obs_events_unknown_type_total",
                "Events stored with unknown=1 (event_type not in catalog)",
            )
            .expect("counter"),
            projection_partial_total: IntCounterVec::new(
                Opts::new(
                    "obs_projection_partial_total",
                    "Projections that fell back to defaults for absent payload fields (D3)",
                ),
                &["event_type"],
            )
            .expect("counter vec"),
            coverage_skipped_total: IntCounter::new(
                "obs_coverage_skipped_total",
                "node-added samples skipped by the coverage projection (missing grid features)",
            )
            .expect("counter"),
            batch_flush_seconds: Histogram::with_opts(HistogramOpts::new(
                "obs_ingest_batch_flush_seconds",
                "Batcher flush-to-durable-commit latency",
            ))
            .expect("histogram"),
            escalation_level: IntGaugeVec::new(
                Opts::new(
                    "obs_run_escalation_level",
                    "Current escalation level per run",
                ),
                &["run_id"],
            )
            .expect("gauge vec"),
        }
    }

    pub fn register(&self, registry: &Registry) {
        registry
            .register(Box::new(self.events_ingested_total.clone()))
            .expect("register");
        registry
            .register(Box::new(self.events_rejected_total.clone()))
            .expect("register");
        registry
            .register(Box::new(self.events_unknown_type_total.clone()))
            .expect("register");
        registry
            .register(Box::new(self.projection_partial_total.clone()))
            .expect("register");
        registry
            .register(Box::new(self.coverage_skipped_total.clone()))
            .expect("register");
        registry
            .register(Box::new(self.batch_flush_seconds.clone()))
            .expect("register");
        registry
            .register(Box::new(self.escalation_level.clone()))
            .expect("register");
    }
}

impl Default for IngestMetrics {
    fn default() -> Self {
        Self::new()
    }
}
