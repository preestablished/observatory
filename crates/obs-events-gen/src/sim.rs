//! Seeded, deterministic search simulator. Grows a tree with configurable
//! branch/prune ratios, walks an (x, y, room) grid so `features` maps
//! exercise the coverage projection, and emits the full v1 vocabulary.
//!
//! Determinism rules: everything derives from the ChaCha PRNG seeded by
//! `--seed`; `ts_wall_ns` is sim epoch + a deterministic per-event
//! increment, strictly monotone per producer — NEVER the real wall clock.

use rand_chacha::rand_core::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

use obs_types::{EventEnvelope, SourceService};

use crate::vocab::{self, Vocab};

/// Sim epoch: 2026-07-01T00:00:00Z in ns (arbitrary fixed constant).
const SIM_EPOCH_NS: u64 = 1_782_950_400_000_000_000;
/// Expansions per simulated batch.
const BATCH_SIZE: u64 = 32;
/// Simulated ns between events (drives the ~30 s checkpoint cadence).
const EVENT_TICK_NS: u64 = 40_000_000; // 40 ms of sim time per event
const CHECKPOINT_EVERY_NS: u64 = 30_000_000_000; // ~30 s sim time

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    /// Total envelopes to emit (including injected bad ones).
    pub events: u64,
    pub run_id: String,
    pub producer_id: String,
    pub vocab: Vocab,
    /// Mix in N bad envelopes (unknown event types + malformed payloads),
    /// spread deterministically through the stream.
    pub inject_bad: u64,
    /// Emit a goal-reached ending.
    pub goal: bool,
    /// First seq to emit (resume support: seq N+1 continues a session).
    pub start_seq: u64,
}

impl SimConfig {
    pub fn new(seed: u64, events: u64) -> Self {
        Self {
            seed,
            events,
            run_id: "sim-run".to_owned(),
            producer_id: "obs-events-gen-1".to_owned(),
            vocab: Vocab::Catalog,
            inject_bad: 0,
            goal: false,
            start_seq: 1,
        }
    }
}

struct NodeState {
    id: u64,
    x: f64,
    y: f64,
    room: u64,
    score: f64,
    depth: u64,
}

/// Deterministic event stream generator.
pub struct Sim {
    config: SimConfig,
    rng: ChaCha8Rng,
    seq: u64,
    emitted: u64,
    ts_wall_ns: u64,
    expansion_idx: u64,
    next_node_id: u64,
    frontier: Vec<NodeState>,
    best_score: f64,
    best_node: u64,
    batch_seq: u64,
    batch_kept: u64,
    batch_dups: u64,
    batch_regressions: u64,
    batch_failed: u64,
    last_checkpoint_ns: u64,
    stall_emitted: bool,
    goal_emitted: bool,
    pending_improvement: bool,
    prev_best: f64,
    /// Emit one bad envelope every `bad_every` events (0 = never).
    bad_every: u64,
    bad_emitted: u64,
    counts: Counts,
}

/// Per-event-type emission counts, used by `verify` as the expected table.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub by_type: std::collections::BTreeMap<String, u64>,
    pub valid: u64,
    pub malformed: u64,
    pub unknown_type: u64,
}

impl Counts {
    fn bump(&mut self, event_type: &str) {
        *self.by_type.entry(event_type.to_owned()).or_insert(0) += 1;
    }
}

impl Sim {
    pub fn new(config: SimConfig) -> Self {
        let rng = ChaCha8Rng::seed_from_u64(config.seed);
        let bad_every = config
            .events
            .checked_div(config.inject_bad)
            .map_or(0, |every| every.max(2));
        let root = NodeState {
            id: 1,
            x: 64.0,
            y: 64.0,
            room: 0,
            score: 0.01,
            depth: 0,
        };
        Self {
            seq: config.start_seq,
            emitted: 0,
            ts_wall_ns: SIM_EPOCH_NS + (config.start_seq - 1) * EVENT_TICK_NS,
            expansion_idx: 0,
            next_node_id: 2,
            frontier: vec![root],
            best_score: 0.01,
            best_node: 1,
            batch_seq: 0,
            batch_kept: 0,
            batch_dups: 0,
            batch_regressions: 0,
            batch_failed: 0,
            last_checkpoint_ns: SIM_EPOCH_NS,
            stall_emitted: false,
            goal_emitted: false,
            pending_improvement: false,
            prev_best: 0.01,
            bad_every,
            bad_emitted: 0,
            counts: Counts::default(),
            config,
            rng,
        }
    }

    /// Emission counts so far (call after draining for the full table).
    #[must_use]
    pub fn counts(&self) -> &Counts {
        &self.counts
    }

    fn envelope(&mut self, event_type: &str, payload_json: Vec<u8>) -> EventEnvelope {
        let envelope = EventEnvelope {
            envelope_version: 1,
            ts_logical: self.expansion_idx,
            ts_wall_ns: self.ts_wall_ns,
            run_id: self.config.run_id.clone(),
            source_service: SourceService::ExplorationOrchestrator as i32,
            event_type: event_type.to_owned(),
            payload_version: 1,
            payload_json,
            seq: self.seq,
            producer_id: self.config.producer_id.clone(),
        };
        self.seq += 1;
        self.emitted += 1;
        self.ts_wall_ns += EVENT_TICK_NS;
        envelope
    }

    fn rand_f64(&mut self) -> f64 {
        (self.rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn rand_below(&mut self, bound: u64) -> u64 {
        self.rng.next_u64() % bound.max(1)
    }

    fn pick_parent(&mut self) -> usize {
        // Bias toward the tail (recent, higher-scoring) of the frontier.
        let len = self.frontier.len();
        let r = self.rand_f64();
        ((r * r * len as f64) as usize).min(len - 1)
    }

    fn next_bad(&mut self) -> EventEnvelope {
        // Alternate malformed payloads and unknown event types.
        self.bad_emitted += 1;
        if self.bad_emitted.is_multiple_of(2) {
            self.counts.malformed += 1;
            self.counts.bump("__malformed__");
            self.envelope("node-added", b"this is not json {".to_vec())
        } else {
            self.counts.unknown_type += 1;
            self.counts.valid += 1;
            self.counts.bump("synthetic-mystery-event");
            self.envelope(
                "synthetic-mystery-event",
                br#"{"novel":true,"payload_version_hint":2}"#.to_vec(),
            )
        }
    }
}

impl Iterator for Sim {
    type Item = EventEnvelope;

    fn next(&mut self) -> Option<EventEnvelope> {
        if self.emitted >= self.config.events {
            return None;
        }
        // A staged best-score improvement goes out first (one-in-one-out).
        if let Some(envelope) = self.take_pending_improvement() {
            return Some(envelope);
        }
        // Reserve the final slot for goal-reached when requested.
        let remaining = self.config.events - self.emitted;
        if self.config.goal && remaining == 1 && !self.goal_emitted {
            self.goal_emitted = true;
            self.counts.valid += 1;
            self.counts.bump("goal-reached");
            let payload = vocab::goal_reached(
                self.config.vocab,
                self.best_node,
                self.best_score,
                self.expansion_idx,
            );
            return Some(self.envelope("goal-reached", payload));
        }
        if self.bad_every > 0 && self.emitted % self.bad_every == self.bad_every - 1 {
            return Some(self.next_bad());
        }

        // Checkpoint cadence (~30 s of sim time).
        if self.ts_wall_ns - self.last_checkpoint_ns >= CHECKPOINT_EVERY_NS {
            self.last_checkpoint_ns = self.ts_wall_ns;
            self.counts.valid += 1;
            self.counts.bump("checkpoint");
            let payload = vocab::checkpoint(
                self.config.vocab,
                self.batch_seq,
                self.expansion_idx,
                self.frontier.len() as u64,
                self.next_node_id - 1,
            );
            return Some(self.envelope("checkpoint", payload));
        }

        // One stall/escalation phase mid-run.
        if !self.stall_emitted && self.emitted * 2 >= self.config.events {
            self.stall_emitted = true;
            self.counts.valid += 1;
            self.counts.bump("stall-detected");
            let payload = vocab::stall_detected(
                self.config.vocab,
                200,
                self.best_score,
                1,
                self.expansion_idx,
            );
            return Some(self.envelope("stall-detected", payload));
        }
        if self.stall_emitted && !self.counts.by_type.contains_key("escalation-changed") {
            self.counts.valid += 1;
            self.counts.bump("escalation-changed");
            let payload = vocab::escalation_changed(self.config.vocab, 0, 1, self.expansion_idx);
            return Some(self.envelope("escalation-changed", payload));
        }

        // Batch boundary.
        if self.expansion_idx > 0
            && self.expansion_idx.is_multiple_of(BATCH_SIZE)
            && self.batch_kept > 0
        {
            self.batch_seq += 1;
            let payload = vocab::batch_completed(
                self.config.vocab,
                self.batch_seq,
                self.batch_kept,
                self.batch_dups,
                self.batch_regressions,
                self.batch_failed,
                12.5 + self.batch_seq as f64,
            );
            self.batch_kept = 0;
            self.batch_dups = 0;
            self.batch_regressions = 0;
            self.batch_failed = 0;
            self.counts.valid += 1;
            self.counts.bump("batch-completed");
            return Some(self.envelope("batch-completed", payload));
        }

        // Default: one expansion — mostly node-added, some prunes,
        // occasional improvements and sprinkled guest relays.
        self.expansion_idx += 1;
        let roll = self.rand_below(100);
        if roll < 12 {
            // Pre-commit discard: id-less node-pruned (decision D4 path).
            self.batch_dups += 1;
            let parent_index = self.pick_parent();
            let parent = self.frontier[parent_index].id;
            self.counts.valid += 1;
            self.counts.bump("node-pruned");
            let payload = vocab::node_pruned(self.config.vocab, None, parent, "duplicate");
            return Some(self.envelope("node-pruned", payload));
        }
        if roll < 16 && self.frontier.len() > 4 {
            // Frontier eviction of a committed node.
            let index = self.rand_below(self.frontier.len() as u64) as usize;
            let node = self.frontier.remove(index);
            self.counts.valid += 1;
            self.counts.bump("node-pruned");
            let payload =
                vocab::node_pruned(self.config.vocab, Some(node.id), node.id, "frontier-evict");
            return Some(self.envelope("node-pruned", payload));
        }
        if roll < 18 {
            self.counts.valid += 1;
            self.counts.bump("reachability-hit");
            let payload = vocab::reachability_hit(
                self.config.vocab,
                self.best_node,
                self.expansion_idx,
                &format!("beacon-{}", self.rand_below(4)),
            );
            return Some(self.envelope("reachability-hit", payload));
        }
        if roll < 19 {
            self.counts.valid += 1;
            self.counts.bump("assertion-violated");
            let payload = vocab::assertion_violated(
                self.config.vocab,
                self.best_node,
                &format!("assert-{}", self.rand_below(3)),
            );
            return Some(self.envelope("assertion-violated", payload));
        }

        // node-added: walk the grid from the parent.
        let parent_index = self.pick_parent();
        let parent = &self.frontier[parent_index];
        let (px, py, proom, pscore, pdepth, pid) = (
            parent.x,
            parent.y,
            parent.room,
            parent.score,
            parent.depth,
            parent.id,
        );
        let dx = (self.rand_f64() - 0.5) * 24.0;
        let dy = (self.rand_f64() - 0.5) * 24.0;
        let mut x = (px + dx).clamp(0.0, 512.0);
        let mut y = (py + dy).clamp(0.0, 512.0);
        let mut room = proom;
        if self.rand_below(64) == 0 {
            room = (room + 1) % 8;
            x = 64.0;
            y = 64.0;
        }
        let score = (pscore + self.rand_f64() * 0.01).min(1.0);
        let node = NodeState {
            id: self.next_node_id,
            x,
            y,
            room,
            score,
            depth: pdepth + 1,
        };
        self.next_node_id += 1;
        self.batch_kept += 1;

        let improved = score > self.best_score;
        let payload = vocab::node_added(
            self.config.vocab,
            &vocab::NodeAdded {
                node_id: node.id,
                parent_id: Some(pid),
                snapshot_ref: format!("snap-{:08x}", node.id),
                depth: node.depth,
                progress_score: score,
                novelty_score: self.rand_f64() * 0.5,
                cell_key: format!("{}:{}:{}", room, (x / 32.0) as i64, (y / 32.0) as i64),
                stage: 0,
                guest_time_ns: self.expansion_idx * 16_666_666,
                input_delta_bytes: 8 + self.rand_below(64),
                expansion_idx: self.expansion_idx,
                x,
                y,
                room,
            },
        );
        let envelope = self.envelope("node-added", payload);
        self.counts.valid += 1;
        self.counts.bump("node-added");

        self.frontier.push(node);
        if self.frontier.len() > 256 {
            self.frontier.remove(0);
        }
        if improved {
            // The improvement event goes out on the next pull (staged so
            // the iterator stays one-in-one-out).
            self.prev_best = self.best_score;
            self.best_score = score;
            self.best_node = self.next_node_id - 1;
            self.pending_improvement = true;
        }
        Some(envelope)
    }
}

impl Sim {
    /// Emits the pending best-score-improved event when staged; used from
    /// `next` indirection to keep the iterator one-in-one-out.
    fn take_pending_improvement(&mut self) -> Option<EventEnvelope> {
        if !self.pending_improvement {
            return None;
        }
        self.pending_improvement = false;
        self.counts.valid += 1;
        self.counts.bump("best-score-improved");
        let payload = vocab::best_score_improved(
            self.config.vocab,
            self.best_node,
            self.best_score,
            self.prev_best,
            self.expansion_idx,
        );
        Some(self.envelope("best-score-improved", payload))
    }
}
