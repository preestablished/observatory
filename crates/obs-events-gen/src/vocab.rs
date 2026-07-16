//! Payload builders for the two vocabulary profiles (decision D5):
//!
//! - `Catalog` — strict API.md §2.1 field shapes: the M1 acceptance
//!   profile.
//! - `OrchAsbuilt` — the byte-shapes of the orchestrator's
//!   `orch-server/src/events.rs` builders today (audited 2026-07-16 at
//!   their `7f97fca`): proves the D3 tolerance path against reality.
//!
//! Payload bytes come from `serde_json::json!` whose map keys serialize
//! sorted — deterministic for a given input.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Vocab {
    Catalog,
    OrchAsbuilt,
}

impl std::str::FromStr for Vocab {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "catalog" => Ok(Self::Catalog),
            "orch-asbuilt" => Ok(Self::OrchAsbuilt),
            other => Err(format!("unknown vocab {other:?} (catalog|orch-asbuilt)")),
        }
    }
}

pub struct NodeAdded {
    pub node_id: u64,
    pub parent_id: Option<u64>,
    pub snapshot_ref: String,
    pub depth: u64,
    pub progress_score: f64,
    pub novelty_score: f64,
    pub cell_key: String,
    pub stage: u64,
    pub guest_time_ns: u64,
    pub input_delta_bytes: u64,
    pub expansion_idx: u64,
    pub x: f64,
    pub y: f64,
    pub room: u64,
}

fn id(node: u64) -> String {
    node.to_string()
}

pub fn node_added(vocab: Vocab, node: &NodeAdded) -> Vec<u8> {
    let features = serde_json::json!({
        "player_x": node.x,
        "player_y": node.y,
        "room_id": node.room as f64,
    });
    let value = match vocab {
        Vocab::Catalog => serde_json::json!({
            "node_id": id(node.node_id),
            "parent_id": node.parent_id.map(id),
            "snapshot_ref": node.snapshot_ref,
            "depth": node.depth,
            "progress_score": node.progress_score,
            "novelty_score": node.novelty_score,
            "cell_key": node.cell_key,
            "stage": node.stage,
            "guest_time_ns": node.guest_time_ns,
            "input_delta_bytes": node.input_delta_bytes,
            "expansion_idx": node.expansion_idx,
            "features": features,
        }),
        // events.rs::node_added_payload: renames, u64 cell_key, 5 fields
        // absent.
        Vocab::OrchAsbuilt => serde_json::json!({
            "node_id": id(node.node_id),
            "parent_node_id": id(node.parent_id.unwrap_or(0)),
            "score": node.progress_score,
            "novelty": node.novelty_score,
            "cell_key": node.node_id * 31 % 4096,
            "stage": node.stage,
            "features": features,
        }),
    };
    value.to_string().into_bytes()
}

pub fn node_pruned(vocab: Vocab, node: Option<u64>, parent: u64, reason: &str) -> Vec<u8> {
    let value = match vocab {
        Vocab::Catalog => match node {
            Some(node) => serde_json::json!({
                "node_id": id(node), "parent_id": id(parent), "reason": reason
            }),
            None => serde_json::json!({ "parent_id": id(parent), "reason": reason }),
        },
        // events.rs::node_pruned_payload: parent_node_id key.
        Vocab::OrchAsbuilt => match node {
            Some(node) => serde_json::json!({
                "node_id": id(node), "parent_node_id": id(parent), "reason": reason
            }),
            None => serde_json::json!({ "parent_node_id": id(parent), "reason": reason }),
        },
    };
    value.to_string().into_bytes()
}

pub fn best_score_improved(
    vocab: Vocab,
    node: u64,
    score: f64,
    prev_best: f64,
    expansion_idx: u64,
) -> Vec<u8> {
    let value = match vocab {
        Vocab::Catalog => serde_json::json!({
            "node_id": id(node), "score": score, "prev_best": prev_best,
            "expansion_idx": expansion_idx,
        }),
        Vocab::OrchAsbuilt => serde_json::json!({
            "node_id": id(node), "best_score": score, "previous_best_score": prev_best,
        }),
    };
    value.to_string().into_bytes()
}

pub fn stall_detected(
    vocab: Vocab,
    window: u64,
    best_score: f64,
    escalation_level: u64,
    since: u64,
) -> Vec<u8> {
    let value = match vocab {
        Vocab::Catalog => serde_json::json!({
            "window_expansions": window, "best_score": best_score,
            "escalation_level": escalation_level, "since_expansion_idx": since,
        }),
        Vocab::OrchAsbuilt => serde_json::json!({
            "expansions_since_improvement": window, "window": window,
        }),
    };
    value.to_string().into_bytes()
}

pub fn escalation_changed(vocab: Vocab, from: u64, to: u64, expansion_idx: u64) -> Vec<u8> {
    let value = match vocab {
        Vocab::Catalog => serde_json::json!({
            "from_level": from, "to_level": to, "expansion_idx": expansion_idx,
        }),
        Vocab::OrchAsbuilt => serde_json::json!({
            "level": to, "previous_level": from,
        }),
    };
    value.to_string().into_bytes()
}

pub fn goal_reached(vocab: Vocab, node: u64, score: f64, expansion_idx: u64) -> Vec<u8> {
    let value = match vocab {
        Vocab::Catalog => serde_json::json!({
            "node_id": id(node), "goal_id": "goal-1", "score": score,
            "expansion_idx": expansion_idx, "path_len": expansion_idx / 8 + 1,
        }),
        Vocab::OrchAsbuilt => serde_json::json!({
            "node_id": id(node), "score": score,
        }),
    };
    value.to_string().into_bytes()
}

#[allow(clippy::too_many_arguments)]
pub fn batch_completed(
    vocab: Vocab,
    batch_seq: u64,
    kept: u64,
    dups: u64,
    regressions: u64,
    failed_jobs: u64,
    batch_wall_ms: f64,
) -> Vec<u8> {
    let value = match vocab {
        Vocab::Catalog => serde_json::json!({
            "batch_seq": batch_seq, "kept": kept, "dups": dups,
            "regressions": regressions, "failed_jobs": failed_jobs,
            "batch_wall_ms": batch_wall_ms,
        }),
        Vocab::OrchAsbuilt => serde_json::json!({
            "batch_seq": batch_seq, "parent_node_id": "1",
            "committed": kept, "discarded": dups + regressions,
        }),
    };
    value.to_string().into_bytes()
}

pub fn checkpoint(
    vocab: Vocab,
    batch_seq: u64,
    expansion_idx: u64,
    frontier_size: u64,
    tree_nodes: u64,
) -> Vec<u8> {
    let value = match vocab {
        Vocab::Catalog => serde_json::json!({
            "checkpoint_id": format!("ckpt-{batch_seq}"),
            "expansion_idx": expansion_idx, "frontier_size": frontier_size,
            "tree_nodes": tree_nodes, "archive_cells": tree_nodes / 3 + 1,
            "seen_set_size": tree_nodes * 2,
        }),
        Vocab::OrchAsbuilt => serde_json::json!({
            "batch_seq": batch_seq, "expansions": expansion_idx,
            "archive_seq": batch_seq,
        }),
    };
    value.to_string().into_bytes()
}

pub fn assertion_violated(vocab: Vocab, node: u64, assertion_id: &str) -> Vec<u8> {
    let value = match vocab {
        Vocab::Catalog => serde_json::json!({
            "node_id": id(node), "assertion_id": assertion_id,
            "message": format!("assertion {assertion_id} tripped"),
        }),
        // events.rs::sdk_event_payload: undecoded relay (bytes rendered as
        // an array by the orchestrator's future JSON adapter).
        Vocab::OrchAsbuilt => serde_json::json!({
            "node_id": id(node), "stream": 1, "payload": [7, 7, 7],
        }),
    };
    value.to_string().into_bytes()
}

pub fn reachability_hit(vocab: Vocab, node: u64, expansion_idx: u64, beacon: &str) -> Vec<u8> {
    let value = match vocab {
        Vocab::Catalog => serde_json::json!({
            "node_id": id(node), "reachability_id": beacon,
            "first_hit": true, "expansion_idx": expansion_idx,
        }),
        Vocab::OrchAsbuilt => serde_json::json!({
            "node_id": id(node), "stream": 2, "payload": [1, 2, 3],
        }),
    };
    value.to_string().into_bytes()
}
