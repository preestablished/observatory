//! The v1 event-type catalog (API.md §2) and the findings severity mapping
//! (API.md §2.4). The vocabulary is data, not behavior: ingest never
//! rejects unknown types (decision D3), it only flags them.

/// The ten v1 event types — exactly the exploration-orchestrator vocabulary
/// (API.md §2.1, the only Phase 5 producer).
pub const V1_EVENT_TYPES: [&str; 10] = [
    "node-added",
    "node-pruned",
    "best-score-improved",
    "stall-detected",
    "escalation-changed",
    "goal-reached",
    "batch-completed",
    "checkpoint",
    "assertion-violated",
    "reachability-hit",
];

/// Phase 7 replay-renderer event types (API.md §2.2) that the v1 catalog
/// already knows for cheap forward compatibility (the `replays` projection).
pub const REPLAY_EVENT_TYPES: [&str; 5] = [
    "replay-job-progress",
    "replay-job-completed",
    "divergence-detected",
    "divergence-bisect-result",
    "replay-artifact-registered",
];

/// True when `event_type` is in the known catalog (v1 + the forward-compat
/// replay set). Anything else is stored with `unknown = 1`.
#[must_use]
pub fn is_known_event_type(event_type: &str) -> bool {
    V1_EVENT_TYPES.contains(&event_type) || REPLAY_EVENT_TYPES.contains(&event_type)
}

/// Findings severity mapping (API.md §2.4). Returns `None` for event types
/// that do not project a finding.
#[must_use]
pub fn finding_severity(event_type: &str) -> Option<&'static str> {
    match event_type {
        "goal-reached" | "reachability-hit" => Some("info"),
        "stall-detected" | "assertion-violated" => Some("warning"),
        "divergence-detected" | "divergence-bisect-result" => Some("critical"),
        _ => None,
    }
}

/// Canonical lowercase-hyphen text for a decoded source-service enum
/// (stored-identity prefix; decision D9).
#[must_use]
pub fn source_service_text(source: crate::SourceService) -> &'static str {
    match source {
        crate::SourceService::Unspecified => "unspecified",
        crate::SourceService::ExplorationOrchestrator => "exploration-orchestrator",
        crate::SourceService::DeterminismHypervisor => "determinism-hypervisor",
        crate::SourceService::StateScorer => "state-scorer",
        crate::SourceService::ReplayRenderer => "replay-renderer",
        crate::SourceService::ControlPlane => "control-plane",
        crate::SourceService::GuestSdk => "guest-sdk",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_vocabulary_is_known() {
        for event_type in V1_EVENT_TYPES {
            assert!(is_known_event_type(event_type), "{event_type}");
        }
        assert!(!is_known_event_type("mystery-event"));
    }

    #[test]
    fn severity_mapping_matches_api_2_4() {
        assert_eq!(finding_severity("goal-reached"), Some("info"));
        assert_eq!(finding_severity("reachability-hit"), Some("info"));
        assert_eq!(finding_severity("stall-detected"), Some("warning"));
        assert_eq!(finding_severity("assertion-violated"), Some("warning"));
        assert_eq!(finding_severity("divergence-detected"), Some("critical"));
        assert_eq!(finding_severity("node-added"), None);
    }
}
