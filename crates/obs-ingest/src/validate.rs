//! Envelope validation (ARCHITECTURE §2 + API.md §1 + decision D3).
//!
//! Rejection is reserved for structural violations; payload SHAPE issues
//! never reject (tolerance, not translation — decision D3). Unknown event
//! types and unknown source services are stored and flagged.

use obs_types::catalog;
use obs_types::{EventEnvelope, EventRecord, Rejection, SourceService};

/// Payload size limit (API.md §1 producer rule 4).
pub const MAX_PAYLOAD_BYTES: usize = 65_536;

pub const REASON_ENVELOPE_VERSION: &str = "unsupported envelope_version";
pub const REASON_NOT_JSON_OBJECT: &str = "payload not a JSON object";
pub const REASON_TOO_LARGE: &str = "payload too large";
pub const REASON_MISSING_RUN_ID: &str = "missing run_id";

/// Validates one envelope into its storage-ready record. `ingested_at_ns`
/// is the caller's clock reading (decision D6).
pub fn validate(envelope: EventEnvelope, ingested_at_ns: i64) -> Result<EventRecord, Rejection> {
    let reject = |reason: &str| Rejection {
        seq: envelope.seq,
        reason: reason.to_owned(),
    };

    if envelope.envelope_version != 1 {
        return Err(reject(REASON_ENVELOPE_VERSION));
    }
    if envelope.payload_json.len() > MAX_PAYLOAD_BYTES {
        return Err(reject(REASON_TOO_LARGE));
    }

    // Exact received bytes are stored (never re-serialized: the events
    // table's byte determinism must not depend on serde map ordering).
    // Zero-length payloads are the proto3 default for "no payload" and
    // canonicalize to the empty object.
    let payload = if envelope.payload_json.is_empty() {
        "{}".to_owned()
    } else {
        match String::from_utf8(envelope.payload_json.clone()) {
            Ok(text) => text,
            Err(_) => return Err(reject(REASON_NOT_JSON_OBJECT)),
        }
    };
    match serde_json::from_str::<serde_json::Value>(&payload) {
        Ok(serde_json::Value::Object(_)) => {}
        _ => return Err(reject(REASON_NOT_JSON_OBJECT)),
    }

    let source = SourceService::try_from(envelope.source_service).ok();
    // Run-scoped sources must carry a run identity (contract addition
    // recorded with D3 in API.md §1's rejection causes). v1: all
    // orchestrator events are run-scoped.
    if envelope.run_id.is_empty() && source == Some(SourceService::ExplorationOrchestrator) {
        return Err(reject(REASON_MISSING_RUN_ID));
    }

    let source_text = match source {
        Some(known) if known != SourceService::Unspecified => {
            catalog::source_service_text(known).to_owned()
        }
        // Unknown/unspecified source: store the enum number as text and
        // flag the row (D3 — never rejected).
        _ => envelope.source_service.to_string(),
    };
    let unknown_source = matches!(source, None | Some(SourceService::Unspecified));
    let unknown_type = !catalog::is_known_event_type(&envelope.event_type);

    Ok(EventRecord {
        run_id: envelope.run_id,
        // Decision D9: ALWAYS the folded form — one rule, no conditionals.
        source_service_stored: format!("{source_text}/{}", envelope.producer_id),
        event_type: envelope.event_type,
        ts_logical: envelope.ts_logical as i64,
        ts_wall_ns: envelope.ts_wall_ns as i64,
        seq: envelope.seq as i64,
        payload_version: i64::from(envelope.payload_version),
        payload,
        unknown: unknown_source || unknown_type,
        ingested_at_ns,
        source_service: source.unwrap_or(SourceService::Unspecified),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(seq: u64) -> EventEnvelope {
        EventEnvelope {
            envelope_version: 1,
            ts_logical: seq,
            ts_wall_ns: 100 + seq,
            run_id: "run-a".into(),
            source_service: SourceService::ExplorationOrchestrator as i32,
            event_type: "node-added".into(),
            payload_version: 1,
            payload_json: br#"{"node_id":"7"}"#.to_vec(),
            seq,
            producer_id: "orchestratord-1".into(),
        }
    }

    #[test]
    fn accepts_and_folds_identity() {
        let record = validate(envelope(3), 42).unwrap();
        assert_eq!(
            record.source_service_stored,
            "exploration-orchestrator/orchestratord-1"
        );
        assert_eq!(record.seq, 3);
        assert!(!record.unknown);
        assert_eq!(record.ingested_at_ns, 42);
    }

    #[test]
    fn rejects_wrong_envelope_version() {
        let mut event = envelope(1);
        event.envelope_version = 2;
        let rejection = validate(event, 0).unwrap_err();
        assert_eq!(rejection.reason, REASON_ENVELOPE_VERSION);
        assert_eq!(rejection.seq, 1);
    }

    #[test]
    fn rejects_non_object_payload() {
        for bad in [&b"[1,2]"[..], b"not json", b"\"str\"", b"\xff\xfe"] {
            let mut event = envelope(2);
            event.payload_json = bad.to_vec();
            assert_eq!(
                validate(event, 0).unwrap_err().reason,
                REASON_NOT_JSON_OBJECT
            );
        }
    }

    #[test]
    fn rejects_oversized_payload() {
        let mut event = envelope(4);
        let mut payload = String::with_capacity(MAX_PAYLOAD_BYTES + 32);
        payload.push_str("{\"blob\":\"");
        payload.push_str(&"x".repeat(MAX_PAYLOAD_BYTES));
        payload.push_str("\"}");
        event.payload_json = payload.into_bytes();
        assert_eq!(validate(event, 0).unwrap_err().reason, REASON_TOO_LARGE);
    }

    #[test]
    fn rejects_missing_run_id_for_orchestrator() {
        let mut event = envelope(5);
        event.run_id = String::new();
        assert_eq!(
            validate(event, 0).unwrap_err().reason,
            REASON_MISSING_RUN_ID
        );
    }

    #[test]
    fn accepts_empty_run_id_for_non_run_scoped_source() {
        let mut event = envelope(6);
        event.run_id = String::new();
        event.source_service = SourceService::ControlPlane as i32;
        assert!(validate(event, 0).is_ok());
    }

    #[test]
    fn unknown_event_type_and_source_flag_not_reject() {
        let mut event = envelope(7);
        event.event_type = "mystery-event".into();
        let record = validate(event, 0).unwrap();
        assert!(record.unknown);

        let mut event = envelope(8);
        event.source_service = 42;
        let record = validate(event, 0).unwrap();
        assert!(record.unknown);
        assert_eq!(record.source_service_stored, "42/orchestratord-1");
    }

    #[test]
    fn empty_payload_canonicalizes_to_empty_object() {
        let mut event = envelope(9);
        event.payload_json = Vec::new();
        assert_eq!(validate(event, 0).unwrap().payload, "{}");
    }
}
