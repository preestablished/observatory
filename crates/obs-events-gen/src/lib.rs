#![forbid(unsafe_code)]
//! Synthetic producer / load harness for the observatory ingest path
//! (IMPLEMENTATION-PLAN §M1). Seeded and deterministic: for a given seed +
//! flags the emitted stream is byte-identical — the CI determinism gate
//! rides on it. Also the demo data source for UI work.

pub mod publish;
pub mod sim;
pub mod verify;
pub mod vocab;

pub use publish::{publish, PublishConfig, PublishReport};
pub use sim::{Counts, Sim, SimConfig};
pub use verify::{verify, VerifyReport};
pub use vocab::Vocab;

/// Serializes one envelope as the recorded-stream JSONL line (sorted keys,
/// payload embedded as the exact UTF-8 text).
pub fn to_jsonl_line(envelope: &obs_types::EventEnvelope) -> String {
    serde_json::json!({
        "envelope_version": envelope.envelope_version,
        "ts_logical": envelope.ts_logical,
        "ts_wall_ns": envelope.ts_wall_ns,
        "run_id": envelope.run_id,
        "source_service": envelope.source_service,
        "event_type": envelope.event_type,
        "payload_version": envelope.payload_version,
        "payload_json": String::from_utf8_lossy(&envelope.payload_json),
        "seq": envelope.seq,
        "producer_id": envelope.producer_id,
    })
    .to_string()
}

/// Parses a recorded-stream JSONL line back into an envelope.
pub fn from_jsonl_line(line: &str) -> Result<obs_types::EventEnvelope, String> {
    let value: serde_json::Value = serde_json::from_str(line).map_err(|error| error.to_string())?;
    let str_field = |key: &str| -> String {
        value
            .get(key)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned()
    };
    let u64_field = |key: &str| value.get(key).and_then(|v| v.as_u64()).unwrap_or_default();
    Ok(obs_types::EventEnvelope {
        envelope_version: u64_field("envelope_version") as u32,
        ts_logical: u64_field("ts_logical"),
        ts_wall_ns: u64_field("ts_wall_ns"),
        run_id: str_field("run_id"),
        source_service: u64_field("source_service") as i32,
        event_type: str_field("event_type"),
        payload_version: u64_field("payload_version") as u32,
        payload_json: str_field("payload_json").into_bytes(),
        seq: u64_field("seq"),
        producer_id: str_field("producer_id"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_is_deterministic_for_a_seed() {
        let stream_a: Vec<String> = Sim::new(SimConfig::new(42, 2_000))
            .map(|e| to_jsonl_line(&e))
            .collect();
        let stream_b: Vec<String> = Sim::new(SimConfig::new(42, 2_000))
            .map(|e| to_jsonl_line(&e))
            .collect();
        assert_eq!(stream_a, stream_b);
        assert_eq!(stream_a.len(), 2_000);

        let stream_c: Vec<String> = Sim::new(SimConfig::new(43, 2_000))
            .map(|e| to_jsonl_line(&e))
            .collect();
        assert_ne!(stream_a, stream_c, "different seeds must differ");
    }

    #[test]
    fn seqs_are_monotone_from_one_and_ts_wall_strictly_monotone() {
        let mut last_seq = 0;
        let mut last_ts = 0;
        for envelope in Sim::new(SimConfig::new(7, 5_000)) {
            assert_eq!(envelope.seq, last_seq + 1);
            assert!(envelope.ts_wall_ns > last_ts);
            last_seq = envelope.seq;
            last_ts = envelope.ts_wall_ns;
        }
        assert_eq!(last_seq, 5_000);
    }

    #[test]
    fn both_vocab_profiles_are_deterministic_and_cover_the_vocabulary() {
        for vocab in [Vocab::Catalog, Vocab::OrchAsbuilt] {
            let mut config = SimConfig::new(11, 20_000);
            config.vocab = vocab;
            config.goal = true;
            let mut sim = Sim::new(config);
            for _ in sim.by_ref() {}
            let counts = sim.counts();
            for event_type in obs_types::catalog::V1_EVENT_TYPES {
                assert!(
                    counts.by_type.get(event_type).copied().unwrap_or(0) > 0,
                    "{vocab:?} never emitted {event_type}: {:?}",
                    counts.by_type
                );
            }
        }
    }

    #[test]
    fn inject_bad_mixes_malformed_and_unknown() {
        let mut config = SimConfig::new(5, 10_000);
        config.inject_bad = 100;
        let mut sim = Sim::new(config);
        for _ in sim.by_ref() {}
        assert!(sim.counts().malformed > 0);
        assert!(sim.counts().unknown_type > 0);
    }

    #[test]
    fn jsonl_round_trips() {
        for envelope in Sim::new(SimConfig::new(3, 100)) {
            let line = to_jsonl_line(&envelope);
            let parsed = from_jsonl_line(&line).unwrap();
            assert_eq!(parsed, envelope);
        }
    }
}
