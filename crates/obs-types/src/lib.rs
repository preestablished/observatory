#![forbid(unsafe_code)]

pub use determinism_proto::observatory::v1::EventEnvelope;

pub fn event_key(event: &EventEnvelope) -> (&str, &str, u64) {
    (&event.run_id, &event.source_service, event.seq)
}
