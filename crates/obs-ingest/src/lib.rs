#![forbid(unsafe_code)]

use obs_types::EventEnvelope;

pub fn validate_event(event: &EventEnvelope) -> Result<(), &'static str> {
    if event.envelope_version != 1 {
        return Err("unsupported envelope_version");
    }
    if event.run_id.is_empty() {
        return Err("run_id is required");
    }
    if event.event_type.is_empty() {
        return Err("event_type is required");
    }
    Ok(())
}
