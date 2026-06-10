#![forbid(unsafe_code)]

use obs_types::EventEnvelope;

#[derive(Default)]
pub struct InMemoryEventStore {
    events: Vec<EventEnvelope>,
}

impl InMemoryEventStore {
    pub fn write_batch(&mut self, events: impl IntoIterator<Item = EventEnvelope>) -> usize {
        let before = self.events.len();
        self.events.extend(events);
        self.events.len() - before
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_batch() {
        let mut store = InMemoryEventStore::default();
        assert_eq!(
            store.write_batch([EventEnvelope {
                run_id: "run".to_string(),
                seq: 1,
                source_service: "test".to_string(),
                event_type: "node-added".to_string(),
                payload_json: "{}".to_string(),
            }]),
            1
        );
        assert_eq!(store.len(), 1);
    }
}
