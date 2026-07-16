//! Flush policy: 500 events or 50 ms, whichever first (ARCHITECTURE §2).
//! Pure state — the async driving lives in the service.

use std::time::Duration;

use obs_types::{EventRecord, Rejection};

pub const MAX_BATCH: usize = 500;
pub const MAX_LINGER: Duration = Duration::from_millis(50);

/// Per-connection pending state.
#[derive(Default)]
pub struct Pending {
    pub records: Vec<EventRecord>,
    pub rejections: Vec<Rejection>,
}

impl Pending {
    /// True when the size threshold demands an immediate flush.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.records.len() >= MAX_BATCH
    }

    /// True when a flush would do anything (records to commit or
    /// rejections to report).
    #[must_use]
    pub fn has_work(&self) -> bool {
        !self.records.is_empty() || !self.rejections.is_empty()
    }

    pub fn take(&mut self) -> (Vec<EventRecord>, Vec<Rejection>) {
        (
            std::mem::take(&mut self.records),
            std::mem::take(&mut self.rejections),
        )
    }
}
