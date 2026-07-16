//! Ack tracking (decision D7): a monotone per-identity high-water mark of
//! seqs committed in stream order — NOT a contiguity scan. Gaps are legal
//! (producers drop-oldest); duplicates advance nothing but are covered.
//! Identities are the folded `(run_id, source_service_stored)` pair, the
//! same key as the events-table UNIQUE constraint.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use obs_store::ReadPool;
use obs_types::StoreError;

pub type Identity = (String, String);

#[derive(Clone, Default)]
pub struct AckTracker {
    inner: Arc<Mutex<HashMap<Identity, u64>>>,
}

impl AckTracker {
    /// Current high-water for an identity, seeding from the persisted
    /// events on first touch (restart/resume: the first ack after a
    /// server restart reports the durable high-water).
    pub async fn high_water(
        &self,
        pool: &ReadPool,
        identity: &Identity,
    ) -> Result<u64, StoreError> {
        if let Some(seq) = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(identity)
            .copied()
        {
            return Ok(seq);
        }
        let pool = pool.clone();
        let (run_id, source) = identity.clone();
        let seeded: u64 = tokio::task::spawn_blocking(move || {
            pool.with_read(|conn| {
                conn.query_row(
                    "SELECT coalesce(max(seq), 0) FROM events
                     WHERE run_id = ?1 AND source_service = ?2",
                    rusqlite::params![run_id, source],
                    |row| row.get::<_, i64>(0),
                )
            })
        })
        .await
        .map_err(|join| StoreError::Io(join.to_string()))?? as u64;

        let mut map = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = map.entry(identity.clone()).or_insert(seeded);
        *entry = (*entry).max(seeded);
        Ok(*entry)
    }

    /// Advances high-waters from a committed batch's per-identity maxima.
    /// Returns nothing; reads go through [`Self::current`].
    pub fn advance(&self, updates: &[(Identity, u64)]) {
        let mut map = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (identity, seq) in updates {
            let entry = map.entry(identity.clone()).or_insert(0);
            *entry = (*entry).max(*seq);
        }
    }

    /// Current in-memory high-water (0 if never touched).
    #[must_use]
    pub fn current(&self, identity: &Identity) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(identity)
            .copied()
            .unwrap_or(0)
    }
}
