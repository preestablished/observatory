//! The rollup ticker (ARCHITECTURE §3.2): every 5 s fold `metrics_raw`
//! rows newer than the 5s high-water into `rollup_5s`; on the coarser
//! cadence (hourly per spec — configurable so tests don't wait an hour)
//! fold closed 5s buckets into `rollup_1m` and closed 1m into
//! `rollup_10m`. CLOSED buckets only: rows in the still-open bucket wait
//! for the next tick (folding an open bucket plus the additive ON-CONFLICT
//! merge would double-count it). The fold rows and the high-water advance
//! commit in one writer transaction.

use std::sync::Arc;
use std::time::Duration;

use obs_store::{ReadPool, WriterHandle};
use obs_types::{Clock, Grain, RollupRow, StoreError, WriteBatch};

use crate::fold;

#[derive(Clone)]
pub struct RollupTicker {
    pool: ReadPool,
    writer: WriterHandle,
    clock: Arc<dyn Clock>,
}

impl RollupTicker {
    pub fn new(pool: ReadPool, writer: WriterHandle, clock: Arc<dyn Clock>) -> Self {
        Self {
            pool,
            writer,
            clock,
        }
    }

    fn high_water(&self, grain: Grain) -> Result<i64, StoreError> {
        self.pool.with_read(|conn| {
            conn.query_row(
                "SELECT high_water_ns FROM rollup_state WHERE grain = ?1",
                [grain.key()],
                |row| row.get(0),
            )
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(i64::MIN),
                other => Err(other),
            })
        })
    }

    /// One 5s fold pass: raw samples in `(high_water, closed_boundary]`
    /// → `rollup_5s`. Returns the folded row count.
    pub async fn tick_5s(&self) -> Result<usize, StoreError> {
        let now = self.clock.now_ns();
        let boundary = fold::bucket_of(now, Grain::S5.width_ns()); // open-bucket edge
        let high_water = self.high_water(Grain::S5)?;
        let samples: Vec<(i64, i64, f64)> = self.pool.with_read(|conn| {
            let mut stmt = conn.prepare_cached(
                "SELECT series_id, ts_ns, value FROM metrics_raw
                 WHERE ts_ns > ?1 AND ts_ns < ?2
                 ORDER BY series_id, ts_ns",
            )?;
            let rows = stmt.query_map(rusqlite::params![high_water, boundary], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
            rows.collect()
        })?;
        if samples.is_empty() {
            return Ok(0);
        }
        let rows = fold::fold_samples(&samples, Grain::S5.width_ns());
        let count = rows.len();
        self.writer
            .write(WriteBatch::RollupFold {
                grain: Grain::S5,
                rows,
                // Every sample below the closed boundary is folded; the
                // mark advances to the boundary edge minus nothing — the
                // max folded ts is what guards re-reads.
                high_water_ns: boundary - 1,
            })
            .await?;
        Ok(count)
    }

    /// Coarse promotion pass: closed 5s buckets → 1m, closed 1m → 10m.
    pub async fn tick_promote(&self) -> Result<usize, StoreError> {
        let mut folded = 0;
        folded += self.promote(Grain::S5, Grain::M1).await?;
        folded += self.promote(Grain::M1, Grain::M10).await?;
        Ok(folded)
    }

    async fn promote(&self, from: Grain, to: Grain) -> Result<usize, StoreError> {
        let now = self.clock.now_ns();
        // Only promote source buckets whose TARGET bucket is closed.
        let boundary = fold::bucket_of(now, to.width_ns());
        let high_water = self.high_water(to)?;
        let source: Vec<RollupRow> = self.pool.with_read(|conn| {
            let sql = format!(
                "SELECT series_id, bucket_ns, n, sum, min, max, first, last
                 FROM {table}
                 WHERE bucket_ns > ?1 AND bucket_ns < ?2
                 ORDER BY series_id, bucket_ns",
                table = from.table()
            );
            let mut stmt = conn.prepare_cached(&sql)?;
            let rows = stmt.query_map(rusqlite::params![high_water, boundary], |row| {
                Ok(RollupRow {
                    series_id: row.get(0)?,
                    bucket_ns: row.get(1)?,
                    n: row.get(2)?,
                    sum: row.get(3)?,
                    min: row.get(4)?,
                    max: row.get(5)?,
                    first: row.get(6)?,
                    last: row.get(7)?,
                })
            })?;
            rows.collect()
        })?;
        if source.is_empty() {
            return Ok(0);
        }
        let max_source_bucket = source.last().map(|row| row.bucket_ns).unwrap_or(high_water);
        let rows = fold::fold_rows(&source, to.width_ns());
        let count = rows.len();
        self.writer
            .write(WriteBatch::RollupFold {
                grain: to,
                rows,
                high_water_ns: max_source_bucket,
            })
            .await?;
        Ok(count)
    }

    /// Production loop: 5s folds every `fine_every`, promotions every
    /// `coarse_every` (spec default hourly; injectable for tests).
    pub async fn run(self, fine_every: Duration, coarse_every: Duration) {
        let mut fine = tokio::time::interval(fine_every);
        fine.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut coarse = tokio::time::interval(coarse_every);
        coarse.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = fine.tick() => {
                    if let Err(error) = self.tick_5s().await {
                        tracing::warn!(%error, "rollup 5s tick failed");
                    }
                }
                _ = coarse.tick() => {
                    if let Err(error) = self.tick_promote().await {
                        tracing::warn!(%error, "rollup promotion tick failed");
                    }
                }
            }
        }
    }
}
