//! The single-writer task: one dedicated thread owns the write connection;
//! input is a bounded mpsc of [`WriteBatch`] messages; one SQLite
//! transaction per flush; a oneshot completion per batch is sent only
//! after the transaction commits (the durable ack the ingest server relies
//! on). Projections run in the same transaction, only for events rows
//! actually inserted; a post-commit broadcast fans the inserted rows out.

use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::{broadcast, mpsc, oneshot};

use obs_types::{
    CommittedBatch, CommittedEvent, EventRecord, MetricSample, StoreError, WriteBatch,
};

use crate::projections::{self, ProjectionContext};

/// Bounded writer input (ARCHITECTURE §1: mpsc, 4096).
pub const WRITER_CHANNEL_CAPACITY: usize = 4096;

/// How many queued batches one flush (= one transaction) may coalesce.
const MAX_BATCHES_PER_FLUSH: usize = 64;

/// Post-commit broadcast capacity (slow consumers observe `Lagged`).
const BROADCAST_CAPACITY: usize = 1024;

/// Event type that aborts the surrounding transaction — a test hook for
/// the bulk-atomicity acceptance (behind the `test-hooks` feature only).
#[cfg(feature = "test-hooks")]
pub const TEST_ABORT_EVENT_TYPE: &str = "__obs_test_abort__";

/// Per-batch application result, delivered post-commit.
#[derive(Clone, Debug, Default)]
pub struct Applied {
    /// Rows actually inserted (duplicates ignored by `INSERT OR IGNORE`).
    pub inserted: usize,
    /// Highest seq per folded identity `(run_id, source_service_stored)`
    /// seen in this batch (committed OR deduplicated — both count as
    /// covered for acks, decision D7).
    pub max_seq_by_identity: Vec<((String, String), u64)>,
}

struct WriteRequest {
    batch: WriteBatch,
    done: oneshot::Sender<Result<Applied, StoreError>>,
}

/// Cloneable handle every producer task uses to reach the writer.
#[derive(Clone)]
pub struct WriterHandle {
    sender: mpsc::Sender<WriteRequest>,
    committed: broadcast::Sender<Arc<CommittedBatch>>,
}

impl WriterHandle {
    /// Sends a batch and awaits its post-commit completion.
    pub async fn write(&self, batch: WriteBatch) -> Result<Applied, StoreError> {
        let (done, completion) = oneshot::channel();
        self.sender
            .send(WriteRequest { batch, done })
            .await
            .map_err(|_| StoreError::WriterClosed)?;
        completion.await.map_err(|_| StoreError::WriterClosed)?
    }

    /// Current queue depth (exported as `obs_ingest_channel_depth`).
    #[must_use]
    pub fn depth(&self) -> usize {
        self.sender.max_capacity() - self.sender.capacity()
    }

    /// Subscribes to the post-commit fan-out.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<CommittedBatch>> {
        self.committed.subscribe()
    }
}

/// Spawns the writer thread owning `conn`. The thread drains the channel,
/// applies each flush in one transaction, and exits (after a final WAL
/// checkpoint) when every [`WriterHandle`] is dropped.
pub fn spawn_writer(
    conn: Connection,
    ctx: ProjectionContext,
) -> (WriterHandle, std::thread::JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(WRITER_CHANNEL_CAPACITY);
    let (committed, _) = broadcast::channel(BROADCAST_CAPACITY);
    let broadcast_out = committed.clone();
    let join = std::thread::Builder::new()
        .name("obs-store-writer".to_owned())
        .spawn(move || writer_loop(conn, receiver, ctx, broadcast_out))
        .expect("spawn writer thread");
    (WriterHandle { sender, committed }, join)
}

fn writer_loop(
    mut conn: Connection,
    mut receiver: mpsc::Receiver<WriteRequest>,
    ctx: ProjectionContext,
    committed: broadcast::Sender<Arc<CommittedBatch>>,
) {
    while let Some(first) = receiver.blocking_recv() {
        let mut requests = vec![first];
        while requests.len() < MAX_BATCHES_PER_FLUSH {
            match receiver.try_recv() {
                Ok(request) => requests.push(request),
                Err(_) => break,
            }
        }
        flush(&mut conn, requests, &ctx, &committed);
    }
    // Graceful shutdown: channel closed and drained — final WAL checkpoint.
    if let Err(error) = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
        tracing::warn!(error = %error, "final WAL checkpoint failed");
    }
}

/// One transaction per flush; completions and the broadcast are sent only
/// after commit.
fn flush(
    conn: &mut Connection,
    requests: Vec<WriteRequest>,
    ctx: &ProjectionContext,
    committed: &broadcast::Sender<Arc<CommittedBatch>>,
) {
    let mut results = Vec::with_capacity(requests.len());
    let mut fanout = CommittedBatch::default();
    let outcome = (|| -> Result<(), rusqlite::Error> {
        let tx = conn.transaction()?;
        for request in &requests {
            let applied = apply_batch(&tx, &request.batch, ctx, &mut fanout)?;
            results.push(applied);
        }
        tx.commit()
    })();

    match outcome {
        Ok(()) => {
            let inserted_events = fanout.events.len();
            if inserted_events > 0 {
                ctx.metrics
                    .events_ingested_total
                    .inc_by(inserted_events as u64);
                let _ = committed.send(Arc::new(fanout));
            }
            for (request, applied) in requests.into_iter().zip(results) {
                let _ = request.done.send(Ok(applied));
            }
        }
        Err(error) => {
            tracing::error!(error = %error, "write flush failed; transaction rolled back");
            for request in requests {
                let _ = request
                    .done
                    .send(Err(StoreError::Sqlite(error.to_string())));
            }
        }
    }
}

fn apply_batch(
    tx: &rusqlite::Transaction<'_>,
    batch: &WriteBatch,
    ctx: &ProjectionContext,
    fanout: &mut CommittedBatch,
) -> Result<Applied, rusqlite::Error> {
    match batch {
        WriteBatch::Events(records) => apply_events(tx, records, ctx, fanout),
        WriteBatch::MetricSamples(samples) => apply_metric_samples(tx, samples),
        WriteBatch::RollupFold {
            grain,
            rows,
            high_water_ns,
        } => apply_rollup_fold(tx, *grain, rows, *high_water_ns),
    }
}

/// Merges folded rows into the grain table and advances the grain's
/// high-water mark — atomically with the fold, which is what makes
/// re-running after a crash converge (a crashed fold never commits rows
/// without also committing the mark).
fn apply_rollup_fold(
    tx: &rusqlite::Transaction<'_>,
    grain: obs_types::Grain,
    rows: &[obs_types::RollupRow],
    high_water_ns: i64,
) -> Result<Applied, rusqlite::Error> {
    let mut applied = Applied::default();
    let sql = format!(
        "INSERT INTO {table} (series_id, bucket_ns, n, sum, min, max, first, last)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT (series_id, bucket_ns) DO UPDATE SET
           n = n + excluded.n,
           sum = sum + excluded.sum,
           min = min(min, excluded.min),
           max = max(max, excluded.max),
           last = excluded.last",
        table = grain.table()
    );
    let mut stmt = tx.prepare_cached(&sql)?;
    for row in rows {
        applied.inserted += stmt.execute(rusqlite::params![
            row.series_id,
            row.bucket_ns,
            row.n,
            row.sum,
            row.min,
            row.max,
            row.first,
            row.last,
        ])?;
    }
    tx.execute(
        "INSERT INTO rollup_state (grain, high_water_ns) VALUES (?1, ?2)
         ON CONFLICT (grain) DO UPDATE SET high_water_ns = max(high_water_ns, excluded.high_water_ns)",
        rusqlite::params![grain.key(), high_water_ns],
    )?;
    Ok(applied)
}

fn apply_events(
    tx: &rusqlite::Transaction<'_>,
    records: &[EventRecord],
    ctx: &ProjectionContext,
    fanout: &mut CommittedBatch,
) -> Result<Applied, rusqlite::Error> {
    let mut applied = Applied::default();
    let mut stmt = tx.prepare_cached(
        "INSERT OR IGNORE INTO events
           (run_id, source_service, event_type, ts_logical, ts_wall_ns, seq,
            payload_version, payload, unknown, ingested_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )?;
    for record in records {
        #[cfg(feature = "test-hooks")]
        if record.event_type == TEST_ABORT_EVENT_TYPE {
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT),
                Some("test-hooks: induced mid-batch failure".to_owned()),
            ));
        }
        let changed = stmt.execute(rusqlite::params![
            record.run_id,
            record.source_service_stored,
            record.event_type,
            record.ts_logical,
            record.ts_wall_ns,
            record.seq,
            record.payload_version,
            record.payload,
            record.unknown as i64,
            record.ingested_at_ns,
        ])?;
        track_identity(&mut applied, record);
        if changed == 0 {
            continue; // Duplicate seq: projections must not run twice.
        }
        applied.inserted += 1;
        let rowid = tx.last_insert_rowid();
        if record.unknown {
            ctx.metrics.events_unknown_type_total.inc();
        }
        projections::apply(tx, record, ctx)?;
        if !fanout.run_ids.contains(&record.run_id) && !record.run_id.is_empty() {
            fanout.run_ids.push(record.run_id.clone());
        }
        fanout.events.push(CommittedEvent {
            rowid,
            record: record.clone(),
        });
    }
    Ok(applied)
}

/// Records the batch max seq per identity — duplicates count as covered
/// (the producer resent something we already have; the ack must still
/// advance past it, decision D7).
fn track_identity(applied: &mut Applied, record: &EventRecord) {
    let seq = record.seq as u64;
    let key = (record.run_id.clone(), record.source_service_stored.clone());
    if let Some(entry) = applied
        .max_seq_by_identity
        .iter_mut()
        .find(|(identity, _)| *identity == key)
    {
        entry.1 = entry.1.max(seq);
    } else {
        applied.max_seq_by_identity.push((key, seq));
    }
}

fn apply_metric_samples(
    tx: &rusqlite::Transaction<'_>,
    samples: &[MetricSample],
) -> Result<Applied, rusqlite::Error> {
    let mut applied = Applied::default();
    let mut intern = tx.prepare_cached(
        "INSERT OR IGNORE INTO metric_series (service, metric, labels) VALUES (?1, ?2, ?3)",
    )?;
    let mut lookup = tx.prepare_cached(
        "SELECT series_id FROM metric_series WHERE service = ?1 AND metric = ?2 AND labels = ?3",
    )?;
    let mut insert = tx.prepare_cached(
        "INSERT OR IGNORE INTO metrics_raw (series_id, ts_ns, value) VALUES (?1, ?2, ?3)",
    )?;
    for sample in samples {
        intern.execute(rusqlite::params![
            sample.key.service,
            sample.key.metric,
            sample.key.labels_json
        ])?;
        let series_id: i64 = lookup.query_row(
            rusqlite::params![
                sample.key.service,
                sample.key.metric,
                sample.key.labels_json
            ],
            |row| row.get(0),
        )?;
        applied.inserted +=
            insert.execute(rusqlite::params![series_id, sample.ts_ns, sample.value])?;
    }
    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use obs_types::{SeriesKey, SourceService, WriteBatch};

    fn record(seq: i64) -> EventRecord {
        EventRecord {
            run_id: "run-a".into(),
            source_service_stored: "exploration-orchestrator/orchestratord-1".into(),
            event_type: "node-added".into(),
            ts_logical: seq,
            ts_wall_ns: 1_000 + seq,
            seq,
            payload_version: 1,
            payload: format!(
                r#"{{"node_id":"{seq}","parent_id":null,"snapshot_ref":"snap-{seq}","depth":1,"progress_score":0.5,"novelty_score":0.1,"stage":0,"guest_time_ns":9,"input_delta_bytes":4,"expansion_idx":{seq}}}"#
            ),
            unknown: false,
            ingested_at_ns: 42,
            source_service: SourceService::ExplorationOrchestrator,
        }
    }

    #[tokio::test]
    async fn writes_events_and_dedups_on_replay() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(&crate::StoreConfig::new(dir.path().join("t.db"))).unwrap();
        let pool = store.read_pool();
        let (conn, _) = store.into_parts();
        let (writer, join) = spawn_writer(conn, ProjectionContext::default());
        let mut committed = writer.subscribe();

        let applied = writer
            .write(WriteBatch::Events(vec![record(1), record(2)]))
            .await
            .unwrap();
        assert_eq!(applied.inserted, 2);
        assert_eq!(
            applied.max_seq_by_identity,
            vec![(
                (
                    "run-a".to_owned(),
                    "exploration-orchestrator/orchestratord-1".to_owned()
                ),
                2
            )]
        );
        let batch = committed.recv().await.unwrap();
        assert_eq!(batch.events.len(), 2);
        assert_eq!(batch.run_ids, vec!["run-a".to_owned()]);

        // Overlapping resend: duplicate seqs vanish via INSERT OR IGNORE,
        // but the ack coverage still includes them.
        let applied = writer
            .write(WriteBatch::Events(vec![record(2), record(3)]))
            .await
            .unwrap();
        assert_eq!(applied.inserted, 1);
        assert_eq!(applied.max_seq_by_identity[0].1, 3);

        let (events, nodes, runs): (i64, i64, i64) = pool
            .with_read(|conn| {
                Ok((
                    conn.query_row("SELECT count(*) FROM events", [], |row| row.get(0))?,
                    conn.query_row("SELECT count(*) FROM tree_nodes", [], |row| row.get(0))?,
                    conn.query_row(
                        "SELECT nodes_added FROM runs WHERE run_id='run-a'",
                        [],
                        |row| row.get(0),
                    )?,
                ))
            })
            .unwrap();
        assert_eq!(events, 3);
        assert_eq!(nodes, 3);
        assert_eq!(runs, 3, "duplicate replay must not double projections");

        drop(writer);
        join.join().unwrap();
    }

    #[tokio::test]
    async fn writes_metric_samples_with_interning() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(&crate::StoreConfig::new(dir.path().join("t.db"))).unwrap();
        let pool = store.read_pool();
        let (conn, _) = store.into_parts();
        let (writer, join) = spawn_writer(conn, ProjectionContext::default());

        let key = SeriesKey::new("event", "kept", "{}");
        let samples = vec![
            MetricSample {
                key: key.clone(),
                ts_ns: 1,
                value: 5.0,
            },
            MetricSample {
                key: key.clone(),
                ts_ns: 2,
                value: 6.0,
            },
        ];
        let applied = writer
            .write(WriteBatch::MetricSamples(samples))
            .await
            .unwrap();
        assert_eq!(applied.inserted, 2);

        let series: i64 = pool
            .with_read(|conn| {
                conn.query_row("SELECT count(*) FROM metric_series", [], |row| row.get(0))
            })
            .unwrap();
        assert_eq!(series, 1);

        drop(writer);
        join.join().unwrap();
    }
}
