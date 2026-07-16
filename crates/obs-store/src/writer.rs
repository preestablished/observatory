//! The single-writer task: one dedicated thread owns the write connection;
//! input is a bounded mpsc of [`WriteBatch`] messages; one SQLite
//! transaction per flush; a oneshot completion per batch is sent only
//! after the transaction commits (the durable-ack hook for ingest).

use rusqlite::Connection;
use tokio::sync::{mpsc, oneshot};

use obs_types::{EventRecord, MetricSample, StoreError, WriteBatch};

/// Bounded writer input (ARCHITECTURE §1: mpsc, 4096).
pub const WRITER_CHANNEL_CAPACITY: usize = 4096;

/// How many queued batches one flush (= one transaction) may coalesce.
const MAX_BATCHES_PER_FLUSH: usize = 64;

/// Per-batch application result, delivered post-commit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Applied {
    /// Rows actually inserted (duplicates ignored by `INSERT OR IGNORE`).
    pub inserted: usize,
}

struct WriteRequest {
    batch: WriteBatch,
    done: oneshot::Sender<Result<Applied, StoreError>>,
}

/// Cloneable handle every producer task uses to reach the writer.
#[derive(Clone)]
pub struct WriterHandle {
    sender: mpsc::Sender<WriteRequest>,
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
}

/// Spawns the writer thread owning `conn`. The thread drains the channel,
/// applies each flush in one transaction, and exits (after a final WAL
/// checkpoint) when every [`WriterHandle`] is dropped.
pub fn spawn_writer(conn: Connection) -> (WriterHandle, std::thread::JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(WRITER_CHANNEL_CAPACITY);
    let join = std::thread::Builder::new()
        .name("obs-store-writer".to_owned())
        .spawn(move || writer_loop(conn, receiver))
        .expect("spawn writer thread");
    (WriterHandle { sender }, join)
}

fn writer_loop(mut conn: Connection, mut receiver: mpsc::Receiver<WriteRequest>) {
    while let Some(first) = receiver.blocking_recv() {
        let mut requests = vec![first];
        while requests.len() < MAX_BATCHES_PER_FLUSH {
            match receiver.try_recv() {
                Ok(request) => requests.push(request),
                Err(_) => break,
            }
        }
        flush(&mut conn, requests);
    }
    // Graceful shutdown: channel closed and drained — final WAL checkpoint.
    if let Err(error) = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
        tracing::warn!(error = %error, "final WAL checkpoint failed");
    }
}

/// One transaction per flush; completions are sent only after commit.
fn flush(conn: &mut Connection, requests: Vec<WriteRequest>) {
    let mut results = Vec::with_capacity(requests.len());
    let outcome = (|| -> Result<(), rusqlite::Error> {
        let tx = conn.transaction()?;
        for request in &requests {
            let applied = apply_batch(&tx, &request.batch)?;
            results.push(applied);
        }
        tx.commit()
    })();

    match outcome {
        Ok(()) => {
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
) -> Result<Applied, rusqlite::Error> {
    match batch {
        WriteBatch::Events(records) => apply_events(tx, records),
        WriteBatch::MetricSamples(samples) => apply_metric_samples(tx, samples),
    }
}

fn apply_events(
    tx: &rusqlite::Transaction<'_>,
    records: &[EventRecord],
) -> Result<Applied, rusqlite::Error> {
    let mut inserted = 0usize;
    let mut stmt = tx.prepare_cached(
        "INSERT OR IGNORE INTO events
           (run_id, source_service, event_type, ts_logical, ts_wall_ns, seq,
            payload_version, payload, unknown, ingested_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    )?;
    for record in records {
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
        // Projections apply only to rows actually inserted (duplicate
        // replays must be byte-invisible); the projection pass lands with
        // the M1 ingest package.
        inserted += changed;
    }
    Ok(Applied { inserted })
}

fn apply_metric_samples(
    tx: &rusqlite::Transaction<'_>,
    samples: &[MetricSample],
) -> Result<Applied, rusqlite::Error> {
    let mut inserted = 0usize;
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
        inserted += insert.execute(rusqlite::params![series_id, sample.ts_ns, sample.value])?;
    }
    Ok(Applied { inserted })
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
            payload: "{}".into(),
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
        let (conn, _) = {
            let (conn, pool2) = store.into_parts();
            (conn, pool2)
        };
        let (writer, join) = spawn_writer(conn);

        let applied = writer
            .write(WriteBatch::Events(vec![record(1), record(2)]))
            .await
            .unwrap();
        assert_eq!(applied.inserted, 2);

        // Overlapping resend: duplicate seqs vanish via INSERT OR IGNORE.
        let applied = writer
            .write(WriteBatch::Events(vec![record(2), record(3)]))
            .await
            .unwrap();
        assert_eq!(applied.inserted, 1);

        let count: i64 = pool
            .with_read(|conn| conn.query_row("SELECT count(*) FROM events", [], |row| row.get(0)))
            .unwrap();
        assert_eq!(count, 3);

        drop(writer);
        join.join().unwrap();
    }

    #[tokio::test]
    async fn writes_metric_samples_with_interning() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(&crate::StoreConfig::new(dir.path().join("t.db"))).unwrap();
        let pool = store.read_pool();
        let (conn, _) = store.into_parts();
        let (writer, join) = spawn_writer(conn);

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
