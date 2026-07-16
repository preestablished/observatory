//! WriteBatch micro-bench (M0 acceptance): batched single-table inserts,
//! 500 rows per transaction, through the real writer task. Target: ≥50k
//! inserts/s sustained on NVMe. Run with:
//!
//!     cargo bench -p obs-store --bench write_batch
//!
//! Plain harness (`harness = false`): measures wall time, prints rows/s.

use std::time::Instant;

use obs_store::{spawn_writer, Store, StoreConfig};
use obs_types::{EventRecord, SourceService, WriteBatch};

fn record(seq: i64) -> EventRecord {
    EventRecord {
        run_id: "bench-run".into(),
        source_service_stored: "exploration-orchestrator/orchestratord-bench".into(),
        event_type: "node-added".into(),
        ts_logical: seq,
        ts_wall_ns: 1_700_000_000_000_000_000 + seq,
        seq,
        payload_version: 1,
        payload: r#"{"node_id":"1","parent_id":"0","progress_score":0.5,"novelty_score":0.1}"#
            .into(),
        unknown: false,
        ingested_at_ns: 1_700_000_000_000_000_000,
        source_service: SourceService::ExplorationOrchestrator,
    }
}

fn main() {
    const TOTAL: i64 = 500_000;
    const BATCH: i64 = 500;

    let dir = tempfile::tempdir().expect("tempdir");
    let store = Store::open(&StoreConfig::new(dir.path().join("bench.db"))).expect("open");
    let (conn, _pool) = store.into_parts();
    let (writer, join) = spawn_writer(conn, obs_store::ProjectionContext::default());

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let start = Instant::now();
    runtime.block_on(async {
        let mut seq = 1;
        while seq <= TOTAL {
            let batch: Vec<EventRecord> = (seq..seq + BATCH).map(record).collect();
            writer
                .write(WriteBatch::Events(batch))
                .await
                .expect("write");
            seq += BATCH;
        }
    });
    let elapsed = start.elapsed();
    drop(writer);
    join.join().expect("writer join");

    let rate = TOTAL as f64 / elapsed.as_secs_f64();
    println!("write_batch: {TOTAL} rows in {elapsed:?} -> {rate:.0} inserts/s (batch {BATCH}/txn)");
}
