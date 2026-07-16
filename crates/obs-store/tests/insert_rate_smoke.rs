//! Reduced CI smoke for the M0 write-rate acceptance: asserts ≥5k
//! inserts/s in release mode to catch order-of-magnitude regressions only.
//! The real ≥50k/s target is local NVMe evidence
//! (`evidence/phase5-m1-m2/m0-writebatch-bench.txt`, produced by
//! `cargo bench -p obs-store --bench write_batch`).

use std::time::Instant;

use obs_store::{spawn_writer, Store, StoreConfig};
use obs_types::{EventRecord, SourceService, WriteBatch};

fn record(seq: i64) -> EventRecord {
    EventRecord {
        run_id: "smoke-run".into(),
        source_service_stored: "exploration-orchestrator/orchestratord-smoke".into(),
        event_type: "node-added".into(),
        ts_logical: seq,
        ts_wall_ns: 1_700_000_000_000_000_000 + seq,
        seq,
        payload_version: 1,
        payload: r#"{"node_id":"1","parent_id":"0","progress_score":0.5}"#.into(),
        unknown: false,
        ingested_at_ns: 1_700_000_000_000_000_000,
        source_service: SourceService::ExplorationOrchestrator,
    }
}

#[tokio::test]
async fn sustains_at_least_5k_inserts_per_second() {
    const TOTAL: i64 = 50_000;
    const BATCH: i64 = 500;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&StoreConfig::new(dir.path().join("smoke.db"))).unwrap();
    let pool = store.read_pool();
    let (conn, _) = store.into_parts();
    let (writer, join) = spawn_writer(conn, obs_store::ProjectionContext::default());

    let start = Instant::now();
    let mut seq = 1;
    while seq <= TOTAL {
        let batch: Vec<EventRecord> = (seq..seq + BATCH).map(record).collect();
        writer.write(WriteBatch::Events(batch)).await.unwrap();
        seq += BATCH;
    }
    let elapsed = start.elapsed();
    drop(writer);
    join.join().unwrap();

    let count: i64 = pool
        .with_read(|conn| conn.query_row("SELECT count(*) FROM events", [], |row| row.get(0)))
        .unwrap();
    assert_eq!(count, TOTAL);

    let rate = TOTAL as f64 / elapsed.as_secs_f64();
    eprintln!("insert rate: {rate:.0}/s over {TOTAL} rows ({elapsed:?})");
    // Only meaningful in release mode; debug builds run this for coverage
    // of the path without asserting the rate.
    if !cfg!(debug_assertions) {
        assert!(
            rate >= 5_000.0,
            "insert rate {rate:.0}/s below the 5k/s CI floor"
        );
    }
}
