//! THE CI determinism gate (MAP.md obligation; IMPLEMENTATION-PLAN §M1
//! acceptance bullet 1): a 100k-envelope seeded stream ingested twice into
//! the same database — and once into a second fresh database — must leave
//! byte-identical tables. Observatory touches no execution path; ingest
//! idempotency is its determinism surface.
//!
//! Runs in CI as `cargo test -p obs-ingest --release --test
//! determinism_replay` (the --test form fails loudly if this target ever
//! disappears; a name filter could pass vacuously).

use std::sync::Arc;

use obs_events_gen::{Sim, SimConfig, Vocab};
use obs_ingest::IngestService;
use obs_store::{GridHint, IngestMetrics, ProjectionContext, StandaloneData, Store, StoreConfig};
use obs_types::event_ingest_client::EventIngestClient;
use obs_types::FixedClock;
use tokio_stream::StreamExt;

const SEED: u64 = 42;
const EVENTS: u64 = 100_000;
const FIXED_NOW: i64 = 1_790_000_000_000_000_000;

fn sim_config() -> SimConfig {
    let mut config = SimConfig::new(SEED, EVENTS);
    config.vocab = Vocab::Catalog;
    config.run_id = "determinism-run".to_owned();
    config.producer_id = "obs-events-gen-gate".to_owned();
    config.goal = true;
    config
}

fn standalone() -> StandaloneData {
    StandaloneData {
        experiment_json: Some(r#"{"goal_id":"goal-1"}"#.to_owned()),
        feature_map_json: Some("features: []".to_owned()),
        grid_hint: Some(GridHint {
            x: "player_x".to_owned(),
            y: "player_y".to_owned(),
            room: Some("room_id".to_owned()),
            cell_w: 32.0,
            cell_h: 32.0,
        }),
    }
}

struct Gate {
    addr: std::net::SocketAddr,
    shutdown: tokio::sync::watch::Sender<bool>,
}

async fn serve(db_path: &std::path::Path) -> Gate {
    let store = Store::open(&StoreConfig::new(db_path)).unwrap();
    let (conn, pool) = store.into_parts();
    let ctx = ProjectionContext {
        standalone: standalone(),
        metrics: IngestMetrics::new(),
    };
    let (writer, _join) = obs_store::spawn_writer(conn, ctx);
    let service = IngestService::new(
        writer,
        pool,
        Arc::new(FixedClock(FIXED_NOW)),
        IngestMetrics::new(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(obs_ingest::server(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _ = shutdown_rx.wait_for(|stop| *stop).await;
                },
            )
            .await
            .unwrap();
    });
    Gate { addr, shutdown }
}

/// Streams the full seeded sim over real gRPC and waits for the final ack.
async fn ingest_stream(addr: std::net::SocketAddr) {
    let mut client = EventIngestClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(512);
    tokio::spawn(async move {
        for envelope in Sim::new(sim_config()) {
            if tx.send(envelope).await.is_err() {
                return;
            }
        }
    });
    let mut acks = client
        .publish_events(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let mut final_ack = 0;
    while let Some(ack) = acks.next().await {
        final_ack = ack.unwrap().acked_seq;
    }
    assert_eq!(final_ack, EVENTS, "every envelope must be covered by acks");
}

fn dump(db_path: &std::path::Path) -> String {
    let conn = rusqlite::Connection::open(db_path).unwrap();
    obs_store::dump::dump_all(&conn).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn hundred_k_double_replay_is_byte_identical() {
    let dir = tempfile::tempdir().unwrap();

    // DB #1, pass 1.
    let db1 = dir.path().join("gate-1.db");
    let gate = serve(&db1).await;
    ingest_stream(gate.addr).await;
    let dump1_first = dump(&db1);

    // Same DB, same stream again: the double replay.
    ingest_stream(gate.addr).await;
    let dump1_second = dump(&db1);
    assert_eq!(
        dump1_first, dump1_second,
        "double replay must be byte-identical"
    );
    let _ = gate.shutdown.send(true);

    // Fresh DB #2, same stream, same FixedClock: cross-instance
    // determinism.
    let db2 = dir.path().join("gate-2.db");
    let gate2 = serve(&db2).await;
    ingest_stream(gate2.addr).await;
    let dump2 = dump(&db2);
    let _ = gate2.shutdown.send(true);
    assert_eq!(
        dump1_first, dump2,
        "a fresh instance fed the same stream must produce identical bytes"
    );

    // Generator-tie assertion: each individual event-derived series in
    // metrics_raw has exactly one sample per batch-completed event — the
    // envelope-ts_wall_ns rule. A Clock-minted ts would collapse these
    // rows on the (series_id, ts_ns) primary key under FixedClock.
    let mut counting_sim = Sim::new(sim_config());
    for _ in counting_sim.by_ref() {}
    let batch_completed = counting_sim
        .counts()
        .by_type
        .get("batch-completed")
        .copied()
        .unwrap_or(0);
    assert!(batch_completed > 0, "sim must emit batch-completed events");
    let conn = rusqlite::Connection::open(&db1).unwrap();
    let kept_rows: i64 = conn
        .query_row(
            "SELECT count(*) FROM metrics_raw WHERE series_id =
               (SELECT series_id FROM metric_series
                WHERE service='event' AND metric='kept')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        kept_rows as u64, batch_completed,
        "the 'kept' series must carry one sample per batch-completed event"
    );
}
