//! M1 ingest integration tests: in-proc tonic client + tempfile DB per the
//! testing strategy. Load-level acceptance lives in the package-06 harness.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use obs_ingest::IngestService;
use obs_store::{GridHint, IngestMetrics, ProjectionContext, StandaloneData, Store, StoreConfig};
use obs_types::event_ingest_client::EventIngestClient;
use obs_types::{EventBatch, EventEnvelope, FixedClock, PublishAck, SourceService};

const FIXED_NOW: i64 = 1_789_000_000_000_000_000;

struct Harness {
    addr: std::net::SocketAddr,
    db_path: std::path::PathBuf,
    metrics: IngestMetrics,
    shutdown: tokio::sync::watch::Sender<bool>,
    _dir: Option<tempfile::TempDir>,
}

impl Harness {
    async fn start(standalone: StandaloneData) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("ingest.db");
        Self::start_at(Some(dir), db_path, standalone).await
    }

    /// Starts (or restarts) a server over an existing database path.
    async fn start_at(
        dir: Option<tempfile::TempDir>,
        db_path: std::path::PathBuf,
        standalone: StandaloneData,
    ) -> Self {
        let store = Store::open(&StoreConfig::new(&db_path)).unwrap();
        let (conn, pool) = store.into_parts();
        let metrics = IngestMetrics::new();
        let ctx = ProjectionContext {
            standalone,
            metrics: metrics.clone(),
        };
        let (writer, _join) = obs_store::spawn_writer(conn, ctx);
        let service = IngestService::new(
            writer,
            pool,
            Arc::new(FixedClock(FIXED_NOW)),
            metrics.clone(),
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
        Self {
            addr,
            db_path,
            metrics,
            shutdown,
            _dir: dir,
        }
    }

    async fn client(&self) -> EventIngestClient<tonic::transport::Channel> {
        EventIngestClient::connect(format!("http://{}", self.addr))
            .await
            .unwrap()
    }

    fn dump(&self) -> String {
        let conn = rusqlite::Connection::open(&self.db_path).unwrap();
        obs_store::dump::dump_all(&conn).unwrap()
    }

    fn query_i64(&self, sql: &str) -> i64 {
        let conn = rusqlite::Connection::open(&self.db_path).unwrap();
        conn.query_row(sql, [], |row| row.get(0)).unwrap()
    }

    fn query_string(&self, sql: &str) -> String {
        let conn = rusqlite::Connection::open(&self.db_path).unwrap();
        conn.query_row(sql, [], |row| row.get(0)).unwrap()
    }

    async fn stop(self) -> std::path::PathBuf {
        let _ = self.shutdown.send(true);
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.db_path
    }
}

fn envelope(seq: u64, event_type: &str, payload: serde_json::Value) -> EventEnvelope {
    EventEnvelope {
        envelope_version: 1,
        ts_logical: seq,
        ts_wall_ns: 1_000_000 + seq,
        run_id: "run-a".into(),
        source_service: SourceService::ExplorationOrchestrator as i32,
        event_type: event_type.into(),
        payload_version: 1,
        payload_json: payload.to_string().into_bytes(),
        seq,
        producer_id: "orchestratord-t".into(),
    }
}

fn node_added(seq: u64, node_id: u64, score: f64) -> EventEnvelope {
    envelope(
        seq,
        "node-added",
        serde_json::json!({
            "node_id": node_id.to_string(),
            "parent_id": if node_id == 1 { serde_json::Value::Null } else { serde_json::json!("1") },
            "snapshot_ref": format!("snap-{node_id}"),
            "depth": 1,
            "progress_score": score,
            "novelty_score": 0.25,
            "cell_key": "3:1:2",
            "stage": 0,
            "guest_time_ns": 500,
            "input_delta_bytes": 12,
            "expansion_idx": seq,
            "features": {"player_x": 40.0, "player_y": 17.0, "room_id": 3.0}
        }),
    )
}

/// Streams envelopes and collects every ack until the stream closes.
async fn publish(
    client: &mut EventIngestClient<tonic::transport::Channel>,
    envelopes: Vec<EventEnvelope>,
) -> Vec<PublishAck> {
    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(async move {
        for envelope in envelopes {
            if tx.send(envelope).await.is_err() {
                return;
            }
        }
    });
    let response = client
        .publish_events(ReceiverStream::new(rx))
        .await
        .unwrap();
    let mut acks = Vec::new();
    let mut stream = response.into_inner();
    while let Some(ack) = stream.next().await {
        acks.push(ack.unwrap());
    }
    acks
}

fn grid_standalone() -> StandaloneData {
    StandaloneData {
        experiment_json: Some(r#"{"goal_id":"boss-1"}"#.to_owned()),
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

#[tokio::test]
async fn round_trip_populates_projection_tables() {
    let harness = Harness::start(grid_standalone()).await;
    let mut client = harness.client().await;

    let events = vec![
        node_added(1, 1, 0.10),
        node_added(2, 2, 0.55),
        envelope(
            3,
            "best-score-improved",
            serde_json::json!({"node_id":"2","score":0.55,"prev_best":0.10,"expansion_idx":2}),
        ),
        envelope(
            4,
            "batch-completed",
            serde_json::json!({"batch_seq":1,"kept":2,"dups":1,"regressions":0,"failed_jobs":0,"batch_wall_ms":12.5}),
        ),
        envelope(
            5,
            "checkpoint",
            serde_json::json!({"checkpoint_id":"ck-1","expansion_idx":2,"frontier_size":4,"tree_nodes":2,"archive_cells":9,"seen_set_size":11}),
        ),
        envelope(
            6,
            "goal-reached",
            serde_json::json!({"node_id":"2","goal_id":"boss-1","score":0.55,"expansion_idx":2,"path_len":2}),
        ),
    ];
    let acks = publish(&mut client, events).await;
    assert_eq!(acks.last().unwrap().acked_seq, 6);
    assert!(acks.iter().all(|ack| ack.rejections.is_empty()));

    assert_eq!(harness.query_i64("SELECT count(*) FROM events"), 6);
    assert_eq!(harness.query_i64("SELECT count(*) FROM tree_nodes"), 2);
    assert_eq!(
        harness.query_string("SELECT snapshot_ref FROM tree_nodes WHERE node_id='2'"),
        "snap-2"
    );
    assert_eq!(
        harness.query_string("SELECT best_node_id FROM runs WHERE run_id='run-a'"),
        "2"
    );
    assert_eq!(
        harness.query_string("SELECT status FROM runs WHERE run_id='run-a'"),
        "goal_reached"
    );
    assert_eq!(harness.query_i64("SELECT goal_reached FROM runs"), 1);
    assert_eq!(harness.query_i64("SELECT expansions FROM runs"), 6);
    assert_eq!(harness.query_i64("SELECT nodes_added FROM runs"), 2);
    assert_eq!(
        harness.query_i64("SELECT count(*) FROM score_points WHERE run_id='run-a'"),
        1
    );
    assert_eq!(
        harness.query_i64("SELECT count(*) FROM checkpoints WHERE checkpoint_id='ck-1'"),
        1
    );
    // goal-reached projects an info finding.
    assert_eq!(
        harness.query_string("SELECT severity FROM findings WHERE kind='goal-reached'"),
        "info"
    );
    // batch-completed projects five event-derived series.
    assert_eq!(
        harness.query_i64("SELECT count(*) FROM metric_series WHERE service='event'"),
        5
    );
    // Standalone config cached on first event.
    assert_eq!(
        harness.query_string("SELECT experiment_json FROM runs WHERE run_id='run-a'"),
        r#"{"goal_id":"boss-1"}"#
    );
    // Coverage: player_x=40, player_y=17, cell 32x32 -> (1, 0), room 3.
    assert_eq!(
        harness.query_i64(
            "SELECT visits FROM coverage_cells WHERE run_id='run-a' AND map_id='3' AND cx=1 AND cy=0"
        ),
        2
    );
}

#[tokio::test]
async fn idempotency_overlapping_resend_changes_nothing() {
    let harness = Harness::start(grid_standalone()).await;
    let mut client = harness.client().await;

    let batch: Vec<_> = (1..=20).map(|seq| node_added(seq, seq, 0.01)).collect();
    publish(&mut client, batch).await;
    let before = harness.dump();

    // Resend an overlapping range (10..=20) plus a fresh tail (21..=25).
    let overlap: Vec<_> = (10..=20).map(|seq| node_added(seq, seq, 0.01)).collect();
    let acks = publish(&mut client, overlap).await;
    assert_eq!(
        acks.last().unwrap().acked_seq,
        20,
        "duplicates are covered by the ack (D7)"
    );
    let after = harness.dump();
    assert_eq!(before, after, "duplicate replay must be byte-invisible");
}

#[tokio::test]
async fn rejection_keeps_stream_alive_and_valid_events_land() {
    let harness = Harness::start(StandaloneData::default()).await;
    let mut client = harness.client().await;

    let mut bad = envelope(2, "node-added", serde_json::json!({}));
    bad.payload_json = b"not json at all".to_vec();
    let events = vec![node_added(1, 1, 0.5), bad, node_added(3, 3, 0.6)];
    let acks = publish(&mut client, events).await;

    let all_rejections: Vec<_> = acks.iter().flat_map(|a| a.rejections.clone()).collect();
    assert_eq!(all_rejections.len(), 1);
    assert_eq!(all_rejections[0].seq, 2);
    assert_eq!(all_rejections[0].reason, "payload not a JSON object");
    assert_eq!(
        acks.last().unwrap().acked_seq,
        3,
        "stream survived and the later valid event committed"
    );
    assert_eq!(harness.query_i64("SELECT count(*) FROM events"), 2);
    assert_eq!(
        harness
            .metrics
            .events_rejected_total
            .with_label_values(&["payload not a JSON object"])
            .get(),
        1
    );
}

#[tokio::test]
async fn unknown_event_type_stored_flagged_not_projected() {
    let harness = Harness::start(StandaloneData::default()).await;
    let mut client = harness.client().await;

    let acks = publish(
        &mut client,
        vec![envelope(1, "mystery-event", serde_json::json!({"x": 1}))],
    )
    .await;
    assert!(acks.iter().all(|a| a.rejections.is_empty()));
    assert_eq!(
        harness.query_i64("SELECT unknown FROM events WHERE seq=1"),
        1
    );
    assert_eq!(
        harness.query_i64("SELECT count(*) FROM runs"),
        0,
        "unknown events project nothing"
    );
    assert_eq!(harness.metrics.events_unknown_type_total.get(), 1);
}

#[tokio::test]
async fn orch_asbuilt_payloads_accepted_with_partial_projections() {
    // D5 fixtures: field shapes of orch-server/src/events.rs today.
    let harness = Harness::start(StandaloneData::default()).await;
    let mut client = harness.client().await;

    let events = vec![
        envelope(
            1,
            "node-added",
            serde_json::json!({
                "node_id": "7", "parent_node_id": "1", "score": 0.5,
                "novelty": 0.1, "cell_key": 42u64, "stage": 0,
                "features": {"player_x": 3.0}
            }),
        ),
        envelope(
            2,
            "best-score-improved",
            serde_json::json!({"node_id":"7","best_score":0.5,"previous_best_score":0.1}),
        ),
        envelope(
            3,
            "checkpoint",
            serde_json::json!({"batch_seq":1,"expansions":3,"archive_seq":2}),
        ),
    ];
    let acks = publish(&mut client, events).await;
    assert!(acks.iter().all(|a| a.rejections.is_empty()));
    assert_eq!(acks.last().unwrap().acked_seq, 3);

    // node-added landed with D3 defaults (no rename shims).
    assert_eq!(
        harness.query_string(
            "SELECT snapshot_ref || '|' || progress_score || '|' || coalesce(parent_id, 'NULL')
             FROM tree_nodes WHERE node_id='7'"
        ),
        "|0.0|NULL"
    );
    // best-score-improved without `score` skips the curve point.
    assert_eq!(harness.query_i64("SELECT count(*) FROM score_points"), 0);
    // checkpoint fell back to the derived id.
    assert_eq!(
        harness.query_string("SELECT checkpoint_id FROM checkpoints"),
        "ckpt-3"
    );
    assert!(
        harness
            .metrics
            .projection_partial_total
            .with_label_values(&["node-added"])
            .get()
            >= 1
    );
    assert!(
        harness
            .metrics
            .projection_partial_total
            .with_label_values(&["best-score-improved"])
            .get()
            >= 1
    );
}

#[tokio::test]
async fn node_pruned_without_node_id_counts_but_touches_no_tree_row() {
    let harness = Harness::start(StandaloneData::default()).await;
    let mut client = harness.client().await;

    let events = vec![
        node_added(1, 1, 0.5),
        envelope(
            2,
            "node-pruned",
            serde_json::json!({"parent_id":"1","reason":"duplicate"}),
        ),
        envelope(
            3,
            "node-pruned",
            serde_json::json!({"node_id":"1","parent_id":"1","reason":"exhausted"}),
        ),
    ];
    publish(&mut client, events).await;

    assert_eq!(harness.query_i64("SELECT nodes_pruned FROM runs"), 2);
    assert_eq!(
        harness.query_i64("SELECT count(*) FROM tree_nodes WHERE pruned=1"),
        1,
        "only the id-carrying prune marks a tree row (D4)"
    );
    assert_eq!(
        harness.query_string("SELECT prune_reason FROM tree_nodes WHERE node_id='1'"),
        "exhausted"
    );
}

#[tokio::test]
async fn bulk_is_atomic_under_midbatch_failure() {
    let harness = Harness::start(StandaloneData::default()).await;
    let mut client = harness.client().await;

    // The test-hooks feature makes this event type abort the transaction.
    let events = vec![
        node_added(1, 1, 0.5),
        envelope(2, "__obs_test_abort__", serde_json::json!({})),
        node_added(3, 3, 0.6),
    ];
    let error = client
        .publish_events_bulk(EventBatch { events })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(
        harness.query_i64("SELECT count(*) FROM events"),
        0,
        "whole batch absent: atomic per batch"
    );

    // A clean bulk batch lands with a single ack.
    let ack = client
        .publish_events_bulk(EventBatch {
            events: vec![node_added(1, 1, 0.5), node_added(2, 2, 0.6)],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ack.acked_seq, 2);
    assert_eq!(harness.query_i64("SELECT count(*) FROM events"), 2);
}

#[tokio::test]
async fn ack_after_restart_reports_persisted_high_water() {
    let harness = Harness::start(StandaloneData::default()).await;
    let mut client = harness.client().await;
    publish(
        &mut client,
        (1..=7).map(|seq| node_added(seq, seq, 0.1)).collect(),
    )
    .await;
    let db_path = harness.stop().await;

    // "Restart": a fresh server (fresh AckTracker) over the same DB.
    let harness = Harness::start_at(None, db_path, StandaloneData::default()).await;
    let mut client = harness.client().await;
    // Producer resends from acked+1 (its own record); a duplicate is fine.
    let acks = publish(&mut client, vec![node_added(7, 7, 0.1)]).await;
    assert_eq!(
        acks.last().unwrap().acked_seq,
        7,
        "first ack after restart must reflect the persisted high-water"
    );
}

#[tokio::test]
async fn coverage_cells_match_hand_computed_grid() {
    let harness = Harness::start(grid_standalone()).await;
    let mut client = harness.client().await;

    // Hand-computed: cell_w = cell_h = 32.
    //  (40, 17, room 3)  -> cx=1, cy=0, map '3'
    //  (63, 17, room 3)  -> cx=1, cy=0, map '3'   (same cell, visits=2)
    //  (64, 40, room 3)  -> cx=2, cy=1, map '3'
    //  (10, 10, room 5)  -> cx=0, cy=0, map '5'
    //  missing player_y  -> skipped + metered
    let mk = |seq: u64, x: f64, y: f64, room: f64, score: f64| {
        envelope(
            seq,
            "node-added",
            serde_json::json!({
                "node_id": seq.to_string(), "parent_id": null, "snapshot_ref": "s",
                "depth": 0, "progress_score": score, "novelty_score": 0.0, "stage": 0,
                "guest_time_ns": 0, "input_delta_bytes": 0, "expansion_idx": seq,
                "features": {"player_x": x, "player_y": y, "room_id": room}
            }),
        )
    };
    let mut missing = envelope(
        5,
        "node-added",
        serde_json::json!({
            "node_id": "5", "parent_id": null, "snapshot_ref": "s", "depth": 0,
            "progress_score": 0.1, "novelty_score": 0.0, "stage": 0,
            "guest_time_ns": 0, "input_delta_bytes": 0, "expansion_idx": 5,
            "features": {"player_x": 1.0, "room_id": 3.0}
        }),
    );
    missing.seq = 5;
    let events = vec![
        mk(1, 40.0, 17.0, 3.0, 0.2),
        mk(2, 63.0, 17.0, 3.0, 0.9),
        mk(3, 64.0, 40.0, 3.0, 0.4),
        mk(4, 10.0, 10.0, 5.0, 0.1),
        missing,
    ];
    publish(&mut client, events).await;

    assert_eq!(harness.query_i64("SELECT count(*) FROM coverage_cells"), 3);
    assert_eq!(
        harness.query_i64("SELECT visits FROM coverage_cells WHERE map_id='3' AND cx=1 AND cy=0"),
        2
    );
    assert_eq!(
        harness.query_string(
            "SELECT best_node_id FROM coverage_cells WHERE map_id='3' AND cx=1 AND cy=0"
        ),
        "2",
        "best node follows the higher score"
    );
    assert_eq!(
        harness.query_i64("SELECT count(*) FROM coverage_cells WHERE map_id='5' AND cx=0 AND cy=0"),
        1
    );
    assert_eq!(harness.metrics.coverage_skipped_total.get(), 1);
}

#[tokio::test]
async fn replay_events_forward_compat_upsert() {
    let harness = Harness::start(StandaloneData::default()).await;
    let mut client = harness.client().await;

    let mut progress = envelope(
        1,
        "replay-job-progress",
        serde_json::json!({
            "job_id":"j-1","artifact_id":"a-1","run_id_target":"run-a","node_id":"7",
            "phase":"reexec","pct":42.5
        }),
    );
    progress.source_service = SourceService::ReplayRenderer as i32;
    let mut registered = envelope(
        2,
        "replay-artifact-registered",
        serde_json::json!({
            "job_id":"j-1","artifact_id":"a-1","node_id":"7","kind":"video",
            "uri":"cp://artifacts/a-1.mp4"
        }),
    );
    registered.source_service = SourceService::ReplayRenderer as i32;
    let mut completed = envelope(
        3,
        "replay-job-completed",
        serde_json::json!({
            "job_id":"j-1","artifact_id":"a-1","node_id":"7","verified":true,
            "status":"done","timeline_uri":"cp://artifacts/a-1.timeline.json"
        }),
    );
    completed.source_service = SourceService::ReplayRenderer as i32;

    publish(&mut client, vec![progress, registered, completed]).await;

    assert_eq!(harness.query_i64("SELECT count(*) FROM replays"), 1);
    assert_eq!(
        harness.query_string(
            "SELECT status || '|' || verified || '|' || video_uri || '|' || timeline_uri FROM replays"
        ),
        "done|1|cp://artifacts/a-1.mp4|cp://artifacts/a-1.timeline.json",
        "completed keeps the registered video_uri via coalesce"
    );
}
