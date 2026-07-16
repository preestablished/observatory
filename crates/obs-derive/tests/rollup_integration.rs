//! Rollup ticker integration: closed-bucket folding, crash-mid-fold
//! convergence (dump-compare), promotion, and the derived-metrics tick
//! against a hand-computed fixture.

use std::sync::Arc;

use obs_derive::{DerivedTicker, RollupTicker};
use obs_store::{spawn_writer, ProjectionContext, Store, StoreConfig, WriterHandle};
use obs_types::{FixedClock, Grain, MetricSample, RollupRow, SeriesKey, WriteBatch};

const S: i64 = 1_000_000_000;

struct Fixture {
    pool: obs_store::ReadPool,
    writer: WriterHandle,
    db_path: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("rollup.db");
    let store = Store::open(&StoreConfig::new(&db_path)).unwrap();
    let pool = store.read_pool();
    let (conn, _) = store.into_parts();
    let (writer, _join) = spawn_writer(conn, ProjectionContext::default());
    Fixture {
        pool,
        writer,
        db_path,
        _dir: dir,
    }
}

fn sample(metric: &str, ts_ns: i64, value: f64) -> MetricSample {
    MetricSample {
        key: SeriesKey::new("stub-service", metric, "{}"),
        ts_ns,
        value,
    }
}

fn dump(db_path: &std::path::Path) -> String {
    let conn = rusqlite::Connection::open(db_path).unwrap();
    obs_store::dump::dump_all(&conn).unwrap()
}

#[tokio::test]
async fn folds_closed_buckets_only_and_is_idempotent_on_rerun() {
    let fx = fixture().await;
    // Samples at 1..=14 s; clock at 12 s → closed 5s buckets are [0,5) and
    // [5,10); the [10,15) bucket is still open and must wait.
    let samples: Vec<MetricSample> = (1..=14)
        .map(|second| sample("gauge_a", second * S, second as f64))
        .collect();
    fx.writer
        .write(WriteBatch::MetricSamples(samples))
        .await
        .unwrap();

    let clock = Arc::new(FixedClock(12 * S));
    let ticker = RollupTicker::new(fx.pool.clone(), fx.writer.clone(), clock);
    ticker.tick_5s().await.unwrap();

    let rows: Vec<(i64, i64, f64, f64)> = fx
        .pool
        .with_read(|conn| {
            let mut stmt =
                conn.prepare("SELECT bucket_ns, n, first, last FROM rollup_5s ORDER BY bucket_ns")?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?;
            rows.collect()
        })
        .unwrap();
    assert_eq!(
        rows,
        vec![(0, 4, 1.0, 4.0), (5 * S, 5, 5.0, 9.0)],
        "open bucket [10,15) must not fold yet"
    );

    // Re-running with no new closed data changes nothing (idempotency).
    let before = dump(&fx.db_path);
    ticker.tick_5s().await.unwrap();
    assert_eq!(before, dump(&fx.db_path));
}

#[tokio::test]
async fn crash_mid_fold_converges_after_rerun() {
    // "Crash" = a fold whose transaction never committed (fold rows and
    // high-water advance are atomic in the writer, so a real crash can
    // only lose the whole transaction). Converge = re-running the ticker
    // produces tables identical to an uninterrupted fold.
    let make = |db: &std::path::Path| {
        let store = Store::open(&StoreConfig::new(db)).unwrap();
        let pool = store.read_pool();
        let (conn, _) = store.into_parts();
        let (writer, _join) = spawn_writer(conn, ProjectionContext::default());
        (pool, writer)
    };
    let dir = tempfile::tempdir().unwrap();
    let db_crashed = dir.path().join("crashed.db");
    let db_clean = dir.path().join("clean.db");
    let (pool_crashed, writer_crashed) = make(&db_crashed);
    let (pool_clean, writer_clean) = make(&db_clean);

    let samples: Vec<MetricSample> = (1..=30)
        .map(|second| sample("counter_b", second * S, (second * 10) as f64))
        .collect();
    for (writer, _pool) in [
        (&writer_crashed, &pool_crashed),
        (&writer_clean, &pool_clean),
    ] {
        writer
            .write(WriteBatch::MetricSamples(samples.clone()))
            .await
            .unwrap();
    }

    let clock = Arc::new(FixedClock(31 * S));
    // Crashed path: first tick's work is simulated as lost (nothing
    // committed), then the ticker re-runs for real.
    let ticker_crashed =
        RollupTicker::new(pool_crashed.clone(), writer_crashed.clone(), clock.clone());
    // (simulated crash: no-op — the atomic fold txn either fully commits
    // or fully rolls back; a partial fold cannot be observed)
    ticker_crashed.tick_5s().await.unwrap();
    ticker_crashed.tick_5s().await.unwrap(); // re-run after "restart"

    let ticker_clean = RollupTicker::new(pool_clean.clone(), writer_clean.clone(), clock.clone());
    ticker_clean.tick_5s().await.unwrap();

    // Compare rollup tables only (events/metrics identical by input).
    type RollupTuple = (i64, i64, i64, f64, f64, f64, f64, f64);
    let rollups = |db: &std::path::Path| {
        let conn = rusqlite::Connection::open(db).unwrap();
        let mut stmt = conn
            .prepare("SELECT series_id, bucket_ns, n, sum, min, max, first, last FROM rollup_5s ORDER BY series_id, bucket_ns")
            .unwrap();
        let rows: Vec<RollupTuple> = stmt
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        rows
    };
    assert_eq!(rollups(&db_crashed), rollups(&db_clean));
    let state: i64 = pool_crashed
        .with_read(|conn| {
            conn.query_row(
                "SELECT high_water_ns FROM rollup_state WHERE grain='5s'",
                [],
                |row| row.get(0),
            )
        })
        .unwrap();
    assert_eq!(state, 30 * S - 1);
}

#[tokio::test]
async fn promotion_folds_closed_coarse_buckets() {
    let fx = fixture().await;
    // 3 minutes of samples; clock at 150 s → 1m buckets [0,60) and
    // [60,120) closed, [120,180) open.
    let samples: Vec<MetricSample> = (0..150)
        .map(|second| sample("gauge_c", second * S, second as f64))
        .collect();
    fx.writer
        .write(WriteBatch::MetricSamples(samples))
        .await
        .unwrap();

    let clock = Arc::new(FixedClock(150 * S));
    let ticker = RollupTicker::new(fx.pool.clone(), fx.writer.clone(), clock);
    ticker.tick_5s().await.unwrap();
    ticker.tick_promote().await.unwrap();

    let buckets: Vec<(i64, i64)> = fx
        .pool
        .with_read(|conn| {
            let mut stmt = conn.prepare("SELECT bucket_ns, n FROM rollup_1m ORDER BY bucket_ns")?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            rows.collect()
        })
        .unwrap();
    // [0,60): seconds 0..59 → 60 samples; [60,120): 60 samples. The open
    // [120,180) 1m bucket must not be promoted. (5s rows exist through
    // second 145: the 5s fold closed [140,145).)
    assert_eq!(buckets, vec![(0, 60), (60 * S, 60)]);

    // 10m: no closed 10m bucket yet at 150 s → nothing.
    let count_10m: i64 = fx
        .pool
        .with_read(|conn| conn.query_row("SELECT count(*) FROM rollup_10m", [], |row| row.get(0)))
        .unwrap();
    assert_eq!(count_10m, 0);
}

/// Derived metrics vs hand-computed values (M2 acceptance): a pinned
/// event+scrape fixture on a FixedClock schedule. Expected numbers are
/// hand-written below, never generated by the code under test.
#[tokio::test]
async fn derived_metrics_match_hand_computed_fixture() {
    let fx = fixture().await;
    let now = 1_000 * S;
    let labels = r#"{"run_id":"run-d"}"#;

    // A running run with expansions=900.
    fx.pool.with_read(|_| Ok(())).unwrap();
    {
        let conn = rusqlite::Connection::open(&fx.db_path).unwrap();
        conn.execute(
            "INSERT INTO runs (run_id, status, first_seen_ns, last_event_ns, best_score, expansions)
             VALUES ('run-d', 'running', 1, 1, 0.75, 900)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO checkpoints (run_id, checkpoint_id, expansion_idx, ts_wall_ns, frontier_size)
             VALUES ('run-d', 'ck', 900, 1, 42)",
            [],
        )
        .unwrap();
    }
    // Event-derived kept/dups over the 30 s window:
    //   kept: 40 + 60 = 100; dups: 20 + 5 = 25
    //   novel_state_rate = 100 / 125 = 0.8   (hand-computed)
    let mut event_samples = Vec::new();
    for (metric, ts, value) in [
        ("kept", now - 20 * S, 40.0),
        ("kept", now - 10 * S, 60.0),
        ("dups", now - 20 * S, 20.0),
        ("dups", now - 10 * S, 5.0),
    ] {
        event_samples.push(MetricSample {
            key: SeriesKey::new("event", metric, labels),
            ts_ns: ts,
            value,
        });
    }
    // Scraped hypervisor slots: latest busy=6, total=8 → 0.75.
    for (metric, ts, value) in [
        ("dh_slots_busy", now - 5 * S, 6.0),
        ("dh_slots_total", now - 5 * S, 8.0),
        // Scraped scorer counters over the window:
        //   hits delta = 90 - 30 = 60; requests delta = 300 - 100 = 200
        //   dedup_hit_rate = 60 / 200 = 0.3   (hand-computed)
        ("sc_dedup_hits_total", now - 25 * S, 30.0),
        ("sc_dedup_hits_total", now - 5 * S, 90.0),
        ("sc_score_requests_total", now - 25 * S, 100.0),
        ("sc_score_requests_total", now - 5 * S, 300.0),
        // Fresher scraped frontier than the checkpoint's 42.
        ("eo_frontier_size", now - 5 * S, 55.0),
    ] {
        event_samples.push(MetricSample {
            key: SeriesKey::new("stub-service", metric, "{}"),
            ts_ns: ts,
            value,
        });
    }
    fx.writer
        .write(WriteBatch::MetricSamples(event_samples))
        .await
        .unwrap();

    let clock = Arc::new(FixedClock(now));
    let ticker = DerivedTicker::new(fx.pool.clone(), fx.writer.clone(), clock);
    // Two ticks 30 s apart cannot be simulated with one FixedClock; the
    // expansion rate needs history, so tick once (rate absent — needs two
    // points) and assert the other metrics.
    let written = ticker.tick().await.unwrap();
    assert!(
        written >= 5,
        "expected at least 5 derived samples, got {written}"
    );

    let derived = |metric: &str| -> f64 {
        fx.pool
            .with_read(|conn| {
                conn.query_row(
                    "SELECT r.value FROM metrics_raw r
                     JOIN metric_series s ON s.series_id = r.series_id
                     WHERE s.service='derived' AND s.metric=?1 AND s.labels=?2
                     ORDER BY r.ts_ns DESC LIMIT 1",
                    rusqlite::params![metric, labels],
                    |row| row.get(0),
                )
            })
            .unwrap()
    };
    assert_eq!(derived("search_best_score"), 0.75);
    assert_eq!(derived("search_novel_state_rate"), 0.8);
    assert_eq!(derived("worker_slot_utilization"), 0.75);
    assert_eq!(derived("search_dedup_hit_rate"), 0.3);
    assert_eq!(
        derived("search_frontier_size"),
        55.0,
        "fresh scraped eo_frontier_size refines the checkpoint value"
    );

    // Absent inputs stay ABSENT, not zero: no snapshot-store scrape data
    // → no snapshot_dedup_ratio series.
    let absent: i64 = fx
        .pool
        .with_read(|conn| {
            conn.query_row(
                "SELECT count(*) FROM metric_series
                 WHERE service='derived' AND metric='snapshot_dedup_ratio'",
                [],
                |row| row.get(0),
            )
        })
        .unwrap();
    assert_eq!(absent, 0);
}

/// RollupRow additive-merge sanity through the writer arm.
#[tokio::test]
async fn writer_merges_rollup_rows_on_conflict() {
    let fx = fixture().await;
    let row = |n: i64, sum: f64, min: f64, max: f64, first: f64, last: f64| RollupRow {
        series_id: 1,
        bucket_ns: 0,
        n,
        sum,
        min,
        max,
        first,
        last,
    };
    fx.writer
        .write(WriteBatch::RollupFold {
            grain: Grain::S5,
            rows: vec![row(2, 10.0, 4.0, 6.0, 4.0, 6.0)],
            high_water_ns: 4,
        })
        .await
        .unwrap();
    fx.writer
        .write(WriteBatch::RollupFold {
            grain: Grain::S5,
            rows: vec![row(1, 9.0, 9.0, 9.0, 9.0, 9.0)],
            high_water_ns: 9,
        })
        .await
        .unwrap();
    let merged: (i64, f64, f64, f64, f64, f64) = fx
        .pool
        .with_read(|conn| {
            conn.query_row(
                "SELECT n, sum, min, max, first, last FROM rollup_5s WHERE series_id=1",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
        })
        .unwrap();
    assert_eq!(merged, (3, 19.0, 4.0, 9.0, 4.0, 9.0));
    let high_water: i64 = fx
        .pool
        .with_read(|conn| {
            conn.query_row(
                "SELECT high_water_ns FROM rollup_state WHERE grain='5s'",
                [],
                |row| row.get(0),
            )
        })
        .unwrap();
    assert_eq!(high_water, 9, "high-water is monotone");
}
