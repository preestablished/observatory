//! End-to-end: a static stub server (tiny axum serving a fixture body)
//! scraped into a tempfile DB through the real Store + single writer.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use axum::Router;
use obs_scrape::{ScrapeConfig, ScrapeTarget, Scraper};
use obs_store::{spawn_writer, ProjectionContext, ReadPool, Store, StoreConfig, WriterHandle};
use obs_types::Clock;

const HYPERVISOR_BODY: &str = include_str!("fixtures/determinism-hypervisor.txt");
/// Distinct series / sample lines in the hypervisor fixture.
const HYPERVISOR_SAMPLES: i64 = 10;

/// Manually advanced test clock — two scrape passes get two distinct ts.
struct StepClock(AtomicI64);

impl Clock for StepClock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

async fn serve_body(body: &'static str) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let app = Router::new().route("/metrics", get(move || async move { body }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, server)
}

fn open_store(dir: &tempfile::TempDir) -> (WriterHandle, ReadPool, std::thread::JoinHandle<()>) {
    let store = Store::open(&StoreConfig::new(dir.path().join("obs.db"))).unwrap();
    let pool = store.read_pool();
    let (conn, _) = store.into_parts();
    let (writer, join) = spawn_writer(conn, ProjectionContext::default());
    (writer, pool, join)
}

fn count(pool: &ReadPool, sql: &str) -> i64 {
    pool.with_read(|conn| conn.query_row(sql, [], |row| row.get(0)))
        .unwrap()
}

#[tokio::test]
async fn fixture_scrapes_into_store_and_reticks_at_new_timestamps() {
    let (addr, server) = serve_body(HYPERVISOR_BODY).await;
    let dir = tempfile::tempdir().unwrap();
    let (writer, pool, join) = open_store(&dir);

    let clock = Arc::new(StepClock(AtomicI64::new(1_000)));
    let config = ScrapeConfig {
        targets: vec![ScrapeTarget {
            name: "determinism-hypervisor".into(),
            url: format!("http://{addr}/metrics"),
        }],
        interval: Duration::from_secs(5),
    };
    let mut scraper = Scraper::new(config, writer.clone(), clock.clone());
    let handle = scraper.handle();

    scraper.scrape_pass().await;
    let health = handle.target("determinism-hypervisor").unwrap();
    assert_eq!(health.last_success_ns(), Some(1_000));
    assert_eq!(health.failures(), 0);
    assert_eq!(
        count(&pool, "SELECT count(*) FROM metric_series"),
        HYPERVISOR_SAMPLES
    );
    assert_eq!(
        count(&pool, "SELECT count(*) FROM metrics_raw"),
        HYPERVISOR_SAMPLES
    );

    // Identical body at a later tick: same series, new rows at the new ts —
    // gauges are time series, not upserts.
    clock.0.store(6_000, Ordering::Relaxed);
    scraper.scrape_pass().await;
    assert_eq!(health.last_success_ns(), Some(6_000));
    assert_eq!(
        count(&pool, "SELECT count(*) FROM metric_series"),
        HYPERVISOR_SAMPLES
    );
    assert_eq!(
        count(&pool, "SELECT count(*) FROM metrics_raw"),
        2 * HYPERVISOR_SAMPLES
    );
    assert_eq!(
        count(&pool, "SELECT count(DISTINCT ts_ns) FROM metrics_raw"),
        2
    );
    // The verbatim `+Inf` bucket landed as its own interned series.
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM metric_series
             WHERE service = 'determinism-hypervisor'
               AND metric = 'dh_fork_restore_seconds_bucket'
               AND labels = '{\"le\":\"+Inf\"}'"
        ),
        1
    );

    server.abort();
    drop(writer);
    drop(scraper);
    join.join().unwrap();
}

#[tokio::test]
async fn target_down_counts_failures_and_freezes_last_success() {
    let (addr, server) = serve_body(HYPERVISOR_BODY).await;
    let dir = tempfile::tempdir().unwrap();
    let (writer, pool, join) = open_store(&dir);

    let clock = Arc::new(StepClock(AtomicI64::new(1_000)));
    let config = ScrapeConfig {
        targets: vec![ScrapeTarget {
            name: "determinism-hypervisor".into(),
            url: format!("http://{addr}/metrics"),
        }],
        interval: Duration::from_secs(1),
    };
    let mut scraper = Scraper::new(config, writer.clone(), clock.clone());
    let handle = scraper.handle();

    scraper.scrape_pass().await;
    let health = handle.target("determinism-hypervisor").unwrap();
    assert_eq!(health.last_success_ns(), Some(1_000));

    // Kill the stub; further passes fail but keep ticking, last_success_ns
    // stays frozen at the pre-outage tick, and nothing new is written.
    server.abort();
    let _ = server.await;

    clock.0.store(2_000, Ordering::Relaxed);
    scraper.scrape_pass().await;
    clock.0.store(3_000, Ordering::Relaxed);
    scraper.scrape_pass().await;

    assert_eq!(health.failures(), 2);
    assert_eq!(health.last_success_ns(), Some(1_000));
    assert_eq!(health.age_seconds(3_000), Some(2e-6));
    assert_eq!(
        count(&pool, "SELECT count(*) FROM metrics_raw"),
        HYPERVISOR_SAMPLES
    );

    drop(writer);
    drop(scraper);
    join.join().unwrap();
}

#[tokio::test]
async fn spawned_loop_ticks_on_its_own() {
    let (addr, server) = serve_body(HYPERVISOR_BODY).await;
    let dir = tempfile::tempdir().unwrap();
    let (writer, pool, join) = open_store(&dir);

    let config = ScrapeConfig {
        targets: vec![ScrapeTarget {
            name: "determinism-hypervisor".into(),
            url: format!("http://{addr}/metrics"),
        }],
        interval: Duration::from_millis(50),
    };
    let (handle, task) =
        obs_scrape::spawn(config, writer.clone(), Arc::new(obs_types::SystemClock));

    // Wait for at least two loop-driven scrapes to land.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if count(&pool, "SELECT count(*) FROM metrics_raw") >= 2 * HYPERVISOR_SAMPLES {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "scrape loop never ticked twice"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(handle
        .target("determinism-hypervisor")
        .unwrap()
        .last_success_ns()
        .is_some());

    // Await the aborted task so its Scraper (holding a WriterHandle clone)
    // is actually dropped — otherwise join() below blocks the runtime
    // thread the drop needs, and the writer thread never sees the channel
    // close.
    task.abort();
    let _ = task.await;
    server.abort();
    drop(writer);
    join.join().unwrap();
}
