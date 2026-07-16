//! The tokio scrape loop: GET each static target, parse, batch all samples
//! of one pass under ONE clock timestamp, send to the single writer.
//!
//! Staleness tracking lives here as plain atomics behind [`TargetHealth`]
//! (no dependency on obs-http): per-target `last_success_ns` and a failure
//! counter, readable through the cloneable [`ScrapeHandle`] — M4's `absent`
//! alert rules consume this; only the tracking lands now. The daemon
//! exports them as `obs_scrape_failures_total{target}` /
//! `obs_scrape_age_seconds{target}` in a later package.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use obs_store::WriterHandle;
use obs_types::{Clock, MetricSample, WriteBatch};

use crate::intern::SeriesInterner;
use crate::parser;

/// One `[[scrape.targets]]` entry: `name` becomes the `service` column of
/// every series scraped from `url`.
#[derive(Clone, Debug)]
pub struct ScrapeTarget {
    pub name: String,
    pub url: String,
}

#[derive(Clone, Debug)]
pub struct ScrapeConfig {
    pub targets: Vec<ScrapeTarget>,
    /// `[scrape] interval`; the per-request timeout is interval / 2
    /// (INTEGRATION §2: 5 s interval, 2 s-ish timeout).
    pub interval: Duration,
}

impl ScrapeConfig {
    pub fn new(targets: Vec<ScrapeTarget>) -> Self {
        Self {
            targets,
            interval: Duration::from_secs(5),
        }
    }
}

/// `last_success_ns` sentinel: never succeeded.
const NEVER: i64 = i64::MIN;

/// Per-target staleness counters. Plain atomics — cheap to read from any
/// task without locking the scraper.
#[derive(Debug)]
pub struct TargetHealth {
    last_success_ns: AtomicI64,
    failures: AtomicU64,
    malformed_lines: AtomicU64,
}

impl TargetHealth {
    fn new() -> Self {
        Self {
            last_success_ns: AtomicI64::new(NEVER),
            failures: AtomicU64::new(0),
            malformed_lines: AtomicU64::new(0),
        }
    }

    /// Clock timestamp of the last successful scrape, `None` if never.
    #[must_use]
    pub fn last_success_ns(&self) -> Option<i64> {
        match self.last_success_ns.load(Ordering::Relaxed) {
            NEVER => None,
            ns => Some(ns),
        }
    }

    /// Total failed scrape attempts (`obs_scrape_failures_total{target}`).
    #[must_use]
    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    /// Total malformed lines skipped across all successful scrapes.
    #[must_use]
    pub fn malformed_lines(&self) -> u64 {
        self.malformed_lines.load(Ordering::Relaxed)
    }

    /// Staleness in seconds relative to `now_ns`
    /// (`obs_scrape_age_seconds{target}`); `None` if never scraped.
    #[must_use]
    pub fn age_seconds(&self, now_ns: i64) -> Option<f64> {
        self.last_success_ns()
            .map(|ns| (now_ns.saturating_sub(ns)) as f64 / 1e9)
    }
}

/// Cloneable read handle over every target's [`TargetHealth`]; later
/// packages (metrics export, `absent` alerting) read staleness through it.
#[derive(Clone)]
pub struct ScrapeHandle {
    targets: Arc<BTreeMap<String, Arc<TargetHealth>>>,
}

impl ScrapeHandle {
    #[must_use]
    pub fn target(&self, name: &str) -> Option<Arc<TargetHealth>> {
        self.targets.get(name).cloned()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &TargetHealth)> {
        self.targets
            .iter()
            .map(|(name, health)| (name.as_str(), health.as_ref()))
    }
}

struct TargetState {
    target: ScrapeTarget,
    health: Arc<TargetHealth>,
    interner: SeriesInterner,
}

/// The scrape driver. [`Scraper::run`] is the production loop; tests drive
/// [`Scraper::scrape_pass`] directly for determinism.
pub struct Scraper {
    targets: Vec<TargetState>,
    interval: Duration,
    client: reqwest::Client,
    writer: WriterHandle,
    clock: Arc<dyn Clock>,
    handle: ScrapeHandle,
}

impl Scraper {
    pub fn new(config: ScrapeConfig, writer: WriterHandle, clock: Arc<dyn Clock>) -> Self {
        let targets: Vec<TargetState> = config
            .targets
            .into_iter()
            .map(|target| TargetState {
                health: Arc::new(TargetHealth::new()),
                interner: SeriesInterner::new(target.name.clone()),
                target,
            })
            .collect();
        let handle = ScrapeHandle {
            targets: Arc::new(
                targets
                    .iter()
                    .map(|state| (state.target.name.clone(), Arc::clone(&state.health)))
                    .collect(),
            ),
        };
        let client = reqwest::Client::builder()
            .timeout(config.interval / 2)
            .build()
            .expect("reqwest client");
        Self {
            targets,
            interval: config.interval,
            client,
            writer,
            clock,
            handle,
        }
    }

    #[must_use]
    pub fn handle(&self) -> ScrapeHandle {
        self.handle.clone()
    }

    /// One scrape pass over every target: all samples of the pass share one
    /// timestamp from the clock and go out as one
    /// [`WriteBatch::MetricSamples`]. A target failure only bumps its
    /// counter — the pass (and the loop) always completes.
    pub async fn scrape_pass(&mut self) {
        let ts_ns = self.clock.now_ns();
        let mut samples = Vec::new();
        for state in &mut self.targets {
            let body = match fetch(&self.client, &state.target.url).await {
                Ok(body) => body,
                Err(error) => {
                    state.health.failures.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        target = %state.target.name,
                        url = %state.target.url,
                        error = %error,
                        "scrape failed"
                    );
                    continue;
                }
            };
            let parsed = parser::parse_exposition(&body);
            if parsed.malformed_lines > 0 {
                state
                    .health
                    .malformed_lines
                    .fetch_add(parsed.malformed_lines as u64, Ordering::Relaxed);
                tracing::warn!(
                    target = %state.target.name,
                    malformed = parsed.malformed_lines,
                    "malformed exposition lines skipped"
                );
            }
            for sample in parsed.samples {
                samples.push(MetricSample {
                    key: state.interner.key(&sample.metric, &sample.labels_json),
                    ts_ns,
                    value: sample.value,
                });
            }
            state.health.last_success_ns.store(ts_ns, Ordering::Relaxed);
        }
        if samples.is_empty() {
            return;
        }
        // Retention note: metrics_raw is 24 h retention in M8 — no sweeper
        // here; rows accumulate until that package lands.
        if let Err(error) = self.writer.write(WriteBatch::MetricSamples(samples)).await {
            tracing::error!(error = %error, "metric batch write failed");
        }
    }

    /// The production loop: tick every `interval`, scrape, repeat forever
    /// (the daemon aborts the task on shutdown).
    pub async fn run(mut self) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            self.scrape_pass().await;
        }
    }
}

async fn fetch(client: &reqwest::Client, url: &str) -> Result<String, reqwest::Error> {
    client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await
}

/// Spawns the scrape loop; returns the staleness handle and the task
/// handle (abort on shutdown).
pub fn spawn(
    config: ScrapeConfig,
    writer: WriterHandle,
    clock: Arc<dyn Clock>,
) -> (ScrapeHandle, tokio::task::JoinHandle<()>) {
    let scraper = Scraper::new(config, writer, clock);
    let handle = scraper.handle();
    (handle, tokio::spawn(scraper.run()))
}
