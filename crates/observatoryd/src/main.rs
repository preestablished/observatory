#![forbid(unsafe_code)]
//! observatoryd: config load, store + writer + ingest gRPC + HTTP wiring,
//! task supervision, graceful shutdown.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use tracing::{info, warn};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config_path = parse_config_flag()?;
    let (config, warnings) =
        observatoryd::load_config(&config_path).context("loading observatoryd.toml")?;
    for warning in warnings {
        warn!(warning, "config");
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;
    runtime.block_on(run(config))
}

fn parse_config_flag() -> anyhow::Result<PathBuf> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("--config") => args
            .next()
            .map(PathBuf::from)
            .context("--config requires a path argument"),
        Some("--help") | Some("-h") => {
            println!("usage: observatoryd --config <path/to/observatoryd.toml>");
            std::process::exit(0);
        }
        Some(other) => anyhow::bail!("unknown argument {other:?} (expected --config <path>)"),
        None => anyhow::bail!("missing required --config <path> flag"),
    }
}

/// Loads the `[standalone]` stand-ins for the control-plane
/// experiment-config fetch (INTEGRATION §3) once at startup.
fn load_standalone(config: &observatoryd::Config) -> obs_store::StandaloneData {
    let mut data = obs_store::StandaloneData::default();
    let Some(standalone) = &config.standalone else {
        return data;
    };
    if let Some(path) = &standalone.experiment_json_path {
        match std::fs::read_to_string(path) {
            Ok(contents) => data.experiment_json = Some(contents),
            Err(error) => {
                warn!(path = %path.display(), %error, "standalone experiment config unreadable")
            }
        }
    }
    if let Some(path) = &standalone.feature_map_path {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                data.grid_hint = obs_ingest::feature_map::parse_grid_hint(&contents);
                if data.grid_hint.is_none() {
                    info!(path = %path.display(), "feature map declares no grid hint; coverage projection stays disarmed");
                }
                data.feature_map_json = Some(contents);
            }
            Err(error) => {
                warn!(path = %path.display(), %error, "standalone feature map unreadable")
            }
        }
    }
    data
}

async fn run(config: observatoryd::Config) -> anyhow::Result<()> {
    let store_config = obs_store::StoreConfig::new(&config.storage.path);
    let store = obs_store::Store::open(&store_config).context("opening store")?;
    let db_path = store.path().to_path_buf();
    let (write_conn, read_pool) = store.into_parts();

    let ingest_metrics = obs_store::IngestMetrics::new();
    let projection_ctx = obs_store::ProjectionContext {
        standalone: load_standalone(&config),
        metrics: ingest_metrics.clone(),
    };
    let (writer, writer_join) = obs_store::spawn_writer(write_conn, projection_ctx);
    info!(path = %db_path.display(), "store open (schema v1, WAL)");

    let metrics = Arc::new(obs_http::Metrics::new());
    ingest_metrics.register(&metrics.registry);

    // Gauge refresher: DB size + writer channel depth every 5 s.
    let gauge_task = tokio::spawn({
        let metrics = Arc::clone(&metrics);
        let db_path = db_path.clone();
        let writer = writer.clone();
        async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tick.tick().await;
                if let Ok(meta) = std::fs::metadata(&db_path) {
                    metrics.db_size_bytes.set(meta.len() as i64);
                }
                metrics.ingest_channel_depth.set(writer.depth() as i64);
            }
        }
    });

    // Shared shutdown signal for both servers.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });
    let wait_shutdown = |mut rx: tokio::sync::watch::Receiver<bool>| async move {
        let _ = rx.wait_for(|stopped| *stopped).await;
    };

    // Ingest gRPC server (:7470).
    let clock: Arc<dyn obs_types::Clock> = Arc::new(obs_types::SystemClock);

    // Scrape loop (one task; degrades gracefully when targets are down —
    // failures are counted, nothing crashes).
    let scrape_task = if config.scrape.targets.is_empty() {
        None
    } else {
        let scrape_config = obs_scrape::ScrapeConfig {
            targets: config
                .scrape
                .targets
                .iter()
                .map(|target| obs_scrape::ScrapeTarget {
                    name: target.name.clone(),
                    url: target.url.clone(),
                })
                .collect(),
            interval: config
                .scrape
                .interval_duration()
                .context("scrape interval")?,
        };
        let (_scrape_handle, task) =
            obs_scrape::spawn(scrape_config, writer.clone(), Arc::clone(&clock));
        info!(targets = config.scrape.targets.len(), "scrape loop up");
        Some(task)
    };
    // Rollup ticker (5 s folds; hourly promotions per ARCHITECTURE §3.2)
    // and the 10 s derived-metrics ticker.
    let rollup_task = tokio::spawn(
        obs_derive::RollupTicker::new(read_pool.clone(), writer.clone(), Arc::clone(&clock)).run(
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(3_600),
        ),
    );
    let derived_task = tokio::spawn(
        obs_derive::DerivedTicker::new(read_pool.clone(), writer.clone(), Arc::clone(&clock))
            .run(std::time::Duration::from_secs(10)),
    );

    let ingest = obs_ingest::IngestService::new(
        writer.clone(),
        read_pool.clone(),
        Arc::clone(&clock),
        ingest_metrics.clone(),
    );
    let grpc_addr: std::net::SocketAddr = config
        .server
        .grpc_listen
        .parse()
        .with_context(|| format!("parsing grpc_listen {}", config.server.grpc_listen))?;
    let grpc_task = tokio::spawn({
        let rx = shutdown_rx.clone();
        async move {
            info!(listen = %grpc_addr, "ingest gRPC server up (EventIngest)");
            tonic::transport::Server::builder()
                .add_service(obs_ingest::server(ingest))
                .serve_with_shutdown(grpc_addr, wait_shutdown(rx))
                .await
        }
    });

    // HTTP server (:7471).
    let state = obs_http::AppState {
        db_path: db_path.clone(),
        metrics: Arc::clone(&metrics),
        pool: read_pool.clone(),
        clock: Arc::clone(&clock),
    };
    let listener = tokio::net::TcpListener::bind(&config.server.http_listen)
        .await
        .with_context(|| format!("binding http listener {}", config.server.http_listen))?;
    info!(listen = %config.server.http_listen, "http server up (/healthz, /metrics)");

    let mut http_shutdown = shutdown_rx.clone();
    axum::serve(listener, obs_http::router(state))
        .with_graceful_shutdown(async move {
            let _ = http_shutdown.wait_for(|stopped| *stopped).await;
        })
        .await
        .context("http server")?;

    info!("shutdown: http stopped; stopping gRPC and draining writer");
    match grpc_task.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "gRPC server exited with error"),
        Err(join_error) => warn!(%join_error, "gRPC server task panicked"),
    }
    // Abort AND await every task holding a WriterHandle clone — the
    // writer thread only exits once all handles drop, and join() below
    // must not race an aborted-but-undropped task.
    gauge_task.abort();
    rollup_task.abort();
    derived_task.abort();
    let _ = gauge_task.await;
    let _ = rollup_task.await;
    let _ = derived_task.await;
    if let Some(task) = scrape_task {
        task.abort();
        let _ = task.await;
    }
    drop(writer);
    tokio::task::spawn_blocking(move || writer_join.join())
        .await
        .ok();
    info!("shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => info!("shutdown: ctrl-c"),
            _ = sigterm.recv() => info!("shutdown: SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
        info!("shutdown: ctrl-c");
    }
}
