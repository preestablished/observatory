#![forbid(unsafe_code)]
//! observatoryd: config load, store + writer + HTTP wiring, task
//! supervision, graceful shutdown. gRPC ingest mounts here with M1.

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

async fn run(config: observatoryd::Config) -> anyhow::Result<()> {
    let store_config = obs_store::StoreConfig::new(&config.storage.path);
    let store = obs_store::Store::open(&store_config).context("opening store")?;
    let db_path = store.path().to_path_buf();
    let _read_pool = store.read_pool();
    let (write_conn, _pool) = store.into_parts();
    let (writer, writer_join) = obs_store::spawn_writer(write_conn);
    info!(path = %db_path.display(), "store open (schema v1, WAL)");

    let metrics = Arc::new(obs_http::Metrics::new());

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

    let state = obs_http::AppState {
        db_path: db_path.clone(),
        metrics: Arc::clone(&metrics),
    };
    let listener = tokio::net::TcpListener::bind(&config.server.http_listen)
        .await
        .with_context(|| format!("binding http listener {}", config.server.http_listen))?;
    info!(listen = %config.server.http_listen, "http server up (/healthz, /metrics)");

    let shutdown = shutdown_signal();
    axum::serve(listener, obs_http::router(state))
        .with_graceful_shutdown(shutdown)
        .await
        .context("http server")?;

    info!("shutdown: http stopped; draining writer");
    gauge_task.abort();
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
