//! gRPC publisher: streams a deterministic sim over `PublishEvents` (or
//! `PublishEventsBulk` batches), honoring acks — after a disconnect it
//! resends from `acked + 1` (the producer rule of API.md §1). Because the
//! sim is seed-deterministic, "resume" regenerates the stream and skips
//! already-acked seqs.

use std::time::Duration;

use tokio_stream::StreamExt;

use obs_types::event_ingest_client::EventIngestClient;
use obs_types::{EventBatch, EventEnvelope};

use crate::sim::{Counts, Sim, SimConfig};

#[derive(Clone, Debug)]
pub struct PublishConfig {
    pub addr: String,
    /// Envelopes per second (token bucket); None = firehose.
    pub rate: Option<u64>,
    pub bulk: bool,
    /// Reconnect + resend from acked+1 on stream failure (loops until the
    /// whole stream is acked). Without it a failure is terminal.
    pub resume: bool,
}

#[derive(Debug, Default)]
pub struct PublishReport {
    pub sent: u64,
    pub acked_seq: u64,
    pub rejections: u64,
    pub reconnects: u64,
    pub counts: Counts,
}

pub async fn publish(
    sim_config: SimConfig,
    publish_config: PublishConfig,
) -> Result<PublishReport, Box<dyn std::error::Error>> {
    let mut report = PublishReport::default();
    let final_seq = sim_config.start_seq + sim_config.events - 1;
    let mut acked: u64 = sim_config.start_seq.saturating_sub(1);
    let mut stalled_attempts = 0u32;

    loop {
        let acked_before = acked;
        let attempt = if publish_config.bulk {
            publish_bulk(&sim_config, &publish_config, acked, &mut report).await
        } else {
            publish_stream(&sim_config, &publish_config, acked, &mut report).await
        };
        match attempt {
            Ok(high) => {
                acked = acked.max(high);
                if acked >= final_seq {
                    break;
                }
                // Rejected envelopes are never acked, so a stream whose
                // TAIL was rejected legitimately ends short of final_seq.
                // Two clean completions with no ack progress means the
                // remaining gap is rejections, not loss — stop rather
                // than resend the same rejected tail forever.
                if acked == acked_before {
                    stalled_attempts += 1;
                    if stalled_attempts >= 2 {
                        tracing_note(&format!(
                            "stopping at acked_seq {acked} (< {final_seq}): no ack progress \
                             across attempts — the unacked tail is rejected envelopes"
                        ));
                        break;
                    }
                } else {
                    stalled_attempts = 0;
                }
                // Stream closed clean but not fully acked (server ended
                // early); treat like a disconnect when resuming.
                if !publish_config.resume {
                    return Err(
                        format!("stream ended at acked_seq {acked}, expected {final_seq}").into(),
                    );
                }
            }
            Err(error) => {
                if !publish_config.resume {
                    return Err(error);
                }
                tracing_note(&format!(
                    "stream failed ({error}); reconnecting from {acked}"
                ));
            }
        }
        report.reconnects += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Counts come from a fresh drain of the deterministic sim.
    let mut sim = Sim::new(sim_config);
    for _ in sim.by_ref() {}
    report.counts = sim.counts().clone();
    report.acked_seq = acked;
    Ok(report)
}

fn tracing_note(message: &str) {
    eprintln!("obs-events-gen: {message}");
}

async fn connect(
    addr: &str,
    resume: bool,
) -> Result<EventIngestClient<tonic::transport::Channel>, Box<dyn std::error::Error>> {
    let mut delay = Duration::from_millis(100);
    let mut attempts = 0;
    loop {
        match EventIngestClient::connect(addr.to_owned()).await {
            Ok(client) => return Ok(client),
            Err(error) => {
                attempts += 1;
                if !resume && attempts >= 3 {
                    return Err(error.into());
                }
                if attempts >= 300 {
                    return Err(error.into());
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(2));
            }
        }
    }
}

async fn publish_stream(
    sim_config: &SimConfig,
    publish_config: &PublishConfig,
    resume_from: u64,
    report: &mut PublishReport,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut client = connect(&publish_config.addr, publish_config.resume).await?;
    let (tx, rx) = tokio::sync::mpsc::channel::<EventEnvelope>(256);

    let sender_config = sim_config.clone();
    let rate = publish_config.rate;
    let sender = tokio::spawn(async move {
        let mut sent = 0u64;
        // Token bucket in 10 ms windows: per-event sleeps are useless at
        // 10k/s (tokio timer granularity ~1 ms would cap the rate at
        // ~1k/s), so pace in chunks instead.
        let chunk = rate.map(|per_second| (per_second / 100).max(1));
        let mut window_sent = 0u64;
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
        for envelope in Sim::new(sender_config) {
            if envelope.seq <= resume_from {
                continue;
            }
            if let Some(chunk) = chunk {
                if window_sent >= chunk {
                    interval.tick().await;
                    window_sent = 0;
                }
                window_sent += 1;
            }
            if tx.send(envelope).await.is_err() {
                break;
            }
            sent += 1;
        }
        sent
    });

    let response = client
        .publish_events(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await?;
    let mut acks = response.into_inner();
    let mut high = resume_from;
    while let Some(ack) = acks.next().await {
        let ack = ack?;
        high = high.max(ack.acked_seq);
        report.rejections += ack.rejections.len() as u64;
    }
    if let Ok(sent) = sender.await {
        report.sent += sent;
    }
    Ok(high)
}

async fn publish_bulk(
    sim_config: &SimConfig,
    publish_config: &PublishConfig,
    resume_from: u64,
    report: &mut PublishReport,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut client = connect(&publish_config.addr, publish_config.resume).await?;
    let mut high = resume_from;
    let mut batch: Vec<EventEnvelope> = Vec::with_capacity(500);
    let mut sim = Sim::new(sim_config.clone()).peekable();
    while sim.peek().is_some() {
        batch.clear();
        for envelope in sim.by_ref() {
            if envelope.seq <= resume_from {
                continue;
            }
            batch.push(envelope);
            if batch.len() == 500 {
                break;
            }
        }
        if batch.is_empty() {
            break;
        }
        report.sent += batch.len() as u64;
        let ack = client
            .publish_events_bulk(EventBatch {
                events: batch.clone(),
            })
            .await?
            .into_inner();
        high = high.max(ack.acked_seq);
        report.rejections += ack.rejections.len() as u64;
    }
    Ok(high)
}
