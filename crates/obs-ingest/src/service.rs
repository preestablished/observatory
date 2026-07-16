//! The tonic `EventIngest` server: bidirectional `PublishEvents` with
//! durable acks and unary `PublishEventsBulk` (atomic per batch).
//!
//! Backpressure: each stream worker awaits the writer's bounded channel
//! before reading its next message, so a full writer stops gRPC reads and
//! HTTP/2 flow control pushes back on producers — nothing buffers
//! unboundedly here.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use obs_store::{IngestMetrics, ReadPool, WriterHandle};
use obs_types::event_ingest_server::EventIngest;
use obs_types::{Clock, EventBatch, EventEnvelope, PublishAck, WriteBatch};

use crate::ack::{AckTracker, Identity};
use crate::batcher::{Pending, MAX_LINGER};
use crate::validate::validate;

#[derive(Clone)]
pub struct IngestService {
    inner: Arc<Inner>,
}

struct Inner {
    writer: WriterHandle,
    pool: ReadPool,
    clock: Arc<dyn Clock>,
    metrics: IngestMetrics,
    acks: AckTracker,
}

impl IngestService {
    pub fn new(
        writer: WriterHandle,
        pool: ReadPool,
        clock: Arc<dyn Clock>,
        metrics: IngestMetrics,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                writer,
                pool,
                clock,
                metrics,
                acks: AckTracker::default(),
            }),
        }
    }
}

impl Inner {
    fn identity_of(record: &obs_types::EventRecord) -> Identity {
        (record.run_id.clone(), record.source_service_stored.clone())
    }

    /// Commits pending records, advances high-waters, and builds the ack.
    /// `stream_identity` is the connection's last-seen identity — the ack
    /// scope (v1 producers are single-identity per session).
    async fn flush(
        &self,
        pending: &mut Pending,
        stream_identity: &mut Option<Identity>,
    ) -> Result<PublishAck, Status> {
        let (records, rejections) = pending.take();
        if let Some(last) = records.last() {
            *stream_identity = Some(Self::identity_of(last));
        }
        if !records.is_empty() {
            let started = Instant::now();
            let applied = self
                .writer
                .write(WriteBatch::Events(records))
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?;
            self.metrics
                .batch_flush_seconds
                .observe(started.elapsed().as_secs_f64());
            self.acks.advance(&applied.max_seq_by_identity);
        }
        let acked_seq = match stream_identity {
            Some(identity) => self
                .acks
                .high_water(&self.pool, identity)
                .await
                .map_err(|error| Status::internal(error.to_string()))?,
            None => 0,
        };
        Ok(PublishAck {
            acked_seq,
            rejections,
        })
    }

    fn validate_into(&self, envelope: EventEnvelope, pending: &mut Pending) {
        match validate(envelope, self.clock.now_ns()) {
            Ok(record) => pending.records.push(record),
            Err(rejection) => {
                self.metrics
                    .events_rejected_total
                    .with_label_values(&[rejection.reason.as_str()])
                    .inc();
                pending.rejections.push(rejection);
            }
        }
    }
}

type AckStream = Pin<Box<dyn Stream<Item = Result<PublishAck, Status>> + Send>>;

#[tonic::async_trait]
impl EventIngest for IngestService {
    type PublishEventsStream = AckStream;

    async fn publish_events(
        &self,
        request: Request<Streaming<EventEnvelope>>,
    ) -> Result<Response<Self::PublishEventsStream>, Status> {
        let mut inbound = request.into_inner();
        let (ack_tx, ack_rx) = mpsc::channel::<Result<PublishAck, Status>>(64);
        let service = Arc::clone(&self.inner);

        tokio::spawn(async move {
            let mut pending = Pending::default();
            let mut identity: Option<Identity> = None;
            loop {
                // Linger only while something is pending; otherwise wait
                // for traffic indefinitely.
                let next = if pending.has_work() {
                    tokio::time::timeout(MAX_LINGER, inbound.next()).await.ok()
                } else {
                    Some(inbound.next().await)
                };

                match next {
                    // Linger expired: flush what we have.
                    None => {
                        let ack = service.flush(&mut pending, &mut identity).await;
                        if send_ack(&ack_tx, ack).await.is_err() {
                            break;
                        }
                    }
                    Some(Some(Ok(envelope))) => {
                        service.validate_into(envelope, &mut pending);
                        if pending.is_full() {
                            let ack = service.flush(&mut pending, &mut identity).await;
                            if send_ack(&ack_tx, ack).await.is_err() {
                                break;
                            }
                        }
                    }
                    // Client closed the stream: final flush + final ack.
                    Some(None) => {
                        if pending.has_work() {
                            let ack = service.flush(&mut pending, &mut identity).await;
                            let _ = send_ack(&ack_tx, ack).await;
                        }
                        break;
                    }
                    Some(Some(Err(status))) => {
                        tracing::debug!(error = %status, "ingest stream error");
                        break;
                    }
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(ack_rx)) as AckStream
        ))
    }

    async fn publish_events_bulk(
        &self,
        request: Request<EventBatch>,
    ) -> Result<Response<PublishAck>, Status> {
        let batch = request.into_inner();
        let mut pending = Pending::default();
        for envelope in batch.events {
            self.inner.validate_into(envelope, &mut pending);
        }
        // One WriteBatch = one transaction slice: atomic per batch (a
        // failure rolls the whole batch back and surfaces as an error).
        let mut identity = None;
        let ack = self.inner.flush(&mut pending, &mut identity).await?;
        Ok(Response::new(ack))
    }
}

async fn send_ack(
    ack_tx: &mpsc::Sender<Result<PublishAck, Status>>,
    ack: Result<PublishAck, Status>,
) -> Result<(), ()> {
    // A storage error is delivered to the producer as a terminal stream
    // error (it should back off and reconnect); a send failure means the
    // client went away.
    let failed = ack.is_err();
    ack_tx.send(ack).await.map_err(|_| ())?;
    if failed {
        Err(())
    } else {
        Ok(())
    }
}

/// Builds the tonic service wrapper for `observatoryd` to mount.
pub fn server(
    service: IngestService,
) -> obs_types::event_ingest_server::EventIngestServer<IngestService> {
    obs_types::event_ingest_server::EventIngestServer::new(service)
}
