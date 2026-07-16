#![forbid(unsafe_code)]
//! gRPC event ingest: validation (D3-tolerant), batching (500/50 ms),
//! durable gap-tolerant acks (D7), and the projection write path via the
//! single store writer.

pub mod ack;
pub mod batcher;
pub mod feature_map;
pub mod service;
pub mod validate;

pub use service::{server, IngestService};
pub use validate::validate;
