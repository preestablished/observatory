#![forbid(unsafe_code)]
//! Rollups (5s/1m/10m idempotent folds with high-water marks), derived
//! search-health metrics, and the query-time rate() used by the REST
//! layer. The fold core and rate math are pure; only the tickers touch
//! the store.

pub mod derived;
pub mod fold;
pub mod rate;
pub mod ticker;

pub use derived::DerivedTicker;
pub use fold::{bucket_of, fold_rows, fold_samples};
pub use rate::rate_points;
pub use ticker::RollupTicker;
