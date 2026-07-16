#![forbid(unsafe_code)]
//! Prometheus text-format scraper (M2 part 1): pure parser, series-key
//! interning, and the tokio scrape loop feeding `metric_series` /
//! `metrics_raw` through the single writer.

pub mod intern;
pub mod parser;
pub mod scraper;

pub use intern::SeriesInterner;
pub use parser::{parse_exposition, ParseOutput, ParsedSample};
pub use scraper::{spawn, ScrapeConfig, ScrapeHandle, ScrapeTarget, Scraper, TargetHealth};
