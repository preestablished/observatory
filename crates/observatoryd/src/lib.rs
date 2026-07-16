#![forbid(unsafe_code)]
//! Daemon wiring kept in a lib for testability: config parsing/validation
//! (versioned per MAP.md conventions) and the task-supervision helpers the
//! binary uses.

pub mod config;

pub use config::{load_config, Config, ConfigError};
