//! `observatoryd.toml` (ARCHITECTURE §8): `version = 1` (other versions
//! rejected), unknown top-level keys warn but don't fail.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

pub const CONFIG_VERSION: i64 = 1;

const KNOWN_TOP_LEVEL_KEYS: [&str; 8] = [
    "version",
    "server",
    "storage",
    "scrape",
    "control_plane",
    "standalone",
    "alerts",
    "retention",
];

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("unsupported config version {found} (this build supports version {supported})")]
    UnsupportedVersion { found: i64, supported: i64 },
    #[error(
        "invalid duration {value:?} for {key}: expected e.g. \"5s\", \"10m\", \"24h\", \"14d\""
    )]
    InvalidDuration { key: &'static str, value: String },
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub version: i64,
    #[serde(default)]
    pub server: ServerConfig,
    pub storage: StorageConfig,
    #[serde(default)]
    pub scrape: ScrapeConfig,
    #[serde(default)]
    pub control_plane: Option<ControlPlaneConfig>,
    #[serde(default)]
    pub standalone: Option<StandaloneConfig>,
    #[serde(default)]
    pub alerts: Option<AlertsConfig>,
    #[serde(default)]
    pub retention: RetentionConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub grpc_listen: String,
    pub http_listen: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            grpc_listen: "0.0.0.0:7470".to_owned(),
            http_listen: "0.0.0.0:7471".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct StorageConfig {
    pub path: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct ScrapeConfig {
    /// Duration string, e.g. `"5s"`.
    pub interval: String,
    pub targets: Vec<ScrapeTarget>,
}

impl Default for ScrapeConfig {
    fn default() -> Self {
        Self {
            interval: "5s".to_owned(),
            targets: Vec::new(),
        }
    }
}

impl ScrapeConfig {
    pub fn interval_duration(&self) -> Result<Duration, ConfigError> {
        parse_duration(&self.interval).ok_or_else(|| ConfigError::InvalidDuration {
            key: "scrape.interval",
            value: self.interval.clone(),
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ScrapeTarget {
    /// Must match the `service` column used in series keys.
    pub name: String,
    pub url: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ControlPlaneConfig {
    pub grpc_url: String,
    #[serde(default)]
    pub auth_token_file: Option<PathBuf>,
}

/// Phase 5 standalone mode: local files stand in for the control-plane
/// experiment-config fetch (INTEGRATION §3).
#[derive(Clone, Debug, Deserialize)]
pub struct StandaloneConfig {
    #[serde(default)]
    pub experiment_json_path: Option<PathBuf>,
    #[serde(default)]
    pub feature_map_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AlertsConfig {
    pub rules_path: PathBuf,
}

/// Parsed and stored; the retention sweeper itself is M8.
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct RetentionConfig {
    pub raw_events: String,
    pub metrics_raw: String,
    pub rollup_5s: String,
    pub rollup_1m: String,
    pub rollup_10m: String,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            raw_events: "14d".to_owned(),
            metrics_raw: "24h".to_owned(),
            rollup_5s: "7d".to_owned(),
            rollup_1m: "30d".to_owned(),
            rollup_10m: "365d".to_owned(),
        }
    }
}

/// Parses `"<n><unit>"` with unit `s`/`m`/`h`/`d`.
pub fn parse_duration(value: &str) -> Option<Duration> {
    let value = value.trim();
    let split = value.len().checked_sub(1)?;
    let (number, unit) = value.split_at(split);
    let number: u64 = number.trim().parse().ok()?;
    let seconds = match unit {
        "s" => number,
        "m" => number.checked_mul(60)?,
        "h" => number.checked_mul(3_600)?,
        "d" => number.checked_mul(86_400)?,
        _ => return None,
    };
    Some(Duration::from_secs(seconds))
}

/// Loads and validates the config. Returns the config plus warnings for
/// unknown top-level keys (warn, don't fail — forward compatibility).
pub fn load_config(path: &Path) -> Result<(Config, Vec<String>), ConfigError> {
    let raw = std::fs::read_to_string(path)?;
    let table: toml::Table = raw.parse()?;

    let mut warnings = Vec::new();
    for key in table.keys() {
        if !KNOWN_TOP_LEVEL_KEYS.contains(&key.as_str()) {
            warnings.push(format!("unknown top-level config key {key:?} ignored"));
        }
    }

    let config: Config = toml::from_str(&raw)?;
    if config.version != CONFIG_VERSION {
        return Err(ConfigError::UnsupportedVersion {
            found: config.version,
            supported: CONFIG_VERSION,
        });
    }
    // Fail fast on malformed durations at load, not first use.
    config.scrape.interval_duration()?;
    Ok((config, warnings))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_config(contents: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("observatoryd.toml");
        std::fs::write(&path, contents).unwrap();
        (dir, path)
    }

    #[test]
    fn loads_minimal_config_with_defaults() {
        let (_dir, path) = write_config(
            r#"
version = 1
[storage]
path = "/tmp/observatory-test.db"
"#,
        );
        let (config, warnings) = load_config(&path).unwrap();
        assert!(warnings.is_empty());
        assert_eq!(config.server.grpc_listen, "0.0.0.0:7470");
        assert_eq!(config.server.http_listen, "0.0.0.0:7471");
        assert_eq!(
            config.scrape.interval_duration().unwrap(),
            Duration::from_secs(5)
        );
        assert_eq!(config.retention.metrics_raw, "24h");
    }

    #[test]
    fn rejects_other_versions() {
        let (_dir, path) = write_config(
            r#"
version = 2
[storage]
path = "/tmp/x.db"
"#,
        );
        match load_config(&path).unwrap_err() {
            ConfigError::UnsupportedVersion { found, supported } => {
                assert_eq!(found, 2);
                assert_eq!(supported, 1);
            }
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }

    #[test]
    fn warns_on_unknown_top_level_keys() {
        let (_dir, path) = write_config(
            r#"
version = 1
future_knob = true
[storage]
path = "/tmp/x.db"
"#,
        );
        let (_config, warnings) = load_config(&path).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("future_knob"));
    }

    #[test]
    fn parses_scrape_targets_and_standalone() {
        let (_dir, path) = write_config(
            r#"
version = 1
[storage]
path = "/tmp/x.db"
[scrape]
interval = "10s"
[[scrape.targets]]
name = "determinism-hypervisor"
url = "http://intel-box:9101/metrics"
[standalone]
experiment_json_path = "/etc/observatory/experiment.json"
feature_map_path = "/etc/observatory/feature-map.yaml"
"#,
        );
        let (config, _warnings) = load_config(&path).unwrap();
        assert_eq!(config.scrape.targets.len(), 1);
        assert_eq!(config.scrape.targets[0].name, "determinism-hypervisor");
        let standalone = config.standalone.unwrap();
        assert!(standalone.feature_map_path.is_some());
    }

    #[test]
    fn duration_parser_units() {
        assert_eq!(parse_duration("5s"), Some(Duration::from_secs(5)));
        assert_eq!(parse_duration("10m"), Some(Duration::from_secs(600)));
        assert_eq!(parse_duration("24h"), Some(Duration::from_secs(86_400)));
        assert_eq!(parse_duration("14d"), Some(Duration::from_secs(1_209_600)));
        assert_eq!(parse_duration("nope"), None);
        assert_eq!(parse_duration(""), None);
    }
}
