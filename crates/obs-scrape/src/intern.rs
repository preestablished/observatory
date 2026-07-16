//! Series-key interning: `(service, metric, labels_json)` → [`SeriesKey`].
//! `service` is the target name from config — it is part of series identity
//! so two targets exposing the same metric never collide in `metrics_raw`.

use std::collections::HashMap;

use obs_types::SeriesKey;

/// Per-target lookup cache; one interner per scrape target keeps the hot
/// path free of repeated string assembly across passes.
pub struct SeriesInterner {
    service: String,
    cache: HashMap<(String, String), SeriesKey>,
}

impl SeriesInterner {
    pub fn new(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            cache: HashMap::new(),
        }
    }

    #[must_use]
    pub fn service(&self) -> &str {
        &self.service
    }

    /// Returns the series key for `(self.service, metric, labels_json)`,
    /// constructing and caching it on first sight.
    pub fn key(&mut self, metric: &str, labels_json: &str) -> SeriesKey {
        let service = &self.service;
        self.cache
            .entry((metric.to_owned(), labels_json.to_owned()))
            .or_insert_with(|| SeriesKey::new(service.clone(), metric, labels_json))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_is_part_of_identity() {
        let mut a = SeriesInterner::new("determinism-hypervisor");
        let mut b = SeriesInterner::new("snapshot-store");
        let ka = a.key("up", "{}");
        let kb = b.key("up", "{}");
        assert_ne!(ka, kb);
        assert_eq!(ka, a.key("up", "{}"));
    }
}
