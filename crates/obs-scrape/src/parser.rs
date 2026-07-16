//! Prometheus text exposition parser. Pure module: no I/O, no tokio.
//!
//! `# TYPE` lines drive handling — `counter`/`gauge` produce one sample per
//! line; `histogram` lines arrive already suffixed (`_bucket`/`_sum`/
//! `_count`) and each stays its own series; `summary` and untyped lines are
//! stored as gauges (data is never dropped silently). Malformed lines are
//! counted and skipped — a bad line never fails the whole scrape.
//!
//! Label sets canonicalize to sorted-key JSON (ARCHITECTURE §3.1). The `le`
//! label value stays a verbatim string (`"0.5"`, `"+Inf"`) — it is never
//! parsed to a float for series identity.

use std::collections::{BTreeMap, HashMap};

/// One parsed sample line: metric name as written (histogram suffixes
/// included), canonical labels JSON, value.
#[derive(Clone, Debug, PartialEq)]
pub struct ParsedSample {
    pub metric: String,
    pub labels_json: String,
    pub value: f64,
}

/// Result of parsing one exposition body.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ParseOutput {
    pub samples: Vec<ParsedSample>,
    pub malformed_lines: usize,
}

/// Metric types a `# TYPE` comment can declare.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Summary,
    Untyped,
}

/// Parses a Prometheus text-format body. Never fails: malformed lines are
/// counted, comments and blank lines skipped.
#[must_use]
pub fn parse_exposition(body: &str) -> ParseOutput {
    let mut out = ParseOutput::default();
    let mut types: HashMap<String, MetricType> = HashMap::new();

    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(comment) = line.strip_prefix('#') {
            // `# TYPE <name> <kind>` is recorded; `# HELP` and free-form
            // comments are skipped. A garbled TYPE line is just a comment.
            let comment = comment.trim_start();
            if let Some(decl) = comment.strip_prefix("TYPE ") {
                let mut parts = decl.split_whitespace();
                if let (Some(name), Some(kind)) = (parts.next(), parts.next()) {
                    types.insert(name.to_owned(), metric_type(kind));
                }
            }
            continue;
        }

        let Some(sample) = parse_sample_line(line) else {
            out.malformed_lines += 1;
            continue;
        };
        match declared_type(&types, &sample.metric) {
            // Counter/gauge: one sample per line. Histogram: `_bucket`
            // (with verbatim `le`), `_sum`, `_count` each stay a separate
            // series. Summary and untyped: stored as gauges — identical
            // shape, kept explicit so the contract is visible here.
            MetricType::Counter
            | MetricType::Gauge
            | MetricType::Histogram
            | MetricType::Summary
            | MetricType::Untyped => out.samples.push(sample),
        }
    }
    out
}

/// Resolves the declared type of a sample line's metric: exact name first,
/// then the histogram/summary base name (`x_bucket`/`x_sum`/`x_count` are
/// declared as `# TYPE x histogram`).
fn declared_type(types: &HashMap<String, MetricType>, metric: &str) -> MetricType {
    if let Some(kind) = types.get(metric) {
        return *kind;
    }
    for suffix in ["_bucket", "_sum", "_count"] {
        if let Some(base) = metric.strip_suffix(suffix) {
            if let Some(kind) = types.get(base) {
                return *kind;
            }
        }
    }
    MetricType::Untyped
}

fn metric_type(kind: &str) -> MetricType {
    match kind {
        "counter" => MetricType::Counter,
        "gauge" => MetricType::Gauge,
        "histogram" => MetricType::Histogram,
        "summary" => MetricType::Summary,
        _ => MetricType::Untyped,
    }
}

/// `metric_name[{labels}] value [timestamp]`. Returns `None` on any
/// malformation. An exposition timestamp is accepted and ignored —
/// observatory mints its own scrape timestamp from the injectable clock.
fn parse_sample_line(line: &str) -> Option<ParsedSample> {
    let name_end = line
        .char_indices()
        .take_while(|&(i, c)| is_name_char(c, i == 0))
        .count();
    if name_end == 0 {
        return None;
    }
    let metric = &line[..name_end];
    let mut rest = &line[name_end..];

    let labels = if let Some(after_brace) = rest.strip_prefix('{') {
        let (labels, remainder) = parse_labels(after_brace)?;
        rest = remainder;
        labels
    } else {
        BTreeMap::new()
    };

    let mut parts = rest.split_whitespace();
    let value = parse_value(parts.next()?)?;
    match parts.next() {
        // Optional exposition timestamp (milliseconds); ignored.
        Some(ts) if ts.parse::<i64>().is_ok() => {
            if parts.next().is_some() {
                return None;
            }
        }
        Some(_) => return None,
        None => {}
    }

    Some(ParsedSample {
        metric: metric.to_owned(),
        labels_json: canonical_labels_json(&labels),
        value,
    })
}

fn is_name_char(c: char, first: bool) -> bool {
    c.is_ascii_alphabetic() || c == '_' || c == ':' || (!first && c.is_ascii_digit())
}

/// Parses `name="value",...}` (input starts after the opening brace) and
/// returns the label map plus the remainder after the closing brace.
/// Tolerates a trailing comma, per the classic Prometheus format.
fn parse_labels(mut s: &str) -> Option<(BTreeMap<String, String>, &str)> {
    let mut labels = BTreeMap::new();
    loop {
        s = s.trim_start();
        if let Some(rest) = s.strip_prefix('}') {
            return Some((labels, rest));
        }
        let eq = s.find('=')?;
        let name = s[..eq].trim();
        if name.is_empty()
            || !name
                .chars()
                .enumerate()
                .all(|(i, c)| is_name_char(c, i == 0))
        {
            return None;
        }
        let quoted = s[eq + 1..].trim_start().strip_prefix('"')?;
        let (value, rest) = parse_quoted(quoted)?;
        labels.insert(name.to_owned(), value);
        s = rest.trim_start();
        if let Some(rest) = s.strip_prefix(',') {
            s = rest;
        } else if !s.starts_with('}') {
            return None;
        }
    }
}

/// Consumes a quoted label value body (input starts after the opening
/// quote), handling the `\\`, `\"`, and `\n` escapes. An unrecognized
/// escape is kept literally rather than rejecting the line.
fn parse_quoted(s: &str) -> Option<(String, &str)> {
    let mut value = String::new();
    let mut chars = s.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '"' => return Some((value, &s[i + 1..])),
            '\\' => match chars.next() {
                Some((_, 'n')) => value.push('\n'),
                Some((_, '\\')) => value.push('\\'),
                Some((_, '"')) => value.push('"'),
                Some((_, other)) => {
                    value.push('\\');
                    value.push(other);
                }
                None => return None,
            },
            other => value.push(other),
        }
    }
    None
}

/// Rust's `f64` parser covers the exposition value grammar, including
/// `+Inf`/`-Inf`/`NaN` (case-insensitive).
fn parse_value(token: &str) -> Option<f64> {
    token.parse::<f64>().ok()
}

/// Canonical sorted-key JSON object; `{}` when unlabeled. Values are the
/// unescaped label strings (JSON escaping re-applied by serde), so `le`
/// stays the verbatim exposition string.
fn canonical_labels_json(labels: &BTreeMap<String, String>) -> String {
    serde_json::to_string(labels).expect("string map serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(line: &str) -> ParsedSample {
        let out = parse_exposition(line);
        assert_eq!(
            out.malformed_lines, 0,
            "line unexpectedly malformed: {line}"
        );
        assert_eq!(out.samples.len(), 1);
        out.samples.into_iter().next().unwrap()
    }

    #[test]
    fn escaped_newline_quote_and_backslash_in_label_values() {
        let s = one(r#"m{a="line1\nline2",b="say \"hi\"",c="back\\slash"} 1"#);
        assert_eq!(
            s.labels_json,
            r#"{"a":"line1\nline2","b":"say \"hi\"","c":"back\\slash"}"#
        );
        assert_eq!(s.value, 1.0);
    }

    #[test]
    fn unknown_escape_is_kept_literally() {
        let s = one(r#"m{a="odd\tescape"} 1"#);
        assert_eq!(s.labels_json, r#"{"a":"odd\\tescape"}"#);
    }

    #[test]
    fn le_label_stays_verbatim() {
        let s = one(r#"h_bucket{le="+Inf"} 4"#);
        assert_eq!(s.labels_json, r#"{"le":"+Inf"}"#);
        let s = one(r#"h_bucket{le="0.50"} 2"#);
        // Never normalized to "0.5" — string identity is the contract.
        assert_eq!(s.labels_json, r#"{"le":"0.50"}"#);
    }

    #[test]
    fn labels_canonicalize_to_sorted_keys() {
        let s = one(r#"m{zeta="1",alpha="2"} 3"#);
        assert_eq!(s.labels_json, r#"{"alpha":"2","zeta":"1"}"#);
    }

    #[test]
    fn trailing_comma_and_spaces_in_label_set() {
        let s = one(r#"m{ a="1" , b="2" , } 9"#);
        assert_eq!(s.labels_json, r#"{"a":"1","b":"2"}"#);
    }

    #[test]
    fn special_values_parse() {
        assert_eq!(one("m 1.5e3").value, 1500.0);
        assert_eq!(one("m +Inf").value, f64::INFINITY);
        assert_eq!(one("m -Inf").value, f64::NEG_INFINITY);
        assert!(one("m NaN").value.is_nan());
    }

    #[test]
    fn exposition_timestamp_is_ignored() {
        let s = one("m{} 4 1720000000123");
        assert_eq!(s.value, 4.0);
    }

    #[test]
    fn malformed_lines_counted_not_fatal() {
        let body = "good 1\nnot a metric\nbad{x=\"1\" 2\nworse{} \ngood 2\n";
        let out = parse_exposition(body);
        assert_eq!(out.malformed_lines, 3);
        assert_eq!(out.samples.len(), 2);
    }

    #[test]
    fn type_comments_are_recorded_and_summary_kept_as_gauge() {
        let body = "# TYPE s summary\ns{quantile=\"0.99\"} 0.2\ns_sum 4\ns_count 20\n";
        let out = parse_exposition(body);
        assert_eq!(out.malformed_lines, 0);
        assert_eq!(out.samples.len(), 3);
        assert_eq!(out.samples[0].labels_json, r#"{"quantile":"0.99"}"#);
    }
}
