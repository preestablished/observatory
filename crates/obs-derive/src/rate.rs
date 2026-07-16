//! Query-time rate() for counter series (ARCHITECTURE §3.1 comment:
//! counters are stored as raw samples; rate is computed from first/last
//! within buckets, counter resets handled — negative delta means the
//! counter restarted, so the segment contributes the post-reset value,
//! clamped ≥ 0).

/// Per-bucket rate from `(bucket_ns, first, last)` rows ordered by
/// `bucket_ns`. The delta spans from the previous bucket's `last` to this
/// bucket's `last` (falling back to this bucket's `first` for the first
/// bucket), divided by the bucket width in seconds.
#[must_use]
pub fn rate_points(rows: &[(i64, f64, f64)], width_ns: i64) -> Vec<(i64, f64)> {
    let width_s = width_ns as f64 / 1e9;
    let mut out = Vec::with_capacity(rows.len());
    let mut prev_last: Option<f64> = None;
    for &(bucket_ns, first, last) in rows {
        let base = prev_last.unwrap_or(first);
        let delta = if last >= base {
            last - base
        } else {
            // Counter reset inside the window: count what accumulated
            // after the reset; never a negative rate.
            last.max(0.0)
        };
        out.push((bucket_ns, delta / width_s));
        prev_last = Some(last);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steady_counter_yields_constant_rate() {
        // 5 s buckets, counter climbing 50 per bucket.
        let rows = vec![
            (0, 0.0, 50.0),
            (5_000_000_000, 60.0, 100.0),
            (10_000_000_000, 110.0, 150.0),
        ];
        let rates = rate_points(&rows, 5_000_000_000);
        assert_eq!(rates[0], (0, 10.0));
        assert_eq!(rates[1], (5_000_000_000, 10.0));
        assert_eq!(rates[2], (10_000_000_000, 10.0));
    }

    #[test]
    fn counter_reset_never_goes_negative() {
        let rows = vec![
            (0, 0.0, 1_000.0),
            // Process restarted: counter fell back near zero.
            (5_000_000_000, 3.0, 40.0),
            (10_000_000_000, 45.0, 90.0),
        ];
        let rates = rate_points(&rows, 5_000_000_000);
        assert!(rates.iter().all(|(_, rate)| *rate >= 0.0), "{rates:?}");
        assert_eq!(rates[1].1, 8.0); // post-reset accumulation: 40 / 5s
        assert_eq!(rates[2].1, 10.0);
    }
}
