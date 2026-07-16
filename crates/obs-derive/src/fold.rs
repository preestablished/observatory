//! The pure fold core (ARCHITECTURE §3.2): samples (or lower-grain rows)
//! → rollup rows per bucket. No DB, no clock — property-testable.
//!
//! Input ordering is explicit: callers pass samples sorted by
//! `(series_id, ts_ns)` (the metrics_raw primary key — ties impossible),
//! and lower-grain rows sorted by `(series_id, bucket_ns)`. "first/last by
//! bucket edge" means ordered by that timestamp within the bucket.

use obs_types::RollupRow;

/// Truncates a timestamp to its bucket edge.
#[must_use]
pub fn bucket_of(ts_ns: i64, width_ns: i64) -> i64 {
    ts_ns - ts_ns.rem_euclid(width_ns)
}

/// Folds raw samples `(series_id, ts_ns, value)` — MUST be sorted by
/// `(series_id, ts_ns)` — into rollup rows of `width_ns` buckets.
#[must_use]
pub fn fold_samples(samples: &[(i64, i64, f64)], width_ns: i64) -> Vec<RollupRow> {
    let mut rows: Vec<RollupRow> = Vec::new();
    for &(series_id, ts_ns, value) in samples {
        let bucket_ns = bucket_of(ts_ns, width_ns);
        match rows.last_mut() {
            Some(row) if row.series_id == series_id && row.bucket_ns == bucket_ns => {
                row.n += 1;
                row.sum += value;
                row.min = row.min.min(value);
                row.max = row.max.max(value);
                row.last = value;
            }
            _ => rows.push(RollupRow {
                series_id,
                bucket_ns,
                n: 1,
                sum: value,
                min: value,
                max: value,
                first: value,
                last: value,
            }),
        }
    }
    rows
}

/// Promotes lower-grain rows — MUST be sorted by `(series_id, bucket_ns)`
/// — into coarser buckets of `width_ns`.
#[must_use]
pub fn fold_rows(rows: &[RollupRow], width_ns: i64) -> Vec<RollupRow> {
    let mut out: Vec<RollupRow> = Vec::new();
    for row in rows {
        let bucket_ns = bucket_of(row.bucket_ns, width_ns);
        match out.last_mut() {
            Some(coarse) if coarse.series_id == row.series_id && coarse.bucket_ns == bucket_ns => {
                coarse.n += row.n;
                coarse.sum += row.sum;
                coarse.min = coarse.min.min(row.min);
                coarse.max = coarse.max.max(row.max);
                coarse.last = row.last;
            }
            _ => out.push(RollupRow {
                series_id: row.series_id,
                bucket_ns,
                ..row.clone()
            }),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn bucket_truncation_handles_negatives() {
        assert_eq!(bucket_of(12, 5), 10);
        assert_eq!(bucket_of(10, 5), 10);
        assert_eq!(bucket_of(-1, 5), -5);
        assert_eq!(bucket_of(0, 5), 0);
    }

    proptest! {
        /// M2 acceptance verbatim: random sample sets -> fold -> equals
        /// direct aggregation of the raw samples for every grain and
        /// every aggregate column.
        #[test]
        fn fold_equals_direct_aggregation(
            mut samples in proptest::collection::vec(
                (0i64..4, 0i64..100_000, -1_000.0f64..1_000.0), 0..300),
            width in prop_oneof![Just(5_000i64), Just(60_000i64), Just(600_000i64)],
        ) {
            samples.sort_by_key(|s| (s.0, s.1));
            samples.dedup_by_key(|s| (s.0, s.1)); // PK uniqueness
            let rows = fold_samples(&samples, width);

            // Direct aggregation.
            let mut expected: std::collections::BTreeMap<(i64, i64), Vec<(i64, f64)>> =
                Default::default();
            for &(series, ts, value) in &samples {
                expected.entry((series, bucket_of(ts, width))).or_default().push((ts, value));
            }
            prop_assert_eq!(rows.len(), expected.len());
            for row in &rows {
                let bucket = &expected[&(row.series_id, row.bucket_ns)];
                prop_assert_eq!(row.n as usize, bucket.len());
                let sum: f64 = bucket.iter().map(|(_, v)| v).sum();
                prop_assert!((row.sum - sum).abs() <= 1e-9 * sum.abs().max(1.0));
                let min = bucket.iter().map(|(_, v)| *v).fold(f64::INFINITY, f64::min);
                let max = bucket.iter().map(|(_, v)| *v).fold(f64::NEG_INFINITY, f64::max);
                prop_assert_eq!(row.min, min);
                prop_assert_eq!(row.max, max);
                prop_assert_eq!(row.first, bucket.first().unwrap().1);
                prop_assert_eq!(row.last, bucket.last().unwrap().1);
            }
        }

        /// Promoting 5s rows into 1m equals folding the raw samples at 1m
        /// directly (sum/min/max/n/first/last all agree).
        #[test]
        fn promotion_equals_direct_fold(
            mut samples in proptest::collection::vec(
                (0i64..3, 0i64..1_000_000, -100.0f64..100.0), 0..300),
        ) {
            samples.sort_by_key(|s| (s.0, s.1));
            samples.dedup_by_key(|s| (s.0, s.1));
            let fine = fold_samples(&samples, 5_000);
            let promoted = fold_rows(&fine, 60_000);
            let direct = fold_samples(&samples, 60_000);
            prop_assert_eq!(promoted.len(), direct.len());
            for (a, b) in promoted.iter().zip(direct.iter()) {
                prop_assert_eq!(a.series_id, b.series_id);
                prop_assert_eq!(a.bucket_ns, b.bucket_ns);
                prop_assert_eq!(a.n, b.n);
                prop_assert!((a.sum - b.sum).abs() <= 1e-9 * b.sum.abs().max(1.0));
                prop_assert_eq!(a.min, b.min);
                prop_assert_eq!(a.max, b.max);
                prop_assert_eq!(a.first, b.first);
                prop_assert_eq!(a.last, b.last);
            }
        }
    }
}
