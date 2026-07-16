# 08 — M2 (part 2): `obs-derive` Rollups, Derived Search-Health Metrics, Timeseries/Score-Curve REST

## Goal

New crate `obs-derive`: the rollup ticker (5s/1m/10m idempotent folds with
high-water marks), the derived search-health metric computer, and the two
M2 REST endpoints (`timeseries`, `score-curve`) on `obs-http`. Owner
accept-when list: IMPLEMENTATION-PLAN §M2 (remaining bullets). Depends on
07 (`metrics_raw`) and 05 (`score_points`, event-derived series, runs
projection).

## Rollup mechanics (ARCHITECTURE §3.2 — implement verbatim)

- Fold core is **pure**: `fold(samples|lower-grain rows, bucket_width) →
  rollup rows {n, sum, min, max, first, last}` — property-testable without
  a DB.
- Ticker (5 s): fold `metrics_raw` rows newer than `rollup_state`'s 5s
  high-water into `rollup_5s` — **closed 5s buckets only** (rows falling in
  the still-open bucket wait for the next tick; folding an open bucket plus
  the additive ON-CONFLICT merge would double-count it on every tick); on
  the coarser cadence (hourly per spec — make it configurable so tests
  don't wait an hour), fold **closed** 5s buckets into `rollup_1m`, closed
  1m into `rollup_10m`. Advance each grain's high-water only for closed
  buckets, in the same transaction as the fold.
- Idempotency: `INSERT … ON CONFLICT DO UPDATE` merging
  n/sum/min/max and keeping first/last by bucket-edge — re-running after a
  crash converges (M2 acceptance bullet). All fold writes go through the
  single writer (`WriteBatch::RollupFold`).
- Counter resets: rate() is **query-time** (per §3.1 comment): compute from
  first/last within buckets; negative delta → treat segment as reset (use
  `last`, clamp ≥ 0). Lives in the query layer used by REST + (later)
  alerts; unit-test with a synthetic reset series (acceptance bullet: "no
  negative rates").

## Derived metrics (ARCHITECTURE §3.3 — every 10 s per running run, written as `service='derived'`, label `run_id`, through the normal rollup path)

Timestamps for derived/rollup rows are observatory-minted via the
injectable `Clock` (D6) — unlike package 05's event-derived `metrics_raw`
samples, which carry the envelope's `ts_wall_ns` (producer time).

Implement the full §3.3 table; sources split by what exists in this phase:

- Event-derived (live with 05/06): `search_expansions_per_sec` (rate of
  `runs.expansions` over 30 s), `search_novel_state_rate` (Σkept/(Σkept+Σdups)
  from `batch-completed` series — D3: absent fields make the metric
  unavailable, skip the tick rather than emit garbage),
  `search_frontier_size` (latest `checkpoint`, refined by scraped
  `eo_frontier_size` when present), `search_best_score` (gauge).
- Scrape-derived (fixture/stub-fed until sister services run):
  `search_dedup_hit_rate` (sc_* rates), `worker_slot_utilization`
  (`dh_slots_busy/dh_slots_total`), `snapshot_dedup_ratio`,
  `disk_burn_bytes_per_hour` (least-squares slope over 30 min of 5s
  rollups), `gpu_batch_latency_ms` p50/p99 (from `sc_gpu_batch_latency_seconds`
  histogram buckets).

## REST (extend `obs-http`; API.md §3.2 shapes exactly)

- `GET /api/v1/runs/{run_id}/score-curve?by=expansions|wall` →
  `{"points":[[x, score, node_id], …]}` straight from `score_points` (full
  curve; it is small).
- `GET /api/v1/runs/{run_id}/timeseries?metrics=m1,m2&from_ns=&to_ns=&step=auto`
  → `{"series":[{metric, labels, points:[[ts_ns, value],…]}]}`; metric
  names: derived names or `svc:<service>:<prom_name>[:p50|p99|rate]`; grain
  auto-selection picks raw/5s/1m/10m so each series ≤ 1500 points; errors
  as `{"error":{code,message}}`.
- (Run list/detail endpoints are M3 — do NOT build them now; score-curve
  and timeseries only, per IMPLEMENTATION-PLAN M2 scope.)

## Files

- `crates/obs-derive/src/{lib.rs,fold.rs,ticker.rs,derived.rs,rate.rs}`
- `crates/obs-store/src/…` (rollup queries, `rollup_state` access,
  `WriteBatch::RollupFold`/`DerivedSamples` arms)
- `crates/obs-http/src/{api.rs,timeseries.rs,score_curve.rs}`
- `crates/observatoryd` (ticker wiring: 5 s rollup / 10 s derived, both
  driven by `Clock` + tokio time so tests can pause/advance)

## Acceptance (exact commands + required tests)

```bash
cargo test -p obs-derive          # property + unit
cargo test -p obs-http
cargo test --workspace
```

- **Rollup correctness property test** (proptest, M2 acceptance verbatim):
  random sample sets → fold → query equals direct aggregation of the raw
  samples for every grain and every aggregate column.
- **Crash-mid-fold**: apply a partial fold (cut the batch at a random
  point, drop the high-water advance), re-run the ticker → tables converge
  to the same contents as an uninterrupted fold (dump-compare).
- **Counter reset**: synthetic counter with a mid-window reset → rate
  series has no negative points.
- **Derived vs hand-computed**: a recorded event+scrape fixture (an
  `obs-events-gen` stream + fixture scrape bodies replayed on a pinned
  FixedClock schedule) → each §3.3 metric equals values hand-computed in
  the fixture's comment block. Hand-write the expected numbers; do not
  generate them with the code under test.
- **≤1500 points**: timeseries responses across raw/5s/1m/10m windows stay
  ≤ 1500 points per series (test windows sized to force each grain).
- End-to-end: daemon up (dev config) →
  `curl -sf 'http://127.0.0.1:7471/api/v1/runs/<run>/score-curve'` and a
  `timeseries` query return §3.2-shaped JSON for a run previously ingested
  by `obs-events-gen`.

## Failure guidance

- Property-test failures on first/last: "first/last by bucket edge" means
  ordered by sample timestamp within the bucket, ties broken by insertion
  order — make the fold input ordering explicit (sort by `(ts_ns, rowid)`),
  never rely on query row order.
- Convergence failures after crash-mid-fold: the high-water advance and the
  fold rows must be in one transaction; if they are and it still diverges,
  a fold is double-merging open buckets — only closed buckets may promote
  to coarser grains.
- If the 10 s derived tick needs data the fixtures don't provide (absent
  scrape series), the metric must be **absent**, not zero — zero is a lie
  the M4 alert engine would act on.
- Timeseries endpoint over raw data slow: check you query `metrics_raw`
  only when the window fits under 1500 raw points; grain selection happens
  before the query, not after.
