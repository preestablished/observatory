# observatory

Web UI is server-rendered (axum + askama + htmx) with two small vendored JavaScript canvas modules

## State of the repo (2026-07-16)

Milestones **M0–M2** of the implementation plan are built and green in CI
(x86_64 + aarch64 legs, clippy `-D warnings`, the 100k double-replay
determinism gate, a 100k kill -9 chaos smoke, and a 30 s / 10k events/s
throughput smoke run on every commit):

- **M0** — SQLite (WAL) store with the full migration-v1 schema,
  single-writer task over a bounded channel, read-only pool, versioned
  TOML config, JSON logs, `/healthz` (fresh-open probe) + `/metrics`,
  graceful shutdown.
- **M1** — tonic `EventIngest` server (`:7470`): bidirectional
  `PublishEvents` with durable gap-tolerant acks, `PublishEventsBulk`,
  D3-tolerant validation, the full same-transaction projection set (runs,
  tree_nodes, score_points, checkpoints, findings, coverage_cells,
  replays, event-derived metric series), post-commit broadcast, and
  `obs-events-gen` — the seeded deterministic producer / load harness.
- **M2** — Prometheus scraper (`obs-scrape`, golden-fixture-tested),
  5s/1m/10m rollups with atomic high-water folds, derived search-health
  metrics, and the `score-curve` + `timeseries` REST endpoints with
  automatic grain selection.

M3+ (web UI, SSE, findings page, alert engine, tree/coverage/replay
views, retention) are not built yet. Live orchestrator ingest is gated on
exploration-orchestrator M5; all M1 acceptance ran against
`obs-events-gen` by design.

### Demo

```bash
cargo run -p observatoryd -- --config ./ci/dev-observatoryd.toml &
timeout 60 bash -c 'until curl -sf http://127.0.0.1:7471/healthz; do sleep 1; done'
cargo run -p obs-events-gen -- publish --addr http://127.0.0.1:7470 \
  --seed 7 --events 5000 --vocab catalog --run-id demo-7
sleep 12   # let the derive ticker mint samples
curl -s 'http://127.0.0.1:7471/api/v1/runs/demo-7/score-curve' | head -c 300
curl -s 'http://127.0.0.1:7471/api/v1/runs/demo-7/timeseries?metrics=search_best_score&step=auto'
```

### Pointers

- Event-contract reconciliation (decisions D1–D9, proto pin):
  `docs/event-contract-reconciliation-v1.md`
- control-plane proto pin + bump procedure: `docs/proto-pin.md`
- Acceptance evidence: `evidence/phase5-m1-m2/`
- Local builds need the sibling `../control-plane` checkout to contain
  the pinned commit (see `docs/proto-pin.md`).

### Naming note

ARCHITECTURE §1 calls the shared-types crate `obs-core`; this repo keeps
the Phase 0 skeleton's name `obs-types` (renaming would buy nothing).
The read pool is a hand-rolled fixed pool of read-only connections
rather than `r2d2` (sanctioned deviation; see `crates/obs-store/src/pool.rs`).
