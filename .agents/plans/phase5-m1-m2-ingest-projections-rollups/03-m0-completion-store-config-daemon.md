# 03 — M0 Completion: Real Store, Config, Daemon

## Goal

Build the M0 foundation that the Phase 0 skeleton only named: `obs-store` as
a real SQLite (WAL) layer with migration v1 and a single-writer task,
`observatoryd` as an actual binary with version-checked TOML config, JSON
logs, `/healthz` + `/metrics`, and graceful shutdown. Owner accept-when
list: IMPLEMENTATION-PLAN §M0. Without this, M1's durability/idempotency
acceptance is meaningless (today's `obs-store` is an in-memory `Vec`).

## Workspace changes

`Cargo.toml` `[workspace.dependencies]` — add (all mainstream,
aarch64-clean per ARCHITECTURE §1): `tokio` (rt-multi-thread, macros, sync,
signal, time), `tonic` + `prost` + `tonic-prost` (versions matching
control-plane's determinism-proto: tonic 0.14.x / prost 0.14.x — mismatched
tonic majors across the path dep will not compile), `axum`, `rusqlite`
(feature `bundled`), `serde`/`serde_json`, `toml`, `tracing`,
`tracing-subscriber` (json), `prometheus`, `thiserror`, `anyhow`,
`tempfile` (dev), `criterion` (dev, optional). Read pool: `r2d2` +
`r2d2_sqlite` per ARCHITECTURE §1, or a hand-rolled fixed pool of read-only
connections if the adapter fights the rusqlite version (record deviation).

## `obs-types` (shared, no I/O)

- `RunId`, `NodeId` newtypes (String-backed), `SeriesKey`
  `{service, metric, labels_json}`.
- `EventRecord` — the validated, storage-ready form of an envelope:
  `{run_id, source_service_stored: String /* always the folded form
  "<service>/<producer_id>" per D9 (package 01) — no plain-service branch */,
  event_type,
  ts_logical, ts_wall_ns, seq, payload_version, payload: String (canonical
  JSON), unknown: bool, ingested_at_ns}`.
- `WriteBatch` enum (typed batch messages the writer consumes: events +
  projection deltas in package 05; metric samples in 07; rollup folds in
  08).
- `Clock` trait (`fn now_ns(&self) -> i64`) + `SystemClock` + `FixedClock`
  (test) — decision D6; every stored timestamp flows through it.
- Error types (`StoreError`, `IngestError`).

## `obs-store`

- `Store::open(path, &Config)` applying ARCHITECTURE §3 pragmas on the
  write conn (`journal_mode=WAL`, `synchronous=NORMAL`,
  `wal_autocheckpoint=4000`, `busy_timeout=5000`, `cache_size=-262144`,
  `mmap_size=268435456`); read conns `query_only=ON`.
- Migration v1: the **full** schema of ARCHITECTURE §3.1 verbatim —
  `schema_meta`, `events` (with `UNIQUE (run_id, source_service, seq)` +
  both indexes), `runs`, `tree_nodes`, `score_points`, `checkpoints`,
  `findings`, `metric_series`, `metrics_raw`, `rollup_5s/1m/10m` (identical
  columns), `coverage_cells`, `replays`, `alert_instances` — plus a
  `rollup_state` table (§3.2 names it for high-water marks:
  `(grain TEXT PRIMARY KEY, high_water_ns INTEGER NOT NULL)`).
  D3 tolerance deltas from package 01: in `tree_nodes`, columns the v1
  producer may omit (`snapshot_ref`, `guest_time_ns`) keep NOT NULL with
  explicit `DEFAULT ''`/`DEFAULT 0`; comment each default with "D3
  tolerance" so schema intent is auditable.
- Version handling: fresh DB writes `schema_meta.version = 1`; equal
  version is a no-op; **higher** on-disk version refuses to start with a
  clear error naming both versions.
- Single-writer task: one tokio task owns the write connection; input is a
  bounded `mpsc` (4096) of `WriteBatch`; one transaction per flush; a
  oneshot completion per batch (durable-ack hook for package 05). Reads go
  through the pool only.
- Micro-bench (`benches/` or a `--bench` bin): batched single-table inserts
  500/txn.

## `observatoryd`

- `src/main.rs` binary (keep the lib for wiring/testability).
- Config: `observatoryd.toml` per ARCHITECTURE §8 — `version = 1`
  (reject other versions), `[server] grpc_listen/http_listen` (defaults
  `0.0.0.0:7470` / `0.0.0.0:7471`), `[storage] path`, `[scrape]`
  interval + `[[scrape.targets]] {name, url}`, `[control_plane]`,
  `[standalone] experiment_json_path/feature_map_path` (optional table),
  `[alerts] rules_path`, `[retention]` (parsed + stored; sweeper is M8).
  Unknown top-level keys: warn, don't fail. `--config <path>` flag.
- Logging: `tracing-subscriber` JSON to stdout.
- HTTP: new crate `crates/obs-http` (axum `Router`) mounted by
  `observatoryd` with `/healthz` (200 `{"status":"ok","db":"ok",...}`, 503
  with `"db":"unavailable"` when the probe fails — the probe MUST open a
  **new read-only connection per request**, not reuse a pooled/long-lived
  handle: `chmod 000` does not revoke already-open SQLite FDs, so the
  owner doc's chmod test passes by construction only with a fresh-open
  probe; this is the design mechanism, not an implementation detail) and
  `/metrics`
  (prometheus text; register `obs_db_size_bytes`,
  `obs_ingest_channel_depth` gauge now, more counters land with 05/07).
- Graceful shutdown on SIGTERM/ctrl-c: stop accepting, drain writer channel,
  final WAL checkpoint, exit 0.

## Acceptance (exact commands)

```bash
cd /Users/punk1290/git/preestablished/observatory
cargo build --workspace
cargo test --workspace
# fresh start / restart / future-version refusal (integration tests in obs-store):
cargo test -p obs-store migration
# healthz reflects DB unavailability (IMPLEMENTATION-PLAN M0 chmod test — as an
# integration test using a tempdir DB made unreadable, or manually):
cargo run -p observatoryd -- --config ./ci/dev-observatoryd.toml &
# readiness-wait: cargo run races compilation — poll, don't curl blind:
timeout 60 bash -c 'until curl -sf http://127.0.0.1:7471/healthz; do sleep 1; done'
chmod 000 <db-path> && curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:7471/healthz  # expect 503 (holds because the probe opens a new conn per request)
kill %1   # clean up the daemon when done
# WriteBatch micro-bench (local evidence; target ≥50k inserts/s on NVMe):
cargo bench -p obs-store --bench write_batch
```

- `cargo test` green; migration tests cover fresh/no-op/refuse paths.
- Bench result recorded in `evidence/phase5-m1-m2/m0-writebatch-bench.txt`
  with the machine noted. CI runs a **reduced smoke**: this package lands
  the test itself — `crates/obs-store/tests/insert_rate_smoke.rs`
  (release-mode, asserts ≥5k inserts/s to catch order-of-magnitude
  regressions only) — while the `ci.yaml` step that runs it lands with
  package 04's CI edits (explicit handoff: this acceptance line is not
  discharged in CI until 04 wires the step). Report the real ≥50k number
  from local hardware honestly; if the local machine is not the
  Spark/NVMe target, say so in the evidence file.
- A committed sample config `ci/dev-observatoryd.toml` (tempdir-relative
  paths) used by tests/CI.

## Failure guidance

- rusqlite `bundled` compiles SQLite from source — if aarch64 CI (package
  04) chokes on build time, that's expected cost, not an error; do not swap
  to a system SQLite.
- If the writer bench misses 50k/s locally: check you're in release mode,
  batching 500/txn, WAL + `synchronous=NORMAL`; if still short on the
  actual target hardware, flag it in evidence — the 10k events/s M1 budget
  needs the 5× headroom (IMPLEMENTATION-PLAN risk table), so a real miss is
  a design conversation, not a threshold edit.
- If tonic/prost versions clash with determinism-proto's generated code:
  align observatory's workspace versions to control-plane's
  (`tonic 0.14.6`, `prost 0.14.4`, `tonic-prost 0.14.6`), never fork the
  proto crate.
