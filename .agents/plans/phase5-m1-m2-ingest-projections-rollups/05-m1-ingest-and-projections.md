# 05 — M1: gRPC Ingest, Validation, Full Projection Set, Post-Commit Broadcast

## Goal

`obs-ingest` becomes the real tonic `EventIngest` server (`:7470`):
`PublishEvents` bidirectional stream with durable acks and
`PublishEventsBulk`, envelope validation against the v1 vocabulary, the
full projection set applied in the same transaction as the raw insert, and
a post-commit broadcast channel. Owner accept-when list:
IMPLEMENTATION-PLAN §M1 (acceptance is *exercised* in package 06 via
`obs-events-gen`; this package lands the machinery plus its unit/
integration tests). Depends on packages 02 (generated types) and 03
(store). Decisions D3/D4/D6/D7/D8/D9 from package 01 are binding here.

## `obs-ingest` design

### Validation (per envelope, ARCHITECTURE §2 + API.md §1 + D3)

Reject → per-seq `Rejection {seq, reason}` in the next ack, event NOT
stored, stream stays open:

- `envelope_version != 1` → `"unsupported envelope_version"`
- `payload_json` not parseable as a JSON **object** → `"payload not a JSON object"`
- `payload_json` > 65536 bytes → `"payload too large"`
- `run_id` empty for a run-scoped source (v1: all orchestrator events) →
  `"missing run_id"` (producers not run-scoped use `""` per API.md — accept
  empty for non-orchestrator sources; this cause is a contract addition
  recorded in API.md §1's rejection causes per package 01/D3)

Never rejected (D3): unknown `event_type` (store with `unknown = 1`),
unknown/`UNSPECIFIED` `source_service` (store enum number as text, flag
unknown), missing/renamed payload fields, unknown `payload_version`
(store; projections use best-effort v1 rules).

Canonicalization: `source_service` enum → lowercase-hyphen text
(`exploration-orchestrator`, …); the stored string is ALWAYS the folded
form `"<service>/<producer_id>"` per decision **D9** (package 01
addendum) — the orchestrator always sets `producer_id`, so the
plain-service branch is dead; one rule, no conditionals. Source-scoped
projection logic (the orchestrator-only `runs.expansions` update, etc.)
keys off the decoded `SourceService` enum at ingest time, in-transaction
— never off the stored string; stored-string consumers must prefix-match
(`'<service>/%'`). Payload stored as the exact
received bytes if already-canonical UTF-8 JSON object; do not re-serialize
(byte determinism of the `events` table must not depend on serde map
ordering).

### Pipeline (ARCHITECTURE §2)

- Per-stream decode → validate → append to a per-connection pending list.
- Batcher: flush at 500 events or 50 ms, whichever first, into one
  `WriteBatch::Events` message to the store writer.
- Writer applies, in ONE transaction: `INSERT OR IGNORE INTO events` (the
  `UNIQUE(run_id, source_service, seq)` constraint is the idempotency
  seam), then projections (below) **only for rows actually inserted**
  (use `changes()`/`RETURNING` to skip duplicate rows' projections — this
  is what makes double-replay byte-identical).
- Ack after commit (oneshot from writer): `acked_seq` = per-identity
  monotone high-water of committed-in-stream-order seqs (D7 — NOT a
  contiguity scan; duplicates of already-seen seqs still advance nothing
  but are acked as covered). `PublishEventsBulk`: atomic per batch — all
  rows in one transaction, single ack.
- Backpressure: bounded writer channel; when full, stop reading from the
  gRPC streams (natural HTTP/2 flow control) — never buffer unboundedly.
- Restart/resume: on reconnect the server replies with the identity's
  current high-water (computed from `events` via
  `SELECT max(seq) … GROUP BY` semantics at first-touch, cached after);
  producers resend from `acked_seq + 1`; dupes vanish via
  `INSERT OR IGNORE`.

### Projections (same-transaction; ARCHITECTURE §2 table + D3/D4/D8)

| Event | Writes |
|---|---|
| any | `runs` upsert: `first_seen_ns`/`last_event_ns` (from `ts_wall_ns`), `expansions = max(ts_logical)` for orchestrator source, `nodes_added`/`nodes_pruned` counters; first event for unknown `run_id` → `status='running'` + standalone experiment/feature-map load (see below) |
| `node-added` | insert `tree_nodes` (D3 defaults for absent fields; `features` → `features_json`); `runs.best_score` if higher; `coverage_cells` upsert when a grid hint is loaded and the features map covers `x`/`y`/`room` — else increment `obs_coverage_skipped_total` |
| `node-pruned` | D4: `node_id` present → `tree_nodes.pruned=1, prune_reason`; absent → counter-only |
| `best-score-improved` | `runs.best_score`, `runs.best_node_id`; insert `score_points` (`expansion_idx` from payload or fallback `ts_logical`) |
| `stall-detected` / `goal-reached` / `assertion-violated` / `reachability-hit` | insert `findings` (kind = event_type, severity per API.md §2.4, one-line summary; D8 undecoded-relay summary when fields are missing); `goal-reached` also sets `runs.status='goal_reached'`, `goal_reached=1` |
| `escalation-changed` | in-memory escalation gauge per run (exported metric; durable trail = events) |
| `batch-completed` | synthetic `metrics_raw` series (`service='event'`): kept/dups/regressions/failed_jobs/batch_wall_ms — fields present-only (D3). Sample `ts_ns` = the ENVELOPE's `ts_wall_ns` (producer time, generator-deterministic) — NOT the injectable `Clock`: under the FixedClock determinism gate a Clock-minted ts would collide every sample on the `(series_id, ts_ns)` PRIMARY KEY. Only observatory-minted ticks (package 08 derive/rollup) use the `Clock`. The event-derived sample insert is `INSERT OR IGNORE` (like the other projections): a nonconforming producer sending equal/zero `ts_wall_ns` must drop the duplicate sample, never abort the write transaction |
| `checkpoint` | insert `checkpoints` (D3 defaults for absent columns) |
| `replay-job-progress` / `-completed` / `replay-artifact-registered` | upsert `replays` (catalog-defined Phase 7 producers; cheap forward compat) |

Standalone experiment-config: on first event of a run, if
`[standalone]` paths are configured, load `experiment_json_path` →
`runs.experiment_json` and `feature_map_path` → `runs.feature_map_json`;
parse the feature map for the first `discretize: {kind: grid, …}` hint
(reference-workload API.md §1 shape: `x`, `y`, `room`, `cell_w`, `cell_h`)
to arm the coverage projection. No control-plane client in this milestone
(the fetch path is M3's `GetExperiment` stub work).

### Post-commit broadcast

`tokio::sync::broadcast<Arc<CommittedBatch>>` published by the writer after
each commit: the inserted `EventRecord`s + affected run ids. Consumers in
this phase: metrics (`obs_events_ingested_total`), tests, and package 08's
derive ticker (frontier gauge). SSE hub / obs-tree / obs-alert attach in
M3/M4 — the channel type and payload must already carry what they need
(record ids for `Last-Event-ID` replay: include `events.rowid`).

### Wiring

`observatoryd` mounts the tonic server on `[server].grpc_listen`, sharing
the writer handle and `Clock` (D6). Metrics: `obs_events_ingested_total`,
`obs_events_rejected_total{reason}`, `obs_events_unknown_type_total`,
`obs_projection_partial_total{event_type}`, `obs_ingest_batch_flush_seconds`
(histogram), `obs_ingest_channel_depth`, `obs_coverage_skipped_total`.

## Files

- `crates/obs-ingest/src/{lib.rs,service.rs,validate.rs,batcher.rs,ack.rs}`
- `crates/obs-store/src/{projections.rs,writer.rs}` (+ queries used by acks)
- `crates/obs-types/src/…` (catalog: the ten v1 event-type names + severity
  map as consts; `CommittedBatch`)
- `crates/observatoryd/src/…` (wiring)
- feature-map grid-hint parse: `crates/obs-ingest/src/feature_map.rs` (kept
  deliberately minimal; full validation/metering is M6's `obs-coverage` —
  don't build that crate now)

## Acceptance (unit/integration level; load-level in package 06)

```bash
cargo test -p obs-ingest
cargo test -p obs-store
cargo test --workspace
```

Required tests (in-proc tonic client + tempfile DB per the testing
strategy):

- Round-trip: stream N envelopes → ack advances → tables populated per the
  projection table (assert rows, not just counts).
- Idempotency: resend an overlapping seq range → zero new rows, projections
  unchanged (dump-compare before/after).
- Rejection: malformed payload JSON → per-seq `Rejection`, stream survives,
  subsequent valid events land (IMPLEMENTATION-PLAN M1 bullet 4).
- Unknown event_type → stored `unknown=1`, no projection, no rejection.
- Orch-asbuilt-shaped payloads (D5 fixtures, hand-written from
  `events.rs` builders) → accepted, partial projections,
  `obs_projection_partial_total` incremented.
- `node-pruned` without `node_id` (D4) → counter bumped, no tree_nodes touch.
- Bulk: atomicity (inject a mid-batch constraint failure in a test hook →
  whole batch absent).
- Ack-after-restart: write events, drop server, reopen store, new
  connection's first ack reports the persisted high-water.
- Coverage: synthetic feature map with a grid hint → `coverage_cells`
  upserts match hand-computed `floor(x/cell_w)`, `floor(y/cell_h)`, per-room
  keying; missing feature → skip + metric.
- Replays forward-compat: one fixture envelope each for
  `replay-job-progress`, `replay-job-completed`,
  `replay-artifact-registered` → the expected `replays` row upserted
  (fields per the catalog; absent fields D3-defaulted).

## Failure guidance

- If byte-identical double-replay (package 06) fails: the usual culprits
  are (1) projections running for duplicate rows — gate on inserted-row
  detection; (2) re-serialized payload JSON — store received bytes; (3)
  wall-clock leaking past the `Clock` trait — grep for
  `SystemTime::now`/`Instant::now` in obs-store/obs-ingest, only the Clock
  impls may call them; (4) HashMap iteration order feeding SQL — use
  BTreeMap or sort before writes.
- If ack p99 blows up under batching: check flush timer (50 ms) isn't reset
  per event, and that acks piggyback per flush, not per envelope.
- If tonic streaming + backpressure deadlocks: never `await` the writer
  send while holding the stream's next-message future in `select!` without
  a branch for outbound acks.
