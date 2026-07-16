# 06 — `obs-events-gen` + M1 Acceptance (CI Determinism Gate)

## Goal

Build the synthetic producer / load harness — IMPLEMENTATION-PLAN §M1 calls
it out: "this tool is also the load harness and the demo data source for UI
work — build it well" — and use it to discharge the four M1 acceptance
bullets, including the repo's **CI determinism obligation** (100k-envelope
double-replay → byte-identical tables). M1 acceptance is designed against
this tool because live-orchestrator integration is out of scope until
orchestrator M5 exists (phase-5 cross-repo ordering).

## New crate: `crates/obs-events-gen` (lib + thin binary)

Seeded, deterministic search simulator emitting the v1 vocabulary over real
gRPC (client of the package-02 generated `EventIngestClient`) or to a file.

- **Crate shape**: lib + thin binary. `src/lib.rs` exposes the
  `sim`/`vocab_*` modules; `src/main.rs` stays a thin CLI over the lib.
  `obs-ingest` consumes the lib as a **dev-dependency** so
  `tests/determinism_replay.rs` can generate its 100k stream in-process
  (no shelling out to the binary in the gate).
- **Simulation model** (small, plausible, deterministic from `--seed` via a
  fixed PRNG e.g. `rand_chacha`): grows a tree with configurable
  branch/prune ratios; walks an (x, y, room) grid so `features` maps
  exercise the coverage projection; emits `node-added`/`node-pruned` as the
  firehose, `batch-completed` per simulated batch, `checkpoint` every ~30 s
  of sim time, `best-score-improved` on improvements, one
  `stall-detected`/`escalation-changed` phase, optional `goal-reached`
  ending, sprinkled `assertion-violated`/`reachability-hit`. `ts_logical` =
  expansion index; `seq` monotone from 1; `producer_id` from flag;
  `ts_wall_ns` = sim epoch + a deterministic per-event increment, strictly
  monotone per producer (NEVER real wall clock — the whole gate rides on
  the stream being byte-reproducible);
  `--run-id <string>` sets the envelope `run_id` (run identity — every
  emitted event is run-scoped; package 09's demo uses `--run-id demo-7`).
- **Vocabulary profiles** (decision D5): `--vocab catalog` (strict API.md
  §2.1 field shapes — the acceptance profile) and `--vocab orch-asbuilt`
  (field shapes of `orch-server/src/events.rs` today — proves D3
  tolerance).
- **Modes**:
  - `generate --out events.jsonl` (one JSON envelope per line — the
    recorded-stream format; deterministic bytes for a given seed/flags),
  - `publish --addr http://…:7470` from a file or live generation, `--rate
    N/s` (token bucket), `--bulk` (PublishEventsBulk batches), `--resume`
    (honors acks: resend from acked+1 after reconnect),
  - `verify` — after a publish run, count/checksum comparison against the
    server: expected event count per type + a checksum over the emitted
    stream vs `SELECT count(*)…`/dump via the REST or direct sqlite read
    (flag `--db <path>` acceptable for v1),
  - profiles: `--profile firehose|bursty|reconnect-storm` (testing-strategy
    §Load names these).
- **Chaos hooks**: `--kill-after N` published envelopes emits nothing —
  killing is the *test's* job (kill -9 the daemon, not the generator);
  generator supports clean reconnect+resume so the test loops.

## M1 acceptance runs (owner: IMPLEMENTATION-PLAN §M1)

1. **CI determinism gate** — integration test
   `crates/obs-ingest/tests/determinism_replay.rs`:
   - generate a 100k-envelope stream (fixed seed, via the `obs-events-gen`
     lib dev-dependency, in-memory or tempfile),
   - ingest into a fresh tempfile DB (FixedClock — D6), dump all tables
     (`VACUUM INTO` then `sqlite3 db .dump`, or iterate tables → canonical
     byte serialization in-test to avoid needing the sqlite3 CLI on CI).
     All dump queries `ORDER BY` primary key (rowid where no explicit PK)
     — byte-equality must never ride on SQLite's unordered row return,
   - ingest the SAME stream again into the SAME DB, dump again →
     **byte-identical**;
   - plus a second assertion: fresh DB #2, same stream, FixedClock → dump
     equals DB #1's first dump (cross-instance determinism);
   - plus a generator-tie assertion: the row count of **each individual**
     `service='event'` series in `metrics_raw` (assert on one named series,
     e.g. the `kept` series — one batch-completed event yields one sample
     PER series, five series total) equals the generator's emitted
     `batch-completed` count (proves the envelope-`ts_wall_ns` rule from
     package 05 — a Clock-minted ts would collapse these rows on the
     `(series_id, ts_ns)` PK under FixedClock).
   Wire into CI by REPLACING package 04's placeholder determinism-gate
   step with the canonical step — written once, HERE, and nowhere else:

   ```yaml
   determinism-gate:
     runs-on: ubuntu-latest
     steps:
       # …checkout + toolchain + pinned control-plane checkout, as in the main job…
       - name: determinism gate (100k double-replay, byte-identical)
         run: cargo test -p obs-ingest --release --test determinism_replay
   ```

   `--test determinism_replay` fails loudly if the target doesn't exist;
   never a `determinism_` name filter, which can match zero tests forever
   and pass vacuously. Runs on every commit thereafter (MAP.md
   obligation); keep runtime < ~2 min on CI (100k events, `--release`).
2. **Kill -9 / zero loss, zero duplicates**: script or `#[ignore]`-gated
   test `scripts/chaos-killnine.sh`: start `observatoryd` (tempdir config),
   `obs-events-gen publish --rate 10000 …` a 1M-event firehose, `kill -9`
   the daemon mid-flight (randomized instant), restart, generator
   reconnects + resumes from acks, then `obs-events-gen verify` proves
   count==1M distinct and checksums match. CI runs a reduced 100k variant
   — wired as the `chaos-smoke` job below, not just claimed; the full 1M
   run is local evidence → `evidence/phase5-m1-m2/m1-killnine.txt`.
3. **Sustained throughput**: 10k events/s for 10 min with ack p99 < 500 ms,
   bounded channel depth, flat memory — this is a Spark-hardware number.
   Run the full profile on the target box if reachable
   (`evidence/phase5-m1-m2/m1-throughput.txt` with hardware noted); CI gets
   a 30-second smoke at 10k/s asserting only "no error, depth bounded" —
   wired as the `throughput-smoke` job below. Report honestly which was
   run where; do not claim the 10-min number from a laptop run.
4. **Unknown/malformed handling**: generator flag `--inject-bad N` mixes in
   unknown event types + malformed payloads; verify unknown stored
   `unknown=1`, malformed produced per-seq Rejections and the stream
   survived (assert final acked_seq covers all valid envelopes).

## CI wiring landed by this package (`.github/workflows/ci.yaml`)

Acceptance claims these runs, so they exist as concrete jobs in this
package's diff — no silent claims:

- `determinism-gate` — `ubuntu-latest`; replaces package 04's placeholder
  step with `cargo test -p obs-ingest --release --test determinism_replay`
  (the canonical form above).
- `chaos-smoke` — `ubuntu-latest`;
  `bash scripts/chaos-killnine.sh 100000` (the script builds
  `observatoryd` + `obs-events-gen` with `--release`, kills -9
  mid-flight, restarts, and asserts count==100k distinct + checksum
  match).
- `throughput-smoke` — `ubuntu-latest`;
  `bash scripts/throughput-smoke.sh` (release build, 30 s at 10k/s
  against a local daemon, asserting only "no error, channel depth
  bounded"; the 10-min p99 number stays Spark-local evidence).

## Files

- `crates/obs-events-gen/src/{main.rs,lib.rs,sim.rs,vocab_catalog.rs,vocab_asbuilt.rs,publish.rs,verify.rs}`
  (lib + thin binary; `sim`/`vocab_*` live in the lib)
- `crates/obs-ingest/tests/determinism_replay.rs` (+ `obs-events-gen` as
  an obs-ingest dev-dependency)
- `scripts/chaos-killnine.sh`, `scripts/throughput-smoke.sh`
- `evidence/phase5-m1-m2/…` (committed evidence artifacts)
- `.github/workflows/ci.yaml` — replace the package-04 placeholder with
  the canonical determinism-gate step and add the `chaos-smoke` +
  `throughput-smoke` jobs (see "CI wiring" above)

## Acceptance (exact commands)

```bash
cargo run -p obs-events-gen -- generate --seed 42 --events 100000 --vocab catalog --out /tmp/stream.jsonl
cargo run -p obs-events-gen -- generate --seed 42 --events 100000 --vocab catalog --out /tmp/stream2.jsonl
cmp /tmp/stream.jsonl /tmp/stream2.jsonl                      # generator itself deterministic
cargo test -p obs-ingest --release --test determinism_replay  # THE gate
bash scripts/chaos-killnine.sh 1000000                         # local full run
cargo test --workspace
```

- Determinism gate green in CI on the merge commit — including the
  generator-tie assertion (per-series row count — the named `kept`
  series — == generator's `batch-completed` count).
- `chaos-smoke` and `throughput-smoke` jobs green in CI (the wired
  reduced variants — see "CI wiring" above).
- Evidence files present for kill-nine (full) and throughput, each naming
  hardware + exact command + numbers.
- Generator determinism (`cmp`) holds across `--vocab` profiles.
- **Mid-plan checkpoint** (M1 boundary): once M1 acceptance + the
  determinism gate are green in CI — push all touched repos (observatory,
  control-plane, exploration-orchestrator), close **any still-open** beads
  among 01–06 serially with `-r` evidence (each package's bead normally
  closes at its own green commit — this is the sweep, not the primary
  close), and write a partial handback note (which M1 bullets
  were discharged and by what, what remains for M2). Review adjudications
  from this checkpoint go to the plan directory's `10-review-log.md`.
  Package 09 may therefore effectively run twice: checkpoint here, final
  pass there.

## Failure guidance

- Dump mismatch triage: diff the two dumps — the offending table names the
  leak (see package 05's failure list: duplicate-row projections, payload
  re-serialization, wall clock, map ordering). `ingested_at_ns` differing
  means a real clock reached the writer despite FixedClock.
- Kill -9 losing events: acks must only be sent post-commit — check the
  oneshot ordering; losing *acks* is fine (producer resends, dedup eats
  it), losing *acked* events is the bug.
- Kill -9 duplicating events: `INSERT OR IGNORE` bypassed (a code path
  doing plain INSERT) or the unique index missing — check migration.
- Throughput short of 10k/s: profile the writer flush (500/50 ms), check
  WAL pragmas took effect (`PRAGMA journal_mode` returns `wal`), and that
  the gRPC decode path isn't re-parsing payload JSON twice.
