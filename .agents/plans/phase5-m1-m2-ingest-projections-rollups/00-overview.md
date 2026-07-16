# Plan: Phase 5 M1–M2 — Event Ingest, Projections, Scraper, Rollups

Plan for the observatory repo's Phase 5 M1 (event ingest + projections) and
M2 (scraper, rollups, derived metrics), plus the M0 residue this planning
pass discovered. Written for a coding agent with no prior conversation
context, working in `/Users/punk1290/git/preestablished/observatory` on
branch `main`.

## Normative sources (precedence order for conflicts)

1. `~/.agents/projects/determinism/docs/observatory/IMPLEMENTATION-PLAN.md`
   §M0–§M2 + §Testing strategy — the accept-when lists this plan must satisfy.
   Milestone scope is only restated as headlines here; that file is the owner.
2. `~/.agents/projects/determinism/docs/observatory/API.md` — §1 envelope
   proto, §2.1 the canonical v1 event vocabulary (observatory OWNS the event
   envelope + event-type catalog per MAP.md's contract-ownership table),
   §2.4 severity mapping, §3 REST shapes.
3. `~/.agents/projects/determinism/docs/observatory/ARCHITECTURE.md` — crate
   layout (§1), ingest pipeline + projection rules (§2), schema v1 (§3.1),
   rollup mechanics (§3.2), derived metrics (§3.3), coverage derivation (§6),
   config (§8).
4. `~/.agents/projects/determinism/docs/observatory/INTEGRATION.md` — scraped
   metric-name contract, experiment-config fetch/standalone rules.
5. `~/.agents/projects/determinism/docs/reference-workload/API.md` §1 — the
   feature-map `grid` discretize schema (owned by reference-workload; the
   coverage projection parses it, never a private dialect).
6. `~/.agents/projects/determinism/phases/phase-5-closed-loop.md` — the
   observatory M1→M4 chain runs parallel to the orchestrator's M1→M7; the
   live orchestrator stream arrives only with orchestrator M5.

## Scope

In scope: reconciliation of the event-envelope contract (open bead
`exploration-orchestrator-75z` names observatory M1 ingest design as owner),
`observatory.proto` authoring + landing in control-plane (cross-repo), M0
completion (real SQLite store, daemon, healthz/metrics — see "Current state"
below: the repo is a Phase 0 skeleton, M0 is largely unbuilt, not just
missing CI legs), CI hardening (aarch64 + clippy), M1 ingest + full
projection set + post-commit broadcast + `obs-events-gen`, and M2 scraper +
rollups + derived metrics + timeseries/score-curve REST.

Out of scope (do not build any of it):

- **Integration with the LIVE orchestrator event stream.** Explicitly gated
  on exploration-orchestrator M5 execution (phase-5-closed-loop.md: "M5 emits
  the canonical event stream; integrate as soon as both sides exist"). All M1
  acceptance runs against the synthetic producer `obs-events-gen`.
- Editing exploration-orchestrator source. The reconciliation notice goes
  back as a docs-only `.agents/requests/` entry there (package 01) — zero
  code changes in that repo.
- M3/M4: web UI, SSE endpoints, findings page, alert engine. (The M1
  post-commit broadcast channel is built; its UI/alert consumers are not.)
- M5–M8: tree index/LOD, coverage UI, replay browser, retention sweeper.
- Promotion of `determinism.observatory.v1` to a frozen proto family — it
  lands/stays **vdev** per control-plane's freeze policy; promotion is a
  later, owner-signaled step via their playbook.

## Current state (verified 2026-07-15)

Repo: 2 commits (`85df3a9` initial, `bf98d46` "Phase 0 skeleton"), clean,
pushed. Workspace `crates/{obs-types,obs-store,obs-ingest,observatoryd}` —
all lib-only stubs:

- `obs-types` re-exports `determinism_proto::observatory::v1::EventEnvelope`
  (feature `observatory`).
- `obs-store` is an in-memory `Vec<EventEnvelope>` — **no SQLite, no schema,
  no writer task**. M0's store does not exist yet.
- `obs-ingest` is a 3-check `validate_event` function; no tonic, no tokio.
- `observatoryd` is a lib with `health() -> "observatory:m0"`; **no binary,
  no config, no /healthz**.
- Workspace deps: only `determinism-proto` via path
  `../control-plane/crates/determinism-proto`, `default-features = false`.
- CI (`.github/workflows/ci.yaml`): single `ubuntu-latest` leg, fmt + build +
  test, **no clippy, no aarch64**; checks out sibling `control-plane` at its
  default branch (needed by the path dep).

Cross-repo state:

- control-plane `main` at `66f0f9f`, tag `proto-v0.2.0` frozen with a Buf
  breaking gate. `determinism.observatory.v1` is a **vdev (pre-release)
  family**: listed in `docs/proto-freeze-policy.md` and ignored by
  `buf.yaml` `breaking.ignore` at `proto/determinism/observatory/v1`. The
  current `proto/determinism/observatory/v1/events.proto` is a 5-field
  placeholder envelope with an **empty** `EventIngest` service.
- `crates/determinism-proto` v0.2.0: feature `observatory = ["common"]`
  exposes a **hand-written** 5-field `EventEnvelope` facade
  (`src/lib.rs:154-166`); `build.rs` runs codegen only for
  scorer/inputsynth/orchestrator/controlplane.
- exploration-orchestrator M0–M4 are done on fakes. Its runtime envelope is a
  transport-free Rust struct (`crates/orch-clients/src/observatory.rs`,
  header cites observatory as contract owner) and its emitter/payload
  builders live in `crates/orch-server/src/events.rs`; the event types it
  actually emits (verified in `crates/orch-server/src/experiment.rs`) are
  exactly the ten v1 names: `node-added`, `node-pruned`,
  `best-score-improved`, `stall-detected`, `escalation-changed`,
  `goal-reached`, `batch-completed`, `checkpoint`, `assertion-violated`,
  `reachability-hit`.

## Grounding notes

Verified against source (per plan-grounding rules):

- **Event-type list**: API.md §2.1's ten names == the string literals in
  `orch-server/src/events.rs` + `experiment.rs`. No extra types, none
  missing. ✅
- **Envelope fields**: API.md §1 proto has
  `{envelope_version, ts_logical, ts_wall_ns, run_id, source_service(enum),
  event_type, payload_version, payload_json(bytes), seq, producer_id}`. The
  orchestrator runtime struct has only
  `{run_id, source_service(String), producer_id, seq, ts_logical,
  event_type, payload(structured map)}` — three fields absent, two typed
  differently. **Divergent — reconciliation is package 01.** The per-event
  payload shapes diverge much further; full table in `01-…`.
- **Ack semantics conflict**: API.md §1 `PublishAck.acked_seq` = "highest
  CONTIGUOUS seq"; the orchestrator's drop-oldest ring deliberately creates
  seq gaps and its own test asserts ack jumps across a gap
  (`events.rs::emitter_never_blocks_and_drops_oldest_on_outage`, seqs
  `[3,4,5,6]` acked 6). Strict contiguity would deadlock a producer that
  dropped. Reconciliation decision D7 (package 01).
- **control-plane proto process**: verified `buf.yaml` (lint STANDARD;
  `SERVICE_SUFFIX` ignore already lists `observatory/v1/events.proto`;
  breaking ignores the family), `docs/proto-freeze-policy.md`,
  `docs/vdev-promotion-playbook.md`, `scripts/{buf-breaking-against.sh,
  check-proto-version.sh}`, CI proto job (buf 1.71.0 + 4 scripts) and rust
  matrix (`ubuntu-latest` + `ubuntu-24.04-arm`, `--all-features`). A vdev
  revision on `main` passes the breaking gate by construction; no version
  bump/tag is mandatory (`check-proto-version.sh` checks only internal
  version consistency and tag context). ✅
- **M0 residue check requested by the task**: CI has neither an aarch64 arm
  nor clippy (see above) → CI-hardening package 04, modeled on
  `state-scorer/.github/workflows/ci.yaml` (matrix incl. `ubuntu-24.04-arm`
  with the documented "free for public repos only" caveat, clippy
  `-D warnings`). Beyond CI, M0's functional scope is unbuilt → package 03.
- **Feature-map grid schema** exists at
  `~/.agents/projects/determinism/docs/reference-workload/API.md` §1
  (`discretize: {kind: grid, x, y, room, cell_w, cell_h}`). ✅

Unconfirmed identifiers (resolve or re-verify while implementing):

- Bead `exploration-orchestrator-75z` full text: that repo's checkout has a
  `.beads/` config but **no local beads database** (`bd show` fails with "no
  beads database found"). Its charge — "reconcile the orchestrator's runtime
  envelope vs the canonical observatory/v1 proto; owner is observatory M1
  ingest design" — is taken from the phase context. Before writing the
  request entry (package 01), try `bd dolt pull`/`bd show` there again and
  cite the bead verbatim if reachable; otherwise cite it by id as relayed.
- `ubuntu-24.04-arm` runner availability if this repo is private (free for
  public repos only — state-scorer recorded the same caveat).
- Perf acceptance hardware ("the Spark", NVMe ≥50k inserts/s, 10k events/s
  sustained): machine-specific; CI runs reduced smokes, full numbers are
  local evidence (§verification rules — report what was actually run).
- `r2d2` for the read pool is named in ARCHITECTURE §1; a hand-rolled pool of
  N read-only connections is an acceptable substitute if the rusqlite/r2d2
  adapter version-matrix fights back — note the deviation if taken.
- Crate naming: ARCHITECTURE §1 calls the shared-types crate `obs-core`; the
  skeleton named it `obs-types`. Keep `obs-types` (renaming buys nothing);
  record the deviation in the repo README.

## Package sequence and dependency graph

| Package | File | Repo(s) touched |
|---|---|---|
| 01 | `01-tracking-and-envelope-reconciliation.md` | observatory (+ docs tree; docs-only request entry in exploration-orchestrator) |
| 02 | `02-proto-authoring-and-landing-control-plane.md` | **control-plane (own commit there)**, then observatory compile-fix |
| 03 | `03-m0-completion-store-config-daemon.md` | observatory |
| 04 | `04-ci-hardening.md` | observatory |
| 05 | `05-m1-ingest-and-projections.md` | observatory |
| 06 | `06-obs-events-gen-and-m1-acceptance.md` | observatory |
| 07 | `07-m2-scrape-and-metrics-raw.md` | observatory |
| 08 | `08-m2-rollups-derived-metrics-rest.md` | observatory |
| 09 | `09-verification-and-handback.md` | observatory |

```
01 ─┬─► 02 ───┬───────────► 05 ─► 06 ─┐
    │         ▼             ▲     ▲   │
    └─► 03 ─► 04 ───────────┼─────┘   ├─► 09
         │                  │         │
         ├──────────────────┘         │
         └─► 07 ────────────► 08 ─────┘
              (08 also needs 05's score_points/events)
```

- 02 and 03 are independent after 01 and may be parallelized (different
  repos; per subagent rules don't share a worktree).
- 04 needs 03 **and 02**: package 04 pins observatory CI's control-plane
  checkout to the package-02 commit SHA. Contingency (recorded in 04): if
  02 has not landed when 04 runs, pin control-plane's current HEAD and bump
  the pin as part of 02's consumer fix-up — but the 02→04 edge is the
  default order.
- 05 needs 02 (generated tonic types) and 03 (real store).
- 06 needs 05 **and 04**: package 06 replaces the determinism-gate
  placeholder job that 04 adds to `ci.yaml` (09 inherits 04 transitively
  through 06).
- 07 needs only 03; it can run parallel to 05/06.
- 08 needs 07 (metrics_raw) and 05 (score_points, event-derived series).

`10-review-log.md` in this plan directory is reserved as the adjudication
record for the review gates (package 06's mid-plan checkpoint and package
09's final pass); every rejected review finding gets a one-line reason
there.

## Ground rules

- Rust 2021, workspace at repo root; keep parser/fold/validation logic in
  lib crates with no tokio where feasible (unit-testable pure cores).
- Every persisted format versioned: `schema_meta.version`,
  `envelope_version`, `payload_version`, config `version = 1` (MAP.md
  conventions restated in ARCHITECTURE §8).
- All timestamps entering the DB flow through an injectable clock so the
  determinism gate can pin them (decision D6, package 01).
- Commit per green package; CI green on every commit. Follow the
  review-workflow (plan → review → implement → fix → verify → commit) — the
  standing `/review` + `/fix-review` gate before each package's commit, and
  package 09 runs the final dual-review pass.
- Cross-repo commits: verify `pwd` + `git remote -v` before every commit
  (three repos are in play). Never commit to control-plane and observatory
  in one shell compound.
- `bd` conventions: `bd create` (not add), short titles + `-d` details,
  close with `-r "evidence"`, run bd commands serially.
- Do not commit this plan directory as part of implementation packages
  unless asked; it may be committed standalone by the user.
