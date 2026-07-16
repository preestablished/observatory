# 01 — Tracking Setup + Envelope/Catalog Reconciliation (bead exploration-orchestrator-75z)

## Goal

(a) Stand up beads tracking for this plan. (b) Execute the open
reconciliation the platform assigned to "observatory M1 ingest design":
compare the orchestrator's **as-built** runtime envelope + payloads against
the canonical catalog (observatory API.md §1–§2.1), decide the proto shape
and the ingest tolerance rules, record the decisions, and file the notice
back to exploration-orchestrator as a **docs-only** `.agents/requests/`
entry. No orchestrator source is edited. Every later package consumes these
decisions.

## Part A — tracking

```bash
cd /Users/punk1290/git/preestablished/observatory
bd init                                    # creates .beads/ (none exists today)
```

Then create one bead per package 01–09 (short titles, details in `-d`,
`-p 0` for 01/02/03/05, `-p 1` for the rest, `--silent` capture). Labels,
assigned explicitly: 01 `-l analysis`, 09 `-l cleanup`, 06 `-l testing`,
02/03/04/05/07/08 `-l impl`. Wire `bd dep add CHILD PARENT` edges exactly
per the overview graph — which includes 04→02 (04 pins observatory CI's
control-plane checkout to the package-02 SHA) and 06→04 (06 replaces 04's
determinism-gate placeholder job); 09 picks up 04 transitively through 06.
Run all `bd` commands serially.

## Part B — the divergence audit (grounded, re-verify while implementing)

Sources to diff, field by field:

- Canonical: `~/.agents/projects/determinism/docs/observatory/API.md` §1
  (envelope proto) and §2.1 (payload catalog).
- As-built: `/Users/punk1290/git/preestablished/exploration-orchestrator/`
  `crates/orch-clients/src/observatory.rs` (envelope struct + `EventSink`)
  and `crates/orch-server/src/events.rs` (payload builders), emissions
  cross-checked in `crates/orch-server/src/experiment.rs`.

### B.1 Envelope divergences (pre-verified during planning)

| Aspect | API.md §1 (canonical) | Orchestrator as-built |
|---|---|---|
| `envelope_version` | `uint32`, MUST be 1 | absent |
| `ts_wall_ns` | `uint64`, advisory | absent |
| `payload_version` | `uint32` per event_type | absent |
| payload | `bytes payload_json` — UTF-8 JSON **object**, ≤64 KiB | structured `Payload = BTreeMap<String, PayloadValue>` (postcard-canonical; JSON encoding is the future tonic adapter's job) |
| `source_service` | `enum SourceService` | `String` (`"EXPLORATION_ORCHESTRATOR"`) |
| `run_id, seq, ts_logical, event_type, producer_id` | — | match semantically (seq restarts per session, `producer_id = "orchestratord-<startup_unix>"`, `ts_logical` = expansion index) |
| ack semantics | `acked_seq` = highest **CONTIGUOUS** seq | drop-oldest ring creates permanent seq gaps; their own unit test (`events.rs::emitter_never_blocks_and_drops_oldest_on_outage`) expects ack to advance across gaps |

### B.2 Payload divergences (API.md §2.1 vs `events.rs` builders)

| event_type | §2.1 fields | as-built fields | Notes |
|---|---|---|---|
| `node-added` | `node_id, parent_id?, snapshot_ref, depth, progress_score, novelty_score, cell_key: str?, stage, guest_time_ns, input_delta_bytes, expansion_idx, features?` | `node_id, parent_node_id, score, novelty, cell_key: u64, stage, features` | 5 fields missing; 3 renamed; `cell_key` type differs; node ids are decimal strings (matches `str`) |
| `node-pruned` | `node_id, reason` | `parent_node_id, reason, node_id?` | **semantic gap in the catalog**: duplicate/regression/frontier-evict prunes discard candidates that never received a node_id — the catalog's mandatory `node_id` is unemittable for them |
| `best-score-improved` | `node_id, score, prev_best, expansion_idx` | `node_id, best_score, previous_best_score` | renames + missing `expansion_idx` (recoverable from `ts_logical`) |
| `stall-detected` | `window_expansions, best_score, escalation_level, since_expansion_idx` | `expansions_since_improvement, window` | mostly disjoint |
| `escalation-changed` | `from_level, to_level, expansion_idx` | `level, previous_level` | renames |
| `goal-reached` | `node_id, goal_id, score, expansion_idx, path_len` | `node_id, score` | missing 3 |
| `batch-completed` | `batch_seq, kept, dups, regressions, failed_jobs, batch_wall_ms` | `batch_seq, parent_node_id, committed, discarded` | mostly disjoint |
| `checkpoint` | `checkpoint_id, expansion_idx, frontier_size, tree_nodes, archive_cells, seen_set_size` | `batch_seq, expansions, archive_seq` | disjoint |
| `assertion-violated` | `node_id?, assertion_id, message, guest_pc?, beacon_seq?` | generic `sdk_event_payload`: `node_id, stream, payload: bytes` | orchestrator relays raw SDK bytes, doesn't decode |
| `reachability-hit` | `node_id, reachability_id, first_hit, expansion_idx` | same generic relay | same |

Aggravating fact for the request: the orchestrator's own spec
(`~/.agents/projects/determinism/docs/exploration-orchestrator/API.md` §6)
claims "Payload field shapes are the catalog's" — its code contradicts its
own doc, so this is a bug on their side by their own doc's standard, not a
contract dispute.

Redo this audit methodically against current source (both repos may have
moved); the table above is planning-time evidence, the committed audit is
acceptance evidence.

## Part C — decisions (recommended; record accepted/changed outcomes)

Write the decision log to
`observatory/docs/event-contract-reconciliation-v1.md` (new file, committed
here — the repo-local durable record) containing the B.1/B.2 tables plus:

- **D1 — proto shape**: `observatory.proto` implements API.md §1 verbatim
  (full envelope incl. `envelope_version`/`ts_wall_ns`/`payload_version`/
  `payload_json`, `SourceService` enum, `EventBatch`, `PublishAck`,
  `Rejection`, both RPCs). Observatory owns the contract; the proto follows
  the owner doc, not the consumer's interim struct.
- **D2 — enum naming for buf lint**: control-plane lints with STANDARD;
  API.md §1's `SOURCE_UNSPECIFIED`/`EXPLORATION_ORCHESTRATOR`… violate
  `ENUM_VALUE_PREFIX` — and only that rule (`SOURCE_UNSPECIFIED` already
  SATISFIES `ENUM_ZERO_VALUE_SUFFIX`; don't overstate the breakage in the
  request entry). Rename in the proto to
  `SOURCE_SERVICE_UNSPECIFIED`, `SOURCE_SERVICE_EXPLORATION_ORCHESTRATOR`,
  etc. (family is vdev, zero generated consumers today — lint-clean beats
  ignore entries), and update API.md §1 to match. Rejected alternative:
  `ignore_only` additions in `buf.yaml` (precedent exists but is reserved
  for already-frozen families).
- **D3 — catalog stays canonical; ingest is tolerant**: §2.1 field shapes
  remain the contract. Ingest NEVER rejects an envelope for missing/renamed
  payload fields — validation rejects only structural violations
  (`envelope_version != 1`, non-object/unparseable `payload_json`,
  \>64 KiB). Projections extract what they can; absent projection inputs
  fall back to per-column defaults and increment
  `obs_projection_partial_total{event_type}`. No alias/rename shims for the
  orchestrator's as-built names — tolerance, not translation (drift stays
  visible; the fix belongs on the producer). One contract addition rides
  with this decision: the `"missing run_id"` rejection (package 05
  validates run-scoped sources) is not among API.md §1's named PublishAck
  rejection causes — add it there as a one-line cause (observatory-owned
  edit, captured with the other doc edits below).
- **D4 — catalog amendments observatory concedes** (update API.md §2.1 +
  ARCHITECTURE §2 projection table in the docs tree, which observatory owns):
  `node-pruned` gains `parent_id: str` and `node_id` becomes optional
  (`node_id?: str` — absent for never-committed candidates); the `runs`
  `nodes_pruned` counter and tree projection handle the absent-id case
  (only marks `tree_nodes.pruned` when `node_id` present). This ALSO
  amends INTEGRATION.md §1's `node-pruned` row (observatory owns that doc
  too), which currently says pre-commit discards are NOT events (rationale:
  firehose volume) — left unamended it would contradict optional `node_id`.
  Record the firehose-volume tradeoff acceptance in the decision log: the
  as-built orchestrator already emits id-less pre-commit prunes
  (`experiment.rs:1560/1588/1861`), so ingestion must reflect reality; if
  volume ever becomes a problem it is a future rate-limit decision on the
  producer side, not a schema one. Everything else
  stays as §2.1 — the orchestrator has the data (snapshot_ref, depth,
  expansion_idx, checkpoint stats…) and should emit it.
- **D5 — dual generator profiles**: `obs-events-gen` (package 06) emits
  `--vocab catalog` (strict §2.1, the M1 acceptance profile) and
  `--vocab orch-asbuilt` (byte-shapes of `events.rs` today) to prove D3's
  tolerance path against reality.
- **D6 — injectable clock**: every DB timestamp observatory itself mints
  (`ingested_at_ns`, derive-tick times) comes from a `Clock` trait so the
  determinism gate can pin time.
- **D7 — ack semantics amendment**: change API.md §1's ack doc from
  "highest CONTIGUOUS seq" to "highest seq committed **in stream order** per
  producer session (gaps permitted — producers legitimately drop-oldest;
  resend-from-`acked_seq+1` remains safe because the server dedups)".
  Strict contiguity would deadlock any producer that dropped an envelope.
  Server implementation (package 05) tracks a monotone per-identity
  high-water mark, not a contiguity scan.
- **D8 — SDK relay shapes**: keep §2.1's typed `assertion-violated`/
  `reachability-hit` shapes canonical; the request asks the orchestrator to
  decode/enrich its relay (or the guest-sdk contract to define the decode).
  Until then D3 stores the as-built relay with the raw payload and projects
  a finding with `summary = "undecoded guest-sdk relay"`.
- **D9 — producer-identity canonicalization (dedup identity)**: the stored
  `source_service` string is ALWAYS the folded form
  `"<service>/<producer_id>"`. The orchestrator always sets `producer_id`,
  so the "plain service name when producer_id is empty" branch is dead —
  one rule, no conditionals. Source-scoped projection logic (e.g. the
  orchestrator-only `runs.expansions` update) keys off the decoded
  `SourceService` enum at ingest time, in-transaction — never off the
  stored string; any consumer of the stored string must prefix-match
  (`'<service>/%'`). Package 05's canonicalization section cites this
  addendum.

Apply the doc edits to
`~/.agents/projects/determinism/docs/observatory/API.md` (§1 enum names +
D7 ack wording + the one-line `"missing run_id"` rejection cause from D3,
§2.1 `node-pruned` row), `ARCHITECTURE.md` (the §2 projection-table row,
AND §2's own ack sentence "highest contiguous `seq` committed per
(run_id, source_service)" — same strict-contiguity wording D7 amends in
API.md §1; both owner docs must say the same thing after the edit),
and the `INTEGRATION.md` §1 `node-pruned` row (D4) — smallest possible
diffs, observatory owns these files. Do not edit any other service's docs.

## Part D — the request entry (exploration-orchestrator repo, docs only)

Create
`/Users/punk1290/git/preestablished/exploration-orchestrator/.agents/requests/phase5-observatory-event-contract-conformance/`
mirroring the house request format there (see the sibling
`phase5-prep-proto-upstream-and-tier2-chaos/` entry: `00-overview.md`,
`01-current-state.md`, `02-requested-work.md`, `03-verification-offer.md`,
leave `04-resolution.md` to them):

- **00**: who's asking (observatory M1 ingest design, executing the
  reconciliation bead `exploration-orchestrator-75z` — cite verbatim if a
  beads DB is reachable, by id otherwise), why now (their M5 builds the real
  tonic sink; conforming before M5 is free, after M5 it's a wire migration).
- **01**: the B.1/B.2 divergence tables verbatim, with file/line evidence,
  including that their own API.md §6 already promises catalog conformance.
- **02**: the asks — (1) conform `events.rs` payload builders to §2.1
  as-amended (D4 gives them `node-pruned` relief; D8 for SDK relays), (2)
  extend the runtime envelope/adapter for `envelope_version`, `ts_wall_ns`,
  `payload_version`, JSON-object payload encoding at the tonic boundary,
  (3) adopt the amended ack wording (their drop-oldest test already assumes
  it — D7 legitimizes their behavior, no code change expected there; do
  cite the `EventSink` trait doc comment,
  `crates/orch-clients/src/observatory.rs:59-60` "highest contiguous
  sequence", as stale wording to fix alongside).
  Target: before their M5 exit. Include the proto pin (package 02's
  control-plane commit SHA + path) once known.
- **03**: verification observatory offers — `obs-events-gen --vocab catalog`
  as their conformance oracle, plus a future joint smoke against the real
  ingest server.

Commit that entry in the exploration-orchestrator repo as its own
docs-only commit, pushed **directly to its `main`** — house practice for
`.agents/` records, no code is touched, no PR needed (verify
`pwd`/`git remote -v` first; nothing outside `.agents/requests/` may
appear in the diff).

## Files touched

- `observatory/.beads/*` (bd init + beads)
- `observatory/docs/event-contract-reconciliation-v1.md` (new)
- `~/.agents/projects/determinism/docs/observatory/API.md`,
  `ARCHITECTURE.md`, `INTEGRATION.md` (surgical D2/D3/D4/D7 edits incl.
  the INTEGRATION.md §1 `node-pruned` row; this docs tree is not a git
  repo — check first with `git -C ~/.agents status` and capture
  before/after if not)
- `exploration-orchestrator/.agents/requests/phase5-observatory-event-contract-conformance/{00,01,02,03}*.md`
  (new, docs-only, own commit in that repo)

## Acceptance

- Package 01's bead is closed at this package's green commit (each
  package's bead closes at its own boundary — the package-06 checkpoint
  only sweeps up any still-open stragglers); after closing it, `bd ready`
  shows package-02 and package-03 beads unblocked, the rest blocked per
  the graph.
- `docs/event-contract-reconciliation-v1.md` exists with both tables, all
  nine decisions marked accepted/amended, and file:line evidence for every
  as-built claim (re-verified, not copied blind from this plan).
- The request entry exists and is committed in exploration-orchestrator
  (docs-only diff), and the observatory-side decision log links to it by
  path.
- API.md/ARCHITECTURE.md/INTEGRATION.md edits are limited to the
  D2/D3/D4/D7 surfaces named above — including the INTEGRATION.md §1
  `node-pruned` row and API.md §1's added `"missing run_id"` rejection
  cause (diff or before/after capture if the tree isn't git-managed).

## Failure guidance

- If re-audit finds the orchestrator moved (e.g., builders already
  conformed): shrink the tables to the remaining deltas; if zero remain,
  the request entry becomes a short confirmation note and D3's tolerance
  machinery still ships (other producers arrive in Phase 7).
- If `bd` fails with an embedded-Dolt lock: retry serially a bounded number
  of times; never parallelize bd calls.
- If any decision here proves wrong downstream (e.g., D7 conflicts with a
  consumer you find later), amend the decision log in place with a dated
  correction — don't fork a second log.
