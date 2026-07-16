# Event-contract reconciliation v1 (bead `exploration-orchestrator-75z`)

Executed 2026-07-16 by observatory M1 ingest design, per plan
`.agents/plans/phase5-m1-m2-ingest-projections-rollups/01-…`. This is the
durable decision record for the envelope/catalog reconciliation between:

- **Canonical**: `~/.agents/projects/determinism/docs/observatory/API.md`
  §1 (envelope proto) + §2.1 (payload catalog) — observatory owns both
  (MAP.md contract-ownership table).
- **As-built**: exploration-orchestrator at commit `7f97fca`
  (`crates/orch-clients/src/observatory.rs` envelope struct + `EventSink`;
  `crates/orch-server/src/events.rs` payload builders; emissions verified
  in `crates/orch-server/src/experiment.rs`).

The bead text (cited verbatim from the exploration-orchestrator beads DB):

> orch-clients/src/observatory.rs EventEnvelope (postcard payload map,
> producer_id, ts_logical, seq-excluded-from-hash per D6) diverges from
> control-plane proto/determinism/observatory/v1/events.proto (payload_json
> string; no producer_id/ts_logical). Owner of the reconciliation:
> observatory M1 ingest design — the canonical proto likely needs
> producer_id + ts_logical and a decision on payload encoding; our emitter
> then converts at the wire boundary. Do not change orch-clients DTO
> semantics unilaterally. Flagged from request
> phase5-prep-proto-upstream-and-tier2-chaos item 1.

Related: the request entry filed back to exploration-orchestrator lives at
`exploration-orchestrator/.agents/requests/phase5-observatory-event-contract-conformance/`.

## B.1 Envelope divergences (re-verified 2026-07-16 at orch `7f97fca`)

| Aspect | API.md §1 (canonical) | Orchestrator as-built (`observatory.rs:47-56`) |
|---|---|---|
| `envelope_version` | `uint32`, MUST be 1 | absent |
| `ts_wall_ns` | `uint64`, advisory | absent |
| `payload_version` | `uint32` per event_type | absent |
| payload | `bytes payload_json` — UTF-8 JSON **object**, ≤64 KiB | structured `Payload = BTreeMap<String, PayloadValue>` (`observatory.rs:38`, postcard-canonical; JSON encoding is the future tonic adapter's job) |
| `source_service` | `enum SourceService` | `String` (`"EXPLORATION_ORCHESTRATOR"`, `events.rs:17`) |
| `run_id, seq, ts_logical, event_type, producer_id` | — | match semantically (`seq` restarts per session `events.rs:61`, `producer_id = "orchestratord-<startup_unix>"` per `observatory.rs:42-45` doc, `ts_logical` = expansion index) |
| ack semantics | `acked_seq` = highest **CONTIGUOUS** seq (API.md §1 `PublishAck` comment) | drop-oldest ring creates permanent seq gaps; their own unit test `events.rs::emitter_never_blocks_and_drops_oldest_on_outage` (`events.rs:334-357`) drains seqs `[3,4,5,6]` and asserts `acked_seq == 6` across the gap |

## B.2 Payload divergences (API.md §2.1 vs `events.rs` builders, re-verified)

| event_type | §2.1 fields | as-built fields (builder) | Notes |
|---|---|---|---|
| `node-added` | `node_id, parent_id?, snapshot_ref, depth, progress_score, novelty_score, cell_key: str?, stage, guest_time_ns, input_delta_bytes, expansion_idx, features?` | `node_id, parent_node_id, score, novelty, cell_key: u64, stage, features` (`events.rs:190-214`) | 5 fields missing; 3 renamed; `cell_key` typed u64 not str; node ids are decimal strings (`events.rs:177-180`, matches `str`) |
| `node-pruned` | `node_id, reason` | `parent_node_id, reason, node_id?` (`events.rs:217-228`) | **semantic gap in the catalog**: duplicate/regression/frontier-evict prunes discard candidates that never received a node_id — emitted with `node: None` at `experiment.rs:1684, 1712, 1988, 2002, 2103`; the catalog's mandatory `node_id` is unemittable for them. `Some(node_id)` sites: `experiment.rs:1609, 1953` (exhausted) |
| `best-score-improved` | `node_id, score, prev_best, expansion_idx` | `node_id, best_score, previous_best_score` (`events.rs:231-237`) | renames + missing `expansion_idx` (recoverable from `ts_logical`) |
| `stall-detected` | `window_expansions, best_score, escalation_level, since_expansion_idx` | `expansions_since_improvement, window` (`events.rs:240-248`) | mostly disjoint |
| `escalation-changed` | `from_level, to_level, expansion_idx` | `level, previous_level` (`events.rs:251-259`) | renames |
| `goal-reached` | `node_id, goal_id, score, expansion_idx, path_len` | `node_id, score` (`events.rs:262-267`) | missing 3 |
| `batch-completed` | `batch_seq, kept, dups, regressions, failed_jobs, batch_wall_ms` | `batch_seq, parent_node_id, committed, discarded` (`events.rs:270-282`) | mostly disjoint |
| `checkpoint` | `checkpoint_id, expansion_idx, frontier_size, tree_nodes, archive_cells, seen_set_size` | `batch_seq, expansions, archive_seq` (`events.rs:285-291`) | disjoint |
| `assertion-violated` | `node_id?, assertion_id, message, guest_pc?, beacon_seq?` | generic `sdk_event_payload`: `node_id, stream, payload: bytes` (`events.rs:296-305`) | orchestrator relays raw SDK bytes, doesn't decode (`experiment.rs:1637`) |
| `reachability-hit` | `node_id, reachability_id, first_hit, expansion_idx` | same generic relay (`experiment.rs:1639`) | same |

Event-type vocabulary: the ten §2.1 names are exactly the string literals
emitted in `experiment.rs` (`node-added` 1619/2045, `node-pruned`
1608/1683/1711/1952/1987/2001/2102, `best-score-improved` 2180,
`stall-detected` 2192, `escalation-changed` 2203, `goal-reached` 1752/2134,
`batch-completed` 1722/2112, `checkpoint` 2462, `assertion-violated` 1637,
`reachability-hit` 1639). No extra types, none missing.

Aggravating fact recorded for the request entry: the orchestrator's own
spec (`~/.agents/projects/determinism/docs/exploration-orchestrator/API.md`
§6, line 568) states "Payload field shapes are the catalog's" — its code
contradicts its own doc, so payload conformance is a bug on their side by
their own doc's standard, not a contract dispute.

## Decisions (all ACCEPTED as recommended by the plan, 2026-07-16)

### D1 — proto shape: canonical, verbatim — ACCEPTED

`observatory.proto` implements API.md §1 verbatim: full envelope including
`envelope_version`/`ts_wall_ns`/`payload_version`/`payload_json`,
`SourceService` enum, `EventBatch`, `PublishAck`, `Rejection`, both RPCs.
Observatory owns the contract; the proto follows the owner doc, not the
consumer's interim struct. (This also answers the bead's question: the
canonical proto already carries `producer_id` + `ts_logical`; the
placeholder in control-plane simply predated the owner doc. Payload
encoding stays `payload_json` bytes; the orchestrator's postcard map
converts at its tonic wire boundary, exactly as the bead anticipated.)

### D2 — enum naming for buf lint — ACCEPTED

control-plane lints with buf STANDARD; API.md §1's original
`SOURCE_UNSPECIFIED`/`EXPLORATION_ORCHESTRATOR`/… violated
`ENUM_VALUE_PREFIX` — and only that rule (`SOURCE_UNSPECIFIED` already
satisfied `ENUM_ZERO_VALUE_SUFFIX`). Renamed in the proto and in API.md §1
to `SOURCE_SERVICE_UNSPECIFIED`, `SOURCE_SERVICE_EXPLORATION_ORCHESTRATOR`,
`SOURCE_SERVICE_DETERMINISM_HYPERVISOR`, `SOURCE_SERVICE_STATE_SCORER`,
`SOURCE_SERVICE_REPLAY_RENDERER`, `SOURCE_SERVICE_CONTROL_PLANE`,
`SOURCE_SERVICE_GUEST_SDK`. Rejected alternative: `ignore_only` additions
in `buf.yaml` (precedent exists but is reserved for already-frozen
families; the observatory family is vdev with zero generated consumers, so
lint-clean beats ignore entries).

### D3 — catalog stays canonical; ingest is tolerant — ACCEPTED

§2.1 field shapes remain the contract. Ingest NEVER rejects an envelope
for missing/renamed payload fields — validation rejects only structural
violations (`envelope_version != 1`, non-object/unparseable
`payload_json`, >64 KiB, and empty `run_id` from a run-scoped source).
Projections extract what they can; absent projection inputs fall back to
per-column defaults and increment
`obs_projection_partial_total{event_type}`. No alias/rename shims for the
orchestrator's as-built names — tolerance, not translation (drift stays
visible; the fix belongs on the producer). Contract addition riding with
this decision: the `"missing run_id"` rejection cause was added to API.md
§1's rejection-cause list (observatory-owned edit).

### D4 — catalog amendments observatory concedes — ACCEPTED

`node-pruned` gains `parent_id: str` and `node_id` becomes optional
(`node_id?: str` — absent for never-committed candidates). The `runs`
`nodes_pruned` counter and tree projection handle the absent-id case (only
mark `tree_nodes.pruned` when `node_id` is present). This also amends
INTEGRATION.md §1's `node-pruned` row, which previously said pre-commit
discards are NOT events (firehose-volume rationale) — left unamended it
would contradict optional `node_id`. **Firehose-volume tradeoff accepted:**
the as-built orchestrator already emits id-less pre-commit prunes
(`experiment.rs:1684, 1988, 2002, 2103`), so ingestion must reflect
reality; if volume ever becomes a problem it is a future rate-limit
decision on the producer side, not a schema one. Everything else stays as
§2.1 — the orchestrator has the data (snapshot_ref, depth, expansion_idx,
checkpoint stats, …) and should emit it.

### D5 — dual generator profiles — ACCEPTED

`obs-events-gen` (package 06) emits `--vocab catalog` (strict §2.1, the M1
acceptance profile) and `--vocab orch-asbuilt` (byte-shapes of `events.rs`
today) to prove D3's tolerance path against reality.

### D6 — injectable clock — ACCEPTED

Every DB timestamp observatory itself mints (`ingested_at_ns`, derive-tick
times) comes from a `Clock` trait so the determinism gate can pin time.
Event-derived `metrics_raw` samples are the deliberate exception: they
carry the envelope's `ts_wall_ns` (producer time, generator-deterministic)
— see package 05's projection rules.

### D7 — ack semantics amendment — ACCEPTED

API.md §1's ack doc changed from "highest CONTIGUOUS seq" to: highest seq
committed **in stream order** per producer session (gaps permitted —
producers legitimately drop-oldest; resend-from-`acked_seq+1` remains safe
because the server dedups). Strict contiguity would deadlock any producer
that dropped an envelope — the orchestrator's own drop-oldest test
(`events.rs:334-357`) already assumes gap-tolerant acks. Server
implementation (package 05) tracks a monotone per-identity high-water
mark, not a contiguity scan. ARCHITECTURE.md §2's matching
strict-contiguity sentence was amended identically. The orchestrator-side
stale wording (`observatory.rs:59-60` "highest contiguous sequence" in the
`EventSink` doc comment) is cited in the request entry as theirs to fix.

### D8 — SDK relay shapes — ACCEPTED

§2.1's typed `assertion-violated`/`reachability-hit` shapes stay
canonical; the request entry asks the orchestrator to decode/enrich its
relay (or the guest-sdk contract to define the decode). Until then D3
stores the as-built relay with the raw payload and projects a finding with
`summary = "undecoded guest-sdk relay"`.

### D9 — producer-identity canonicalization (dedup identity) — ACCEPTED

The stored `source_service` string is ALWAYS the folded form
`"<service>/<producer_id>"`. The orchestrator always sets `producer_id`,
so the "plain service name when producer_id is empty" branch is dead — one
rule, no conditionals. Source-scoped projection logic (e.g. the
orchestrator-only `runs.expansions` update) keys off the decoded
`SourceService` enum at ingest time, in-transaction — never off the stored
string; any consumer of the stored string must prefix-match
(`'<service>/%'`).

## Owner-doc edits applied (2026-07-16)

The docs tree `~/.agents/projects/determinism/docs/` is not git-managed
(verified: `git -C ~/.agents status` → not a repository); the before/after
capture of these edits is committed at
`evidence/phase5-m1-m2/docs-tree-edits.diff` in this repo.

- `observatory/API.md` §1: `SourceService` value renames (D2); `PublishAck`
  ack-semantics wording (D7); `"missing run_id"` added to the named
  rejection causes (D3).
- `observatory/API.md` §2.1: `node-pruned` row — `node_id?` optional +
  `parent_id` added (D4).
- `observatory/ARCHITECTURE.md` §2: ack sentence aligned with D7;
  projection-table `node-pruned` row handles the absent-id case (D4).
- `observatory/INTEGRATION.md` §1: `node-pruned` row no longer claims
  pre-commit discards are not events (D4).

## Proto pin (package 02)

- control-plane merge commit: `pending — filled by package 02`
- `proto/determinism/observatory/v1/events.proto` blake3:
  `pending — filled by package 02`
