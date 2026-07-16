# 02 — Author `observatory.proto` and Land It in control-plane (cross-repo)

## Goal

Replace control-plane's placeholder
`proto/determinism/observatory/v1/events.proto` (5-field envelope, empty
service) with the full reconciled schema (package 01 D1/D2/D7), wire real
codegen into `determinism-proto`'s `observatory` feature, and pass
control-plane's documented gates. **This package is a cross-repo change
with its own commit in `/Users/punk1290/git/preestablished/control-plane`**
— the implementing agent must work there directly, then fix up observatory's
consuming stubs so both repos are green in the same wave.

The family stays **vdev**: `docs/proto-freeze-policy.md` lists
`determinism.observatory.v1` as pre-release and `buf.yaml` ignores
`proto/determinism/observatory/v1` for breaking checks, so replacing the
placeholder is legal against the `proto-v0.2.0` baseline by construction.
Do NOT run the promotion playbook (`docs/vdev-promotion-playbook.md`) — that
is a later, separately-signaled freeze; this package is the "owner authors
the schema" prerequisite it depends on.

## Schema content

`proto/determinism/observatory/v1/events.proto`, package
`determinism.observatory.v1` (keep the filename `events.proto` — buf's
`SERVICE_SUFFIX` lint ignore already references that exact path):

- `service EventIngest` with
  `rpc PublishEvents(stream EventEnvelope) returns (stream PublishAck)` and
  `rpc PublishEventsBulk(EventBatch) returns (PublishAck)`.
- `EventEnvelope` exactly per API.md §1: `envelope_version u32=1`,
  `ts_logical u64=2`, `ts_wall_ns u64=3`, `run_id string=4`,
  `SourceService source_service=5`, `event_type string=6`,
  `payload_version u32=7`, `payload_json bytes=8`, `seq u64=9`,
  `producer_id string=10`.
- `enum SourceService` with D2 lint-clean names:
  `SOURCE_SERVICE_UNSPECIFIED = 0`,
  `SOURCE_SERVICE_EXPLORATION_ORCHESTRATOR = 1`,
  `SOURCE_SERVICE_DETERMINISM_HYPERVISOR = 2`,
  `SOURCE_SERVICE_STATE_SCORER = 3`, `SOURCE_SERVICE_REPLAY_RENDERER = 4`,
  `SOURCE_SERVICE_CONTROL_PLANE = 5`, `SOURCE_SERVICE_GUEST_SDK = 6`.
  (D2 precision: the old API.md names violated only `ENUM_VALUE_PREFIX` —
  `SOURCE_UNSPECIFIED` already satisfied `ENUM_ZERO_VALUE_SUFFIX`; the
  rename fixes the prefix rule, nothing more.)
- `message EventBatch { repeated EventEnvelope events = 1; }`
- `PublishAck { uint64 acked_seq = 1; repeated Rejection rejections = 2; }`
  with the D7 ack comment (highest seq committed in stream order; gaps
  permitted), `Rejection { uint64 seq = 1; string reason = 2; }`.
- Field/enum comments condensed from API.md §1 (source-of-truth pointer in
  the header comment: observatory API.md; payload schemas documented there,
  not in proto).

No import of `common/v1` unless something is actually used from it (the
schema above needs nothing; `observatory = ["common"]` feature dep can stay
for consistency).

## Steps (control-plane repo)

Work on a branch from current `main` (was `66f0f9f` at planning time;
re-verify) — their CI runs on push/PR.

1. Replace `proto/determinism/observatory/v1/events.proto` (root tree).
2. Mirror the byte-identical file to
   `crates/determinism-proto/proto/determinism/observatory/v1/events.proto`
   (the packaged copy; `build.rs::assert_proto_copies_match` will compare
   once extended).
3. `crates/determinism-proto/Cargo.toml`: change
   `observatory = ["common"]` →
   `observatory = ["common", "dep:prost", "dep:tonic", "dep:tonic-prost"]`.
4. `build.rs`: add `cargo:rerun-if-env-changed=CARGO_FEATURE_OBSERVATORY`,
   an `include_observatory` flag, push
   `determinism/observatory/v1/events.proto` into `protos`, and extend
   `assert_proto_copies_match` with the observatory family (mirror the
   existing four families' pattern exactly).
5. `src/lib.rs`: delete the hand-written facade (lines ~154–166, the
   `#[cfg(feature = "observatory")] pub mod observatory` block) and replace
   with the generated include, mirroring the other tonic families:
   `pub mod observatory { pub mod v1 { tonic::include_proto!("determinism.observatory.v1"); } }`.
6. Add `crates/determinism-proto/tests/observatory_v1.rs` (mirror
   `tests/scorer_v1.rs` style): envelope encode/decode round-trip with all
   ten fields, `SourceService` variant values 0–6, `EventBatch`/`PublishAck`/
   `Rejection` round-trips, and symbol existence for
   `event_ingest_client::EventIngestClient` /
   `event_ingest_server::EventIngest` (streaming + unary).
7. Versioning: keep crate/workspace `0.2.0` and `PROTO_VERSION =
   "proto-v0.2.0"` — a vdev revision is not a release
   (`check-proto-version.sh` only enforces internal consistency + tag
   context; no tag is being cut). If control-plane's owners want a
   `proto-v0.2.1` marker tag, that's their call — record whichever outcome
   in the commit body.

## Gates (run all, from the control-plane repo root, individually checked)

```bash
buf lint
scripts/buf-breaking-against.sh          # baseline = latest proto-v* tag (proto-v0.2.0)
scripts/check-buf-breaking-self-test.sh
scripts/check-proto-descriptor-eq.sh
scripts/check-proto-version.sh
cargo check -p determinism-proto --no-default-features
cargo check -p determinism-proto --no-default-features --features observatory
cargo build --workspace --all-features
cargo test --workspace --all-features
```

(`buf` may need installing locally: their CI pins 1.71.0 via
`bufbuild/buf-action`; match the major.) Landing mechanics: the
control-plane change lands **via PR** — commit on the branch (body naming
this plan + the reconciliation decision log path), push, open a PR, wait
for control-plane CI green (proto job + both rust legs), then
`gh pr merge --squash` (the method flag is required non-interactively).
Record the merge commit SHA via
`gh pr view --json mergeCommit -q .mergeCommit.oid` — packages 01 (request
entry pin), 04 (CI pin), and 05 reference it.

## Consumer fix-up (observatory repo, same wave)

The generated type breaks observatory's stubs (field set changed,
`payload_json` becomes `Vec<u8>`, `source_service` becomes `i32` enum in
prost). observatory CI checks out control-plane's **default branch**, so
landing on control-plane `main` breaks observatory `main` until this lands:

- `crates/obs-types/src/lib.rs`: re-export the full generated `v1` module
  (envelope, acks, batch, enum, client/server), keep `event_key` compiling
  (`seq` unchanged).
- `crates/obs-store/src/lib.rs` test + `crates/obs-ingest/src/lib.rs`:
  update `EventEnvelope` construction to the new fields.
- `crates/obs-types/Cargo.toml`: unchanged (feature `observatory` now pulls
  tonic/prost transitively).

Land the control-plane merge and the observatory fix-up commit
back-to-back (control-plane first), minimizing the red window on
observatory `main`. If package 04 already landed with the contingency
control-plane HEAD pin (02 not yet merged at the time), bump observatory
CI's pinned `ref:` to this package's merge SHA as part of this fix-up
commit and update `docs/proto-pin.md` accordingly.

## Acceptance

- All nine gate commands above pass in control-plane; their CI (proto job +
  x86_64 + aarch64 rust jobs) green on the branch/`main`.
- `buf breaking` output shows the observatory path skipped (family ignored)
  — i.e., the gate passes *because of policy*, not because of `--force`
  anything; never weaken `buf.yaml`.
- In observatory: `cargo build --workspace && cargo test --workspace` green
  against the sibling checkout;
  `cargo tree -p obs-types -e features | grep -A2 determinism-proto` shows
  only the `observatory` (+`common`) features enabled — the
  `default-features = false` posture must survive.
- Recorded: control-plane commit SHA + blake3 of `events.proto`
  (`b3sum proto/determinism/observatory/v1/events.proto`) in
  `observatory/docs/event-contract-reconciliation-v1.md` (proto pin section)
  and in the exploration-orchestrator request entry.

## Failure guidance

- **`buf lint` failures**: fix names in the proto, not `buf.yaml`. Adding
  ignore entries for a brand-new vdev schema is a smell; only the
  pre-existing `SERVICE_SUFFIX` entry for `EventIngest` is expected to carry
  it (service is named per API.md; renaming to `EventIngestService` would
  diverge from the owner doc — keep `EventIngest`, it's already ignored).
- **`buf breaking` fails**: you are diffing against the wrong baseline or
  the family's ignore path changed — stop and re-read
  `docs/proto-freeze-policy.md`; do not "fix" by editing `breaking.ignore`.
- **`assert_proto_copies_match` mismatch**: root and packaged copies
  drifted; re-copy bytes, never hand-edit one side.
- **prost `bytes` type friction downstream**: prost generates
  `Vec<u8>` for `bytes` by default — if observatory code wants `Bytes`,
  adapt in observatory, don't add prost config in control-plane for one
  consumer.
- If control-plane `main` has moved and conflicts (another proto landing in
  flight): proto promotions/landings serialize — rebase, re-run all gates.
