# 04 — CI Hardening (M0 residue: aarch64 leg, clippy, proto pin)

## Goal

Bring `.github/workflows/ci.yaml` up to the M0 acceptance bar ("cargo test
green on x86_64 and aarch64 (CI matrix)") and the repo hygiene the sister
repos already have. Verified current state: one `ubuntu-latest` leg running
fmt/build/test only — no clippy, no aarch64, control-plane sibling checked
out at its default branch (unpinned). Model: `state-scorer`'s workflow
(matrix + arm caveat + cross-repo pin job).

Ordering: this package depends on **package 02** (the pin target below is
02's control-plane merge SHA) as well as 03. Contingency if 02 has not
landed when this runs: pin control-plane's current `main` HEAD instead and
bump the pin as part of 02's consumer fix-up commit — but 02-before-04 is
the default order per the overview graph.

## Changes (`.github/workflows/ci.yaml`)

1. **Matrix**: `runner: [ubuntu-latest, ubuntu-24.04-arm]`.
   Keep the state-scorer caveat as a comment: `ubuntu-24.04-arm` is free
   for public repos only; if the leg cannot be provisioned for this repo,
   record it as pending CI debt in the handback (package 09) — do not drop
   the aarch64 requirement silently.
2. **Clippy**: add `cargo clippy --workspace --all-targets -- -D warnings`
   (after fmt, before build). Expect a cleanup pass over the skeleton +
   package-03 code the first time it runs.
3. **Pin the control-plane checkout**: the path dep
   (`../control-plane/crates/determinism-proto`) makes observatory CI track
   control-plane `main` — unpinned, any upstream change can break this repo
   asynchronously. Pin `ref:` to the package-02 commit SHA and record the
   pin + update procedure in `docs/proto-pin.md` (SHA, `events.proto`
   blake3, "bump deliberately with a commit that names the new SHA").
   Local builds still use the sibling checkout; note in `docs/proto-pin.md`
   that local `../control-plane` must contain the pinned commit
   (`git -C ../control-plane merge-base --is-ancestor <SHA> HEAD`).
4. **Determinism-gate job — explicit placeholder**: add the job here with
   `continue-on-error: false` and a step that is *visibly* a stand-in
   (e.g. `run: echo "determinism-gate placeholder — replaced by package 06"`,
   green by construction). Package 06 REPLACES this step with the
   canonical `--release --test determinism_replay` form — the final
   ci.yaml step text is written ONCE, in 06, and this placeholder is what
   it replaces. Never a name filter like `cargo test … determinism_` — a filter that
   matches zero tests passes vacuously forever, while `--test` fails
   loudly if the target doesn't exist. Green-when-absent is acceptable
   only until 06 lands; MAP.md's CI determinism obligation requires the
   real gate visibly on every commit thereafter.
5. **M0 store-smoke job**: `m0-store-smoke` on `ubuntu-latest`, step
   `cargo test -p obs-store --release --test insert_rate_smoke` — wires
   the reduced ≥5k inserts/s smoke whose test lands in package 03 (this
   step is 03's acceptance handoff; without it that claim is unwired).
6. Keep `fmt --check`, `build --workspace`, `test --workspace` on both
   legs; fmt/clippy may be x86-only (state-scorer runs fmt on one leg —
   either is fine; build/test MUST run on both).

## Files

- `/Users/punk1290/git/preestablished/observatory/.github/workflows/ci.yaml`
- `/Users/punk1290/git/preestablished/observatory/docs/proto-pin.md` (new)

## Acceptance

```bash
cd /Users/punk1290/git/preestablished/observatory
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace && cargo test --workspace
git push   # then (gh run watch without an ID is interactive — resolve it first):
RUN_ID=$(gh run list --commit $(git rev-parse HEAD) --json databaseId -q '.[0].databaseId')
gh run watch $RUN_ID --exit-status   # matrix legs + placeholder determinism-gate + m0-store-smoke green
```

- Both legs green on GitHub (or the arm-leg debt honestly recorded if the
  runner is unavailable — check `gh api repos/{owner}/{repo} --jq
  .private` and note it).
- Clippy clean at `-D warnings` across the workspace.
- CI checkout of control-plane shows the pinned ref in the workflow file.

## Failure guidance

- Arm runner queue-stuck or rejected (private repo): switch that leg to
  `continue-on-error: true` is NOT acceptable — instead remove the leg,
  file a bead (`bd create "Provision aarch64 CI leg" -p 1 -l cleanup`) and
  record it in the package-09 handback, mirroring state-scorer's recorded
  debt pattern.
- Clippy explosion on generated/stub code: fix code, not lint config;
  `#[allow]` only with a one-line justification comment, never module-wide
  blankets.
- If the pinned control-plane ref breaks checkout (force-push upstream —
  they shouldn't), stop and coordinate; never repoint the pin without
  re-running the package-02 gate suite mentally against the new SHA (does
  `events.proto` still match the recorded blake3?).
