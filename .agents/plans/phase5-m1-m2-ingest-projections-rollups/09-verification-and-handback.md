# 09 — Verification, Review Gate, Handback

## Goal

Close the plan the way the review-workflow demands: full verification run,
dual-review pass over the whole change set, evidence consolidated, beads
closed with reasons, and an honest handback that names what was exercised
where — including the pieces that are *pending by design* (live
orchestrator integration; Spark-hardware numbers if unavailable).

Note: package 06's mid-plan checkpoint already pushed the touched repos,
closed beads 01–06, and wrote a partial handback at the M1 boundary — this
package may therefore effectively run twice; the final pass here covers
07–09 plus the whole-set review.

## Verification run (each command individually checked, not `&&`-chained)

```bash
cd /Users/punk1290/git/preestablished/observatory
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace
cargo test -p obs-ingest --release --test determinism_replay   # the CI determinism gate, locally
bash scripts/chaos-killnine.sh 100000                       # CI-sized chaos re-run
cargo run -p observatoryd -- --config ci/dev-observatoryd.toml &   # runtime smoke:
timeout 60 bash -c 'until curl -sf http://127.0.0.1:7471/healthz; do sleep 1; done'   # readiness-wait: cargo run races compilation
cargo run -p obs-events-gen -- publish --addr http://127.0.0.1:7470 --seed 7 --events 5000 --vocab catalog --run-id demo-7
curl -sf 'http://127.0.0.1:7471/api/v1/runs/demo-7/score-curve' | head -c 300
curl -sf 'http://127.0.0.1:7471/api/v1/runs/demo-7/timeseries?metrics=search_best_score&step=auto' | head -c 300
kill %1   # clean up the daemon
```

Runtime behavior must be observed (score-curve JSON actually contains the
generator's improvements), not inferred from exit codes. Then confirm both
CI legs green on the final commit — `gh run watch` without an ID is
interactive, so resolve it first:

```bash
RUN_ID=$(gh run list --commit $(git rev-parse HEAD) --json databaseId -q '.[0].databaseId')
gh run watch $RUN_ID --exit-status
```

## Review gate

1. `/review` — two independent reviewers over the branch diff (Sonnet-class
   per subagent routing; this is a multi-crate change touching a
   serialization boundary — the mandatory case).
2. Reconcile findings; verify every referenced symbol exists before acting
   (reviewers hallucinate too). Stricter severity wins on disagreement.
   Adjudications go to this plan directory's `10-review-log.md` (the
   reserved review-log home, shared with the package-06 checkpoint pass) —
   every rejected finding gets a one-line reason there.
3. `/fix-review`, re-verify (the command list above), iterate until both
   reviewers approve. Non-trivial fixes get their own review pass.
4. Cross-repo diffs (control-plane package 02; the exploration-orchestrator
   request entry) are part of the review scope — hand reviewers those paths
   explicitly, they won't find sibling repos on their own.

## Evidence consolidation (`evidence/phase5-m1-m2/`)

- `m0-writebatch-bench.txt`, `m1-determinism-gate.txt` (test output),
  `m1-killnine.txt` (full 1M local), `m1-throughput.txt` (hardware named;
  "not run on Spark" recorded if so), `m2-rollup-properties.txt`,
  `m2-derived-fixture.txt`. Each file: exact command, machine, date, raw
  numbers. Skipped-or-reduced runs are labeled as such — no implied
  coverage.

## Beads + handback

- `bd close <id> -r "evidence: <test names / evidence file / commit>"` per
  package bead, serially.
- File follow-up beads for the known deferrals:
  - live-orchestrator integration smoke (blocked on
    exploration-orchestrator M5; depends-on note referencing their request
    entry `phase5-observatory-event-contract-conformance`),
  - aarch64 CI leg if it could not be provisioned (package 04),
  - Spark-hardware throughput/soak evidence if not yet run,
  - M3 SSE/UI consumers of the broadcast channel (next milestone, not
    debt — only file if something in 05's channel shape was compromised).
- Repo docs sync: README gains a short "state of the repo" section (M0–M2
  done, what runs, how to fire up the demo: daemon + `obs-events-gen`
  + curl), pointer to `docs/event-contract-reconciliation-v1.md` and
  `docs/proto-pin.md`.
- Push and confirm: `git status` up to date with origin in **all three**
  touched repos (observatory, control-plane, exploration-orchestrator) —
  verify `pwd`/`git remote -v` before each.

## Acceptance

- All verification commands above pass locally; CI green (both legs +
  determinism gate) on the final commit of each touched repo.
- Both reviewers approved the final state; each rejected finding has a
  one-line reason recorded in `10-review-log.md` (this plan directory).
- Every plan bead closed with `-r` evidence or converted to an explicit
  follow-up bead; `bd ready` shows only intentional follow-ups.
- Handback summary (final session message) states: what M1/M2 acceptance
  bullets were discharged and by which evidence, what is pending and why —
  specifically that **M1 acceptance was discharged against `obs-events-gen`
  by design**, with live-stream integration gated on orchestrator M5.

## Failure guidance

- If reviewers disagree on a D-decision from package 01 (e.g., D7 ack
  semantics): the decision log is the arbiter for *what was intended*; a
  reviewer claiming the intent is wrong escalates to the user rather than
  silently re-deciding — contract semantics are cross-repo.
- If CI is green but a runtime smoke fails: fix before commit — "it builds"
  is not "it works"; the smoke exists precisely because ingest wiring bugs
  (ports, config paths) don't compile-fail.
- If a push lands mid-way (observatory pushed, control-plane not): finish
  the set; the proto pin in observatory CI references the control-plane
  commit — a half-pushed set leaves observatory CI red for everyone else.
