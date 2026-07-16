# 10 — Review Log (adjudication record)

Reserved home for review adjudications across this plan's two review
gates: the package-06 mid-plan checkpoint (M1 boundary) and package 09's
final dual-review pass. Written during execution, not planning.

Format per pass: date, reviewers, then one row per finding with its
disposition — `applied` (naming the fixing commit) or `rejected` with a
one-line reason. No silent rejections. Conflicting reviewer findings are
reconciled here per the review-workflow: stricter severity wins unless
the claim is disproven against source, and no finding is actionable until
its referenced symbol/API is confirmed to exist.

## Checkpoint review (after package 06; ran with 08 already landed)

Date: 2026-07-16. Reviewers: (a) the implementing session's independent
critical pass over the full 0f668c8..HEAD diff; (b) a subagent reviewer
(three launches terminated server-side with API 529 Overloaded before
producing findings — retried at the final pass; its eventual findings are
recorded under the final-review section).

Findings from pass (a):

| # | Severity | Finding | Disposition |
|---|---|---|---|
| 1 | Important | Rollup promotion could advance the coarse grain's high-water past 5s rows not yet folded (fine ticker lag), permanently excluding them from 1m/10m (`obs-derive/src/ticker.rs::promote`) | applied — 80e54ba caps the promotion boundary at the source grain's folded frontier |
| 2 | Important | `obs-events-gen publish --resume` loops forever when the stream's tail is a rejected envelope (rejections are never acked, so `acked_seq` cannot reach `final_seq`) | applied — 80e54ba stops after two clean completions with zero ack progress |
| 3 | Minor | Aborted daemon tasks holding `WriterHandle` clones must be awaited before joining the writer thread or shutdown deadlocks | applied — already fixed in 1819b04 (`observatoryd/src/main.rs`), after the same class of bug was found live in the obs-scrape e2e test (7ebd64f) |
| 4 | Checked, no issue | AckTracker seed/advance race: `flush()` advances only after the writer's post-commit completion and reads after advancing; the seed merge takes `max(entry, seeded)` — monotone under concurrency | no change |
| 5 | Checked, no issue | Coverage upsert tie-break: SQLite evaluates all `DO UPDATE SET` expressions against the pre-update row, so `best_node_id`'s CASE and `best_score`'s max cannot observe each other | no change |
| 6 | Checked, no issue | `tokio::time::timeout` around `Streaming::next()` in the ingest linger loop is cancel-safe (items are only consumed on `Poll::Ready`) | no change |

## Final review (package 09)

Date: 2026-07-16. The plan asks for two independent reviewers. Reviewer
availability was constrained: five consecutive Claude-subagent review
launches terminated server-side with API 529 (Overloaded) before
producing findings. The dual review was completed with:

- Reviewer 1: the implementing session's independent critical pass
  (findings 1–6 in the checkpoint section above; both code findings
  fixed in 80e54ba).
- Reviewer 2: GPT (`openai/gpt-5.4-fast` via the OpenCode wrapper —
  the house dual-AI review path), scoped to the highest-risk store/ingest
  surfaces (projections.rs, writer.rs, service.rs, ack.rs).

Reviewer-2 findings:

| # | Severity | Finding | Disposition |
|---|---|---|---|
| 1 | High | `replays` cache upserts are not monotone: a delayed `replay-job-progress` inserted after `replay-job-completed` rewinds status/pct/updated_ns (`projections.rs` replay arm) | applied — progress/completed upserts now guarded `WHERE excluded.updated_ns >= updated_ns`; artifact-registered keeps `max(updated_ns, …)` (it only coalesces URIs) |
| 2 | Medium | `coverage_cells.last_ns` can move backwards on out-of-order `ts_wall_ns` | applied — `last_ns = max(...)`, plus `first_ns = min(...)` for the symmetric case |
| 3 | — | `obs-ingest/src/ack.rs`: clean | no change |

Post-fix verification: full workspace tests green (27 targets), clippy
`-D warnings` clean, release determinism gate re-run green (101.7 s under
concurrent load), 100k chaos re-run green at final HEAD.
