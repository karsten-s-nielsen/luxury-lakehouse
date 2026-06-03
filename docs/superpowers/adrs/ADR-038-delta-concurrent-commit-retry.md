# ADR-038: Delta concurrent-commit retry in `write_delta_table`

| Field | Value |
|---|---|
| **Date** | 2026-06-03 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The AC-1 worker-drain (ADR-037, PR #334, main `c8006c1`) runs up to 8 persistent drain workers
concurrently, each writing one match's action-context rows to the **single shared** table
`bronze.spadl_action_context` via `write_delta_table(..., replace_where="match_id = '<id>'")`.

The first scoped serverless run (`statsbomb` / `max_units=5`, run `730644476818402`) wrote **1 of 5
games**; the other 4 failed at the Delta commit. All five worker logs confirm the mechanism:

- 5 workers each finished a fast statsbomb game in ~17 s and reached the commit together at
  ~01:22:51 — all racing to write the SAME `_delta_log` version `…00035.json`.
- Worker 3 won (2217 rows). Workers 0/1/2/4 got the **identical** `AmazonS3Exception 400 Bad
  Request` on `HEAD …/00035.json` at the `putIfAbsent`/CRC layer (`MultiClusterLogStore.writeCommit`
  → `S3CommitClient`, coordinated-commits + S3 conditional-write), within ~6 ms of each other.
- Delta's own `doCommitRetryIteratively` (in the stack) did **not** catch the raw S3-400 → 4 hard
  failures instead of retrying at v36/37….

Confirmed: not flakiness (4/4 identical, same version, same instant), not per-match (4 different
matches), not data loss (`replaceWhere` honored; metrica/skillcorner/wyscout intact). The
worker-drain makes it **deterministic** — persistent workers start together, so fast event-only
games all commit at once (the old per-chunk model staggered them via cold-starts).

**Mechanism honesty (evidence-before-claim):** the logs evidence *what* happened, not *why* a
conditional-write conflict surfaces as a malformed-request `400` rather than the canonical `409
Conflict` / `412 Precondition Failed`. Two readings: (a) known UC coordinated-commit-under-contention
behavior (retry is exactly right), or (b) a transient `S3CommitClient` bug under concurrency (retry
is a fine mitigation, but a Databricks support case is warranted). **A Databricks support case should
be filed to confirm.** The fix is correct under either reading; the matcher is scoped to the commit
path so it does not over-retry unrelated 4xx errors.

## Decision

Wrap the commit in `ingestion.utils.write_delta_table` in a bounded, **fully-jittered** retry that
triggers ONLY on the concurrent-commit error class:

- **Matcher** (`_is_concurrent_commit_error`): the typed Delta conflict family
  (`Concurrent{Append,DeleteRead,DeleteDelete,Modification}Exception`, `ProtocolChangedException`,
  `CommitFailedException`) **OR** the S3-commit signature (`_delta_log` AND a commit-path marker
  `putIfAbsent`/`writeCommit`/`MultiClusterLogStore`/`S3CommitClient` AND a 4xx marker). Marker-based
  because Spark Connect returns these as stringly-typed `SparkException`s (ADR-002 §3 discipline).
  The `Status Code: 4` 4xx marker is **deliberately broad** so a future Databricks change of the
  400 → 409/412 keeps matching.
- **Backoff:** `_COMMIT_MAX_ATTEMPTS = 10` (sized to the 8-way for-each fan-out — a worker can lose
  the race more than once under 8-way contention), base 0.5 s, cap 20 s, full jitter
  (`random.uniform(0, ceiling)`). Full jitter is essential — un-jittered retries re-collide in
  lockstep.
- **Safety:** AC-1 workers write **disjoint** `replaceWhere` partitions (distinct `match_id`); the
  only collision is on the `_delta_log` version filename, never on data. So retry is **idempotent** —
  the loser re-attempts and lands the same disjoint overwrite at the next version.

## Alternatives considered (eliminate vs mitigate)

`_delta_log` serialization is inherent to a single Delta table, so the only ways to **eliminate**
(not mitigate) contention are: (a) serialize commits via a single-committer / the work-queue, or (b)
split into multiple tables. Both are larger changes. **Retry is the pragmatic mitigation;** the
dynamic-claim follow-up ([[project_ac1_drain_dynamic_claim_followup]]) is partial elimination. We
chose retry because the writes are disjoint (idempotent under retry), Delta is designed for
concurrent writers via commit-retry, and it fixes the observed failure with no topology change.

## Consequences

### Positive
- Concurrent drain workers writing one Delta table now succeed (they serialise across `_delta_log`
  versions). Fixes the observed 4/5 loss.
- DRY: any future concurrent writer through `write_delta_table` is covered.

### Negative / scope
- **Repo-wide behavior change.** EVERY `write_delta_table` caller now retries this class. Under
  genuine contention a write can add latency — worst case ~91 s across 10 attempts (attempts 7–10 are
  pinned at the 20 s cap; expected ~46 s), well under the 1800 s per-game watchdog. A future
  maintainer seeing "why did this overwrite take 7 s longer once" should find the answer here.
- **No behavior change for single-writer callers** — they never raise the conflict, so the loop
  succeeds on attempt 1.
- `ProtocolChangedException` / `CommitFailedException` are included as **Delta's own retryable
  family** — slightly wider than pure peer-worker contention (a one-shot protocol change would burn
  up to 10 harmless retries before raising). Bounded attempts make this safe; named so a reader isn't
  surprised.
- **Matcher-drift risk:** stringly-typed on JVM messages — a DBR upgrade could change the format and
  silently stop matching, reopening the failure. Mitigation: `_commit_with_retry` logs an ERROR on
  exhaustion ("contention severe OR matcher drifted; compare against `_S3_COMMIT_PATH_MARKERS`"), and
  the matcher carries a drift-risk comment. (A full runtime drift-hint on raw never-retried S3-400s
  belongs in the fail-loud follow-up's raise message, where the symptom returns loudly.)

### Neutral
- `merge_delta_table` is NOT wrapped (single-writer today) but carries a pointer comment to
  `_commit_with_retry` for the next concurrent-merge author.

## CLAUDE.md Amendment

Database Performance → Databricks: documents the new retry so the added-latency-under-contention
behavior is discoverable repo-wide.

## Related

- **ADR-037** — the worker-drain fan-out that exposed this contention (run 730644476818402).
- **Follow-up (not optional):** drain fail-loud + per-unit attempt-count + quarantine-after-K
  ([[project_ac1_drain_commit_contention_next_pr]]). Retry shrinks the silent-failure volume but
  *enriches* the residual with genuine (non-contention) failures — exactly the ones to surface loudly
  — so a green for-each post-retry must not be read as "loss solved."
- **Code:** `src/ingestion/utils.py` (`_is_concurrent_commit_error`, `_commit_with_retry`,
  `write_delta_table`). **Tests:** `src/tests/test_ingestion_utils.py`.
