# Delta Concurrent-Commit Retry in `write_delta_table` — Design Spec

| Field | Value |
|---|---|
| **Date** | 2026-06-03 |
| **Status** | Approved (brainstorming, revised after review #1) — pending implementation plan |
| **Author** | Karsten Nielsen (architect) + Claude |
| **Branch** | `feat/ac1-commit-retry-fail-loud` (off `c8006c1`) |
| **ADR** | New **ADR-038** (cross-linked from ADR-037) |
| **Scope** | **Retry only.** Drain fail-loud + bounded quarantine split to a follow-up (see §10). |

## 1. Problem

The AC-1 worker-drain (PR #334, ADR-037, main `c8006c1`) had its first scoped serverless run
(`statsbomb` / `max_units=5`, run `730644476818402`) fail **4 of 5 games** with concurrent-commit
contention, confirmed from all five worker logs + the data:

- 5 drain workers each finished a fast statsbomb game in ~17 s and reached the Delta commit
  together at ~01:22:51 — all racing to write the SAME `_delta_log` version `…00035.json` on the
  shared `bronze.spadl_action_context`. Worker 3 (match 16010) won (2217 rows); workers 0/1/2/4
  (15978/15986/15998/16023) all got the **identical** `AmazonS3Exception 400 Bad Request` on
  `HEAD …/00035.json` at the `putIfAbsent`/CRC layer (`MultiClusterLogStore.writeCommit` →
  `S3CommitClient`, coordinated-commits + S3 conditional-write), within ~6 ms of each other.
- Delta's own `doCommitRetryIteratively` (in the stack) did **not** catch the raw S3-400 → 4 hard
  failures instead of retrying at v36/37….
- Not flakiness (4/4 identical, same version, same instant), not per-match (4 different matches),
  not data loss (`replaceWhere` honored; metrica/skillcorner/wyscout intact).
- The worker-drain makes it **deterministic**: persistent workers start together, so fast
  event-only games all commit at the same instant (the old per-chunk model staggered them via
  cold-starts; and statsbomb writes only began working in #334, so this was never exercised).

## 2. Scope (this PR = retry only)

This PR fixes the **contention** — the actual observed failure — with a bounded, jittered retry in
the shared `write_delta_table`. It does **not** add the drain fail-loud signal: that was found (in
review) to introduce a poison-pill (a deterministically-failing game would fail the for-each every
run → skip `dbt_build_output_marts` indefinitely, because the queue is run-scoped scratch with **no
attempt-count** and discovery is a pure anti-join). Fail-loud is only safe paired with a bounded
attempt-count + quarantine; that cohesive piece is split to a follow-up (§10). The silent-N/M-loss
observability gap is **pre-existing** (since #334) — deferring fail-loud one PR makes nothing worse
in total volume. **But the §10 follow-up is NOT optional and must land soon after:** retry *enriches*
the residual silent failures — before retry they were dominated by transient contention (self-healing
next run); after retry, contention mostly succeeds, so the failures that stay silent under a green
for-each are disproportionately the **genuine** ones (real data/processing bugs retry can't fix) —
exactly the ones you most want loud. A green for-each post-retry must **not** be read as "loss problem
solved." Tracked: [[project_ac1_drain_commit_contention_next_pr]].

## 3. Why retry is safe here (the load-bearing property)

Each worker writes a **distinct** `match_id` via `write_delta_table(..., replace_where="match_id =
'<id>'")` (`action_context.py:1382/1740/1869`). The only collision is on the `_delta_log` *version
filename*, never on data — disjoint `replaceWhere` predicates. So re-running a write under retry is
**idempotent**: the loser re-attempts and lands at the next version with the same disjoint
overwrite. This is the property that makes blind retry correct; without disjoint predicates, retry
could double-write.

## 4. Decisions (locked; revised after review)

1. **Retry lives in the shared `ingestion.utils.write_delta_table`** — DRY, fixes the
   concurrent-writer class for any caller. Gated so single-writer callers (the vast majority) never
   change behavior (they never raise a concurrent-commit error → loop succeeds on attempt 1).
2. **Retry matches the Delta conflict family AND the observed S3-400-at-commit signature** —
   matching only typed Delta conflicts would miss what actually broke.
3. **Attempt budget sized to the fan-out:** `_COMMIT_MAX_ATTEMPTS = 10` (≥ the for-each
   concurrency of 8, with headroom — a worker can lose the race more than once under 8-way
   contention). Comment ties the number to the 8-way fan-out so it isn't read as arbitrary. Cost of
   an extra event-only retry is ~seconds; cost of exhausting is a hard failure — so over-provision.
4. **New standalone ADR-038** (not an ADR-037 amendment): this is a **repo-wide** behavior change
   to a shared util (every caller now silently retries + may add latency under contention). A future
   maintainer debugging "why did this single-writer overwrite take 7 s longer once" must find it.
   Cross-linked from ADR-037.

## 5. Component — concurrent-commit retry in `write_delta_table`

`ingestion/utils.py:230 write_delta_table` builds a `writer` and calls
`writer.saveAsTable(full_table)` once (line 278). Factor the commit into a **pure helper** taking a
callable, so the wiring is unit-testable without Spark.

### 5.1 Matcher — `_is_concurrent_commit_error(exc) -> bool` (pure)

True if `str(exc)` contains EITHER a Delta conflict marker (`ConcurrentAppendException`,
`ConcurrentDeleteReadException`, `ConcurrentDeleteDeleteException`, `ConcurrentModificationException`,
`ProtocolChangedException`, `CommitFailedException`) OR the **S3-commit signature**: contains
`_delta_log` AND a commit-path marker (`putIfAbsent` / `writeCommit` / `MultiClusterLogStore` /
`S3CommitClient`) AND a 4xx marker (`Bad Request` / `Status Code: 4` / ` 400`). The
`_delta_log`+commit-path scoping means a `_delta_log` *read* 4xx or an unrelated 400 won't
false-match. Markers are module-level frozensets (ADR-002 §3 string-marker discipline).

### 5.2 `_commit_with_retry(commit_fn, table, logger) -> None` (pure)

Bounded loop (`_COMMIT_MAX_ATTEMPTS = 10`, base `0.5 s`, cap `20 s`) calling `commit_fn()`:
- Success → return.
- Non-retryable (not `_is_concurrent_commit_error`) or last attempt → re-raise.
- Retryable, attempts remain → **full jitter** sleep `random.uniform(0, min(cap, base·2^(n-1)))`
  (full jitter is essential — un-jittered retries re-collide in lockstep), `logger.warning` the
  retry.
- **Matcher-drift self-diagnosis (exhaustion only — no over-fire):** when a *retryable* error
  exhausts all `_COMMIT_MAX_ATTEMPTS` and is about to re-raise, `logger.error` a hint: "exhausted N
  concurrent-commit retries on <table> — contention severe, OR `_is_concurrent_commit_error` may have
  drifted (DBR message-format change); compare against `_S3_COMMIT_PATH_MARKERS`." Fires only on the
  genuinely-exhausted path, so it does not over-fire on legitimate `_delta_log` read 4xx errors. (A
  full runtime drift-hint on raw, never-retried S3-400s belongs in the fail-loud PR's raise message,
  where the symptom returns loudly; here it would over-fire.) The matcher itself carries a code
  comment naming the drift risk.

`write_delta_table` replaces the bare `writer.saveAsTable(full_table)` with
`_commit_with_retry(lambda: writer.saveAsTable(full_table), full_table, logger)`. All other branches
(`replace_where` / `mode` / `overwriteSchema`, lines 271-276) unchanged.

### 5.3 Cost note (tracking tier)

`saveAsTable` retry re-triggers the write DAG — trivial for event-only; for tracking it
re-materialises the `applyInPandas` result (recompute). Accepted: LPT spreads tracking across
workers and each tracking write is ~30 min, so they're naturally staggered and rarely co-commit.
The proper "compute once, commit-retry only" split is **blocked on serverless** (`.cache()/.persist()`
forbidden — CLAUDE.md), so it's genuinely out of scope. A tracking-tier retry tripwire (log ERROR
when a *tracking* unit retries) needs per-unit attempt-count + provider context → folded into the
follow-up (§10).

### 5.4 `merge_delta_table`

Not wrapped (its callers are single-writer today). Add a one-line comment in `merge_delta_table`
pointing at `_commit_with_retry` so the next concurrent-merge author doesn't re-derive the fix
(Hyrum's-law pointer; DRY).

## 6. Testing

**Pure unit (offline — this is the full automated coverage):**
- `test_is_concurrent_commit_error`: True for each Delta marker; True for the real S3-400-at-commit
  message snippet; **False** for a plain unrelated 400, a `_delta_log` read 4xx, a
  `TABLE_OR_VIEW_NOT_FOUND`, and a generic `ValueError`.
- `_commit_with_retry`: succeeds after N marked failures (asserts call count); re-raises a
  non-retryable immediately (1 call); exhausts after `_COMMIT_MAX_ATTEMPTS` then re-raises;
  **jitter-bound assertion** — capture the sleep arg (monkeypatch `time.sleep` to record) and assert
  each is within `[0, ceiling]` for its attempt (proves backoff is bounded + jittered, not just that
  it loops).

**No local concurrent-Spark test is possible:** pyspark is Databricks-runtime-only here (CLAUDE.md
line 238; Spark-touching tests `patch.dict(sys.modules, {"pyspark": MagicMock()})` — no real local
Delta). The callable-injection design puts the wiring (lambda capture, retry count, jitter, re-raise)
under offline unit tests; the real concurrent-commit **survival** is verified by the serverless
re-run (§7), consistent with the local-first rules (logic local, Spark-property serverless). If
local-Spark test infra is ever added, a threaded disjoint concurrent-write test is the right
addition — flagged for the test-infra/Monstah work.

**Offline gate:** ruff + ruff-format + pyright (0 errors) + import-linter. Full `uv run pytest`
(mirror CI — full suite, not subsets; PR #334 lesson).

## 7. Serverless verification (manual, post-merge)

Re-run `statsbomb` / `max_units=5`. Expectation: **all 5 write, for-each GREEN** (retry serialises
the commits across `_delta_log` versions). This is the proof the contention fix works; it cannot run
in offline CI. (Wait for the post-merge wheel deploy first — operator-runtime rule.)

## 8. Error handling / idempotency invariants

- Disjoint `replaceWhere` → retry is idempotent (§3).
- A write that exhausts retries raises → the unit's transaction never commits → the unit isn't in
  results → re-discovered next run by the skip-guard. (Today, with fail-loud deferred, the drain
  worker still logs `ac1_drain_unit_failed` ERROR per the existing #334 behavior and the task stays
  GREEN — the pre-existing silent-loss gap that the §10 follow-up closes.)
- Single-writer callers: no behavior change (never raise the conflict).
- Retry-path `logger.warning`/`logger.error` only (ADR-002 — no silent swallow; the loop re-raises).

## 9. Governance

- **ADR-038** "Delta concurrent-commit retry in write_delta_table" (Nygard format): context = the
  confirmed contention finding (run 730644476818402); decision = gated jittered retry in the shared
  util; consequences = **repo-wide** (all callers retry the conflict class; single-writer unaffected;
  up to ~tens of seconds added latency only under genuine contention); matcher-drift risk +
  self-diagnosing mitigation. Cross-link ADR-037.
- **CLAUDE.md** → Database Performance / Databricks: one bullet documenting the retry (so the
  added-latency-under-contention behavior is discoverable repo-wide).
- **Wheel bump** via `scripts/bump_wheel.py`. **C4**: no change (internal behavior).

## 10. Out of scope (explicit follow-ups)

- **Drain fail-loud + bounded quarantine (NEXT PR).** `drain_worker` drains all units then raises
  if any failed/timed-out (loud), **paired with** a persistent per-unit attempt-count + quarantine
  -after-K (excluded from the discovery anti-join + dead-letter ERROR/alert) so a poison game pages
  ONCE and stops wedging the build, instead of failing the for-each forever. Include the tracking
  -tier retry tripwire there (needs the attempt-count + provider context). This is the start of the
  long-overdue **Monstah** observability-everywhere work. See
  [[project_ac1_drain_commit_contention_next_pr]].
- A→B dynamic-claim load-balancing (Lakebase `SKIP LOCKED`) — separate deferred follow-up
  ([[project_ac1_drain_dynamic_claim_followup]]).
- Per-unit rich observability events (runtime/counts/stats) → Monstah.

## 11. Files

- Modify: `src/ingestion/utils.py` (markers, `_is_concurrent_commit_error`, `_commit_with_retry`,
  wire into `write_delta_table`, `import random` + `Callable`, `merge_delta_table` pointer comment).
- Tests: `src/tests/test_ingestion_utils.py` (matcher truth table + retry behavior + jitter bound).
- Docs: `docs/superpowers/adrs/ADR-038-delta-concurrent-commit-retry.md` (new),
  `docs/superpowers/adrs/ADR-037-*.md` (cross-link line), `CLAUDE.md`, wheel bump.
- **No change** to `drain.py` / `action_context.py` / Terraform this PR (fail-loud is §10).
