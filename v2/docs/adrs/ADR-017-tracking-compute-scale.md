# ADR-017: Tracking compute at scale — exact skip key, one frame build, deadline and resume

## Status

Accepted (2026-11-06). Spec `docs/specs/2026-11-05-v2-tracking-compute-performance-design.md` and plan
`docs/plans/2026-11-06-v2-tracking-compute-performance-plan.md`, both A/B-reviewed and owner-approved. Uncommitted,
pending the owner's commit approval. Item 8 (line_breaking) records the Mac e2e measurement (plan Task 9 Step 6,
2026-11-23). Amended 2026-11-23 (item 9): pitch_control and shape_graphs are excluded from the default run. Amended 2026-11-23 (item 4): the key reads push the match filter down.

## Context

- The calc layer (pitch_control, shape_graphs, line_breaking) was too slow for the governing runtime: the HF scheduled
  job `lakehouse-ingest` on `cpu-basic` (2 vCPU, 16 GB), `--timeout 21600`. HF sends SIGKILL about 2 s after SIGTERM,
  so on HF only a deadline the run sets for itself ends a run cleanly.
- The old per-step skip keys were not exact: they hashed a subset of the built frame (`player_id`, `period_id`,
  `frame_id`, `x`, `y`) or of `fct_actions`, so a change to `team_id`, `is_goalkeeper` or most action columns did not
  re-compute. They also needed the whole frame built before the skip decision.
- Owner decisions (2026-11-06): HF governs (the lake holds public data only); the cold backfill completes across
  nightly runs (resume); add numba and pin the parity oracle to it; no in-process parallelism this cycle (HF scale-out
  with parallel `cpu-upgrade` tasks is later, owner-scheduled work); **no downsampling** ("gold standard with no loss
  in quality": every tracking frame is scored).

## Decision

1. **Change classification rule.** An *optimization* keeps the output rows the same (gate: a parity oracle on real
   production-scale frames). A *backend change* runs the same algorithm with another floating-point order (gate: a
   recorded maximum absolute difference inside a stated tolerance). A *scope change* computes a different set of rows
   and is not allowed without the owner's explicit sign-off. The parity comparator is the per-match output rows
   without audit columns, sorted by the table's keys, compared value for value.
2. **Lever N — numba (backend change).** `silly-kicks[parse-dfl,numba]`; the parity oracle runs on numba;
   `compute.backend.BACKEND_TOLERANCE = 1e-9` against numpy. `numba_active()` is logged per run ("compute backend").
3. **Lever S — one frame build per match.** pitch_control and shape_graphs run as one step group
   (`TrackingFrameMetrics`): one frame build, two outputs, each with the semantics of a separate step.
4. **Lever K — an exact skip key, decided before the frame build.** The key has three parts:
   - *Input content*, computed in DuckDB over the SAME declared reads the step makes (`frames.FRAME_INPUT_READS`, and
     `ACTIONS_INPUT_READ` for line_breaking): `sum` of the 128-bit MD5 of `to_json(row)` (as two `HUGEINT` halves) plus
     the row count. Independent of row order, sensitive to duplicates, every column of every read. Control reads
     (match discovery, presence checks, visibility, the skip lookup) are declared separately and stay out of the key;
     a recording-connection test fails on any read in neither declaration.
   - *Code digest*, derived, never hand-kept: SHA-256 of the docstring-free `ast` dump of every module in
     `TRACKING_CODE_MODULES`. A guard test fails if a `lakehouse` module the builder or a scorer imports is in neither
     that list nor `INFRA_MODULES`. `INFRA_MODULES` are modules that read, write, log or control but never shape an
     output value (the write loop, the key itself, the backend flag, bronze, catalog, logging, hashing, the error
     sink, config); a change to them does not re-compute.
   - *Library versions* of every library on the path (`TRACKING_LIBRARIES`), with a guard test that maps each
     third-party import of the declared modules to its distribution.
   The combined key is stored as each output's `_source_hash`, so the existing skip comparison is unchanged.
   **One known exception to the "any input change re-computes" anti-goal:** `to_json` writes `-0.0` as `0.0`, so a
   change from `0.0` to `-0.0` alone does not change the key. The values are numerically equal, and no scorer branches
   on the sign of zero.
   **Read pushdown (amended 2026-11-23, spec §7.4a).** A filter on the derived `canonical_match_key` alone did not
   reach the gradientsports dedup window, so the steady state spent 56 min in skip checks. The frames and ball reads
   also filter on the match's provider record keys (`list_contains(?::BIGINT[], match_key)`). One declared control
   read finds those keys in `ops_canonical_pointers`. The canonical filter stays, so the row set is provably the same.
   `tracking_cross_provider` resolves pointers with scalar subqueries, because per-match filtering made the planner
   flip the pointer joins and hash on one thread. `fct_tracking_ball` does the same, so the owner ruling below holds
   for both views. Consequence (owner ruling 2026-11-23, "fail loud + writer
   guard"): a duplicate `(entity, record_key)` pointer no longer silently duplicates rows. A scalar lookup raises
   DuckDB's multiple-rows error in every read that evaluates it for the duplicated key. A duplicate match pointer
   fails the affected match's reads and every whole-view or canonical-only reader, because each one filters or
   projects `canonical_match_key`. A duplicate player or team pointer fails the affected match's reads and every
   reader that projects the looked-up key; a reader that projects none of them, such as `TRACKING_DISCOVERY_SQL`,
   may still succeed (spec §7.4a item 2). The resolution writer refuses to write such a pointer, so the error appears
   at its source.
5. **Lever R — deadline, durable progress, stop handling.** `ComputeConfig` (`compute.*`), calibrated on HF
   `cpu-basic` on 2026-11-24 (perf plan Task 11 Step 3; `docs/ops/deploy.md` gives each measurement): `budget_seconds`
   21300, `startup_allowance_seconds` 300, `finish_reserve_seconds` 450, `layer2_reserve_seconds` 7200,
   `worst_match_seconds` 105, `check_every_frames` 500 (the initial values were 18000, 600, 2700, 1800, 1200 and 500).
   The plan put the uninterruptible pre-check segment into `finish_reserve`; it is not added, because only line_breaking
   runs on HF and `worst_match` covers its whole match. Add it when pitch_control or shape_graphs return to the default
   run (calc-layer spec D10). A match that is already done still costs its skip check: on HF a median 7.9 s per
   line_breaking match and 0.5-0.7 s per SPADL match (the 2026-11-24 steady-state run).
   Per layer, hard deadline = budget − finish_reserve (layer 1 also leaves layer2_reserve); soft
   deadline = hard − worst_match. No match starts after the soft deadline; a scorer checks `should_stop()` every
   `check_every_frames` frames and raises `ComputeAbandoned` past the hard deadline or on a stop: the match writes
   nothing and counts as deferred. Each written match calls `checkpoints.maybe("compute")`, so progress publishes
   during the run. A stop skips every later `finish` phase, cleanup included, and goes to the run-end commit.
   `budget_seconds + startup_allowance_seconds` must stay within the job `--timeout` (a config test).
6. **Visible coverage, no silent gap.** A match not reached is *deferred*, not failed: `ComputeOutcome.deferred_matches`
   and one `compute_deferred` warning per step. The v1 rule "never silently substitute data" (v1 `AGENTS.md`) is
   adopted for v2: any later page that shows `fct_tracking_metrics` must show its coverage.
7. **Lever C — batched TTI: not built.** The gate was: build C only if TTI is ≥ 20 % of pitch_control's per-match
   time under numba. Measured on the Mac (largest match per provider, 2026-11-06): TTI is 6.5 % (gradientsports,
   56.2 s of 863.8 s), 3.7 % (idsse, 17.6 of 471.4 s) and 3.2 % (skillcorner, 7.2 of 229.9 s). numba alone cut TTI by
   about 63 % (gradientsports 152.4 → 56.2 s). C would not fuse the phases that dominate: player extraction
   (`extract`, gradientsports 325 s), ball resolution (`ball`, 91 s) and velocity derivation (182 s), all per-frame
   silly-kicks work. shape_graphs dominates the whole match (gradientsports 4,858 s wall) and numba does not change
   it. These are reported to silly-kicks (handoff `D:\Development\_handoffs\2026-11-06-sk-tracking-batch-gaps-handoff.md`).
8. **line_breaking: no chunk fallback (measured on the Mac, 2026-11-23, plan Task 9 Step 6).**
   `detect_line_breaking` is one silly-kicks call per match, so line_breaking is checked between matches only. The
   gate: build lakehouse-side action chunks (with a "chunked = whole-match" parity oracle) only if one match's
   `detect_line_breaking` time is above 30 s, half the local 60 s stop grace. Measured on the largest match per
   provider: gradientsports 28.5 s (1.5 s under the gate), idsse 14.8 s, skillcorner 6.3 s. The idsse number covers the incomplete idsse action set (D10: 18 to 27 actions per
   match), so the upgrade cycle re-runs the gate after the DFL derive (calc-layer spec D10 item 6). **Decision:** no
   chunk fallback is built. The slowest whole line_breaking match (key, frame build, scorer and write) in the
   recompute of all 169 matches was 35.2 s (median 7.5 s). A stop therefore waits at most about 35 s, inside the
   60 s grace. On HF `cpu-basic` the slowest whole match is 83.3 s, and it sets `worst_match_seconds` (105 s; item 5).
9. **pitch_control and shape_graphs leave the default run (owner, 2026-11-23; calc-layer spec D10).** The
   silly-kicks batch upgrade (`compute_pitch_control_batch`, `compute_shape_graphs`) replaces both scorers, and the
   silly-kicks cycle is late. `DEFAULT_COMPUTE_STEPS` is now `REGISTERED_COMPUTE_STEPS` minus
   `EXCLUDED_COMPUTE_STEPS` (the `TrackingFrameMetrics` group). The code, its skip key, its tests, the bronze tables
   and the mart columns stay. The Workflows page marks the two cards `excluded` and the cards below them `incomplete`,
   so the excluded and partial outputs are not silent (item 6). Two real-scale gates did not run in this cycle and move with the group
   to the upgrade cycle: the lever S oracle on the largest real match per provider, and the lever N backend bench on
   the Mac. The lever N tolerance stays proven on the fixtures and the Task 2 profile. The upgrade cycle puts the group
   back in the default run.

## Consequences

- A nightly run on `cpu-basic` stops cleanly inside its window and the next one continues: the cold backfill takes as
  many nights as the measured cost needs, and after it each night computes only new or changed matches.
- The skip key re-computes on any input change, any declared code change and any library change, and never on a
  docstring or format edit. The cost is one in-DuckDB scan of each match's input rows per run.
- A deferred match has no mart row until a later run reaches it; that is visible in ops, never silent.
- `Settings` is `extra="forbid"`: once a lake config holds a `compute.*` key, older code cannot load it. A rollback
  first restores the config without those keys, then redeploys the older image (`docs/ops/deploy.md`).
- Further speed comes from silly-kicks batch kernels and the owner's later HF scale-out, never from fewer frames.

## Alternatives rejected

- **Downsampling frames (round 1 lever B):** a scope change; the owner rejected it (no loss in quality).
- **In-process parallelism (round 1 lever A):** conflicts with "one heavy match at a time"; withdrawn by the owner.
- **An enumerated lineage fingerprint (round 2):** misses macro branches (`lake_table_present`) in the input views.
- **A hand-kept code-version constant:** easy to forget; the derived code digest replaces it.
