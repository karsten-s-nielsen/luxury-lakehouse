# SPADL/VAEP Chunked Execution

**Date:** 2026-05-17
**Scope:** Single PR — refactor `compute_spadl_vaep` into preflight + for_each_task pattern
**Depends on:** No external dependencies

## Problem

`compute_spadl_vaep` is a monolithic task with a 30-minute timeout that performs both SPADL conversion (5 providers) and VAEP scoring in a single invocation. When the backlog exceeds ~200 matches (e.g. after a table rebuild or new provider onboarding), the task times out at 30 minutes.

The tracking context pipeline already solved this problem via the guard + `for_each_task` chunking pattern (ADR-024). The VAEP pipeline must adopt the same architecture.

## Current Architecture

```
compute_spadl_vaep (single task, 30 min timeout)
  |- Guard: _VaepGuard.check() discovers work
  |    Stage 1: source events NOT IN spadl_actions -> new_spadl_match_ids
  |    Stage 2: spadl_actions NOT IN vaep_action_values -> unscored_vaep_match_ids
  |- Phase A: Convert ALL new events -> spadl_actions (5 providers, sequential)
  |- Phase B: Load VAEP models from MLflow
  +- Phase C: Score ALL unscored matches via applyInPandas -> vaep_action_values
```

All work happens in one 30-min window. No fan-out. No incremental progress saved if the task times out mid-execution.

## Target Architecture

```
preflight_spadl_vaep (new task, 5 min timeout)
  |- Guard: _VaepGuard.check() discovers work (unchanged logic)
  |- Emit task value: spadl_vaep_chunks (list[str])
  +- Emit task value: vaep_model_path (UC Volume path string)

compute_spadl_vaep (for_each_task, 30 min timeout per iteration)
  |- inputs: {{tasks.preflight_spadl_vaep.values.spadl_vaep_chunks}}
  |- Parse chunk string (validated against provider allowlist)
  |- Phase A: Convert chunk's matches -> spadl_actions (single provider per chunk)
  |- Phase B: Load models from UC Volume path, score chunk -> vaep_action_values
  +- replaceWhere scoped to chunk match_ids (idempotent)
```

## Chunk Grammar

```
chunk        := convert_chunk | score_chunk
convert_chunk := provider ":" match_id_list
score_chunk  := "score:" match_id_list
provider     := "statsbomb" | "wyscout" | "idsse" | "metrica" | "skillcorner"
match_id_list := BIGINT ("," BIGINT)*
```

- `convert_chunk`: iteration runs Phase A (conversion) + Phase B (scoring).
- `score_chunk`: iteration skips Phase A, runs Phase B only (actions already exist in `spadl_actions`).

The iteration entry point validates the provider prefix against the allowlist `{"statsbomb", "wyscout", "idsse", "metrica", "skillcorner", "score"}` and raises `SystemExit` on anything else. Match IDs are validated as integers before interpolation into `replaceWhere` predicates.

## Design Decisions

### 1. Two-phase chunks (conversion + scoring in same iteration)

Each chunk converts events -> SPADL and then immediately scores those actions with VAEP. This avoids a second fan-out pass and keeps the pipeline idempotent and self-healing per chunk. If an iteration crashes between the SPADL write and the VAEP write, the partial state (actions converted but not scored) is automatically discovered by the next run's Stage 2 guard and emitted as a `score:` chunk.

**Rationale:** The current pipeline already does both phases sequentially. Splitting into two separate for_each_task passes (one for conversion, one for scoring) would double infrastructure cost and require an intermediate synchronization point. The per-match workload is small enough (~1600 actions per match, ~5 MB) that both phases fit within a single 30-min iteration even for large chunks.

**Note:** Two separate Delta writes (`spadl_actions` then `vaep_action_values`) means the iteration is NOT transactional. The guarantee is idempotency + self-healing, not atomicity.

### 2. Chunk sizes

| Provider | Matches per chunk | Rationale |
|----------|------------------|-----------|
| statsbomb | 200 | ~500 events/match, light conversion, bulk of volume |
| wyscout | 200 | Similar weight to StatsBomb |
| idsse | 50 | Heavier conversion (DFL XML parsing + home_team resolution) |
| metrica | 50 | Similar to IDSSE |
| skillcorner | 50 | Similar to IDSSE |
| score (unscored) | 200 | Scoring only — handles self-healing of partial failures (converted but not scored) |

### 3. Guard metadata — count contract

Current `FilterResult.metadata`:
```python
{
    "new_spadl_match_ids": sorted(new_spadl),       # union of all providers
    "unscored_vaep_match_ids": sorted(unscored),
}
```

New `FilterResult.metadata` (per-provider keys replace the union key):
```python
{
    "sb_new": sb_new,
    "ws_new": ws_new,
    "idsse_new": idsse_new,
    "metrica_new": metrica_new,
    "sc_new": sc_new,
    "unscored_vaep_match_ids": sorted(unscored),
}
```

**Count contract:** `count = len(sb_new) + len(ws_new) + len(idsse_new) + len(metrica_new) + len(sc_new) + len(unscored)`. Each match appears in exactly one list (providers are mutually exclusive; the production invariant `new_spadl ∩ unscored = {}` holds because a match is EITHER unconverted OR converted-but-unscored, never both). The `test_guard_conformance` sum-of-list-lengths check passes without exemptions.

The union key `new_spadl_match_ids` is removed — it served no downstream purpose beyond the conformance contract, which is now satisfied by the per-provider keys directly. Confirmed consumers (grep `new_spadl_match_ids` across `src/`): `spadl_vaep.py:220` (emitter), `spadl_vaep.py:666` (consumer in `run_pipeline`), `test_spadl_vaep.py` (assertions). Only `run_pipeline` reads it; it must switch to computing the union from per-provider keys.

### 4. VAEP model loading — UC Volume path

Preflight:
1. Resolves MLflow `@Champion` model URIs for `vaep_scores` and `vaep_concedes`.
2. Downloads model artifacts (XGBoost booster raw bytes, ~200 KB each).
3. Writes both to a run-scoped UC Volume path: `/Volumes/soccer_analytics/dev_gold/model_weights/vaep_cache/{run_id}/scores.xgb` and `concedes.xgb`. Uses existing `model_weights` Volume (managed by Terraform, used by `artifact_deploy.py`).
4. After both files are written successfully, emits the run-scoped directory path as task value `vaep_model_path` (short string, well within 48 KB limit).

Each iteration:
1. Reads `vaep_model_path` from task value.
2. Loads model bytes from UC Volume via FUSE (`open(path, 'rb').read()`). Requires DBR 14.3+ (confirmed: serverless uses DBR 15.4+).
3. Deserializes XGBoost booster from raw bytes.

**Why not per-iteration MLflow resolution:** MLflow `@Champion` resolution is a network call (~5s). Across 50 iterations that's ~4 min of pure overhead + risk of transient `HTTPError` on any single iteration. The preflight resolves once, writes bytes, iterations read locally.

**Atomic write guarantee:** The task value is emitted only AFTER both model files are written. If preflight crashes after writing `scores.xgb` but before `concedes.xgb`, no task value is emitted → `for_each_task` never runs → no iteration sees stale/mismatched models. The run-scoped directory (`{run_id}`) prevents interference between concurrent or retried preflight runs.

**Cleanup:** Old run-scoped directories accumulate (~400 KB each). Preflight deletes directories older than 7 days on each invocation. Non-critical — stale directories waste negligible space.

### 5. Unscored matches (Stage 2) handling

Matches already in `spadl_actions` but not yet in `vaep_action_values` (the "unscored" set from the guard's Stage 2 diff) are emitted as `"score:match_id1,match_id2,...,match_idN"` chunks. The iteration recognizes the `score:` prefix, skips conversion (Phase A), and proceeds directly to scoring (Phase B).

These are chunked at 200 matches per batch (scoring is lighter than conversion + scoring).

### 6. Concurrency

`for_each_task` concurrency: **4** (same as tracking context).

No workspace session conflict: `compute_tracking_context` depends on `compute_spadl_vaep` completing, so both for_each_task fans never overlap.

### 7. Idempotent writes

- SPADL conversion: `replaceWhere = "data_source = '{provider}' AND match_id IN ({ids})"`.
- VAEP scoring: `replaceWhere = "match_id IN ({ids})"`.

**Validation:** Provider string is checked against the allowlist `_VALID_PROVIDERS = {"statsbomb", "wyscout", "idsse", "metrica", "skillcorner"}` before interpolation. Match IDs are validated as integers via `int(mid)` — non-integer values raise `ValueError` before reaching the SQL string. This matches the existing `_make_statsbomb_replace_where` / `_make_wyscout_replace_where` pattern which also casts to `int` before interpolation.

### 8. Parent task timeout

`timeout_seconds: 0` on the parent `for_each_task` wrapper means "no timeout" (Databricks documentation: "A value of 0 means no timeout"). Confirmed by existing production usage: `compute_tracking_context` parent has `timeout_seconds: 0` and has been live since 2026-05-14 without issue.

## Worst-Case Timing (Full Rebuild)

Source match counts (as of 2026-05-17):
- StatsBomb: ~4500 matches -> 23 chunks at 200/chunk
- Wyscout: ~3600 matches -> 18 chunks at 200/chunk
- IDSSE: ~10 matches -> 1 chunk at 50/chunk
- Metrica: ~5 matches -> 1 chunk at 50/chunk
- SkillCorner: ~10 matches (A-League only, current) -> 1 chunk at 50/chunk
- **Total: ~44 convert chunks**

Each convert iteration does both conversion AND scoring (§1). After run 1, all matches are both converted and scored. No second run needed unless an iteration partially failed (converted but crashed before scoring) — in which case the next run's Stage 2 guard discovers those unscored matches and emits `score:` chunks to complete the work.

**Full rebuild:** 44 chunks / 4 concurrency = 11 waves x ~10 min = **~110 min wall-clock**.

**Steady state (1-5 new matches):** 1 chunk, completes in <5 min.

## Entry Points

| Entry point (pyproject.toml) | Function | Purpose |
|------------------------------|----------|---------|
| `preflight_spadl_vaep` | `spadl_vaep:main_preflight` (new) | Guard + chunk emission + model caching |
| `compute_spadl_vaep` | `spadl_vaep:main` (modified) | Per-chunk conversion + scoring |

## Job Definition Changes

Replace the current single `compute_spadl_vaep` task with:

```json
{
  "task_key": "preflight_spadl_vaep",
  "depends_on": [
    {"task_key": "backfill_statsbomb_extra"},
    {"task_key": "ingest_idsse_events"},
    {"task_key": "ingest_metrica"},
    {"task_key": "ingest_skillcorner"},
    {"task_key": "ingest_wyscout"}
  ],
  "python_wheel_task": {
    "entry_point": "preflight_spadl_vaep",
    "package_name": "luxury_lakehouse",
    "parameters": ["--catalog", "soccer_analytics", "--schema", "bronze"]
  },
  "timeout_seconds": 300
}
```

```json
{
  "task_key": "compute_spadl_vaep",
  "depends_on": [{"task_key": "preflight_spadl_vaep"}],
  "for_each_task": {
    "concurrency": 4,
    "inputs": "{{tasks.preflight_spadl_vaep.values.spadl_vaep_chunks}}",
    "task": {
      "task_key": "compute_spadl_vaep_iteration",
      "python_wheel_task": {
        "entry_point": "compute_spadl_vaep",
        "package_name": "luxury_lakehouse",
        "parameters": [
          "--catalog", "soccer_analytics",
          "--schema", "bronze",
          "--match-ids", "{{input}}"
        ]
      },
      "timeout_seconds": 1800
    }
  },
  "timeout_seconds": 0
}
```

## Downstream Dependencies

`preflight_tracking_context` currently depends on `compute_spadl_vaep`. This dependency remains unchanged — it now depends on the `for_each_task` wrapper completing (all iterations succeed).

## Observability

Databricks `for_each_task` natively provides per-iteration status, duration, and log access in the job run UI. Each failed iteration shows its error message and can be inspected individually. No custom observability infrastructure needed — the platform provides it.

## Migration

1. Deploy wheel with new `main_preflight` entry point + modified `main` (accepts `--match-ids`).
2. Update Databricks job definition: replace monolithic task with preflight + for_each_task.
3. No table schema changes. No data migration. Fully backward-compatible output.

## `--match-ids` Argument Parsing

`main()` gains an optional `--match-ids` argument (same pattern as `tracking_context.py:_parse_tracking_match_ids_arg`):

- **Present (chunk mode):** Parses chunk string per the grammar above. Runs conversion + scoring for that chunk only.
- **Absent (backward-compatible mode):** Runs the full monolithic pipeline (all providers, all unscored). Preserved for local development and debugging. Logs a deprecation warning pointing operators to the preflight + for_each_task path.

The parser validates the provider prefix against `_VALID_PROVIDERS | {"score"}` and validates match IDs as integers. Invalid input raises `SystemExit` with a descriptive message.

## Test Plan

- [ ] `test_guard_conformance.py` — `_VaepGuard` count contract holds with per-provider metadata (no double-counting)
- [ ] New `test_spadl_vaep_preflight.py` — chunk format round-trip, chunk grammar validation, model path emission
- [ ] Model cache round-trip: serialize XGBoost booster → write to temp path → read from path → predict on known input → assert expected output
- [ ] Existing `test_spadl_vaep_writer_parity.py` — schema unchanged
- [ ] Chunk parser rejects invalid provider prefixes + non-integer match IDs
- [ ] `--match-ids` absent → monolithic path still works (backward compat)
- [ ] Manual: trigger with empty `spadl_actions` (full backfill) — all iterations complete within 30 min each, total ~110 min
- [ ] Manual: trigger with 5 new matches only — single iteration, completes in <5 min

## Risks

| Risk | Mitigation |
|------|-----------|
| Task value size limit (48 KB) for chunk list | At 200 matches/chunk with ~10-char IDs, 50 chunks = ~10 KB. Safe. Model path is a short string (~80 chars). |
| UC Volume FUSE read latency per iteration | XGBoost models are ~200 KB each. FUSE read is <1s. Negligible vs 10 min iteration. |
| Partial failure: some matches converted but not scored | Next run's Stage 2 guard discovers unscored matches and emits `score:` chunks. Self-healing by design. |
| Chunk size too large for peak load | Monitor iteration durations post-deploy; adjust `chunk_sizes` dict in code (no job definition change needed). |
| Full rebuild takes ~110 min wall-clock | Acceptable for rare event (table rebuild). Steady-state is <5 min. Operator should expect ~2 hours on first deploy. |
