# Data Integrity Foundation — Design Spec

**Date**: 2026-04-14 (drafted) / 2026-04-15 (rewritten after session 40 investigation)
**Branch**: `feat/gold-data-repair`
**Scope**: SPADL goal encoding (D57) + UDF silent-swallow removal + warm-tier hook schema drift (`assert_warm_tier_not_empty` blocker) + systemic silent-swallow audit across src/, scripts/, and hf_taipy_app/

**Deferred to a follow-up cycle**: D45 (Football2vec v2 StatsBomb coverage + v1 deletion). Rationale: data integrity is foundational; downstream feature work waits until the integrity baseline is secured.

---

## Spec correction history

This spec was drafted on 2026-04-14 with a DIFFERENT (and **WRONG**) D57 diagnosis. The original §4.1 blamed `_flatten_extra()` in `silly_kicks/spadl/statsbomb.py` and proposed a narrow `replace_where` fix as the primary remediation. Session 40 (2026-04-14, ~22h of investigation) discovered TWO deeper root causes that were hidden behind silent-exception-swallow anti-patterns:

1. **`_VaepGuard.check()` metadata staleness** (`spadl_vaep.py:102-121`) — Stage 2's `unscored_vaep_match_ids` was computed from `find_new_ids(spadl, vaep)` BEFORE Stage 1 runs. When Stage 1 repopulated statsbomb in the same run, Stage 2 skipped it with stale metadata and reported SUCCEEDED with zero rows written.
2. **3 UDF silent swallows** in `spadl_vaep._make_scoring_udf` + `spadl_conversion._make_sb_spadl_udf` + `spadl_conversion._make_ws_spadl_udf` — `except Exception: return empty_df` inside Spark `applyInPandas` closures silently dropped per-group data loss.

**Neither was visible in the original investigation because the silent-swallow patterns hid the ground truth.** The spec's §4.1 + §4.2 are rewritten below.

The original spec is preserved in git history as `69ad7e4f` (plan + spec combined snapshot) and the session 40 memory files (`project_spadl_vaep_chain.md`, `project_warm_tier_blocker.md`) provide citable retractions of the earlier diagnosis.

---

## 1. Problem Statement

Three linked data-integrity defects observed or hidden in the gold layer:

1. **D57 SPADL goal encoding** — `dev_gold.fct_action_values` had no `shot / success` rows for 9 competitions (EPL, NWSL, Euro, La Liga, etc.). Conversion Funnel page shows Goals=0 for those teams. Originally diagnosed as an `_flatten_extra()` bug; **actual root cause discovered in session 40 is a guard-metadata staleness bug in `_VaepGuard.check()` plus per-game silent swallows in the scoring UDF.**
2. **Warm-tier hook schema drift** — `CostEstimateHook._merge()` has been silently failing every MERGE since 2026-04-12T02:43Z because an orphaned `task_key` column in the live Delta table was never dropped after PR #115 removed it from the canonical schema. `whenMatchedUpdateAll()` raises `DELTA_MERGE_UNRESOLVED_EXPRESSION` at parse time. 62+ hours of silent failure ended when the D59 cycle wired `dbt_build` into the daily job, and `assert_warm_tier_not_empty` started firing.
3. **Systemic silent-swallow anti-pattern** — both defects above were hidden for weeks by `except Exception: logger.warning(...)` / `except Exception: return empty_df` patterns. Repo-wide audit found 55+ instances in `src/`, 40+ in `scripts/`, and 24 in `hf_taipy_app/`. Without removing the pattern at the source, the next invisible data-integrity bug is a matter of time.

---

## 2. Investigation Evidence (Citable)

### D57 (corrected diagnosis — 2026-04-14 session 40)

All evidence below is reproduced and cited in `memory/project_spadl_vaep_chain.md`.

**Single-match trace, both providers:**

| Layer | StatsBomb match 3754348 | Wyscout match 2500097 |
|---|---|---|
| Raw events — total / shots / goals | 3,499 / 21 / 9 | 1,508 / 30 / 8 |
| `bronze.spadl_actions` — shot-like rows TODAY | type_id=11: 12 fail + 8 success; type_id=12 (shot_penalty): 1 success. **Total 21, successes 9** ✓ | type_id=11: 22 fail + 8 success; type_id=13 (shot_freekick): 1 fail. **Total 31, successes 8** ✓ |
| `bronze.vaep_action_values` — rows for this match | **0** (match entirely absent — pre-ALTER state) | 1,075 rows, 30 shot / 8 success ✓ |
| `dev_gold.fct_action_values` — rows for this match | 21 shot / **0 success**; missing cross, corner_*, freekick_*, goalkick, keeper_*, throw_in, shot_penalty, tackle; pass-success=930, dribble-success=1,647 (historic, no longer matches current bronze) | 30 shot / 8 success; all action subtypes present; all counts match current bronze ✓ |

**What this proves:**
1. **`_flatten_extra()` is NOT the bug** — today's `bronze.spadl_actions` contains correct per-provider output for both matches. StatsBomb has 9 successes (= 8 shot + 1 penalty = raw goal count 9). Wyscout has 8 successes (= raw goal count 8). Stage 1 works.
2. **Wyscout chain works end-to-end TODAY** — all four layers agree for match 2500097.
3. **StatsBomb chain is broken TODAY at Stage 2** — `bronze.vaep_action_values` has zero rows for this match. Gold has stale historic data from an earlier bronze state.
4. **`fct_action_values` pass/dribble counts for StatsBomb 3754348** do NOT match current `bronze.spadl_actions` (pass success 930 vs 591, dribble success 1647 vs 761) — the gold rows came from an earlier era and haven't been refreshed because of the incremental-merge gate (`match_id not in {{ this }}`).

**Guard metadata staleness — the real D57 root cause:**

`src/ingestion/spadl_vaep.py:_VaepGuard.check()` lines 80-121 runs at task start. It computes:
- `sb_new = find_new_ids(bronze.statsbomb_events, bronze.spadl_actions)` — statsbomb match_ids not in spadl
- `ws_new = find_new_ids(bronze.wyscout_events, bronze.spadl_actions)` — wyscout match_ids not in spadl
- `unscored = find_new_ids(bronze.spadl_actions, bronze.vaep_action_values)` — spadl match_ids not in vaep

Then stores `unscored_vaep_match_ids` in `filter_result.metadata`. Stage 2 at `spadl_vaep.py:429` reads this metadata AFTER Stage 1 has run.

**The failure mode at 2026-04-14T16:22:09Z:**
1. User had wiped statsbomb from both `bronze.spadl_actions` (v638) and `bronze.vaep_action_values` (v8) four seconds apart.
2. Daily job ran at 16:22:09Z. Guard computed:
   - `sb_new` = 3,462 (all statsbomb match_ids in events, none in now-empty spadl)
   - `unscored = find_new_ids(spadl, vaep)` = **0 match_ids** (both tables have identical wyscout and zero statsbomb; set difference = empty)
3. Guard metadata frozen as `unscored_vaep_match_ids = []`.
4. Stage 1 ran Stage 1, wrote `spadl_actions` v639 at 16:32:41Z with ~7M statsbomb rows.
5. Stage 2 read `unscored_match_ids = []` (stale), hit the `if not unscored_match_ids: return 0` short-circuit at line 468.
6. Task reported SUCCEEDED with zero new rows in `bronze.vaep_action_values`.

**Fix**: union `new_spadl` and `unscored` in the guard metadata. The two sets are disjoint by construction (a match_id cannot be both `¬in_spadl` and `in_spadl`). The union is lossless and captures match_ids Stage 1 is about to add to spadl that Stage 2 must score.

**Silent-swallow UDF bugs (3 instances, all fixed)**:

- `spadl_vaep.py:_make_scoring_udf` — `except Exception: pass  # noqa: S110` silently dropped failed VAEP-scoring games. Would have masked any post-fix scoring-UDF failures indefinitely.
- `spadl_conversion.py:_make_sb_spadl_udf:141-145` — `except Exception: return _pd.DataFrame(columns=_spadl_cols)` silently dropped failed StatsBomb SPADL conversions per-match. **Probably the real source of the D57 symptom we originally observed** — earlier `_flatten_extra()` bugs (before PR #95 migration to silly_kicks) would have been silently losing per-match data.
- `spadl_conversion.py:_make_ws_spadl_udf:340-344` — same pattern for Wyscout.

All three now raise `RuntimeError(f"... failed for match_id={x}")` with group-key context, so Spark propagates errors to the driver.

### Warm-tier hook schema drift

Investigation evidence is fully cited in `memory/project_warm_tier_blocker.md`. Key facts:

- **The exception**: `pyspark.errors.exceptions.connect.AnalysisException: [DELTA_MERGE_UNRESOLVED_EXPRESSION] Cannot resolve task_key in UPDATE clause given columns s.workflow_id, s.phase, s.run_id, s.runtime, s.hf_job_id, s.state, s.started_at, s.ended_at, s.duration_seconds, s.row_count, s.entity_count, s.guard_duration_seconds, s.rate_usd_per_hour, s.estimated_cost_usd, s.cost_source, s.updated_at.` — thrown by `com.databricks.sql.transaction.tahoe.ResolveDeltaMergeInto` on every hook MERGE.
- **Pulled from a live task log** (job run `938577837538347`, task `ingest_statsbomb` at 2026-04-14T16:20:04Z) via `w.jobs.get_run_output(run_id=...)`.
- **Live table history** (`bronze.observability.workflow_cost_live`):
  - v2735 @ 2026-04-12T02:20:20Z — last successful MERGE (schema had orphaned `task_key`, `job_run_id` still present)
  - v2736 @ 2026-04-12T02:43:19Z — user `karstenskyt@gmail.com` enabled column mapping (`delta.columnMapping.mode=name`)
  - v2737 @ 2026-04-12T02:43:24Z — user dropped `job_run_id` but NOT `task_key`
  - v2738–v2749 — only DELETE operations from dbt post-hooks. **Zero successful writes for 62+ hours.**
- **Canonical schema** (`scripts/create_cost_table.sql`, hook's `_COST_LIVE_COLUMNS`): 16 columns, no `task_key`, no `job_run_id`.
- **Live table pre-ALTER**: 17 columns (16 canonical + orphaned `task_key`).
- **Mechanism**: `whenMatchedUpdateAll()` expands to `SET target.col = source.col FOR EVERY col IN target_schema`. The hook's 16-column source DataFrame doesn't have `task_key`. Delta's resolver fails at parse time on the first `on_start` call, before any data is written.
- **Why it was invisible**: `cost_hook.py` wraps every method in `try: ... except Exception: logger.warning(...)`. Warning-level logs are filtered out of standard error-log queries. `workflows/runner.py:30` also catches hook failures at warning level and continues. Both layers had to be loud to surface the failure.

### Systemic silent-swallow audit

- **`src/` count**: 55 instances of `except Exception:` surveyed, categorized A–H by risk level (memory: `feedback_no_silent_swallows.md`).
- **`scripts/` count**: 40 instances.
- **`hf_taipy_app/` count**: 24 instances.
- **UDF silent swallows (Category C) — highest risk**: 3 in `src/` (all fixed); 0 in scripts/ or hf_taipy_app/.
- **Pipeline-critical fallbacks (Category F) — second highest risk**: 5 in `src/` (all fixed or reclassified); 0 in scripts/ or hf_taipy_app/.
- **Validation silent-skip (Category E)**: 6 in `model_validation.py` (all narrowed to `tolerate_missing_table`).
- **Table-missing bootstrap (Category B)**: ~25 in `src/ingestion/` (all narrowed).

---

## 3. Design Decisions

| Decision | Rationale |
|---|---|
| **D57 fix**: guard metadata union (1 line) + per-game scoring-UDF raise + per-match converter-UDF raise | Root cause is metadata staleness in the guard, not `_flatten_extra()` or `replace_where`. Narrow fix in the guard + loud per-group failures for the UDFs eliminates the class of bug without touching silly_kicks. |
| **Narrow `replace_where` predicate helpers** (retained from earlier plan) | Already in the working tree from session 40 earlier work. Narrow predicates prevent the per-provider clobber that was a LATENT risk (not yet fired). Defensive, cheap. |
| **Warm-tier hook fix**: drop orphan column from live table + extract `_COST_LIVE_COLUMNS` constant + schema-drift guard test | Live table had to change (the ALTER is the only way to resolve the DELTA_MERGE_UNRESOLVED_EXPRESSION). The constant + guard test prevents future recurrence by making code/DDL drift fail in CI instead of in production 62 hours later. |
| **`_build_cost_live_schema()` factory** | Lazy pyspark import so module import doesn't require Spark (pyspark is optional at install time). |
| **Log level bump `warning → error`** in `cost_hook.py` (4 sites) and `workflows/runner.py` (hook dispatcher) | Fire-and-forget pattern is preserved (the hook still doesn't crash pipelines), but failures are now visible in standard error-log queries. |
| **`sync_hf_costs.py` schema alignment** | Second victim of the same schema drift. The module's `map_to_delta_schema` was missing `entity_count` and `guard_duration_seconds` and still had `task_key`. Aligned to canonical 16 cols. |
| **`scripts/sync_hf_costs.py` deletion** | Dead stale copy discovered during investigation. Not referenced anywhere. `test_sync_hf_costs.py` was importing from the dead copy — meaning tests were exercising the pre-PR-#115 schema and NEVER the live module. |
| **Silent-swallow systemic audit — in scope for this cycle** | User directive: "no rush on anything else, fixing this is foundational to any other work planned". Isolated patching of individual sites leaves landmines. |
| **`tolerate_missing_table` context manager** | Single source of truth for "Spark table-missing errors may be suppressed". Suppresses ONLY errors matching 6 specific error-message markers (`TABLE_OR_VIEW_NOT_FOUND`, `Table or view not found`, `Path does not exist`, `DELTA_MISSING_DELTA_TABLE`, `DELTA_TABLE_NOT_FOUND`, `TableNotFoundException`). Everything else propagates with a regression test verifying the exact `DELTA_MERGE_UNRESOLVED_EXPRESSION` from the warm-tier blocker is NOT suppressed. |
| **Enable `BLE001` in ruff** | Forces all future broad catches to either be narrowed or explicitly justified via `# noqa: BLE001 — <reason>` or a per-file-ignores entry. Future drift becomes visible in PRs. |
| **D45 deferred** | User directive. Data integrity baseline must be in place before downstream feature work. |

---

## 4. Scope

### 4.1 Commit 1 — D57 SPADL integrity (shipped as `e0c4360`)

**Files**:
- `src/ingestion/spadl_conversion.py`:
  - New helpers `_make_statsbomb_replace_where(new_game_ids)` + `_make_wyscout_replace_where(new_game_ids)` producing `data_source = '<provider>' AND match_id IN (...)` predicates.
  - Wire helpers into `_convert_statsbomb_from_bronze` and `_convert_wyscout_from_bronze`.
  - **NEW**: `_make_sb_spadl_udf` UDF closure — `except Exception: return empty` replaced with `raise RuntimeError(f"StatsBomb SPADL conversion failed for match_id={match_id}") from exc`.
  - **NEW**: `_make_ws_spadl_udf` UDF closure — same pattern.
- `src/ingestion/spadl_vaep.py`:
  - **The D57 root fix**: `_VaepGuard.check()` line 118 — `unscored_vaep_match_ids = sorted(set(new_spadl) | set(unscored))` (was `sorted(unscored)`).
  - **NEW**: `_make_scoring_udf` per-game loop — `except Exception: pass  # noqa: S110` replaced with `raise RuntimeError(f"VAEP scoring failed for game_id={game_id}") from exc`.
- `src/ingestion/vaep_training.py`:
  - **NEW**: `extract_features_for_games` per-game loop — `except Exception: _log.exception(...)` replaced with `raise RuntimeError(f"VAEP feature extraction failed for game_id={game_id}") from exc`.
- Tests (6 + 12 = 18 new test cases):
  - `src/tests/test_spadl_conversion.py` (NEW file): 6 helper tests + `TestStatsBombConverterErrorPropagation` (3) + `TestWyscoutConverterErrorPropagation` (3).
  - `src/tests/test_spadl_vaep.py`: `TestVaepGuardMetadata` (4) + `TestScoringUdfErrorPropagation` (2) + `TestVaepTrainingFeatureExtractionErrorPropagation` (2).

**Out of scope**: re-upload of the SPADL wheel (no artifact change); dbt contract expansion.

### 4.2 Commit 2 — Warm-tier hook schema drift fix (shipped as `52a5cf8`)

**Files**:
- `src/ingestion/cost_hook.py`:
  - **New module-level constant**: `_COST_LIVE_COLUMNS: list[tuple[name, type, nullable]]` — 16 tuples matching `scripts/create_cost_table.sql` exactly. Single source of truth.
  - **New factory**: `_build_cost_live_schema() -> StructType` — lazy pyspark import, converts `_COST_LIVE_COLUMNS` to `StructType` at call time.
  - `_merge` now uses `_build_cost_live_schema()` instead of inline StructType literal.
  - 4× `logger.warning` → `logger.error` on hook method failure log lines.
- `src/workflows/runner.py:30`:
  - Hook dispatcher `logger.warning` → `logger.error`. Docstring updated to explain why.
- `src/ingestion/sync_hf_costs.py:152-171`:
  - `map_to_delta_schema` aligned to 16-col canonical schema: remove `task_key`, add `entity_count: None` + `guard_duration_seconds: None`. Docstring explains why `task_key` parameter is retained (call-site compatibility) but unused.
- `scripts/sync_hf_costs.py` — **deleted** (dead stale copy).
- Tests:
  - `src/tests/test_cost_hook.py`: `TestColumnCompleteness.REQUIRED_COLUMNS` now derived from `_COST_LIVE_COLUMNS` (single source of truth). New `TestCostHookSchemaDriftGuard` class (4 tests) parses `create_cost_table.sql` and asserts column-list equality by order AND set, plus regression asserts that `task_key` and `job_run_id` stay out.
  - `src/tests/test_cost_hook_integration.py` — **NEW file**. Real-Spark MERGE round-trip. Auto-skips if local Spark unavailable. Separate file so the autouse mock fixture in `test_cost_hook.py` doesn't contaminate it.
  - `src/tests/test_sync_hf_costs.py`: imports updated from `scripts.sync_hf_costs` → `ingestion.sync_hf_costs`. `test_maps_completed_record` now asserts exact `_COST_LIVE_COLUMNS` equality + absence of orphan cols.

**Destructive ops (Commit 2 post-ship, phase 1)**:
- `ALTER TABLE soccer_analytics.observability.workflow_cost_live DROP COLUMN task_key` — metadata-only (column mapping already enabled).
- SQL MERGE seed row with `WHEN MATCHED UPDATE SET * / WHEN NOT MATCHED INSERT *` (same semantic as hook) to empirically verify the schema drift is resolved.
- Full daily job trigger (`soccer-analytics-ingestion-dev`, 28 tasks) — end-to-end verification that production hook code path works post-ALTER.

**Verification artifacts** (all citable):
- `assert_warm_tier_not_empty` fire condition: `warm_not_running=1, recent_null_billing=26, test_status='TEST WILL PASS'` (post-seed query).
- Daily job `641288498990290`: **28/28 tasks SUCCESS, 0 failures**. Production hooks writing cleanly to post-ALTER table.

### 4.3 Commit 3 — Systemic `src/` silent-swallow remediation (shipped as `9ad7e4f`)

**Files** (34 modified + 1 new = 35 files):
- `src/ingestion/utils.py`:
  - **New helper**: `tolerate_missing_table(logger, msg)` context manager + `_TABLE_NOT_FOUND_MARKERS` tuple of 6 specific error-message markers.
  - The helper catches `Exception` (broad, because the concrete class varies between classic PySpark, Spark Connect, Delta Lake, and Unity Catalog) and suppresses ONLY matches to the marker list. Non-matching exceptions re-raise.
- **~25 Category B (bootstrap) rewrites** across `ingestion/{statsbomb, statsbomb_backfill_360, statsbomb_backfill_extra, spadl_conversion, idsse, idsse_events, metrica, metrica_tracking, metrica_events, skillcorner, wyscout, spadl_vaep, defcon_lite_common, xg_model, xg_model_v2, export_embeddings_training_data, prepare_360_training_data, export_scoutgpt_training_data, player_embeddings_v1, utils}.py`.
- **6 Category E rewrites** in `model_validation.py` — validation-skip paths narrowed to `tolerate_missing_table` + local `_to_float` helper for pyright-clean pandas cell handling.
- **5 Category F fixes**:
  - `hf_sync.py:126` — sub-workflow failure log bumped to ERROR level.
  - `player_embeddings_v2.py:143,283` — fallback metadata flags + ERROR level logs explaining what was lost.
  - `xg_model_v2.py:332` — missing weights now **raises `RuntimeError`** (was silently returning 0).
  - `analytics/defcon_lite.py:161` — narrowed to `(TypeError, ValueError)`.
  - `pausa.py:166` — missing upstream now **raises `WorkflowSkippedError`** (was silently returning 0).
- **5 MLflow @Champion fallback sites** kept broad with line-level `# noqa: BLE001 — MLflow registry raises many unrelated exception types on missing Champion`. Typed `None` return is the documented contract.
- **5 `utils.py` defensive paths** (HF token fallbacks, MLflow artifact hash, volume sidecar, pyspark optional import) kept broad with line-level `# noqa: BLE001 — <reason>`.
- **3 cleanup sites** (`xg_model.py`, `xg_model_v2.py`, `workflows/loader.py`) kept broad with line-level noqa + explanation.
- `pyproject.toml`: enabled `BLE001` in ruff select. 9 per-file-ignores documented with one-line architectural reasons.
- Tests: 4 updated mock error messages to realistic Spark errors, 2 new regression tests for narrowing (`test_propagates_non_missing_table_errors`), 1 new `test_utils.py` (11 tests covering every suppress/propagate path).

### 4.4 Commit 4 — `scripts/` + `hf_taipy_app/` narrowing (shipped as `3b02e1c`)

**Files** (10 modified):
- **2 real fixes**: `scripts/compute_obso_hf.py` + `compute_space_creation_hf.py` column-projection fallback — narrow from `except Exception` to `except ValueError` (actual pyarrow error) + critical-columns assertion (`match_id`, `frame_id`, `player_id`, `x`, `y`) that raises `RuntimeError` if critical cols are missing.
- **10 Taipy query narrowings**: `queries/{defensive, players, shots, tactical_positions, team_shape, workflows}.py` now catch `RuntimeError` — what `execute_query` raises after wrapping `psycopg2.Error`.
- **2 percentile sentinel clarifications**: `fetch_defcon_percentiles` + `fetch_player_percentiles_batch` return `dict | None` where None = "feature unavailable" vs `{}` = "no matching rows". Callers unchanged (truthy checks work for both). Failure sites bumped from `debug` to `warning` level with explicit "feature unavailable" message.
- **1 bonus pyright fix**: `players.py:124` `.head(1)` → `.iloc[[0]]` (eliminates pre-existing `reportArgumentType` error).
- **1 test fix**: `test_taipy_workflows_perf.py` now mocks `fetch_latest_run_metrics` + `_fetch_hf_cost_history`. Exposed a latent test defect — the old broad catch was masking a missing `LAKEBASE_HOST` env var in the benchmark fixture (pydantic ValidationError was being silently suppressed). This is exactly the class of hidden bug the systemic cleanup is supposed to expose.
- `pyproject.toml`: 18 file-level `BLE001` ignores for Taipy UI state/daemons + operational CLI/HF Jobs scripts where broad catches are architecturally correct.

### 4.5 Commit 5 — Docs + memory entry + tmp cleanup (this commit)

**Files**:
- `memory/feedback_no_silent_swallows.md` — **NEW** durable rule: default exception handling is raise-or-observable, never silent-warn. With three worked examples from session 40 and enforcement mechanisms.
- `memory/MEMORY.md` — index entry added.
- `docs/superpowers/specs/2026-04-14-gold-data-repair-design.md` — this file (rewritten from old spec).
- `docs/superpowers/plans/2026-04-14-gold-data-repair.md` — plan doc written during the cycle (references each commit).
- `TODO.md` — remove D57 (shipped), mark D45 as BLOCKED on this cycle's completion (preserved from original entry with a "BLOCKED on" note added), add D65 (warm-tier post-hook watermark follow-up). The mad-scientist-skills update is NOT tracked in this repo's TODO — it's sibling-repo work and belongs in that repo's own tracking.
- Delete all session 40 `tmp_*.py` + `tmp_*.txt` investigation scratch files.

---

## 5. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| **Hard-fail-first SPADL UDFs**: a re-run of `compute_spadl_vaep` after Commit 1 may RAISE on a previously-hidden per-match silly_kicks failure. | User decision (plan Task 41 step 3): STOP, investigate the specific match, fix or filter out, retry. Daily job run `641288498990290` completed 28/28 SUCCESS with the new code — no hidden per-match failures found in production during the cycle. |
| **Live table ALTER** on `workflow_cost_live` | Metadata-only (column mapping already enabled). Reversible via `ALTER TABLE ADD COLUMN task_key STRING`. |
| **`tolerate_missing_table` over-narrowing**: a real Spark error with a message that happens to contain "TABLE_OR_VIEW_NOT_FOUND" would be incorrectly suppressed. | The 6 markers are specific enough (strict substring match) that false positives are implausible. Test coverage includes the exact `DELTA_MERGE_UNRESOLVED_EXPRESSION` from the warm-tier blocker as a regression guard — it propagates, not suppresses. |
| **Schema-drift guard test** (`TestCostHookSchemaDriftGuard`) is local-only; if the live table drifts again, CI won't catch it until next mart build. | Not in scope for this cycle to fix. A future enhancement would be a CI step that runs `DESCRIBE TABLE workflow_cost_live` and asserts against `_COST_LIVE_COLUMNS`. Added to follow-up TODO. |
| **Test coverage gap for integration test**: `test_cost_hook_integration.py` skips when local Spark isn't available. | Runs in Databricks CI where Spark is installed. Local devs see it skip cleanly with an informative reason. |

---

## 6. Execution Order (Single PR, Five Commits)

1. **Commit 1** — D57 SPADL integrity fix (shipped as `e0c4360`).
2. **Commit 2** — Warm-tier hook schema drift + observability (shipped as `52a5cf8`).
3. **Destructive ops phase 1** — `ALTER TABLE DROP COLUMN task_key` + SQL MERGE seed row + daily job trigger for end-to-end verification. Verified 28/28 SUCCESS.
4. **Commit 3** — Systemic `src/` silent-swallow remediation + BLE001 enabled (shipped as `9ad7e4f`).
5. **Commit 4** — `scripts/` + `hf_taipy_app/` narrowing + percentile sentinel clarification (shipped as `3b02e1c`).
6. **Commit 5** — Docs + memory + TODO + tmp cleanup (this commit).
7. **Destructive ops phase 2** — `compute_spadl_vaep` re-run + `dbt build --full-refresh --select fct_action_values+` + Puppeteer verification of EPL Shot Map goal counts. **Requires explicit user approval before running.**
8. **PR creation**.

---

## 7. Success Criteria

**Data integrity** (all verified in destructive ops phase 2):
- `bronze.vaep_action_values` has ~9M statsbomb rows after Stage 2 re-run.
- `dev_gold.fct_action_values` has `shot / success` rows for EPL and the 8 other previously-broken competitions.
- Puppeteer Shot Map page shows goals for Man United in EPL.
- `assert_warm_tier_not_empty` dbt test passes (already verified post-seed).

**Silent-swallow elimination** (verified post-Commit 4):
- `ruff check` with BLE001 enabled passes on `src/`, `scripts/`, and `hf_taipy_app/`.
- Every remaining broad catch has either a line-level `# noqa: BLE001 — <reason>` or a per-file-ignores entry with an architectural justification.
- `test_utils.py::TestTolerateMissingTable` passes, including the regression guard for `DELTA_MERGE_UNRESOLVED_EXPRESSION` propagation.
- `TestCostHookSchemaDriftGuard` passes.

**Production signal**:
- Daily job `641288498990290` (triggered during destructive ops phase 1) shipped 28/28 tasks SUCCESS with the new hook code. Zero failures.

---

## 8. Follow-up / deferred items

1. **D45 Football2vec v2 StatsBomb coverage + v1 deletion** — deferred to a follow-up cycle. Was the original scope of this spec alongside D57; removed per user directive.
2. **Warm-tier post-hook watermark logic** — the `MAX(usage_date) WHERE attributed_cost_usd IS NOT NULL` watermark in `fct_workflow_costs.sql` post-hook 1 is subtly wrong (prunes rows whose billing hasn't caught up). Not currently firing because the warm tier is being freshly populated post-Commit 2, but the logic bug should be fixed. Added to TODO as D65.
3. **mad-scientist-skills audit anti-pattern updates** — anti-pattern additions to `D:/Development/karstenskyt__mad-scientist-skills/plugins/mad-scientist-skills/skills/{architecture-audit, observability-audit}/SKILL.md`. **Not tracked anywhere, not committed.** Intended changes are staged as uncommitted working-tree modifications in the sibling repo for future session review and manual commit outside of this cycle.
4. **Second-round `scripts/` audit** — the audit agent found fewer dangerous patterns than expected; a future cycle could widen the remit to CI scripts, dbt Python hooks, and Terraform-invoked scripts.
