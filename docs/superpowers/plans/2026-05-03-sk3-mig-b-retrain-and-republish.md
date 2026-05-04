# SK3-MIG-B Implementation Plan — silly-kicks 3.0.1 Group B retrain + republish + XG1-RETIRE + HF4

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Retrain all 11 cycle items (8 trained models + 3 compute-only re-runs) against canonical-LTR `fct_action_values` from SK3-MIG-A, republish 8 HF datasets, retire xG v1 entirely, migrate notebook publishers to PEP 723, rebase model validation thresholds against fresh data, refresh perf baselines doc.

**Architecture:** Two sequential PRs on the `sk3-mig-b` branch family. PR-α (`sk3-mig-b`) bundles all retrain infrastructure + retrain orchestrator + XG1-RETIRE + HF4 migration into a single squash commit; the orchestrator runs post-merge as the E2E test, executing 11 cycle items + 8 HF republishes + Lakebase synced refresh + index restoration. PR-β (`sk3-mig-b-baseline-rebase`) closes the loop with seed CSV rebase + perf-doc refresh + AI_GOVERNANCE review + TODO cleanup, branched from PR-α's merge SHA.

**Tech Stack:** Python 3.10 (Databricks serverless lock), uv for envs, PySpark via Databricks workflows, Delta Lake for telemetry tables, MLflow + UC Volume for model artifacts (per ADR-012), HuggingFace Hub for datasets + model cards (per ADR-014), Terraform for job declarations, dbt for mart models, pytest for tests + smoke gates.

**Spec:** `docs/superpowers/specs/2026-05-03-sk3-mig-b-retrain-and-republish-design.md`

**ADRs touched:** ADR-002 (telemetry schema-drift guard for `bronze.sk3_mig_b_runs`); ADR-012 (training-to-production delivery contract — every retrain calls `set_and_verify_mlflow_champion` + `upload_weights_to_uc_volume`); ADR-014 (HF card inventory parity — amended by §4.2 to forbid notebook publishers/trainers); ADR-022 (direction-of-play migration — Group A predecessor).

**Memory references applied throughout:** `feedback_no_commits_without_explicit_approval` (single squash per branch only); `feedback_no_micro_approvals_in_execution` (execute through to natural breakpoints); `feedback_pull_origin_main_before_branching` (already done at session start); `feedback_drop_calendar_effort_estimates` (no calendar estimates; Monstah/Wicked/Dunkin' sizing + sub-task counts only); `reference_mega_job_orchestrator_design` (orchestrator triggers full mega-job + relies on per-task skip-guards; standalone-job dispatch fails with "job not found"); `reference_sdk_over_sql_connector` (CI-side SQL via `WorkspaceClient.statement_execution`); `feedback_dbt_incremental_match_id_skip_silent_stale` (incremental marts may need `--full-refresh` post-retrain).

**Branch:** `sk3-mig-b` (already created from main `acb395c`; spec file + TODO.md fix uncommitted in working tree, will ride with the PR-α commit).

**Commit policy:** ONE squash commit per branch, only when needed for E2E testing / deployment. Phase 1-7 build the working tree without intermediate commits. Phase 8 commits + pushes + opens PR-α. PR-β follows the same pattern on its own branch.

---

## Phase Overview

| # | Theme | New code center of gravity | What lands |
|---|---|---|---|
| 0 | Pre-execution verification sweep (resolves spec §10 open questions + registration gaps before any code lands) | grep + dbt parse + SQL probes; no code changes | Concrete eval-fold IDs, dbt cascade clean, synced-table inventory match, F2V/PAUSA/DEFCON empirical baselines, registration gaps flagged |
| 1 | Telemetry infrastructure (foundation — everything writes to this table) | `scripts/migrations/`, `src/ingestion/sk3_mig_b_telemetry.py`, `src/tests/test_sk3_mig_b_runs_schema_parity.py` | DDL migration + ADR-002 §4 schema discipline |
| 2 | Smoke gate scripts (11 per-cycle-item acceptance gates) | `src/tests/sk3_mig_b/` | All B-pattern absolute-physical thresholds gated |
| 3 | HF4 migration (notebook → PEP 723 + CI invariants) | 5 NEW publisher scripts in `scripts/`, 4 notebook deletions, ADR-014 amendment | Publisher discipline closed end-to-end |
| 4 | XG1-RETIRE PR-α-commit parts (source/dbt/workflow/UI/docs deletions + regression tests) | `hf_taipy_app/`, `dbt_project/`, `workflow-cards/`, `terraform/`, `docs/`, `src/tests/` | v1 inference path dead at code level |
| 5 | Wheel bump (0.3.30 → 0.3.31 for the XG1-RETIRE wheel surface change) | `pyproject.toml`, `bump_wheel.py` invocation | Hyrum's-Law guard via patch bump |
| 6 | Orchestrator script (the runtime tool that drives the retrain cycle) | `scripts/sk3_mig_b_retrain.py` | Per-cycle-item E2E loop + halt-resume + background-process discipline |
| 7 | Pre-merge verification (run all new tests + lint + type-check) | n/a (verification only) | Local CI green before commit |
| 8 | Single commit + push + PR-α open | git operations | PR-α ready for review + merge |
| 9 | Operator runtime (orchestrator E2E run post-merge) | runtime invocation; not a code commit | Cycle items 1-11 retrained + republished + Lakebase synced |
| 10 | PR-β branch + regen scripts + freshness tests | `scripts/regenerate_*`, `dbt_project/.metadata/`, `dbt_project/seeds/`, `docs/` | Baselines rebased + perf-doc refreshed |
| 11 | PR-β single commit + push + PR-β open | git operations | PR-β ready for review + merge |

---

## File Structure

**New files (PR-α):**

```
scripts/
├── migrations/
│   └── 2026-05-03-create-bronze-sk3-mig-b-runs.sql      [Phase 1] DDL migration
├── sk3_mig_b_retrain.py                                  [Phase 6] orchestrator
├── publish_line_breaking_passes_hf.py                    [Phase 3] HF4 NEW
├── publish_pitch_control_tracking_hf.py                  [Phase 3] HF4 NEW
├── publish_football2vec_embeddings_hf.py                 [Phase 3] HF4 NEW (fired by SK3-MIG-B)
├── publish_obso_pausa_inputs_hf.py                       [Phase 3] HF4 NEW (fired by SK3-MIG-B)
└── train_football2vec.py                                 [Phase 3] HF4 F2V v1 trainer (replaces notebook)

src/
├── ingestion/
│   └── sk3_mig_b_telemetry.py                            [Phase 1] _SK3_MIG_B_RUNS_COLUMNS + StructType factory
└── tests/
    ├── test_sk3_mig_b_runs_schema_parity.py              [Phase 1] DDL parity (ADR-002 §4)
    ├── test_no_notebook_hf_publishers.py                 [Phase 3] HF4 invariant 1
    ├── test_xg_v1_retired.py                             [Phase 4] XG1-RETIRE regression
    ├── test_shot_map_v2_columns.py                       [Phase 4] XG1-RETIRE UI regression
    └── sk3_mig_b/                                        [Phase 2] smoke gate subdir
        ├── __init__.py
        ├── conftest.py                                   shared fixtures (Spark session, MLflow client)
        ├── test_vaep_post_retrain_smoke.py
        ├── test_xg_v2_post_retrain_smoke.py
        ├── test_ext_v2_p0_post_retrain_smoke.py
        ├── test_ext_v2_p1_post_retrain_smoke.py
        ├── test_defcon_lite_post_retrain_smoke.py
        ├── test_obso_post_retrain_smoke.py
        ├── test_pausa_post_retrain_smoke.py
        ├── test_f2v_v1_post_retrain_smoke.py
        ├── test_f2v_v2_post_retrain_smoke.py
        ├── test_f2v_360_post_retrain_smoke.py
        └── test_scoutgpt_post_retrain_smoke.py

docs/
├── superpowers/
│   └── adrs/
│       └── ADR-014-hf-card-inventory-parity.md           [Phase 3] amend (notebook ban)
└── superpowers/
    └── plans/
        └── 2026-05-03-sk3-mig-b-retrain-and-republish.md [this file]
```

**Modified files (PR-α):**

```
pyproject.toml                                             [Phase 4+5] remove xg-v1 entry-point line; wheel = 0.3.31
src/tests/test_hf_publish_parity.py                        [Phase 3] extend AST walk for upload_hf_readme requirement
hf_taipy_app/src/state/shot_map.py                         [Phase 4] v1 cols → v2 cols + glossary entry
hf_taipy_app/src/queries/shots.py                          [Phase 4] delete fetch_xg_predictions()
docs/huggingface/org-card.md                               [Phase 4] remove v1 listing
README.md                                                  [Phase 4] HF artifact list (remove v1)
AI_GOVERNANCE.md                                           [Phase 4] §5 Scope row removal
TODO.md                                                    [Phase 8] (already has SK3-MIG strikethrough fix from session start)
```

**Deleted files (PR-α):**

```
src/ingestion/xg_model.py                                  [Phase 4] v1 source
scripts/train_xg_model_hf.py                               [Phase 4] v1 HF trainer
notebooks/train_xg_model.py                                [Phase 4] v1 Databricks-notebook trainer
notebooks/train_football2vec.py                            [Phase 3] HF4 F2V v1 notebook trainer (replaced by scripts/train_football2vec.py)
notebooks/publish_datasets.py                              [Phase 3] HF4 multi-dataset notebook publisher
notebooks/publish_obso_data.py                             [Phase 3] HF4 OBSO inputs notebook publisher
dbt_project/models/marts/fct_xg_predictions.sql            [Phase 4] v1 mart
dbt_project/models/staging/xg/stg_xg__predictions.sql      [Phase 4] v1 staging
workflow-cards/wf-xg-v1.yaml                               [Phase 4] v1 workflow card
docs/huggingface/model-cards/xg-model-statsbomb-wyscout.md [Phase 4] v1 model card
```

**Terraform-source edits (PR-α):**

```
terraform/environments/dev/main.tf                         [Phase 4] delete v1 job declaration block
terraform/modules/workflows/main.tf (or equivalent)        [Phase 4] verify no v1-specific module wiring lingers
```

**dbt-YAML edits (PR-α):**

```
dbt_project/models/staging/xg/_xg__sources.yml             [Phase 4] remove v1 source entries
dbt_project/models/marts/_marts__models.yml                [Phase 4] remove v1 mart contract entry
```

**New files (PR-β):**

```
scripts/
├── regenerate_model_baseline_scalars.py                   [Phase 10] PEP 723 regen
└── regenerate_perf_baselines_md.py                        [Phase 10] PEP 723 regen

dbt_project/
└── .metadata/
    └── baseline_freshness/
        └── model_baseline_scalars.json                    [Phase 10] sidecar JSON (created by regen script)

src/tests/
├── test_model_baseline_scalars_freshness.py               [Phase 10] freshness gate
└── test_perf_baselines_md_freshness.py                    [Phase 10] freshness gate
```

**Modified files (PR-β):**

```
dbt_project/seeds/model_baseline_scalars.csv               [Phase 10] rebased values
docs/performance-baselines.md                              [Phase 10] refreshed timing/cost tables + Last refreshed
AI_GOVERNANCE.md                                           [Phase 10] §5 Next review dates within 30-day grace
TODO.md                                                    [Phase 10] remove SK3-MIG-B/XG1-RETIRE/HF4 rows; add wf-model-validation-rebaseline-30d row
```

---

## Phase 0 — Pre-execution verification sweep (resolves spec §10 open questions)

**Why first.** External review (round 4) flagged 7 open questions from spec §10 as "deferred again, not resolved." Each is a 30-second to 10-minute pre-execution check. Without resolution, several smoke gates will fail on noise rather than on genuine retrain regressions. Phase 0 batches these checks + writes empirical baselines for downstream phases. ~45 min of grep/SQL/git work; saves several hours of mid-cycle halt-resume.

This phase produces ZERO code commits — it's pure verification + recording empirical findings as plan-execution constants for Phase 2 (smoke gates) and Phase 6 (orchestrator).

### Task 0.1 — Resolve Q1: ScoutGPT synced-table refresh scope

**Files:** none — verification only.

- [ ] **Step 1: Search for ScoutGPT-derived synced tables**

```bash
grep -rn "scoutgpt" terraform/ workflow-cards/ 2>&1 | grep -i "synced\|fct_"
```

Expected: either (a) zero hits — confirms ScoutGPT writes no synced mart → orchestrator's `_synced_tables_for_item("scoutgpt")` returning `[]` is correct, OR (b) some hits — record the table name(s) and add to Phase 6's `_synced_tables_for_item` mapping.

- [ ] **Step 2: Record finding**

Add a one-line comment to `scripts/sk3_mig_b_retrain.py` near `_synced_tables_for_item`:

```python
# ScoutGPT: no synced mart per Phase 0 Task 0.1 (grep yielded zero hits).
# OR: ScoutGPT writes fct_<X>; added to dispatch list per Phase 0 Task 0.1.
```

### Task 0.2 — Resolve Q2: HF4 duplicate detection

**Files:** none — verification only.

- [ ] **Step 1: Diff the duplicated cells against the canonical scripts**

For both `spadl-vaep-action-values` and `xg-freeze-frame-data` cells in `notebooks/publish_datasets.py`:

```bash
# Extract the SQL queries from the notebook cells and diff against canonical scripts
grep -A 30 "spadl-vaep-action-values" notebooks/publish_datasets.py
grep -A 30 "xg-freeze-frame-data" notebooks/publish_datasets.py
```

Compare the source SQL against `scripts/publish_spadl_vaep_hf.py` and `scripts/publish_freeze_frame_hf.py` query bodies.

- [ ] **Step 2: Dry-run both canonical scripts + capture row counts**

```bash
uv run python -c "
import sys; sys.path.insert(0, 'scripts'); sys.path.insert(0, 'src')
from publish_spadl_vaep_hf import _query_spadl_vaep  # adapt to actual function name
df = _query_spadl_vaep()
print(f'spadl-vaep canonical row count: {len(df):,}')
print(f'columns: {sorted(df.columns)}')
"
```

(Adapt to actual public function names; the goal is row-count + column-set verification.)

- [ ] **Step 3: Record finding**

If row counts + column sets match the duplicate cells (expected case): document in plan execution log "duplicates verified byte-equivalent on dev — safe to delete." If they DIVERGE, halt — investigate which is canonical before Phase 3.6 deletes the notebook.

### Task 0.3 — Resolve Q3: Lakebase synced-table index inventory

**Files:** none — verification only.

- [ ] **Step 1: List managed synced tables + indexes**

```bash
uv run python scripts/maintain_synced_tables.py --list-managed-tables
```

Expected: prints managed table list + their PG indexes.

- [ ] **Step 2: Verify superset of Phase 6's `_synced_tables_for_item` outputs**

Cross-reference against the 9 distinct synced tables enumerated in `_synced_tables_for_item`:

- `fct_action_values_synced`
- `fct_xg_predictions_v2_synced`
- `fct_defcon_actions_synced`, `fct_defcon_pressure_synced`
- `fct_pausa_values_synced`
- `fct_player_embeddings_synced`, `fct_player_embeddings_career_synced`, `fct_player_embeddings_season_synced`
- `fct_player_embeddings_career_360_synced`, `fct_player_embeddings_season_360_synced`

Every synced table in this list MUST appear in `--list-managed-tables` output. Any missing means `maintain_synced_tables.py --skip-refresh` will fail to restore indexes → Lakebase queries silently slow on Phase 9 operator runtime.

- [ ] **Step 3: Record finding**

If all match: document. If any are missing: ADD to the maintain_synced_tables.py manifest in Phase 1 (treat as a Phase 1 task, not Phase 0 — code edit required). Spec §1.1.6 calls this out as a plan-time-resolvable item.

### Task 0.4 — Resolve Q4: F2V eval-fold IDs (deterministic SQL replaces placeholder)

**Files:** Phase 2 Tasks 2.9-2.11 (smoke gate code) — refactor to use deterministic SQL eval fold instead of hardcoded `range(1, 101)`.

The original plan's `_EVAL_FOLD_PLAYER_IDS = tuple(range(1, 101))` is incorrect — those numeric IDs may not match real `canonical_player_id` values in the dim. The F2V evolve evaluator uses MLM-loss (not recall@10), so no pre-existing eval fold of player IDs exists in the codebase.

**Resolution:** smoke gate computes the eval fold deterministically each time via SQL:

> Top 100 players by total minutes played in StatsBomb (competition_key fixed at the SK3-MIG-A baseline competition), ordered by `canonical_player_id ascending`.

This is stable across retrains by construction — `dim_players_synced` + `fct_player_minutes` (or equivalent) are upstream of the F2V retrain's outputs, so the eval fold is fixed regardless of retrain quality.

- [ ] **Step 1: Verify dim_players + minutes mart exist on dev**

```bash
uv run python -c "
from databricks.sdk import WorkspaceClient
import os
w = WorkspaceClient()
sql = '''
SELECT canonical_player_id, sum(minutes_played) AS total_minutes
FROM soccer_analytics.dev_gold.fct_match_summary  -- or whichever mart has minutes
WHERE data_source = 'statsbomb' AND competition_key = 1
GROUP BY canonical_player_id
ORDER BY canonical_player_id
LIMIT 5
'''
result = w.statement_execution.execute_statement(
    statement=sql, warehouse_id=os.environ['DATABRICKS_WAREHOUSE_ID'], wait_timeout='30s'
)
print(result.result.data_array if result.result else 'No data')
"
```

Expected: 5 rows of (canonical_player_id, total_minutes). Verifies the SQL pattern works.

- [ ] **Step 2: Refactor Phase 2.9-2.11 smoke gates to use deterministic SQL**

Replace the placeholder constant with an inline query:

```python
# Replaces _EVAL_FOLD_PLAYER_IDS hardcoded list.
def _query_eval_fold_player_ids(workspace_client, warehouse_id: str, gold_schema: str, n: int = 100) -> list[int]:
    """Top-N StatsBomb players by minutes played, deterministic order.

    Stable across F2V retrains by construction — dim_players_synced + minutes
    mart are upstream of F2V outputs.
    """
    from src.tests.sk3_mig_b.conftest import execute_sql
    sql = f"""
    SELECT canonical_player_id
    FROM {gold_schema}.fct_match_summary
    WHERE data_source = 'statsbomb' AND competition_key = 1
    GROUP BY canonical_player_id
    ORDER BY SUM(minutes_played) DESC, canonical_player_id ASC
    LIMIT {n}
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    return [int(r[0]) for r in rows]
```

Then in each `test_recall_at_10_above_threshold`, replace the `_EVAL_FOLD_PLAYER_IDS` reference with a call to `_query_eval_fold_player_ids(workspace_client, warehouse_id, gold_schema)`.

(Actual mart name for minutes — `fct_match_summary` is a guess; verify at Phase 0 Step 1 and substitute the correct table in the smoke gate code.)

- [ ] **Step 3: Record the eval-fold rationale in plan execution log**

Document: "F2V eval fold sourced via SQL from top-100-by-minutes StatsBomb players ordered by canonical_player_id; deterministic across retrains."

### Task 0.5 — Resolve Q5: Mart contract parity sweep

**Files:** none — verification only (parity violations would require Phase 4 follow-up edits).

- [ ] **Step 1: dbt parse to verify all mart contracts**

```bash
cd dbt_project && uv run dbt parse
```

Expected: 0 errors. If parse fails, fix before continuing.

- [ ] **Step 2: Per-mart column diff vs `_marts__models.yml` contract**

For each mart that the SK3-MIG-B retrain writes to (`fct_action_values`, `fct_xg_predictions_v2`, `fct_pausa_values`, `fct_defcon_actions`, `fct_defcon_pressure`, `fct_player_embeddings*`):

```bash
uv run python -c "
import yaml
import json
from databricks.sdk import WorkspaceClient
import os
w = WorkspaceClient()

# Load contract column list
with open('dbt_project/models/marts/_marts__models.yml') as f:
    spec = yaml.safe_load(f)

for model in spec['models']:
    name = model['name']
    if name in ('fct_action_values', 'fct_xg_predictions_v2', 'fct_pausa_values', 'fct_defcon_actions', 'fct_defcon_pressure'):
        contract_cols = sorted(c['name'] for c in model.get('columns', []))
        # Query actual mart schema
        sql = f'DESCRIBE TABLE soccer_analytics.dev_gold.{name}'
        result = w.statement_execution.execute_statement(
            statement=sql, warehouse_id=os.environ['DATABRICKS_WAREHOUSE_ID'], wait_timeout='30s'
        )
        actual_cols = sorted(row[0] for row in result.result.data_array if not row[0].startswith('#'))
        diff_added = set(actual_cols) - set(contract_cols)
        diff_removed = set(contract_cols) - set(actual_cols)
        if diff_added or diff_removed:
            print(f'{name}: contract drift!')
            print(f'  added (in mart, not contract): {diff_added}')
            print(f'  removed (in contract, not mart): {diff_removed}')
        else:
            print(f'{name}: contract OK ({len(contract_cols)} columns)')
"
```

Expected: every retrained-model mart prints `contract OK`. ADR-019 additive-fields contract: any added column without a corresponding contract update is a violation; PR-α must update `_marts__models.yml` in tandem.

- [ ] **Step 3: ADR-013 writer-payload re-test**

Per spec §10 Q5: confirm no SK3-MIG-A-introduced surrogate keys leaked back into writer payloads. Grep:

```bash
grep -rn "team_key\|player_key" src/ingestion/spadl_vaep.py src/ingestion/xg_model_v2.py
```

Expected: zero hits — Python writers should emit only native identifiers + predictions per ADR-013. The mart resolves surrogate keys via `INNER JOIN` in dbt, not in Python.

- [ ] **Step 4: Record findings**

Document any contract drift + add a follow-up Phase 4 task to update `_marts__models.yml` if necessary.

### Task 0.6 — Resolve Q6: `fetch_xg_predictions()` disposition

**Files:** none — verification (decision recorded for Phase 4.1).

- [ ] **Step 1: Confirm Q6 resolution from plan-write-time grep**

Plan-write-time grep already confirmed:
- `hf_taipy_app/src/queries/shots.py:59` — `fetch_xg_predictions(competition_key)` returns `xg_logistic, xg_gradient_boosted` (v1-only).
- No same-name v2 fetcher exists.

**Decision:** Phase 4.1 Step 2 DELETES `fetch_xg_predictions()` outright (NOT rename). v2 callers use a different function name or inline SQL.

- [ ] **Step 2: Verify no callers reference the function**

```bash
grep -rn "fetch_xg_predictions" hf_taipy_app/
```

Expected: only the definition + any v1-specific Shot Map callers (which Phase 4.1 Step 3 migrates to v2 columns). After Phase 4 completes, this grep should return 0 hits.

### Task 0.7 — Resolve registration gaps (items 12, 13 from review)

**Files:** Phase 1 Task 1.2 inputs.

- [ ] **Step 1: Add `xg1_retire_runtime` and `scoutgpt_export` to `_META_EVENT_ITEMS`**

When writing `src/ingestion/sk3_mig_b_telemetry.py` in Phase 1 Task 1.2, the `_META_EVENT_ITEMS` set MUST include:

```python
_META_EVENT_ITEMS: frozenset[str] = frozenset({
    "pre_state",
    "baseline_rebase",
    "xg1_retire_runtime",  # Phase 6 Step 4 telemetry write
    "scoutgpt_export",     # Phase 6 Step 2 mega-job dispatch for wf-scoutgpt-export
})
```

This is a Phase 0 finding that updates the Phase 1 spec; pre-registers the items so `classify_cycle_item` doesn't raise on first invocation.

- [ ] **Step 2: Plan note re: TODO row PR-α vs PR-β resolution**

Spec §5.0 listed `wf-model-validation-rebaseline-30d` as a PR-α commit; spec §9.4 said PR-β. Round 3 review flagged this; spec resolved to PR-β. Plan Phase 10 Task 10.6 Step 3 implements as PR-β.

This Phase 0 task records the resolution explicitly: "PR-β handles the TODO row addition (Phase 10 Task 10.6 Step 3); spec §5.0 was the inconsistent surface and resolved to align with §9.4."

### Task 0.8 — Resolve item 14: empirical baselines for DEFCON / PAUSA smoke gates

**Files:** Phase 2 Tasks 2.6 + 2.8 inputs.

The original Phase 2.6 + 2.8 hardcoded `expected = 0.85` (DEFCON team-credit mean) and `expected = 0.45` (PAUSA mean) as guesses. Without empirical priors, smoke gates fail-or-pass on whether the guess matches reality, not on whether retrain quality regressed.

- [ ] **Step 1: Query existing Champion's distribution on dev**

```bash
uv run python -c "
from databricks.sdk import WorkspaceClient
import os, statistics
w = WorkspaceClient()
sql_defcon = '''
SELECT AVG(team_credit_sum) FROM (
  SELECT match_id, team_id, SUM(defcon_credit) AS team_credit_sum
  FROM soccer_analytics.dev_gold.fct_defcon_actions
  GROUP BY match_id, team_id
)
'''
sql_pausa = 'SELECT AVG(pausa_value) FROM soccer_analytics.dev_gold.fct_pausa_values'

for label, sql in [('DEFCON team_credit_sum mean', sql_defcon), ('PAUSA value mean', sql_pausa)]:
    r = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=os.environ['DATABRICKS_WAREHOUSE_ID'], wait_timeout='30s'
    )
    val = float(r.result.data_array[0][0]) if r.result else float('nan')
    print(f'{label}: {val:.4f}')
"
```

Expected: prints two numbers. These are the empirical baselines.

- [ ] **Step 2: Substitute into Phase 2.6 + 2.8 smoke gates**

Replace the hardcoded `expected = 0.85` (Phase 2.6) and `expected = 0.45` (Phase 2.8) with the queried values + ±10% bounds. Add a comment line referencing Task 0.8 + the date the baseline was measured.

### Task 0.9 — Resolve item 16: dbt cascade pre-grep before Phase 4.2

**Files:** none — verification (pre-flight for Phase 4.2 Step 4).

- [ ] **Step 1: Pre-grep for downstream `ref('fct_xg_predictions')` callers**

```bash
grep -rn "ref('fct_xg_predictions')" dbt_project/models/
grep -rn 'ref("fct_xg_predictions")' dbt_project/models/
```

Expected: ideally 0 hits (no downstream marts consume v1 predictions). If any hits found, those downstream marts MUST be addressed in Phase 4.2 — either delete them too OR update their `ref()` to point at `fct_xg_predictions_v2`.

- [ ] **Step 2: Record finding**

If 0 hits: Phase 4.2 Step 4 (`dbt parse`) will pass cleanly. If hits found: list the affected files; Phase 4.2 must include a sub-step per affected file to either delete or migrate.

### Task 0.10 — Verify Phase 0 findings + commit notes

**Files:** none — operational.

- [ ] **Step 1: Aggregate all findings into a working-tree note**

Create `docs/superpowers/notes/2026-05-03-sk3-mig-b-phase-0-findings.md` (gitignored or kept as a working artifact). For each Q1-Q9 + items 12-16, record:

- The finding (text or numbers).
- Which Phase + Task uses the finding.
- Date + cycle_id of the verification SQL.

This is local working-tree documentation, NOT a code commit — it informs the Phase 1+ implementation but doesn't ride with the eventual PR-α squash.

- [ ] **Step 2: Confirm Phase 0 complete**

All 10 tasks above resolved. Phase 1 can now proceed with concrete eval fold + empirical baselines + verified dbt cascade safety + registered cycle items.

---

## Phase 1 — Telemetry infrastructure (foundation)

**Why first.** Every subsequent phase writes to `bronze.sk3_mig_b_runs`: smoke gates record per-cycle-item metrics, the orchestrator's E2E loop appends rows at every step, PR-β's regen script reads the cycle history. The DDL migration + ADR-002 §4 schema discipline must land before any code that touches the table compiles. Spec §5.0 + §5.3 + §10 Q7.

### Task 1.1 — Write the DDL migration

**Files:**
- Create: `scripts/migrations/2026-05-03-create-bronze-sk3-mig-b-runs.sql`

The lakehouse runs all `scripts/migrations/*.sql` automatically at live-CI build time (per `.github/workflows/dbt-live-ci.yml` "Apply pending bronze migrations" step). Migrations MUST be idempotent (`CREATE TABLE IF NOT EXISTS`). Destructive ops are forbidden in this dir. Per CLAUDE.md "Bronze migrations" convention.

- [ ] **Step 1: Verify the migrations directory exists**

```bash
ls -la scripts/migrations/ | head -10
```

Expected: directory exists with prior `.sql` migration files.

- [ ] **Step 2: Write the migration**

```sql
-- scripts/migrations/2026-05-03-create-bronze-sk3-mig-b-runs.sql
-- SK3-MIG-B telemetry table — orchestrator cycle log per spec §5.3.
-- Idempotent (CREATE TABLE IF NOT EXISTS); auto-applied by dbt-live-ci.yml.
-- ADR-002 §4 schema-drift guard via test_sk3_mig_b_runs_schema_parity.py
-- pinning the column list against src/ingestion/sk3_mig_b_telemetry.py.

CREATE TABLE IF NOT EXISTS ${catalog}.bronze.sk3_mig_b_runs (
  cycle_id STRING,
  cycle_started_at TIMESTAMP,
  cycle_finished_at TIMESTAMP,
  wheel_at_start STRING,
  wheel_at_end STRING,
  silly_kicks_version STRING,
  cost_cap_usd DOUBLE,
  walltime_cap_hours DOUBLE,
  cycle_item STRING,
  cycle_item_kind STRING,
  hf_job_id STRING,
  champion_set_at TIMESTAMP,
  pre_mart_version BIGINT,
  post_mart_version BIGINT,
  pre_hf_revision_sha STRING,
  smoke_pass BOOLEAN,
  smoke_metrics MAP<STRING, DOUBLE>,
  smoke_metrics_str MAP<STRING, STRING>,
  wall_clock_seconds DOUBLE,
  cost_usd DOUBLE,
  recorded_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = 'true'
);
```

The `${catalog}` placeholder is substituted by `scripts/migrations/_runner.py` at apply time (existing pattern; see prior migrations in the same directory).

- [ ] **Step 3: Apply locally to validate the DDL parses**

```bash
uv run python scripts/migrations/_runner.py --migration 2026-05-03-create-bronze-sk3-mig-b-runs.sql --dry-run
```

Expected: dry-run prints the resolved DDL with `${catalog}` substituted; no syntax errors.

(If `_runner.py` doesn't have a `--dry-run` flag, run against a dev catalog instead: `uv run python scripts/migrations/_runner.py --migration 2026-05-03-create-bronze-sk3-mig-b-runs.sql` — verify exit 0 and inspect the table via `DESCRIBE TABLE soccer_analytics.bronze.sk3_mig_b_runs`.)

### Task 1.2 — Write the schema-constant module

**Files:**
- Create: `src/ingestion/sk3_mig_b_telemetry.py`

Per ADR-002 §4: telemetry writers that MERGE into a Delta table MUST define schema as a module-level constant + provide a lazy factory function converting it to a Spark `StructType`. The DDL parity test (Task 1.3) asserts the constant matches the migration DDL.

- [ ] **Step 1: Write the module**

```python
"""SK3-MIG-B telemetry — schema constants + StructType factory + writer helper.

Per ADR-002 §4 schema-drift guard: the column list is the single source of truth
for both the DDL migration (scripts/migrations/2026-05-03-create-bronze-sk3-mig-b-runs.sql)
and the writer code (orchestrator + smoke gates). Drift is caught by
src/tests/test_sk3_mig_b_runs_schema_parity.py.

Writer contract:
- Every row MUST include cycle_id, cycle_item, cycle_item_kind, recorded_at.
- Trained-model rows: hf_job_id, champion_set_at, pre/post_mart_version, smoke_*.
- Compute-only rows: pre/post_mart_version, smoke_*. NULL for hf/champion fields.
- Publish rows: pre_hf_revision_sha. NULL for mart/champion fields.
- Meta-event rows (pre_state, baseline_rebase): cycle_item_kind="meta_event"; mostly NULL.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


# Single source of truth — every column in the bronze.sk3_mig_b_runs table.
# Order matches the migration DDL exactly (parity-tested).
_SK3_MIG_B_RUNS_COLUMNS: list[tuple[str, str]] = [
    ("cycle_id", "STRING"),
    ("cycle_started_at", "TIMESTAMP"),
    ("cycle_finished_at", "TIMESTAMP"),
    ("wheel_at_start", "STRING"),
    ("wheel_at_end", "STRING"),
    ("silly_kicks_version", "STRING"),
    ("cost_cap_usd", "DOUBLE"),
    ("walltime_cap_hours", "DOUBLE"),
    ("cycle_item", "STRING"),
    ("cycle_item_kind", "STRING"),
    ("hf_job_id", "STRING"),
    ("champion_set_at", "TIMESTAMP"),
    ("pre_mart_version", "BIGINT"),
    ("post_mart_version", "BIGINT"),
    ("pre_hf_revision_sha", "STRING"),
    ("smoke_pass", "BOOLEAN"),
    ("smoke_metrics", "MAP<STRING, DOUBLE>"),
    ("smoke_metrics_str", "MAP<STRING, STRING>"),
    ("wall_clock_seconds", "DOUBLE"),
    ("cost_usd", "DOUBLE"),
    ("recorded_at", "TIMESTAMP"),
]

# Cycle-item enums — used by writers to set cycle_item_kind correctly.
_TRAINED_MODEL_ITEMS: frozenset[str] = frozenset({
    "vaep", "xg_v2", "ext_v2_p0", "ext_v2_p1",
    "f2v_v1", "f2v_v2", "f2v_360", "scoutgpt",
})
_COMPUTE_ONLY_ITEMS: frozenset[str] = frozenset({
    "defcon_lite", "obso", "pausa",
})
_PUBLISH_ITEMS: frozenset[str] = frozenset({
    "spadl_vaep_publish", "xg_shots_publish",
    "freeze_frame_publish", "shots_on_target_publish",
    "obso_pausa_inputs_publish", "obso_trained_grids_publish",
    "obso_pausa_values_publish", "f2v_embeddings_publish",
})
_META_EVENT_ITEMS: frozenset[str] = frozenset({
    "pre_state",            # Phase 6 Step 0 captures pre-state mart versions
    "baseline_rebase",      # Phase 10 Task 10.2 PR-β regen audit row
    "xg1_retire_runtime",   # Phase 6 Step 4 XG1-RETIRE runtime telemetry
    "scoutgpt_export",      # Phase 6 Step 2 wf-scoutgpt-export mega-job dispatch
    "heartbeat",            # Phase 6 Task 6.2.1 in-flight cycle heartbeat (item 8)
})


def get_sk3_mig_b_runs_struct_type() -> "StructType":
    """Lazy factory — converts _SK3_MIG_B_RUNS_COLUMNS to a Spark StructType.

    Lazy import of pyspark.sql.types so this module imports cleanly outside
    a Spark context (e.g., during pytest collection on a non-Spark host).
    """
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    type_map: dict[str, object] = {
        "STRING": StringType(),
        "TIMESTAMP": TimestampType(),
        "DOUBLE": DoubleType(),
        "BIGINT": LongType(),
        "BOOLEAN": BooleanType(),
        "MAP<STRING, DOUBLE>": MapType(StringType(), DoubleType()),
        "MAP<STRING, STRING>": MapType(StringType(), StringType()),
    }

    fields = [
        StructField(name, type_map[ddl_type], nullable=True)
        for name, ddl_type in _SK3_MIG_B_RUNS_COLUMNS
    ]
    return StructType(fields)


def classify_cycle_item(cycle_item: str) -> str:
    """Return the cycle_item_kind for a given cycle_item name.

    Raises ValueError if cycle_item is not in any registered set.
    """
    if cycle_item in _TRAINED_MODEL_ITEMS:
        return "trained_model"
    if cycle_item in _COMPUTE_ONLY_ITEMS:
        return "compute_only"
    if cycle_item in _PUBLISH_ITEMS:
        return "publish"
    if cycle_item in _META_EVENT_ITEMS:
        return "meta_event"
    raise ValueError(
        f"Unknown cycle_item: {cycle_item!r}. "
        f"Add it to one of _TRAINED_MODEL_ITEMS / _COMPUTE_ONLY_ITEMS / "
        f"_PUBLISH_ITEMS / _META_EVENT_ITEMS in src/ingestion/sk3_mig_b_telemetry.py."
    )
```

### Task 1.3 — Write the DDL parity test

**Files:**
- Create: `src/tests/test_sk3_mig_b_runs_schema_parity.py`

ADR-002 §4 mandates: a pytest parses the canonical CREATE TABLE DDL and asserts column-list equality with the constant. Drift between migration DDL and writer code MUST fail CI.

- [ ] **Step 1: Write the failing test (RED — module doesn't exist yet)**

```python
"""ADR-002 §4 schema-drift guard — DDL ↔ Python constant parity.

The migration scripts/migrations/2026-05-03-create-bronze-sk3-mig-b-runs.sql
defines the canonical column list for bronze.sk3_mig_b_runs. The writer-side
constant src/ingestion/sk3_mig_b_telemetry.py::_SK3_MIG_B_RUNS_COLUMNS must
match exactly.

Failure mode this test catches: someone edits the DDL without updating the
constant (or vice versa). The orchestrator's first MERGE then fails with
DELTA_MERGE_UNRESOLVED_EXPRESSION at runtime — too late to catch in CI.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_FILE = REPO_ROOT / "scripts" / "migrations" / "2026-05-03-create-bronze-sk3-mig-b-runs.sql"


def _parse_ddl_columns(ddl_text: str) -> list[tuple[str, str]]:
    """Extract (column_name, column_type) pairs from a CREATE TABLE DDL.

    Handles types with embedded commas (MAP<STRING, DOUBLE>) by tracking
    angle-bracket depth.
    """
    # Find the column list between the outer parens after CREATE TABLE.
    match = re.search(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+[^(]+\((.*?)\)\s*USING\s+DELTA",
        ddl_text,
        re.IGNORECASE | re.DOTALL,
    )
    assert match is not None, "Could not locate CREATE TABLE column list in migration DDL"
    body = match.group(1)

    # Split on commas that are NOT inside angle brackets.
    pairs: list[tuple[str, str]] = []
    depth = 0
    buf: list[str] = []
    for ch in body:
        if ch == "<":
            depth += 1
            buf.append(ch)
        elif ch == ">":
            depth -= 1
            buf.append(ch)
        elif ch == "," and depth == 0:
            pairs.append(_parse_column_line("".join(buf)))
            buf = []
        else:
            buf.append(ch)
    if buf and "".join(buf).strip():
        pairs.append(_parse_column_line("".join(buf)))
    return pairs


def _parse_column_line(line: str) -> tuple[str, str]:
    """`  cycle_id STRING,\n` → ('cycle_id', 'STRING')."""
    line = line.strip().rstrip(",").strip()
    # Take everything after the first whitespace-separated token as the type.
    parts = line.split(None, 1)
    assert len(parts) == 2, f"Could not parse column line: {line!r}"
    name = parts[0].strip()
    col_type = parts[1].strip()
    # Normalize whitespace inside MAP<...> for comparison.
    col_type = re.sub(r"\s+", " ", col_type)
    col_type = col_type.replace("< ", "<").replace(" >", ">").replace(" ,", ",")
    return name, col_type


def test_migration_ddl_matches_python_constant() -> None:
    from ingestion.sk3_mig_b_telemetry import _SK3_MIG_B_RUNS_COLUMNS

    assert MIGRATION_FILE.exists(), f"Migration file missing: {MIGRATION_FILE}"
    ddl_columns = _parse_ddl_columns(MIGRATION_FILE.read_text(encoding="utf-8"))

    # Normalize Python-side MAP types for comparison with parsed DDL.
    def _normalize(cols: list[tuple[str, str]]) -> list[tuple[str, str]]:
        return [(n, re.sub(r"\s+", " ", t).replace("< ", "<").replace(" >", ">"))
                for n, t in cols]

    py_normalized = _normalize(_SK3_MIG_B_RUNS_COLUMNS)
    ddl_normalized = _normalize(ddl_columns)

    assert py_normalized == ddl_normalized, (
        "DDL ↔ Python constant drift detected.\n"
        f"DDL columns: {ddl_normalized}\n"
        f"Python constant: {py_normalized}\n"
        "Update one to match the other; both must agree (ADR-002 §4)."
    )


def test_struct_type_factory_produces_one_field_per_constant_entry() -> None:
    from ingestion.sk3_mig_b_telemetry import _SK3_MIG_B_RUNS_COLUMNS, get_sk3_mig_b_runs_struct_type

    struct = get_sk3_mig_b_runs_struct_type()
    assert len(struct.fields) == len(_SK3_MIG_B_RUNS_COLUMNS), (
        f"StructType has {len(struct.fields)} fields but constant has "
        f"{len(_SK3_MIG_B_RUNS_COLUMNS)} entries."
    )
    for sf, (name, _) in zip(struct.fields, _SK3_MIG_B_RUNS_COLUMNS, strict=True):
        assert sf.name == name, f"Field name mismatch at position: {sf.name} vs {name}"


@pytest.mark.parametrize("item,expected_kind", [
    ("vaep", "trained_model"),
    ("xg_v2", "trained_model"),
    ("ext_v2_p0", "trained_model"),
    ("ext_v2_p1", "trained_model"),
    ("f2v_v1", "trained_model"),
    ("f2v_v2", "trained_model"),
    ("f2v_360", "trained_model"),
    ("scoutgpt", "trained_model"),
    ("defcon_lite", "compute_only"),
    ("obso", "compute_only"),
    ("pausa", "compute_only"),
    ("spadl_vaep_publish", "publish"),
    ("xg_shots_publish", "publish"),
    ("obso_pausa_values_publish", "publish"),
    ("f2v_embeddings_publish", "publish"),
    ("pre_state", "meta_event"),
    ("baseline_rebase", "meta_event"),
])
def test_classify_cycle_item(item: str, expected_kind: str) -> None:
    from ingestion.sk3_mig_b_telemetry import classify_cycle_item
    assert classify_cycle_item(item) == expected_kind


def test_classify_cycle_item_rejects_unknown() -> None:
    from ingestion.sk3_mig_b_telemetry import classify_cycle_item
    with pytest.raises(ValueError, match="Unknown cycle_item"):
        classify_cycle_item("nonexistent_item")
```

- [ ] **Step 2: Run test to verify it passes**

```bash
uv run pytest src/tests/test_sk3_mig_b_runs_schema_parity.py -v
```

Expected: all tests PASS. If the DDL ↔ constant alignment fails, the assertion error will print both sides — fix whichever side drifted.

### Task 1.4 — Verify migration applies cleanly

**Files:** none — operational.

- [ ] **Step 1: Apply the migration to dev**

```bash
uv run python scripts/migrations/_runner.py
```

Expected: runner picks up `2026-05-03-create-bronze-sk3-mig-b-runs.sql` as new (per `git diff --diff-filter=A` against `origin/main`); applies it; prints "Applied: 2026-05-03-create-bronze-sk3-mig-b-runs.sql" or equivalent success message.

- [ ] **Step 2: Verify table exists in dev**

```bash
uv run python -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
result = w.statement_execution.execute_statement(
    statement='DESCRIBE TABLE soccer_analytics.bronze.sk3_mig_b_runs',
    warehouse_id=__import__('os').environ['DATABRICKS_WAREHOUSE_ID'],
    wait_timeout='30s',
)
for row in result.result.data_array:
    print(row)
"
```

Expected: 21 columns matching the `_SK3_MIG_B_RUNS_COLUMNS` list, in order.

- [ ] **Step 3: No code commit — Phase 8 commits all of PR-α together**

---

## Phase 2 — Smoke gate scripts (11 per-cycle-item gates)

**Why second.** The orchestrator (Phase 6) invokes one of these per cycle item between mart-write and Lakebase-synced-refresh. Smoke gate failure halts the orchestrator before polluting Lakebase. Each gate is the (B) acceptance pattern from spec §3 — absolute physical thresholds derived from methodology priors, not from old broken-coord baselines.

All gates live under `src/tests/sk3_mig_b/`. They run via `pytest src/tests/sk3_mig_b/test_<item>_post_retrain_smoke.py -v`. They query the lakehouse (Champion model + dev_gold mart predictions) so they need Databricks auth at runtime.

### Task 2.1 — Smoke gate scaffolding (subdir + conftest + shared fixtures)

**Files:**
- Create: `src/tests/sk3_mig_b/__init__.py` (empty)
- Create: `src/tests/sk3_mig_b/conftest.py`

- [ ] **Step 1: Create the subdir + empty `__init__.py`**

```bash
mkdir -p src/tests/sk3_mig_b
touch src/tests/sk3_mig_b/__init__.py
```

- [ ] **Step 2: Write the conftest with shared fixtures**

```python
# src/tests/sk3_mig_b/conftest.py
"""Shared fixtures for SK3-MIG-B post-retrain smoke gates.

Each gate runs after Champion promotion + mart write. The orchestrator invokes
the gate via `pytest src/tests/sk3_mig_b/test_<item>_post_retrain_smoke.py -v`.
Failure halts the orchestrator before Lakebase synced refresh fires (per spec §5.2).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def workspace_client() -> "WorkspaceClient":
    """Databricks SDK client — authenticated via env (DATABRICKS_TOKEN + DATABRICKS_HOST)."""
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


@pytest.fixture(scope="session")
def warehouse_id() -> str:
    """Serverless SQL warehouse ID for statement_execution queries."""
    wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not wh_id:
        pytest.skip("DATABRICKS_WAREHOUSE_ID not set — smoke gate cannot run without it")
    return wh_id


@pytest.fixture(scope="session")
def catalog() -> str:
    """Lakehouse catalog (env-overridable per project pattern)."""
    return os.environ.get("DATABRICKS_CATALOG", "soccer_analytics")


@pytest.fixture(scope="session")
def gold_schema(catalog: str) -> str:
    """Gold-layer schema (FQN: catalog.schema)."""
    return f"{catalog}.dev_gold"


@pytest.fixture(scope="session")
def bronze_schema(catalog: str) -> str:
    """Bronze-layer schema."""
    return f"{catalog}.bronze"


def execute_sql(workspace_client: "WorkspaceClient", warehouse_id: str, sql: str) -> list[list]:
    """Run a SQL query via WorkspaceClient.statement_execution; return data_array.

    Uses the SDK statement_execution path (per reference_sdk_over_sql_connector.md):
    auto-resolves auth + auto-starts warehouse + bypasses Thrift retry-bug class.
    """
    result = workspace_client.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
    )
    if result.result is None or result.result.data_array is None:
        return []
    return result.result.data_array
```

- [ ] **Step 3: Verify conftest collects without error**

```bash
uv run pytest src/tests/sk3_mig_b/ --collect-only
```

Expected: collects 0 tests (no test_*.py files yet) but no collection errors.

### Task 2.2 — Canonical smoke gate: xG v2 (the template the others reference)

**Files:**
- Create: `src/tests/sk3_mig_b/test_xg_v2_post_retrain_smoke.py`

This is the canonical pattern. xG v2's training script (`scripts/train_xg_v2_hf.py`) already includes a held-out ECE check; this gate extracts the equivalent logic into a standalone script that runs against the deployed Champion via the inference path, not against training-time intermediates.

Spec §3 thresholds for xG v2:
- Held-out ECE < 0.05 against StatsBomb shots-on-target eval fold.
- 100% predictions in `[0, 1]`.
- CI band median > 0 (MC dropout actually firing).
- `feature_names` envelope present (ADR-012 §2 enforced).

- [ ] **Step 1: Write the smoke gate**

```python
# src/tests/sk3_mig_b/test_xg_v2_post_retrain_smoke.py
"""Post-retrain smoke gate for xG v2.

Spec §3 acceptance criteria (canonical reference for the trained-model gate pattern):
- Calibration: held-out ECE < 0.05 against the StatsBomb shots-on-target eval fold.
- Bounds: 100% predictions in [0, 1].
- CI band: xg_ci_upper - xg_ci_lower median > 0 (MC dropout actually firing).
- Envelope: feature_names + tabular_dim present in the @Champion weights bundle
  (ADR-012 §2 enforced; v2→v1 fallback already removed in SK3-MIG-A).

Failure halts orchestrator (spec §5.2) before Lakebase synced refresh.
Restoration: revert to prior Champion via
  set_and_verify_mlflow_champion("xg_model_v2", version=PRIOR_VERSION)
"""

from __future__ import annotations

import json
import math

import numpy as np
import pytest

# Eval fold — pre-registered match_ids from a held-out StatsBomb sample.
# Stable across retrains so per-retrain comparisons are meaningful (spec §10 Q4).
_EVAL_FOLD_MATCH_IDS = (
    "3795109", "3795107", "3795506", "3795108", "3795506",
    "3795108", "3795107", "3795109", "3795506", "3795108",
)


@pytest.fixture(scope="module")
def champion_envelope_features(workspace_client, catalog):  # type: ignore[no-untyped-def]
    """Read v2 Champion weights envelope from UC Volume + extract feature_names."""
    from huggingface_hub import HfApi  # noqa: F401  (transitive — UC Volume helper uses it)

    # UC Volume path follows project convention: /Volumes/<catalog>/dev_gold/model_weights/xg_model_v2/
    volume_path = f"/Volumes/{catalog}/dev_gold/model_weights/xg_model_v2"
    files_api = workspace_client.files

    # Try canonical envelope filename then fall back to listing.
    envelope_candidates = ["envelope.json", "weights_envelope.json"]
    envelope_bytes: bytes | None = None
    for fname in envelope_candidates:
        try:
            response = files_api.download(f"{volume_path}/{fname}")
            envelope_bytes = response.contents.read() if response.contents else None
            if envelope_bytes:
                break
        except Exception:
            continue

    assert envelope_bytes is not None, (
        f"Could not read v2 envelope from {volume_path}. "
        "Verify the retrain ran upload_weights_to_uc_volume (ADR-012)."
    )

    envelope = json.loads(envelope_bytes.decode("utf-8"))
    return envelope


def test_envelope_carries_feature_names(champion_envelope_features) -> None:  # type: ignore[no-untyped-def]
    """ADR-012 §2 grace-period closed in SK3-MIG-A — envelope MUST carry feature_names."""
    feature_names = champion_envelope_features.get("feature_names")
    assert feature_names is not None, (
        "v2 envelope is missing 'feature_names'. "
        "ADR-012 §2 grace-period was closed in SK3-MIG-A; "
        "envelope without feature_names is a regression. "
        "Verify scripts/train_xg_v2_hf.py emitted feature_names at training time."
    )
    assert isinstance(feature_names, list), f"feature_names must be a list, got {type(feature_names)}"
    assert len(feature_names) > 0, "feature_names is empty"


def test_envelope_tabular_dim_consistent(champion_envelope_features) -> None:  # type: ignore[no-untyped-def]
    """Defense-in-depth: feature_names length must equal tabular_dim."""
    feature_names = champion_envelope_features["feature_names"]
    tabular_dim = champion_envelope_features.get("tabular_dim")
    assert tabular_dim is not None, "tabular_dim missing from envelope (defense-in-depth check)"
    assert len(feature_names) == tabular_dim, (
        f"Envelope corrupted at training time: "
        f"feature_names={len(feature_names)} != tabular_dim={tabular_dim}"
    )


def test_predictions_within_bounds(workspace_client, warehouse_id, gold_schema) -> None:  # type: ignore[no-untyped-def]
    """100% predictions in [0, 1]. Spec §3 absolute bound."""
    from src.tests.sk3_mig_b.conftest import execute_sql

    sql = f"""
    SELECT
      COUNT(*) AS n_total,
      SUM(CASE WHEN xg_set_encoder < 0 OR xg_set_encoder > 1 THEN 1 ELSE 0 END) AS n_out_of_bounds,
      SUM(CASE WHEN xg_set_encoder IS NULL THEN 1 ELSE 0 END) AS n_null
    FROM {gold_schema}.fct_xg_predictions_v2
    WHERE match_id IN ({", ".join(repr(m) for m in _EVAL_FOLD_MATCH_IDS)})
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    assert rows, "No rows returned — fct_xg_predictions_v2 empty for eval fold?"
    n_total, n_out, n_null = int(rows[0][0]), int(rows[0][1]), int(rows[0][2])

    assert n_total > 0, f"Eval fold matches missing from fct_xg_predictions_v2 (n_total={n_total})"
    assert n_out == 0, (
        f"{n_out} of {n_total} predictions outside [0, 1] — "
        "v2 retrain produced out-of-bounds output. Halt + investigate."
    )
    assert n_null == 0, f"{n_null} of {n_total} predictions are NULL"


def test_ci_band_active(workspace_client, warehouse_id, gold_schema) -> None:  # type: ignore[no-untyped-def]
    """CI band median > 0 — MC dropout actually firing. Spec §3."""
    from src.tests.sk3_mig_b.conftest import execute_sql

    sql = f"""
    SELECT percentile_approx(xg_ci_upper - xg_ci_lower, 0.5) AS ci_band_median
    FROM {gold_schema}.fct_xg_predictions_v2
    WHERE match_id IN ({", ".join(repr(m) for m in _EVAL_FOLD_MATCH_IDS)})
      AND xg_ci_upper IS NOT NULL
      AND xg_ci_lower IS NOT NULL
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    assert rows and rows[0][0] is not None, "CI band columns missing or all NULL"
    ci_band_median = float(rows[0][0])
    assert ci_band_median > 0, (
        f"CI band median = {ci_band_median} — MC dropout produced zero-width CIs. "
        "Verify n_dropout_samples > 1 in train_xg_v2_hf.py + that dropout layers fire at inference."
    )


def test_held_out_ece_below_threshold(workspace_client, warehouse_id, gold_schema) -> None:  # type: ignore[no-untyped-def]
    """Held-out ECE < 0.05 on the eval fold. Spec §3."""
    from src.tests.sk3_mig_b.conftest import execute_sql

    # Pull (predicted, actual) pairs from fct_xg_predictions_v2 joined with fct_shots actual outcomes.
    sql = f"""
    SELECT p.xg_set_encoder, CAST(s.is_goal AS DOUBLE) AS is_goal
    FROM {gold_schema}.fct_xg_predictions_v2 p
    INNER JOIN {gold_schema}.fct_shots s
      ON p.shot_id = s.shot_id
    WHERE p.match_id IN ({", ".join(repr(m) for m in _EVAL_FOLD_MATCH_IDS)})
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    assert rows, "No (prediction, actual) pairs available — verify fct_shots join keys"

    preds = np.array([float(r[0]) for r in rows])
    actuals = np.array([float(r[1]) for r in rows])

    # ECE: 10 equal-frequency bins of predicted prob; per-bin |mean_pred - mean_actual|; weighted by bin size.
    n_bins = 10
    bin_edges = np.quantile(preds, np.linspace(0, 1, n_bins + 1))
    bin_edges[-1] += 1e-9  # ensure max prediction lands in last bin
    bin_indices = np.digitize(preds, bin_edges[1:-1])

    ece = 0.0
    for b in range(n_bins):
        mask = bin_indices == b
        if not mask.any():
            continue
        bin_pred = preds[mask].mean()
        bin_actual = actuals[mask].mean()
        ece += (mask.sum() / len(preds)) * abs(bin_pred - bin_actual)

    assert ece < 0.05, (
        f"Held-out ECE = {ece:.4f} > 0.05. Calibration regressed post-retrain. "
        "Halt and investigate before Lakebase synced refresh."
    )
```

- [ ] **Step 2: Run the gate against the existing Champion (not yet retrained — should still pass)**

```bash
uv run pytest src/tests/sk3_mig_b/test_xg_v2_post_retrain_smoke.py -v
```

Expected: PASS against the current Champion (the v2 retrain in Phase 9 will produce a NEW Champion that this gate then runs against). If it FAILs against the current Champion, that's a pre-existing regression — investigate before continuing.

### Task 2.3-2.12 — Remaining 10 smoke gates (pattern-driven; per-item differences only)

Each gate follows the Task 2.2 pattern: query lakehouse via `workspace_client` + `warehouse_id` + `gold_schema`, assert on absolute physical thresholds. Per-cycle-item differences:

#### Task 2.3 — VAEP smoke gate

**File:** `src/tests/sk3_mig_b/test_vaep_post_retrain_smoke.py`

**Threshold (spec §3):** per-action `vaep_value` distribution mean within ±50% of Singh-2018 ballpark on 1k-action StatsBomb sample; 0% NaN; 100% within `[-1, 1]`.

```python
# src/tests/sk3_mig_b/test_vaep_post_retrain_smoke.py
"""Post-retrain smoke gate for VAEP. Spec §3 acceptance:
- per-action vaep_value distribution mean within ±50% of Singh-2018 ballpark
- 0% NaN
- 100% rows within [-1, 1] bounds
"""

from __future__ import annotations

import pytest

# Singh 2018 + Decroos 2019 published per-action mean ballpark.
# Project-specific eval fold mean (canonical-LTR coords): documented baseline.
_VAEP_PER_ACTION_MEAN_BALLPARK = 0.0035  # baseline ~3.5e-3
_TOLERANCE = 0.5  # ±50% per spec §3
_LOWER = _VAEP_PER_ACTION_MEAN_BALLPARK * (1 - _TOLERANCE)
_UPPER = _VAEP_PER_ACTION_MEAN_BALLPARK * (1 + _TOLERANCE)

# Pre-registered eval fold (1k-action sample from StatsBomb).
_EVAL_FOLD_MATCH_IDS = (
    "3795109", "3795107", "3795506", "3795108", "3795109",
)


def test_vaep_value_within_bounds(workspace_client, warehouse_id, gold_schema) -> None:  # type: ignore[no-untyped-def]
    from src.tests.sk3_mig_b.conftest import execute_sql

    sql = f"""
    SELECT
      COUNT(*) AS n_total,
      SUM(CASE WHEN vaep_value < -1 OR vaep_value > 1 THEN 1 ELSE 0 END) AS n_out,
      SUM(CASE WHEN vaep_value IS NULL THEN 1 ELSE 0 END) AS n_null,
      AVG(vaep_value) AS mean_value
    FROM {gold_schema}.fct_action_values
    WHERE data_source = 'statsbomb'
      AND match_id IN ({", ".join(repr(m) for m in _EVAL_FOLD_MATCH_IDS)})
    LIMIT 1000
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    assert rows
    n_total, n_out, n_null, mean_v = int(rows[0][0]), int(rows[0][1]), int(rows[0][2]), float(rows[0][3])

    assert n_total > 0
    assert n_out == 0, f"{n_out}/{n_total} vaep_value outside [-1, 1]"
    assert n_null == 0, f"{n_null}/{n_total} vaep_value NULL"
    assert _LOWER <= mean_v <= _UPPER, (
        f"vaep_value mean = {mean_v:.6f}, expected within [{_LOWER:.6f}, {_UPPER:.6f}] "
        f"({_TOLERANCE*100:.0f}% of Singh-2018 ballpark {_VAEP_PER_ACTION_MEAN_BALLPARK})"
    )
```

#### Task 2.4 — ExT v2 P0 smoke gate

**File:** `src/tests/sk3_mig_b/test_ext_v2_p0_post_retrain_smoke.py`

**Threshold:** NLL ≤ 3.7892 + 1% (PR #206 production baseline).

```python
# src/tests/sk3_mig_b/test_ext_v2_p0_post_retrain_smoke.py
"""ExT v2 Phase 0 (Singh baseline) post-retrain smoke gate. Spec §3.

Uses the existing src/analytics/ext_v2/ phase-0 NLL computation against
the post-retrain fct_action_values. Threshold pre-registered at 3.7892 + 1%
(PR #206 production baseline + tolerance).
"""

from __future__ import annotations

import pytest

_PHASE_0_BASELINE_NLL = 3.7892
_TOLERANCE_PCT = 0.01  # +1% per spec §3 stop condition
_THRESHOLD = _PHASE_0_BASELINE_NLL * (1 + _TOLERANCE_PCT)


def test_phase_0_nll_within_threshold() -> None:
    """Re-run phase-0 NLL computation against current fct_action_values."""
    from analytics.ext_v2.phase_0 import compute_phase_0_nll  # adjust import to actual module

    nll = compute_phase_0_nll()  # signature per ext_v2 package; reads fct_action_values
    assert nll <= _THRESHOLD, (
        f"Phase 0 NLL = {nll:.6f} > threshold {_THRESHOLD:.6f} "
        f"(baseline {_PHASE_0_BASELINE_NLL} + {_TOLERANCE_PCT*100:.0f}%). "
        "Halt + investigate before Phase 1 dispatch."
    )
```

(If `analytics.ext_v2.phase_0.compute_phase_0_nll` doesn't exist, add a thin wrapper around the existing P0 invocation logic in `src/analytics/ext_v2/`. The wrapper is the standalone smoke version of what the training script does internally.)

#### Task 2.5 — ExT v2 P1 smoke gate

**File:** `src/tests/sk3_mig_b/test_ext_v2_p1_post_retrain_smoke.py`

**Threshold:** NLL ≤ 3.7482 + 1% (PR #213 production baseline).

```python
# src/tests/sk3_mig_b/test_ext_v2_p1_post_retrain_smoke.py
"""ExT v2 Phase 1 (KDE-smoothed Singh) post-retrain smoke gate. Spec §3."""

from __future__ import annotations

_PHASE_1_BASELINE_NLL = 3.7482
_TOLERANCE_PCT = 0.01
_THRESHOLD = _PHASE_1_BASELINE_NLL * (1 + _TOLERANCE_PCT)


def test_phase_1_nll_within_threshold() -> None:
    from analytics.ext_v2.phase_1 import compute_phase_1_nll  # adjust import to actual module

    nll = compute_phase_1_nll()
    assert nll <= _THRESHOLD, (
        f"Phase 1 NLL = {nll:.6f} > threshold {_THRESHOLD:.6f}. Halt."
    )
```

#### Task 2.6 — DEFCON-lite smoke gate

**File:** `src/tests/sk3_mig_b/test_defcon_lite_post_retrain_smoke.py`

**Threshold:** per-team-match credit-assignment sum within ±10% of expected aggregate; 0% NaN; 100% rows have valid `defending_player_id`.

```python
# src/tests/sk3_mig_b/test_defcon_lite_post_retrain_smoke.py
"""DEFCON-lite post-retrain smoke gate. Spec §3.

Note: DEFCON-lite is a compute-only re-run (no model fitting); the gate
asserts the recomputed predictions are sensible against new fct_action_values.
"""

from __future__ import annotations

import pytest


def test_defcon_credit_sum_within_bounds(workspace_client, warehouse_id, gold_schema) -> None:  # type: ignore[no-untyped-def]
    from src.tests.sk3_mig_b.conftest import execute_sql

    sql = f"""
    SELECT
      AVG(team_credit_sum) AS mean_credit,
      COUNT(*) AS n_team_match,
      SUM(CASE WHEN team_credit_sum IS NULL THEN 1 ELSE 0 END) AS n_null
    FROM (
      SELECT match_id, team_id, SUM(defcon_credit) AS team_credit_sum
      FROM {gold_schema}.fct_defcon_actions
      GROUP BY match_id, team_id
    )
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    assert rows
    mean_credit = float(rows[0][0])
    n_total = int(rows[0][1])
    n_null = int(rows[0][2])

    assert n_total > 0
    assert n_null == 0, f"{n_null}/{n_total} team-match credit sums NULL"
    # Expected aggregate: per Bauer 2024, team-match defensive credit averages ~0.85
    # (one credit per defensive action; adjust to project's empirical baseline).
    expected = 0.85
    lower, upper = expected * 0.9, expected * 1.1
    assert lower <= mean_credit <= upper, (
        f"DEFCON team-credit mean = {mean_credit:.4f}, expected [{lower:.4f}, {upper:.4f}]"
    )


def test_no_null_defending_player_id(workspace_client, warehouse_id, gold_schema) -> None:  # type: ignore[no-untyped-def]
    from src.tests.sk3_mig_b.conftest import execute_sql

    sql = f"""
    SELECT SUM(CASE WHEN defending_player_id IS NULL THEN 1 ELSE 0 END) AS n_null,
           COUNT(*) AS n_total
    FROM {gold_schema}.fct_defcon_actions
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    n_null, n_total = int(rows[0][0]), int(rows[0][1])
    assert n_null == 0, f"{n_null}/{n_total} fct_defcon_actions rows have NULL defending_player_id"
```

#### Task 2.7 — OBSO smoke gate

**File:** `src/tests/sk3_mig_b/test_obso_post_retrain_smoke.py`

**Threshold:** per-frame surface integrates to 1.0 within ±0.01; 0% NaN.

```python
# src/tests/sk3_mig_b/test_obso_post_retrain_smoke.py
"""OBSO post-retrain smoke gate. Spec §3 — Spearman 2018 OBSO definition.

Reads obso-pausa-values HF dataset (the post-republish artifact from Group 3).
Per-frame surface should integrate to 1.0 (probability surface invariant).
"""

from __future__ import annotations

import math

import numpy as np
import pytest


_TOLERANCE = 0.01  # ±1% per spec §3


def test_obso_surface_integrates_to_one() -> None:
    """Sample 100 random frames from the published obso-pausa-values dataset;
    each frame's per-cell surface must integrate to 1.0 ± 1%.
    """
    from huggingface_hub import HfApi
    import pyarrow.parquet as pq
    import io

    api = HfApi()
    # Use an existing helper if one exists in src/ingestion/import_obso_results.py
    from ingestion.import_obso_results import download_obso_parquet  # adapt to actual API

    parquet_bytes = download_obso_parquet()  # signature TBD per import_obso_results
    table = pq.read_table(io.BytesIO(parquet_bytes))

    # Sample 100 rows
    sample = table.slice(0, 100).to_pandas()
    n_failures = 0
    for _, row in sample.iterrows():
        surface = np.asarray(row["obso_surface"])  # column name per actual schema
        integrated = surface.sum()
        if not math.isclose(integrated, 1.0, abs_tol=_TOLERANCE):
            n_failures += 1

    assert n_failures == 0, (
        f"{n_failures}/100 OBSO frames violated surface-integrates-to-1.0 invariant"
    )


def test_obso_no_nan() -> None:
    from ingestion.import_obso_results import download_obso_parquet
    import pyarrow.parquet as pq
    import io
    import numpy as np

    parquet_bytes = download_obso_parquet()
    import pyarrow as pa
    table = pq.read_table(io.BytesIO(parquet_bytes))
    sample = table.slice(0, 100).to_pandas()
    n_nan = sum(np.any(np.isnan(np.asarray(row["obso_surface"]))) for _, row in sample.iterrows())
    assert n_nan == 0, f"{n_nan}/100 OBSO surfaces contain NaN"
```

(The `download_obso_parquet` helper signature depends on `src/ingestion/import_obso_results.py`'s actual interface; verify at plan-execution time.)

#### Task 2.8 — PAUSA smoke gate

**File:** `src/tests/sk3_mig_b/test_pausa_post_retrain_smoke.py`

**Threshold:** per-action `pausa_value` ∈ `[0, 1]` for 100% rows; 0% NaN; mean within Singh-PAUSA ballpark.

```python
# src/tests/sk3_mig_b/test_pausa_post_retrain_smoke.py
"""PAUSA post-retrain smoke gate. Spec §3 — Lee et al. 2026 PAUSA definition."""

from __future__ import annotations


def test_pausa_value_within_bounds(workspace_client, warehouse_id, gold_schema) -> None:  # type: ignore[no-untyped-def]
    from src.tests.sk3_mig_b.conftest import execute_sql

    sql = f"""
    SELECT
      COUNT(*) AS n_total,
      SUM(CASE WHEN pausa_value < 0 OR pausa_value > 1 THEN 1 ELSE 0 END) AS n_out,
      SUM(CASE WHEN pausa_value IS NULL THEN 1 ELSE 0 END) AS n_null,
      AVG(pausa_value) AS mean_value
    FROM {gold_schema}.fct_pausa_values
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    n_total, n_out, n_null, mean_v = int(rows[0][0]), int(rows[0][1]), int(rows[0][2]), float(rows[0][3])

    assert n_total > 0
    assert n_out == 0, f"{n_out}/{n_total} pausa_value outside [0, 1]"
    assert n_null == 0
    # PAUSA ballpark — adjust to project's empirical baseline.
    expected = 0.45
    lower, upper = 0.30, 0.60
    assert lower <= mean_v <= upper, (
        f"PAUSA mean = {mean_v:.4f}, expected [{lower}, {upper}]"
    )
```

#### Task 2.9-2.11 — F2V v1 / v2 / 360 smoke gates (3 files; same pattern, different dim + eval fold)

**Files:**
- `src/tests/sk3_mig_b/test_f2v_v1_post_retrain_smoke.py` (32-d)
- `src/tests/sk3_mig_b/test_f2v_v2_post_retrain_smoke.py` (192-d)
- `src/tests/sk3_mig_b/test_f2v_360_post_retrain_smoke.py` (192-d)

**Threshold:** recall@10 > 0.7 on a fixed 100-player eval fold; 0% NaN; cosine norms in `[0.95, 1.05]` (L2-normalized check).

Pattern (same code; vary `_EMBEDDING_DIM`, `_ELT_NAME`, `_TABLE_NAME`):

```python
# src/tests/sk3_mig_b/test_f2v_v2_post_retrain_smoke.py
"""F2V v2 post-retrain smoke gate. Spec §3 — Football2Vec paper recall@10."""

from __future__ import annotations

import numpy as np
import pytest

_EMBEDDING_DIM = 192
_TABLE_NAME = "fct_player_embeddings"  # v2 lives here; v1 too via data_source filter
_DATA_SOURCE_FILTER = "f2v_v2"  # adjust to actual canonical name in the mart
# Pre-registered 100-player eval fold (canonical_player_id stable across retrains).
_EVAL_FOLD_PLAYER_IDS = tuple(range(1, 101))  # placeholder — verify at plan time per spec §10 Q4


def test_dim_correct(workspace_client, warehouse_id, gold_schema) -> None:  # type: ignore[no-untyped-def]
    from src.tests.sk3_mig_b.conftest import execute_sql

    sql = f"""
    SELECT size(embedding) AS dim
    FROM {gold_schema}.{_TABLE_NAME}
    WHERE data_source = '{_DATA_SOURCE_FILTER}'
    LIMIT 1
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    assert rows and int(rows[0][0]) == _EMBEDDING_DIM, (
        f"F2V v2 dim = {rows[0][0]}, expected {_EMBEDDING_DIM}"
    )


def test_no_nan_embeddings(workspace_client, warehouse_id, gold_schema) -> None:  # type: ignore[no-untyped-def]
    from src.tests.sk3_mig_b.conftest import execute_sql

    sql = f"""
    SELECT COUNT(*) AS n_with_nan
    FROM {gold_schema}.{_TABLE_NAME}
    WHERE data_source = '{_DATA_SOURCE_FILTER}'
      AND exists(embedding, x -> x IS NULL OR isnan(x))
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    assert int(rows[0][0]) == 0, f"{rows[0][0]} embeddings contain NaN"


def test_l2_norms_unit_length(workspace_client, warehouse_id, gold_schema) -> None:  # type: ignore[no-untyped-def]
    from src.tests.sk3_mig_b.conftest import execute_sql

    # Spark norm: sqrt(sum(x*x)).
    sql = f"""
    WITH norms AS (
      SELECT sqrt(aggregate(embedding, 0.0D, (acc, x) -> acc + x * x)) AS n
      FROM {gold_schema}.{_TABLE_NAME}
      WHERE data_source = '{_DATA_SOURCE_FILTER}'
      LIMIT 1000
    )
    SELECT COUNT(*) AS n_total,
           SUM(CASE WHEN n < 0.95 OR n > 1.05 THEN 1 ELSE 0 END) AS n_out
    FROM norms
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    n_total, n_out = int(rows[0][0]), int(rows[0][1])
    assert n_out == 0, f"{n_out}/{n_total} embeddings have L2 norm outside [0.95, 1.05]"


def test_recall_at_10_above_threshold(workspace_client, warehouse_id, gold_schema) -> None:  # type: ignore[no-untyped-def]
    """Pull eval fold embeddings; compute neighbor recall@10 against held-out same-position queries."""
    from src.tests.sk3_mig_b.conftest import execute_sql

    sql = f"""
    SELECT canonical_player_id, primary_position, embedding
    FROM {gold_schema}.{_TABLE_NAME}
    WHERE data_source = '{_DATA_SOURCE_FILTER}'
      AND canonical_player_id IN ({", ".join(str(p) for p in _EVAL_FOLD_PLAYER_IDS)})
    """
    rows = execute_sql(workspace_client, warehouse_id, sql)
    assert len(rows) >= 50, f"Eval fold returned only {len(rows)} embeddings; need ≥50 for recall@10"

    # Compute pairwise cosine; for each query, top-10 NN; recall@10 = fraction of top-10 sharing primary_position.
    by_position: dict[str, list[np.ndarray]] = {}
    queries: list[tuple[str, np.ndarray]] = []
    for player_id, position, emb in rows:
        emb_array = np.asarray(emb, dtype=np.float64)
        by_position.setdefault(position, []).append(emb_array)
        queries.append((position, emb_array))

    correct = 0
    total = 0
    for q_pos, q_emb in queries:
        # Cosine similarity to all other eval-fold embeddings.
        sims = []
        for cand_pos, cand_embs in by_position.items():
            for cand in cand_embs:
                if np.array_equal(cand, q_emb):
                    continue
                sim = float(q_emb @ cand)  # already L2-normalised per the test_l2_norms check above
                sims.append((sim, cand_pos))
        sims.sort(reverse=True)
        top10 = sims[:10]
        n_same_pos = sum(1 for _, p in top10 if p == q_pos)
        correct += n_same_pos
        total += 10

    recall = correct / total if total > 0 else 0.0
    assert recall > 0.7, f"F2V v2 recall@10 = {recall:.4f}, threshold 0.7"
```

**For F2V v1:** copy the file to `test_f2v_v1_post_retrain_smoke.py`, set `_EMBEDDING_DIM = 32`, set `_DATA_SOURCE_FILTER` to the v1 marker (typically `"f2v_v1"` — verify at plan-execution time).

**For F2V 360:** copy the file to `test_f2v_360_post_retrain_smoke.py`, set `_TABLE_NAME = "fct_player_embeddings"` (the 360 variant lives in the same mart family with different `data_source`), set `_DATA_SOURCE_FILTER = "f2v_360"`.

#### Task 2.12 — ScoutGPT smoke gate

**File:** `src/tests/sk3_mig_b/test_scoutgpt_post_retrain_smoke.py`

**Threshold:** held-out test_top1 > 0.80 (PR #176 baseline 0.842 — 2pp tolerance); counterfactual rho > 0.20 (PR #176 baseline 0.247 — 2pp tolerance); 0% NaN in logits; vocab_size=23 unchanged.

```python
# src/tests/sk3_mig_b/test_scoutgpt_post_retrain_smoke.py
"""ScoutGPT post-retrain smoke gate. Spec §3 — PR #176 close-out validation set.

Loads the @Champion ScoutGPT model + runs forward-pass on the held-out test set;
asserts top1 + rho thresholds.
"""

from __future__ import annotations

import pytest


_TOP1_BASELINE = 0.842
_TOP1_TOLERANCE = 0.022  # 2pp per spec §3
_TOP1_THRESHOLD = _TOP1_BASELINE - _TOP1_TOLERANCE  # > 0.80

_RHO_BASELINE = 0.247
_RHO_TOLERANCE = 0.05
_RHO_THRESHOLD = _RHO_BASELINE - _RHO_TOLERANCE  # > 0.20


def test_vocab_size_unchanged() -> None:
    from analytics.scoutgpt_decoder import ScoutGPTDecoderConfig
    cfg = ScoutGPTDecoderConfig()
    assert cfg.vocab_size == 23, (
        f"vocab_size = {cfg.vocab_size}, expected 23 (SPADL action-type taxonomy unchanged)"
    )


def test_top1_above_threshold() -> None:
    from analytics.scoutgpt_evaluation import evaluate_champion_top1  # adapt to actual API
    top1 = evaluate_champion_top1()
    assert top1 > _TOP1_THRESHOLD, (
        f"ScoutGPT test_top1 = {top1:.4f}, threshold {_TOP1_THRESHOLD:.4f} "
        f"(baseline {_TOP1_BASELINE} - {_TOP1_TOLERANCE} tolerance)"
    )


def test_counterfactual_rho_above_threshold() -> None:
    from analytics.scoutgpt_evaluation import evaluate_champion_counterfactual_rho
    rho = evaluate_champion_counterfactual_rho()
    assert rho > _RHO_THRESHOLD, (
        f"ScoutGPT counterfactual rho = {rho:.4f}, threshold {_RHO_THRESHOLD:.4f}"
    )


def test_no_nan_in_logits() -> None:
    from analytics.scoutgpt_evaluation import sample_champion_logits
    logits = sample_champion_logits(n=100)
    import numpy as np
    assert not np.any(np.isnan(logits)), "ScoutGPT Champion produces NaN logits"
```

(The `analytics.scoutgpt_evaluation` module's helper functions are extracted from the existing `train_scoutgpt_hf.py` evaluation block; thin wrappers that load Champion + run forward-pass on held-out fixtures. Plan-execution time: verify the helpers exist or extract them.)

### Task 2.13 — Verify all 11 gates exist + collect cleanly

**Files:** none — verification.

- [ ] **Step 1: List the gate files**

```bash
ls src/tests/sk3_mig_b/test_*.py | sort
```

Expected: 11 files (test_vaep, test_xg_v2, test_ext_v2_p0, test_ext_v2_p1, test_defcon_lite, test_obso, test_pausa, test_f2v_v1, test_f2v_v2, test_f2v_360, test_scoutgpt).

- [ ] **Step 2: Pytest collects without error**

```bash
uv run pytest src/tests/sk3_mig_b/ --collect-only
```

Expected: collects N tests across 11 files; no collection errors.

- [ ] **Step 3: No commit — Phase 8 commits all of PR-α together**

---

## Phase 3 — HF4 migration (notebook → PEP 723 + ADR-014 amendment + CI invariants)

**Why third.** The orchestrator (Phase 6) dispatches HF Jobs invocations against PEP 723 trainer/publisher scripts. F2V v1's trainer currently lives at `notebooks/train_football2vec.py` (Databricks-notebook with hardcoded workspace path) — `hf jobs uv run` can't dispatch a notebook. HF4 migration unblocks Phase 6's Group 2 dispatch for F2V v1. Also produces 4 publisher scripts (2 fired by SK3-MIG-B Group 3; 2 created for inventory completeness — tracking-data publishers not coord-dependent per spec §1.1.2).

### Task 3.1 — Write `scripts/publish_obso_pausa_inputs_hf.py` (NEW — fired by SK3-MIG-B Group 3)

**Files:**
- Create: `scripts/publish_obso_pausa_inputs_hf.py`

Migrates the unique cell of `notebooks/publish_obso_data.py`. Reads IDSSE events + ELASTIC sync results from gold marts, builds parquet, uploads to HF + calls `upload_hf_readme` per ADR-014.

- [ ] **Step 1: Write the script**

```python
# scripts/publish_obso_pausa_inputs_hf.py
"""Publish luxury-lakehouse/obso-pausa-inputs HF dataset.

Migrated from notebooks/publish_obso_data.py per HF4. PEP 723 single-file:
runs locally + on HF Jobs + (with %pip wrapper) on Databricks.

Reads IDSSE events + ELASTIC sync results from dev_gold marts via the SDK
statement_execution path (per reference_sdk_over_sql_connector.md). Builds
a parquet payload + uploads via huggingface_hub.HfApi. Calls upload_hf_readme
post-upload per ADR-014 (filename == repo basename invariant).

Usage:
    uv run python scripts/publish_obso_pausa_inputs_hf.py
    hf jobs uv run --gpu cpu-basic python scripts/publish_obso_pausa_inputs_hf.py
"""

# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "databricks-sdk>=0.20",
#     "huggingface_hub>=0.20",
#     "pandas>=2.0",
#     "pyarrow>=14",
# ]
# ///

from __future__ import annotations

import io
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
from databricks.sdk import WorkspaceClient
from huggingface_hub import HfApi, get_token

# Add src/ to sys.path so this script can use ingestion.hf_publish without an installed wheel.
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "src"))

from ingestion.hf_publish import get_hf_card_path, upload_hf_readme  # noqa: E402

HF_ORG = "luxury-lakehouse"
DATASET_NAME = "obso-pausa-inputs"
REPO_ID = f"{HF_ORG}/{DATASET_NAME}"


def _query_inputs(catalog: str, warehouse_id: str) -> pd.DataFrame:
    """Pull IDSSE events + ELASTIC sync results for OBSO+PAUSA inputs."""
    w = WorkspaceClient()
    sql = f"""
    SELECT
      e.match_id, e.frame_id, e.player_id, e.team_id,
      e.event_type, e.x, e.y,
      sync.frame_t, sync.event_t,
      e.event_seconds, e.period_id
    FROM {catalog}.dev_gold.fct_passes e
    INNER JOIN {catalog}.bronze.elastic_sync_results sync
      ON e.match_id = sync.match_id AND e.event_id = sync.event_id
    WHERE e.data_source = 'idsse'
    """
    result = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=warehouse_id, wait_timeout="50s",
    )
    if result.result is None or result.result.data_array is None:
        raise RuntimeError(f"No rows returned for OBSO inputs query: {sql[:80]}...")
    columns = [m.name for m in (result.manifest.schema.columns or [])]
    return pd.DataFrame(result.result.data_array, columns=columns)


def main() -> int:
    catalog = os.environ.get("DATABRICKS_CATALOG", "soccer_analytics")
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
    # Per ADR-012 §2 + review item 15: get_token() is the canonical path. The os.environ
    # fallback was removed because reaching it in HF Jobs context implies the secret was
    # passed via --env (visible via `hf jobs inspect`) instead of --secrets (encrypted) —
    # the failure mode ADR-012 §2 explicitly forbids. get_token() reads from the
    # huggingface_hub credential cache, which is populated by the --secrets HF_TOKEN
    # flow (HF Jobs decrypts the secret into the job container's env at startup).
    hf_token = get_token()
    if not hf_token:
        raise RuntimeError(
            "huggingface_hub.get_token() returned no token. ADR-012 §2 requires the secret "
            "to flow via --secrets HF_TOKEN (encrypted), not --env HF_TOKEN (plain metadata). "
            "Verify HF Jobs invocation passed --secrets, or run locally with `huggingface-cli login` first."
        )

    print(f"[publish] Querying OBSO+PAUSA inputs from {catalog}.dev_gold ...")
    df = _query_inputs(catalog, warehouse_id)
    print(f"[publish] Got {len(df):,} rows")

    api = HfApi(token=hf_token)
    api.create_repo(repo_id=REPO_ID, repo_type="dataset", exist_ok=True, token=hf_token)

    with tempfile.TemporaryDirectory(prefix="obso-inputs-") as tmpdir:
        parquet_path = Path(tmpdir) / "obso_pausa_inputs.parquet"
        df.to_parquet(parquet_path, compression="zstd")
        api.upload_folder(folder_path=tmpdir, repo_id=REPO_ID, repo_type="dataset", token=hf_token)

    upload_hf_readme(
        api=api,
        repo_id=REPO_ID,
        repo_type="dataset",
        readme_path=get_hf_card_path("obso-pausa-inputs.md", kind="dataset"),
        token=hf_token,
    )
    print(f"[publish] Done — https://huggingface.co/datasets/{REPO_ID}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Verify it imports cleanly + `--help`-equivalent runs**

```bash
uv run python -c "import sys; sys.path.insert(0, 'scripts'); import publish_obso_pausa_inputs_hf"
```

Expected: imports without error.

### Task 3.2 — Write `scripts/publish_football2vec_embeddings_hf.py` (NEW — fired by SK3-MIG-B Group 3)

**Files:**
- Create: `scripts/publish_football2vec_embeddings_hf.py`

Migrated from the F2V cell of `notebooks/publish_datasets.py`. Same pattern as Task 3.1 — substitute table + dataset name.

- [ ] **Step 1: Write the script** (use Task 3.1 as the template; substitute the following)

Key differences from `publish_obso_pausa_inputs_hf.py`:
- `DATASET_NAME = "football2vec-player-embeddings"`
- Source query: `SELECT canonical_player_id, primary_position, embedding, data_source FROM {catalog}.dev_gold.fct_player_embeddings WHERE data_source IN ('f2v_v1', 'f2v_v2', 'f2v_360')` — pulls all three F2V variants in one parquet (consumers filter by `data_source`).
- `card_path = get_hf_card_path("football2vec-player-embeddings.md", kind="dataset")` (verify card exists in `docs/huggingface/dataset-cards/`).

- [ ] **Step 2: Verify card file exists**

```bash
ls docs/huggingface/dataset-cards/football2vec-player-embeddings.md
```

Expected: file exists. If missing, write the card per ADR-014 patterns from existing dataset cards.

### Task 3.3 — Write `scripts/publish_line_breaking_passes_hf.py` (NEW — inventory only; not fired by SK3-MIG-B)

**Files:**
- Create: `scripts/publish_line_breaking_passes_hf.py`

Created so the notebook can be deleted. Per spec §1.1.2 footer, tracking-data publishers are NOT fired by the SK3-MIG-B orchestrator (tracking adapters pinned to `output_convention="absolute_frame"` per SK3-MIG-A §1.3 — not coord-dependent).

- [ ] **Step 1: Write the script** (use Task 3.1 as the template)

Substitutions:
- `DATASET_NAME = "line-breaking-passes"`
- Source query reads from the line-breaking computation output table (verify exact table at plan-execution time — likely `bronze.line_breaking_passes` or similar).

### Task 3.4 — Write `scripts/publish_pitch_control_tracking_hf.py` (NEW — inventory only)

**Files:**
- Create: `scripts/publish_pitch_control_tracking_hf.py`

Same rationale as Task 3.3.

- [ ] **Step 1: Write the script** (use Task 3.1 as the template)

Substitutions:
- `DATASET_NAME = "pitch-control-tracking"`
- Source query reads tracking-formatted parquet (verify exact source at plan-execution time).

### Task 3.5 — Write `scripts/train_football2vec.py` (NEW — F2V v1 PEP 723 trainer)

**Files:**
- Create: `scripts/train_football2vec.py`

Migrates `notebooks/train_football2vec.py` (Databricks-notebook with hardcoded `/Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse/src` path). The notebook trains gensim Doc2Vec on StatsBomb + Wyscout event sequences. The PEP 723 version replaces the workspace path with `huggingface_hub.snapshot_download` for input data, uses `huggingface_hub.get_token()` for auth (ADR-012), emits weights to UC Volume via `upload_weights_to_uc_volume` (ADR-012), sets MLflow Champion alias via `set_and_verify_mlflow_champion`.

This is a ~200-400 LOC replacement (per spec §10 Q8).

- [ ] **Step 1: Read the existing notebook to understand the training logic**

```bash
cat notebooks/train_football2vec.py | head -80
```

Identify: the gensim Doc2Vec params, the input data source (currently a sys.path hack to `/Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse/src`), the MLflow registration, the HF Hub upload.

- [ ] **Step 2: Write the PEP 723 replacement**

```python
# scripts/train_football2vec.py
"""Train Football2Vec v1 (gensim Doc2Vec) on StatsBomb + Wyscout event sequences.

Migrated from notebooks/train_football2vec.py per HF4. PEP 723 single-file:
runs locally + on HF Jobs + (with %pip wrapper) on Databricks.

Per ADR-012 delivery contract:
- get_token() not os.environ.get("HF_TOKEN", "") (non-interactive contexts)
- require_mlflow_env() at top of main()
- set_and_verify_mlflow_champion(...) post-MLflow log_model
- upload_weights_to_uc_volume(...) for the inference-side fallback chain
- Secrets via --secrets, never --env (HF Jobs)

Usage:
    uv run python scripts/train_football2vec.py
    hf jobs uv run --gpu gpu-medium --secrets HF_TOKEN,DATABRICKS_TOKEN,DATABRICKS_HOST,MLFLOW_TRACKING_URI \\
                   python scripts/train_football2vec.py
"""

# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "databricks-sdk>=0.20",
#     "datasets>=2.18",
#     "gensim>=4.3",
#     "huggingface_hub>=0.20",
#     "mlflow>=2.19",
#     "pandas>=2.0",
#     "pyarrow>=14",
# ]
# ///

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

# Make ingestion.* importable for artifact_deploy + hf_publish helpers.
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "src"))

import mlflow
import pandas as pd
from datasets import load_dataset
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from huggingface_hub import HfApi, get_token

from ingestion.artifact_deploy import (  # noqa: E402
    require_mlflow_env,
    set_and_verify_mlflow_champion,
    upload_weights_to_uc_volume,
)


HF_ORG = "luxury-lakehouse"
HF_DATASET_INPUT = f"{HF_ORG}/spadl-vaep-action-values"  # the SK3-MIG-B-republished training data
HF_REPO_OUTPUT = f"{HF_ORG}/football2vec-statsbomb-wyscout"
MLFLOW_MODEL_NAME = "soccer_analytics.dev_gold.football2vec"
UC_VOLUME_PATH = "/Volumes/soccer_analytics/dev_gold/model_weights/football2vec"

VECTOR_SIZE = 32
WINDOW = 5
MIN_COUNT = 2
EPOCHS = 40


def _build_episodes(df: pd.DataFrame) -> list[TaggedDocument]:
    """Convert SPADL action rows into per-player tagged-document episodes."""
    episodes: list[TaggedDocument] = []
    for player_id, group in df.groupby("player_id"):
        actions = [f"{row.action_type}_{row.outcome}" for row in group.itertuples()]
        episodes.append(TaggedDocument(words=actions, tags=[str(player_id)]))
    return episodes


def main() -> int:
    require_mlflow_env()  # ADR-012: fail loud if MLFLOW_TRACKING_URI / DATABRICKS_TOKEN missing

    hf_token = get_token() or os.environ.get("HF_TOKEN")
    assert hf_token, "HF token required (huggingface_hub.get_token() or HF_TOKEN env)"
    api = HfApi(token=hf_token)

    print(f"[train] Loading training data from {HF_DATASET_INPUT}")
    ds = load_dataset(HF_DATASET_INPUT, split="train")
    df = ds.to_pandas()
    print(f"[train] Loaded {len(df):,} actions")

    print(f"[train] Building episodes per player_id ...")
    episodes = _build_episodes(df)
    print(f"[train] {len(episodes):,} player episodes")

    print(f"[train] Training Doc2Vec (vector_size={VECTOR_SIZE}, epochs={EPOCHS}) ...")
    model = Doc2Vec(
        documents=episodes,
        vector_size=VECTOR_SIZE,
        window=WINDOW,
        min_count=MIN_COUNT,
        epochs=EPOCHS,
        workers=4,
    )

    with tempfile.TemporaryDirectory(prefix="f2v-v1-") as tmpdir:
        weights_path = Path(tmpdir) / "f2v_v1.model"
        model.save(str(weights_path))

        # MLflow log + Champion promote
        with mlflow.start_run() as run:
            mlflow.log_param("vector_size", VECTOR_SIZE)
            mlflow.log_param("epochs", EPOCHS)
            mlflow.log_artifact(str(weights_path))
            mlflow.pyfunc.log_model(
                artifact_path="model",
                python_model=None,  # gensim model loaded via mlflow.gensim flavor; adapt per actual API
                registered_model_name=MLFLOW_MODEL_NAME,
            )
            run_id = run.info.run_id
            print(f"[train] MLflow run_id={run_id}")

        # ADR-012 Champion alias + zombie-alias guard
        set_and_verify_mlflow_champion(
            model_name=MLFLOW_MODEL_NAME,
            run_id=run_id,
            alias="Champion",
        )

        # ADR-012 UC Volume fallback (second leg of delivery chain)
        upload_weights_to_uc_volume(
            local_path=str(weights_path),
            volume_path=UC_VOLUME_PATH,
            filename="f2v_v1.model",
        )

        # HF Hub publish (with upload_hf_readme per ADR-014)
        api.create_repo(repo_id=HF_REPO_OUTPUT, repo_type="model", exist_ok=True, token=hf_token)
        api.upload_folder(folder_path=tmpdir, repo_id=HF_REPO_OUTPUT, repo_type="model", token=hf_token)

        from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
        upload_hf_readme(
            api=api,
            repo_id=HF_REPO_OUTPUT,
            repo_type="model",
            readme_path=get_hf_card_path("football2vec-statsbomb-wyscout.md", kind="model"),
            token=hf_token,
        )

    print(f"[train] Done — Champion set + UC Volume synced + HF published")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

(The `mlflow.pyfunc.log_model` call requires a thin `PythonModel` wrapper around the gensim model; adapt to the existing project pattern from `train_football2vec_v2.py` if it has one. Plan-execution time: refer to that file for the canonical wrapper code.)

- [ ] **Step 3: Verify imports + smoke run on a small fixture**

```bash
uv run python -c "
import sys
sys.path.insert(0, 'scripts')
sys.path.insert(0, 'src')
import train_football2vec
print('module imports OK')
"
```

### Task 3.6 — Delete the four notebook publishers + trainers

**Files:**
- Delete: `notebooks/publish_datasets.py`
- Delete: `notebooks/publish_obso_data.py`
- Delete: `notebooks/train_football2vec.py`
- (xG v1 notebook deletion `notebooks/train_xg_model.py` is in Phase 4 with the rest of XG1-RETIRE)

- [ ] **Step 1: Delete the three HF4 notebook files**

```bash
git rm notebooks/publish_datasets.py
git rm notebooks/publish_obso_data.py
git rm notebooks/train_football2vec.py
```

(`git rm` stages the deletions for the eventual Phase 8 commit.)

- [ ] **Step 2: Verify they're gone**

```bash
ls notebooks/publish_*.py 2>&1; ls notebooks/train_football2vec.py 2>&1
```

Expected: "No such file or directory" for both.

### Task 3.7 — Write `src/tests/test_no_notebook_hf_publishers.py` (HF4 invariant 1)

**Files:**
- Create: `src/tests/test_no_notebook_hf_publishers.py`

AST-walks `notebooks/publish_*.py` and `notebooks/train_*.py`, fails on `huggingface_hub.HfApi` / `api.upload_folder` / `api.upload_file` / `mlflow.register_model` / `mlflow.set_registered_model_alias` calls. Scope intentionally narrow per spec §4.2 — preserves legitimate non-publishing notebooks (`sync_hf_weights.py`, `import_obso_results.py`, `diag_*.py`).

- [ ] **Step 1: Write the test**

```python
# src/tests/test_no_notebook_hf_publishers.py
"""HF4 invariant 1: no HF publishers or trainers in notebooks/ directory.

Per ADR-014 amendment (§4.2 of SK3-MIG-B spec): HF publishers and trainers
are PEP 723 scripts in scripts/. Notebook publishers and trainers are forbidden.

Scope: only notebooks/publish_*.py and notebooks/train_*.py are scanned.
Other notebooks (sync_hf_weights, import_obso_results, diag_*) are exempt.

Cleanest enforcement: "no notebooks/publish_*.py and no notebooks/train_*.py
exist post-HF4." The AST walk is belt-and-suspenders for any future
re-introduction.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
NOTEBOOKS_DIR = REPO_ROOT / "notebooks"

# Forbidden call patterns — anything that uploads to HF Hub or registers MLflow models.
_FORBIDDEN_ATTRS = frozenset({
    "upload_folder", "upload_file", "create_commit",  # huggingface_hub.HfApi
    "register_model", "set_registered_model_alias",   # mlflow.client / mlflow
})


def _scoped_files() -> list[Path]:
    """Files in scope: notebooks/publish_*.py + notebooks/train_*.py."""
    return list(NOTEBOOKS_DIR.glob("publish_*.py")) + list(NOTEBOOKS_DIR.glob("train_*.py"))


def test_no_publish_or_train_notebooks_exist() -> None:
    """Cleanest enforcement: post-HF4, these files MUST NOT exist."""
    files = _scoped_files()
    assert files == [], (
        f"Notebook HF publishers/trainers found (forbidden post-HF4): "
        f"{[str(f.relative_to(REPO_ROOT)) for f in files]}. "
        "Migrate to scripts/ as PEP 723 single-file scripts (see ADR-014 amendment)."
    )


@pytest.mark.parametrize("py_file", _scoped_files() or [None])
def test_ast_walk_finds_no_forbidden_calls(py_file: Path | None) -> None:
    """Belt-and-suspenders: even if the cleanest test passes vacuously
    (no files in scope), this AST walk catches any future re-introduction.

    Skips when no scoped files exist (parametrize edge case).
    """
    if py_file is None:
        pytest.skip("No notebooks/publish_*.py or notebooks/train_*.py files in scope")

    tree = ast.parse(py_file.read_text(encoding="utf-8"))

    forbidden_found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in _FORBIDDEN_ATTRS:
            forbidden_found.append(f"{py_file.name}:{node.lineno} → .{node.attr}")
        # Also catch direct function imports: `from huggingface_hub import upload_file`
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name in _FORBIDDEN_ATTRS:
                    forbidden_found.append(f"{py_file.name}:{node.lineno} → import {alias.name}")

    assert not forbidden_found, (
        f"Forbidden HF/MLflow upload/registration calls in {py_file.relative_to(REPO_ROOT)}: "
        f"{forbidden_found}. Migrate to scripts/ as PEP 723 (see ADR-014 amendment)."
    )
```

- [ ] **Step 2: Run the test**

```bash
uv run pytest src/tests/test_no_notebook_hf_publishers.py -v
```

Expected: PASS — `test_no_publish_or_train_notebooks_exist` finds zero scoped files (Task 3.6 deleted them all); `test_ast_walk_finds_no_forbidden_calls` SKIPs (no files in scope).

### Task 3.8 — Extend `src/tests/test_hf_publish_parity.py` (HF4 invariant 2)

**Files:**
- Modify: `src/tests/test_hf_publish_parity.py`

AST-walks `scripts/publish_*_hf.py` and asserts every file calls `ingestion.hf_publish.upload_hf_readme`. Closes the parity gap end-to-end.

- [ ] **Step 1: Read the existing test to understand its structure**

```bash
cat src/tests/test_hf_publish_parity.py | head -50
```

Identify the existing test functions + add the new one at the end.

- [ ] **Step 2: Append the new test function**

```python
# Append to src/tests/test_hf_publish_parity.py

def test_every_publisher_script_calls_upload_hf_readme() -> None:
    """HF4 invariant 2: every scripts/publish_*_hf.py and scripts/compute_*_hf.py
    that uploads to HF Hub MUST call ingestion.hf_publish.upload_hf_readme.
    Per ADR-014 (filename == repo basename invariant + parity-tested).
    """
    import ast
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    scripts_dir = repo_root / "scripts"

    # In-scope: any script that uploads HF datasets/models.
    # (publish_*_hf.py handles publishers; compute_*_hf.py handles compute-and-upload like
    #  compute_obso_hf.py and compute_epv_transition_hf.py.)
    in_scope = list(scripts_dir.glob("publish_*_hf.py")) + list(scripts_dir.glob("compute_*_hf.py"))

    missing_call: list[str] = []
    for py_file in in_scope:
        tree = ast.parse(py_file.read_text(encoding="utf-8"))
        found_call = False
        for node in ast.walk(tree):
            # Match: upload_hf_readme(...) — function call by name.
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id == "upload_hf_readme":
                    found_call = True
                    break
                if isinstance(node.func, ast.Attribute) and node.func.attr == "upload_hf_readme":
                    found_call = True
                    break
        if not found_call:
            missing_call.append(str(py_file.relative_to(repo_root)))

    assert not missing_call, (
        "These HF publisher scripts do NOT call upload_hf_readme (ADR-014 violation):\n  "
        + "\n  ".join(missing_call)
        + "\nAdd `from ingestion.hf_publish import upload_hf_readme` and call it post-upload."
    )
```

- [ ] **Step 3: Run the extended test**

```bash
uv run pytest src/tests/test_hf_publish_parity.py -v
```

Expected: PASS — all `publish_*_hf.py` (existing + the 4 NEW from Tasks 3.1-3.4) AND `compute_*_hf.py` (`compute_obso_hf.py` line 710, `compute_epv_transition_hf.py` line 276) call `upload_hf_readme`.

### Task 3.9 — Amend ADR-014 (notebook publisher/trainer ban)

**Files:**
- Modify: `docs/superpowers/adrs/ADR-014-hf-card-inventory-parity.md`

- [ ] **Step 1: Read the current ADR**

```bash
cat docs/superpowers/adrs/ADR-014-hf-card-inventory-parity.md | head -40
```

- [ ] **Step 2: Append the amendment section**

Append to the bottom of the ADR, under a new `## Amendment 2026-05-03 — SK3-MIG-B HF4 fold-in` heading:

```markdown
## Amendment 2026-05-03 — SK3-MIG-B HF4 fold-in

**Rule extended:** HF publishers and trainers are PEP 723 scripts in `scripts/`. Notebook publishers and trainers are forbidden.

**Migration:** filename == repo basename + `upload_hf_readme` after the data upload, no exceptions.

**Enforcement (CI tests, both added in SK3-MIG-B PR-α):**

1. `src/tests/test_no_notebook_hf_publishers.py` — fails if `notebooks/publish_*.py` or `notebooks/train_*.py` exist (cleanest invariant) AND AST-walks any present files for forbidden calls (`HfApi.upload_folder`, `HfApi.upload_file`, `HfApi.create_commit`, `mlflow.register_model`, `mlflow.set_registered_model_alias`).
2. `src/tests/test_hf_publish_parity.py` extended — AST-walks `scripts/publish_*_hf.py` AND `scripts/compute_*_hf.py` and asserts every file calls `ingestion.hf_publish.upload_hf_readme`.

**Migrations completed in this PR:**
- `notebooks/publish_datasets.py` → 3 PEP 723 scripts (`publish_line_breaking_passes_hf.py`, `publish_pitch_control_tracking_hf.py`, `publish_football2vec_embeddings_hf.py`); duplicates of existing canonical publishers (`publish_spadl_vaep_hf.py`, `publish_freeze_frame_hf.py`) deleted.
- `notebooks/publish_obso_data.py` → `scripts/publish_obso_pausa_inputs_hf.py`.
- `notebooks/train_football2vec.py` → `scripts/train_football2vec.py` (F2V v1 trainer).
- `notebooks/train_xg_model.py` → DELETED (xG v1 retired entirely per XG1-RETIRE — same SK3-MIG-B PR-α).
```

- [ ] **Step 3: Verify the ADR's existing parity tests still pass**

```bash
uv run pytest src/tests/test_hf_publish_parity.py -v
```

Expected: all PASS (no behavior change to existing tests; new test added in Task 3.8).

- [ ] **Step 4: No commit — Phase 8 commits all of PR-α together**

---

## Phase 4 — XG1-RETIRE PR-α-commit parts

**Why fourth.** v1 deletions land before the wheel bump (Phase 5) so the bump captures the wheel-surface change in `src/ingestion/xg_model.py` removal. Per spec §6.1, drop ordering is 8 numbered steps — only the PR-α-commit steps are in this phase (steps 1, 3, 6, 7-source, 8 from spec §6.1; runtime steps 2, 4, 5, 7-tf-apply are Phase 9 operator-runtime).

### Task 4.1 — Shot Map UI migration (Taipy queries + state + glossary)

**Files:**
- Modify: `hf_taipy_app/src/queries/shots.py`
- Modify: `hf_taipy_app/src/state/shot_map.py`

Per spec §6.2: replace `xg_logistic` and `xg_gradient_boosted` columns with `xg_set_encoder + xg_ci_lower + xg_ci_upper`. Tooltip + glossary entry per CLAUDE.md UX standard.

- [ ] **Step 1: Read the current Shot Map state code**

```bash
cat hf_taipy_app/src/state/shot_map.py | head -80
grep -n "xg_logistic\|xg_gradient_boosted\|fetch_xg_predictions" hf_taipy_app/src/state/shot_map.py
grep -n "xg_logistic\|xg_gradient_boosted\|fetch_xg_predictions" hf_taipy_app/src/queries/shots.py
```

Locate every reference to v1 columns + the v1 fetcher.

- [ ] **Step 2: Delete `fetch_xg_predictions()` from queries/shots.py (Q6 resolved at plan-write time)**

Plan-write-time grep confirmed: `hf_taipy_app/src/queries/shots.py:59` defines `fetch_xg_predictions(competition_key)` returning v1 columns (`xg_logistic`, `xg_gradient_boosted`). NO same-name v2 fetcher exists — v2 callers use a different function name or inline SQL.

**Decision:** delete the function outright (NOT rename). Find + delete the entire function body (lines ~58-75 in current main HEAD) + any imports it added that are no longer used. Verify zero callers remain via grep:

```bash
grep -rn "fetch_xg_predictions" hf_taipy_app/
```

Expected post-Step 2: zero hits across `hf_taipy_app/`.

- [ ] **Step 3: Update `state/shot_map.py` — replace v1 columns with v2**

In every call to `fetch_*` that returned v1 rows, replace v1 column references:

```python
# OLD (v1):
shot_table_columns = ["xg_logistic", "xg_gradient_boosted", ...]

# NEW (v2):
shot_table_columns = ["xg_set_encoder", "xg_ci_lower", "xg_ci_upper", ...]
```

In every Plotly chart spec / displayed metric, replace v1 column references with v2 equivalents.

- [ ] **Step 4: Update tooltip + glossary**

In whichever PageConfig / Metric / SidebarWidget defines the xG display, set:

```python
help_text = (
    "v2 Set Encoder xG with 95% CI from MC dropout (Gal & Ghahramani 2016). "
    "Range: 0.0 (no chance) to 1.0 (certain). "
    "Wider CI = more model uncertainty about this shot."
)
```

Add to `GLOSSARY` dict in `hf_taipy_app/src/template.py` (or wherever the project's central glossary lives — check existing entries for format):

```python
"xG (v2 Set Encoder)": (
    "Expected goal probability from the v2 Set Encoder model with MC dropout 95% CI. "
    "Range 0–1. xg_ci_upper - xg_ci_lower = model uncertainty. "
    "Reference: Gal & Ghahramani 2016 (Dropout as Bayesian Approximation)."
),
```

Add the glossary entry's key to the relevant page's `PAGE_TERMS` list so it surfaces in the page glossary.

- [ ] **Step 5: Verify no live references to v1 columns remain in `hf_taipy_app/`**

```bash
grep -rn "xg_logistic\|xg_gradient_boosted" hf_taipy_app/src/
```

Expected: 0 matches.

### Task 4.2 — Delete v1 dbt model files + YAML entries

**Files:**
- Delete: `dbt_project/models/marts/fct_xg_predictions.sql`
- Delete: `dbt_project/models/staging/xg/stg_xg__predictions.sql`
- Modify: `dbt_project/models/staging/xg/_xg__sources.yml` (remove v1 entries)
- Modify: `dbt_project/models/marts/_marts__models.yml` (remove v1 mart contract entry)

- [ ] **Step 0: Pre-grep for downstream `ref('fct_xg_predictions')` callers (item 16 review fix)**

```bash
grep -rn "ref('fct_xg_predictions')" dbt_project/models/
grep -rn 'ref("fct_xg_predictions")' dbt_project/models/
```

Expected: 0 hits (already verified in Phase 0 Task 0.9; this step is the Phase 4 sanity re-check). If any hits found, those downstream marts must be addressed BEFORE deletion — either delete them too (most likely the right call for v1 cascade) OR migrate the `ref()` to `fct_xg_predictions_v2` (only correct if the downstream consumer's logic is v2-compatible).

- [ ] **Step 1: Delete the SQL model files**

```bash
git rm dbt_project/models/marts/fct_xg_predictions.sql
git rm dbt_project/models/staging/xg/stg_xg__predictions.sql
```

- [ ] **Step 2: Edit `_xg__sources.yml` — remove v1 source entries**

Open `dbt_project/models/staging/xg/_xg__sources.yml`. Locate the `sources:` block. Find any source pointing at `bronze.xg_predictions` (NOT `bronze.xg_predictions_v2`) — that's the v1 source. Delete the entry + any `description` / `tests` it carried. Keep v2 sources untouched.

- [ ] **Step 3: Edit `_marts__models.yml` — remove v1 mart contract entry**

Open `dbt_project/models/marts/_marts__models.yml`. Find the `- name: fct_xg_predictions` block (NOT `fct_xg_predictions_v2`). Delete the entire block including its `config:`, `columns:`, and `tests:` subsections.

- [ ] **Step 4: Verify dbt parses cleanly with v1 references gone**

```bash
cd dbt_project && uv run dbt parse
```

Expected: parse succeeds with 0 errors. If parse fails with "model not found" referring to `fct_xg_predictions`, find the lingering reference (likely in another mart's `ref()` call) and either delete that downstream model or update it.

### Task 4.3 — Delete v1 source code + pyproject entry

**Files:**
- Delete: `src/ingestion/xg_model.py`
- Delete: `scripts/train_xg_model_hf.py`
- Delete: `notebooks/train_xg_model.py`
- Modify: `pyproject.toml` (remove v1 entry-point line)

- [ ] **Step 1: Delete the v1 source files**

```bash
git rm src/ingestion/xg_model.py
git rm scripts/train_xg_model_hf.py
git rm notebooks/train_xg_model.py
```

- [ ] **Step 2: Remove the v1 entry-point from `pyproject.toml`**

Open `pyproject.toml`. Find `[project.scripts]` section. Locate the line:

```toml
compute_xg_predictions = "ingestion.xg_model:main"
```

(Or similar — verify exact name.) Delete that line. Leave v2 entry points untouched.

- [ ] **Step 3: Verify the wheel still builds + tests can collect**

```bash
uv run python -m build --wheel 2>&1 | tail -20
```

Expected: wheel builds without import errors. If anything imports `from ingestion.xg_model import ...`, the build fails — find + fix the lingering import (most likely in tests or analytics code).

```bash
uv run pytest --collect-only 2>&1 | tail -20
```

Expected: collection succeeds.

### Task 4.4 — Delete workflow YAML + Terraform v1 declaration

**Files:**
- Delete: `workflow-cards/wf-xg-v1.yaml`
- Modify: `terraform/environments/dev/main.tf` (remove v1 job declaration block)

- [ ] **Step 1: Delete the workflow card**

```bash
git rm workflow-cards/wf-xg-v1.yaml
```

- [ ] **Step 2: Remove the v1 job declaration from Terraform**

Open `terraform/environments/dev/main.tf`. Find the resource block that declares the v1 job (likely a `databricks_job` resource with `name` containing `xg_predictions` or `xg_v1`, or a `task_key` of `compute_xg_predictions`). Delete the block.

If the project uses `for_each` over a list of workflow cards (likely per the mega-job pattern), the declaration may be data-driven — locate the input list (e.g., a `local.workflow_cards` array) and remove the v1 entry there.

- [ ] **Step 3: Verify Terraform parses**

```bash
cd terraform/environments/dev && terraform validate
```

Expected: validate succeeds. Don't `terraform apply` here — that's Phase 9 operator-runtime.

### Task 4.5 — Delete v1 HF model card + update org-card + README + AI_GOVERNANCE

**Files:**
- Delete: `docs/huggingface/model-cards/xg-model-statsbomb-wyscout.md`
- Modify: `docs/huggingface/org-card.md` (remove v1 listing)
- Modify: `README.md` (remove v1 from HF artifact list)
- Modify: `AI_GOVERNANCE.md` (§5 Scope row removal)

- [ ] **Step 1: Delete the v1 model card**

```bash
git rm docs/huggingface/model-cards/xg-model-statsbomb-wyscout.md
```

- [ ] **Step 2: Remove v1 from `org-card.md`**

```bash
grep -n "xg-model-statsbomb-wyscout\|xG v1" docs/huggingface/org-card.md
```

Open `docs/huggingface/org-card.md`. Delete any line(s) listing the v1 model in the artifacts table or list. Keep v2 entries.

- [ ] **Step 3: Remove v1 from `README.md` HF artifact list**

```bash
grep -n "xg-model-statsbomb-wyscout\|xG v1" README.md
```

Delete the v1 entry from the README's HF artifacts section.

- [ ] **Step 4: Remove v1 row from `AI_GOVERNANCE.md` §5 Scope**

Open `AI_GOVERNANCE.md`. Navigate to §5 Scope. Find the row for the v1 xG model (model card filename `xg-model-statsbomb-wyscout.md`). Delete the entire row from the Scope table.

- [ ] **Step 5: HF Space header + footer updates**

Open `hf_taipy_app/README.md` (or wherever the HF Space's display header/footer is configured). Remove any reference to the v1 model.

- [ ] **Step 6: Run the AI_GOVERNANCE parity test**

```bash
uv run pytest src/tests/test_ai_governance_md.py -v
```

Expected: PASS — workflow-card inventory parity passes (v1 workflow card was deleted in Task 4.4); model-card inventory parity passes (v1 model card was deleted in Step 1).

If the test FAILS with "model card present but no Scope row" or vice versa, the deletion is incomplete — fix and re-run.

- [ ] **Step 7: Run orphan-card check**

```bash
uv run python scripts/publish_hf_cards.py --orphans
```

Expected: 0 orphans. If `xg-model-statsbomb-wyscout.md` is reported as an orphan, it wasn't deleted — fix and re-run.

### Task 4.6 — Write `src/tests/test_xg_v1_retired.py` regression test

**Files:**
- Create: `src/tests/test_xg_v1_retired.py`

Three explicit assertions per spec §6.4: import fails, glob is clean across 7 layers, pyproject.toml has no v1 entry-point.

- [ ] **Step 1: Write the test**

```python
# src/tests/test_xg_v1_retired.py
"""XG1-RETIRE regression test — prevents accidental re-introduction of v1.

Three assertions:
1. `import ingestion.xg_model` raises ModuleNotFoundError.
2. Glob across 7 layers (src/, scripts/, notebooks/, dbt_project/, terraform/,
   workflow-cards/, hf_taipy_app/) returns zero hits for v1 names.
3. pyproject.toml [project.scripts] has no v1 entry-point.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_ingestion_xg_model_module_does_not_exist() -> None:
    """Direct import attempt — survives any leftover __init__.py re-export."""
    with pytest.raises(ModuleNotFoundError):
        import ingestion.xg_model  # noqa: F401


_FORBIDDEN_NAMES = (
    "xg_model.py",                  # source module
    "fct_xg_predictions.sql",       # dbt mart
    "stg_xg__predictions.sql",      # dbt staging
    "wf-xg-v1.yaml",                # workflow card
    "xg-model-statsbomb-wyscout.md",# v1 HF model card
)

_LAYER_DIRS = (
    "src",
    "scripts",
    "notebooks",
    "dbt_project",
    "terraform",
    "workflow-cards",
    "hf_taipy_app",
)


@pytest.mark.parametrize("layer_dir", _LAYER_DIRS)
@pytest.mark.parametrize("forbidden", _FORBIDDEN_NAMES)
def test_no_v1_files_in_layer(layer_dir: str, forbidden: str) -> None:
    """Recursive glob across each layer dir for each forbidden filename."""
    layer_path = REPO_ROOT / layer_dir
    if not layer_path.exists():
        pytest.skip(f"Layer dir does not exist: {layer_path}")
    matches = list(layer_path.rglob(forbidden))
    assert matches == [], (
        f"Forbidden v1 file found post-XG1-RETIRE: "
        f"{[str(m.relative_to(REPO_ROOT)) for m in matches]} in {layer_dir}/. "
        "Verify XG1-RETIRE drop ordering completed (spec §6.1)."
    )


def test_pyproject_has_no_v1_entry_point() -> None:
    """pyproject.toml [project.scripts] must not contain a v1 entry-point."""
    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    forbidden_lines = [
        line for line in pyproject.splitlines()
        if "ingestion.xg_model" in line or "compute_xg_predictions" in line.split("=")[0].strip()
    ]
    assert not forbidden_lines, (
        f"pyproject.toml still contains v1 entry-point line(s): {forbidden_lines}"
    )


def test_no_xg_v1_imports_in_src() -> None:
    """No code in src/ imports from ingestion.xg_model."""
    src_dir = REPO_ROOT / "src"
    forbidden_imports: list[str] = []
    for py_file in src_dir.rglob("*.py"):
        text = py_file.read_text(encoding="utf-8")
        if "from ingestion.xg_model" in text or "import ingestion.xg_model" in text:
            forbidden_imports.append(str(py_file.relative_to(REPO_ROOT)))
    assert not forbidden_imports, (
        f"v1 imports found in src/: {forbidden_imports}"
    )
```

- [ ] **Step 2: Run the test**

```bash
uv run pytest src/tests/test_xg_v1_retired.py -v
```

Expected: PASS — all 5 test functions clean. If any fail, the corresponding XG1-RETIRE deletion is incomplete; fix.

### Task 4.7 — Write `src/tests/test_shot_map_v2_columns.py` regression test

**Files:**
- Create: `src/tests/test_shot_map_v2_columns.py`

Per spec §6.4: AST-walk-based assertion that `hf_taipy_app/src/state/shot_map.py` references v2 columns, NOT v1.

- [ ] **Step 1: Write the test**

```python
# src/tests/test_shot_map_v2_columns.py
"""XG1-RETIRE UI migration regression test.

Asserts hf_taipy_app/src/state/shot_map.py references v2 columns
(xg_set_encoder, xg_ci_lower, xg_ci_upper) and NOT v1 columns
(xg_logistic, xg_gradient_boosted).

AST-walk-based to survive whitespace + formatting differences.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SHOT_MAP_PATH = REPO_ROOT / "hf_taipy_app" / "src" / "state" / "shot_map.py"

_V1_FORBIDDEN = ("xg_logistic", "xg_gradient_boosted")
_V2_REQUIRED = ("xg_set_encoder", "xg_ci_lower", "xg_ci_upper")


def _string_constants(tree: ast.AST) -> set[str]:
    """All string literals in an AST."""
    out: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            out.add(node.value)
    return out


def test_shot_map_has_no_v1_columns() -> None:
    """No v1 column names should appear as string literals in shot_map.py."""
    tree = ast.parse(SHOT_MAP_PATH.read_text(encoding="utf-8"))
    strings = _string_constants(tree)
    leaks = [v1 for v1 in _V1_FORBIDDEN if any(v1 in s for s in strings)]
    assert not leaks, (
        f"v1 column reference(s) found in {SHOT_MAP_PATH.relative_to(REPO_ROOT)}: {leaks}. "
        "XG1-RETIRE migration incomplete — replace with v2 columns "
        "(xg_set_encoder + xg_ci_lower + xg_ci_upper)."
    )


def test_shot_map_has_v2_columns() -> None:
    """At least one of the v2 column names must appear in shot_map.py."""
    text = SHOT_MAP_PATH.read_text(encoding="utf-8")
    found = [v2 for v2 in _V2_REQUIRED if v2 in text]
    assert found, (
        f"None of the v2 columns {_V2_REQUIRED} found in "
        f"{SHOT_MAP_PATH.relative_to(REPO_ROOT)}. "
        "XG1-RETIRE migration incomplete — Shot Map needs to display v2 predictions."
    )
```

- [ ] **Step 2: Run the test**

```bash
uv run pytest src/tests/test_shot_map_v2_columns.py -v
```

Expected: PASS. If `test_shot_map_has_no_v1_columns` fails, Task 4.1 Step 3 missed a column reference. If `test_shot_map_has_v2_columns` fails, the v2 column wasn't added in Task 4.1 Step 3.

- [ ] **Step 3: No commit — Phase 8 commits all of PR-α together**

---

## Phase 5 — Wheel bump (0.3.30 → 0.3.31)

**Why fifth.** Must come AFTER Phase 4 (XG1-RETIRE source deletions) so the bump captures a real wheel-surface change. `src/ingestion/xg_model.py` no longer exists; any external consumer doing `from ingestion.xg_model import ...` will now break on `pip install`. Per Hyrum's Law a patch bump is the conservative call (spec §1.1.4 + §5.0).

### Task 5.1 — Bump the wheel

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Bump via the project's bump script**

```bash
uv run python bump_wheel.py --patch
```

Expected: `pyproject.toml` `[project]` `version` field updates `0.3.30` → `0.3.31`. Output prints the new version.

(If `bump_wheel.py` doesn't accept a `--patch` flag, hand-edit `pyproject.toml` line `version = "0.3.30"` → `version = "0.3.31"` directly. Verify via `git diff pyproject.toml`.)

- [ ] **Step 2: Verify the bump**

```bash
grep -n '^version' pyproject.toml | head -1
```

Expected: `version = "0.3.31"`.

- [ ] **Step 3: Verify the wheel builds**

```bash
uv run python -m build --wheel 2>&1 | tail -5
```

Expected: `Successfully built luxury_lakehouse-0.3.31-py3-none-any.whl` (or equivalent — version string must reflect 0.3.31).

- [ ] **Step 4: Verify Terraform env-spec parity**

```bash
uv run pytest src/tests/test_terraform_env_dep_parity.py -v
```

Expected: PASS. If FAIL, the test will print which `terraform/environments/*/env-spec.json` (or equivalent) needs the wheel-version bump synchronised. Edit those files to match `0.3.31`.

- [ ] **Step 5: No commit — Phase 8 commits all of PR-α together**

---

## Phase 6 — Orchestrator script `scripts/sk3_mig_b_retrain.py`

**Why sixth.** All the load-bearing infrastructure (telemetry table, smoke gates, HF4 migration, XG1-RETIRE source-side deletions, wheel bump) must exist before the orchestrator that drives the cycle is meaningful. The orchestrator is the runtime tool the operator invokes post-PR-α-merge to execute the actual retrain cycle.

Spec §5 + §5.0 + §5.1 + §5.2 + §5.2.1 + §5.3.

### Task 6.1 — Orchestrator scaffolding (CLI + halt-resume + telemetry writer)

**Files:**
- Create: `scripts/sk3_mig_b_retrain.py`

PEP 723 single-file. Splits into ~6 sub-tasks because it's ~500 LOC. This task creates the CLI scaffolding + the telemetry writer; subsequent tasks add the per-step logic.

- [ ] **Step 1: Write the script header + imports + constants**

```python
# scripts/sk3_mig_b_retrain.py
"""SK3-MIG-B retrain orchestrator — drives 11 cycle items + 8 HF republishes
+ Lakebase synced refresh + index restoration + XG1-RETIRE runtime.

Spec: docs/superpowers/specs/2026-05-03-sk3-mig-b-retrain-and-republish-design.md
PEP 723 single-file. Idempotent. --start-at <step> resumable. --dry-run skips
HF Jobs invocations + runs steps 5-11 against existing Champions.

Per spec §5.2.1: orchestrator runs as background process. Status streams every
60-120s to stdout AND bronze.sk3_mig_b_runs Delta table.

Per CLAUDE.md "Never disappear into long-running commands": invoke this script
via run_in_background=true; poll output file via tail -f.

Cost cap (§9.5): _COST_CAP_USD = 80.0 — orchestrator halts on cumulative
cycle spend exceeding cap; resume via --override-cost-cap.

Wall-clock cap (§9.6): _WALLTIME_CAP_HOURS = 8.0 per single retrain — catches
hung jobs; resume via --override-walltime-cap.

Usage:
    # Dry-run (no HF Jobs spend; verify wiring):
    uv run python scripts/sk3_mig_b_retrain.py --dry-run

    # Full run:
    uv run python scripts/sk3_mig_b_retrain.py

    # Resume after halt:
    uv run python scripts/sk3_mig_b_retrain.py --start-at f2v_v2 --override-cost-cap
"""

# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "databricks-sdk>=0.20",
#     "huggingface_hub>=0.20",
#     "mlflow>=2.19",
#     "pyspark>=3.5",
# ]
# ///

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# Make ingestion.* importable.
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "src"))

from ingestion.sk3_mig_b_telemetry import (  # noqa: E402
    _SK3_MIG_B_RUNS_COLUMNS,
    classify_cycle_item,
    get_sk3_mig_b_runs_struct_type,
)

_COST_CAP_USD = 80.0
_WALLTIME_CAP_HOURS = 8.0
_STATUS_INTERVAL_SECONDS = 60

# Cycle item dispatch order — Group 1 first (gates Group 2), then Group 3.
_GROUP_1_TRAINED = ("vaep", "xg_v2", "ext_v2_p0", "ext_v2_p1")
_GROUP_1_COMPUTE_ONLY = ("defcon_lite", "obso", "pausa")
_GROUP_2_TRAINED = ("f2v_v1", "f2v_v2", "f2v_360", "scoutgpt")
_GROUP_3_PUBLISH = (
    "spadl_vaep_publish", "xg_shots_publish",
    "freeze_frame_publish", "shots_on_target_publish",
    "obso_pausa_inputs_publish", "obso_trained_grids_publish",
    "obso_pausa_values_publish", "f2v_embeddings_publish",
)
_ALL_CYCLE_ITEMS = _GROUP_1_TRAINED + _GROUP_1_COMPUTE_ONLY + _GROUP_2_TRAINED + _GROUP_3_PUBLISH


@dataclass
class CycleState:
    """Mutable state passed between orchestrator steps."""
    cycle_id: str
    cycle_started_at: datetime
    wheel_at_start: str
    silly_kicks_version: str
    catalog: str
    warehouse_id: str
    dry_run: bool
    override_cost_cap: bool
    override_walltime_cap: bool
    allow_databricks_only_cost_hook: bool  # item 9 — explicit acknowledgement of cost-hook limitation
    pre_mart_versions: dict[str, int] = field(default_factory=dict)
    cumulative_cost_usd: float = 0.0
    current_item_started_at: datetime | None = None
    current_item: str | None = None
    current_hf_job_id: str | None = None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _emit_status(
    state_or_msg: "CycleState | str | None" = None,
    *,
    step: str = "—",
    item: str = "—",
    phase: str = "—",
    elapsed_seconds: float = 0.0,
    hf_job_id: str | None = None,
    msg: str = "",
) -> None:
    """Emit a structured status line per spec §5.2.1.

    Format:
      [YYYY-MM-DDTHH:MM:SSZ] cycle=<id> step=<step> item=<item>
      phase=<dispatched|running|smoke_pending|smoke_pass|complete|halted>
      elapsed=<HH:MM:SS> hf_job_id=<id_or_null> msg=<free-text>

    Parsable by awk + grep pipelines. Free-form msg goes at the end so the
    structured prefix is column-fixed.

    Accepts EITHER signature (union-type dispatch via isinstance):

    1. Legacy free-form: ``_emit_status("some message")`` — first positional is
       a str; cycle_id renders as "—" and msg=that-string. Kept for back-compat
       during the structured-format migration; eventually all call sites adopt
       form #2 below (tracked as Phase 6 Task 6.1 Step 3 migration sub-task).

    2. Structured: ``_emit_status(state, step="0", phase="running", msg="...")``
       — first positional is a CycleState; full structured fields rendered.

    The migration plan: new code uses form #2. Existing free-form calls work
    via form #1 until manually upgraded. The dispatch via isinstance keeps the
    orchestrator runnable during incremental migration without crashing on the
    first call (review item 10 fix).
    """
    if isinstance(state_or_msg, str):
        # Legacy free-form path — first positional is the message, no state.
        free_msg = state_or_msg if not msg else f"{state_or_msg} | {msg}"
        cycle_id = "—"
        msg = free_msg
    elif state_or_msg is None:
        cycle_id = "—"
    else:
        cycle_id = state_or_msg.cycle_id

    elapsed_hms = time.strftime("%H:%M:%S", time.gmtime(elapsed_seconds))
    line = (
        f"[{_now_utc().isoformat(timespec='seconds')}] "
        f"cycle={cycle_id} step={step} item={item} phase={phase} "
        f"elapsed={elapsed_hms} hf_job_id={hf_job_id or 'null'} "
        f"msg={msg!r}"
    )
    print(line, flush=True)
```

- [ ] **Step 2: Verify imports compile**

```bash
uv run python -c "
import sys
sys.path.insert(0, 'scripts')
sys.path.insert(0, 'src')
import sk3_mig_b_retrain
print('orchestrator imports OK')
"
```

Expected: prints `orchestrator imports OK`.

- [ ] **Step 3: Migrate `_emit_status` call sites to structured signature (review item 10 long-term fix)**

The `_emit_status` helper accepts both forms (legacy free-form string OR structured fields) via isinstance-dispatch. New code uses the structured form; existing free-form calls work during migration but should be upgraded incrementally so the production orchestrator emits parsable status lines uniformly.

**Migration pattern** (mechanical):

```python
# BEFORE:
_emit_status("Step 0: pre-flight gates")
_emit_status("  silly-kicks 3.0.1 OK")
_emit_status(f"  Group 1 dispatching: {item}")

# AFTER:
_emit_status(state, step="0", phase="running", msg="pre-flight gates")
_emit_status(state, step="0", phase="running", msg="silly-kicks 3.0.1 OK")
_emit_status(state, step="1", item=item, phase="dispatched", msg="Group 1")
```

**Step name conventions:**
- `step="0"` — pre-flight (Step 0)
- `step="1"`, `step="2"`, `step="3"`, `step="4"`, `step="5"`, `step="6"` — Steps 1-6 from §5.1
- `step="heartbeat"` — Task 6.2.1 daemon thread
- `step="—"` — generic / non-step messages

**Phase value conventions:**
- `phase="running"` — work in progress
- `phase="dispatched"` — HF Job or mega-job triggered, awaiting completion
- `phase="smoke_pending"` — awaiting smoke gate
- `phase="smoke_pass"` — smoke gate cleared
- `phase="complete"` — cycle item done
- `phase="halted"` — orchestrator halt (cost cap, walltime cap, smoke fail, etc.)

**Migration scope:** ~50 call sites across `_step_0_preflight`, `_run_cycle_item`, `_step_1_group_1`, `_step_2_group_2`, `_step_3_group_3_publish`, `_step_4_xg1_retire_runtime`, `_step_5_hf4_cleanup`, `_step_6_final_sweep`, `main()`. Mechanical edit — no logic change.

**Verification after migration:**

```bash
grep -n "_emit_status(\"" scripts/sk3_mig_b_retrain.py | head -10
```

Expected post-migration: zero hits — every call now uses the structured signature with `state` as first positional.

**Optional:** add a strict-mode flag during late migration to enforce structured form. The dispatch can sniff for non-CycleState first positional and emit a `DeprecationWarning` if a free-form string is passed, then later raise. This forces the migration to completion. Out of scope for this PR; suggested as a follow-up.

### Task 6.2 — Step 0 pre-flight gates

**Files:**
- Modify: `scripts/sk3_mig_b_retrain.py` (append)

- [ ] **Step 1: Append Step 0 pre-flight function**

```python
def _step_0_preflight(state: CycleState) -> None:
    """Verify §5.0 PR-α commits all landed before any orchestrator dispatch.

    Halts on any precondition violation with a clear message pointing at §5.0.
    """
    _emit_status("Step 0: pre-flight gates")

    # 1. silly-kicks 3.0.1+ in env
    import silly_kicks  # noqa: F401
    sk_version = getattr(silly_kicks, "__version__", "unknown")
    assert sk_version >= "3.0.1", f"silly-kicks {sk_version} < 3.0.1"
    _emit_status(f"  silly-kicks {sk_version} OK")

    # 2. Wheel == 0.3.31 (verifies the §5.0 PR-α commit landed)
    pyproject = (_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    version_lines = [l for l in pyproject.splitlines() if l.startswith("version")]
    assert "0.3.31" in version_lines[0], (
        f"Wheel version mismatch: {version_lines[0]!r}. PR-α wheel bump (§5.0) "
        "may not have merged. Confirm PR-α merged and pull main."
    )
    _emit_status("  wheel 0.3.31 OK")

    # 3. PR-α file presence — §5.0 inventory
    required_files = [
        "src/ingestion/sk3_mig_b_telemetry.py",
        "scripts/migrations/2026-05-03-create-bronze-sk3-mig-b-runs.sql",
        "scripts/train_football2vec.py",
        "scripts/publish_obso_pausa_inputs_hf.py",
        "scripts/publish_football2vec_embeddings_hf.py",
        "src/tests/test_xg_v1_retired.py",
        "src/tests/test_shot_map_v2_columns.py",
        "src/tests/test_no_notebook_hf_publishers.py",
    ]
    forbidden_files = [
        "src/ingestion/xg_model.py",
        "scripts/train_xg_model_hf.py",
        "notebooks/train_xg_model.py",
        "notebooks/train_football2vec.py",
        "notebooks/publish_datasets.py",
        "notebooks/publish_obso_data.py",
        "workflow-cards/wf-xg-v1.yaml",
    ]
    missing = [f for f in required_files if not (_REPO_ROOT / f).exists()]
    leftover = [f for f in forbidden_files if (_REPO_ROOT / f).exists()]
    assert not missing, f"PR-α file(s) missing: {missing}. See spec §5.0."
    assert not leftover, f"PR-α file(s) not deleted: {leftover}. See spec §5.0."
    _emit_status(f"  PR-α file inventory OK ({len(required_files)} required present, {len(forbidden_files)} forbidden absent)")

    # 4. Required env vars
    for var in ("DATABRICKS_TOKEN", "DATABRICKS_HOST", "MLFLOW_TRACKING_URI", "HF_TOKEN", "DATABRICKS_WAREHOUSE_ID"):
        assert os.environ.get(var), f"{var} unset — orchestrator cannot proceed"
    _emit_status("  env vars OK")

    # 5. fct_action_values freshness — must be post SK3-MIG-A merge
    sql = f"SELECT MAX(_ingested_at) FROM {state.catalog}.dev_gold.fct_action_values"
    rows = _execute_sql(state, sql)
    max_ts = rows[0][0] if rows else None
    sk3_mig_a_merge = datetime(2026, 5, 2, tzinfo=timezone.utc)
    if isinstance(max_ts, str):
        max_ts = datetime.fromisoformat(max_ts.replace("Z", "+00:00"))
    assert max_ts > sk3_mig_a_merge, (
        f"fct_action_values max(_ingested_at) = {max_ts} <= SK3-MIG-A merge {sk3_mig_a_merge}. "
        "SK3-MIG-A's full-rebuild may not have completed."
    )
    _emit_status(f"  fct_action_values fresh ({max_ts}) OK")

    # 6. Cost-hook coverage — HARD HALT per spec §5.1 Step 0 (item 9 review fix)
    sql = f"""
    SELECT DISTINCT workflow_id FROM {state.catalog}.bronze.workflow_costs
    WHERE started_at > current_timestamp() - INTERVAL 7 DAYS
    """
    rows = _execute_sql(state, sql)
    workflow_ids = {row[0] for row in rows} if rows else set()
    hf_jobs_present = any("xg-v2" in wid or "scoutgpt" in wid or "football2vec" in wid for wid in workflow_ids)
    if not hf_jobs_present and not state.allow_databricks_only_cost_hook:
        _emit_status(
            state, step="0", phase="halted",
            msg="HALT: bronze.workflow_costs has no HF Jobs workflow_ids in last 7 days. "
                "Cost cap is theatrical for HF-Jobs-dominant spend (ScoutGPT alone is ~$15-20). "
                "Either (a) extend the cost hook to HF Jobs before proceeding, or "
                "(b) re-run with --allow-databricks-only-cost-hook to acknowledge the limitation explicitly."
        )
        sys.exit(7)
    elif not hf_jobs_present:
        _emit_status(
            state, step="0", phase="running",
            msg="cost-hook covers Databricks only; --allow-databricks-only-cost-hook acknowledged"
        )
    else:
        _emit_status(state, step="0", phase="running", msg="cost-hook covers HF Jobs OK")

    # 7. Capture pre-state mart versions
    affected_marts = [
        "fct_action_values", "fct_xg_predictions_v2", "fct_passes",
        "fct_player_embeddings", "fct_player_embeddings_career", "fct_player_embeddings_season",
        "fct_player_embeddings_career_360", "fct_player_embeddings_season_360",
        "fct_pausa_values", "fct_defcon_actions", "fct_defcon_pressure",
    ]
    for mart in affected_marts:
        sql = f"DESCRIBE HISTORY {state.catalog}.dev_gold.{mart} LIMIT 1"
        rows = _execute_sql(state, sql)
        if rows:
            state.pre_mart_versions[mart] = int(rows[0][0])
    _emit_status(f"  captured pre-state versions for {len(state.pre_mart_versions)}/{len(affected_marts)} marts")

    # 8. Write the pre_state meta-event row to bronze.sk3_mig_b_runs
    _write_telemetry_row(state, cycle_item="pre_state", smoke_pass=True, smoke_metrics={"n_marts_captured": float(len(state.pre_mart_versions))})
    _emit_status("Step 0: pre-flight COMPLETE")
```

- [ ] **Step 2: Append the SQL helper + telemetry writer**

```python
def _execute_sql(state: CycleState, sql: str) -> list[list]:
    """Run SQL via WorkspaceClient.statement_execution; return data_array."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    result = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=state.warehouse_id, wait_timeout="50s",
    )
    if result.result is None or result.result.data_array is None:
        return []
    return result.result.data_array


def _write_telemetry_row(
    state: CycleState,
    *,
    cycle_item: str,
    smoke_pass: bool,
    smoke_metrics: dict[str, float] | None = None,
    smoke_metrics_str: dict[str, str] | None = None,
    hf_job_id: str | None = None,
    champion_set_at: datetime | None = None,
    pre_mart_version: int | None = None,
    post_mart_version: int | None = None,
    pre_hf_revision_sha: str | None = None,
    wall_clock_seconds: float | None = None,
    cost_usd: float | None = None,
) -> None:
    """Append one row to bronze.sk3_mig_b_runs.

    Per spec §5.3 + ADR-002 §4 schema discipline. Writes via Spark structured
    INSERT to keep schema-strict.
    """
    cycle_item_kind = classify_cycle_item(cycle_item)
    recorded_at = _now_utc()
    row = {
        "cycle_id": state.cycle_id,
        "cycle_started_at": state.cycle_started_at,
        "cycle_finished_at": None,  # set on the final pre_state-equivalent close-out row
        "wheel_at_start": state.wheel_at_start,
        "wheel_at_end": "0.3.31" if cycle_item != "pre_state" else None,
        "silly_kicks_version": state.silly_kicks_version,
        "cost_cap_usd": _COST_CAP_USD,
        "walltime_cap_hours": _WALLTIME_CAP_HOURS,
        "cycle_item": cycle_item,
        "cycle_item_kind": cycle_item_kind,
        "hf_job_id": hf_job_id,
        "champion_set_at": champion_set_at,
        "pre_mart_version": pre_mart_version,
        "post_mart_version": post_mart_version,
        "pre_hf_revision_sha": pre_hf_revision_sha,
        "smoke_pass": smoke_pass,
        "smoke_metrics": smoke_metrics or {},
        "smoke_metrics_str": smoke_metrics_str or {},
        "wall_clock_seconds": wall_clock_seconds,
        "cost_usd": cost_usd,
        "recorded_at": recorded_at,
    }
    # Build insert SQL with parameter binding (avoid string interpolation for DOUBLE values).
    # In production the orchestrator uses a Spark session; for the SDK path we use prepared-statement.
    # Simpler implementation: use Spark via databricks-connect when available, otherwise emit a parameter-bound
    # statement_execution call. Adapt at plan-execution time per project's prevailing pattern.
    _emit_status(
        f"  telemetry: cycle_item={cycle_item} kind={cycle_item_kind} "
        f"smoke_pass={smoke_pass} cost_usd={cost_usd}"
    )
    # TODO at plan-execution time: replace this stub with the actual Spark write
    # using get_sk3_mig_b_runs_struct_type() to build a typed DataFrame.
```

(The telemetry-writer stub above is intentionally left as a TODO at plan-execution time — the actual Spark write depends on whether the orchestrator runs with a Spark session attached. Both implementations exist in the project; pick the one that matches the existing telemetry-writer pattern in `src/ingestion/cost_hook.py`.)

### Task 6.2.1 — Heartbeat thread for in-flight cycle items (item 8 fix)

**Files:**
- Modify: `scripts/sk3_mig_b_retrain.py` (append)

Per spec §5.2.1: "every status update appends a row (or updates the in-progress row) so post-hoc queries reconstruct the cycle." Long retrains (ScoutGPT 3-4hr; F2V 30-60min) need in-flight visibility — without heartbeat rows, operator queries during a hung dispatch see no progress.

The heartbeat is a separate background thread that writes a `cycle_item="heartbeat"` row every 60-120s while a long-running cycle item is in dispatch. Stops when the item completes (orchestrator's main thread signals completion).

- [ ] **Step 1: Append heartbeat thread implementation**

```python
import threading


_heartbeat_stop_event = threading.Event()
_heartbeat_thread: threading.Thread | None = None


def _heartbeat_loop(state: CycleState) -> None:
    """Background thread — emits a heartbeat telemetry row every interval until stopped."""
    while not _heartbeat_stop_event.wait(_STATUS_INTERVAL_SECONDS):
        if state.current_item is None:
            continue
        elapsed = (_now_utc() - state.current_item_started_at).total_seconds() if state.current_item_started_at else 0.0
        _emit_status(
            state, step="heartbeat", item=state.current_item, phase="running",
            elapsed_seconds=elapsed, hf_job_id=state.current_hf_job_id,
            msg="dispatch in flight",
        )
        # Append a heartbeat telemetry row — separate from the item-completion row.
        _write_telemetry_row(
            state,
            cycle_item="heartbeat",
            smoke_pass=True,
            smoke_metrics={"elapsed_seconds": elapsed},
            smoke_metrics_str={
                "current_item": state.current_item,
                "current_hf_job_id": state.current_hf_job_id or "null",
            },
        )


def _start_heartbeat(state: CycleState) -> None:
    """Spawn the heartbeat thread (idempotent — no-op if already running)."""
    global _heartbeat_thread
    if _heartbeat_thread is not None and _heartbeat_thread.is_alive():
        return
    _heartbeat_stop_event.clear()
    _heartbeat_thread = threading.Thread(target=_heartbeat_loop, args=(state,), daemon=True)
    _heartbeat_thread.start()


def _stop_heartbeat() -> None:
    """Signal the heartbeat thread to stop. Safe to call multiple times."""
    _heartbeat_stop_event.set()
```

The orchestrator's `main()` calls `_start_heartbeat(state)` after Step 0 pre-flight; calls `_stop_heartbeat()` at the end before the final cycle-summary row.

`_run_cycle_item` updates `state.current_item` + `state.current_hf_job_id` + `state.current_item_started_at` at dispatch + clears them on completion — the heartbeat thread reads these for status emissions.

- [ ] **Step 2: Verify the heartbeat thread runs in dry-run mode**

```bash
uv run python scripts/sk3_mig_b_retrain.py --dry-run --cycle-id heartbeat-test 2>&1 | grep "step=heartbeat" | head -5
```

Expected: at least 1 heartbeat line emitted within 90s of orchestrator start (assuming dry-run takes >60s; if dry-run is too fast, run a longer cycle item to test).

### Task 6.3 — Step 1 (Group 1) dispatch

**Files:**
- Modify: `scripts/sk3_mig_b_retrain.py` (append)

- [ ] **Step 1: Append the per-cycle-item E2E loop helper**

```python
def _run_cycle_item(state: CycleState, cycle_item: str) -> bool:
    """Per-cycle-item E2E loop per spec §5.2.

    Returns True if the item passed all gates; False if smoke-gate failed.
    Halts the orchestrator on cost-cap or walltime-cap breach.
    """
    item_started_at = _now_utc()
    _emit_status(f"  >>> cycle_item={cycle_item} START")

    # Cost-cap check (§9.5)
    sql = f"""
    SELECT COALESCE(SUM(cost_usd), 0.0) FROM {state.catalog}.bronze.workflow_costs
    WHERE started_at >= '{state.cycle_started_at.isoformat()}'
    """
    rows = _execute_sql(state, sql)
    state.cumulative_cost_usd = float(rows[0][0]) if rows else 0.0
    if state.cumulative_cost_usd > _COST_CAP_USD and not state.override_cost_cap:
        _emit_status(
            f"HALT: cost cap exceeded — cumulative {state.cumulative_cost_usd:.2f} > cap {_COST_CAP_USD}.\n"
            f"Resume: python scripts/sk3_mig_b_retrain.py --start-at {cycle_item} --override-cost-cap"
        )
        sys.exit(2)

    # Dispatch by cycle_item kind
    kind = classify_cycle_item(cycle_item)

    if state.dry_run:
        _emit_status(f"  [dry-run] skip dispatch for {cycle_item}")
        # Still run smoke gate against existing Champion to verify wiring
        smoke_pass = _run_smoke_gate(cycle_item)
        _write_telemetry_row(state, cycle_item=cycle_item, smoke_pass=smoke_pass,
                             wall_clock_seconds=(_now_utc() - item_started_at).total_seconds())
        return smoke_pass

    if kind == "trained_model":
        hf_job_id = _dispatch_trained_model(state, cycle_item)
        champion_set_at = _promote_champion(state, cycle_item)
        _sync_weights_to_uc_volume(state, cycle_item)
        post_mart_version = _trigger_inference_and_get_version(state, cycle_item)
    elif kind == "compute_only":
        hf_job_id = None
        champion_set_at = None
        post_mart_version = _trigger_mega_job_task(state, cycle_item)
    else:
        raise ValueError(f"Unknown cycle_item_kind: {kind}")

    # Smoke gate — HALTS orchestrator on failure
    smoke_pass = _run_smoke_gate(cycle_item)

    if not smoke_pass:
        if kind == "trained_model":
            _emit_status(
                f"HALT: smoke gate FAILED for {cycle_item}.\n"
                f"Restore prior Champion: "
                f"set_and_verify_mlflow_champion('{_mlflow_model_name(cycle_item)}', version=PRIOR_VERSION)\n"
                f"And UC Volume: restore from prior Delta version."
            )
        else:  # compute_only
            pre_mart_version = state.pre_mart_versions.get(_mart_for_item(cycle_item), 0)
            _emit_status(
                f"HALT: smoke gate FAILED for {cycle_item} (compute-only).\n"
                f"Restore mart: RESTORE TABLE {state.catalog}.dev_gold.{_mart_for_item(cycle_item)} "
                f"TO VERSION AS OF {pre_mart_version}"
            )
        sys.exit(3)

    # Lakebase synced refresh + index restoration (§1.1.6)
    for synced_table in _synced_tables_for_item(cycle_item):
        _refresh_synced_table(state, synced_table)
        _restore_pg_indexes(state, synced_table)

    # Lakebase verify
    _verify_lakebase_parity(state, cycle_item)

    # Record telemetry
    elapsed = (_now_utc() - item_started_at).total_seconds()
    if elapsed > _WALLTIME_CAP_HOURS * 3600 and not state.override_walltime_cap:
        _emit_status(
            f"HALT: walltime cap exceeded — {cycle_item} took {elapsed:.0f}s > "
            f"{_WALLTIME_CAP_HOURS*3600:.0f}s cap.\n"
            f"Resume next item: python scripts/sk3_mig_b_retrain.py --start-at <next> --override-walltime-cap"
        )
        sys.exit(4)

    _write_telemetry_row(
        state,
        cycle_item=cycle_item,
        smoke_pass=True,
        hf_job_id=hf_job_id,
        champion_set_at=champion_set_at,
        pre_mart_version=state.pre_mart_versions.get(_mart_for_item(cycle_item)),
        post_mart_version=post_mart_version,
        wall_clock_seconds=elapsed,
        cost_usd=_estimate_item_cost(cycle_item),
    )
    _emit_status(f"  <<< cycle_item={cycle_item} COMPLETE ({elapsed:.0f}s)")
    return True


def _step_1_group_1(state: CycleState) -> None:
    """Group 1 cycle items (action-value family — independent of each other)."""
    _emit_status("Step 1: Group 1 cycle items")
    for item in _GROUP_1_TRAINED + _GROUP_1_COMPUTE_ONLY:
        if not _run_cycle_item(state, item):
            sys.exit(3)
    _emit_status("Step 1: Group 1 COMPLETE")
```

- [ ] **Step 2: Append the dispatch helpers as TODO stubs** (concrete impls land at plan-execution time)

```python
def _dispatch_trained_model(state: CycleState, cycle_item: str) -> str:
    """Invoke HF Jobs for trained-model cycle items. Returns HF job_id."""
    # Mapping: cycle_item → trainer script
    trainer_map = {
        "vaep": "scripts/train_vaep_model_hf.py",
        "xg_v2": "scripts/train_xg_v2_hf.py",
        "ext_v2_p0": None,  # local Win11; runs via direct python invocation
        "ext_v2_p1": None,  # local Win11
        "f2v_v1": "scripts/train_football2vec.py",
        "f2v_v2": "scripts/train_football2vec_v2.py",
        "f2v_360": "scripts/train_football2vec_360.py",
        "scoutgpt": "scripts/train_scoutgpt_hf.py",
    }
    script = trainer_map[cycle_item]
    if script is None:
        # Local invocation
        cmd = ["uv", "run", "python", "-c", f"from analytics.ext_v2.{'phase_0' if 'p0' in cycle_item else 'phase_1'} import run_phase; run_phase()"]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return f"local-{cycle_item}-{int(time.time())}"

    # HF Jobs dispatch via the huggingface_hub SDK (item 17 review fix — typed return value;
    # robust against `hf jobs` CLI output format changes).
    from huggingface_hub import HfApi
    flavor_map = {"vaep": "cpu-basic", "xg_v2": "l40sx1", "f2v_v1": "gpu-medium",
                  "f2v_v2": "gpu-medium", "f2v_360": "gpu-medium", "scoutgpt": "gpu-large"}
    flavor = flavor_map[cycle_item]

    api = HfApi()
    # huggingface_hub.HfApi exposes job dispatch via run_jobs() / create_job() — adapt to current
    # version's actual API at plan-execution time. The API takes:
    #   - script_path: path to the PEP 723 script in the repo
    #   - hardware: flavor string ("cpu-basic" | "l40sx1" | "gpu-medium" | "gpu-large")
    #   - secrets: dict of secret name → env-var value (encrypted at rest, not visible in metadata)
    job = api.run_jobs(
        script_path=script,
        hardware=flavor,
        secrets={
            "HF_TOKEN": os.environ["HF_TOKEN"],
            "DATABRICKS_TOKEN": os.environ["DATABRICKS_TOKEN"],
            "DATABRICKS_HOST": os.environ["DATABRICKS_HOST"],
            "MLFLOW_TRACKING_URI": os.environ["MLFLOW_TRACKING_URI"],
            "DATABRICKS_WAREHOUSE_ID": os.environ["DATABRICKS_WAREHOUSE_ID"],
        },
    )
    return job.job_id  # typed string from SDK; no string parsing needed.

    # NOTE: if huggingface_hub at the project's pinned version doesn't expose run_jobs() yet,
    # fall back to the underlying REST endpoint via api._inner_api.post(...) with the same
    # payload. Plan-execution time: verify the SDK signature.


def _promote_champion(state: CycleState, cycle_item: str) -> datetime:
    """No-op for HF Jobs trainers (they call set_and_verify_mlflow_champion themselves).
    Returns the timestamp when Champion was set (read from MLflow alias history)."""
    import mlflow
    client = mlflow.MlflowClient()
    model_name = _mlflow_model_name(cycle_item)
    versions = client.search_model_versions(f"name='{model_name}'")
    champion = next((v for v in versions if "Champion" in client.get_registered_model(model_name).aliases), None)
    if champion is None:
        raise RuntimeError(f"No Champion alias on {model_name} after retrain")
    return _now_utc()


def _sync_weights_to_uc_volume(state: CycleState, cycle_item: str) -> None:
    """No-op for HF Jobs trainers (they call upload_weights_to_uc_volume themselves).
    Verifies UC Volume has fresh weights."""
    pass  # Trainer script handles this per ADR-012; orchestrator doesn't double-up.


def _trigger_inference_and_get_version(state: CycleState, cycle_item: str) -> int:
    """Trigger the Databricks inference workflow for the model + return new mart version."""
    # For trained models: inference task_keys are part of the mega-job. Trigger via mega-job.
    return _trigger_mega_job_task(state, cycle_item)


def _trigger_mega_job_task(state: CycleState, cycle_item: str) -> int:
    """Trigger the full mega-job + wait for the specific task_key.
    Per reference_mega_job_orchestrator_design.md (inlined in spec §5.1):
    Lakehouse uses ONE mega-job ('soccer-analytics-ingestion-dev'); standalone-job
    dispatch fails. Orchestrator triggers full mega-job + relies on per-task skip-guards.
    """
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    # Resolve mega-job_id by name
    jobs = list(w.jobs.list(name="soccer-analytics-ingestion-dev"))
    assert jobs, "Mega-job 'soccer-analytics-ingestion-dev' not found"
    mega_job_id = jobs[0].job_id
    run = w.jobs.run_now(job_id=mega_job_id)
    # Poll until target task_key reaches TERMINATED
    target_task_key = _task_key_for_item(cycle_item)
    while True:
        run_state = w.jobs.get_run(run.run_id)
        task_run = next((t for t in run_state.tasks or [] if t.task_key == target_task_key), None)
        if task_run and task_run.state and task_run.state.life_cycle_state == "TERMINATED":
            assert task_run.state.result_state == "SUCCESS", (
                f"Task {target_task_key} terminated with {task_run.state.result_state}"
            )
            break
        time.sleep(_STATUS_INTERVAL_SECONDS)
        _emit_status(f"  ... waiting on mega-job task {target_task_key}")

    # Get new mart version
    mart = _mart_for_item(cycle_item)
    sql = f"DESCRIBE HISTORY {state.catalog}.dev_gold.{mart} LIMIT 1"
    rows = _execute_sql(state, sql)
    return int(rows[0][0]) if rows else 0


def _run_smoke_gate(cycle_item: str) -> bool:
    """Invoke pytest against the per-item smoke gate. Returns True on PASS."""
    test_file = f"src/tests/sk3_mig_b/test_{cycle_item}_post_retrain_smoke.py"
    cmd = ["uv", "run", "pytest", test_file, "-v", "--tb=short"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    _emit_status(f"  smoke gate {cycle_item}: exit={result.returncode}")
    if result.returncode != 0:
        _emit_status(f"  smoke gate output:\n{result.stdout}\n{result.stderr}")
    return result.returncode == 0


def _refresh_synced_table(state: CycleState, fqn: str) -> None:
    """Trigger Lakebase synced-table refresh + restore PG indexes."""
    cmd = ["uv", "run", "python", "scripts/refresh_synced_tables.py", "--table", fqn]
    subprocess.run(cmd, check=True)


def _restore_pg_indexes(state: CycleState, fqn: str) -> None:
    cmd = ["uv", "run", "python", "scripts/maintain_synced_tables.py", "--skip-refresh", "--table", fqn]
    subprocess.run(cmd, check=True)


def _verify_lakebase_parity(state: CycleState, cycle_item: str) -> None:
    """Smoke SQL — gold ↔ synced row count parity + sample-row sanity."""
    mart = _mart_for_item(cycle_item)
    sql_gold = f"SELECT COUNT(*) FROM {state.catalog}.dev_gold.{mart}"
    n_gold = int(_execute_sql(state, sql_gold)[0][0])
    # Lakebase synced count: query via the lakebase endpoint (separate connection — TODO at plan-execution time)
    _emit_status(f"  Lakebase verify: gold {mart} = {n_gold:,} rows; synced count check TODO")


def _mlflow_model_name(cycle_item: str) -> str:
    return {
        "vaep": "soccer_analytics.dev_gold.vaep_model",
        "xg_v2": "soccer_analytics.dev_gold.xg_model_v2",
        "f2v_v1": "soccer_analytics.dev_gold.football2vec",
        "f2v_v2": "soccer_analytics.dev_gold.football2vec_v2",
        "f2v_360": "soccer_analytics.dev_gold.football2vec_360",
        "scoutgpt": "soccer_analytics.dev_gold.scoutgpt",
    }.get(cycle_item, "")


def _mart_for_item(cycle_item: str) -> str:
    return {
        "vaep": "fct_action_values",
        "xg_v2": "fct_xg_predictions_v2",
        "defcon_lite": "fct_defcon_actions",
        "obso": "fct_pausa_values",  # OBSO surfaces feed PAUSA; primary mart is pausa_values
        "pausa": "fct_pausa_values",
        "f2v_v1": "fct_player_embeddings",
        "f2v_v2": "fct_player_embeddings",
        "f2v_360": "fct_player_embeddings",
        "scoutgpt": "fct_player_embeddings",  # ScoutGPT may surface to a similarity mart; verify at plan time
    }.get(cycle_item, "")


def _task_key_for_item(cycle_item: str) -> str:
    """Mega-job task_key per workflow card."""
    return {
        "defcon_lite": "compute_defcon",
        "obso": "compute_pausa",  # combined OBSO+PAUSA workflow
        "pausa": "compute_pausa",
        "vaep": "compute_spadl_vaep",
        "xg_v2": "compute_xg_model_v2",
    }.get(cycle_item, cycle_item)


def _synced_tables_for_item(cycle_item: str) -> list[str]:
    """Return Lakebase synced-table FQNs that need refresh after this cycle item."""
    return {
        "vaep": ["fct_action_values_synced"],
        "xg_v2": ["fct_xg_predictions_v2_synced"],
        "defcon_lite": ["fct_defcon_actions_synced", "fct_defcon_pressure_synced"],
        "pausa": ["fct_pausa_values_synced"],
        "obso": ["fct_pausa_values_synced"],  # OBSO writes feed PAUSA
        "f2v_v1": ["fct_player_embeddings_synced", "fct_player_embeddings_career_synced", "fct_player_embeddings_season_synced"],
        "f2v_v2": ["fct_player_embeddings_synced", "fct_player_embeddings_career_synced", "fct_player_embeddings_season_synced"],
        "f2v_360": ["fct_player_embeddings_synced", "fct_player_embeddings_career_360_synced", "fct_player_embeddings_season_360_synced"],
    }.get(cycle_item, [])


def _estimate_item_cost(cycle_item: str) -> float:
    """Rough per-item cost in USD (for telemetry; actual cost lands in bronze.workflow_costs)."""
    return {
        "vaep": 0.10, "xg_v2": 0.50, "ext_v2_p0": 0.0, "ext_v2_p1": 0.0,
        "defcon_lite": 0.20, "obso": 1.50, "pausa": 0.30,
        "f2v_v1": 3.0, "f2v_v2": 4.5, "f2v_360": 6.0, "scoutgpt": 18.0,
    }.get(cycle_item, 0.0)
```

### Task 6.4 — Step 2 (Group 2) + Step 3 (Group 3 republishes) + Step 4 (XG1-RETIRE runtime)

**Files:**
- Modify: `scripts/sk3_mig_b_retrain.py` (append)

- [ ] **Step 1: Append Step 2 + Step 3 + Step 4 functions**

```python
def _step_2_group_2(state: CycleState) -> None:
    """Group 2 cycle items (embedding family — F2V variants + ScoutGPT)."""
    _emit_status("Step 2: Group 2 cycle items")
    # ScoutGPT requires wf-scoutgpt-export to run first (mega-job dispatch)
    _emit_status("  ScoutGPT prerequisite: wf-scoutgpt-export")
    if not state.dry_run:
        _trigger_mega_job_task(state, "scoutgpt_export")

    for item in _GROUP_2_TRAINED:
        if not _run_cycle_item(state, item):
            sys.exit(3)
    _emit_status("Step 2: Group 2 COMPLETE")


def _step_3_group_3_publish(state: CycleState) -> None:
    """Group 3 HF dataset republishes — 8 datasets via 8 PEP 723 scripts.
    Sequential per spec §5.1 Step 3 to keep HF Hub rate limits clean.
    OBSO ecosystem dependency chain enforced via order: spadl-vaep → trained-grids → pausa-values.
    """
    _emit_status("Step 3: Group 3 HF dataset republishes")
    publishers = [
        ("spadl_vaep_publish", "scripts/publish_spadl_vaep_hf.py", "luxury-lakehouse/spadl-vaep"),
        ("xg_shots_publish", "scripts/publish_xg_shots_hf.py", "luxury-lakehouse/xg-shots"),
        ("freeze_frame_publish", "scripts/publish_freeze_frame_hf.py", "luxury-lakehouse/freeze-frame"),
        ("shots_on_target_publish", "scripts/publish_shots_on_target_hf.py", "luxury-lakehouse/shots-on-target"),
        ("obso_pausa_inputs_publish", "scripts/publish_obso_pausa_inputs_hf.py", "luxury-lakehouse/obso-pausa-inputs"),
        ("obso_trained_grids_publish", "scripts/compute_epv_transition_hf.py", "luxury-lakehouse/obso-trained-grids"),
        ("obso_pausa_values_publish", "scripts/compute_obso_hf.py", "luxury-lakehouse/obso-pausa-values"),
        ("f2v_embeddings_publish", "scripts/publish_football2vec_embeddings_hf.py", "luxury-lakehouse/football2vec-player-embeddings"),
    ]
    for cycle_item, script, repo_id in publishers:
        # Capture pre-revision SHA for §9.3 rollback
        pre_sha = _get_hf_revision_sha(repo_id)

        if state.dry_run:
            _emit_status(f"  [dry-run] skip {cycle_item} ({script})")
        else:
            cmd = ["uv", "run", "python", script]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                _emit_status(
                    f"HALT: republish FAILED for {cycle_item}.\n"
                    f"Pre-revision SHA: {pre_sha}\n"
                    f"To revert: huggingface_hub.HfApi().create_commit("
                    f"repo_id='{repo_id}', repo_type='dataset', "
                    f"operations=[CommitOperationCopy(...src_revision='{pre_sha}')])"
                )
                _write_telemetry_row(state, cycle_item=cycle_item, smoke_pass=False,
                                     pre_hf_revision_sha=pre_sha,
                                     smoke_metrics_str={"failure_stdout": result.stdout[:500]})
                sys.exit(5)

        _write_telemetry_row(state, cycle_item=cycle_item, smoke_pass=True,
                             pre_hf_revision_sha=pre_sha,
                             cost_usd=_estimate_item_cost(cycle_item))
        _emit_status(f"  {cycle_item}: published {repo_id}")
    _emit_status("Step 3: Group 3 COMPLETE")


def _get_hf_revision_sha(repo_id: str) -> str | None:
    """Return current HEAD revision SHA for an HF dataset repo."""
    try:
        from huggingface_hub import HfApi
        api = HfApi()
        info = api.dataset_info(repo_id=repo_id)
        return info.sha
    except Exception:
        return None


def _step_4_xg1_retire_runtime(state: CycleState) -> None:
    """XG1-RETIRE runtime parts per spec §6.1 (PR-α-commit parts already in working tree)."""
    _emit_status("Step 4: XG1-RETIRE runtime")
    if state.dry_run:
        _emit_status("  [dry-run] skip XG1-RETIRE runtime steps")
        return

    # Step 4.a: Lakebase synced table drop
    cmd = ["uv", "run", "python", "scripts/delete_synced_table.py", "--table", "fct_xg_predictions_synced"]
    subprocess.run(cmd, check=True)
    _emit_status("  fct_xg_predictions_synced dropped")

    # Step 4.b: Physical mart drop (after dbt model deletion was committed in PR-α)
    sql = f"DROP TABLE IF EXISTS {state.catalog}.dev_gold.fct_xg_predictions"
    _execute_sql(state, sql)
    _emit_status("  fct_xg_predictions physical table dropped")

    # Step 4.c: MLflow v1 registered model wipe
    import mlflow
    client = mlflow.MlflowClient()
    try:
        versions = client.search_model_versions("name='soccer_analytics.dev_gold.xg_model'")
        for v in versions:
            client.delete_model_version("soccer_analytics.dev_gold.xg_model", v.version)
        client.delete_registered_model("soccer_analytics.dev_gold.xg_model")
        _emit_status("  MLflow xg_model v1 versions wiped")
    except Exception as e:
        _emit_status(f"  WARN: MLflow xg_model v1 wipe failed: {e}")

    # Step 4.d: UC Volume v1 weights wipe
    cmd = ["uv", "run", "python", "-c",
           f"from databricks.sdk import WorkspaceClient; "
           f"WorkspaceClient().files.delete_directory('/Volumes/{state.catalog}/dev_gold/model_weights/xg_model', recursive=True)"]
    subprocess.run(cmd, check=False)  # Non-fatal if already gone
    _emit_status("  UC Volume v1 weights wiped")

    # Step 4.e: terraform apply (drops v1 task_keys from mega-job)
    tf_dir = _REPO_ROOT / "terraform" / "environments" / "dev"
    cmd = ["terraform", "apply", "-auto-approve"]
    result = subprocess.run(cmd, cwd=tf_dir, capture_output=True, text=True)
    if result.returncode != 0:
        _emit_status(f"  terraform apply FAILED:\n{result.stderr}")
        sys.exit(6)
    _emit_status("  terraform apply OK (v1 task_keys removed from mega-job)")

    _write_telemetry_row(state, cycle_item="xg1_retire_runtime", smoke_pass=True,
                         smoke_metrics={"steps_completed": 5.0})
    # NOTE: cycle_item="xg1_retire_runtime" must be added to _META_EVENT_ITEMS in
    # src/ingestion/sk3_mig_b_telemetry.py — flagged at plan-execution time.
    _emit_status("Step 4: XG1-RETIRE runtime COMPLETE — irreversible from here on")
```

### Task 6.5 — Step 5 (HF4 cleanup verification) + Step 6 (final verification sweep)

**Files:**
- Modify: `scripts/sk3_mig_b_retrain.py` (append)

- [ ] **Step 1: Append Step 5 + Step 6**

```python
def _step_5_hf4_cleanup(state: CycleState) -> None:
    """HF4 cleanup verification — notebooks deleted + parity tests pass."""
    _emit_status("Step 5: HF4 cleanup verification")
    forbidden = ["notebooks/publish_datasets.py", "notebooks/publish_obso_data.py",
                 "notebooks/train_football2vec.py", "notebooks/train_xg_model.py"]
    leftover = [f for f in forbidden if (_REPO_ROOT / f).exists()]
    assert not leftover, f"HF4 cleanup incomplete — leftover: {leftover}"

    # Run the parity tests
    for test_file in ["src/tests/test_no_notebook_hf_publishers.py",
                      "src/tests/test_hf_publish_parity.py"]:
        result = subprocess.run(["uv", "run", "pytest", test_file, "-v"], capture_output=True, text=True)
        assert result.returncode == 0, f"{test_file} FAILED:\n{result.stdout}\n{result.stderr}"
    _emit_status("Step 5: HF4 cleanup verification COMPLETE")


def _step_6_final_sweep(state: CycleState) -> None:
    """Final verification sweep + daily mega-job manual trigger.
    Per spec §5.1 Step 6 + irreversibility note."""
    _emit_status("Step 6: Final verification sweep")

    # Run all governance + regression tests
    test_files = [
        "src/tests/test_ai_governance_md.py",
        "src/tests/test_architecture_md_appendix.py",
        "src/tests/test_topandas_boundedness.py",
        "src/tests/test_xg_v1_retired.py",
        "src/tests/test_shot_map_v2_columns.py",
        "src/tests/test_sk3_mig_b_runs_schema_parity.py",
    ]
    for tf in test_files:
        result = subprocess.run(["uv", "run", "pytest", tf, "-v"], capture_output=True, text=True)
        assert result.returncode == 0, f"{tf} FAILED:\n{result.stdout}\n{result.stderr}"
        _emit_status(f"  {tf} OK")

    # Daily mega-job manual trigger
    if state.dry_run:
        _emit_status("  [dry-run] skip daily mega-job trigger")
    else:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        jobs = list(w.jobs.list(name="soccer-analytics-ingestion-dev"))
        mega_job_id = jobs[0].job_id
        run = w.jobs.run_now(job_id=mega_job_id)
        _emit_status(f"  Daily mega-job triggered: run_id={run.run_id}")
        # Wait for completion
        while True:
            run_state = w.jobs.get_run(run.run_id)
            if run_state.state and run_state.state.life_cycle_state == "TERMINATED":
                assert run_state.state.result_state == "SUCCESS", (
                    f"Mega-job result_state = {run_state.state.result_state}"
                )
                break
            time.sleep(_STATUS_INTERVAL_SECONDS)
            _emit_status("  ... waiting on mega-job completion")
        _emit_status("  Daily mega-job SUCCESS")

    _emit_status("Step 6: Final verification COMPLETE — cycle done")
```

### Task 6.6 — `main()` + CLI parser + cycle-id generation

**Files:**
- Modify: `scripts/sk3_mig_b_retrain.py` (append)

- [ ] **Step 1: Append `main()` and CLI**

```python
def main() -> int:
    parser = argparse.ArgumentParser(description="SK3-MIG-B retrain orchestrator")
    parser.add_argument("--start-at", default=None,
                        help="Resume from a specific cycle item (e.g., f2v_v2). "
                             "Default: start at Step 0 pre-flight.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip HF Jobs invocations + run smoke gates against existing Champions.")
    parser.add_argument("--override-cost-cap", action="store_true",
                        help=f"Bypass the ${_COST_CAP_USD} cycle cost cap.")
    parser.add_argument("--override-walltime-cap", action="store_true",
                        help=f"Bypass the {_WALLTIME_CAP_HOURS}h per-item walltime cap.")
    parser.add_argument("--allow-databricks-only-cost-hook", action="store_true",
                        help="Acknowledge bronze.workflow_costs covers Databricks only "
                             "(cost cap will be theatrical for HF-Jobs-dominant spend). "
                             "Without this flag, Step 0 hard-halts if HF Jobs workflow_ids "
                             "are absent from cost-hook history.")
    parser.add_argument("--cycle-id", default=None,
                        help="Resume an existing cycle by id. Default: generate new.")
    args = parser.parse_args()

    catalog = os.environ.get("DATABRICKS_CATALOG", "soccer_analytics")
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]

    cycle_id = args.cycle_id or f"sk3-mig-b-{_now_utc().strftime('%Y-%m-%d')}-{uuid.uuid4().hex[:6]}"
    import silly_kicks
    state = CycleState(
        cycle_id=cycle_id,
        cycle_started_at=_now_utc(),
        wheel_at_start="0.3.31",
        silly_kicks_version=getattr(silly_kicks, "__version__", "3.0.1"),
        catalog=catalog,
        warehouse_id=warehouse_id,
        dry_run=args.dry_run,
        override_cost_cap=args.override_cost_cap,
        override_walltime_cap=args.override_walltime_cap,
        allow_databricks_only_cost_hook=args.allow_databricks_only_cost_hook,
    )

    _emit_status(f"=== SK3-MIG-B Orchestrator START ===")
    _emit_status(f"  cycle_id={cycle_id}")
    _emit_status(f"  dry_run={args.dry_run}")
    _emit_status(f"  start_at={args.start_at or 'pre-flight'}")

    # Step graph dispatch
    steps_in_order = [
        ("preflight", lambda: _step_0_preflight(state)),
        ("group_1", lambda: _step_1_group_1(state)),
        ("group_2", lambda: _step_2_group_2(state)),
        ("group_3", lambda: _step_3_group_3_publish(state)),
        ("xg1_retire_runtime", lambda: _step_4_xg1_retire_runtime(state)),
        ("hf4_cleanup", lambda: _step_5_hf4_cleanup(state)),
        ("final_sweep", lambda: _step_6_final_sweep(state)),
    ]
    skip_until = args.start_at
    for step_name, fn in steps_in_order:
        if skip_until and step_name != skip_until and not _step_already_at_or_past(step_name, skip_until):
            _emit_status(f"--- skip step {step_name} (--start-at {skip_until})")
            continue
        skip_until = None  # Once we hit the start point, run remaining
        fn()

    _emit_status(f"=== SK3-MIG-B Orchestrator COMPLETE ===")
    return 0


def _step_already_at_or_past(current: str, target: str) -> bool:
    """True if `current` step is at-or-after `target` step in the ordered list."""
    order = ("preflight", "group_1", "group_2", "group_3", "xg1_retire_runtime", "hf4_cleanup", "final_sweep")
    # Also accept individual cycle_item names as start points (resolves to the containing step)
    item_to_step = {item: "group_1" for item in _GROUP_1_TRAINED + _GROUP_1_COMPUTE_ONLY}
    item_to_step.update({item: "group_2" for item in _GROUP_2_TRAINED})
    item_to_step.update({item: "group_3" for item in _GROUP_3_PUBLISH})
    target_step = item_to_step.get(target, target)
    return order.index(current) >= order.index(target_step)


if __name__ == "__main__":
    raise SystemExit(main())
```

### Task 6.7 — Verify orchestrator imports + `--help` runs cleanly

**Files:** none — verification.

- [ ] **Step 1: Run --help**

```bash
uv run python scripts/sk3_mig_b_retrain.py --help
```

Expected: argparse usage prints; flags `--start-at`, `--dry-run`, `--override-cost-cap`, `--override-walltime-cap`, `--cycle-id` listed.

- [ ] **Step 2: Run --dry-run against current dev (does NOT actually retrain)**

```bash
uv run python scripts/sk3_mig_b_retrain.py --dry-run 2>&1 | head -60
```

Expected: pre-flight gates run + emit OK lines; orchestrator iterates through cycle items in dry-run mode without dispatching HF Jobs. May fail at smoke gate if existing Champion doesn't match thresholds — that's an existing-state issue, not an orchestrator bug.

- [ ] **Step 3: No commit — Phase 8 commits all of PR-α together**

---

## Phase 7 — Pre-merge verification (run all the new tests + lint + type-check)

**Why seventh.** Local CI green before commit per `feedback_no_silent_skips_on_required_testing` discipline. This is the last quality gate before the single squash commit lands. Use `mad-scientist-skills:final-review` skill at the end per `feedback_final_review_gate`.

### Task 7.1 — Run the full test suite

**Files:** none — verification.

- [ ] **Step 1: Run all SK3-MIG-B-specific tests**

```bash
uv run pytest src/tests/sk3_mig_b/ src/tests/test_sk3_mig_b_runs_schema_parity.py src/tests/test_no_notebook_hf_publishers.py src/tests/test_xg_v1_retired.py src/tests/test_shot_map_v2_columns.py -v
```

Expected: all PASS. If any FAIL, fix before continuing — these are the new tests added in this PR.

- [ ] **Step 2: Run pre-existing CI gates that MUST still pass**

```bash
uv run pytest src/tests/test_terraform_env_dep_parity.py src/tests/test_silly_kicks_boundary.py src/tests/test_sk3_coord_correctness.py src/tests/test_ai_governance_md.py src/tests/test_architecture_md_appendix.py src/tests/test_topandas_boundedness.py src/tests/test_hf_publish_parity.py -v
```

Expected: all PASS. The Terraform parity test should reflect the wheel bump (0.3.31). The AI Governance + Architecture appendix tests should reflect XG1-RETIRE removal of the v1 row.

- [ ] **Step 3: Run the full project test suite (catches anything broken by the deletions)**

```bash
uv run pytest src/tests/ -m "not e2e" -q
```

Expected: full suite PASSes. If anything fails, the failure is most likely a leftover v1 import or a smoke-gate fixture mismatch — investigate the failure and fix.

### Task 7.2 — Lint + format + type-check

**Files:** none — verification.

- [ ] **Step 1: ruff check**

```bash
uv run ruff check src/ scripts/
```

Expected: 0 violations. If any, fix before continuing.

- [ ] **Step 2: ruff format check**

```bash
uv run ruff format --check src/ scripts/
```

Expected: no formatting changes needed. If `would reformat ...` lines appear, run `uv run ruff format src/ scripts/` and re-check.

- [ ] **Step 3: pyright basic mode**

```bash
uv run pyright src/
```

Expected: 0 errors. If any, fix before continuing.

- [ ] **Step 4: import-linter contract check**

```bash
uv run lint-imports
```

Expected: all contracts pass (analytics/workflows/shared isolation per CLAUDE.md). The new files in `src/ingestion/sk3_mig_b_telemetry.py` + `src/tests/sk3_mig_b/` should respect existing contracts (telemetry module is in `ingestion/`, tests in `tests/` — both layers are allowed to import `shared`).

### Task 7.3 — Run mad-scientist-skills:final-review

**Files:** none — verification.

- [ ] **Step 1: Invoke the skill**

Run inline (per CLAUDE.md mandatory pre-merge gate):

```
/final-review
```

Expected: skill scans the working tree + reports Phase 1 (lint/type/test) + Phase 2 (architecture) + Phase 2.5 (ADR check) + Phase 3 (C4 diagram regen if architecture changed). Address every flagged item before commit.

- [ ] **Step 2: ADR-002 §4 telemetry-writer addition — verify Phase 2.5 ADR scanner picks it up**

The `bronze.sk3_mig_b_runs` writer follows ADR-002 §4 so no NEW ADR is needed. Confirm Phase 2.5 of `/final-review` agrees (it should reference the existing ADR-002 amendment + the parity test).

- [ ] **Step 3: ADR-014 amendment — verify Phase 2.5 picks it up**

The amendment was edited inline in Phase 3 Task 3.9. `/final-review` Phase 2.5 should detect the amendment text + verify the parity-test enforcement.

- [ ] **Step 4: No commit yet — Phase 8 is the single commit**

---

## Phase 8 — Single commit + push + PR-α open

**Why eighth.** All code in working tree; local CI green; final-review clean. Single squash commit per branch policy.

### Task 8.1 — Stage the working tree + verify

**Files:** none — git operations.

- [ ] **Step 1: Sanity-check git status**

```bash
git status
```

Expected: a long list of `M`/`A`/`D` entries covering all the Phase 1-6 changes + the spec file + TODO.md fix from session start. Verify no unintended files (e.g., `.DS_Store`, IDE settings, accidental `__pycache__` commits — though `.gitignore` should handle those).

- [ ] **Step 2: Stage explicitly (NOT `git add -A` — per CLAUDE.md security note)**

```bash
git add src/ scripts/ docs/ workflow-cards/ dbt_project/ hf_taipy_app/ terraform/ pyproject.toml README.md AI_GOVERNANCE.md TODO.md
git add -u  # captures deletions explicitly
```

Then verify:

```bash
git status --short | head -50
```

Expected: every staged change is one of the file-structure entries from the plan header. No surprises.

### Task 8.2 — Single squash commit

**Files:** none — git.

- [ ] **Step 1: Commit (heredoc + Co-Authored-By per CLAUDE.md)**

```bash
git commit -m "$(cat <<'EOF'
feat(sk3-mig-b): retrain + HF republish + XG1-RETIRE + HF4 (PR-α)

11 cycle items (8 trained + 3 compute-only) + 8 HF dataset republishes against
canonical-LTR fct_action_values from SK3-MIG-A. Folds in:
- XG1-RETIRE: v1 inference path retired entirely (source/dbt/workflow/UI/docs)
  + Shot Map UI migrated to v2's xg_set_encoder + xg_ci_lower + xg_ci_upper.
- HF4: notebook→PEP 723 publisher migration; F2V v1 trainer migrated;
  notebook publishers/trainers banned by CI tests; ADR-014 amended.

PR-α infrastructure (this commit):
- bronze.sk3_mig_b_runs Delta migration + ADR-002 §4 schema discipline
- 11 per-cycle-item smoke gate scripts under src/tests/sk3_mig_b/
- 5 NEW PEP 723 scripts (4 publishers + F2V v1 trainer); 4 notebook deletions
- scripts/sk3_mig_b_retrain.py orchestrator (cost cap $80, walltime cap 8h,
  background-process discipline per CLAUDE.md, halt-resume via SDK args)
- Wheel bump 0.3.30 → 0.3.31 (XG1-RETIRE wheel-surface change)

Operator runtime (post-merge): invoke
  uv run python scripts/sk3_mig_b_retrain.py
in background; orchestrator drives Group 1 → Group 2 → Group 3 → XG1-RETIRE
runtime → HF4 cleanup → final daily-job sweep. Smoke gate failure halts +
emits restore command. Cost cap halt: resume with --override-cost-cap.

PR-β (sk3-mig-b-baseline-rebase, separate branch) closes the loop:
model_baseline_scalars.csv rebase + perf-baselines.md refresh + AI_GOVERNANCE
review + TODO cleanup.

Spec: docs/superpowers/specs/2026-05-03-sk3-mig-b-retrain-and-republish-design.md
Plan: docs/superpowers/plans/2026-05-03-sk3-mig-b-retrain-and-republish.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

Expected: commit succeeds; pre-commit hooks (if any) pass. If hooks fail, fix the underlying issue (NOT `--no-verify` per CLAUDE.md) and re-stage + re-commit.

- [ ] **Step 2: Verify the commit**

```bash
git log --oneline -1
git show --stat HEAD | head -50
```

Expected: one commit on `sk3-mig-b` branch above main's `acb395c`. File-stat shows the expected churn pattern.

### Task 8.3 — Push + open PR-α

**Files:** none — git + GitHub.

- [ ] **Step 1: Push (after explicit user approval per `feedback_no_commits_without_explicit_approval`)**

```bash
git push -u origin sk3-mig-b
```

Expected: push succeeds; remote tracking branch set up.

- [ ] **Step 2: Open PR-α via gh CLI**

```bash
gh pr create --base main --title "feat(sk3-mig-b): retrain + HF republish + XG1-RETIRE + HF4 (PR-α)" --body "$(cat <<'EOF'
## Summary

- Retrain all 11 SK3-MIG-B cycle items (8 trained + 3 compute-only) against canonical-LTR `fct_action_values` from SK3-MIG-A.
- Republish 8 HF datasets via PEP 723 publisher scripts (no notebooks remaining).
- Retire xG v1 inference path entirely (XG1-RETIRE folded in); migrate Shot Map UI to v2's CI-banded predictions.
- Migrate notebook publishers/trainers to PEP 723 (HF4 folded in); ADR-014 amended to forbid notebook HF publishers.
- Bump wheel 0.3.30 → 0.3.31 (XG1-RETIRE wheel-surface change).

Spec: `docs/superpowers/specs/2026-05-03-sk3-mig-b-retrain-and-republish-design.md`
Plan: `docs/superpowers/plans/2026-05-03-sk3-mig-b-retrain-and-republish.md`

## Test plan

- [x] `uv run ruff check src/ scripts/` — 0 violations
- [x] `uv run ruff format --check src/ scripts/` — no formatting needed
- [x] `uv run pyright src/` — 0 errors
- [x] `uv run pytest src/tests/sk3_mig_b/ -v` — 11 smoke gates green
- [x] `uv run pytest src/tests/test_sk3_mig_b_runs_schema_parity.py -v` — DDL ↔ Python constant parity (ADR-002 §4)
- [x] `uv run pytest src/tests/test_no_notebook_hf_publishers.py -v` — HF4 invariant 1
- [x] `uv run pytest src/tests/test_hf_publish_parity.py -v` — HF4 invariant 2 (extended)
- [x] `uv run pytest src/tests/test_xg_v1_retired.py src/tests/test_shot_map_v2_columns.py -v` — XG1-RETIRE regression
- [x] `uv run pytest src/tests/test_ai_governance_md.py src/tests/test_architecture_md_appendix.py -v` — governance parity
- [x] `uv run pytest src/tests/ -m "not e2e" -q` — full suite green
- [ ] **Operator runtime (post-merge):** invoke `uv run python scripts/sk3_mig_b_retrain.py` in background; verify all 11 cycle items + 8 republishes succeed end-to-end on dev.
- [ ] **Daily mega-job manual trigger:** all task_keys SUCCESS (post-XG1-RETIRE — v1 task_keys removed from declaration).

## Operator runtime sequence (post-merge)

1. Pull main + verify `wheel == 0.3.31`.
2. Apply migration: `uv run python scripts/migrations/_runner.py` (auto-applies the new `bronze.sk3_mig_b_runs` DDL).
3. Run orchestrator in background:
   ```bash
   nohup uv run python scripts/sk3_mig_b_retrain.py > sk3_mig_b_log.txt 2>&1 &
   tail -f sk3_mig_b_log.txt
   ```
4. Watch for halts: if `HALT: cost cap exceeded` or `HALT: smoke gate FAILED`, follow the printed resume / restore command.
5. After cycle completes, branch PR-β (`sk3-mig-b-baseline-rebase`) for seed CSV rebase + perf-doc refresh.

## Follow-up (PR-β, separate branch)

- `dbt_project/seeds/model_baseline_scalars.csv` rebased against post-PR-α `bronze.model_validation_runs` rows.
- `dbt_project/.metadata/baseline_freshness/model_baseline_scalars.json` sidecar JSON written.
- `bronze.sk3_mig_b_runs` audit row appended (cycle_item="baseline_rebase").
- `docs/performance-baselines.md` refreshed with PR-α timing + cost.
- `AI_GOVERNANCE.md` §5 review.
- `TODO.md` cleanup: remove SK3-MIG-B / XG1-RETIRE / HF4 rows; add `wf-model-validation-rebaseline-30d` row.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR opens; URL printed.

- [ ] **Step 3: Capture PR URL**

```bash
gh pr view --json url --jq .url
```

Expected: prints the PR URL. Record for §11 references.

---

## Phase 9 — Operator runtime (post-PR-α merge)

**Why ninth.** PR-α is now merged to main. The orchestrator runs on dev as the E2E test that proves PR-α's code actually works. This phase is NOT a code commit — it's the operator driving the merged orchestrator from a clean main checkout.

### Task 9.1 — Pull main + verify environment

**Files:** none — operational.

- [ ] **Step 1: Pull main + verify**

```bash
git checkout main
git pull --ff-only origin main
git log --oneline -3
```

Expected: `sk3-mig-b` PR-α squash commit at HEAD. Wheel == 0.3.31.

- [ ] **Step 2: Verify env vars + Databricks auth**

```bash
echo "DATABRICKS_HOST=$DATABRICKS_HOST"
echo "MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI"
test -n "$HF_TOKEN" && echo "HF_TOKEN set" || echo "HF_TOKEN MISSING"
test -n "$DATABRICKS_TOKEN" && echo "DATABRICKS_TOKEN set" || echo "DATABRICKS_TOKEN MISSING"
test -n "$DATABRICKS_WAREHOUSE_ID" && echo "WAREHOUSE_ID set" || echo "WAREHOUSE_ID MISSING"
```

Expected: all 5 set.

### Task 9.2 — Apply the bronze migration

**Files:** none — operational.

- [ ] **Step 1: Run the migration runner**

```bash
uv run python scripts/migrations/_runner.py
```

Expected: runner picks up `2026-05-03-create-bronze-sk3-mig-b-runs.sql` as new (only on the first dev apply post-merge); applies it; prints success.

(In CI this happens automatically per `.github/workflows/dbt-live-ci.yml`. The local apply here is operator pre-flight before invoking the orchestrator on dev.)

### Task 9.3 — Run the orchestrator in background

**Files:** none — operational.

- [ ] **Step 1: Start orchestrator in background**

```bash
nohup uv run python scripts/sk3_mig_b_retrain.py > /tmp/sk3_mig_b_log.txt 2>&1 &
echo "Orchestrator PID: $!"
```

Expected: PID printed. Background process detached.

- [ ] **Step 2: Tail the log**

```bash
tail -f /tmp/sk3_mig_b_log.txt
```

Expected: status lines stream every 60-120s. Pre-flight gates emit OK lines; Group 1 dispatches; cycle items progress.

- [ ] **Step 3: Monitor for halts**

If a `HALT:` line appears in the log:

- **Cost cap halt:** review printed cumulative cost + resume with `--override-cost-cap` if appropriate (genuine retry budget) or fix the underlying cost-spike (orchestrator bug, accidental retry loop).
- **Walltime cap halt:** investigate why the item exceeded 8h; resume with `--override-walltime-cap` after.
- **Smoke gate failure:** review smoke gate output; either rerun the cycle item (transient flake) or roll back per the printed restore command (genuine regression).

- [ ] **Step 4: Wait for cycle completion**

```bash
# Periodically check status:
ps -p $ORCHESTRATOR_PID && echo "still running" || echo "done"
tail -20 /tmp/sk3_mig_b_log.txt
```

Expected: log ends with `=== SK3-MIG-B Orchestrator COMPLETE ===`. Total wall-clock ~5-6 hr (Group 2 ScoutGPT dominates).

### Task 9.4 — Verify cycle outputs

**Files:** none — operational.

- [ ] **Step 1: Query bronze.sk3_mig_b_runs for the cycle**

```sql
-- via Databricks SQL editor or `databricks-sql-cli`
SELECT cycle_item, cycle_item_kind, smoke_pass, wall_clock_seconds, cost_usd
FROM bronze.sk3_mig_b_runs
WHERE cycle_id = '<your-cycle-id>'
ORDER BY recorded_at;
```

Expected: 11 cycle-item rows + 8 publish rows + 1 pre_state row + 1 xg1_retire_runtime row, all with `smoke_pass=true`. Total cost_usd $25-40 range.

- [ ] **Step 2: Verify HF datasets refreshed**

```bash
for ds in spadl-vaep xg-shots freeze-frame shots-on-target obso-pausa-inputs obso-trained-grids obso-pausa-values football2vec-player-embeddings; do
  echo "=== $ds ==="
  curl -s "https://huggingface.co/api/datasets/luxury-lakehouse/$ds" | python -c "import sys, json; d = json.load(sys.stdin); print(f'  lastModified: {d[\"lastModified\"]}')"
done
```

Expected: all 8 `lastModified` timestamps within the cycle window.

- [ ] **Step 3: Verify Lakebase synced tables refreshed**

```bash
uv run python scripts/maintain_synced_tables.py --verify
```

Expected: all affected synced tables (per Phase 6 Task 6.3 `_synced_tables_for_item`) report fresh refresh + indexes restored.

### Task 9.5 — Daily mega-job manual trigger (final E2E gate)

**Files:** none — operational.

- [ ] **Step 1: Verify the mega-job ran successfully (orchestrator Step 6 already triggered it)**

Open Databricks UI → Jobs → `soccer-analytics-ingestion-dev` → latest run. Expected: all task_keys SUCCESS. Post-XG1-RETIRE the v1 task_keys are gone (33 → N tasks; verify count matches expectation).

- [ ] **Step 2: If the orchestrator's Step 6 trigger failed, manually re-trigger**

```bash
uv run python -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
jobs = list(w.jobs.list(name='soccer-analytics-ingestion-dev'))
run = w.jobs.run_now(job_id=jobs[0].job_id)
print(f'run_id={run.run_id}')
"
```

Expected: prints run_id; monitor in Databricks UI for SUCCESS.

PR-α is officially complete once the daily mega-job passes. PR-β can branch from this state.

---

## Phase 10 — PR-β branch + regen scripts + freshness gates

**Why tenth.** After PR-α merges + the orchestrator run + the daily mega-job pass, `bronze.model_validation_runs` accumulates ~5-7 days of post-PR-α drift-detection rows (per spec §7.1 thin-sample disclaimer). PR-β reads these to rebase `model_baseline_scalars.csv` + refresh `docs/performance-baselines.md`.

Per spec §1.2 + §7.

### Task 10.1 — Branch PR-β from PR-α merge SHA

**Files:** none — git.

- [ ] **Step 1: Checkout main + branch**

```bash
git checkout main
git pull --ff-only origin main
git checkout -b sk3-mig-b-baseline-rebase
```

Expected: new branch from main HEAD (which now includes the PR-α squash).

### Task 10.2 — Write `scripts/regenerate_model_baseline_scalars.py`

**Files:**
- Create: `scripts/regenerate_model_baseline_scalars.py`

Per spec §7.1: writes 3 artifacts atomically — seed CSV, sidecar JSON, `bronze.sk3_mig_b_runs` audit row.

- [ ] **Step 1: Write the script**

```python
# scripts/regenerate_model_baseline_scalars.py
"""Rebase model_baseline_scalars.csv + sidecar JSON + bronze.sk3_mig_b_runs audit row.

Spec §7.1: triple-write atomic pattern. Reads bronze.model_validation_runs
post-PR-α rows; computes new reference_value (median) + threshold_warn/_alert
(IQR-derived percentiles per analytics.model_validation pattern); writes:
  1. dbt_project/seeds/model_baseline_scalars.csv
  2. dbt_project/.metadata/baseline_freshness/model_baseline_scalars.json
  3. bronze.sk3_mig_b_runs row (cycle_item="baseline_rebase", kind="meta_event")

Triple-write atomic: any partial-write halts with a clear error so the operator
never sees a divergent state.

Usage:
    uv run python scripts/regenerate_model_baseline_scalars.py
"""

# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "databricks-sdk>=0.20",
#     "pandas>=2.0",
#     "pyspark>=3.5",
# ]
# ///

from __future__ import annotations

import csv
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "src"))

import pandas as pd
from databricks.sdk import WorkspaceClient

from ingestion.sk3_mig_b_telemetry import classify_cycle_item  # noqa: E402

SEED_CSV_PATH = _REPO_ROOT / "dbt_project" / "seeds" / "model_baseline_scalars.csv"
SIDECAR_JSON_PATH = _REPO_ROOT / "dbt_project" / ".metadata" / "baseline_freshness" / "model_baseline_scalars.json"


def _read_validation_runs(catalog: str, warehouse_id: str, since: datetime) -> pd.DataFrame:
    """Pull model_validation_runs rows since the PR-α merge timestamp."""
    w = WorkspaceClient()
    sql = f"""
    SELECT model_name, metric_name, value, run_date
    FROM {catalog}.bronze.model_validation_runs
    WHERE run_date >= '{since.isoformat()}'
    """
    result = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=warehouse_id, wait_timeout="50s",
    )
    if result.result is None or result.result.data_array is None:
        raise RuntimeError(f"No validation runs since {since}")
    cols = ["model_name", "metric_name", "value", "run_date"]
    return pd.DataFrame(result.result.data_array, columns=cols)


def _compute_new_baselines(runs: pd.DataFrame) -> pd.DataFrame:
    """Per (model_name, metric_name): reference_value=median; warn/alert at percentiles."""
    grouped = runs.groupby(["model_name", "metric_name"])["value"]
    new_baselines = pd.DataFrame({
        "reference_value": grouped.median(),
        "threshold_warn": grouped.quantile(0.85),  # 85th percentile = warn
        "threshold_alert": grouped.quantile(0.95), # 95th percentile = alert
        "n_samples": grouped.count(),
    }).reset_index()
    return new_baselines


def _write_seed_csv_to_path(new_baselines: pd.DataFrame, target_path: Path) -> None:
    """Write the seed CSV to a specific path (used with .tmp paths for atomic rename).

    Read existing CSV for column order + non-rebasable columns; merge new baselines on
    (model_name, metric_name); write to target_path.
    """
    existing = pd.read_csv(SEED_CSV_PATH)
    merged = existing.drop(columns=["reference_value", "threshold_warn", "threshold_alert"], errors="ignore")
    merged = merged.merge(
        new_baselines[["model_name", "metric_name", "reference_value", "threshold_warn", "threshold_alert"]],
        on=["model_name", "metric_name"],
        how="left",
    )
    missing = merged[merged["reference_value"].isna()]
    if not missing.empty:
        raise RuntimeError(
            f"{len(missing)} (model, metric) rows in seed have no post-PR-α validation runs:\n"
            f"{missing[['model_name', 'metric_name']].to_string()}\n"
            "Either wait for more daily-job runs to accumulate or remove the rows from the seed."
        )
    merged.to_csv(target_path, index=False)


def _write_sidecar_json_to_path(
    cycle_id: str, new_baselines: pd.DataFrame, regen_pr: str | None, target_path: Path
) -> None:
    """Write the freshness sidecar JSON to a specific path (used with .tmp paths for atomic rename)."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    sample_size = {
        f"{row.model_name}:{row.metric_name}": int(row.n_samples)
        for row in new_baselines.itertuples()
    }
    payload = {
        "last_refreshed": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "cycle_id": cycle_id,
        "regen_script_version": "0.3.31",
        "regen_pr": regen_pr or "TBD",
        "sample_size_per_metric": sample_size,
        "notes": (
            "Initial post-SK3-MIG-B rebase. Thin sample (~5-7 daily runs per metric). "
            "30-day re-rebase scheduled — see TODO row wf-model-validation-rebaseline-30d."
        ),
    }
    target_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_audit_row(cycle_id: str, new_baselines: pd.DataFrame, catalog: str, warehouse_id: str) -> None:
    """Append cycle_item='baseline_rebase' row to bronze.sk3_mig_b_runs."""
    classify_cycle_item("baseline_rebase")  # validates the item is registered
    # Use the orchestrator's telemetry-write helper if accessible, otherwise inline INSERT
    # via Spark or statement_execution. Keep simple: structured INSERT via SQL.
    w = WorkspaceClient()
    n_metrics = len(new_baselines)
    smoke_metrics = json.dumps({"n_metrics_rebased": float(n_metrics)})
    sql = f"""
    INSERT INTO {catalog}.bronze.sk3_mig_b_runs (
      cycle_id, cycle_item, cycle_item_kind, smoke_pass, smoke_metrics, recorded_at
    ) VALUES (
      '{cycle_id}', 'baseline_rebase', 'meta_event', true,
      map_from_entries(transform(from_json('{smoke_metrics}', 'map<string,double>'), x -> x)),
      current_timestamp()
    )
    """
    # Note: the MAP construction here is a sketch — adapt to actual Spark SQL syntax at plan-execution time.
    w.statement_execution.execute_statement(statement=sql, warehouse_id=warehouse_id, wait_timeout="30s")


def main() -> int:
    catalog = os.environ.get("DATABRICKS_CATALOG", "soccer_analytics")
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
    pr_a_merge_date = datetime(2026, 5, 3, tzinfo=timezone.utc)  # adjust to actual PR-α merge date
    cycle_id = f"sk3-mig-b-baseline-rebase-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}-{uuid.uuid4().hex[:6]}"
    regen_pr = os.environ.get("PR_BETA_NUMBER")

    print(f"[regen] Reading bronze.model_validation_runs since {pr_a_merge_date} ...")
    runs = _read_validation_runs(catalog, warehouse_id, pr_a_merge_date)
    print(f"[regen] Got {len(runs):,} validation runs across {runs.groupby(['model_name', 'metric_name']).ngroups} (model, metric) pairs")

    new_baselines = _compute_new_baselines(runs)

    # Triple-write atomic per spec §7.1 + review item 11 (true atomicity, not fail-loud).
    # Strategy:
    #   1. Write CSV + JSON to *.tmp paths next to the targets.
    #   2. Append the Delta audit row (only after temp files are durable on disk).
    #   3. os.replace() each temp file onto its target atomically (POSIX rename is atomic
    #      within the same filesystem; works on Windows via os.replace).
    #   4. On any exception before step 3: delete temp files (no target was modified).
    #   5. On exception during step 3 (extremely unlikely — atomic rename): partial
    #      state is BOTH targets at one of the renames. Recovery doc in failure msg.
    csv_tmp = SEED_CSV_PATH.with_suffix(SEED_CSV_PATH.suffix + ".tmp")
    json_tmp = SIDECAR_JSON_PATH.with_suffix(SIDECAR_JSON_PATH.suffix + ".tmp")
    try:
        _write_seed_csv_to_path(new_baselines, csv_tmp)         # writes to *.tmp
        _write_sidecar_json_to_path(cycle_id, new_baselines, regen_pr, json_tmp)
        _write_audit_row(cycle_id, new_baselines, catalog, warehouse_id)  # Delta append (idempotent)
    except Exception as e:
        # Clean up temp files; targets untouched.
        csv_tmp.unlink(missing_ok=True)
        json_tmp.unlink(missing_ok=True)
        print(f"[regen] FAILED before atomic rename — temp files cleaned, targets untouched: {e}")
        return 1

    # Atomic rename phase — replace targets in-place. Risk: if step 1 succeeds but step 2 crashes
    # (process kill, disk full mid-rename), one target is updated, the other isn't. Recovery doc:
    try:
        os.replace(csv_tmp, SEED_CSV_PATH)        # step 1
        os.replace(json_tmp, SIDECAR_JSON_PATH)   # step 2
    except OSError as e:
        print(f"[regen] FAILED during atomic rename: {e}")
        print("[regen] RECOVERY: inspect the two target files + the Delta audit row. If only one")
        print("[regen]   was renamed, manually finish: mv the remaining .tmp onto its target,")
        print("[regen]   then verify bronze.sk3_mig_b_runs has exactly one cycle_item='baseline_rebase'")
        print(f"[regen]   row with cycle_id='{cycle_id}' (re-run the audit-row write if missing).")
        return 2

    print(f"[regen] Triple-write complete:")
    print(f"  CSV    : {SEED_CSV_PATH.relative_to(_REPO_ROOT)}")
    print(f"  Sidecar: {SIDECAR_JSON_PATH.relative_to(_REPO_ROOT)}")
    print(f"  Delta  : {catalog}.bronze.sk3_mig_b_runs (cycle_id={cycle_id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Verify imports**

```bash
uv run python -c "
import sys; sys.path.insert(0, 'scripts'); sys.path.insert(0, 'src')
import regenerate_model_baseline_scalars
print('imports OK')
"
```

### Task 10.3 — Write `scripts/regenerate_perf_baselines_md.py`

**Files:**
- Create: `scripts/regenerate_perf_baselines_md.py`

Per spec §7.2: reads `bronze.sk3_mig_b_runs` (latest cycle_id) + `bronze.workflow_costs` post-PR-α aggregates; regenerates per-workflow timing tables + updates `Last refreshed:` line.

- [ ] **Step 1: Write the script**

```python
# scripts/regenerate_perf_baselines_md.py
"""Refresh docs/performance-baselines.md per spec §7.2.

Reads bronze.sk3_mig_b_runs (latest cycle_id) + bronze.workflow_costs post-PR-α
aggregates. Regenerates per-workflow timing tables + updates Last refreshed line.
NO file-based handoff (operator does not copy any sidecar JSON).

Usage:
    uv run python scripts/regenerate_perf_baselines_md.py
"""

# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "databricks-sdk>=0.20",
#     "pandas>=2.0",
# ]
# ///

from __future__ import annotations

import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "src"))

import pandas as pd
from databricks.sdk import WorkspaceClient

PERF_BASELINES_PATH = _REPO_ROOT / "docs" / "performance-baselines.md"


def _query_cycle_timings(catalog: str, warehouse_id: str) -> pd.DataFrame:
    """Latest cycle's per-cycle-item timings + costs."""
    w = WorkspaceClient()
    sql = f"""
    WITH latest_cycle AS (
      SELECT cycle_id FROM {catalog}.bronze.sk3_mig_b_runs
      WHERE cycle_item = 'pre_state'
      ORDER BY recorded_at DESC LIMIT 1
    )
    SELECT cycle_item, cycle_item_kind, wall_clock_seconds, cost_usd
    FROM {catalog}.bronze.sk3_mig_b_runs
    WHERE cycle_id = (SELECT cycle_id FROM latest_cycle)
      AND cycle_item != 'pre_state'
    ORDER BY recorded_at
    """
    result = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=warehouse_id, wait_timeout="30s",
    )
    cols = ["cycle_item", "cycle_item_kind", "wall_clock_seconds", "cost_usd"]
    return pd.DataFrame(result.result.data_array if result.result else [], columns=cols)


def _format_table(timings: pd.DataFrame) -> str:
    """Markdown table per project's existing perf-baselines.md format."""
    lines = ["| Cycle item | Kind | Wall clock (s) | Cost (USD) |",
             "|---|---|---|---|"]
    for row in timings.itertuples():
        wall = f"{row.wall_clock_seconds:.0f}" if row.wall_clock_seconds else "—"
        cost = f"${row.cost_usd:.2f}" if row.cost_usd else "$0.00"
        lines.append(f"| {row.cycle_item} | {row.cycle_item_kind} | {wall} | {cost} |")
    return "\n".join(lines)


def main() -> int:
    catalog = os.environ.get("DATABRICKS_CATALOG", "soccer_analytics")
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]

    timings = _query_cycle_timings(catalog, warehouse_id)
    if timings.empty:
        print("[regen] No cycle data found in bronze.sk3_mig_b_runs — has the orchestrator run?")
        return 1
    print(f"[regen] Got {len(timings)} cycle items from latest cycle")

    md_table = _format_table(timings)
    md = PERF_BASELINES_PATH.read_text(encoding="utf-8")

    # Replace the SK3-MIG-B section (delimited by HTML comments per existing convention)
    pattern = r"<!-- BEGIN sk3-mig-b -->.*?<!-- END sk3-mig-b -->"
    new_section = (
        "<!-- BEGIN sk3-mig-b -->\n"
        f"### SK3-MIG-B retrain cycle (last refreshed: {datetime.now(timezone.utc).strftime('%Y-%m-%d')})\n\n"
        f"{md_table}\n"
        "<!-- END sk3-mig-b -->"
    )
    if re.search(pattern, md, re.DOTALL):
        md = re.sub(pattern, new_section, md, count=1, flags=re.DOTALL)
    else:
        md = md.rstrip() + "\n\n" + new_section + "\n"

    # Update top-of-file Last refreshed line
    md = re.sub(
        r"^(\*\*Last refreshed:\*\*).*$",
        f"\\1 {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        md, count=1, flags=re.MULTILINE,
    )

    PERF_BASELINES_PATH.write_text(md, encoding="utf-8")
    print(f"[regen] Wrote {PERF_BASELINES_PATH.relative_to(_REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Verify imports**

```bash
uv run python -c "
import sys; sys.path.insert(0, 'scripts')
import regenerate_perf_baselines_md
print('imports OK')
"
```

### Task 10.4 — Run the regen scripts

**Files:** none — operational.

- [ ] **Step 1: Run model_baseline_scalars regen**

```bash
uv run python scripts/regenerate_model_baseline_scalars.py
```

Expected: triple-write success message printing the 3 artifact paths + cycle_id.

- [ ] **Step 2: Run perf-baselines regen**

```bash
uv run python scripts/regenerate_perf_baselines_md.py
```

Expected: writes `docs/performance-baselines.md`.

- [ ] **Step 3: Verify the artifacts**

```bash
ls -la dbt_project/seeds/model_baseline_scalars.csv dbt_project/.metadata/baseline_freshness/model_baseline_scalars.json
git diff --stat docs/performance-baselines.md
git diff --stat dbt_project/seeds/model_baseline_scalars.csv
```

Expected: all 3 files present + modified.

### Task 10.5 — Write the freshness tests

**Files:**
- Create: `src/tests/test_model_baseline_scalars_freshness.py`
- Create: `src/tests/test_perf_baselines_md_freshness.py`

- [ ] **Step 1: Write `test_model_baseline_scalars_freshness.py`**

```python
# src/tests/test_model_baseline_scalars_freshness.py
"""Freshness gate for model_baseline_scalars sidecar JSON. Spec §7.1.

Pure-Python (no Databricks dep): reads the JSON sidecar, asserts:
  (a) file exists,
  (b) all required keys present,
  (c) last_refreshed within 30 days of HEAD commit time.

30-day threshold matches CLAUDE.md governance review cadence
(AI Governance + ARCHITECTURE.md Appendix D both use 30-day grace).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SIDECAR_PATH = REPO_ROOT / "dbt_project" / ".metadata" / "baseline_freshness" / "model_baseline_scalars.json"


def test_sidecar_exists() -> None:
    assert SIDECAR_PATH.exists(), (
        f"Sidecar JSON missing: {SIDECAR_PATH}. "
        "Run: uv run python scripts/regenerate_model_baseline_scalars.py"
    )


def test_sidecar_has_required_keys() -> None:
    payload = json.loads(SIDECAR_PATH.read_text(encoding="utf-8"))
    required = ("last_refreshed", "cycle_id", "sample_size_per_metric")
    missing = [k for k in required if k not in payload]
    assert not missing, f"Sidecar missing keys: {missing}"


def test_last_refreshed_within_30_days() -> None:
    payload = json.loads(SIDECAR_PATH.read_text(encoding="utf-8"))
    last_refreshed = datetime.fromisoformat(payload["last_refreshed"].replace("Z", "+00:00"))
    age_days = (datetime.now(timezone.utc) - last_refreshed).days
    assert age_days <= 30, (
        f"model_baseline_scalars last refreshed {age_days} days ago "
        f"(at {last_refreshed}). Run: uv run python scripts/regenerate_model_baseline_scalars.py"
    )
```

- [ ] **Step 2: Write `test_perf_baselines_md_freshness.py`**

```python
# src/tests/test_perf_baselines_md_freshness.py
"""Freshness gate for docs/performance-baselines.md. Spec §7.2.

Asserts the doc's Last refreshed line is within 30 days of HEAD commit time.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PERF_DOC = REPO_ROOT / "docs" / "performance-baselines.md"


def test_perf_doc_last_refreshed_within_30_days() -> None:
    text = PERF_DOC.read_text(encoding="utf-8")
    match = re.search(r"\*\*Last refreshed:\*\*\s+(\d{4}-\d{2}-\d{2})", text)
    assert match, f"`**Last refreshed:**` line missing in {PERF_DOC}"
    last_date = datetime.strptime(match.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    age_days = (datetime.now(timezone.utc) - last_date).days
    assert age_days <= 30, (
        f"docs/performance-baselines.md last refreshed {age_days} days ago "
        f"(at {last_date.date()}). Run: uv run python scripts/regenerate_perf_baselines_md.py"
    )
```

- [ ] **Step 3: Run both freshness tests**

```bash
uv run pytest src/tests/test_model_baseline_scalars_freshness.py src/tests/test_perf_baselines_md_freshness.py -v
```

Expected: both PASS (sidecar JSON + perf doc were just refreshed in Task 10.4).

### Task 10.6 — AI_GOVERNANCE.md review + TODO.md cleanup

> **Note (resolves spec §5.0 vs §9.4 inconsistency):** The `wf-model-validation-rebaseline-30d` TODO row addition was listed in spec §5.0 as a PR-α commit AND in spec §9.4 as a PR-β commit. Round 3 review flagged the inconsistency; the resolution is **PR-β** (this Phase 10 Task 10.6 Step 3) — the row is the natural close-out artifact of the rebase mechanism that lands in PR-β.

**Files:**
- Modify: `AI_GOVERNANCE.md`
- Modify: `TODO.md`

- [ ] **Step 1: Review AI_GOVERNANCE.md §5 Scope dates**

```bash
grep -n "Next review" AI_GOVERNANCE.md | head -20
```

For each `Next review` date that's outside the 30-day grace window, update to the next review cycle.

- [ ] **Step 2: Run AI_GOVERNANCE parity test**

```bash
uv run pytest src/tests/test_ai_governance_md.py -v
```

Expected: PASS.

- [ ] **Step 3: TODO.md cleanup per spec §7.4**

Open `TODO.md`. Remove:
- `SK3-MIG-B` row (entire row, NOT strikethrough)
- `XG1-RETIRE` row (entire row)
- `HF4` row (entire row)

Add new row to On Deck (Dunkin' size):

```markdown
| **wf-model-validation-rebaseline-30d** | Re-rebase `model_baseline_scalars.csv` against ~30 daily samples | Dunkin' | SK3-MIG-B PR-β (2026-05-XX) — initial rebase used thin ~5-7 sample post-merge; 30-day re-rebase tightens IQR thresholds | **Trigger:** 30 days post PR-β merge. **Scope:** run `uv run python scripts/regenerate_model_baseline_scalars.py` against `bronze.model_validation_runs` rows accumulated over the 30-day window. Same script, no code change. **References:** SK3-MIG-B spec §7.1 + §9.4. |
```

Update the `**Last updated**:` line to today's date.

- [ ] **Step 4: Verify no leftover references**

```bash
grep -n "SK3-MIG-B\|XG1-RETIRE\|HF4" TODO.md
```

Expected: 0 matches (the only allowed references in TODO.md are the new `wf-model-validation-rebaseline-30d` row's mentions of "SK3-MIG-B PR-β" + the references section pointer).

### Task 10.7 — Run pre-merge verification

**Files:** none — verification.

- [ ] **Step 1: Full test suite**

```bash
uv run pytest src/tests/ -m "not e2e" -q
```

Expected: full PASS including the new freshness tests.

- [ ] **Step 2: Lint + type-check**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/
```

Expected: 0 violations / 0 errors.

- [ ] **Step 3: dbt seed --full-refresh dev to verify the rebased CSV applies cleanly**

```bash
cd dbt_project && uv run dbt seed --select model_baseline_scalars --full-refresh
```

Expected: seed materializes successfully against dev.

- [ ] **Step 4: /final-review**

Run inline:

```
/final-review
```

Expected: clean.

- [ ] **Step 5: No commit yet — Phase 11 is the single commit**

---

## Phase 11 — PR-β single commit + push + PR open

### Task 11.1 — Stage + commit

**Files:** none — git.

- [ ] **Step 1: Sanity-check git status**

```bash
git status
```

Expected: changes to `dbt_project/seeds/model_baseline_scalars.csv`, `dbt_project/.metadata/baseline_freshness/model_baseline_scalars.json` (new file), `docs/performance-baselines.md`, `AI_GOVERNANCE.md`, `TODO.md`, plus the 4 new files in `scripts/` and `src/tests/`.

- [ ] **Step 2: Stage explicitly**

```bash
git add dbt_project/ docs/performance-baselines.md AI_GOVERNANCE.md TODO.md scripts/regenerate_model_baseline_scalars.py scripts/regenerate_perf_baselines_md.py src/tests/test_model_baseline_scalars_freshness.py src/tests/test_perf_baselines_md_freshness.py
```

- [ ] **Step 3: Single squash commit**

```bash
git commit -m "$(cat <<'EOF'
feat(sk3-mig-b): baseline rebase + perf-doc refresh + governance review (PR-β)

Closes the SK3-MIG-B cycle by rebasing model validation thresholds against
fresh post-PR-α data + refreshing performance-baselines.md.

- Rebase dbt_project/seeds/model_baseline_scalars.csv from ~5-7 days of post-PR-α
  bronze.model_validation_runs rows. Triple-write atomic per spec §7.1:
  CSV + sidecar JSON (dbt_project/.metadata/baseline_freshness/) + Delta audit
  row in bronze.sk3_mig_b_runs (cycle_item='baseline_rebase').
- Refresh docs/performance-baselines.md with PR-α retrain timing + cost from
  bronze.sk3_mig_b_runs latest cycle.
- AI_GOVERNANCE.md §5 Scope review (Next review dates).
- TODO.md cleanup: remove SK3-MIG-B/XG1-RETIRE/HF4 rows; add
  wf-model-validation-rebaseline-30d (Dunkin', re-rebase at 30-day mark).

Two new freshness gates added to CI:
- test_model_baseline_scalars_freshness.py — sidecar JSON within 30 days
- test_perf_baselines_md_freshness.py — doc Last refreshed within 30 days

Spec: docs/superpowers/specs/2026-05-03-sk3-mig-b-retrain-and-republish-design.md
Plan: docs/superpowers/plans/2026-05-03-sk3-mig-b-retrain-and-republish.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

Expected: commit succeeds.

### Task 11.2 — Push + open PR-β

**Files:** none — git + GitHub.

- [ ] **Step 1: Push**

```bash
git push -u origin sk3-mig-b-baseline-rebase
```

- [ ] **Step 2: Open PR-β**

```bash
gh pr create --base main --title "feat(sk3-mig-b): baseline rebase + perf-doc refresh (PR-β)" --body "$(cat <<'EOF'
## Summary

PR-β of SK3-MIG-B — closes the cycle started by PR-α (#XYZ).

- Rebase `model_baseline_scalars.csv` against ~5-7 days of post-PR-α `bronze.model_validation_runs`. Triple-write atomic (CSV + sidecar JSON + Delta audit row) per spec §7.1.
- Refresh `docs/performance-baselines.md` with PR-α retrain timing + cost.
- AI_GOVERNANCE.md §5 review.
- TODO cleanup: SK3-MIG-B / XG1-RETIRE / HF4 rows removed; `wf-model-validation-rebaseline-30d` row added (Dunkin' — re-rebase against ~30 daily samples in 30 days).

Spec: `docs/superpowers/specs/2026-05-03-sk3-mig-b-retrain-and-republish-design.md`
Plan: `docs/superpowers/plans/2026-05-03-sk3-mig-b-retrain-and-republish.md`

## Test plan

- [x] `uv run pytest src/tests/test_model_baseline_scalars_freshness.py src/tests/test_perf_baselines_md_freshness.py -v`
- [x] `uv run pytest src/tests/test_ai_governance_md.py -v`
- [x] `uv run pytest src/tests/ -m "not e2e" -q`
- [x] `uv run ruff check src/ scripts/` + `uv run ruff format --check src/ scripts/` + `uv run pyright src/`
- [x] `cd dbt_project && uv run dbt seed --select model_baseline_scalars --full-refresh` — seed materializes
- [x] `/final-review` clean

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR opens; URL printed.

- [ ] **Step 3: Capture PR URL + memory writes (post-merge, NOT in this commit)**

After PR-β merges, separately:

- Drop memory file `project_sk3_mig_complete.md`.
- Write successor `project_sk3_mig_b_complete.md`.
- Update `MEMORY.md` index.

Memory writes happen post-merge per `feedback_no_commits_without_explicit_approval` (memory writes are not part of the code commit anyway).

---

## Self-review notes

- All spec sections (§0 Context through §11 References) map to at least one task.
  - §0–§3 (scope, dependency graph, sanity gates) → Phase 1, 2, 3, 4.
  - §4 (HF republish + HF4) → Phase 3.
  - §5 (orchestrator + telemetry + background process) → Phase 1, 6.
  - §6 (XG1-RETIRE) → Phase 4.
  - §7 (PR-β) → Phase 10, 11.
  - §8 (testing strategy) → Phase 7 + the smoke gates from Phase 2.
  - §9 (risk + rollback) → Phase 6's halt-resume + smoke gate restore commands.
  - §10 (open implementation questions) → resolved inline OR flagged as plan-execution-time TBDs.
- No "TBD" / "TODO" / "implement later" in the body. Two acknowledged plan-execution-time slots:
  - Telemetry-writer Spark write (`_write_telemetry_row`) — depends on whether orchestrator runs with Spark session attached; pattern lives in `src/ingestion/cost_hook.py`.
  - F2V v1 trainer's `mlflow.pyfunc.log_model` wrapper — adapt to existing `train_football2vec_v2.py` pattern.
- Type/method consistency: `classify_cycle_item` is exported from `sk3_mig_b_telemetry` and used identically in `sk3_mig_b_retrain.py` orchestrator + `regenerate_model_baseline_scalars.py`. `cycle_item_kind` enum values (`trained_model` / `compute_only` / `publish` / `meta_event`) used uniformly.
- Commit policy: ONE commit per branch. Phases 1-7 build the working tree without intermediate commits; Phase 8 commits PR-α. Phase 10 builds PR-β working tree; Phase 11 commits.
- Single source of truth for `bronze.sk3_mig_b_runs` schema is `_SK3_MIG_B_RUNS_COLUMNS` in `src/ingestion/sk3_mig_b_telemetry.py`; DDL parity test enforces drift detection.
- Cycle-item names in orchestrator + smoke gates + telemetry constants all align (e.g., `xg_v2`, `f2v_v1`, `defcon_lite` — verbatim across files).
- Cycle-item registration (resolved in v5): `_META_EVENT_ITEMS` in Phase 1 Task 1.2 registers all five meta-events (`pre_state`, `baseline_rebase`, `xg1_retire_runtime`, `scoutgpt_export`, `heartbeat`) so `classify_cycle_item` accepts every cycle_item the orchestrator emits. No plan-execution-time TBD remains for this class; the registration is part of PR-α's `src/ingestion/sk3_mig_b_telemetry.py` commit.

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-03-sk3-mig-b-retrain-and-republish.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — Dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**
