# Cycle 5 Phase 3 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve warm-tier lifecycle data in cost model, remove centralized freshness gate in favor of per-pipeline guards, deploy OAuth M2M to HF Spaces, and codify the HF-app service principal in Terraform.

**Architecture:** Four independent work items sharing one branch. D51 enriches the dbt cost model via LEFT JOIN on `workflow_cost_live`. D52 deletes the gate task and simplifies 33 pipeline `main()` functions to always run their own guard. TF-SP adds the HF-app SP to Terraform with UC grants. M2 deploys OAuth credentials (manual coordination with user).

**Tech Stack:** dbt (Databricks SQL), Python 3.10, Terraform (Databricks provider), HF Spaces deployment via `manage_space.py`

**Spec:** `docs/superpowers/specs/2026-04-09-cycle5-phase3-design.md`

---

## File Map

### D51 — dbt enrichment
| Action | File |
|--------|------|
| Create | `dbt_project/models/marts/_observability__sources.yml` |
| Modify | `dbt_project/models/marts/fct_workflow_costs.sql` |
| Modify | `dbt_project/models/marts/_marts__models.yml:1972-2016` |

### D52 — Gate removal
| Action | File |
|--------|------|
| Delete | `src/ingestion/freshness_gate.py` |
| Delete | `src/tests/test_freshness_gate.py` |
| Modify | `src/ingestion/guards.py` (remove `to_json`, `from_json`, `read_gate_result`, update docstring) |
| Modify | 33 pipeline files in `src/ingestion/` (remove `read_gate_result` branch) |
| Modify | `pyproject.toml:94` (remove entry point) |
| Modify | `terraform/modules/workflows/main.tf:65-96` (remove gate task + deps) |
| Modify | `src/tests/test_guard_conformance.py:800-839` (replace `TestMainStandaloneResolution`) |
| Modify | `src/tests/test_guards.py:14-98,432-522` (remove gate-related tests) |

### TF-SP — Service principal
| Action | File |
|--------|------|
| Modify | `terraform/modules/service_principals/main.tf` |
| Modify | `terraform/modules/service_principals/outputs.tf` |
| Modify | `terraform/modules/catalog/main.tf:216-232` (add observability grant) |
| Modify | `terraform/modules/catalog/variables.tf:28-31` (update description) |
| Modify | `terraform/environments/dev/main.tf:101` (wire SP) |

### Docs
| Action | File |
|--------|------|
| Modify | `TODO.md` |
| Modify | `ARCHITECTURE.md` (if gate is mentioned) |

---

## Task 1: D51 — Add dbt source for observability schema

**Files:**
- Create: `dbt_project/models/marts/_observability__sources.yml`

- [ ] **Step 1: Create the source YAML**

```yaml
version: 2

sources:
  - name: observability
    description: >
      Platform operational metadata schema. Tables written by runtime hooks
      (CostEstimateHook), not by dbt. Used as read-only enrichment sources.
    database: "{{ target.catalog }}"
    schema: observability
    tables:
      - name: workflow_cost_live
        description: >
          Warm-tier cost and lifecycle data written by CostEstimateHook during
          pipeline execution. Rows are pruned by fct_workflow_costs post-hook
          after cold-tier capture. Not managed by dbt.
        loaded_at_field: updated_at
        columns:
          - name: job_run_id
            description: Databricks job run identifier. Join key to billing tables.
          - name: task_key
            description: Databricks task key. Join key to job_task_run_timeline.
          - name: state
            description: "Pipeline outcome: RUNNING, COMPLETED, SKIPPED, or ERROR."
          - name: duration_seconds
            description: Wall-clock pipeline duration (excludes scheduling overhead).
          - name: entity_count
            description: Number of entities the guard found for this run.
          - name: row_count
            description: Number of rows the pipeline output.
          - name: estimated_cost_usd
            description: "Hook's per-task cost estimate (rate * duration)."
```

- [ ] **Step 2: Validate dbt can parse the source**

Run: `cd dbt_project && dbt parse --profiles-dir .`
Expected: No errors. `workflow_cost_live` appears in the manifest as a source node.

- [ ] **Step 3: Commit**

```
git add dbt_project/models/marts/_observability__sources.yml
git commit -m "feat(dbt): add observability source for workflow_cost_live (D51)"
```

---

## Task 2: D51 — Enrich fct_workflow_costs with warm-tier data

**Files:**
- Modify: `dbt_project/models/marts/fct_workflow_costs.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml:1972-2016`

- [ ] **Step 1: Add warm_tier CTE and LEFT JOIN to the model**

In `fct_workflow_costs.sql`, add a `warm_tier` CTE after the `workflow_ids` CTE (after current line 59), and update the final SELECT to LEFT JOIN it and include the new columns.

The full model after changes:

```sql
{{ config(
    materialized='table',
    liquid_clustered_by=['task_key', 'usage_date'],
    post_hook=[
        "DELETE FROM {{ this.database }}.observability.workflow_cost_live WHERE state != 'RUNNING' AND ended_at IS NOT NULL AND ended_at < (SELECT COALESCE(MAX(usage_date), DATE '1970-01-01') + INTERVAL 1 DAY FROM {{ this }})",
        "DELETE FROM {{ this.database }}.observability.workflow_cost_live WHERE state = 'RUNNING' AND started_at < CURRENT_TIMESTAMP - INTERVAL 24 HOURS"
    ]
) }}
-- fct_workflow_costs.sql
-- Gold-layer workflow cost attribution from Databricks system tables.
--
-- Joins billing usage with list prices and attributes cost per-task
-- proportionally by execution duration within each job run.
-- 90-day rolling window refreshed daily.
--
-- Warm-tier enrichment: LEFT JOINs workflow_cost_live (written by
-- CostEstimateHook) to capture lifecycle fields (duration, entity_count,
-- row_count, state, estimated_cost_usd) before the post-hook prunes them.
--
-- Post-hook cleanup removes redundant warm-tier rows from workflow_cost_live.
-- COALESCE sentinel: if table is empty (first build), threshold becomes
-- 1970-01-02 — no legitimate workflow ended in 1970, so DELETE matches zero rows.
-- Secondary cleanup: orphaned RUNNING rows >24h. This window is aligned to the
-- 2h compute task budget (CLAUDE.md) — a 24h-old RUNNING row is certainly orphaned.

WITH billing AS (
    SELECT
        usage_metadata.job_run_id AS job_run_id,
        usage_date,
        SUM(usage_quantity) AS dbu,
        SUM(
            usage_quantity
            * CAST(prices.pricing.effective_list.default AS DECIMAL(10, 4))
        ) AS cost_usd
    FROM system.billing.usage AS usage
    INNER JOIN system.billing.list_prices AS prices
        ON prices.sku_name = usage.sku_name
        AND usage.usage_end_time >= prices.price_start_time
        AND (
            prices.price_end_time IS NULL
            OR usage.usage_end_time < prices.price_end_time
        )
    WHERE
        usage.billing_origin_product = 'JOBS'
        AND usage.usage_date >= CURRENT_DATE - INTERVAL 90 DAYS
    GROUP BY 1, 2
),

tasks AS (
    SELECT
        job_run_id,
        task_key,
        SUM(execution_duration_seconds) AS execution_duration_seconds
    FROM system.lakeflow.job_task_run_timeline
    WHERE
        result_state IS NOT NULL
        AND period_start_time >= CURRENT_DATE - INTERVAL 90 DAYS
    GROUP BY job_run_id, task_key
),

workflow_ids AS (
    SELECT task_key, workflow_id
    FROM {{ ref('task_workflow_mapping') }}
),

warm_tier AS (
    SELECT
        job_run_id,
        task_key,
        duration_seconds,
        entity_count,
        row_count,
        state AS pipeline_state,
        estimated_cost_usd
    FROM {{ source('observability', 'workflow_cost_live') }}
    WHERE state != 'RUNNING'
)

SELECT
    tasks.task_key,
    billing.usage_date,
    CAST(billing.job_run_id AS BIGINT) AS job_run_id,
    wcl.workflow_id,
    CAST(ROUND(
        billing.dbu * (
            tasks.execution_duration_seconds
            / NULLIF(SUM(tasks.execution_duration_seconds)
                OVER (PARTITION BY billing.job_run_id), 0)
        ),
        4
    ) AS DECIMAL(10, 4)) AS attributed_dbu,
    CAST(ROUND(
        billing.cost_usd * (
            tasks.execution_duration_seconds
            / NULLIF(SUM(tasks.execution_duration_seconds)
                OVER (PARTITION BY billing.job_run_id), 0)
        ),
        4
    ) AS DECIMAL(10, 4)) AS attributed_cost_usd,
    wt.duration_seconds,
    wt.entity_count,
    wt.row_count,
    wt.pipeline_state,
    wt.estimated_cost_usd
FROM billing
INNER JOIN tasks ON billing.job_run_id = tasks.job_run_id
LEFT JOIN workflow_ids AS wcl
    ON wcl.task_key = tasks.task_key
LEFT JOIN warm_tier AS wt
    ON CAST(billing.job_run_id AS BIGINT) = wt.job_run_id
    AND tasks.task_key = wt.task_key
```

- [ ] **Step 2: Update the contract in `_marts__models.yml`**

Add the 5 new columns after the `attributed_cost_usd` column (after line ~2016):

```yaml
      - name: duration_seconds
        data_type: int
        description: >
          Pipeline wall-clock duration in seconds from CostEstimateHook.
          More accurate than execution_duration_seconds (excludes scheduling
          overhead). NULL for runs predating hook deployment.
      - name: entity_count
        data_type: int
        description: >
          Number of entities the guard found for this run (e.g., match count).
          NULL for runs predating hook deployment or when hook failed.
      - name: row_count
        data_type: int
        description: >
          Number of rows the pipeline wrote. NULL for runs predating hook
          deployment, SKIPPED runs (always 0), or when hook failed.
      - name: pipeline_state
        data_type: string
        description: >
          Pipeline outcome from CostEstimateHook: COMPLETED, SKIPPED, or ERROR.
          NULL for runs predating hook deployment.
      - name: estimated_cost_usd
        data_type: decimal(10,4)
        description: >
          Hook's per-task cost estimate (rate_usd_per_hour * duration).
          Independent of billing attribution — useful for comparing hook
          estimates against actual attributed_cost_usd. NULL for older runs.
```

- [ ] **Step 3: Verify dbt compiles cleanly**

Run: `cd dbt_project && dbt compile --select fct_workflow_costs --profiles-dir .`
Expected: No errors. Compiled SQL shows the `warm_tier` CTE and LEFT JOIN.

- [ ] **Step 4: Commit**

```
git add dbt_project/models/marts/fct_workflow_costs.sql dbt_project/models/marts/_marts__models.yml
git commit -m "feat(dbt): enrich fct_workflow_costs with warm-tier lifecycle data (D51)"
```

---

## Task 3: D52 — Remove gate infrastructure from guards.py

**Files:**
- Modify: `src/ingestion/guards.py`

- [ ] **Step 1: Remove `to_json`, `from_json`, and `read_gate_result`**

In `guards.py`:

1. Remove `import json` from line 13 (only used by `to_json`/`from_json`).
2. Remove the `to_json` method (lines 42-51).
3. Remove the `from_json` classmethod (lines 53-57).
4. Remove the `read_gate_result` function (lines 112-141).
5. Update the module docstring (lines 0-8) to remove references to the freshness gate task:

```python
"""Port/adapter infrastructure for pipeline skip guards.

Each workflow exposes a :class:`SkipGuard` adapter whose ``check()``
method returns a :class:`FilterResult` describing whether the workflow
has new work and how to chunk it for fan-out.

Each pipeline's ``main()`` calls its guard's ``check()`` at startup
and raises ``WorkflowSkippedError`` when ``count == 0``.
"""
```

- [ ] **Step 2: Run existing guard tests (expect some failures)**

Run: `uv run pytest src/tests/test_guards.py -v -x`
Expected: `TestFilterResult` JSON tests and `TestReadGateResult` fail (removed code).

- [ ] **Step 3: Commit**

```
git add src/ingestion/guards.py
git commit -m "refactor: remove gate serialization and read_gate_result from guards.py (D52)"
```

---

## Task 4: D52 — Delete freshness_gate.py and entry point

**Files:**
- Delete: `src/ingestion/freshness_gate.py`
- Delete: `src/tests/test_freshness_gate.py`
- Modify: `pyproject.toml:94`

- [ ] **Step 1: Delete the gate orchestrator and its tests**

```bash
rm src/ingestion/freshness_gate.py src/tests/test_freshness_gate.py
```

- [ ] **Step 2: Remove the entry point from pyproject.toml**

In `pyproject.toml`, remove line 94:
```
freshness_gate = "ingestion.freshness_gate:main"
```

- [ ] **Step 3: Commit**

```
git add -u src/ingestion/freshness_gate.py src/tests/test_freshness_gate.py pyproject.toml
git commit -m "refactor: delete freshness_gate orchestrator and entry point (D52)"
```

---

## Task 5: D52 — Simplify pipeline main() functions

**Files:**
- Modify: 31 pipeline files in `src/ingestion/` (see list below)

Every pipeline `main()` currently has this pattern:

```python
    from ingestion.guards import read_gate_result

    filter_result = read_gate_result("wf-xxx")
    if filter_result is None:
        filter_result = skip_guard.check(spark, args.catalog, args.schema)
```

Replace with:

```python
    filter_result = skip_guard.check(spark, args.catalog, args.schema)
```

**Special case — `defcon_lite.py`** has two guards. Current (lines 101-113):

```python
    from ingestion.guards import read_gate_result

    filter_360 = read_gate_result("wf-defcon")
    if filter_360 is None:
        from ingestion.defcon_lite_360 import skip_guard as guard_360

        filter_360 = guard_360.check(spark, args.catalog, args.schema)

    filter_tracking = read_gate_result("wf-defcon-tracking")
    if filter_tracking is None:
        from ingestion.defcon_lite_tracking import skip_guard as guard_tracking

        filter_tracking = guard_tracking.check(spark, args.catalog, args.schema)
```

Replace with:

```python
    from ingestion.defcon_lite_360 import skip_guard as guard_360
    from ingestion.defcon_lite_tracking import skip_guard as guard_tracking

    filter_360 = guard_360.check(spark, args.catalog, args.schema)
    filter_tracking = guard_tracking.check(spark, args.catalog, args.schema)
```

**Special case — `statsbomb_backfill_extra.py`, `statsbomb_backfill_360.py`, `idsse_events.py`** have top-level `from ingestion.guards import FilterResult, read_gate_result`. Change to `from ingestion.guards import FilterResult` (keep `FilterResult` if used elsewhere in the file, remove entirely if only `read_gate_result` was needed).

- [ ] **Step 1: Apply the standard pattern to all 29 standard pipelines**

Standard files (each has one `main()` with the 4-line pattern → 1-line replacement):

| File | `read_gate_result` import line | Block start | Workflow ID |
|------|-------------------------------|-------------|-------------|
| `pitch_control_batch.py` | 303 | 305-307 | `wf-pitch-control` |
| `off_ball_xt.py` | 343 | 345-347 | `wf-off-ball-xt` |
| `elastic_sync.py` | 326 | 328-330 | `wf-elastic-sync` |
| `pausa.py` | 292 | 294-296 | `wf-obso-pausa` |
| `line_breaking.py` | 144 | 146-148 | `wf-line-breaking` |
| `formations_shape_graph.py` | 451 | 453-455 | `wf-formations-sg` |
| `spadl_vaep.py` | 517 | 519-521 | `wf-vaep` |
| `player_embeddings_v1.py` | 408 | 410-412 | `wf-football2vec-v1` |
| `xg_model.py` | 256 | 258-260 | `wf-xg` |
| `xg_model_v2.py` | 356 | 358-360 | `wf-xg-v2` |
| `expected_threat.py` | 245 | 247-249 | `wf-expected-threat` |
| `export_embeddings_training_data.py` | 338 | 340-342 | `wf-export-training` |
| `prepare_360_training_data.py` | 551 | 553-555 | `wf-360-training` |
| `entity_resolution.py` | 193 | 195-197 | `wf-entity-resolution` |
| `statsbomb.py` | 706 | 708-710 | `wf-statsbomb` |
| `metrica.py` | 93 | 95-97 | `wf-metrica` |
| `wyscout.py` | 567 | 569-571 | `wf-wyscout` |
| `idsse.py` | 410 | 412-414 | `wf-idsse` |
| `skillcorner.py` | 273 | 275-277 | `wf-skillcorner` |
| `import_obso_results.py` | 269 | 271-273 | `wf-obso` |
| `import_psxg_predictions.py` | 192 | 194-196 | `wf-psxg` |
| `import_space_creation.py` | 195 | 197-199 | `wf-import-space-creation` |
| `tracking_metadata.py` | 397 | 399-401 | `wf-tracking-metadata` |
| `model_validation.py` | 462 | 464-466 | `wf-model-validation` |
| `sync_hf_costs.py` | 280 | 282-285 | `wf-sync-hf-costs` |
| `hf_sync.py` | 164 | 166-168 | `wf-hf-sync` |

Files with **two `main()` functions** (two blocks each):

| File | Import line | Block 1 | Block 2 |
|------|-------------|---------|---------|
| `formations_efpi.py` | 329, 349 | 331-333 | 351-353 |
| `player_embeddings_v2.py` | 304, 323 | 306-308 | 325-327 |

Files with **top-level import** (remove `read_gate_result` from import statement):

| File | Top-level import line |
|------|-----------------------|
| `statsbomb_backfill_extra.py` | 15 |
| `statsbomb_backfill_360.py` | 15 |
| `idsse_events.py` | 15 |

For each standard file, the edit replaces:
```python
    from ingestion.guards import read_gate_result

    filter_result = read_gate_result("wf-xxx")
    if filter_result is None:
        filter_result = skip_guard.check(spark, args.catalog, args.schema)
```
with:
```python
    filter_result = skip_guard.check(spark, args.catalog, args.schema)
```

- [ ] **Step 2: Apply the defcon_lite.py special case**

Replace lines 101-113 as shown above.

- [ ] **Step 3: Apply the top-level import special cases**

For `statsbomb_backfill_extra.py` (line 15), `statsbomb_backfill_360.py` (line 15), `idsse_events.py` (line 15): remove `, read_gate_result` from the import. Then replace the 4-line block with the 1-line direct call, same as standard pattern.

- [ ] **Step 4: Run ruff to catch any unused import warnings**

Run: `uv run ruff check src/ingestion/ --select F401`
Expected: No unused import warnings for `read_gate_result`.

- [ ] **Step 5: Commit**

```
git add src/ingestion/
git commit -m "refactor: simplify 33 pipeline main() to call skip_guard.check() directly (D52)"
```

---

## Task 6: D52 — Update tests

**Files:**
- Modify: `src/tests/test_guards.py`
- Modify: `src/tests/test_guard_conformance.py`

- [ ] **Step 1: Remove gate-related tests from test_guards.py**

Remove:
1. `TestFilterResult.test_json_round_trip` (line 51)
2. `TestFilterResult.test_json_round_trip_skip` (line 62)
3. `TestFilterResult.test_manual_json_interop` (line 68)
4. `TestFilterResult.test_manual_json_construction` (line 84)
5. The entire `TestReadGateResult` class (lines 432-522)

Keep all other `TestFilterResult` tests (construction, count, chunks, metadata) and all `TestFindNewIds` tests.

- [ ] **Step 2: Replace `TestMainStandaloneResolution` in test_guard_conformance.py**

Replace the class at lines 800-839 with a new `TestDirectGuardCall` that verifies every `main()` calls `skip_guard.check()` directly (and does NOT reference `read_gate_result`):

```python
class TestDirectGuardCall:
    """main() must call skip_guard.check() directly — no gate indirection.

    After D52, the centralized freshness gate is removed. Each pipeline's
    main() calls its guard's check() at startup. read_gate_result must not
    appear anywhere in main().
    """

    _EXEMPT: ClassVar[set[str]] = {
        "ingestion.defcon_lite_360",
        "ingestion.defcon_lite_tracking",
    }

    def test_main_calls_skip_guard_directly(self) -> None:
        """main() must call skip_guard.check and must NOT call read_gate_result."""
        failures: list[str] = []
        for module_path in _GUARD_MODULES:
            if module_path in self._EXEMPT:
                continue

            mod = importlib.import_module(module_path)
            if not hasattr(mod, "main"):
                continue

            source_file = inspect.getfile(mod)
            source = Path(source_file).read_text(encoding="utf-8")
            tree = ast.parse(source)

            main_fns = [
                node for node in ast.walk(tree)
                if isinstance(node, ast.FunctionDef) and node.name.startswith("main")
            ]

            for main_fn in main_fns:
                has_guard = _ast_has_name_or_attr(main_fn, "skip_guard")
                has_gate = _ast_has_name_or_attr(main_fn, "read_gate_result")

                if not has_guard:
                    failures.append(f"{module_path}.{main_fn.name}() does not call skip_guard.check()")
                if has_gate:
                    failures.append(f"{module_path}.{main_fn.name}() still references read_gate_result (removed in D52)")

        assert not failures, "main() guard call conformance failures:\n" + "\n".join(failures)
```

Note: `defcon_lite.py` is not in `_GUARD_MODULES` (only its sub-guards `defcon_lite_360` and `defcon_lite_tracking` are, and they're EXEMPT with no `main()`). So this test does not cover defcon_lite's dual-guard orchestrator — that's validated by the existing `TestEarlyExitBehavior` which exercises actual `run_pipeline` calls.

- [ ] **Step 3: Run the full test suite**

Run: `uv run pytest src/tests/test_guards.py src/tests/test_guard_conformance.py -v`
Expected: All tests pass. No references to deleted code.

- [ ] **Step 4: Run the complete test suite**

Run: `uv run pytest src/tests/ -v`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```
git add src/tests/test_guards.py src/tests/test_guard_conformance.py
git commit -m "test: update guard conformance tests for gate removal (D52)"
```

---

## Task 7: D52 — Update Terraform DAG

**Files:**
- Modify: `terraform/modules/workflows/main.tf`

- [ ] **Step 1: Remove the freshness_gate task block**

Delete lines 65-87 (the `task { task_key = "freshness_gate" ... }` block and the D40c TODO comment that follows it).

- [ ] **Step 2: Remove `depends_on { task_key = "freshness_gate" }` from all tasks**

Remove the `depends_on` block referencing `freshness_gate` from these 5 tasks:
- `ingest_statsbomb` (line 96)
- `ingest_metrica` (line 120)
- `ingest_wyscout` (line 143)
- `ingest_idsse` (line 169)
- `ingest_skillcorner` (line 192)

Each of these tasks has `depends_on { task_key = "freshness_gate" }` as its first dependency. Remove only that block — keep any other `depends_on` blocks the task may have.

After removal, these 5 ingest tasks become DAG roots (no upstream dependencies), which is correct — they run their own guards at startup.

- [ ] **Step 3: Validate Terraform**

Run: `cd terraform/environments/dev && terraform fmt -recursive ../.. && terraform validate`
Expected: No errors. `terraform fmt` clean.

- [ ] **Step 4: Commit**

```
git add terraform/modules/workflows/main.tf
git commit -m "infra: remove freshness_gate task and dependencies from DAG (D52)"
```

---

## Task 8: TF-SP — Add HF-app service principal to Terraform

**Files:**
- Modify: `terraform/modules/service_principals/main.tf`
- Modify: `terraform/modules/service_principals/outputs.tf`
- Modify: `terraform/modules/catalog/main.tf` (add observability grant)
- Modify: `terraform/modules/catalog/variables.tf:28-31`
- Modify: `terraform/environments/dev/main.tf:101`

- [ ] **Step 1: Add the HF-app SP resource**

In `terraform/modules/service_principals/main.tf`, after the `terraform_ci` SP (after line 87), add:

```hcl
# ── HF Spaces App (OAuth M2M) ───────────────────────────────────────────────
# Taipy app on HF Spaces authenticates to Databricks and Lakebase via OAuth
# M2M (no expiring PAT). Read-only: SELECT on gold + observability schemas.

resource "databricks_service_principal" "hf_app" {
  display_name = "luxury-lakehouse-hf-app-v2-${var.environment}"
}
```

- [ ] **Step 2: Add the output**

In `terraform/modules/service_principals/outputs.tf`, after line 13, add:

```hcl
output "hf_app_sp_application_id" {
  description = "Application ID of the HF Spaces app service principal"
  value       = databricks_service_principal.hf_app.application_id
}
```

- [ ] **Step 3: Update catalog variable description**

In `terraform/modules/catalog/variables.tf`, update the `app_sp_application_id` variable description (line 29) from `"Application ID of the Streamlit app service principal (empty = skip grants)"` to:

```hcl
variable "app_sp_application_id" {
  description = "Application ID of the HF Spaces app service principal (empty = skip grants)"
  type        = string
  default     = ""
}
```

- [ ] **Step 4: Add observability schema grant for the app SP**

In `terraform/modules/catalog/main.tf`, after the `app_sp_gold_schema` grant (after line 232), add:

```hcl
resource "databricks_grant" "app_sp_observability_schema" {
  count = var.app_sp_application_id != "" ? 1 : 0

  schema = "${var.catalog_name}.${databricks_schema.observability.name}"

  principal  = var.app_sp_application_id
  privileges = ["USE_SCHEMA", "SELECT"]
}
```

- [ ] **Step 5: Wire the SP in dev/main.tf**

In `terraform/environments/dev/main.tf`, change line 101 from:
```hcl
  app_sp_application_id       = "" # Databricks App deprecated — Streamlit runs on HF Spaces
```
to:
```hcl
  app_sp_application_id       = module.service_principals.hf_app_sp_application_id
```

- [ ] **Step 6: Validate Terraform**

Run: `cd terraform/environments/dev && terraform fmt -recursive ../.. && terraform validate`
Expected: No errors.

- [ ] **Step 7: Commit**

```
git add terraform/modules/service_principals/ terraform/modules/catalog/ terraform/environments/dev/main.tf
git commit -m "infra: add HF-app service principal to Terraform with UC grants (TF-SP)"
```

**Note:** Before `terraform apply`, the existing SP must be imported:
```bash
cd terraform/environments/dev
terraform import 'module.service_principals.databricks_service_principal.hf_app' 1a1dbf08-df56-48de-b97a-276b2a4232d8
```

---

## Task 9: Update TODO.md and documentation

**Files:**
- Modify: `TODO.md`
- Modify: `ARCHITECTURE.md` (if freshness gate is mentioned)

- [ ] **Step 1: Update TODO.md**

1. Add D53 (Taipy wiring) as the first item in the On Deck table:

```markdown
| D53 | Taipy Workflows page — surface enriched cost columns | Dunkin' | D51 follow-up | Wire `duration_seconds`, `entity_count`, `row_count`, `pipeline_state` from enriched `fct_workflow_costs` into AI/ML Workflows page stat cards and/or detail drilldown. Depends on D51. |
```

2. Remove D51 row (completed).
3. Remove D52 row (completed).
4. Remove D40c row (superseded by D52 — each pipeline decides for itself).
5. Remove M2 row (completed).
6. Update M1 row to note it's superseded: change Notes to `**Superseded by M2.** OAuth M2M credentials deployed — PAT rotation no longer needed. Keep PAT removal confirmation as cleanup item.`
7. Update the "Last updated" line at the top.

- [ ] **Step 2: Check ARCHITECTURE.md for gate references**

Search `ARCHITECTURE.md` for "freshness_gate" or "freshness gate". If found, update to reflect the new guard-as-wrapper architecture (each pipeline runs its own guard at startup, no centralized gate task).

- [ ] **Step 3: Commit**

```
git add TODO.md ARCHITECTURE.md
git commit -m "docs: update TODO (D53, close D51/D52/D40c/M2) and architecture for gate removal"
```

---

## Task 10: M2 — OAuth M2M deployment (user-coordinated)

This task is a manual operational procedure. **Do not execute until user coordinates timing with the UI session.**

- [ ] **Step 1: User generates OAuth client secret**

User navigates to Databricks workspace → Settings → Service Principals → `luxury-lakehouse-hf-app-v2-dev` → Secrets → Generate new secret.

- [ ] **Step 2: User exports credentials locally**

```bash
export DATABRICKS_CLIENT_ID=1a1dbf08-df56-48de-b97a-276b2a4232d8
export DATABRICKS_CLIENT_SECRET=<secret-from-step-1>
```

- [ ] **Step 3: Terraform import + apply**

```bash
cd terraform/environments/dev
terraform import 'module.service_principals.databricks_service_principal.hf_app' 1a1dbf08-df56-48de-b97a-276b2a4232d8
terraform apply
```

- [ ] **Step 4: Deploy staging with OAuth**

```bash
python scripts/manage_space.py deploy staging
```

Wait for staging to reach RUNNING. Verify the logs show `Auth method: OAuth M2M`.

- [ ] **Step 5: Rebuild staging and verify**

```bash
python scripts/manage_space.py rebuild staging
```

Confirm staging is RUNNING and pages load correctly.

- [ ] **Step 6: Deploy production with OAuth**

```bash
python scripts/manage_space.py deploy production
```

Wait for production to reach RUNNING. Verify OAuth auth.

- [ ] **Step 7: Rebuild production and verify**

```bash
python scripts/manage_space.py rebuild production
```

Confirm production is RUNNING and pages load correctly.

- [ ] **Step 8: Remove PAT from both Spaces**

Via HF Space settings UI, delete the `DATABRICKS_TOKEN` secret from both:
- `luxury-lakehouse/soccer-analytics-app`
- `luxury-lakehouse/staging`

- [ ] **Step 9: Update docs**

Update `docs/decisions/secrets-inventory.md` to reflect OAuth as primary auth.
Update `docs/decisions/pat-rotation-runbook.md` with deprecation note.

- [ ] **Step 10: Final commit**

```
git add docs/decisions/
git commit -m "docs: update secrets inventory and PAT runbook for OAuth M2M (M2)"
```

---

## Task 11: Final verification

- [ ] **Step 1: Run full lint + type check + test suite**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/
uv run pytest src/tests/ -v
```

Expected: All 4 checks pass with zero violations.

- [ ] **Step 2: Terraform plan**

```bash
cd terraform/environments/dev && terraform plan
```

Expected: Clean plan showing only the gate task removal and SP/grant additions.

- [ ] **Step 3: Verify no stale references**

```bash
grep -r "freshness_gate" src/ terraform/ --include="*.py" --include="*.tf"
grep -r "read_gate_result" src/ --include="*.py"
```

Expected: Zero matches (except possibly in docs/ or this plan file).
