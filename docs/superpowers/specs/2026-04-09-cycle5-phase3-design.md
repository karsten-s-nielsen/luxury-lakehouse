# Cycle 5 Phase 3 — Cost Enrichment, Gate Removal, OAuth M2M

**Date:** 2026-04-09
**Scope:** D51, D52, M2, TF-SP, plus new TODO item for Taipy wiring

---

## Overview

Four work items that collectively simplify the pipeline DAG, preserve operational
data that was being lost, and complete the OAuth M2M migration:

| # | Task | Size | Summary |
|---|------|------|---------|
| D51 | Preserve warm-tier lifecycle data | Wicked | LEFT JOIN `workflow_cost_live` into `fct_workflow_costs` before post-hook prune |
| D52 | Remove centralized freshness gate | Wicked | Delete gate task, each pipeline runs its own guard at startup |
| M2 | Deploy OAuth M2M credentials | Dunkin' | Replace PAT with non-expiring OAuth M2M on both HF Spaces |
| TF-SP | Add HF-app SP to Terraform | Dunkin' | Codify existing SP in `service_principals/main.tf` |

**Explicitly excluded:** D40c (condition_task — made unnecessary by D52), D40d
(for_each_task fan-out — independent future work).

**Deferred to future cycle:** Taipy wiring of enriched cost columns (another session
is handling UI updates; add as TODO item).

---

## D51 — Preserve Warm-Tier Lifecycle Data in `fct_workflow_costs`

### Problem

`CostEstimateHook` writes `duration_seconds`, `row_count`, `entity_count`, `state`,
and `estimated_cost_usd` to `workflow_cost_live` (warm tier). The dbt
`fct_workflow_costs` model never reads these columns — it only joins billing system
tables with `job_task_run_timeline`. The post-hook then prunes the warm-tier rows.
Result: lifecycle fields are permanently lost after each `dbt build`.

### Solution

Add a LEFT JOIN on `workflow_cost_live` in the model SELECT, keyed on
`job_run_id` + `task_key`. dbt semantics guarantee the SELECT materializes before
post-hooks fire, so warm-tier data is captured before cleanup.

### New columns

| Column | Source | Data Type | Nullable | Description |
|--------|--------|-----------|----------|-------------|
| `duration_seconds` | warm tier | `int` | Yes | Pipeline wall-clock time (excludes scheduling overhead) |
| `entity_count` | warm tier | `int` | Yes | Number of entities the guard found for this run |
| `row_count` | warm tier | `int` | Yes | Number of rows the pipeline output |
| `pipeline_state` | warm tier `state` | `string` | Yes | COMPLETED / SKIPPED / ERROR |
| `estimated_cost_usd` | warm tier | `decimal(10,4)` | Yes | Hook's per-task cost estimate (vs billing proportional attribution) |

All nullable because warm-tier data may not exist for runs predating the
`CostEstimateHook` deployment or for runs where the hook failed silently.

`state` is aliased to `pipeline_state` to avoid ambiguity. `workflow_id` resolution
stays on the `task_workflow_mapping` seed (static, always present).

### dbt changes

1. **Add dbt source declaration** for `workflow_cost_live` in the observability
   schema so the model uses `{{ source('observability', 'workflow_cost_live') }}`
   instead of hardcoded table names.
2. **Add CTE** in `fct_workflow_costs.sql` joining warm-tier data:
   ```sql
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
   ```
   Filter out `RUNNING` rows — they represent in-progress work with no final data.
3. **LEFT JOIN** `warm_tier` in the final SELECT on `job_run_id` + `task_key`.
4. **Update contract** in `_marts__models.yml` with all 5 new columns, explicit
   `data_type`, and descriptions.

### Post-hook interaction

The post-hook DELETE condition is:
```sql
WHERE state != 'RUNNING' AND ended_at < (MAX(usage_date) + 1 DAY)
```

The SELECT (which now includes the LEFT JOIN) runs first and materializes the table.
Then the post-hook fires and removes warm-tier rows that are now captured in the cold
tier. No ordering change needed.

### Warm-tier join semantics

A single warm-tier row may match multiple cold-tier rows (one per `usage_date` when a
job run spans midnight). The lifecycle fields are the same across all dates for a
given run, so the LEFT JOIN produces correct denormalized results.

---

## D52 — Remove Centralized Freshness Gate

### Problem

The freshness gate (`freshness_gate.py`) runs as the DAG root task, executing all 33
guards sequentially (~170s) via `ThreadPoolExecutor(max_workers=4)` which provides
zero parallelism on Databricks serverless (Spark Connect serializes queries within a
session — confirmed by monotonic log timestamps with no overlap).

### Root cause (confirmed)

Spark Connect on serverless uses a single gRPC channel with server-side FIFO
scheduling. SPARK-49544 (coarse-grained `executionsLock` in
`SparkConnectExecutionManager`) was fixed in Spark 4.0; serverless runs 3.5.x.
Code-level alternatives (multiple SparkSessions, Statement Execution API, asyncio)
are either unproven on serverless or add undesirable dependencies.

### Solution — guard-as-wrapper

Delete the centralized gate entirely. Each pipeline runs its own guard at startup
(the existing fallback path). The `@workflow` decorator already handles
`WorkflowSkippedError` → `CostEstimateHook.on_skip` → SKIPPED cost record. All 33
pipelines already raise `WorkflowSkippedError` on `count==0` (enforced by 2
conformance test classes: AST + behavioral).

### Performance impact

| Metric | With gate | Without gate |
|--------|-----------|--------------|
| Critical path to first compute | 170s gate + chain of cold starts | Chain of cold starts + ~5s guard each |
| Longest chain (6 deep, all skip) | 170 + 6×5 = ~200s | 6×9 = ~54s |
| Extra cold starts for skipped tasks | 0 (gate decides) | ~15-20 (minimal cost per user confirmation) |

Net improvement: ~150s on the critical path.

### Files deleted

| File | Reason |
|------|--------|
| `src/ingestion/freshness_gate.py` | Gate orchestrator — no longer needed |
| `src/tests/test_freshness_gate.py` | Gate-specific tests |

### Files modified

| File | Change |
|------|--------|
| 33 pipeline `main()` functions | Remove `read_gate_result` import + branch → always `skip_guard.check()` |
| `src/ingestion/guards.py` | Remove `read_gate_result()`, `FilterResult.to_json()`, `FilterResult.from_json()` |
| `pyproject.toml` | Remove `freshness_gate` entry point |
| `terraform/modules/workflows/main.tf` | Remove `freshness_gate` task block, D40c TODO, all `depends_on { task_key = "freshness_gate" }` |
| `src/tests/test_guard_conformance.py` | Remove `TestTaskValuePropagation` class (validates gate task value writes). Add `TestDirectGuardCall` AST test: every `main()` calls `skip_guard.check()` without `read_gate_result` branch |
| `src/tests/test_guards.py` | Remove `read_gate_result` and `to_json/from_json` tests |

### What stays unchanged

- All 33 guard modules and `_GUARD_MODULES` registry (used by conformance tests)
- `FilterResult` dataclass (used by guards and pipelines)
- `FilterResult.chunks` field (needed for future D40d fan-out)
- `find_new_ids()` helper
- `SkipGuard` protocol
- `@workflow` decorator + `CostEstimateHook` lifecycle hooks
- All pipeline-to-pipeline dependencies in the Terraform DAG

### Impact on related items

- **D40c** (condition_task): Made unnecessary — each pipeline decides for itself.
- **D40d** (for_each_task fan-out): Unaffected — will read chunks from guard's
  `FilterResult` directly.
- **D52 in TODO:** Close as resolved.
- **D40c in TODO:** Close as unnecessary (note: superseded by D52 redesign).

---

## M2 — Deploy OAuth M2M Credentials to HF Spaces

### Prerequisites already in place

| Component | Status | Location |
|-----------|--------|----------|
| HF Space secrets schema | Done | `scripts/manage_space.py` (`OAUTH_SECRETS` dict) |
| Runtime dual-auth parsing | Done | `hf_taipy_app/src/config.py` (`AppSettings`) |
| Lakebase PG role for HF-app SP | Done | `scripts/setup_lakebase_roles.py` |
| SP exists in workspace | Done | `1a1dbf08-df56-48de-b97a-276b2a4232d8` |

### Remaining work

**Code (TF-SP):** Add the HF-app SP to Terraform — see next section.

**Manual operational steps (M2):**

1. User retrieves OAuth client secret from Databricks workspace
   (Settings → Service Principals → `luxury-lakehouse-hf-app-v2-dev` → Secrets → Generate)
2. User exports `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` locally
3. Deploy staging: `python scripts/manage_space.py deploy staging`
4. Verify staging RUNNING with OAuth auth
5. Deploy production: `python scripts/manage_space.py deploy production`
6. Verify production RUNNING
7. Remove `DATABRICKS_TOKEN` PAT from both Spaces

**Coordination:** Another session is doing UI updates. M2 deployment (steps 3-7)
must wait until UI work is deployed and verified. User will coordinate timing.
Terraform + code changes can land first.

### Post-M2 cleanup

- M1 (PAT rotation) becomes unnecessary — close as superseded by M2
- Update `docs/decisions/secrets-inventory.md` to reflect OAuth as primary auth
- Update `docs/decisions/pat-rotation-runbook.md` with deprecation note

---

## TF-SP — Add HF-App Service Principal to Terraform

### Change

Add a third SP resource to `terraform/modules/service_principals/main.tf`:

```hcl
resource "databricks_service_principal" "hf_app" {
  display_name = "luxury-lakehouse-hf-app-v2-${var.environment}"
}
```

### Import

The SP already exists in the workspace. Before first `terraform apply`:

```bash
terraform import 'module.service_principals.databricks_service_principal.hf_app' \
  1a1dbf08-df56-48de-b97a-276b2a4232d8
```

### Grants

Add UC grants in `dev/main.tf` for the HF-app SP:
- `SELECT` on `soccer_analytics.dev_gold` (read analytics data)
- `SELECT` on `soccer_analytics.observability` (read cost/workflow data)

### Output

Add `hf_app_sp_application_id` to `outputs.tf`.

---

## TODO Updates

### New item — top of On Deck

| # | Task | Size | Notes |
|---|------|------|-------|
| D53 | Taipy Workflows page — surface enriched cost columns | Dunkin' | Wire `duration_seconds`, `entity_count`, `row_count`, `pipeline_state` from enriched `fct_workflow_costs` into AI/ML Workflows page stat cards and/or detail drilldown. Depends on D51. |

### Items to close/update

| # | Action | Reason |
|---|--------|--------|
| D52 | Close as resolved | Gate removed, guard-as-wrapper implemented |
| D40c | Close as unnecessary | Superseded by D52 — each pipeline decides for itself, no task values to gate on |
| M1 | Close as superseded | OAuth M2M (M2) doesn't expire, PAT rotation no longer needed |
| M2 | Close as resolved | OAuth deployed to both Spaces |

---

## Testing Strategy

### D51

- `dbt build --select fct_workflow_costs` — verify new columns populated for recent
  runs (join hit) and NULL for older runs (join miss)
- Contract validation: `dbt build` fails if column types mismatch
- Verify post-hook still prunes warm-tier rows correctly

### D52

- `uv run pytest src/tests/test_guard_conformance.py -v` — all conformance tests pass
- `uv run pytest src/tests/ -v` — full test suite passes
- `terraform plan` — clean plan showing gate task removal
- E2E: trigger Databricks job run, verify pipelines run their own guards and
  skip/proceed correctly

### M2

- Staging deploy + rebuild → verify RUNNING with OAuth
- Production deploy + rebuild → verify RUNNING with OAuth
- Confirm PAT removed from both Spaces

### TF-SP

- `terraform plan` after import — no diff (SP already exists with correct config)
- `terraform apply` — grants applied
