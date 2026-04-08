# D40 — Adaptive Pipeline Fan-Out Design

**Date**: 2026-04-07
**Status**: Draft
**Scope**: Strategies A–E from cold-start investigation, port/adapter freshness gate, HF task consolidation, fan-out extension point

---

## Problem Statement

The Databricks ingestion job (32 tasks, 8 environment keys) has excessive wall-clock time driven by cold-start overhead. Each serverless session in standard mode pays 4–6 minutes of infrastructure startup before Python code runs. On a typical run where only 1–2 data sources have new data, 20+ tasks launch, run their skip guard, find nothing to do, and exit — each having paid the full cold-start penalty. HF-related tasks are split across 7 separate Databricks tasks with 3 environment keys despite near-identical dependencies.

**Root causes identified:**
1. No `performance_target` set on the job — defaults to standard mode (4–6 min cold start vs 20–30 sec in performance-optimized mode)
2. No centralized freshness check — every task pays cold start just to discover there's no work
3. HF tasks fragmented across 7 tasks / 3 environments — 6 minutes of overhead for I/O-bound operations
4. Wheel already on UC Volume (Strategy E verified as already in place — no action needed)

## Sequencing

**Sequential, not parallel.** Track 1 merges to main first. Its timing measurements establish the baseline that validates Track 2's design.

| Track | Scope | Dependencies |
|-------|-------|-------------|
| Track 1 | Terraform: perf-optimized mode + HF env consolidation | None |
| Track 2 | Architecture: SkipGuard protocol, freshness gate, HF task consolidation, fan-out extension point | Branches from main after Track 1 merges |
| Follow-up | Selective fan-out activation per pipeline | Monitor post-Track-2 runtimes |

---

## Track 1 — Terraform Infrastructure (Strategies A + C)

### Strategy A: Performance-Optimized Mode

Add `performance_target = "PERFORMANCE_OPTIMIZED"` to the `databricks_job.data_ingestion` resource in `terraform/modules/workflows/main.tf`.

**Expected impact**: Cold starts drop from 4–6 minutes to 20–30 seconds per concurrent serverless session. For a 32-task job with ~5 serial cold-start transitions in the critical path, this saves ~15–25 minutes per run.

**Cost trade-off**: Standard mode advertises "up to 70% fewer DBUs." However, cold-start time is billed at DBU/second. For short tasks (many finish in <60s of compute), the idle cold-start DBU cost in standard mode likely exceeds the per-second savings. Net cost may decrease or stay neutral.

**Fallback**: If the Terraform provider does not yet expose `performance_target` on `databricks_job`, set it via the Jobs API (`PATCH /api/2.1/jobs/update`) or UI as a stopgap.

### Strategy C: Consolidate HF Environments

Merge `hf`, `hf-readonly`, and `hf-sync` into a single `hf` environment key.

| Current Key | Extra Deps | Tasks |
|---|---|---|
| `hf` | `huggingface_hub>=0.25.0` | 3 (exports) |
| `hf-readonly` | `huggingface_hub>=0.25.0` | 3 (imports) |
| `hf-sync` | `huggingface_hub>=0.25.0`, `pyyaml>=6.0` | 1 (cost sync) |

`pyyaml` is already a transitive dependency of the wheel (core dep in `pyproject.toml`). The read/write distinction was semantic — `dbutils.secrets` access is controlled by secret scope at runtime, not by environment key. Merging reduces environment keys from 8 to 6.

**Changes:**
- `terraform/modules/workflows/main.tf`: Delete `hf-readonly` and `hf-sync` environment blocks. Update 4 task `environment_key` references to `hf`. Remove `pyyaml` from consolidated `hf` spec (already in wheel).

### Strategy E: Wheel from UC Volume

**Already in place.** `var.wheel_path` resolves to `/Volumes/soccer_analytics/bronze/libs/luxury_lakehouse-0.1.0-py3-none-any.whl`. No action needed.

### Measurement

After applying Track 1, trigger one manual job run and query:

```sql
-- Compare execution_duration_seconds (includes cold start) vs workflow_cost_live.duration_seconds (app-measured)
SELECT
    t.task_key,
    t.execution_duration_seconds AS total_with_coldstart,
    w.duration_seconds AS app_measured,
    t.execution_duration_seconds - COALESCE(w.duration_seconds, 0) AS cold_start_estimate
FROM system.lakeflow.job_task_run_timeline t
LEFT JOIN soccer_analytics.observability.workflow_cost_live w
    ON w.task_key = t.task_key
    AND w.job_run_id = CAST(t.job_run_id AS STRING)
WHERE t.job_run_id = <latest_run_id>
ORDER BY cold_start_estimate DESC;
```

This reveals the actual cold-start time per task and validates the perf-optimized mode improvement.

---

## Track 2 — Port/Adapter Architecture (Strategies B + D)

### The SkipGuard Protocol

```python
# src/ingestion/guards.py

from dataclasses import dataclass, field
from typing import Any, Protocol

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class FilterResult:
    """What the freshness gate learns from a single workflow's guard."""

    workflow_id: str
    count: int                                    # 0 = skip entirely
    chunks: list[list[str]] | None = None         # pre-computed fan-out chunks (None = single task)
    metadata: dict[str, Any] = field(default_factory=dict)  # pass-through context for the pipeline


class SkipGuard(Protocol):
    """Port: each workflow exposes its freshness check."""

    workflow_id: str

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult: ...
```

**Design decisions:**
- `FilterResult` is a frozen dataclass, safe for JSON serialization (for `run_if` / `for_each_task` task value output). `chunks` contains string IDs only.
- `chunks = None` means single-task execution. `len(chunks) > 1` triggers fan-out. The adapter owns chunk sizing (Option B — the adapter knows its data shape).
- `metadata` carries workflow-specific context the pipeline needs downstream (e.g., StatsBomb's competitions DataFrame reference, expected threat's `need_global` flag). Avoids re-computing what the guard already discovered.
- Adapters that always run (metrica, imports, model_validation) return `FilterResult(workflow_id=..., count=1)`.

### Guard Adapter Extraction

Each pipeline extracts its existing skip guard into a module-level `skip_guard` instance. The extraction is mechanical — the top half of `run_pipeline()` becomes the adapter's `check()` method:

| Guard Type | Pipelines | Complexity |
|---|---|---|
| Type A (match-level set diff) | ~12 pipelines (SPADL/VAEP, pitch control, off-ball xT, DEFCON, PAUSA, elastic sync, line-breaking, embeddings v1, formations) | Low |
| Type B (competition-level set diff) | xG v1, xG v2, expected_threat | Low |
| Type C (row-count comparison) | export_embeddings, prepare_360 | Low |
| Type D (presence heuristic) | entity_resolution | Low |
| No guard (always runs) | metrica, imports, model_validation, tracking_metadata | Trivial |

**Where adapters live**: Each adapter is a `skip_guard` object (or function) in the same module as its pipeline. No new files per workflow. A central registry in `src/ingestion/guards.py` collects them:

```python
WORKFLOW_GUARDS: dict[str, SkipGuard] = {
    "wf-vaep": spadl_vaep.skip_guard,
    "wf-pitch-control": pitch_control_batch.skip_guard,
    "wf-xg-v1": xg_model.skip_guard,
    # ... all workflows
}
```

**Pipeline receives pre-computed results**: Each `run_pipeline()` gains an optional `filter_result: FilterResult | None` parameter. When called via the gate-aware orchestration, it receives pre-computed IDs and skips its inline guard. When called standalone (local dev, testing), it falls back to its inline guard. The `@workflow` decorator passes this through without change.

### Chunk Sizing (Adapter's Domain Knowledge)

Each adapter declares its sizing based on the data source's memory characteristics:

| Pipeline | Group Key | Peak Memory/Group | Matches/Chunk |
|---|---|---|---|
| SPADL/VAEP | `match_id` | ~20 MB | 500 |
| Pitch Control | `match_id` | ~200–400 MB | 2 |
| Off-ball xT | `match_id` | ~200–400 MB | 2 |
| DEFCON-lite | `match_id` | ~50 MB | 15 |
| xG v1/v2 | `competition_id` | ~200 MB | 4 |
| Respo.Vision (future) | sub-match key | ~8 GB | sub-match grouping required |

The constraint: 1 GB UDF executor memory limit (800 MB practical) on Databricks serverless. Formula: `max_per_chunk = floor(800 MB / peak_memory_per_group)`.

### The Freshness Gate Task

A single Databricks task (`freshness_gate`) runs first in the job, using the `default` environment (wheel only — all guards use just Spark SQL).

**Execution flow:**

```
freshness_gate starts (1 cold start, default env, ~20-30s with perf-optimized)
    │
    ├─ for wf_id, guard in WORKFLOW_GUARDS:
    │     t0 = time.monotonic()
    │     result = guard.check(spark, catalog, schema)
    │     elapsed = time.monotonic() - t0
    │     log guard_check(workflow_id, count, elapsed_seconds)
    │     store result + timing
    │
    ├─ Emit SKIPPED records to workflow_cost_live for count=0 workflows
    ├─ Write results + per-guard timings to task values (dbutils.jobs.taskValues)
    └─ done (~30-60 seconds total)
```

**Per-guard timing**: Each `check()` call is individually timed and logged as structured JSON. This surfaces slow guards (a 30-second `DISTINCT match_id` on a 3M-row table signals the guard's query needs optimization) and provides baseline data for the gate's own performance budget.

**SKIPPED record emission**: For every guard returning `count == 0`, the gate MERGEs a SKIPPED record into `workflow_cost_live` with the workflow's `workflow_id`, `duration_seconds=0`, and `state="SKIPPED"`. Preserves the "every workflow has a record for every job run" invariant.

**DAG shape change**: Today ~5 root tasks launch in parallel. With the gate, 1 root task (`freshness_gate`) runs first, all other tasks depend on it with `run_if` conditions. Tasks that previously ran in parallel still can — they all wait for the gate first (~30-60 seconds), then launch concurrently as their `run_if` conditions are met.

**Downstream consumption**: Each task's `main()` entry point reads its `FilterResult` from task values:

```python
filter_json = dbutils.jobs.taskValues.get("freshness_gate", workflow_id, debugValue=None)
if filter_json:
    filter_result = FilterResult(**json.loads(filter_json))
else:
    filter_result = None  # standalone run — use inline guard
run_pipeline(spark, catalog, schema, filter_result=filter_result)
```

### HF Task Consolidation (Strategy D)

Merge 7 HF-related Databricks tasks into 1 `hf_sync` task in the `hf` environment.

**Structure:**

```python
# src/ingestion/hf_sync.py

def main():
    """Single Databricks task, multiple @workflow-decorated operations."""
    spark, catalog, schema = bootstrap()

    # Imports (HF Hub → Delta)
    run_import_space_creation(spark, catalog, schema)
    run_import_obso_results(spark, catalog, schema)
    run_import_psxg_predictions(spark, catalog, schema)

    # Exports (Delta → HF Hub)
    run_export_embeddings(spark, catalog, schema)
    run_export_shots(spark, catalog, schema)
    run_prepare_360(spark, catalog, schema)

    # Cost sync
    run_sync_costs(spark, catalog, schema)
```

Each sub-operation keeps its own `@workflow` decorator → separate records in `workflow_cost_live` with individual `duration_seconds`, `row_count`, `state`. Workflow cards unchanged.

**What changes:**
- `task_workflow_mapping.csv`: one new row (`hf_sync → wf-hf-sync`). The 7 individual `workflow_id`s are tracked via `workflow_cost_live` directly.
- DBU attribution in `fct_workflow_costs` is lumped for the combined task. Acceptable — HF tasks are collectively small.
- Terraform: 7 task blocks → 1 task block. 3 environment blocks → already consolidated in Track 1.

**Dependency ordering**: The `hf_sync` task depends on the freshness gate. Exports that depend on compute results (e.g., `export_shots_on_target` needs xG) are naturally satisfied because compute tasks run before `hf_sync` in the DAG.

**Integration with the freshness gate**: The `hf_sync` guard adapter combines all 7 sub-operation guards. If any sub-operation has work, the task runs. Inside, each sub-operation handles its own skip logic.

### Fan-Out Extension Point

`FilterResult.chunks` exists from day one but all adapters initially return `chunks=None` (single-task mode). Fan-out gets wired per-pipeline as data grows.

**Databricks `for_each_task` mechanics**: The gate writes chunk arrays as task values. A `for_each_task` block reads the array and spawns one subtask per chunk. Each iteration gets its own serverless session but shares the cached venv (same `environment_key`). Max 100 concurrent iterations.

**Aggregation**: All chunks write to the same Delta table using `replaceWhere` keyed on partition. No post-fan-out merge task needed — each chunk is idempotent.

**Activation sequence**: Monitor post-Track-2 runtimes. Wire `for_each_task` for longest-running pipelines first (pitch control, off-ball xT). Respo.Vision will require fan-out from day one when data arrives.

---

## Files Changed

### Track 1

| File | Change |
|------|--------|
| `terraform/modules/workflows/main.tf` | Add `performance_target`. Delete `hf-readonly` + `hf-sync` env blocks. Update 4 task `environment_key` refs to `hf`. |

### Track 2

| File | Change |
|------|--------|
| `src/ingestion/guards.py` | New — `FilterResult`, `SkipGuard` protocol, `WORKFLOW_GUARDS` registry |
| `src/ingestion/freshness_gate.py` | New — gate entry point, guard orchestration, SKIPPED emission, per-guard timing |
| `src/ingestion/hf_sync.py` | New — combined HF task entry point |
| `src/ingestion/*.py` (~20 files) | Extract skip guard into `skip_guard` adapter; add `filter_result` param to `run_pipeline()` |
| `pyproject.toml` | New entry points: `freshness_gate`, `hf_sync` |
| `terraform/modules/workflows/main.tf` | Add `freshness_gate` task (root, `default` env). Add `depends_on` + `run_if` to all tasks. Replace 7 HF task blocks with 1 `hf_sync` block. |
| `dbt_project/seeds/task_workflow_mapping.csv` | Update for new task structure |
| `workflow-cards/wf-freshness-gate.yaml` | New — gate workflow card |

---

## TODO Items (Post-Implementation)

- **Selective fan-out activation**: Monitor post-Track-2 per-task runtimes from `system.lakeflow.job_task_run_timeline`. Wire `for_each_task` for pipelines where single-task runtime exceeds 15 minutes. Priority order: pitch control → off-ball xT → SPADL/VAEP (at scale).
- **D45 v1/v2 skip guard conflict**: The guard extraction naturally surfaces this — v2 has no Delta skip guard (always overwrites via `replaceWhere`). The adapter for `wf-football2vec` should implement a proper source-scoped guard that doesn't trigger v1 re-runs.

---

## EIP Pattern Mapping

| EIP Pattern | Implementation |
|---|---|
| **Message Filter** | Freshness gate — routes work vs skip |
| **Content-Based Router** | `run_if` conditions from gate output |
| **Claim Check** | Gate computes IDs; pipeline receives IDs via task values, loads data itself |
| **Splitter** | Fan-out chunks via `for_each_task` |
| **Aggregator** | `replaceWhere` idempotent Delta writes per chunk |
| **Pipes and Filters** | Gate → compute → HF sync sequential stages |

---

## Risk Assessment

| Risk | Mitigation |
|------|-----------|
| Terraform provider doesn't support `performance_target` | Set via Jobs API or UI as stopgap |
| Gate task becomes a bottleneck (>60s) | Per-guard timing identifies slow guards; optimize their queries |
| `run_if` / task values API changes | Thin abstraction layer; task value reads have `debugValue` fallback for standalone runs |
| Guard logic diverges from pipeline logic | Guard and pipeline share the same module; the guard IS the extracted top half |
| Fan-out `replaceWhere` conflicts between chunks | Chunks are non-overlapping ID sets; partition-level writes are atomic |
