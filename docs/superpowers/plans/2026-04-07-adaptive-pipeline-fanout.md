# D40 Adaptive Pipeline Fan-Out — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut Databricks ingestion job wall-clock from ~16m to ~5m steady-state via performance-optimized mode, centralized freshness gate with port/adapter skip guards, and HF task consolidation.

**Architecture:** Two sequential tracks. Track 1 is Terraform-only (perf-optimized mode + HF env consolidation). Track 2 extracts skip guards from ~20 pipelines into a `SkipGuard` protocol, builds a freshness gate task that runs all guards centrally, consolidates 7 HF tasks into 1, and adds a fan-out extension point via `FilterResult.chunks`.

**Tech Stack:** Terraform (Databricks provider), Python 3.10, PySpark, Delta Lake, `@workflow` decorator, `CostEstimateHook`

**Spec:** `docs/superpowers/specs/2026-04-07-adaptive-pipeline-fanout-design.md`

**Branch:** `feat/d40-adaptive-pipeline-fanout` (no commits without explicit user approval)

---

## File Structure

### New Files

| File | Responsibility |
|------|---------------|
| `src/ingestion/guards.py` | `FilterResult` dataclass, `SkipGuard` protocol, `WORKFLOW_GUARDS` registry |
| `src/ingestion/freshness_gate.py` | Gate entry point — orchestrates all guards, emits SKIPPED records, writes task values |
| `src/ingestion/hf_sync.py` | Combined HF task entry point — calls 7 `@workflow`-decorated operations sequentially |
| `src/tests/test_guards.py` | Tests for `FilterResult`, guard adapters, serialization |
| `src/tests/test_freshness_gate.py` | Tests for gate orchestration, SKIPPED emission, timing |
| `src/tests/test_hf_sync.py` | Tests for combined HF task |
| `workflow-cards/wf-freshness-gate.yaml` | Workflow card for the freshness gate |

### Modified Files

| File | Change |
|------|--------|
| `terraform/modules/workflows/main.tf` | Track 1: `performance_target`, env consolidation. Track 2: `freshness_gate` task, `run_if` conditions, replace 7 HF task blocks with 1 |
| `src/ingestion/pitch_control_batch.py` | Extract skip guard (lines 164–193) into `skip_guard` adapter; add `filter_result` param |
| `src/ingestion/*.py` (~19 more) | Same pattern as pitch_control — extract guard, add `filter_result` param |
| `pyproject.toml` | Add `freshness_gate` and `hf_sync` entry points |
| `dbt_project/seeds/task_workflow_mapping.csv` | Add `freshness_gate` + `hf_sync` rows, remove old HF task rows |

---

## Track 1 — Terraform Infrastructure

### Task 1: Enable Performance-Optimized Mode

**Files:**
- Modify: `terraform/modules/workflows/main.tf:44-46`

- [ ] **Step 1: Check Terraform provider support for `performance_target`**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && grep -r "performance_target" .terraform/` to check if the provider schema includes this field. Also check the Terraform registry docs for `databricks_job`.

If the provider supports it, proceed to Step 2. If not, document the fallback (API/UI) and skip to Task 2.

- [ ] **Step 2: Add `performance_target` to the job resource**

In `terraform/modules/workflows/main.tf`, after line 46 (`max_concurrent_runs = 1`), add:

```hcl
  performance_target = "PERFORMANCE_OPTIMIZED"
```

- [ ] **Step 3: Validate Terraform**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse/terraform/environments/dev && terraform validate`
Expected: Success

- [ ] **Step 4: Terraform plan**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse/terraform/environments/dev && terraform plan -target=module.workflows`
Expected: 1 resource to update (in-place change on `databricks_job.data_ingestion`)

### Task 2: Consolidate HF Environments

**Files:**
- Modify: `terraform/modules/workflows/main.tf:674,694,720,797,957-968,971-983`

- [ ] **Step 1: Update task environment_key references**

In `terraform/modules/workflows/main.tf`, change these lines:

- Line 674: `environment_key = "hf-sync"` → `environment_key = "hf"`
- Line 694: `environment_key = "hf-readonly"` → `environment_key = "hf"`
- Line 720: `environment_key = "hf-readonly"` → `environment_key = "hf"`
- Line 797: `environment_key = "hf-readonly"` → `environment_key = "hf"`

- [ ] **Step 2: Delete the `hf-readonly` environment block**

Delete lines 957–968 (the entire `environment { environment_key = "hf-readonly" ... }` block).

- [ ] **Step 3: Delete the `hf-sync` environment block**

Delete lines 971–983 (the entire `environment { environment_key = "hf-sync" ... }` block). Note: after deleting `hf-readonly`, these line numbers will have shifted — delete the block that starts with `environment_key = "hf-sync"`.

- [ ] **Step 4: Validate Terraform**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse/terraform/environments/dev && terraform validate`
Expected: Success

- [ ] **Step 5: Terraform plan**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse/terraform/environments/dev && terraform plan -target=module.workflows`
Expected: 1 resource to update (environment blocks removed, task env_keys changed)

### Task 3: Apply Track 1 and Measure

This task requires user approval and action — it applies Terraform changes and triggers a job run.

- [ ] **Step 1: User applies Terraform**

User runs: `terraform apply -target=module.workflows`

- [ ] **Step 2: Trigger manual job run**

User triggers the ingestion job from the Databricks UI or CLI.

- [ ] **Step 3: Query timing data after run completes**

Run this SQL in Databricks SQL editor against the most recent job run:

```sql
SELECT
    t.task_key,
    t.execution_duration_seconds AS total_with_coldstart,
    w.duration_seconds AS app_measured,
    t.execution_duration_seconds - COALESCE(w.duration_seconds, 0) AS cold_start_estimate,
    w.state
FROM system.lakeflow.job_task_run_timeline t
LEFT JOIN soccer_analytics.observability.workflow_cost_live w
    ON w.task_key = t.task_key
    AND w.job_run_id = CAST(t.job_run_id AS STRING)
WHERE t.job_run_id = (
    SELECT MAX(job_run_id)
    FROM system.lakeflow.job_task_run_timeline
    WHERE period_start_time >= CURRENT_DATE
)
ORDER BY cold_start_estimate DESC;
```

Record results — this is the baseline for Track 2.

---

## Track 2 — Port/Adapter Architecture

### Task 4: FilterResult and SkipGuard Protocol

**Files:**
- Create: `src/ingestion/guards.py`
- Create: `src/tests/test_guards.py`

- [ ] **Step 1: Write tests for FilterResult**

```python
# src/tests/test_guards.py
"""Tests for the SkipGuard protocol and FilterResult dataclass."""

from __future__ import annotations

import json

from ingestion.guards import FilterResult


class TestFilterResult:
    """FilterResult dataclass behavior."""

    def test_skip_result(self) -> None:
        result = FilterResult(workflow_id="wf-vaep", count=0)
        assert result.count == 0
        assert result.chunks is None
        assert result.metadata == {}

    def test_work_result_single_chunk(self) -> None:
        result = FilterResult(workflow_id="wf-vaep", count=47, chunks=None)
        assert result.count == 47
        assert result.chunks is None

    def test_work_result_with_chunks(self) -> None:
        chunks = [["m1", "m2"], ["m3", "m4"]]
        result = FilterResult(
            workflow_id="wf-pitch-control",
            count=4,
            chunks=chunks,
        )
        assert len(result.chunks) == 2
        assert result.chunks[0] == ["m1", "m2"]

    def test_metadata_passthrough(self) -> None:
        result = FilterResult(
            workflow_id="wf-xt-grids",
            count=3,
            metadata={"need_global": True, "new_comps": ["comp1"]},
        )
        assert result.metadata["need_global"] is True

    def test_frozen(self) -> None:
        result = FilterResult(workflow_id="wf-vaep", count=0)
        try:
            result.count = 5  # type: ignore[misc]
            raise AssertionError("Should not allow mutation")
        except AttributeError:
            pass  # Expected — frozen dataclass

    def test_json_serialization(self) -> None:
        """FilterResult must survive JSON round-trip for task values."""
        result = FilterResult(
            workflow_id="wf-vaep",
            count=47,
            chunks=[["m1", "m2"], ["m3"]],
            metadata={"need_global": True},
        )
        serialized = json.dumps(
            {
                "workflow_id": result.workflow_id,
                "count": result.count,
                "chunks": result.chunks,
                "metadata": result.metadata,
            }
        )
        data = json.loads(serialized)
        restored = FilterResult(**data)
        assert restored.workflow_id == "wf-vaep"
        assert restored.count == 47
        assert restored.chunks == [["m1", "m2"], ["m3"]]
        assert restored.metadata["need_global"] is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_guards.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ingestion.guards'`

- [ ] **Step 3: Implement FilterResult and SkipGuard**

```python
# src/ingestion/guards.py
"""Port/adapter infrastructure for the freshness gate.

Each workflow exposes a :class:`SkipGuard` adapter whose ``check()``
method returns a :class:`FilterResult` describing whether the workflow
has new work and how to chunk it for fan-out.

The freshness gate task (:mod:`ingestion.freshness_gate`) calls every
registered guard once at job start and uses the results to skip or
invoke downstream tasks.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass(frozen=True)
class FilterResult:
    """What the freshness gate learns from a single workflow's guard.

    Attributes:
        workflow_id: The ``wf-xxx`` identifier matching the workflow card.
        count: Number of unprocessed items.  ``0`` means skip entirely.
        chunks: Pre-computed fan-out partitions — a list of ID lists.
            ``None`` means single-task execution (no fan-out).
            ``len(chunks) > 1`` triggers ``for_each_task``.
            The adapter owns chunk sizing (knows its data shape).
        metadata: Pass-through context for the pipeline — avoids
            re-computing what the guard already discovered (e.g.,
            ``need_global`` flag, competitions DataFrame).
    """

    workflow_id: str
    count: int
    chunks: list[list[str]] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        """Serialize for ``dbutils.jobs.taskValues.set``."""
        return json.dumps(
            {
                "workflow_id": self.workflow_id,
                "count": self.count,
                "chunks": self.chunks,
                "metadata": self.metadata,
            }
        )

    @classmethod
    def from_json(cls, raw: str) -> FilterResult:
        """Deserialize from ``dbutils.jobs.taskValues.get``."""
        data = json.loads(raw)
        return cls(**data)


class SkipGuard(Protocol):
    """Port: each workflow exposes its freshness check.

    Implementations live alongside their pipeline module as a
    module-level ``skip_guard`` object or function.
    """

    workflow_id: str

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Run the skip guard and return a FilterResult.

        Must be safe to call from the ``default`` environment —
        only Spark SQL, no analytics imports.
        """
        ...
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_guards.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Add JSON round-trip test via to_json/from_json**

Add to `TestFilterResult` in `src/tests/test_guards.py`:

```python
    def test_to_json_from_json_roundtrip(self) -> None:
        original = FilterResult(
            workflow_id="wf-vaep",
            count=47,
            chunks=[["m1", "m2"], ["m3"]],
            metadata={"need_global": True},
        )
        restored = FilterResult.from_json(original.to_json())
        assert restored == original
```

- [ ] **Step 6: Run all tests**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_guards.py -v`
Expected: All 7 tests PASS

- [ ] **Step 7: Lint check**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run ruff check src/ingestion/guards.py src/tests/test_guards.py && uv run pyright src/ingestion/guards.py`
Expected: Clean

### Task 5: Extract First Guard Adapter (Pitch Control — Template)

This is the template extraction that all other pipelines will follow.

**Files:**
- Modify: `src/ingestion/pitch_control_batch.py:148-193`
- Modify: `src/tests/test_guards.py`

- [ ] **Step 1: Write test for the pitch control guard adapter**

Add to `src/tests/test_guards.py`:

```python
from unittest.mock import MagicMock, patch


class TestPitchControlGuard:
    """Pitch control skip guard adapter."""

    def test_returns_skip_when_all_processed(self) -> None:
        """Guard returns count=0 when all matches are already in results."""
        from ingestion.pitch_control_batch import skip_guard

        spark = MagicMock()
        # Gold table has matches m1, m2
        gold_rows = [MagicMock(match_id="m1"), MagicMock(match_id="m2")]
        # Results table also has m1, m2
        results_rows = [MagicMock(match_id="m1"), MagicMock(match_id="m2")]

        def table_side_effect(name: str) -> MagicMock:
            mock_df = MagicMock()
            if "fct_tracking_frames" in name:
                mock_df.select.return_value.distinct.return_value.collect.return_value = gold_rows
            else:
                mock_df.select.return_value.distinct.return_value.collect.return_value = results_rows
            return mock_df

        spark.table.side_effect = table_side_effect

        result = skip_guard.check(spark, "soccer_analytics", "bronze")
        assert result.count == 0
        assert result.workflow_id == "wf-pitch-control"

    def test_returns_new_matches(self) -> None:
        """Guard returns count and IDs for unprocessed matches."""
        from ingestion.pitch_control_batch import skip_guard

        spark = MagicMock()
        gold_rows = [MagicMock(match_id="m1"), MagicMock(match_id="m2"), MagicMock(match_id="m3")]
        results_rows = [MagicMock(match_id="m1")]

        def table_side_effect(name: str) -> MagicMock:
            mock_df = MagicMock()
            if "fct_tracking_frames" in name:
                mock_df.select.return_value.distinct.return_value.collect.return_value = gold_rows
            else:
                mock_df.select.return_value.distinct.return_value.collect.return_value = results_rows
            return mock_df

        spark.table.side_effect = table_side_effect

        result = skip_guard.check(spark, "soccer_analytics", "bronze")
        assert result.count == 2
        assert set(result.metadata["new_match_ids"]) == {"m2", "m3"}

    def test_returns_all_when_no_results_table(self) -> None:
        """Guard returns all matches when results table doesn't exist."""
        from ingestion.pitch_control_batch import skip_guard

        spark = MagicMock()
        gold_rows = [MagicMock(match_id="m1"), MagicMock(match_id="m2")]

        def table_side_effect(name: str) -> MagicMock:
            mock_df = MagicMock()
            if "fct_tracking_frames" in name:
                mock_df.select.return_value.distinct.return_value.collect.return_value = gold_rows
            else:
                raise Exception("Table not found")
            return mock_df

        spark.table.side_effect = table_side_effect

        result = skip_guard.check(spark, "soccer_analytics", "bronze")
        assert result.count == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_guards.py::TestPitchControlGuard -v`
Expected: FAIL — `skip_guard` not defined

- [ ] **Step 3: Extract the guard from pitch_control_batch.py**

Add the guard adapter class to `src/ingestion/pitch_control_batch.py`, after the imports and before `run_pipeline`. This extracts lines 164–193 into the adapter:

```python
# After the existing imports at the top of the file, add:
from ingestion.guards import FilterResult

# Before run_pipeline, add:

class _PitchControlGuard:
    """SkipGuard adapter for pitch control batch pipeline."""

    workflow_id = "wf-pitch-control"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check which tracking matches need pitch control computation."""
        from shared.constants import DEFAULT_GOLD_SCHEMA

        gold_table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_tracking_frames"
        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"

        try:
            match_id_rows = spark.table(gold_table).select("match_id").distinct().collect()
        except Exception:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        if not match_id_rows:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        all_match_ids = [row["match_id"] for row in match_id_rows]

        existing_ids: set[str] = set()
        try:
            existing_rows = spark.table(results_table).select("match_id").distinct().collect()
            existing_ids = {str(row["match_id"]) for row in existing_rows}
        except Exception:
            pass  # Table doesn't exist yet — process all

        new_match_ids = [str(mid) for mid in all_match_ids if str(mid) not in existing_ids]

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(new_match_ids),
            metadata={"new_match_ids": new_match_ids} if new_match_ids else {},
        )


skip_guard = _PitchControlGuard()
```

- [ ] **Step 4: Update run_pipeline to accept filter_result**

Modify the `run_pipeline` function signature at line 148 to add the optional parameter, and use it when available:

Change the signature from:
```python
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> int:
```

To:
```python
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult | None = None,
) -> int:
```

Then replace lines 161–196 (the existing guard + filter logic) with:

```python
    gold_table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_tracking_frames"
    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"

    # Use pre-computed filter result from freshness gate, or run inline guard
    if filter_result and filter_result.metadata.get("new_match_ids"):
        new_ids_str = filter_result.metadata["new_match_ids"]
    else:
        # Standalone execution — run inline guard
        try:
            match_id_rows = spark.table(gold_table).select("match_id").distinct().collect()
        except Exception:
            logger.warning("Cannot read table %s", gold_table)
            return 0

        if not match_id_rows:
            logger.info("No matches in %s", gold_table)
            return 0

        all_match_ids = [row["match_id"] for row in match_id_rows]

        existing_ids: set[str] = set()
        try:
            existing_rows = spark.table(results_table).select("match_id").distinct().collect()
            existing_ids = {str(row["match_id"]) for row in existing_rows}
        except Exception:
            logger.info("No existing %s table -- processing all matches", results_table)

        new_ids_str = [str(mid) for mid in all_match_ids if str(mid) not in existing_ids]

    logger.info("%d matches to process", len(new_ids_str))

    if not new_ids_str:
        return 0
```

The rest of the function (line 197 onward — building `tracking_df` with `F.col("match_id").isin(new_ids_str)`) is unchanged.

- [ ] **Step 5: Run tests**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_guards.py -v`
Expected: All tests PASS

- [ ] **Step 6: Run existing pitch control tests**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_pitch_control_batch.py -v`
Expected: All existing tests PASS (the inline fallback path preserves backward compatibility)

- [ ] **Step 7: Lint check**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run ruff check src/ingestion/pitch_control_batch.py src/ingestion/guards.py && uv run pyright src/ingestion/pitch_control_batch.py`
Expected: Clean

### Task 6: Extract Remaining Guard Adapters

Follow the exact pattern from Task 5 for each pipeline. Each extraction is mechanical: (1) add `from ingestion.guards import FilterResult`, (2) create `_XxxGuard` class with `check()`, (3) create `skip_guard = _XxxGuard()`, (4) add `filter_result` param to `run_pipeline`, (5) add `if filter_result` early path.

**Files:** All files in `src/ingestion/` that have skip guards.

This task is large — the implementer should process files in batches of 3–4, running lint + existing tests after each batch.

- [ ] **Step 1: Type A guards (match-level set diff) — 10 pipelines**

Extract the same pattern from each. The guard's `check()` queries the results table for `DISTINCT match_id`, diffs against the source table, returns `FilterResult` with `new_match_ids` in metadata.

Pipelines (file → `workflow_id`):
- `src/ingestion/off_ball_xt.py` → `wf-off-ball-xt`
- `src/ingestion/spadl_vaep.py` → `wf-vaep` (has TWO guards — SPADL conversion + VAEP scoring; combine into one adapter returning both exclusion sets)
- `src/ingestion/defcon_lite_360.py` → `wf-defcon` (adds `data_source` filter)
- `src/ingestion/defcon_lite_tracking.py` → `wf-defcon` (separate guard, same workflow_id — merge with above or keep as two adapters keyed differently)
- `src/ingestion/elastic_sync.py` → `wf-elastic-sync`
- `src/ingestion/pausa.py` → `wf-obso-pausa`
- `src/ingestion/line_breaking.py` → `wf-line-breaking`
- `src/ingestion/player_embeddings_v1.py` → `wf-football2vec`
- `src/ingestion/formations_efpi.py` → `wf-formations`
- `src/ingestion/formations_shape_graph.py` → `wf-formations` (shares workflow_id with EFPI)

After each batch of 3–4 files:
Run: `uv run ruff check src/ingestion/ && uv run pytest src/tests/ -x -q`

- [ ] **Step 2: Type B guards (competition-level set diff) — 3 pipelines**

- `src/ingestion/xg_model.py` → `wf-xg-v1`
- `src/ingestion/xg_model_v2.py` → `wf-xg-v2`
- `src/ingestion/expected_threat.py` → `wf-xt-grids` (metadata includes `need_global` flag)

Run: `uv run ruff check src/ingestion/ && uv run pytest src/tests/ -x -q`

- [ ] **Step 3: Type C guards (row-count comparison) — 2 pipelines**

- `src/ingestion/export_embeddings_training_data.py` → `wf-football2vec-v2-export`
- `src/ingestion/prepare_360_training_data.py` → `wf-prepare-360-data`

Run: `uv run ruff check src/ingestion/ && uv run pytest src/tests/ -x -q`

- [ ] **Step 4: Type D (presence heuristic) — 1 pipeline**

- `src/ingestion/entity_resolution.py` → `wf-entity-resolution`

Run: `uv run ruff check src/ingestion/ && uv run pytest src/tests/ -x -q`

- [ ] **Step 5: Always-run adapters — remaining pipelines**

For pipelines with no guard (always run): create a trivial adapter returning `FilterResult(workflow_id=..., count=1)`.

- `src/ingestion/statsbomb.py` → `wf-statsbomb` (has multi-stage guard but also always runs at least the competitions check — keep as guard with count=1 when competitions present + new matches = 0)
- `src/ingestion/metrica.py` → `wf-metrica` (always count=1)
- `src/ingestion/wyscout.py` → `wf-wyscout`
- `src/ingestion/idsse.py` → `wf-idsse`
- `src/ingestion/skillcorner.py` → `wf-skillcorner`
- `src/ingestion/backfill_statsbomb_extra.py` → handled by statsbomb guard
- `src/ingestion/import_obso_results.py` → `wf-import-obso` (always count=1)
- `src/ingestion/import_psxg_predictions.py` → `wf-import-psxg` (always count=1)
- `src/ingestion/import_space_creation.py` → `wf-import-space-creation` (always count=1)
- `src/ingestion/tracking_metadata.py` → `wf-tracking-metadata` (always count=1)
- `src/ingestion/model_validation.py` → `wf-model-validation` (always count=1)
- `src/ingestion/sync_hf_costs.py` → `wf-sync-hf-costs` (always count=1)
- `src/ingestion/player_embeddings_v2.py` → `wf-football2vec-v2` (always count=1 — no Delta guard, always overwrites)

Run: `uv run ruff check src/ingestion/ && uv run pytest src/tests/ -x -q`

- [ ] **Step 6: Populate WORKFLOW_GUARDS registry**

In `src/ingestion/guards.py`, add the registry. Use lazy imports to avoid circular dependencies:

```python
def get_workflow_guards() -> dict[str, SkipGuard]:
    """Build the guard registry with lazy imports.

    Lazy imports avoid circular dependencies and keep the guards.py
    module importable without Spark.
    """
    from ingestion import (
        defcon_lite_360,
        defcon_lite_tracking,
        elastic_sync,
        entity_resolution,
        expected_threat,
        export_embeddings_training_data,
        formations_efpi,
        idsse,
        import_obso_results,
        import_psxg_predictions,
        import_space_creation,
        line_breaking,
        metrica,
        model_validation,
        off_ball_xt,
        pausa,
        pitch_control_batch,
        player_embeddings_v1,
        player_embeddings_v2,
        prepare_360_training_data,
        skillcorner,
        spadl_vaep,
        statsbomb,
        sync_hf_costs,
        tracking_metadata,
        wyscout,
        xg_model,
        xg_model_v2,
    )

    return {
        guard.workflow_id: guard
        for guard in [
            statsbomb.skip_guard,
            metrica.skip_guard,
            wyscout.skip_guard,
            idsse.skip_guard,
            skillcorner.skip_guard,
            spadl_vaep.skip_guard,
            xg_model.skip_guard,
            xg_model_v2.skip_guard,
            expected_threat.skip_guard,
            off_ball_xt.skip_guard,
            pitch_control_batch.skip_guard,
            defcon_lite_360.skip_guard,
            defcon_lite_tracking.skip_guard,
            elastic_sync.skip_guard,
            pausa.skip_guard,
            line_breaking.skip_guard,
            entity_resolution.skip_guard,
            formations_efpi.skip_guard,
            player_embeddings_v1.skip_guard,
            player_embeddings_v2.skip_guard,
            export_embeddings_training_data.skip_guard,
            prepare_360_training_data.skip_guard,
            import_obso_results.skip_guard,
            import_psxg_predictions.skip_guard,
            import_space_creation.skip_guard,
            tracking_metadata.skip_guard,
            model_validation.skip_guard,
            sync_hf_costs.skip_guard,
        ]
    }
```

- [ ] **Step 7: Full test suite**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run ruff check src/ingestion/ && uv run pyright src/ingestion/guards.py && uv run pytest src/tests/ -x -q`
Expected: All pass

### Task 7: Freshness Gate Task

**Files:**
- Create: `src/ingestion/freshness_gate.py`
- Create: `src/tests/test_freshness_gate.py`
- Modify: `pyproject.toml:56-93` (add entry point)

- [ ] **Step 1: Write tests for the freshness gate**

```python
# src/tests/test_freshness_gate.py
"""Tests for the freshness gate orchestrator."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ingestion.freshness_gate import run_gate
from ingestion.guards import FilterResult


class TestRunGate:
    """Gate orchestration behavior."""

    def test_collects_results_from_all_guards(self) -> None:
        guard_a = MagicMock()
        guard_a.workflow_id = "wf-a"
        guard_a.check.return_value = FilterResult(workflow_id="wf-a", count=5)

        guard_b = MagicMock()
        guard_b.workflow_id = "wf-b"
        guard_b.check.return_value = FilterResult(workflow_id="wf-b", count=0)

        spark = MagicMock()
        results = run_gate(spark, "cat", "schema", guards={"wf-a": guard_a, "wf-b": guard_b})

        assert results["wf-a"].count == 5
        assert results["wf-b"].count == 0
        guard_a.check.assert_called_once_with(spark, "cat", "schema")
        guard_b.check.assert_called_once_with(spark, "cat", "schema")

    def test_records_per_guard_timing(self) -> None:
        guard = MagicMock()
        guard.workflow_id = "wf-a"
        guard.check.return_value = FilterResult(workflow_id="wf-a", count=1)

        spark = MagicMock()
        results = run_gate(spark, "cat", "schema", guards={"wf-a": guard})

        # Timing is in the return value metadata
        assert "wf-a" in results
        assert results["wf-a"].count == 1

    def test_guard_failure_does_not_crash_gate(self) -> None:
        """A failing guard should not prevent other guards from running."""
        guard_ok = MagicMock()
        guard_ok.workflow_id = "wf-ok"
        guard_ok.check.return_value = FilterResult(workflow_id="wf-ok", count=3)

        guard_bad = MagicMock()
        guard_bad.workflow_id = "wf-bad"
        guard_bad.check.side_effect = RuntimeError("Delta table gone")

        spark = MagicMock()
        results = run_gate(
            spark, "cat", "schema",
            guards={"wf-bad": guard_bad, "wf-ok": guard_ok},
        )

        # Bad guard treated as count=0 (skip)
        assert results["wf-bad"].count == 0
        # Good guard still ran
        assert results["wf-ok"].count == 3
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_freshness_gate.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement the freshness gate**

```python
# src/ingestion/freshness_gate.py
"""Freshness gate — centralized skip guard orchestration.

Runs all workflow guards in a single Databricks task, emits SKIPPED
records for workflows with no new work, and writes FilterResults as
task values for downstream ``run_if`` conditions.

Uses the ``default`` environment (wheel only) — guards use only
Spark SQL, no analytics imports.
"""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING, Any

from ingestion.guards import FilterResult, get_workflow_guards
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from shared.constants import DEFAULT_GOLD_SCHEMA
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from ingestion.guards import SkipGuard

logger = logging.getLogger(__name__)


def run_gate(
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    guards: dict[str, SkipGuard] | None = None,
) -> dict[str, FilterResult]:
    """Run all guards and collect results.

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        schema: Pipeline target schema (passed to guards).
        guards: Override guard registry (for testing).

    Returns:
        Dict mapping workflow_id to FilterResult.
    """
    if guards is None:
        guards = get_workflow_guards()

    results: dict[str, FilterResult] = {}
    timings: dict[str, float] = {}

    for wf_id, guard in guards.items():
        t0 = time.monotonic()
        try:
            result = guard.check(spark, catalog, schema)
            elapsed = round(time.monotonic() - t0, 2)
            results[wf_id] = result
            timings[wf_id] = elapsed
            logger.info(
                "guard_check",
                extra={
                    "workflow_id": wf_id,
                    "count": result.count,
                    "elapsed_seconds": elapsed,
                    "chunks": len(result.chunks) if result.chunks else 0,
                },
            )
        except Exception:
            elapsed = round(time.monotonic() - t0, 2)
            logger.warning(
                "guard_check_failed",
                extra={"workflow_id": wf_id, "elapsed_seconds": elapsed},
                exc_info=True,
            )
            # Treat failure as skip — don't crash the gate
            results[wf_id] = FilterResult(workflow_id=wf_id, count=0)
            timings[wf_id] = elapsed

    logger.info(
        "gate_summary",
        extra={
            "total_guards": len(guards),
            "with_work": sum(1 for r in results.values() if r.count > 0),
            "skipped": sum(1 for r in results.values() if r.count == 0),
            "total_elapsed": round(sum(timings.values()), 2),
            "slowest_guard": max(timings, key=timings.get) if timings else "none",
            "slowest_seconds": max(timings.values()) if timings else 0,
        },
    )

    return results


def _emit_skipped_records(
    spark: SparkSession,
    catalog: str,
    schema: str,
    results: dict[str, FilterResult],
) -> None:
    """MERGE SKIPPED records into workflow_cost_live for count=0 workflows.

    Uses CostEstimateHook directly rather than reaching into runner internals.
    """
    from ingestion.cost_hook import CostEstimateHook
    from workflows.context import WorkflowContext

    hook = CostEstimateHook(spark, catalog, schema)

    for wf_id, result in results.items():
        if result.count == 0:
            ctx = WorkflowContext(
                workflow_id=wf_id,
                phase="gate_skip",
            )
            hook.on_skip(ctx, "No new work (freshness gate)")


def _write_task_values(results: dict[str, FilterResult]) -> None:
    """Write FilterResults as Databricks task values for downstream run_if."""
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark:
            dbutils = DBUtils(spark)
            for wf_id, result in results.items():
                dbutils.jobs.taskValues.set(key=wf_id, value=result.to_json())
    except Exception:
        logger.debug("Task values not available (standalone mode)")


@workflow("wf-freshness-gate", phase="orchestration")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger_arg: logging.Logger,
) -> int:
    """Freshness gate entry point — run all guards, emit skips, write task values."""
    results = run_gate(spark, catalog, schema)

    _emit_skipped_records(spark, catalog, schema, results)
    _write_task_values(results)

    work_count = sum(1 for r in results.values() if r.count > 0)
    logger_arg.info("Freshness gate complete: %d workflows with work", work_count)
    return work_count


def main() -> None:
    """CLI entry point for the freshness_gate Databricks task."""
    configure_logging()
    args = parse_ingestion_args()
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)
```

- [ ] **Step 4: Run tests**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_freshness_gate.py -v`
Expected: All 3 tests PASS

- [ ] **Step 5: Add entry point to pyproject.toml**

In `pyproject.toml`, in the `[project.scripts]` section, add:

```toml
freshness_gate = "ingestion.freshness_gate:main"
```

- [ ] **Step 6: Lint check**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run ruff check src/ingestion/freshness_gate.py src/tests/test_freshness_gate.py && uv run pyright src/ingestion/freshness_gate.py`
Expected: Clean

### Task 8: HF Task Consolidation

**Files:**
- Create: `src/ingestion/hf_sync.py`
- Create: `src/tests/test_hf_sync.py`
- Modify: `pyproject.toml` (add entry point)

- [ ] **Step 1: Write tests for hf_sync**

```python
# src/tests/test_hf_sync.py
"""Tests for the combined HF sync task."""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call


class TestHfSync:
    """Combined HF task calls all sub-operations."""

    @patch("ingestion.hf_sync._run_sub_workflow")
    def test_calls_all_sub_operations(self, mock_run: MagicMock) -> None:
        from ingestion.hf_sync import run_pipeline

        spark = MagicMock()
        logger_mock = MagicMock()

        run_pipeline(spark, "cat", "schema", logger_mock)

        # Verify all 7 sub-operations were called
        assert mock_run.call_count == 7

    @patch("ingestion.hf_sync._run_sub_workflow")
    def test_continues_on_sub_operation_failure(self, mock_run: MagicMock) -> None:
        """One failing sub-operation must not prevent others from running."""
        from ingestion.hf_sync import run_pipeline

        # Second call fails
        mock_run.side_effect = [None, RuntimeError("HF Hub down"), None, None, None, None, None]

        spark = MagicMock()
        logger_mock = MagicMock()

        run_pipeline(spark, "cat", "schema", logger_mock)

        # All 7 were attempted despite failure at index 1
        assert mock_run.call_count == 7
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_hf_sync.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement hf_sync.py**

```python
# src/ingestion/hf_sync.py
"""Combined HF Hub sync task — imports and exports in a single Databricks task.

Replaces 7 separate HF tasks (3 imports, 3 exports, 1 cost sync) with
one task that calls each as a ``@workflow``-decorated sub-operation.
Each sub-operation gets its own record in ``workflow_cost_live``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from workflows import workflow

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Sub-operations in execution order: imports first (no deps), then exports
_SUB_OPERATIONS: list[tuple[str, str]] = [
    ("ingestion.import_space_creation", "run_pipeline"),
    ("ingestion.import_obso_results", "run_pipeline"),
    ("ingestion.import_psxg_predictions", "run_pipeline"),
    ("ingestion.export_embeddings_training_data", "run_pipeline"),
    ("ingestion.export_shots_on_target", "run_pipeline"),
    ("ingestion.prepare_360_training_data", "run_pipeline"),
    ("ingestion.sync_hf_costs", "run_pipeline"),
]


def _run_sub_workflow(
    module_path: str,
    func_name: str,
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger_arg: logging.Logger,
) -> None:
    """Import and run a single sub-workflow, swallowing failures."""
    import importlib

    try:
        mod = importlib.import_module(module_path)
        func = getattr(mod, func_name)
        func(spark, catalog, schema, logger_arg)
    except Exception:
        logger_arg.warning(
            "Sub-workflow %s.%s failed — continuing with remaining operations",
            module_path,
            func_name,
            exc_info=True,
        )


@workflow("wf-hf-sync", phase="orchestration")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger_arg: logging.Logger,
) -> int:
    """Run all HF import/export sub-operations sequentially."""
    completed = 0
    for module_path, func_name in _SUB_OPERATIONS:
        _run_sub_workflow(module_path, func_name, spark, catalog, schema, logger_arg)
        completed += 1
    return completed


def main() -> None:
    """CLI entry point for the hf_sync Databricks task."""
    configure_logging()
    args = parse_ingestion_args()
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)
```

- [ ] **Step 4: Run tests**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_hf_sync.py -v`
Expected: All 2 tests PASS

- [ ] **Step 5: Add entry point to pyproject.toml**

In `pyproject.toml`, in the `[project.scripts]` section, add:

```toml
hf_sync = "ingestion.hf_sync:main"
```

- [ ] **Step 6: Lint check**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run ruff check src/ingestion/hf_sync.py src/tests/test_hf_sync.py && uv run pyright src/ingestion/hf_sync.py`
Expected: Clean

### Task 9: Terraform DAG Restructure (Track 2)

**Files:**
- Modify: `terraform/modules/workflows/main.tf`

- [ ] **Step 1: Add freshness_gate task block**

Add as the first task in the job, before `ingest_statsbomb` (after line 63):

```hcl
  # ── Task: Freshness Gate — centralized skip guard ────────────────────
  task {
    task_key        = "freshness_gate"
    timeout_seconds = 300

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "freshness_gate"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "default"
  }
```

- [ ] **Step 2: Add depends_on freshness_gate to all existing tasks**

Every task that currently has no `depends_on` (the root parallel tasks) gets:

```hcl
    depends_on {
      task_key = "freshness_gate"
    }
```

Tasks that already have `depends_on` also need `freshness_gate` added (they depend on both the gate AND their existing dependencies).

Update these root tasks: `ingest_statsbomb`, `ingest_metrica`, `ingest_wyscout`, `ingest_idsse`, `ingest_skillcorner`, `import_space_creation`, `sync_hf_costs`.

- [ ] **Step 3: Replace 7 HF task blocks with 1 hf_sync block**

Delete these task blocks:
- `export_embeddings_training_data` (line ~555)
- `sync_hf_costs` (line ~674)
- `import_space_creation` (line ~694)
- `import_obso_results` (line ~720)
- `export_shots_on_target` (line ~773)
- `import_psxg_predictions` (line ~797)
- `prepare_360_training_data` (line ~854)

Add one combined block:

```hcl
  # ── Task: HF Hub sync — combined imports + exports ───────────────────
  task {
    task_key        = "hf_sync"
    timeout_seconds = 1800

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "hf_sync"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    # Depends on gate + all compute tasks that produce data for exports
    depends_on {
      task_key = "freshness_gate"
    }
    depends_on {
      task_key = "compute_spadl_vaep"
    }
    depends_on {
      task_key = "resolve_players"
    }
    depends_on {
      task_key = "compute_xg_model"
    }
    depends_on {
      task_key = "backfill_statsbomb_360"
    }

    environment_key = "hf"
  }
```

- [ ] **Step 4: Validate Terraform**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse/terraform/environments/dev && terraform validate`
Expected: Success

- [ ] **Step 5: Terraform plan**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse/terraform/environments/dev && terraform plan -target=module.workflows`
Expected: 1 resource to update

### Task 10: Update dbt Seed + Workflow Card

**Files:**
- Modify: `dbt_project/seeds/task_workflow_mapping.csv`
- Create: `workflow-cards/wf-freshness-gate.yaml`

- [ ] **Step 1: Update task_workflow_mapping.csv**

Remove rows for deleted HF task keys. Add new rows:

```csv
freshness_gate,wf-freshness-gate
hf_sync,wf-hf-sync
```

- [ ] **Step 2: Create workflow card for freshness gate**

```yaml
# workflow-cards/wf-freshness-gate.yaml
id: wf-freshness-gate
name: Freshness Gate
type: orchestration
description: >
  Centralized skip guard that runs all workflow freshness checks in a
  single Databricks task. Emits SKIPPED records for workflows with no
  new work and writes FilterResults as task values for downstream run_if
  conditions.

inputs:
  - name: All source and results Delta tables
    description: Each guard queries its own source/results tables via Spark SQL

outputs:
  - name: Task values (FilterResult JSON per workflow)
    description: Written via dbutils.jobs.taskValues for downstream consumption
  - name: SKIPPED records in workflow_cost_live
    description: One record per skipped workflow per job run

execution:
  runtime: databricks
  environment: default
  timeout_seconds: 300
  typical_duration_minutes: 1

monitoring:
  alert_if_duration_exceeds_minutes: 3
  alert_if_guard_exceeds_seconds: 30

dependencies:
  - None (root task)

academic_provenance: null

cost_estimate:
  sku: jobs_serverless_compute_run_dbus
  rate_usd_per_dbu: 0.07
  typical_dbu: 5
  estimated_cost_usd: 0.35
```

- [ ] **Step 3: Run dbt seed validation**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse && uv run validate_workflow_cards`
Expected: All cards valid

### Task 11: Update TODO.md

**Files:**
- Modify: `TODO.md`

- [ ] **Step 1: Add fan-out activation item to On Deck**

Add to the On Deck table in `TODO.md`:

```markdown
| D40a | Selective Fan-Out Activation | Wicked | D40 follow-up | Monitor post-D40 per-task runtimes from `system.lakeflow.job_task_run_timeline`. Wire `for_each_task` for pipelines where single-task runtime exceeds 15 minutes. Priority: pitch control → off-ball xT → SPADL/VAEP (at scale). `FilterResult.chunks` extension point ready — each adapter returns pre-sized chunks, gate writes chunk arrays as task values. Respo.Vision will require fan-out from day one. |
```

- [ ] **Step 2: Move D40 from On Deck to completed (or in-progress)**

Mark D40's core scope as complete, reference the PR.

---

## Verification Checklist

Before requesting user review:

- [ ] `uv run ruff check src/ scripts/` — zero violations
- [ ] `uv run ruff format --check src/ scripts/` — format clean
- [ ] `uv run pyright src/` — type check clean
- [ ] `uv run pytest src/tests/ -v` — all tests pass
- [ ] `terraform validate` — success
- [ ] `terraform plan` — shows expected changes only
- [ ] No secrets or credentials in any changed file
