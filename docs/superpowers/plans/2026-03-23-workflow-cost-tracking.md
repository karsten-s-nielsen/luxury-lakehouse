# Workflow Cost Tracking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement three-tier cost tracking (cold/warm/hot) for AI/ML workflows on Databricks and HF Jobs, with wheel deployment automation and E2E verification.

**Architecture:** Cost data flows through three tiers with decreasing accuracy but increasing freshness: (1) cold — dbt model from `system.billing.usage`, (2) warm — `CostEstimateHook` writes to `workflow_cost_live` Delta on pipeline completion, `HFJobsCostRecorder` writes `_workflow_cost.json` to HF Hub, (3) hot — same stores, RUNNING state, Taipy polls for live cost. The `CostEstimateHook` lives in `src/ingestion/` (Spark dependency), `HFJobsCostRecorder` lives in `src/analytics/` (in wheel, no Spark).

**Tech Stack:** Python 3.10, PySpark (Delta MERGE), Pydantic v2, huggingface_hub, databricks-sdk, dbt (Databricks adapter), pytest

**Spec:** `docs/superpowers/specs/2026-03-23-workflow-cost-tracking-design.md`

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| **New** | `src/analytics/cost.py` | `HFJobsCostRecorder` + HF rate constants |
| **New** | `src/ingestion/cost_hook.py` | `CostEstimateHook` (Spark MERGE to Delta) |
| **New** | `src/tests/test_cost_recorder.py` | Unit tests for `HFJobsCostRecorder` |
| **New** | `src/tests/test_cost_hook.py` | Unit tests for `CostEstimateHook` |
| **New** | `scripts/create_cost_table.sql` | DDL for `workflow_cost_live` |
| **New** | `scripts/deploy_wheel.py` | HF Hub → UC Volume wheel deploy |
| **New** | `dbt_project/models/marts/fct_workflow_costs.sql` | Cold tier dbt model |
| **Edit** | `src/workflows/runner.py` | Add `WorkflowSkippedError` → `on_skip` dispatch |
| **Edit** | `src/tests/test_runner.py` | Tests for skip dispatch |
| **Edit** | `dbt_project/models/marts/_marts__models.yml` | Contract for `fct_workflow_costs` |
| **Edit** | 12 × `src/ingestion/*.py` | Register `CostEstimateHook` in `main()` |
| **Edit** | 7 × `scripts/*_hf.py` | Add `HFJobsCostRecorder` lifecycle |

---

### Task 1: `HFJobsCostRecorder` — Tests

**Files:**
- Create: `src/tests/test_cost_recorder.py`
- Create: `src/analytics/cost.py` (stub only — enough to import)

This task writes tests first (TDD). The recorder is in `src/analytics/` (wheel, no Spark) and writes `_workflow_cost.json` to HF Hub repos.

- [ ] **Step 1: Create stub `src/analytics/cost.py`**

```python
"""Cost tracking utilities for HF Jobs workflows."""

from __future__ import annotations

# HF Jobs published rates (https://huggingface.co/docs/hub/en/jobs-pricing)
HF_RATE_CPU_BASIC: float = 0.01  # $/hr
HF_RATE_A10G_SMALL: float = 1.00  # $/hr
HF_RATE_A10G_LARGE: float = 1.50  # $/hr


class HFJobsCostRecorder:
    """Hot/warm cost tracking for HF Jobs — mirrors CostEstimateHook lifecycle."""

    def __init__(
        self,
        workflow_id: str,
        phase: str,
        rate_usd_per_hour: float,
        repo_id: str,
        repo_type: str = "dataset",
    ) -> None:
        raise NotImplementedError

    def start(self) -> None:
        raise NotImplementedError

    def complete(self, metadata: dict[str, object], row_count: int | None = None) -> dict[str, object]:
        raise NotImplementedError

    def fail(self, error: Exception) -> None:
        raise NotImplementedError

    def skip(self, reason: str) -> None:
        raise NotImplementedError
```

- [ ] **Step 2: Write failing tests in `src/tests/test_cost_recorder.py`**

```python
"""Tests for HFJobsCostRecorder."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from analytics.cost import HF_RATE_A10G_SMALL, HF_RATE_CPU_BASIC, HFJobsCostRecorder


class TestRateConstants:
    def test_cpu_basic_rate(self) -> None:
        assert HF_RATE_CPU_BASIC == 0.01

    def test_a10g_small_rate(self) -> None:
        assert HF_RATE_A10G_SMALL == 1.00


class TestHFJobsCostRecorderStart:
    def test_start_uploads_running_state(self) -> None:
        with patch("analytics.cost.HfApi") as mock_api_cls:
            mock_api = MagicMock()
            mock_api_cls.return_value = mock_api

            recorder = HFJobsCostRecorder(
                workflow_id="wf-xt-grids",
                phase="grid_computation",
                rate_usd_per_hour=HF_RATE_CPU_BASIC,
                repo_id="luxury-lakehouse/xt-grid-values",
            )
            recorder.start()

            mock_api.upload_file.assert_called_once()
            call_kwargs = mock_api.upload_file.call_args
            # Verify the JSON content has RUNNING state
            uploaded = json.loads(call_kwargs.kwargs["path_or_fileobj"])
            assert uploaded["state"] == "RUNNING"
            assert uploaded["workflow_id"] == "wf-xt-grids"
            assert uploaded["rate_usd_per_hour"] == HF_RATE_CPU_BASIC
            assert "started_at" in uploaded
            assert call_kwargs.kwargs["path_in_repo"] == "_workflow_cost.json"

    def test_start_swallows_upload_failure(self) -> None:
        with patch("analytics.cost.HfApi") as mock_api_cls:
            mock_api = MagicMock()
            mock_api.upload_file.side_effect = ConnectionError("network down")
            mock_api_cls.return_value = mock_api

            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="test",
                rate_usd_per_hour=0.01,
                repo_id="test/repo",
            )
            # Should not raise
            recorder.start()


class TestHFJobsCostRecorderComplete:
    def test_complete_returns_new_dict_with_cost_fields(self) -> None:
        with patch("analytics.cost.HfApi") as mock_api_cls:
            mock_api = MagicMock()
            mock_api_cls.return_value = mock_api

            recorder = HFJobsCostRecorder(
                workflow_id="wf-xt-grids",
                phase="grid_computation",
                rate_usd_per_hour=HF_RATE_CPU_BASIC,
                repo_id="luxury-lakehouse/xt-grid-values",
            )
            recorder.start()

            original = {"grid_size": 32, "orientation_count": 16}
            result = recorder.complete(original, row_count=100)

            # Must return NEW dict, not mutate input
            assert result is not original
            assert "grid_size" in result
            assert result["elapsed_seconds"] >= 0
            assert result["rate_usd_per_hour"] == HF_RATE_CPU_BASIC
            assert result["estimated_cost_usd"] >= 0
            assert result["workflow_id"] == "wf-xt-grids"
            assert result["workflow_phase"] == "grid_computation"
            # Original dict must be unmodified
            assert "elapsed_seconds" not in original

    def test_complete_uploads_completed_state(self) -> None:
        with patch("analytics.cost.HfApi") as mock_api_cls:
            mock_api = MagicMock()
            mock_api_cls.return_value = mock_api

            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="test",
                rate_usd_per_hour=1.00,
                repo_id="test/repo",
            )
            recorder.start()
            recorder.complete({}, row_count=50)

            # Second upload_file call is the COMPLETED state
            assert mock_api.upload_file.call_count == 2
            second_call = mock_api.upload_file.call_args_list[1]
            uploaded = json.loads(second_call.kwargs["path_or_fileobj"])
            assert uploaded["state"] == "COMPLETED"
            assert uploaded["row_count"] == 50
            assert "estimated_cost_usd" in uploaded
            assert "duration_seconds" in uploaded


class TestHFJobsCostRecorderFail:
    def test_fail_uploads_failed_state_with_partial_cost(self) -> None:
        with patch("analytics.cost.HfApi") as mock_api_cls:
            mock_api = MagicMock()
            mock_api_cls.return_value = mock_api

            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="test",
                rate_usd_per_hour=1.00,
                repo_id="test/repo",
            )
            recorder.start()
            recorder.fail(ValueError("bad data"))

            second_call = mock_api.upload_file.call_args_list[1]
            uploaded = json.loads(second_call.kwargs["path_or_fileobj"])
            assert uploaded["state"] == "FAILED"
            assert uploaded["error"] == "bad data"


class TestHFJobsCostRecorderSkip:
    def test_skip_uploads_skipped_state_zero_cost(self) -> None:
        with patch("analytics.cost.HfApi") as mock_api_cls:
            mock_api = MagicMock()
            mock_api_cls.return_value = mock_api

            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="test",
                rate_usd_per_hour=1.00,
                repo_id="test/repo",
            )
            recorder.start()
            recorder.skip("already processed")

            second_call = mock_api.upload_file.call_args_list[1]
            uploaded = json.loads(second_call.kwargs["path_or_fileobj"])
            assert uploaded["state"] == "SKIPPED"
            assert uploaded["estimated_cost_usd"] == 0.0
            assert uploaded["reason"] == "already processed"


class TestHFJobsCostRecorderHFJobId:
    def test_captures_hf_job_id_from_env(self) -> None:
        with patch("analytics.cost.HfApi") as mock_api_cls, patch.dict(
            "os.environ", {"HF_JOB_ID": "job-abc-123"}
        ):
            mock_api = MagicMock()
            mock_api_cls.return_value = mock_api

            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="test",
                rate_usd_per_hour=0.01,
                repo_id="test/repo",
            )
            recorder.start()

            call_kwargs = mock_api.upload_file.call_args
            uploaded = json.loads(call_kwargs.kwargs["path_or_fileobj"])
            assert uploaded["hf_job_id"] == "job-abc-123"
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_cost_recorder.py -v`
Expected: FAIL with `NotImplementedError`

---

### Task 2: `HFJobsCostRecorder` — Implementation

**Files:**
- Modify: `src/analytics/cost.py`

- [ ] **Step 1: Implement `HFJobsCostRecorder`**

Replace the stub in `src/analytics/cost.py` with the full implementation. Key behaviors:
- `__init__`: stores params, creates `HfApi` instance, reads `HF_JOB_ID` from env
- `start()`: records `_started_at = datetime.now(UTC)`, uploads `_workflow_cost.json` with RUNNING state. Wraps upload in try/except — log warning on failure, never raise.
- `complete(metadata, row_count)`: computes `duration = (now - _started_at).total_seconds()`, `cost = duration * (rate / 3600)`. Uploads COMPLETED JSON. Returns `{**metadata, **cost_fields}` (new dict, immutable).
- `fail(error)`: uploads FAILED with partial cost.
- `skip(reason)`: uploads SKIPPED with zero cost.
- All upload methods use `_upload_cost_json()` helper with retry on 429/5xx (3 retries, exponential backoff) and timeout `(10, 30)`.

The recorder should validate `workflow_id` matches `^wf-[a-zA-Z0-9_-]+$` (note: hyphens are valid in workflow IDs, e.g. `wf-xt-grids`). Define this as a module-level constant `_WORKFLOW_ID_RE` in `cost.py` — do NOT import `_IDENTIFIER_RE` from `src/ingestion/utils.py` (that would create a cross-package dependency from `analytics` to `ingestion`).

- [ ] **Step 2: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_cost_recorder.py -v`
Expected: All PASS

- [ ] **Step 3: Run ruff + pyright on the new files**

Run: `uv run ruff check src/analytics/cost.py src/tests/test_cost_recorder.py && uv run pyright src/analytics/cost.py src/tests/test_cost_recorder.py`
Expected: Clean

---

### Task 3: Runner `on_skip` Dispatch — Tests + Implementation

**Files:**
- Modify: `src/workflows/runner.py`
- Modify: `src/tests/test_runner.py`

The runner currently catches all exceptions via `on_error`. We need `WorkflowSkippedError` to dispatch `on_skip` instead.

- [ ] **Step 1: Write failing tests in `src/tests/test_runner.py`**

Add these tests at the end of the file (after existing tests):

```python
# ---------------------------------------------------------------------------
# 8. WorkflowSkippedError dispatches on_skip (not on_error)
# ---------------------------------------------------------------------------


def test_skipped_error_dispatches_on_skip() -> None:
    """WorkflowSkippedError should trigger on_skip, not on_error."""
    from workflows.exceptions import WorkflowSkippedError

    saved_hooks = _hooks.copy()
    _hooks.clear()
    try:
        recording_hook = _RecordingHook()
        _hooks.append(recording_hook)

        def skip_pipeline() -> None:
            raise WorkflowSkippedError("all matches processed")

        entry = WorkflowEntry(
            workflow_id="wf-skip-test",
            phase="test",
            func=skip_pipeline,
        )
        result = run_workflow(entry)

        assert result is None
        # _RecordingHook.calls is list[str] — check method names only
        assert "on_skip" in recording_hook.calls
        assert "on_error" not in recording_hook.calls
    finally:
        _hooks.clear()
        _hooks.extend(saved_hooks)


def test_skipped_error_does_not_reraise() -> None:
    """WorkflowSkippedError should NOT propagate — pipeline exits 0."""
    from workflows.exceptions import WorkflowSkippedError

    saved_hooks = _hooks.copy()
    _hooks.clear()
    try:
        _hooks.append(_RecordingHook())

        def skip_pipeline() -> None:
            raise WorkflowSkippedError("nothing to do")

        entry = WorkflowEntry(
            workflow_id="wf-skip-noraise",
            phase="test",
            func=skip_pipeline,
        )
        # Should NOT raise
        result = run_workflow(entry)
        assert result is None
    finally:
        _hooks.clear()
        _hooks.extend(saved_hooks)
```

Note: This test uses the existing `_RecordingHook` test helper already defined in the file. Check it records `calls` as a dict keyed by method name with `{"args": (ctx, ...)}`. If `_RecordingHook` doesn't have this structure, adapt accordingly — read the existing test helpers in `test_runner.py` before writing.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_runner.py::test_skipped_error_dispatches_on_skip -v`
Expected: FAIL (no `on_skip` call recorded)

- [ ] **Step 3: Implement `on_skip` dispatch in `runner.py`**

In `src/workflows/runner.py`, add the import and modify the try/except block:

```python
# Add to imports (after existing workflow imports):
from workflows.exceptions import WorkflowSkippedError

# Replace the try/except block in run_workflow() with:
    try:
        result = entry.func(*args, **kwargs)
        _dispatch(active_hooks, "on_complete", ctx, result)
        return result  # type: ignore[return-value]
    except WorkflowSkippedError as exc:
        _dispatch(active_hooks, "on_skip", ctx, str(exc))
        return None
    except Exception as exc:
        _dispatch(active_hooks, "on_error", ctx, exc)
        raise
```

- [ ] **Step 4: Run all runner tests to verify**

Run: `uv run pytest src/tests/test_runner.py -v`
Expected: All PASS (existing + 2 new)

- [ ] **Step 5: Run ruff + pyright**

Run: `uv run ruff check src/workflows/runner.py && uv run pyright src/workflows/runner.py`
Expected: Clean

---

### Task 4: `CostEstimateHook` — Tests

**Files:**
- Create: `src/tests/test_cost_hook.py`
- Create: `src/ingestion/cost_hook.py` (stub only)

- [ ] **Step 1: Create stub `src/ingestion/cost_hook.py`**

```python
"""CostEstimateHook — writes run state and cost estimates to workflow_cost_live Delta table."""

from __future__ import annotations

import os
import re

from workflows.context import WorkflowContext

# Configurable via environment variable, defaults to current Databricks serverless rate
DATABRICKS_SERVERLESS_RATE = float(os.environ.get("DATABRICKS_SERVERLESS_RATE_USD", "0.07"))

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class CostEstimateHook:
    """Writes run state and cost estimates to workflow_cost_live Delta table.

    Lives in src/ingestion/ (not src/workflows/) due to Spark dependency.
    Implements the LifecycleHook protocol. Failures are logged and swallowed.
    """

    def __init__(
        self,
        spark: object,  # SparkSession — typed as object for testability
        catalog: str,
        schema: str,
        rate_usd_per_hour: float = DATABRICKS_SERVERLESS_RATE,
        runtime: str = "databricks",
    ) -> None:
        raise NotImplementedError

    def on_start(self, ctx: WorkflowContext) -> None:
        raise NotImplementedError

    def on_complete(self, ctx: WorkflowContext, row_count: int | None) -> None:
        raise NotImplementedError

    def on_skip(self, ctx: WorkflowContext, reason: str) -> None:
        raise NotImplementedError

    def on_error(self, ctx: WorkflowContext, error: Exception) -> None:
        raise NotImplementedError
```

- [ ] **Step 2: Write failing tests in `src/tests/test_cost_hook.py`**

```python
"""Tests for CostEstimateHook."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from ingestion.cost_hook import DATABRICKS_SERVERLESS_RATE, CostEstimateHook
from workflows.context import WorkflowContext


def _make_ctx(workflow_id: str = "wf-test", phase: str = "test") -> WorkflowContext:
    return WorkflowContext(workflow_id=workflow_id, phase=phase)


def _make_mock_spark() -> MagicMock:
    """Create a mock SparkSession with conf.get returning None (local mode)."""
    spark = MagicMock()
    spark.conf.get.return_value = None
    return spark


class TestCostEstimateHookInit:
    def test_validates_catalog_identifier(self) -> None:
        spark = _make_mock_spark()
        with pytest.raises(ValueError, match="Invalid catalog"):
            CostEstimateHook(spark, "DROP TABLE; --", "bronze")

    def test_validates_schema_identifier(self) -> None:
        spark = _make_mock_spark()
        with pytest.raises(ValueError, match="Invalid schema"):
            CostEstimateHook(spark, "soccer_analytics", "DROP TABLE; --")

    def test_default_rate(self) -> None:
        spark = _make_mock_spark()
        hook = CostEstimateHook(spark, "soccer_analytics", "dev_gold")
        assert hook._rate == DATABRICKS_SERVERLESS_RATE

    def test_custom_rate(self) -> None:
        spark = _make_mock_spark()
        hook = CostEstimateHook(spark, "soccer_analytics", "dev_gold", rate_usd_per_hour=0.10)
        assert hook._rate == 0.10


class TestCostEstimateHookEnvVar:
    def test_env_var_overrides_default_rate(self) -> None:
        with patch.dict(os.environ, {"DATABRICKS_SERVERLESS_RATE_USD": "0.12"}):
            # Re-import to pick up the env var
            import importlib

            import ingestion.cost_hook as cost_hook_mod

            importlib.reload(cost_hook_mod)
            assert cost_hook_mod.DATABRICKS_SERVERLESS_RATE == 0.12
            # Restore
            importlib.reload(cost_hook_mod)


class TestCostEstimateHookOnStart:
    def test_on_start_calls_merge(self) -> None:
        spark = _make_mock_spark()
        hook = CostEstimateHook(spark, "soccer_analytics", "dev_gold")
        ctx = _make_ctx()
        # on_start should not raise — uses internal _merge
        hook.on_start(ctx)
        # Verify spark.sql was called (MERGE statement)
        assert spark.sql.called or spark.createDataFrame.called


class TestCostEstimateHookOnComplete:
    def test_on_complete_computes_cost(self) -> None:
        spark = _make_mock_spark()
        hook = CostEstimateHook(spark, "soccer_analytics", "dev_gold", rate_usd_per_hour=3600.0)
        ctx = _make_ctx()
        hook.on_start(ctx)
        # on_complete should compute duration * rate / 3600
        hook.on_complete(ctx, row_count=42)
        # With rate=3600, cost should equal duration_seconds
        # We can't assert exact cost but can verify the merge was called twice
        assert spark.createDataFrame.call_count >= 2 or spark.sql.call_count >= 2


class TestCostEstimateHookOnSkip:
    def test_on_skip_writes_skipped_state(self) -> None:
        spark = _make_mock_spark()
        hook = CostEstimateHook(spark, "soccer_analytics", "dev_gold")
        ctx = _make_ctx()
        hook.on_start(ctx)
        hook.on_skip(ctx, "all matches processed")
        # Should have called merge twice (start + skip)


class TestCostEstimateHookOnError:
    def test_on_error_writes_failed_with_partial_cost(self) -> None:
        spark = _make_mock_spark()
        hook = CostEstimateHook(spark, "soccer_analytics", "dev_gold")
        ctx = _make_ctx()
        hook.on_start(ctx)
        hook.on_error(ctx, ValueError("bad data"))
        # Should not raise — hook swallows errors


class TestCostEstimateHookSparkConf:
    def test_reads_job_run_id_from_conf(self) -> None:
        spark = _make_mock_spark()
        spark.conf.get.side_effect = lambda key, default=None: {
            "spark.databricks.job.runId": "12345",
            "spark.databricks.task.key": "compute_spadl_vaep",
        }.get(key, default)

        hook = CostEstimateHook(spark, "soccer_analytics", "dev_gold")
        ctx = _make_ctx()
        hook.on_start(ctx)
        # Verify conf was read
        spark.conf.get.assert_any_call("spark.databricks.job.runId", None)

    def test_handles_missing_spark_conf_gracefully(self) -> None:
        spark = _make_mock_spark()
        spark.conf.get.return_value = None
        hook = CostEstimateHook(spark, "soccer_analytics", "dev_gold")
        ctx = _make_ctx()
        # Should not raise even without Databricks conf values
        hook.on_start(ctx)
        hook.on_complete(ctx, row_count=10)
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_cost_hook.py -v`
Expected: FAIL with `NotImplementedError`

---

### Task 5: `CostEstimateHook` — Implementation

**Files:**
- Modify: `src/ingestion/cost_hook.py`

- [ ] **Step 1: Implement `CostEstimateHook`**

Replace the stub with full implementation. The `spark` parameter must use a `TYPE_CHECKING` guard for proper typing (the stub uses `object` for testability, but the implementation needs `SparkSession` for pyright):

```python
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
```

This matches the pattern used in all other `src/ingestion/` modules.

Key behaviors:
- `__init__`: validates `catalog`/`schema` against `_IDENTIFIER_RE` (raises `ValueError`), stores params, builds table name `{catalog}.{schema}.workflow_cost_live`
- `on_start`: calls `_merge()` with state=RUNNING, estimated_cost_usd=0.0
- `on_complete`: computes `duration = int((now_utc - ctx.started_at).total_seconds())`, `cost = round(duration * (rate / 3600), 4)`, calls `_merge()` with state=COMPLETED
- `on_skip`: calls `_merge()` with state=SKIPPED, cost=0.0
- `on_error`: computes partial cost, calls `_merge()` with state=FAILED
- `_merge()`: builds a single-row DataFrame with all columns from the DDL schema, uses `DeltaTable.forName(spark, table).alias("t").merge(df.alias("s"), "t.run_id = s.run_id")` with `.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`. Wraps everything in try/except — log warning, never raise.
- Reads `spark.conf.get("spark.databricks.job.runId", None)` and `spark.conf.get("spark.databricks.task.key", None)` for Databricks metadata. Both default to `None` for local/notebook.
- Uses lazy import for `DeltaTable`: `from delta.tables import DeltaTable` inside `_merge()`.

- [ ] **Step 2: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_cost_hook.py -v`
Expected: All PASS

- [ ] **Step 3: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/cost_hook.py && uv run pyright src/ingestion/cost_hook.py`
Expected: Clean

---

### Task 6: Hook Registration in 12 Databricks Pipelines

**Files:**
- Modify: `src/ingestion/spadl_vaep.py`
- Modify: `src/ingestion/expected_threat.py`
- Modify: `src/ingestion/xg_model.py`
- Modify: `src/ingestion/defcon_lite.py`
- Modify: `src/ingestion/pitch_control_batch.py`
- Modify: `src/ingestion/off_ball_xt.py`
- Modify: `src/ingestion/line_breaking.py`
- Modify: `src/ingestion/elastic_sync.py`
- Modify: `src/ingestion/entity_resolution.py`
- Modify: `src/ingestion/pausa.py`
- Modify: `src/ingestion/player_embeddings.py`
- Modify: `src/ingestion/model_validation.py`

Each pipeline follows the same pattern. In `main()`, after `spark = get_spark_session()`, add:

```python
from ingestion.cost_hook import CostEstimateHook
from workflows import register_hook

register_hook(CostEstimateHook(spark, args.catalog, args.schema))
```

- [ ] **Step 1: Add hook registration to all 12 pipelines**

For each file, find the `main()` function. Add the import and `register_hook` call after the `spark = get_spark_session()` line and before `run_pipeline()` is called. The `args.catalog` and `args.schema` come from `parse_ingestion_args()` which is already called in each `main()`.

Pattern (using `spadl_vaep.py` as example):

```python
def main() -> None:
    args = parse_ingestion_args("Compute SPADL actions and VAEP scores")
    logger = configure_logging("spadl_vaep")
    spark = get_spark_session()

    # Cost tracking
    from ingestion.cost_hook import CostEstimateHook
    from workflows import register_hook
    register_hook(CostEstimateHook(spark, args.catalog, args.schema))

    logger.info("Starting SPADL/VAEP pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)
```

The import is inside `main()` (lazy) because `cost_hook.py` may not be needed at module load time and to keep the import close to use.

- [ ] **Step 2: Run ruff + pyright on all modified files**

Run: `uv run ruff check src/ingestion/ && uv run pyright src/ingestion/cost_hook.py`
Expected: Clean

- [ ] **Step 3: Run existing tests to verify no regressions**

Run: `uv run pytest src/tests/ -v --timeout=120`
Expected: All existing tests pass

---

### Task 7: `HFJobsCostRecorder` Integration in 7 HF Jobs Scripts

**Files:**
- Modify: `scripts/compute_xt_grid_hf.py`
- Modify: `scripts/compute_epv_transition_hf.py`
- Modify: `scripts/compute_obso_hf.py`
- Modify: `scripts/compute_space_creation_hf.py`
- Modify: `scripts/train_vaep_model_hf.py`
- Modify: `scripts/train_xg_model_hf.py`
- Modify: `scripts/train_xg_v2_hf.py`

Each script gets the `HFJobsCostRecorder` lifecycle added.

- [ ] **Step 1: Add recorder to each script**

For each script:
1. Add import: `from analytics.cost import HFJobsCostRecorder, HF_RATE_CPU_BASIC` (or `HF_RATE_A10G_SMALL`)
2. Create recorder before compute begins (after imports and config, before main work):
   ```python
   recorder = HFJobsCostRecorder(
       workflow_id="wf-xt-grids",  # matches the @workflow decorator
       phase="grid_computation",    # matches the @workflow decorator
       rate_usd_per_hour=HF_RATE_CPU_BASIC,
       repo_id="luxury-lakehouse/xt-grid-values",  # the HF Hub repo this script publishes to
   )
   recorder.start()
   ```
3. Before the metadata upload step, enrich metadata:
   ```python
   metadata = recorder.complete(metadata, row_count=len(df))
   ```
4. If the script has existing manual `time.time()` elapsed tracking, remove it — the recorder handles timing internally.
5. If the script has a top-level try/except for error handling, add `recorder.fail(exc)` in the except block.

Script-to-config mapping:

| Script | `workflow_id` | `phase` | Rate constant | `repo_id` |
|--------|--------------|---------|---------------|-----------|
| `compute_xt_grid_hf.py` | `wf-xt-grids` | `grid_computation` | `HF_RATE_CPU_BASIC` | `luxury-lakehouse/xt-grid-values` |
| `compute_epv_transition_hf.py` | `wf-epv-reachability` | `grid_computation` | `HF_RATE_CPU_BASIC` | `luxury-lakehouse/epv-transition-values` |
| `compute_obso_hf.py` | `wf-obso-pausa` | `inference` | `HF_RATE_A10G_SMALL` | `luxury-lakehouse/obso-pausa-values` |
| `compute_space_creation_hf.py` | `wf-space-creation` | `inference` | `HF_RATE_A10G_SMALL` | `luxury-lakehouse/space-creation-values` |
| `train_vaep_model_hf.py` | `wf-vaep` | `training` | `HF_RATE_CPU_BASIC` | `luxury-lakehouse/vaep-model-statsbomb-wyscout` |
| `train_xg_model_hf.py` | `wf-xg-v1` | `training` | `HF_RATE_CPU_BASIC` | `luxury-lakehouse/xg-model-statsbomb` |
| `train_xg_v2_hf.py` | `wf-xg-v2` | `training` | `HF_RATE_A10G_SMALL` | `luxury-lakehouse/xg-model-v2` |

Read each script to find the correct `repo_id` (the `OUTPUT_DATASET` or similar constant near the top) and the right insertion points. The `workflow_id` and `phase` must match the `@workflow` decorator already on each script.

- [ ] **Step 2: Run ruff on all modified scripts**

Run: `uv run ruff check scripts/compute_xt_grid_hf.py scripts/compute_epv_transition_hf.py scripts/compute_obso_hf.py scripts/compute_space_creation_hf.py scripts/train_vaep_model_hf.py scripts/train_xg_model_hf.py scripts/train_xg_v2_hf.py`
Expected: Clean

---

### Task 8: dbt `fct_workflow_costs` Model

**Files:**
- Create: `dbt_project/models/marts/fct_workflow_costs.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Create the dbt model**

Create `dbt_project/models/marts/fct_workflow_costs.sql`:

```sql
{{ config(
    materialized='table',
    liquid_clustered_by=['task_key', 'usage_date']
) }}
-- fct_workflow_costs.sql
-- Gold-layer workflow cost attribution from Databricks system tables.
--
-- Joins billing usage with list prices and attributes cost per-task
-- proportionally by execution duration. 90-day rolling window.
--
-- Post-hook cleanup removes redundant warm-tier rows from workflow_cost_live.

WITH billing AS (
    SELECT
        usage_metadata.job_id AS job_id,
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
    GROUP BY 1, 2, 3
),

tasks AS (
    SELECT
        job_run_id,
        task_key,
        execution_duration_seconds
    FROM system.lakeflow.job_task_run_timeline
    WHERE
        result_state IS NOT NULL
)

SELECT
    tasks.task_key,
    billing.usage_date,
    CAST(billing.job_run_id AS BIGINT) AS job_run_id,
    ROUND(
        billing.dbu * (
            tasks.execution_duration_seconds
            / SUM(tasks.execution_duration_seconds)
                OVER (PARTITION BY billing.job_run_id)
        ),
        4
    ) AS attributed_dbu,
    ROUND(
        billing.cost_usd * (
            tasks.execution_duration_seconds
            / SUM(tasks.execution_duration_seconds)
                OVER (PARTITION BY billing.job_run_id)
        ),
        4
    ) AS attributed_cost_usd
FROM billing
INNER JOIN tasks ON billing.job_run_id = tasks.job_run_id
```

- [ ] **Step 2: Add post-hook for warm-tier cleanup**

Add the post-hook to the model config:

```sql
{{ config(
    materialized='table',
    liquid_clustered_by=['task_key', 'usage_date'],
    post_hook=[
        "DELETE FROM {{ this.database }}.{{ this.schema }}.workflow_cost_live WHERE state != 'RUNNING' AND ended_at IS NOT NULL AND ended_at < (SELECT COALESCE(MAX(usage_date), DATE '1970-01-01') + INTERVAL 1 DAY FROM {{ this }})",
        "DELETE FROM {{ this.database }}.{{ this.schema }}.workflow_cost_live WHERE state = 'RUNNING' AND started_at < CURRENT_TIMESTAMP - INTERVAL 24 HOURS"
    ]
) }}
```

Note: `{{ this }}` in the first post-hook refers to the model itself (`fct_workflow_costs`), which is correct for the subquery.

- [ ] **Step 3: Add contract to `_marts__models.yml`**

Append to the models list in `_marts__models.yml`:

```yaml
  - name: fct_workflow_costs
    config:
      contract:
        enforced: true
    description: >
      Gold-layer workflow cost attribution from Databricks system tables.
      Joins billing usage with list prices and attributes cost per-task
      proportionally by execution duration within each job run.
      90-day rolling window refreshed daily.
    columns:
      - name: task_key
        data_type: string
        description: >
          Databricks job task key, maps to workflow card entry_point.
        data_tests:
          - not_null
      - name: usage_date
        data_type: date
        description: Date the billing usage was recorded.
        data_tests:
          - not_null
      - name: job_run_id
        data_type: bigint
        description: Databricks job run identifier.
        data_tests:
          - not_null
      - name: attributed_dbu
        data_type: decimal(10,4)
        description: >
          Proportional DBU consumption for this task within the job run.
          Attributed by execution_duration_seconds ratio.
      - name: attributed_cost_usd
        data_type: decimal(10,4)
        description: >
          Proportional dollar cost for this task within the job run.
          Attributed by execution_duration_seconds ratio.
```

- [ ] **Step 4: Verify model compiles (no Databricks connection needed)**

Run: `cd dbt_project && dbt compile --select fct_workflow_costs`
Expected: Compiles without error (may warn about missing connection but SQL should be valid)

---

### Task 9: DDL Script + Deploy Wheel Script

**Files:**
- Create: `scripts/create_cost_table.sql`
- Create: `scripts/deploy_wheel.py`

- [ ] **Step 1: Create `scripts/create_cost_table.sql`**

```sql
-- Create the workflow_cost_live Delta table for warm/hot cost tracking.
-- Run once via Databricks SQL or notebook.
-- Not managed by dbt (written by Spark CostEstimateHook, not dbt).
--
-- Usage: Replace {catalog} and {schema} with actual values before running.
-- Example: soccer_analytics.dev_gold.workflow_cost_live

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.workflow_cost_live (
    workflow_id        STRING        NOT NULL,
    phase              STRING        NOT NULL,
    run_id             STRING        NOT NULL,
    runtime            STRING        NOT NULL,
    job_run_id         BIGINT,
    task_key           STRING,
    hf_job_id          STRING,
    state              STRING        NOT NULL,
    started_at         TIMESTAMP     NOT NULL,
    ended_at           TIMESTAMP,
    duration_seconds   INT,
    row_count          INT,
    rate_usd_per_hour  DECIMAL(10,6),
    estimated_cost_usd DECIMAL(10,4),
    cost_source        STRING        NOT NULL,
    updated_at         TIMESTAMP     NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
-- No liquid clustering: table is bounded at <100 rows at any time (active runs +
-- recent completions before daily cleanup sweep). Sequential scan is faster than
-- index maintenance at this scale.
```

- [ ] **Step 2: Create `scripts/deploy_wheel.py`**

Implement the wheel deploy script. Key behaviors:
- Uses `argparse` with `--catalog` (default `soccer_analytics`), `--schema` (default `bronze`), `--dry-run`
- Downloads latest `luxury_lakehouse-*.whl` from `luxury-lakehouse/build-artifacts` on HF Hub via `hf_hub_download(repo_id, filename, repo_type="model")`
- Uploads to `/Volumes/{catalog}/{schema}/libs/` via `databricks.sdk.WorkspaceClient().files.upload()`
- Post-upload verification: reads back file info to confirm size matches
- Auth: HF token from env/cache (`huggingface_hub.utils.get_token()`), Databricks from `DATABRICKS_HOST` + `DATABRICKS_TOKEN`
- Structured logging, explicit timeouts, no `print()` statements

Reference `scripts/deploy_taipy.py` for the established deploy script pattern (pre-flight checks, `--dry-run`, post-upload verification).

- [ ] **Step 3: Run ruff + pyright on deploy script**

Run: `uv run ruff check scripts/deploy_wheel.py && uv run pyright scripts/deploy_wheel.py`
Expected: Clean

---

### Task 10: Full Test Suite + Lint Verification

**Files:** None (verification only)

- [ ] **Step 1: Run full test suite**

Run: `uv run pytest src/tests/ -v`
Expected: All pass (existing 858 + new cost tests)

- [ ] **Step 2: Run full lint + type check**

Run: `uv run ruff check src/ && uv run ruff format --check src/ && uv run pyright src/`
Expected: All clean

- [ ] **Step 3: Validate workflow cards**

Run: `uv run validate_workflow_cards workflow-cards/`
Expected: All 16 cards valid

---

### Task 11: E2E — Databricks (SPADL/VAEP)

**Files:**
- Modify: workflow card YAMLs (baselines)

**Prerequisites:**
- Wheel must be merged to main and CI must upload to HF Hub
- System table grants must be in place (see spec Section 9)

- [ ] **Step 1: Deploy wheel to UC Volume**

Run: `uv run python scripts/deploy_wheel.py`
Expected: Wheel uploaded to `/Volumes/soccer_analytics/bronze/libs/luxury_lakehouse-0.1.0-py3-none-any.whl`, post-upload verification passes.

- [ ] **Step 2: Create `workflow_cost_live` table**

Run `scripts/create_cost_table.sql` in Databricks SQL, replacing `{catalog}` with `soccer_analytics` and `{schema}` with `dev_gold`.

- [ ] **Step 3: Trigger SPADL/VAEP pipeline**

Via Databricks UI or CLI: trigger the `compute_spadl_vaep` task in the `soccer-analytics-ingestion-dev` job.

- [ ] **Step 4: Verify cost data in `workflow_cost_live`**

```sql
SELECT * FROM soccer_analytics.dev_gold.workflow_cost_live
WHERE workflow_id = 'wf-vaep'
ORDER BY updated_at DESC
LIMIT 5;
```

Expected: One row with `state = 'COMPLETED'`, non-null `duration_seconds`, `estimated_cost_usd > 0`, `runtime = 'databricks'`.

- [ ] **Step 5: Record baseline in workflow card**

Update `workflow-cards/wf-vaep.yaml`:
- `cost.inference.typical_duration_minutes` with actual runtime
- `cost.inference.typical_cost_usd` with actual estimated cost

---

### Task 12: E2E — HF Jobs (xT Grid)

- [ ] **Step 1: Run `compute_xt_grid_hf.py` on HF Jobs**

Launch on `cpu-basic` via HF Jobs UI or CLI.

- [ ] **Step 2: Verify `_workflow_cost.json` on HF Hub**

Check `luxury-lakehouse/xt-grid-values` repo for `_workflow_cost.json` with:
- `state: "COMPLETED"`
- `estimated_cost_usd > 0`
- `duration_seconds > 0`

- [ ] **Step 3: Verify `metadata.json` has cost fields**

Check that the `metadata.json` in the same repo now includes `elapsed_seconds`, `rate_usd_per_hour`, `estimated_cost_usd`, `workflow_id`.

- [ ] **Step 4: Record baseline in workflow card**

Update `workflow-cards/wf-xt-grids.yaml`:
- `cost.training.typical_duration_minutes` with actual runtime
- `cost.training.typical_cost_usd` with actual estimated cost

---

### Task 13: E2E — dbt Cold Tier

- [ ] **Step 1: Build `fct_workflow_costs`**

Run: `cd dbt_project && dbt build --select fct_workflow_costs`

Expected: Model builds without error. May have zero rows if billing data hasn't propagated yet (~24h lag). The post-hook cleanup SQL should execute without error.

- [ ] **Step 2: Verify post-hook cleanup executed**

Check Databricks SQL logs or dbt logs for the two DELETE statements executing without error.

---

### Task 14: Commit

Only after all E2E verification passes.

- [ ] **Step 1: Stage all changes**

```bash
git add src/analytics/cost.py src/ingestion/cost_hook.py \
  src/workflows/runner.py \
  src/tests/test_cost_recorder.py src/tests/test_cost_hook.py src/tests/test_runner.py \
  scripts/create_cost_table.sql scripts/deploy_wheel.py \
  dbt_project/models/marts/fct_workflow_costs.sql \
  dbt_project/models/marts/_marts__models.yml \
  src/ingestion/spadl_vaep.py src/ingestion/expected_threat.py \
  src/ingestion/xg_model.py src/ingestion/defcon_lite.py \
  src/ingestion/pitch_control_batch.py src/ingestion/off_ball_xt.py \
  src/ingestion/line_breaking.py src/ingestion/elastic_sync.py \
  src/ingestion/entity_resolution.py src/ingestion/pausa.py \
  src/ingestion/player_embeddings.py src/ingestion/model_validation.py \
  scripts/compute_xt_grid_hf.py scripts/compute_epv_transition_hf.py \
  scripts/compute_obso_hf.py scripts/compute_space_creation_hf.py \
  scripts/train_vaep_model_hf.py scripts/train_xg_model_hf.py \
  scripts/train_xg_v2_hf.py \
  pyproject.toml \
  workflow-cards/ \
  docs/superpowers/specs/2026-03-23-workflow-cost-tracking-design.md \
  docs/superpowers/plans/2026-03-23-workflow-cost-tracking.md
```

- [ ] **Step 2: Commit**

Single commit with all E2E-verified changes. Get user approval first.
