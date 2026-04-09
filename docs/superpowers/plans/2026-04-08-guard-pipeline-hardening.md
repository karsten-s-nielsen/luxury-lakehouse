# Guard & Pipeline Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 5 production guard/pipeline bugs (D48-D50, D46-D47), add 3 cross-cutting TDD conformance guarantees, and parallelize the freshness gate (D40e).

**Architecture:** TDD-first — all conformance tests written before implementation (start red, turn green as fixes land). Fixes ordered by dependency: schema fixes (D50) -> import isolation (D49) -> backfill chunking (D48) -> guard promotions (D47) -> cleanup (D46) -> parallelization (D40e).

**Tech Stack:** Python 3.10, pytest, PySpark (mocked), AST analysis, `concurrent.futures.ThreadPoolExecutor`, Delta Lake MERGE, dbt seeds.

**Spec:** `docs/superpowers/specs/2026-04-08-guard-pipeline-hardening-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `src/tests/test_guard_conformance.py` | Modify | +5 test classes (TDD reqs + D46 consistency + D49 runtime isolation) |
| `src/tests/test_freshness_gate.py` | Modify | +1 test class (task value propagation) + 2 parallelization tests |
| `src/ingestion/spadl_vaep.py` | Modify | 1-line schema fix (D50) |
| `src/ingestion/line_breaking.py` | Modify | 1-line schema fix (D50) |
| `src/ingestion/defcon_lite_common.py` | Modify | Break import chain (D49) |
| `src/ingestion/defcon_lite_360.py` | Modify | Inline `_TABLE_NAME` (D49) |
| `src/ingestion/defcon_lite_tracking.py` | Modify | Inline `_TABLE_NAME` (D49) |
| `src/ingestion/statsbomb.py` | Modify | Chunk MERGE, re-raise exceptions (D48) |
| `src/ingestion/statsbomb_backfill_extra.py` | Modify | Full guard, fix condition (D47+D48) |
| `src/ingestion/statsbomb_backfill_360.py` | Modify | Pass entity IDs through metadata (D47) |
| `src/ingestion/entity_resolution.py` | Modify | Full guard with `find_new_ids` (D47) |
| `src/ingestion/tracking_metadata.py` | Modify | Full guard with `find_new_ids` (D47) |
| `src/ingestion/formations_shape_graph.py` | Modify | Fix workflow_id (D46) |
| `src/ingestion/export_scoutgpt_training_data.py` | Modify | Remove orphaned decorator (D46) |
| `src/ingestion/freshness_gate.py` | Modify | ThreadPoolExecutor parallelization (D40e) |
| `dbt_project/seeds/task_workflow_mapping.csv` | Modify | +4 rows (D46) |

---

## Task 1: TDD — `TestFreshnessGateTaskValuePropagation`

Tests that the freshness gate writes guard results as Databricks task values faithfully.

**Files:**
- Modify: `src/tests/test_freshness_gate.py`

- [ ] **Step 1: Write the test class**

Add after the existing `TestRunGate` class (line 73):

```python
import json

from ingestion.guards import FilterResult


class TestFreshnessGateTaskValuePropagation:
    """FilterResults must be faithfully written as Databricks task values."""

    def test_write_task_values_two_keys_per_workflow(self) -> None:
        """Each workflow gets {wf_id} (JSON) and {wf_id}-count (int)."""
        from unittest.mock import patch

        from ingestion.freshness_gate import _write_task_values

        result = FilterResult(
            workflow_id="wf-test",
            count=5,
            metadata={"new_match_ids": ["m1", "m2", "m3", "m4", "m5"]},
        )

        mock_dbutils = MagicMock()
        mock_spark = MagicMock()

        with (
            patch("ingestion.freshness_gate.DBUtils", mock_dbutils, create=True),
            patch("pyspark.sql.SparkSession.getActiveSession", return_value=mock_spark),
            patch.dict("sys.modules", {"pyspark.dbutils": MagicMock(DBUtils=mock_dbutils)}),
        ):
            mock_dbutils.return_value = mock_dbutils
            _write_task_values({"wf-test": result})

        calls = mock_dbutils.jobs.taskValues.set.call_args_list
        assert len(calls) == 2, f"Expected 2 set() calls, got {len(calls)}"

        # First call: JSON FilterResult
        json_call = calls[0]
        assert json_call.kwargs["key"] == "wf-test"

        # Second call: raw count integer
        count_call = calls[1]
        assert count_call.kwargs["key"] == "wf-test-count"
        assert count_call.kwargs["value"] == 5

    def test_json_payload_preserves_metadata(self) -> None:
        """Metadata with entity IDs round-trips through JSON task value."""
        from unittest.mock import patch

        from ingestion.freshness_gate import _write_task_values

        ids = ["m1", "m2", "m3"]
        result = FilterResult(
            workflow_id="wf-test",
            count=3,
            metadata={"new_match_ids": ids},
        )

        mock_dbutils = MagicMock()
        mock_spark = MagicMock()

        with (
            patch("ingestion.freshness_gate.DBUtils", mock_dbutils, create=True),
            patch("pyspark.sql.SparkSession.getActiveSession", return_value=mock_spark),
            patch.dict("sys.modules", {"pyspark.dbutils": MagicMock(DBUtils=mock_dbutils)}),
        ):
            mock_dbutils.return_value = mock_dbutils
            _write_task_values({"wf-test": result})

        json_str = mock_dbutils.jobs.taskValues.set.call_args_list[0].kwargs["value"]
        parsed = json.loads(json_str)
        assert parsed["metadata"]["new_match_ids"] == ids
        assert parsed["count"] == 3

    def test_json_payload_preserves_chunks(self) -> None:
        """Chunks round-trip through JSON task value."""
        from unittest.mock import patch

        from ingestion.freshness_gate import _write_task_values

        chunks = [["m1", "m2"], ["m3"]]
        result = FilterResult(
            workflow_id="wf-test",
            count=3,
            chunks=chunks,
            metadata={"new_match_ids": ["m1", "m2", "m3"]},
        )

        mock_dbutils = MagicMock()
        mock_spark = MagicMock()

        with (
            patch("ingestion.freshness_gate.DBUtils", mock_dbutils, create=True),
            patch("pyspark.sql.SparkSession.getActiveSession", return_value=mock_spark),
            patch.dict("sys.modules", {"pyspark.dbutils": MagicMock(DBUtils=mock_dbutils)}),
        ):
            mock_dbutils.return_value = mock_dbutils
            _write_task_values({"wf-test": result})

        json_str = mock_dbutils.jobs.taskValues.set.call_args_list[0].kwargs["value"]
        parsed = json.loads(json_str)
        assert parsed["chunks"] == chunks

    def test_count_value_is_integer(self) -> None:
        """The -count key must be a plain int, not string or Decimal."""
        from unittest.mock import patch

        from ingestion.freshness_gate import _write_task_values

        result = FilterResult(workflow_id="wf-test", count=7)

        mock_dbutils = MagicMock()
        mock_spark = MagicMock()

        with (
            patch("ingestion.freshness_gate.DBUtils", mock_dbutils, create=True),
            patch("pyspark.sql.SparkSession.getActiveSession", return_value=mock_spark),
            patch.dict("sys.modules", {"pyspark.dbutils": MagicMock(DBUtils=mock_dbutils)}),
        ):
            mock_dbutils.return_value = mock_dbutils
            _write_task_values({"wf-test": result})

        count_val = mock_dbutils.jobs.taskValues.set.call_args_list[1].kwargs["value"]
        assert isinstance(count_val, int), f"Expected int, got {type(count_val).__name__}"

    def test_guard_exception_yields_count_zero_still_written(self) -> None:
        """Failed guard produces count=0 result which is still written as a task value."""
        from unittest.mock import patch

        from ingestion.freshness_gate import _write_task_values, run_gate

        guard_bad = MagicMock()
        guard_bad.workflow_id = "wf-bad"
        guard_bad.check.side_effect = RuntimeError("Boom")

        spark = MagicMock()
        results = run_gate(spark, "cat", "schema", guards={"wf-bad": guard_bad})

        # Gate caught the exception and produced count=0
        assert results["wf-bad"].count == 0

        mock_dbutils = MagicMock()
        mock_spark = MagicMock()

        with (
            patch("ingestion.freshness_gate.DBUtils", mock_dbutils, create=True),
            patch("pyspark.sql.SparkSession.getActiveSession", return_value=mock_spark),
            patch.dict("sys.modules", {"pyspark.dbutils": MagicMock(DBUtils=mock_dbutils)}),
        ):
            mock_dbutils.return_value = mock_dbutils
            _write_task_values(results)

        calls = mock_dbutils.jobs.taskValues.set.call_args_list
        assert len(calls) == 2  # JSON + count, even for count=0

    def test_read_gate_result_round_trip(self) -> None:
        """FilterResult survives write -> JSON -> read_gate_result round trip."""
        original = FilterResult(
            workflow_id="wf-round",
            count=4,
            chunks=[["a", "b"], ["c", "d"]],
            metadata={"new_match_ids": ["a", "b", "c", "d"]},
        )

        json_str = original.to_json()
        parsed = FilterResult.from_json(json_str)

        assert parsed.workflow_id == original.workflow_id
        assert parsed.count == original.count
        assert parsed.chunks == original.chunks
        assert parsed.metadata == original.metadata

    def test_standalone_mode_no_crash(self) -> None:
        """_write_task_values handles missing Spark/dbutils gracefully."""
        from unittest.mock import patch

        from ingestion.freshness_gate import _write_task_values

        result = FilterResult(workflow_id="wf-test", count=1)

        # Simulate standalone: pyspark.dbutils import fails
        with patch.dict("sys.modules", {"pyspark.dbutils": None}):
            _write_task_values({"wf-test": result})  # Must not raise
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
uv run pytest src/tests/test_freshness_gate.py -v
```

Expected: These tests should all PASS (they test existing behavior that works — we're locking it in).

- [ ] **Step 3: Commit**

```bash
git add src/tests/test_freshness_gate.py
git commit -m "test: add TestFreshnessGateTaskValuePropagation (TDD — task value chain)"
```

---

## Task 2: TDD — `TestExceptionPropagation`

Two-layer test: AST scan for try/except-without-raise in `run_pipeline`, plus behavioral test.

**Files:**
- Modify: `src/tests/test_guard_conformance.py`

- [ ] **Step 1: Write the AST-layer test class**

Add after `TestGuardImportIsolation` (after line 278):

```python
class TestExceptionPropagation:
    """run_pipeline must never silently swallow exceptions from its body.

    Layer 1: AST scan for try/except blocks that catch Exception without re-raise.
    Layer 2: Behavioral test verifying on_error fires when body raises.
    """

    def test_no_silent_exception_swallow_in_run_pipeline(self) -> None:
        """AST: run_pipeline bodies must not catch Exception without raise."""
        failures: list[str] = []

        for module_path in _GUARD_MODULES:
            if module_path in _NO_OWN_PIPELINE:
                continue
            mod = importlib.import_module(module_path)
            source_file = inspect.getfile(mod)
            source = Path(source_file).read_text(encoding="utf-8")
            tree = ast.parse(source)

            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if node.name != "run_pipeline":
                    continue

                # Walk the function body for try/except blocks
                for child in ast.walk(node):
                    if not isinstance(child, ast.ExceptHandler):
                        continue
                    # Check if it catches Exception or bare except
                    catches_broad = child.type is None or (
                        isinstance(child.type, ast.Name) and child.type.id == "Exception"
                    )
                    if not catches_broad:
                        continue

                    # Check for raise statement in handler body
                    has_raise = any(isinstance(stmt, ast.Raise) for stmt in ast.walk(child))
                    # Exempt: catching WorkflowSkippedError is fine
                    is_wse = child.type is not None and (
                        (isinstance(child.type, ast.Name) and child.type.id == "WorkflowSkippedError")
                        or (isinstance(child.type, ast.Attribute) and child.type.attr == "WorkflowSkippedError")
                    )
                    if not has_raise and not is_wse:
                        failures.append(
                            f"{module_path}:run_pipeline:{child.lineno}: "
                            f"catches Exception without raise"
                        )

        assert not failures, (
            "run_pipeline functions silently swallow exceptions:\n" + "\n".join(sorted(failures))
        )

    def test_helper_functions_propagate_exceptions(self) -> None:
        """AST: functions called by run_pipeline in the same module must re-raise.

        Scans the SAME source file as each run_pipeline for functions that
        catch Exception without raise. This catches helpers like
        backfill_extra_json that are in a different module but imported
        by the pipeline.
        """
        # Scan ALL modules that contain run_pipeline callees
        helper_modules = set()
        for module_path in _GUARD_MODULES:
            if module_path in _NO_OWN_PIPELINE:
                continue
            mod = importlib.import_module(module_path)
            source_file = inspect.getfile(mod)
            source = Path(source_file).read_text(encoding="utf-8")
            tree = ast.parse(source)

            # Find run_pipeline and collect names it calls
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if node.name != "run_pipeline":
                    continue

                for call_node in ast.walk(node):
                    if isinstance(call_node, ast.Call):
                        # from ingestion.X import Y -> Y(...)
                        if isinstance(call_node.func, ast.Name):
                            # Track the import source
                            for imp in ast.walk(tree):
                                if isinstance(imp, ast.ImportFrom) and imp.module:
                                    for alias in imp.names:
                                        actual_name = alias.asname or alias.name
                                        if actual_name == call_node.func.id:
                                            helper_modules.add(imp.module)

        failures: list[str] = []
        for mod_path in helper_modules:
            try:
                mod = importlib.import_module(mod_path)
            except ImportError:
                continue
            source_file = inspect.getfile(mod)
            source = Path(source_file).read_text(encoding="utf-8")
            tree = ast.parse(source)

            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                # Skip __init__, main, and guard check methods
                if node.name.startswith("_") and node.name not in ("__init__",):
                    # Check for public-ish helpers called from run_pipeline
                    pass
                for child in ast.walk(node):
                    if not isinstance(child, ast.ExceptHandler):
                        continue
                    catches_broad = child.type is None or (
                        isinstance(child.type, ast.Name) and child.type.id == "Exception"
                    )
                    if not catches_broad:
                        continue
                    has_raise = any(isinstance(stmt, ast.Raise) for stmt in ast.walk(child))
                    if not has_raise:
                        failures.append(
                            f"{mod_path}:{node.name}:{child.lineno}: "
                            f"catches Exception without raise"
                        )

        assert not failures, (
            "Helper functions called by run_pipeline swallow exceptions:\n"
            + "\n".join(sorted(failures))
        )
```

- [ ] **Step 2: Run tests to verify they fail (RED)**

```bash
uv run pytest src/tests/test_guard_conformance.py::TestExceptionPropagation -v
```

Expected: `test_helper_functions_propagate_exceptions` FAILS because `ingestion.statsbomb:backfill_extra_json:603` catches Exception without raise. `test_no_silent_exception_swallow_in_run_pipeline` should PASS (no run_pipeline body directly swallows).

- [ ] **Step 3: Commit**

```bash
git add src/tests/test_guard_conformance.py
git commit -m "test: add TestExceptionPropagation (TDD — RED for D48 silent swallow)"
```

---

## Task 3: TDD — `TestGuardCountMatchesIds`

**Files:**
- Modify: `src/tests/test_guard_conformance.py`

- [ ] **Step 1: Write the test class**

Add after `TestExceptionPropagation`:

```python
class TestGuardCountMatchesIds:
    """Non-exempt guards: count must equal len(distinct entity IDs) in metadata."""

    def test_count_equals_metadata_id_count(self) -> None:
        """result.count == len(id_list) for all non-exempt guards."""
        failures: list[str] = []

        for module_path in _GUARD_MODULES:
            mod = importlib.import_module(module_path)
            guard = mod.skip_guard
            if guard.workflow_id in _METADATA_EXEMPT:
                continue

            spark = _make_permissive_spark_mock()
            result = guard.check(spark, "soccer_analytics", "dev_gold")

            if result.count == 0:
                continue

            # Find the first list[str] in metadata — that's the ID list
            id_list: list[str] | None = None
            for val in result.metadata.values():
                if isinstance(val, list) and all(isinstance(v, str) for v in val):
                    id_list = val
                    break

            if id_list is None:
                # Multi-key guards (spadl_vaep, line_breaking) sum multiple lists
                total_ids = 0
                for val in result.metadata.values():
                    if isinstance(val, list):
                        total_ids += len(val)
                if total_ids > 0 and result.count != total_ids:
                    failures.append(
                        f"{guard.workflow_id}: count={result.count} != "
                        f"sum(metadata lists)={total_ids}"
                    )
            else:
                if result.count != len(id_list):
                    failures.append(
                        f"{guard.workflow_id}: count={result.count} != "
                        f"len(metadata IDs)={len(id_list)}"
                    )

        assert not failures, (
            "Guard count != metadata ID count:\n" + "\n".join(sorted(failures))
        )

    def test_metadata_ids_are_distinct(self) -> None:
        """No duplicate IDs in metadata lists."""
        failures: list[str] = []

        for module_path in _GUARD_MODULES:
            mod = importlib.import_module(module_path)
            guard = mod.skip_guard
            if guard.workflow_id in _METADATA_EXEMPT:
                continue

            spark = _make_permissive_spark_mock()
            result = guard.check(spark, "soccer_analytics", "dev_gold")

            for key, val in result.metadata.items():
                if isinstance(val, list):
                    if len(val) != len(set(val)):
                        failures.append(
                            f"{guard.workflow_id}: metadata['{key}'] has duplicates"
                        )

        assert not failures, (
            "Guard metadata contains duplicate IDs:\n" + "\n".join(sorted(failures))
        )
```

- [ ] **Step 2: Run tests to verify current state**

```bash
uv run pytest src/tests/test_guard_conformance.py::TestGuardCountMatchesIds -v
```

Expected: PASS for all currently non-exempt guards (they already set count=len(ids)). The 4 guards being promoted in D47 are still in `_METADATA_EXEMPT`, so they're skipped.

- [ ] **Step 3: Commit**

```bash
git add src/tests/test_guard_conformance.py
git commit -m "test: add TestGuardCountMatchesIds (TDD — count/ID consistency)"
```

---

## Task 4: TDD — `TestCostTimeCapture`

**Files:**
- Modify: `src/tests/test_guard_conformance.py`

- [ ] **Step 1: Write the test class**

Add after `TestGuardCountMatchesIds`:

```python
from decimal import Decimal
from unittest.mock import patch


class TestCostTimeCapture:
    """Every workflow through run_workflow with CostEstimateHook must produce observability data."""

    def _run_with_hook(self, *, should_raise: bool = False) -> dict:
        """Helper: run a trivial workflow with CostEstimateHook, return the MERGE row."""
        from workflows.registry import WorkflowEntry

        merge_rows: list[dict] = []
        original_delta = None

        def _capture_merge(*_args: object, **_kwargs: object) -> MagicMock:
            """Mock DeltaTable.forName that captures the MERGE row data."""
            mock_table = MagicMock()

            def _mock_merge(source: object, condition: object) -> MagicMock:
                merger = MagicMock()
                merger.whenMatchedUpdateAll.return_value = merger
                merger.whenNotMatchedInsertAll.return_value = merger

                def _execute() -> None:
                    pass

                merger.execute = _execute
                return merger

            mock_table.alias.return_value.merge = _mock_merge
            return mock_table

        # Capture the row from spark.createDataFrame
        mock_spark = MagicMock()

        def _capture_create_df(data: list, schema: object = None) -> MagicMock:
            if data and isinstance(data[0], (list, tuple)):
                # cost_hook passes a list of [values]
                merge_rows.append({"raw": data[0]})
            return MagicMock()

        mock_spark.createDataFrame.side_effect = _capture_create_df

        from ingestion.cost_hook import CostEstimateHook
        from workflows.runner import _hooks, register_hook

        saved_hooks = list(_hooks)
        _hooks.clear()

        try:
            hook = CostEstimateHook.__new__(CostEstimateHook)
            hook._spark = mock_spark
            hook._catalog = "test_cat"
            hook._schema = "test_schema"
            register_hook(hook)

            def _body(s: object, c: str, sc: str, lg: object, *, filter_result: object, ctx: object = None) -> None:
                if should_raise:
                    msg = "test error"
                    raise RuntimeError(msg)

            entry = WorkflowEntry(
                func=_body,
                workflow_id="wf-test-cost",
                phase="test",
                card=None,
            )

            fr = FilterResult(workflow_id="wf-test-cost", count=3)

            from workflows.runner import run_workflow

            try:
                run_workflow(entry, mock_spark, "cat", "schema", MagicMock(), filter_result=fr)
            except RuntimeError:
                if not should_raise:
                    raise

        finally:
            _hooks.clear()
            _hooks.extend(saved_hooks)

        return {"rows": merge_rows, "create_df_calls": mock_spark.createDataFrame.call_count}

    def test_completed_workflow_produces_cost_row(self) -> None:
        """Successful pipeline produces at least 2 MERGE rows (on_start + on_complete)."""
        result = self._run_with_hook(should_raise=False)
        assert result["create_df_calls"] >= 2, "Expected on_start + on_complete MERGE calls"

    def test_failed_workflow_produces_cost_row(self) -> None:
        """Failed pipeline produces at least 2 MERGE rows (on_start + on_error)."""
        result = self._run_with_hook(should_raise=True)
        assert result["create_df_calls"] >= 2, "Expected on_start + on_error MERGE calls"
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
uv run pytest src/tests/test_guard_conformance.py::TestCostTimeCapture -v
```

Expected: PASS — these test the existing cost hook mechanism which works.

- [ ] **Step 3: Commit**

```bash
git add src/tests/test_guard_conformance.py
git commit -m "test: add TestCostTimeCapture (TDD — observability guarantee)"
```

---

## Task 5: TDD — `TestWorkflowIdConsistency`

Prevents future ID mismatches like D46's `wf-formations-sg` vs `wf-shape-graphs`.

**Files:**
- Modify: `src/tests/test_guard_conformance.py`

- [ ] **Step 1: Write the test class**

Add after `TestCostTimeCapture`:

```python
class TestWorkflowIdConsistency:
    """Guard workflow_id must match the @workflow decorator's ID on run_pipeline."""

    def test_guard_id_matches_decorator_id(self) -> None:
        """guard.workflow_id == @workflow('wf-xxx') for all guards with own pipelines."""
        failures: list[str] = []

        for module_path in _GUARD_MODULES:
            if module_path in _NO_OWN_PIPELINE:
                continue

            mod = importlib.import_module(module_path)
            guard = mod.skip_guard
            source_file = inspect.getfile(mod)
            source = Path(source_file).read_text(encoding="utf-8")
            tree = ast.parse(source)

            # Find @workflow("wf-xxx") on run_pipeline* functions
            decorator_id: str | None = None
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if not node.name.startswith("run_pipeline"):
                    continue
                for dec in node.decorator_list:
                    if isinstance(dec, ast.Call) and _ast_has_name_or_attr(dec.func, "workflow"):
                        if dec.args and isinstance(dec.args[0], ast.Constant):
                            decorator_id = dec.args[0].value

            if decorator_id is None:
                continue  # No @workflow decorator found

            if guard.workflow_id != decorator_id:
                failures.append(
                    f"{module_path}: guard.workflow_id={guard.workflow_id!r} "
                    f"!= @workflow({decorator_id!r})"
                )

        assert not failures, (
            "Guard/decorator workflow_id mismatch:\n" + "\n".join(sorted(failures))
        )
```

- [ ] **Step 2: Run tests to verify RED**

```bash
uv run pytest src/tests/test_guard_conformance.py::TestWorkflowIdConsistency -v
```

Expected: FAILS because `formations_shape_graph` guard has `wf-formations-sg` but decorator has `wf-shape-graphs`. D46 fix turns it green.

- [ ] **Step 3: Commit**

```bash
git add src/tests/test_guard_conformance.py
git commit -m "test: add TestWorkflowIdConsistency (TDD — RED for D46 ID mismatch)"
```

---

## Task 6: D50 — Fix `spadl_vaep` schema mismatch

**Files:**
- Modify: `src/ingestion/spadl_vaep.py:71-74`

- [ ] **Step 1: Fix the `id_column` parameter**

At line 71-74, the `ws_new = find_new_ids(...)` call uses the default `id_column="match_id"`, but `wyscout_events` stores it as `matchId`:

```python
# Before (line 71-74):
        ws_new = find_new_ids(
            spark,
            f"{catalog}.{schema}.wyscout_events",
            spadl_table,
        )

# After:
        ws_new = find_new_ids(
            spark,
            f"{catalog}.{schema}.wyscout_events",
            spadl_table,
            id_column="matchId",
        )
```

- [ ] **Step 2: Run guard conformance tests**

```bash
uv run pytest src/tests/test_guard_conformance.py::TestGuardRegistry -v
```

Expected: PASS (guard still returns FilterResult with mock).

- [ ] **Step 3: Commit**

```bash
git add src/ingestion/spadl_vaep.py
git commit -m "fix(D50): spadl_vaep guard id_column=matchId for wyscout_events"
```

---

## Task 7: D50 — Fix `line_breaking` schema mismatch

**Files:**
- Modify: `src/ingestion/line_breaking.py:65`

- [ ] **Step 1: Fix the `source_filter` column name**

At line 65, change `event_type` to `type`:

```python
# Before (line 61-67):
        metrica_ids = find_new_ids(
            spark,
            f"{catalog}.bronze.metrica_events",
            results_table,
            source_filter="event_type = 'PASS'",
            results_filter="data_source = 'metrica_tracking'",
        )

# After:
        metrica_ids = find_new_ids(
            spark,
            f"{catalog}.bronze.metrica_events",
            results_table,
            source_filter="type = 'PASS'",
            results_filter="data_source = 'metrica_tracking'",
        )
```

- [ ] **Step 2: Run guard conformance tests**

```bash
uv run pytest src/tests/test_guard_conformance.py::TestGuardRegistry -v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/ingestion/line_breaking.py
git commit -m "fix(D50): line_breaking guard source_filter type column for metrica_events"
```

---

## Task 8: D49 — Break `defcon_lite_common` import chain

**Files:**
- Modify: `src/ingestion/defcon_lite_common.py`
- Modify: `src/ingestion/defcon_lite_360.py:15`
- Modify: `src/ingestion/defcon_lite_tracking.py:15`

- [ ] **Step 1: Remove module-level analytics import from `defcon_lite_common.py`**

Replace lines 7-23 of `defcon_lite_common.py`:

```python
# Before (lines 7-23):
from __future__ import annotations

import logging

import pandas as pd

from analytics.defcon_lite import DefconLiteParams
from shared.constants import mlflow_model_uri

__all__ = [
    "_ACTION_PREFIX",
    "_FF_PREFIX",
    "_TABLE_NAME",
    "DefconLiteParams",
    "_make_values_udf",
    "_try_load_champion_defcon",
]

# After:
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from shared.constants import mlflow_model_uri

if TYPE_CHECKING:
    from analytics.defcon_lite import DefconLiteParams

__all__ = [
    "_ACTION_PREFIX",
    "_FF_PREFIX",
    "_TABLE_NAME",
    "_make_values_udf",
    "_try_load_champion_defcon",
]
```

Note: `DefconLiteParams` removed from `__all__` — it was only re-exported, and consumers already import it from `analytics.defcon_lite` inside UDF closures (line 102 of `defcon_lite_common.py`).

- [ ] **Step 2: Inline `_TABLE_NAME` in consumer modules**

In `src/ingestion/defcon_lite_360.py`, replace line 15:

```python
# Before:
from ingestion.defcon_lite_common import _TABLE_NAME

# After:
_TABLE_NAME = "defcon_results"
```

In `src/ingestion/defcon_lite_tracking.py`, replace line 15:

```python
# Before:
from ingestion.defcon_lite_common import _TABLE_NAME

# After:
_TABLE_NAME = "defcon_results"
```

- [ ] **Step 3: Run import isolation tests**

```bash
uv run pytest src/tests/test_guard_conformance.py::TestGuardImportIsolation -v
```

Expected: PASS (the existing AST test still passes — it didn't catch this before because it's a transitive issue, but now the chain is broken so it's moot).

- [ ] **Step 4: Verify manually that `defcon_lite_common` no longer imports `analytics` at module level**

```bash
uv run python -c "import ast; tree = ast.parse(open('src/ingestion/defcon_lite_common.py').read()); [print(f'FOUND: from {n.module}') for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module and n.module.startswith('analytics') and not any(isinstance(p, ast.If) for p in ast.walk(tree))]"
```

Expected: No output (no unconditional `from analytics.*` at module level).

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/defcon_lite_common.py src/ingestion/defcon_lite_360.py src/ingestion/defcon_lite_tracking.py
git commit -m "fix(D49): break defcon_lite_common transitive xgboost import chain"
```

---

## Task 9: D49 — Strengthen `TestGuardImportIsolation` for transitive deps

**Files:**
- Modify: `src/tests/test_guard_conformance.py`

- [ ] **Step 1: Add runtime import test**

Add a new test method to `TestGuardImportIsolation` (after the existing `test_no_analytics_imports_at_module_level` at line 277):

```python
    def test_no_transitive_analytics_imports_at_module_level(self) -> None:
        """Runtime check: importing a guard module must not trigger analytics package loads.

        Uses sys.modules patching to detect transitive imports that AST
        analysis on the guard module's source alone would miss.
        """
        import importlib
        from unittest.mock import patch

        failures: list[str] = []

        for module_path in _GUARD_MODULES:
            # Unload the guard module so re-import triggers fresh load
            mods_to_remove = [k for k in sys.modules if k.startswith(module_path.split(".")[0])]

            saved_modules: dict[str, object] = {}
            sentinels: dict[str, object] = {}

            # Plant sentinel modules that raise on attribute access
            for pkg in self._ANALYTICS_PACKAGES:
                if pkg in sys.modules:
                    saved_modules[pkg] = sys.modules[pkg]

                class _Sentinel:
                    _pkg_name = pkg

                    def __getattr__(self, name: str) -> None:
                        msg = f"Guard transitively imported {self._pkg_name}"
                        raise ImportError(msg)

                sentinels[pkg] = _Sentinel()

            try:
                # Remove cached guard module
                guard_cache = {}
                for k in list(sys.modules.keys()):
                    if k == module_path or k.startswith(module_path + "."):
                        guard_cache[k] = sys.modules.pop(k)

                # Plant sentinels
                for pkg, sentinel in sentinels.items():
                    sys.modules[pkg] = sentinel  # type: ignore[assignment]

                try:
                    importlib.import_module(module_path)
                except ImportError as exc:
                    failures.append(f"{module_path}: {exc}")
            finally:
                # Restore original modules
                for pkg, original in saved_modules.items():
                    sys.modules[pkg] = original  # type: ignore[assignment]
                for pkg in sentinels:
                    if pkg not in saved_modules:
                        sys.modules.pop(pkg, None)
                # Restore guard module cache
                sys.modules.update(guard_cache)

        assert not failures, (
            "Guard modules have transitive analytics imports at module level:\n"
            + "\n".join(sorted(failures))
        )
```

- [ ] **Step 2: Run the new test**

```bash
uv run pytest src/tests/test_guard_conformance.py::TestGuardImportIsolation::test_no_transitive_analytics_imports_at_module_level -v
```

Expected: PASS (D49 Part A already broke the chain in Task 8).

- [ ] **Step 3: Commit**

```bash
git add src/tests/test_guard_conformance.py
git commit -m "test(D49): add runtime transitive import isolation check for guards"
```

---

## Task 10: D48 — Re-raise exceptions in `backfill_extra_json`

**Files:**
- Modify: `src/ingestion/statsbomb.py:551-553, 602-604`

- [ ] **Step 1: Add `raise` to both exception handlers**

At lines 551-553 (initial query failure):

```python
# Before:
    except Exception:
        logger.exception("Cannot read %s for backfill — table may not exist", events_table)
        return

# After:
    except Exception:
        logger.exception("Cannot read %s for backfill — table may not exist", events_table)
        raise
```

At lines 602-604 (MERGE failure):

```python
# Before:
    except Exception:
        logger.exception("Failed batch MERGE for _raw_extra_json backfill")

# After:
    except Exception:
        logger.exception("Failed batch MERGE for _raw_extra_json backfill")
        raise
```

- [ ] **Step 2: Run exception propagation tests to verify GREEN**

```bash
uv run pytest src/tests/test_guard_conformance.py::TestExceptionPropagation -v
```

Expected: PASS — both layers now pass since `backfill_extra_json` re-raises.

- [ ] **Step 3: Commit**

```bash
git add src/ingestion/statsbomb.py
git commit -m "fix(D48): re-raise exceptions in backfill_extra_json — no silent swallow"
```

---

## Task 11: D48 — Chunk MERGE by (competition_id, season_id)

**Files:**
- Modify: `src/ingestion/statsbomb.py:544-604`

- [ ] **Step 1: Rewrite `backfill_extra_json` with chunked loop**

Replace the body of `backfill_extra_json` (lines 544-604) with the chunked pattern. The function signature at lines 532-538 stays the same:

```python
def backfill_extra_json(
    spark: SparkSession,
    catalog: str,
    schema: str,
    competitions_pdf: pd.DataFrame,
    logger: logging.Logger,
    *,
    match_ids: list[str] | None = None,
) -> None:
    """Backfill ``_raw_extra_json`` for existing events that lack it.

    When *match_ids* is provided (from guard metadata), only those matches
    are processed. Otherwise falls back to a discovery query.

    Chunks work by ``(competition_id, season_id)`` to stay within the
    Spark Connect protobuf size limit (~2 GB per message).
    """
    events_table = f"{catalog}.{schema}.statsbomb_events"

    if match_ids is not None:
        # Use pre-computed IDs from guard — avoid full table scan
        needs_backfill_rows = (
            spark.sql(
                f"SELECT DISTINCT match_id, competition_id, season_id "  # noqa: S608
                f"FROM {events_table} "
                f"WHERE match_id IN ({','.join(repr(m) for m in match_ids)}) "
                f"AND _raw_extra_json IS NULL"
            ).collect()
        )
    else:
        needs_backfill_rows = spark.sql(
            f"SELECT DISTINCT match_id, competition_id, season_id "  # noqa: S608
            f"FROM {events_table} "
            f"WHERE _raw_extra_json IS NULL"
        ).collect()

    if not needs_backfill_rows:
        logger.info("No matches need _raw_extra_json backfill")
        return

    logger.info("Found %d match partitions needing _raw_extra_json backfill", len(needs_backfill_rows))

    # Group by (competition_id, season_id) for chunked MERGE
    from collections import defaultdict

    groups: dict[tuple[str, str], list[int]] = defaultdict(list)
    for row in needs_backfill_rows:
        key = (str(row["competition_id"]), str(row["season_id"]))
        groups[key].append(int(row["match_id"]))

    total_events = 0
    total_matches = 0

    for (comp_id, season_id), group_match_ids in groups.items():
        logger.info(
            "Backfilling group %s/%s: %d matches",
            comp_id,
            season_id,
            len(group_match_ids),
        )

        # Concurrent HTTP fetch for this group
        extra_maps: dict[int, dict[str, str]] = {}
        with ThreadPoolExecutor(max_workers=_HTTP_MAX_WORKERS) as executor:
            futures = {executor.submit(_build_raw_extra_json, mid, logger): mid for mid in group_match_ids}
            for future in as_completed(futures):
                mid = futures[future]
                try:
                    extra_maps[mid] = future.result()
                except Exception:
                    logger.exception("Failed to fetch _raw_extra_json for match %d", mid)

        # Build mapping rows for this group
        mapping_rows: list[tuple[str, str]] = []
        for extra_map in extra_maps.values():
            if not extra_map:
                continue
            for eid, ejson in extra_map.items():
                mapping_rows.append((eid, ejson))

        if not mapping_rows:
            logger.info("No extra JSON mappings for group %s/%s — skipping", comp_id, season_id)
            continue

        # MERGE this group's updates
        mapping_sdf = spark.createDataFrame(mapping_rows, ["_eid", "_extra_json"])
        mapping_sdf.createOrReplaceTempView("_backfill_map")

        spark.sql(
            f"MERGE INTO {events_table} AS t "
            "USING _backfill_map AS s "
            "ON t.id = s._eid "
            "WHEN MATCHED THEN UPDATE SET t._raw_extra_json = s._extra_json"
        )

        total_events += len(mapping_rows)
        total_matches += len(extra_maps)
        logger.info(
            "Backfilled group %s/%s: %d events across %d matches",
            comp_id,
            season_id,
            len(mapping_rows),
            len(extra_maps),
        )

    logger.info(
        "Backfill complete: %d events across %d matches in %d groups",
        total_events,
        total_matches,
        len(groups),
    )
```

- [ ] **Step 2: Update `run_pipeline` in `statsbomb_backfill_extra.py` to pass match IDs**

Replace lines 62-65 of `statsbomb_backfill_extra.py`:

```python
# Before:
    from ingestion.statsbomb import backfill_extra_json, ingest_competitions

    competitions_pdf = ingest_competitions(spark, catalog, schema, logger)
    backfill_extra_json(spark, catalog, schema, competitions_pdf, logger)

# After:
    from ingestion.statsbomb import backfill_extra_json, ingest_competitions

    competitions_pdf = ingest_competitions(spark, catalog, schema, logger)
    match_ids = filter_result.metadata.get("new_match_ids")
    backfill_extra_json(spark, catalog, schema, competitions_pdf, logger, match_ids=match_ids)
```

- [ ] **Step 3: Run linting and type checks**

```bash
uv run ruff check src/ingestion/statsbomb.py src/ingestion/statsbomb_backfill_extra.py && uv run pyright src/ingestion/statsbomb.py src/ingestion/statsbomb_backfill_extra.py
```

Expected: Clean.

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/statsbomb.py src/ingestion/statsbomb_backfill_extra.py
git commit -m "fix(D48): chunk backfill_extra_json MERGE by (comp_id, season_id)"
```

---

## Task 12: D47 — Promote `wf-backfill-extra` guard + fix semantic mismatch

**Files:**
- Modify: `src/ingestion/statsbomb_backfill_extra.py:25-42`
- Modify: `src/tests/test_guard_conformance.py` (remove from `_METADATA_EXEMPT`)

- [ ] **Step 1: Rewrite the guard to return entity IDs**

Replace `_BackfillExtraGuard.check()` (lines 28-42):

```python
    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Find matches needing _raw_extra_json backfill.

        Uses ``IS NULL`` only — events with ``'{}'`` are legitimately
        backfilled (no type-specific data). This prevents the infinite
        re-run bug where "successfully processed" events re-triggered.
        """
        table = f"{catalog}.{schema}.statsbomb_events"
        try:
            rows = (
                spark.table(table)
                .filter("_raw_extra_json IS NULL")
                .select("match_id")
                .distinct()
                .collect()
            )
            match_ids = sorted({str(row["match_id"]) for row in rows})

            if not match_ids:
                return FilterResult(workflow_id=self.workflow_id, count=0)

            return FilterResult(
                workflow_id=self.workflow_id,
                count=len(match_ids),
                metadata={"new_match_ids": match_ids},
            )
        except Exception:
            # Table may not exist — assume work needed
            return FilterResult(workflow_id=self.workflow_id, count=1)
```

- [ ] **Step 2: Remove `wf-backfill-extra` from `_METADATA_EXEMPT`**

In `src/tests/test_guard_conformance.py`, remove line 28:

```python
# Remove this line from _METADATA_EXEMPT:
    "wf-backfill-extra",  # Existence check, no ID metadata
```

- [ ] **Step 3: Run conformance tests**

```bash
uv run pytest src/tests/test_guard_conformance.py::TestGuardCountMatchesIds src/tests/test_guard_conformance.py::TestGuardMetadataContract -v
```

Expected: PASS — guard now returns count=len(match_ids) with metadata.

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/statsbomb_backfill_extra.py src/tests/test_guard_conformance.py
git commit -m "fix(D47+D48): promote wf-backfill-extra to full guard, fix IS NULL condition"
```

---

## Task 13: D47 — Promote `wf-backfill-360` guard

**Files:**
- Modify: `src/ingestion/statsbomb_backfill_360.py:34-55`
- Modify: `src/tests/test_guard_conformance.py` (remove from `_METADATA_EXEMPT`)

- [ ] **Step 1: Pass entity IDs through metadata**

Replace the guard's check method (lines 34-55):

```python
        try:
            event_ids = {
                str(row["match_id"])
                for row in spark.table(f"{catalog}.{schema}.statsbomb_events").select("match_id").distinct().collect()
            }
            try:
                three60_ids = {
                    str(row["match_id"])
                    for row in spark.table(f"{catalog}.{schema}.statsbomb_360").select("match_id").distinct().collect()
                }
            except Exception:
                # 360 table doesn't exist — all event matches need backfill
                return FilterResult(
                    workflow_id=self.workflow_id,
                    count=len(event_ids),
                    metadata={"new_match_ids": sorted(event_ids)},
                )

            missing = sorted(event_ids - three60_ids)
            if not missing:
                return FilterResult(workflow_id=self.workflow_id, count=0)
            return FilterResult(
                workflow_id=self.workflow_id,
                count=len(missing),
                metadata={"new_match_ids": missing},
            )
        except Exception:
            # Events table doesn't exist — nothing to backfill
            return FilterResult(workflow_id=self.workflow_id, count=0)
```

- [ ] **Step 2: Remove `wf-backfill-360` from `_METADATA_EXEMPT`**

In `src/tests/test_guard_conformance.py`, remove line 29:

```python
# Remove this line from _METADATA_EXEMPT:
    "wf-backfill-360",  # Set-difference guard, no ID metadata
```

- [ ] **Step 3: Run conformance tests**

```bash
uv run pytest src/tests/test_guard_conformance.py::TestGuardCountMatchesIds src/tests/test_guard_conformance.py::TestGuardMetadataContract -v
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/statsbomb_backfill_360.py src/tests/test_guard_conformance.py
git commit -m "fix(D47): promote wf-backfill-360 to full guard with entity IDs"
```

---

## Task 14: D47 — Promote `wf-entity-resolution` guard

**Files:**
- Modify: `src/ingestion/entity_resolution.py:39-55`
- Modify: `src/tests/test_guard_conformance.py` (remove from `_METADATA_EXEMPT`)

- [ ] **Step 1: Rewrite guard to use `find_new_ids`**

Replace `_EntityResolutionGuard.check()` (lines 39-55):

```python
    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check if entity resolution needs to run.

        Finds StatsBomb player IDs that lack cross-reference entries.
        """
        from ingestion.guards import find_new_ids

        xref_table = f"{catalog}.{schema}.player_xref_raw"
        lineups_table = f"{catalog}.{schema}.statsbomb_lineups"

        try:
            new_player_ids = find_new_ids(
                spark,
                source_table=lineups_table,
                results_table=xref_table,
                id_column="player_id",
            )
        except Exception:
            _guard_logger.debug("Cannot check %s — needs resolution", xref_table)
            return FilterResult(workflow_id=self.workflow_id, count=1)

        if not new_player_ids:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(new_player_ids),
            metadata={"new_player_ids": new_player_ids},
        )
```

- [ ] **Step 2: Remove `wf-entity-resolution` from `_METADATA_EXEMPT`**

In `src/tests/test_guard_conformance.py`, remove line 44:

```python
# Remove this line from _METADATA_EXEMPT:
    "wf-entity-resolution",  # Binary existence check
```

- [ ] **Step 3: Run conformance tests**

```bash
uv run pytest src/tests/test_guard_conformance.py::TestGuardCountMatchesIds src/tests/test_guard_conformance.py::TestGuardMetadataContract -v
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/entity_resolution.py src/tests/test_guard_conformance.py
git commit -m "fix(D47): promote wf-entity-resolution to full guard with find_new_ids"
```

---

## Task 15: D47 — Promote `wf-tracking-metadata` guard

**Files:**
- Modify: `src/ingestion/tracking_metadata.py:40-51`
- Modify: `src/tests/test_guard_conformance.py` (remove from `_METADATA_EXEMPT`)

- [ ] **Step 1: Rewrite guard to return entity IDs**

Replace `_TrackingMetadataGuard.check()` (lines 43-51):

```python
    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Find tracking data sources that lack metadata extraction.

        Compares IDSSE + SkillCorner match IDs against the existing
        tracking_player_metadata table.
        """
        from ingestion.guards import find_new_ids

        results_table = f"{catalog}.{schema}.{TABLE_NAME}"

        # Check IDSSE matches
        idsse_ids: list[str] = []
        try:
            idsse_ids = find_new_ids(
                spark,
                source_table=f"{catalog}.bronze.idsse_tracking",
                results_table=results_table,
            )
        except Exception:  # noqa: S110
            pass

        # Check SkillCorner matches
        skillcorner_ids: list[str] = []
        try:
            skillcorner_ids = find_new_ids(
                spark,
                source_table=f"{catalog}.bronze.skillcorner_tracking",
                results_table=results_table,
            )
        except Exception:  # noqa: S110
            pass

        all_ids = sorted(set(idsse_ids) | set(skillcorner_ids))

        if not all_ids:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(all_ids),
            metadata={"new_match_ids": all_ids},
        )
```

- [ ] **Step 2: Remove `wf-tracking-metadata` from `_METADATA_EXEMPT`**

In `src/tests/test_guard_conformance.py`, remove line 45:

```python
# Remove this line from _METADATA_EXEMPT:
    "wf-tracking-metadata",  # Simple existence check
```

- [ ] **Step 3: Run conformance tests**

```bash
uv run pytest src/tests/test_guard_conformance.py::TestGuardCountMatchesIds src/tests/test_guard_conformance.py::TestGuardMetadataContract -v
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/tracking_metadata.py src/tests/test_guard_conformance.py
git commit -m "fix(D47): promote wf-tracking-metadata to full guard with find_new_ids"
```

---

## Task 16: D46 — Fix `wf-formations-sg` workflow ID mismatch

**Files:**
- Modify: `src/ingestion/formations_shape_graph.py:63`

- [ ] **Step 1: Change guard `workflow_id`**

At line 63:

```python
# Before:
    workflow_id = "wf-formations-sg"

# After:
    workflow_id = "wf-shape-graphs"
```

- [ ] **Step 2: Run `TestWorkflowIdConsistency` to verify GREEN**

```bash
uv run pytest src/tests/test_guard_conformance.py::TestWorkflowIdConsistency -v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/ingestion/formations_shape_graph.py
git commit -m "fix(D46): align formations_shape_graph guard workflow_id to wf-shape-graphs"
```

---

## Task 17: D46 — Add missing seed rows + remove orphaned decorator

**Files:**
- Modify: `dbt_project/seeds/task_workflow_mapping.csv`
- Modify: `src/ingestion/export_scoutgpt_training_data.py:575`

- [ ] **Step 1: Add 4 missing rows to seed CSV**

Add before the blank line at the end of `dbt_project/seeds/task_workflow_mapping.csv`:

```csv
backfill_statsbomb_extra,wf-backfill-extra
backfill_statsbomb_360,wf-backfill-360
extract_tracking_metadata,wf-tracking-metadata
sync_hf_costs,wf-sync-hf-costs
```

Note: `compute_xg_model_v2` already maps to `wf-xg-v2` on line 18. Adding `sync_hf_costs` instead since it has a guard entry but no seed row.

- [ ] **Step 2: Remove orphaned `@workflow` decorator from `export_scoutgpt_training_data.py`**

At line 575:

```python
# Before:
@workflow("wf-scoutgpt-export", phase="export")
def run_pipeline(

# After:
def run_pipeline(
```

Also remove the unused `workflow` import if it becomes the only use. Check the file for other `@workflow` uses first.

- [ ] **Step 3: Run linting**

```bash
uv run ruff check src/ingestion/export_scoutgpt_training_data.py dbt_project/seeds/task_workflow_mapping.csv
```

Expected: Clean (or remove unused `from workflows import workflow` import if flagged).

- [ ] **Step 4: Commit**

```bash
git add dbt_project/seeds/task_workflow_mapping.csv src/ingestion/export_scoutgpt_training_data.py
git commit -m "fix(D46): add missing seed rows, remove orphaned scoutgpt @workflow decorator"
```

---

## Task 18: D40e — Parallelize freshness gate

**Files:**
- Modify: `src/ingestion/freshness_gate.py:1-91`

- [ ] **Step 1: Add `ThreadPoolExecutor` import and `_check_one_guard` helper**

Add import at line 14 (after `import time`):

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
```

Add helper function before `run_gate` (after line 27):

```python
_GATE_MAX_WORKERS = 4


def _check_one_guard(
    wf_id: str,
    guard: SkipGuard,
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> tuple[FilterResult, float]:
    """Run a single guard with timing and exception handling."""
    t0 = time.monotonic()
    try:
        result = guard.check(spark, catalog, schema)
        elapsed = round(time.monotonic() - t0, 2)
        logger.info(
            "guard_check",
            extra={
                "workflow_id": wf_id,
                "count": result.count,
                "elapsed_seconds": elapsed,
                "chunks": len(result.chunks) if result.chunks else 0,
            },
        )
        return result, elapsed
    except Exception:
        elapsed = round(time.monotonic() - t0, 2)
        logger.warning(
            "guard_check_failed",
            extra={"workflow_id": wf_id, "elapsed_seconds": elapsed},
            exc_info=True,
        )
        return FilterResult(workflow_id=wf_id, count=0), elapsed
```

- [ ] **Step 2: Replace the sequential loop in `run_gate` with `ThreadPoolExecutor`**

Replace the loop body (lines 53-77) with:

```python
    with ThreadPoolExecutor(max_workers=_GATE_MAX_WORKERS) as pool:
        futures = {
            pool.submit(_check_one_guard, wf_id, guard, spark, catalog, schema): wf_id
            for wf_id, guard in guards.items()
        }
        for future in as_completed(futures):
            wf_id = futures[future]
            result, elapsed = future.result()
            results[wf_id] = result
            timings[wf_id] = elapsed
```

The `gate_summary` logging block (lines 79-89) stays unchanged.

- [ ] **Step 3: Remove `TYPE_CHECKING`-only import of `SkipGuard` — now needed at runtime**

Since `_check_one_guard` uses `SkipGuard` in its type annotation at module level, move it from `TYPE_CHECKING`:

```python
# Before:
from ingestion.guards import FilterResult, get_workflow_guards

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from ingestion.guards import SkipGuard

# After:
from ingestion.guards import FilterResult, SkipGuard, get_workflow_guards

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
```

Wait — `SkipGuard` is a `Protocol`, so it's fine at runtime. But check if `from __future__ import annotations` is present (line 11). It IS present, so all annotations are strings — `SkipGuard` in the type hint is never evaluated at runtime. Keep it in `TYPE_CHECKING`.

- [ ] **Step 4: Run existing tests**

```bash
uv run pytest src/tests/test_freshness_gate.py -v
```

Expected: All existing `TestRunGate` tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/freshness_gate.py
git commit -m "feat(D40e): parallelize freshness gate with ThreadPoolExecutor(max_workers=4)"
```

---

## Task 19: D40e — Add parallelization tests

**Files:**
- Modify: `src/tests/test_freshness_gate.py`

- [ ] **Step 1: Add parallelization tests to `TestRunGate`**

Add after the existing `test_all_guards_skip` (line 72):

```python
    def test_parallel_execution_faster_than_sequential(self) -> None:
        """4 guards each sleeping 0.1s should complete in <0.25s (parallel)."""
        import time as _time

        def _slow_check(spark: object, catalog: str, schema: str) -> FilterResult:
            _time.sleep(0.1)
            return FilterResult(workflow_id="wf-slow", count=1)

        guards = {}
        for i in range(4):
            g = MagicMock()
            g.workflow_id = f"wf-slow-{i}"
            g.check.side_effect = _slow_check
            guards[f"wf-slow-{i}"] = g

        spark = MagicMock()
        t0 = _time.monotonic()
        results = run_gate(spark, "cat", "schema", guards=guards)
        elapsed = _time.monotonic() - t0

        assert len(results) == 4
        assert elapsed < 0.25, f"Expected <0.25s (parallel), got {elapsed:.2f}s"

    def test_guard_exception_in_thread_does_not_crash_others(self) -> None:
        """One guard raises in a thread, others succeed, all 4 results present."""
        guard_ok1 = MagicMock()
        guard_ok1.workflow_id = "wf-ok1"
        guard_ok1.check.return_value = FilterResult(workflow_id="wf-ok1", count=2)

        guard_ok2 = MagicMock()
        guard_ok2.workflow_id = "wf-ok2"
        guard_ok2.check.return_value = FilterResult(workflow_id="wf-ok2", count=3)

        guard_ok3 = MagicMock()
        guard_ok3.workflow_id = "wf-ok3"
        guard_ok3.check.return_value = FilterResult(workflow_id="wf-ok3", count=1)

        guard_bad = MagicMock()
        guard_bad.workflow_id = "wf-bad"
        guard_bad.check.side_effect = RuntimeError("Thread explosion")

        spark = MagicMock()
        results = run_gate(
            spark,
            "cat",
            "schema",
            guards={"wf-ok1": guard_ok1, "wf-bad": guard_bad, "wf-ok2": guard_ok2, "wf-ok3": guard_ok3},
        )

        assert len(results) == 4
        assert results["wf-bad"].count == 0
        assert results["wf-ok1"].count == 2
        assert results["wf-ok2"].count == 3
        assert results["wf-ok3"].count == 1
```

- [ ] **Step 2: Run all freshness gate tests**

```bash
uv run pytest src/tests/test_freshness_gate.py -v
```

Expected: All PASS including new parallelization tests.

- [ ] **Step 3: Commit**

```bash
git add src/tests/test_freshness_gate.py
git commit -m "test(D40e): add parallelization and thread-safety tests for freshness gate"
```

---

## Task 20: Full test suite + lint + type check

**Files:** None (verification only)

- [ ] **Step 1: Run ruff lint**

```bash
uv run ruff check src/ scripts/
```

Expected: Clean.

- [ ] **Step 2: Run ruff format check**

```bash
uv run ruff format --check src/ scripts/
```

Expected: Clean.

- [ ] **Step 3: Run pyright**

```bash
uv run pyright src/
```

Expected: Clean (basic mode).

- [ ] **Step 4: Run full test suite**

```bash
uv run pytest src/tests/ -v
```

Expected: All tests PASS.

- [ ] **Step 5: Commit any formatting fixes if needed**

```bash
uv run ruff format src/ scripts/
git add -u
git commit -m "style: format fixes from ruff"
```

---

## Task 21: E2E — Databricks verification

Full end-to-end testing using Databricks access. See spec E2E Verification Plan for detailed steps.

- [ ] **Step 1: Build and upload wheel 0.3.0**

```bash
uv run python -m build
# Upload to UC Volume
```

- [ ] **Step 2: Ensure warehouse is running**

```bash
python scripts/ensure_warehouse.py -- echo "Warehouse ready"
```

- [ ] **Step 3: Run freshness gate standalone**

Verify: all 33 guards execute without `guard_check_failed`, gate completes in <2 min, `wf-shape-graphs` ID consistent.

- [ ] **Step 4: Run `backfill_statsbomb_extra` standalone**

Verify: guard returns actual match count with entity IDs, chunked MERGE succeeds, `workflow_cost_live` shows `state="COMPLETED"`.

- [ ] **Step 5: Run `backfill_statsbomb_360` standalone**

Verify: guard returns match IDs in metadata.

- [ ] **Step 6: Run `entity_resolution` standalone**

Verify: guard returns player IDs in metadata.

- [ ] **Step 7: Run `tracking_metadata` standalone**

Verify: guard returns match IDs in metadata.

- [ ] **Step 8: dbt seed + build verification**

```bash
dbt seed --select task_workflow_mapping
dbt build --select fct_workflow_costs
```

- [ ] **Step 9: Query `workflow_cost_live` for observability verification**

Verify: all runs have `entity_count > 0`, `duration_seconds > 0`, `estimated_cost_usd > 0`. No false `COMPLETED` rows.
