# Guard Plumbing & Observability Implementation Plan (D40h + D40g + D40c + D40b)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate redundant guard execution across all compute pipelines by making `FilterResult` mandatory, then wire Terraform `run_if` conditions and add entity count observability.

**Architecture:** Mandatory guard injection — `run_pipeline()` receives `FilterResult` as a **required** parameter and cannot run its own guard. `main()` is the single resolution point: production reads from task values via `read_gate_result()`, standalone calls `skip_guard.check()` as fallback. Terraform `run_if` prevents idle tasks from starting. Entity count (input) joins `row_count` (output) in `workflow_cost_live`.

**Tech Stack:** Python 3.10, PySpark, Terraform (Databricks provider), Delta Lake

---

## Commit Boundaries

| Commit | Scope | TODO Items |
|--------|-------|-----------|
| 1 | Failing tests + guard import isolation + pipeline mandatory injection | D40h + D40g |
| 2 | Terraform `run_if` conditions | D40c |
| 3 | Entity count observability (tests + implementation) | D40b |

## File Map

### Tests (modify)
- `src/tests/test_guard_conformance.py` — 4 new test classes, 2 replaced, exempt list updates
- `src/tests/test_cost_hook.py` — `entity_count` column tests (Task 7)

### Guard Import Isolation (D40h) — defer analytics imports to function bodies
- `src/ingestion/spadl_vaep.py` — `silly_kicks`, `xgboost`
- `src/ingestion/entity_resolution.py` — `analytics.entity_resolution`
- `src/ingestion/defcon_lite_360.py` — `analytics.defcon_lite`
- `src/ingestion/defcon_lite_tracking.py` — `analytics.defcon_lite`
- `src/ingestion/line_breaking.py` — `analytics.line_breaking`, sub-module imports

### Pipeline Mandatory Injection (D40g merged into D40h)

#### Group A — Already consuming metadata (remove fallback, make required)
- `src/ingestion/pitch_control_batch.py`
- `src/ingestion/off_ball_xt.py`
- `src/ingestion/elastic_sync.py`
- `src/ingestion/pausa.py`

#### Group B — ID-based metadata ignored (consume metadata, remove inline guard)
- `src/ingestion/defcon_lite.py` (orchestrator: `filter_360` + `filter_tracking`)
- `src/ingestion/line_breaking.py` (forward metadata to sub-functions)
- `src/ingestion/formations_efpi.py` (use `new_match_ids` instead of re-discovery)
- `src/ingestion/formations_shape_graph.py` (same)
- `src/ingestion/spadl_vaep.py` (use `new_spadl_match_ids` + `unscored_vaep_match_ids`)
- `src/ingestion/xg_model.py` (use `new_competition_ids`)
- `src/ingestion/xg_model_v2.py` (use `new_competition_ids`)
- `src/ingestion/expected_threat.py` (use `new_competition_ids` + `need_global`)
- `src/ingestion/entity_resolution.py` (remove inline existence re-check)

#### Group C — Count-based guards (remove inline count re-check)
- `src/ingestion/export_embeddings_training_data.py`
- `src/ingestion/prepare_360_training_data.py`

#### Group D — Special cases
- `src/ingestion/tracking_metadata.py` (add `filter_result` param + `read_gate_result`)
- `src/ingestion/player_embeddings_v2.py` (stub guard, just wire required param)

### Terraform (D40c)
- `terraform/modules/workflows/main.tf` — `run_if` on downstream tasks

### Entity Count (D40b)
- `scripts/create_cost_table.sql` — add `entity_count INT` column
- `src/ingestion/cost_hook.py` — write `entity_count` in `on_start`
- `src/workflows/context.py` — add `entity_count: int | None` field
- `src/workflows/runner.py` — extract `entity_count` from `filter_result` kwarg

---

## The Pattern

Every non-exempt pipeline changes from this:

```python
# BEFORE — dual-path (run_pipeline can run its own guard)
@workflow("wf-xxx", phase="yyy")
def run_pipeline(spark, catalog, schema, logger, *, filter_result=None, ctx=None):
    if filter_result and filter_result.count == 0:
        return
    # Gate path (often missing)
    if filter_result and filter_result.metadata.get("new_match_ids"):
        new_ids = filter_result.metadata["new_match_ids"]
    else:
        # Inline fallback — REDUNDANT, duplicates the guard
        new_ids = find_new_ids(spark, source, results)
    ...

def main():
    filter_result = read_gate_result("wf-xxx")
    run_pipeline(spark, ..., filter_result=filter_result)
```

To this:

```python
# AFTER — single-path (run_pipeline trusts what it's given)
@workflow("wf-xxx", phase="yyy")
def run_pipeline(spark, catalog, schema, logger, *, filter_result: FilterResult, ctx=None):
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")
    new_ids = filter_result.metadata["new_match_ids"]
    ...

def main():
    filter_result = read_gate_result("wf-xxx")
    if filter_result is None:
        filter_result = skip_guard.check(spark, catalog, schema)
    run_pipeline(spark, ..., filter_result=filter_result)
```

**Key differences:**
1. `filter_result: FilterResult` — required, not Optional
2. `run_pipeline` body — NO `find_new_ids()`, NO inline anti-joins, NO re-queries
3. `main()` — standalone fallback calls `skip_guard.check()` when gate result is None
4. Early exit uses `WorkflowSkippedError` (triggers `on_skip` hook) instead of bare `return`

---

## Pipeline Reference Table

Each pipeline's specific metadata keys and inline logic to remove:

| Pipeline | `workflow_id` | Metadata keys consumed | Inline logic to remove |
|----------|--------------|----------------------|----------------------|
| `pitch_control_batch` | `wf-pitch-control` | `new_match_ids` | `find_new_ids` fallback in `_process_matches` |
| `off_ball_xt` | `wf-off-ball-xt` | `new_match_ids` | `find_new_ids` fallback in `_process_matches` |
| `elastic_sync` | `wf-elastic-sync` | `new_match_ids` | `find_new_ids` fallback in `run_pipeline` |
| `pausa` | `wf-pausa` | `new_match_ids` | `find_new_ids` fallback in `_process_matches` |
| `defcon_lite` | `wf-defcon` | `filter_360.metadata`, `filter_tracking.metadata` | N/A (orchestrator passes through) |
| `line_breaking` | `wf-line-breaking` | `statsbomb_360_ids`, `metrica_ids`, `idsse_ids` | Inline `existing_ids` set-building in each `_process_*` sub-function |
| `formations_efpi` | `wf-formations` | `new_match_ids` | `prepare_tracking_data` re-discovery; pass IDs into `_run_efpi` |
| `formations_shape_graph` | `wf-formations-sg` | `new_match_ids` | `prepare_tracking_data` re-discovery; pass IDs into `_run_shape_graph` |
| `spadl_vaep` | `wf-vaep` | `new_spadl_match_ids`, `unscored_vaep_match_ids` | `_read_existing_match_ids` + inline `unscored` computation |
| `xg_model` | `wf-xg-v1` | `new_competition_ids` | Inline `collect()` + set difference for `existing` |
| `xg_model_v2` | `wf-xg-v2` | `new_competition_ids` | Inline `collect()` + set difference for `existing` |
| `expected_threat` | `wf-xt-grids` | `new_competition_ids`, `need_global` | Inline `collect()` + set difference + `need_global` recompute |
| `entity_resolution` | `wf-entity-resolution` | (none — binary guard) | Inline existence re-check in `run_pipeline` body |
| `export_embeddings_training_data` | `wf-football2vec-v2-export` | (none — count-based) | Inline count comparison in `_export_training_sequences` |
| `prepare_360_training_data` | `wf-prepare-360-data` | (none — count-based) | Inline count comparison in step 1 |
| `player_embeddings_v2` | `wf-football2vec-v2` | (none — stub guard) | No inline guard to remove; just wire required param |
| `tracking_metadata` | `wf-tracking-metadata` | (none — existence guard) | No inline guard; add `filter_result` param + early exit |

---

## Exempt Pipelines (no changes needed)

These pipelines do NOT use the freshness gate pattern (data ingestors, importers, monitors):

`statsbomb`, `metrica`, `idsse`, `skillcorner`, `wyscout`, `import_obso_results`,
`import_psxg_predictions`, `import_space_creation`, `model_validation`, `sync_hf_costs`,
`freshness_gate`

Sub-modules with no own `main()` (orchestrated by parent):
`defcon_lite_360`, `defcon_lite_tracking`

---

## Task 1: Write Failing Conformance Tests (RED)

**Files:**
- Modify: `src/tests/test_guard_conformance.py`

- [ ] **Step 1: Add `TestGuardImportIsolation` class**

This test scans each guard module's AST for analytics-extra imports at module level. Guards must only use stdlib + PySpark + `ingestion.guards` — analytics packages belong inside function bodies.

```python
class TestGuardImportIsolation:
    """Guard modules must not have analytics-extra imports at module level.

    The freshness gate runs in the ``default`` Databricks environment which
    only has the luxury-lakehouse wheel (no analytics extras like scipy,
    xgboost, silly-kicks). Module-level imports of these packages cause
    silent guard failures — the gate swallows the ImportError and treats
    the guard as count=0, so the pipeline bypasses the gate entirely.
    """

    _ANALYTICS_PACKAGES: ClassVar[frozenset[str]] = frozenset({
        "analytics",
        "silly_kicks",
        "xgboost",
        "scipy",
        "sklearn",
        "rapidfuzz",
        "sparse_dot_topn",
        "unidecode",
        "mplsoccer",
        "matplotlib",
        "torch",
        "socceraction",
    })

    def test_no_analytics_imports_at_module_level(self) -> None:
        """Top-level imports in guard modules must not pull analytics extras."""
        failures: list[str] = []
        for module_path in _GUARD_MODULES:
            mod = importlib.import_module(module_path)
            source_file = inspect.getfile(mod)
            source = Path(source_file).read_text(encoding="utf-8")
            tree = ast.parse(source)

            for node in ast.iter_child_nodes(tree):
                # Only check TYPE_CHECKING=False imports (skip TYPE_CHECKING blocks)
                if isinstance(node, ast.If):
                    continue
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        root = alias.name.split(".")[0]
                        if root in self._ANALYTICS_PACKAGES:
                            failures.append(f"{module_path}: import {alias.name}")
                elif isinstance(node, ast.ImportFrom) and node.module:
                    root = node.module.split(".")[0]
                    if root in self._ANALYTICS_PACKAGES:
                        failures.append(f"{module_path}: from {node.module}")

        assert not failures, (
            "Guard modules have analytics imports at module level "
            "(move to function bodies):\n" + "\n".join(sorted(failures))
        )
```

Add `ClassVar` to the existing imports from `typing` at the top of the file if not already present.

- [ ] **Step 2: Replace `TestPipelineAcceptsFilterResult` with `TestMandatoryFilterResult`**

Delete the entire `TestPipelineAcceptsFilterResult` class and replace with:

```python
class TestMandatoryFilterResult:
    """run_pipeline() must accept filter_result as a REQUIRED parameter.

    The mandatory injection pattern means pipelines cannot run without a
    FilterResult — they receive it from main() which resolves it from
    either the freshness gate (production) or skip_guard.check() (standalone).
    """

    _EXEMPT: ClassVar[set[str]] = {
        "ingestion.statsbomb",
        "ingestion.metrica",
        "ingestion.idsse",
        "ingestion.skillcorner",
        "ingestion.wyscout",
        "ingestion.import_obso_results",
        "ingestion.import_psxg_predictions",
        "ingestion.import_space_creation",
        "ingestion.model_validation",
        "ingestion.sync_hf_costs",
        "ingestion.defcon_lite_360",
        "ingestion.defcon_lite_tracking",
    }

    _SPECIAL_CASES: ClassVar[dict[str, list[str]]] = {
        "ingestion.defcon_lite": ["filter_360", "filter_tracking"],
    }

    def test_filter_result_is_required(self) -> None:
        """filter_result param must have no default value."""
        for module_path in _GUARD_MODULES:
            if module_path in self._EXEMPT:
                continue

            mod = importlib.import_module(module_path)

            pipeline_fn = None
            for name, obj in inspect.getmembers(mod, inspect.isfunction):
                if name.startswith("run_pipeline"):
                    pipeline_fn = obj
                    break

            if pipeline_fn is None:
                continue

            sig = inspect.signature(pipeline_fn)
            expected_params = self._SPECIAL_CASES.get(module_path, ["filter_result"])

            for param_name in expected_params:
                assert param_name in sig.parameters, (
                    f"{module_path}.{pipeline_fn.__name__}() missing '{param_name}' param"
                )
                param = sig.parameters[param_name]
                assert param.default is inspect.Parameter.empty, (
                    f"{module_path}.{pipeline_fn.__name__}(): '{param_name}' must be "
                    f"required (no default), but has default={param.default}"
                )
```

Note: `tracking_metadata` is intentionally **removed** from `_EXEMPT` — it's being promoted to use the mandatory injection pattern.

- [ ] **Step 3: Add `TestNoInlineGuardInPipeline` class**

```python
class TestNoInlineGuardInPipeline:
    """Pipeline functions must not run inline guards — IDs come from filter_result.

    ``find_new_ids`` and ``find_incomplete_formation_ids`` must only appear
    inside guard classes (``*Guard``) and ``main*`` functions. Any other
    function calling them duplicates the gate's work.
    """

    _EXEMPT: ClassVar[set[str]] = {
        "ingestion.statsbomb",
        "ingestion.metrica",
        "ingestion.idsse",
        "ingestion.skillcorner",
        "ingestion.wyscout",
        "ingestion.import_obso_results",
        "ingestion.import_psxg_predictions",
        "ingestion.import_space_creation",
        "ingestion.model_validation",
        "ingestion.sync_hf_costs",
        "ingestion.defcon_lite_360",
        "ingestion.defcon_lite_tracking",
    }

    _GUARD_CALL_MARKERS: ClassVar[frozenset[str]] = frozenset({
        "find_new_ids",
        "find_incomplete_formation_ids",
    })

    def test_no_guard_calls_outside_guard_class(self) -> None:
        """Non-guard, non-main functions must not call find_new_ids."""
        failures: list[str] = []
        for module_path in _GUARD_MODULES:
            if module_path in self._EXEMPT:
                continue

            mod = importlib.import_module(module_path)
            source_file = inspect.getfile(mod)
            source = Path(source_file).read_text(encoding="utf-8")
            tree = ast.parse(source)

            guard_classes = {
                node.name
                for node in ast.iter_child_nodes(tree)
                if isinstance(node, ast.ClassDef) and "Guard" in node.name
            }

            for node in ast.iter_child_nodes(tree):
                if isinstance(node, ast.ClassDef) and node.name in guard_classes:
                    continue
                if isinstance(node, ast.FunctionDef) and node.name.startswith("main"):
                    continue
                if isinstance(node, ast.FunctionDef):
                    for marker in self._GUARD_CALL_MARKERS:
                        if _ast_has_name_or_attr(node, marker):
                            failures.append(
                                f"{module_path}.{node.name}() calls {marker}()"
                            )

        assert not failures, (
            "Pipeline functions must not run inline guards "
            "(use filter_result.metadata instead):\n" + "\n".join(sorted(failures))
        )
```

- [ ] **Step 4: Replace `TestMainCallsReadGateResult` with `TestMainStandaloneResolution`**

Delete the entire `TestMainCallsReadGateResult` class and replace with:

```python
class TestMainStandaloneResolution:
    """main() must resolve guard result: gate first, skip_guard.check() fallback.

    In production, main() reads FilterResult from Databricks task values
    via read_gate_result(). In standalone mode (no task values), it must
    call skip_guard.check() to compute the result locally. Both paths
    must be present.
    """

    _EXEMPT: ClassVar[set[str]] = {
        "ingestion.statsbomb",
        "ingestion.metrica",
        "ingestion.idsse",
        "ingestion.skillcorner",
        "ingestion.wyscout",
        "ingestion.import_obso_results",
        "ingestion.import_psxg_predictions",
        "ingestion.import_space_creation",
        "ingestion.model_validation",
        "ingestion.sync_hf_costs",
        "ingestion.defcon_lite_360",
        "ingestion.defcon_lite_tracking",
    }

    def test_main_has_gate_and_fallback(self) -> None:
        """main() must call both read_gate_result and skip_guard.check."""
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
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.FunctionDef) and node.name.startswith("main")
            ]

            for main_fn in main_fns:
                has_gate = _ast_has_name_or_attr(main_fn, "read_gate_result")
                has_fallback = _ast_has_name_or_attr(main_fn, "skip_guard")

                assert has_gate, (
                    f"{module_path}.{main_fn.name}() does not call read_gate_result()"
                )
                assert has_fallback, (
                    f"{module_path}.{main_fn.name}() does not call "
                    f"skip_guard.check() as standalone fallback"
                )
```

Note: `tracking_metadata` is intentionally **removed** from `_EXEMPT`.

- [ ] **Step 5: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_guard_conformance.py -v --tb=line 2>&1 | tail -60`

Expected failures:
- `TestGuardImportIsolation` — 5+ modules with analytics imports at module level
- `TestMandatoryFilterResult` — ~15 modules with `filter_result` defaulting to `None`
- `TestNoInlineGuardInPipeline` — ~6 modules calling `find_new_ids` in pipeline functions
- `TestMainStandaloneResolution` — ~15 modules missing `skip_guard.check()` fallback

Record the exact failure count for each test class.

---

## Task 2: Guard Import Isolation (D40h)

**Files:**
- Modify: `src/ingestion/spadl_vaep.py`
- Modify: `src/ingestion/entity_resolution.py`
- Modify: `src/ingestion/defcon_lite_360.py`
- Modify: `src/ingestion/defcon_lite_tracking.py`
- Modify: `src/ingestion/line_breaking.py`

The pattern for each file: move analytics-extra imports from module level into the function bodies that use them. The guard class and `skip_guard` instance must be importable with only stdlib + PySpark + `ingestion.guards`.

- [ ] **Step 1: Fix `spadl_vaep.py`**

Move these from module level to inside `run_pipeline()` (or the first function that uses them):

```python
# REMOVE from module level:
import silly_kicks.vaep.features as fs
from xgboost import XGBClassifier

# ADD inside run_pipeline() or the function body that uses them:
def run_pipeline(...):
    import silly_kicks.vaep.features as fs
    from xgboost import XGBClassifier
    ...
```

Keep all other module-level imports unchanged. The guard class `_VaepGuard` only uses `find_new_ids` from `ingestion.guards` — it does not need `silly_kicks` or `xgboost`.

- [ ] **Step 2: Fix `entity_resolution.py`**

```python
# REMOVE from module level:
from analytics.entity_resolution import ResolutionConfig, resolve_players

# ADD inside run_pipeline() body:
def run_pipeline(...):
    from analytics.entity_resolution import ResolutionConfig, resolve_players
    ...
```

- [ ] **Step 3: Fix `defcon_lite_360.py`**

```python
# REMOVE from module level:
from analytics.defcon_lite import DefconLiteParams
from ingestion.defcon_lite_common import _TABLE_NAME, _make_values_udf

# ADD inside process_360_matches() body:
def process_360_matches(...):
    from analytics.defcon_lite import DefconLiteParams
    from ingestion.defcon_lite_common import _TABLE_NAME, _make_values_udf
    ...
```

Keep `from ingestion.guards import FilterResult` at module level (needed by guard + process function signature).

- [ ] **Step 4: Fix `defcon_lite_tracking.py`**

Same pattern as Step 3 — move `analytics.defcon_lite` and `ingestion.defcon_lite_common` imports into `process_tracking_matches()` body.

- [ ] **Step 5: Fix `line_breaking.py`**

```python
# REMOVE from module level:
from analytics.line_breaking import LineBreakingParams
from ingestion.line_breaking_360 import _make_statsbomb_udf as _make_statsbomb_udf
from ingestion.line_breaking_360 import _process_statsbomb_360
from ingestion.line_breaking_tracking import _make_idsse_udf as _make_idsse_udf
from ingestion.line_breaking_tracking import _make_metrica_udf as _make_metrica_udf
from ingestion.line_breaking_tracking import _process_idsse_tracking, _process_metrica_tracking

# ADD inside run_pipeline() body:
def run_pipeline(...):
    from analytics.line_breaking import LineBreakingParams
    from ingestion.line_breaking_360 import _process_statsbomb_360
    from ingestion.line_breaking_tracking import (
        _process_idsse_tracking,
        _process_metrica_tracking,
    )
    ...
```

The sub-module imports (`line_breaking_360`, `line_breaking_tracking`) are deferred because they themselves import `analytics.line_breaking` at their top level.

- [ ] **Step 6: Run `TestGuardImportIsolation` to verify GREEN**

Run: `uv run pytest src/tests/test_guard_conformance.py::TestGuardImportIsolation -v`

Expected: PASS — all guard modules now importable without analytics extras.

---

## Task 3: Pipeline Mandatory Injection — Group A (Reference Pipelines)

**Files:**
- Modify: `src/ingestion/off_ball_xt.py`
- Modify: `src/ingestion/pitch_control_batch.py`
- Modify: `src/ingestion/elastic_sync.py`
- Modify: `src/ingestion/pausa.py`

These 4 already consume `filter_result.metadata` but have a `find_new_ids` fallback branch. Changes:
1. Make `filter_result` required (remove `| None = None` default)
2. Remove the `else: find_new_ids(...)` fallback branch from `_process_matches` / `run_pipeline`
3. Add `skip_guard.check()` fallback in `main()`
4. Use `WorkflowSkippedError` for early exit instead of bare `return`

- [ ] **Step 1: Refactor `off_ball_xt.py` (reference example)**

**`run_pipeline` signature** — change:
```python
filter_result: FilterResult | None = None,
```
to:
```python
filter_result: FilterResult,
```

**`_process_matches` body** — replace the dual-path:
```python
if filter_result and filter_result.metadata.get("new_match_ids"):
    new_match_ids = filter_result.metadata["new_match_ids"]
    logger.info("Processing %d new matches (from freshness gate)", len(new_match_ids))
else:
    new_match_ids = find_new_ids(spark, source_table, results_table)
    logger.info("Processing %d new matches (standalone mode)", len(new_match_ids))
```
with single-path:
```python
new_match_ids = filter_result.metadata["new_match_ids"]
logger.info("Processing %d new matches", len(new_match_ids))
```

Remove the `find_new_ids` import if it was imported for this function.

**`run_pipeline` early exit** — change bare return to WorkflowSkippedError:
```python
if filter_result.count == 0:
    raise WorkflowSkippedError("No new work")
```
Add import: `from workflows.exceptions import WorkflowSkippedError`

**`main()`** — add standalone fallback:
```python
filter_result = read_gate_result("wf-off-ball-xt")
if filter_result is None:
    filter_result = skip_guard.check(spark, args.catalog, args.schema)
run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
```

- [ ] **Step 2: Apply same pattern to `pitch_control_batch.py`**

Same changes as Step 1. Metadata key: `new_match_ids`. The `_process_matches` function has the same dual-path structure. `main()` gate ID: `"wf-pitch-control"`.

- [ ] **Step 3: Apply same pattern to `elastic_sync.py`**

Same changes. Metadata key: `new_match_ids`. Gate ID: `"wf-elastic-sync"`.

- [ ] **Step 4: Apply same pattern to `pausa.py`**

Same changes. Metadata key: `new_match_ids`. Gate ID: `"wf-pausa"`.

- [ ] **Step 5: Run conformance tests for Group A**

Run: `uv run pytest src/tests/test_guard_conformance.py -v -k "off_ball or pitch_control or elastic or pausa" --tb=short`

If no per-pipeline parametrization exists, run the full suite and check that failure count decreased by 4 for each test class.

---

## Task 4: Pipeline Mandatory Injection — Group B (ID-Based Compute Pipelines)

**Files:**
- Modify: `src/ingestion/defcon_lite.py`
- Modify: `src/ingestion/line_breaking.py`
- Modify: `src/ingestion/formations_efpi.py`
- Modify: `src/ingestion/formations_shape_graph.py`
- Modify: `src/ingestion/spadl_vaep.py`
- Modify: `src/ingestion/xg_model.py`
- Modify: `src/ingestion/xg_model_v2.py`
- Modify: `src/ingestion/expected_threat.py`
- Modify: `src/ingestion/entity_resolution.py`

Each pipeline gets the same 4 changes as Group A, plus removing its specific inline guard logic. Refer to the Pipeline Reference Table for each file's metadata keys and inline logic.

- [ ] **Step 1: Refactor `defcon_lite.py` (orchestrator)**

**Signature** — change both filter params from Optional to required:
```python
filter_360: FilterResult,
filter_tracking: FilterResult,
```

**`main()`** — add standalone fallback for both:
```python
filter_360 = read_gate_result("wf-defcon")
if filter_360 is None:
    from ingestion.defcon_lite_360 import skip_guard as guard_360
    filter_360 = guard_360.check(spark, args.catalog, args.schema)

filter_tracking = read_gate_result("wf-defcon-tracking")
if filter_tracking is None:
    from ingestion.defcon_lite_tracking import skip_guard as guard_tracking
    filter_tracking = guard_tracking.check(spark, args.catalog, args.schema)
```

The orchestrator passes these through to sub-module process functions — no inline guard logic to remove.

- [ ] **Step 2: Refactor `line_breaking.py`**

**Signature** — make `filter_result` required.

**`run_pipeline` body** — forward metadata to sub-functions. The guard produces `statsbomb_360_ids`, `metrica_ids`, `idsse_ids`. Replace:
```python
path_a_rows = _process_statsbomb_360(spark, catalog, schema, logger, params)
path_b_rows = _process_metrica_tracking(spark, catalog, schema, logger, params)
path_c_rows = _process_idsse_tracking(spark, catalog, schema, logger, params)
```
with:
```python
sb_ids = filter_result.metadata.get("statsbomb_360_ids", [])
met_ids = filter_result.metadata.get("metrica_ids", [])
idsse_ids = filter_result.metadata.get("idsse_ids", [])

path_a_rows = _process_statsbomb_360(spark, catalog, schema, logger, params, new_ids=sb_ids)
path_b_rows = _process_metrica_tracking(spark, catalog, schema, logger, params, new_ids=met_ids)
path_c_rows = _process_idsse_tracking(spark, catalog, schema, logger, params, new_ids=idsse_ids)
```

Each `_process_*` sub-function signature adds `new_ids: list[str]` parameter and removes its inline `existing_ids` set-building logic, using the passed IDs directly.

Note: the `_process_*` functions live in `line_breaking_360.py` and `line_breaking_tracking.py`. Modify those files to accept `new_ids` and remove the inline `existing_ids` re-computation. This is the sub-function forwarding that was missing.

**`main()`** — add `skip_guard.check()` fallback. Gate ID: `"wf-line-breaking"`.

**Early exit** — use `WorkflowSkippedError`.

- [ ] **Step 3: Refactor `formations_efpi.py`**

**Signature** — make `filter_result` required on `run_pipeline_efpi()`.

**Body** — pass `filter_result.metadata["new_match_ids"]` into `_run_efpi()` instead of letting it call `prepare_tracking_data()` for re-discovery. The `_run_efpi` function should accept a `new_match_ids: list[str]` parameter.

**`main_efpi()`** — add `skip_guard.check()` fallback. Gate ID: `"wf-formations"`.

Also update `run_pipeline()` (the combined orchestrator) if it exists, with same pattern.

- [ ] **Step 4: Refactor `formations_shape_graph.py`**

Same as Step 3. Metadata key: `new_match_ids`. Gate ID: `"wf-formations-sg"`. Pass IDs to `_run_shape_graph()`.

- [ ] **Step 5: Refactor `spadl_vaep.py`**

**Signature** — make `filter_result` required.

**Body** — replace inline `_read_existing_match_ids` + `unscored` computation with:
```python
new_spadl_ids = filter_result.metadata["new_spadl_match_ids"]
unscored_ids = filter_result.metadata["unscored_vaep_match_ids"]
```

Remove the `_read_existing_match_ids` calls and the inline `unscored_match_ids` computation from `run_pipeline`.

**`main()`** — add `skip_guard.check()` fallback. Gate ID: `"wf-vaep"`.

- [ ] **Step 6: Refactor `xg_model.py`**

**Signature** — make `filter_result` required.

**Body** — replace the inline `collect()` + set difference:
```python
# REMOVE this block:
existing = {row.competition_id for row in spark.table(results_table).select(...).distinct().collect()}
available_comps = ...
new_comps = sorted(available_comps - existing)
```
with:
```python
new_comps = filter_result.metadata["new_competition_ids"]
```

**`main()`** — add `skip_guard.check()` fallback. Gate ID: `"wf-xg-v1"`.

- [ ] **Step 7: Refactor `xg_model_v2.py`**

Same as Step 6. Gate ID: `"wf-xg-v2"`.

- [ ] **Step 8: Refactor `expected_threat.py`**

**Signature** — make `filter_result` required.

**Body** — replace the inline re-computation (most expensive case: full `.toPandas()` reload):
```python
# REMOVE the inline existing/available_comps/need_global recomputation
# REPLACE with:
new_comps = filter_result.metadata["new_competition_ids"]
need_global = filter_result.metadata.get("need_global", False)
```

**`main()`** — add `skip_guard.check()` fallback. Gate ID: `"wf-xt-grids"`.

- [ ] **Step 9: Refactor `entity_resolution.py`**

**Signature** — make `filter_result` required.

**Body** — remove the inline existence re-check (the `try: existing_count = spark.table(xref_table).limit(1).count()` block). The guard already determined whether to run. Trust `filter_result.count > 0`.

**`main()`** — add `skip_guard.check()` fallback. Gate ID: `"wf-entity-resolution"`.

- [ ] **Step 10: Run conformance tests for Group B**

Run: `uv run pytest src/tests/test_guard_conformance.py -v --tb=short`

Check that `TestNoInlineGuardInPipeline` failures decreased to 0 for Group B modules.

---

## Task 5: Pipeline Mandatory Injection — Groups C + D (Count-Based + Special Cases)

**Files:**
- Modify: `src/ingestion/export_embeddings_training_data.py`
- Modify: `src/ingestion/prepare_360_training_data.py`
- Modify: `src/ingestion/player_embeddings_v2.py`
- Modify: `src/ingestion/tracking_metadata.py`

- [ ] **Step 1: Refactor `export_embeddings_training_data.py`**

**Signature** — make `filter_result` required.

**Body** — remove inline count comparison in `_export_training_sequences()`. Trust `filter_result.count > 0` from the gate.

**`main()`** — add `skip_guard.check()` fallback. Gate ID: `"wf-football2vec-v2-export"`.

- [ ] **Step 2: Refactor `prepare_360_training_data.py`**

**Signature** — make `filter_result` required. Note: this file has a non-standard signature with `volume_path` as 4th positional arg.

**Body** — remove inline count comparison in step 1.

**`main()`** — add `skip_guard.check()` fallback. Gate ID: `"wf-prepare-360-data"`.

- [ ] **Step 3: Refactor `player_embeddings_v2.py`**

**Signature** — make `filter_result` required on both `run_pipeline()` and `run_pipeline_v2()`.

No inline guard to remove (stub guard always returns count=1). The change is purely structural.

**`main()` and `main_v2()`** — add `skip_guard.check()` fallback. Gate ID: `"wf-football2vec-v2"`.

- [ ] **Step 4: Refactor `tracking_metadata.py`**

This is the most structurally divergent file — it predates the guard pattern entirely.

**`run_pipeline` signature** — add `filter_result: FilterResult` parameter:
```python
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    data_dir: str = _IDSSE_DATA_DIR,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> None:
```

**`run_pipeline` body** — add early exit:
```python
if filter_result.count == 0:
    raise WorkflowSkippedError("No new work — tracking metadata already populated")
```

**`main()`** — add `read_gate_result` + `skip_guard.check()` fallback:
```python
from ingestion.guards import read_gate_result
filter_result = read_gate_result("wf-tracking-metadata")
if filter_result is None:
    filter_result = skip_guard.check(spark, args.catalog, args.schema)
run_pipeline(spark, args.catalog, args.schema, filter_result=filter_result)
```

Import `WorkflowSkippedError` and `FilterResult` at module level.

- [ ] **Step 5: Run full conformance suite — verify ALL GREEN**

Run: `uv run pytest src/tests/test_guard_conformance.py -v --tb=short`

Expected: ALL tests pass. Every conformance test class should show 0 failures.

Also run the full test suite to check for regressions:

Run: `uv run pytest src/tests/ -v --tb=short -x`

---

## Task 6: Terraform `run_if` (D40c)

**Files:**
- Modify: `terraform/modules/workflows/main.tf`

- [ ] **Step 1: Research `run_if` / `condition_task` syntax**

Check the Databricks Terraform provider version in use:

Run: `grep -r "databricks" terraform/.terraform.lock.hcl 2>/dev/null | head -5`
Run: `grep "required_providers" terraform/environments/dev/main.tf`

Verify the provider supports `condition_task` or `run_if` for job task definitions. Consult the [Databricks Terraform provider docs](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/job) for the exact syntax.

The expected pattern (verify against docs):
```hcl
task {
  task_key = "compute_pitch_control"

  condition_task {
    op    = "GREATER_THAN"
    left  = "{{tasks.freshness_gate.values.wf-pitch-control}}"
    right = "0"
  }

  depends_on {
    task_key = "freshness_gate"
  }
  # ... rest of task config
}
```

If `condition_task` is not the right mechanism, the alternative is a lightweight Python task that reads the task value and exits with a specific code. Document findings before proceeding.

- [ ] **Step 2: Wire `run_if` on downstream compute tasks**

Add the condition to every compute task that has a corresponding guard. The task value key matches the `workflow_id` from the guard.

Affected tasks in `main.tf` (compute pipelines, not ingestors):
- `compute_pitch_control` → `wf-pitch-control`
- `compute_off_ball_xt` → `wf-off-ball-xt`
- `compute_spadl_vaep` → `wf-vaep`
- `compute_xg_model` → `wf-xg-v1`
- `compute_xg_model_v2` → `wf-xg-v2`
- `compute_expected_threat` → `wf-xt-grids`
- `compute_defcon_lite` → `wf-defcon` (uses two guards)
- `compute_line_breaking` → `wf-line-breaking`
- `compute_formations_efpi` → `wf-formations`
- `compute_formations_shape_graph` → `wf-formations-sg`
- `compute_pausa` → `wf-pausa`
- `compute_elastic_sync` → `wf-elastic-sync`
- `compute_entity_resolution` → `wf-entity-resolution`
- `compute_tracking_metadata` → `wf-tracking-metadata`

Remove the D40a TODO comment from line 64.

- [ ] **Step 3: Validate Terraform plan**

Run: `cd terraform/environments/dev && terraform plan -no-color 2>&1 | tail -30`

Verify the plan shows the expected changes to the job resource (task conditions added) with no errors.

---

## Task 7: Entity Count Observability (D40b)

**Files:**
- Modify: `src/tests/test_cost_hook.py`
- Modify: `scripts/create_cost_table.sql`
- Modify: `src/workflows/context.py`
- Modify: `src/workflows/runner.py`
- Modify: `src/ingestion/cost_hook.py`

- [ ] **Step 1: Write failing tests for `entity_count`**

In `src/tests/test_cost_hook.py`:

Add `entity_count` to the `REQUIRED_COLUMNS` set in `TestColumnCompleteness`:
```python
REQUIRED_COLUMNS = {
    "workflow_id", "phase", "run_id", "runtime",
    "job_run_id", "task_key", "hf_job_id",
    "state", "started_at", "ended_at",
    "duration_seconds", "row_count", "entity_count",  # <-- ADD
    "rate_usd_per_hour", "estimated_cost_usd",
    "cost_source", "updated_at",
}
```

Add a test in `TestOnStart`:
```python
def test_on_start_includes_entity_count(self) -> None:
    """on_start row should include entity_count from context."""
    ctx = WorkflowContext(
        workflow_id="wf-test",
        phase="compute",
        entity_count=5,
    )
    self.hook.on_start(ctx)
    row = self._extract_row()
    assert row["entity_count"] == 5
```

Add a test for entity_count=None (skip/error cases):
```python
def test_on_start_entity_count_none_when_not_set(self) -> None:
    """entity_count should be None when context has no entity_count."""
    ctx = WorkflowContext(workflow_id="wf-test", phase="compute")
    self.hook.on_start(ctx)
    row = self._extract_row()
    assert row["entity_count"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_cost_hook.py -v --tb=short -k entity_count`

Expected: FAIL — `entity_count` not in `WorkflowContext`, not in cost hook schema.

- [ ] **Step 3: Add `entity_count` to DDL**

In `scripts/create_cost_table.sql`, add after `row_count`:
```sql
    entity_count INT,
```

- [ ] **Step 4: Add `entity_count` to `WorkflowContext`**

In `src/workflows/context.py`:
```python
@dataclass(frozen=True)
class WorkflowContext:
    workflow_id: str
    phase: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    workflow_name: str = ""
    workflow_type: str = ""
    partition_key: str = ""
    entity_count: int | None = None  # <-- ADD
```

Update `log_extra()` to include `entity_count` when set:
```python
def log_extra(self) -> dict[str, str | int]:
    extra = {
        "workflow_id": self.workflow_id,
        "workflow_phase": self.phase,
        "run_id": self.run_id,
        "workflow_name": self.workflow_name,
        "started_at": self.started_at.isoformat(),
    }
    if self.workflow_type:
        extra["workflow_type"] = self.workflow_type
    if self.partition_key:
        extra["partition_key"] = self.partition_key
    if self.entity_count is not None:
        extra["entity_count"] = self.entity_count
    return extra
```

- [ ] **Step 5: Runner extracts `entity_count` from `filter_result`**

In `src/workflows/runner.py`, in `run_workflow()`, after building the `WorkflowContext`, extract entity_count from kwargs:

```python
# Extract entity_count from filter_result if present
entity_count = None
filter_result = kwargs.get("filter_result")
if filter_result is not None and hasattr(filter_result, "count"):
    entity_count = filter_result.count

# Also check defcon_lite special case (filter_360 / filter_tracking)
if entity_count is None:
    for key in ("filter_360", "filter_tracking"):
        fr = kwargs.get(key)
        if fr is not None and hasattr(fr, "count"):
            entity_count = (entity_count or 0) + fr.count

ctx = WorkflowContext(
    workflow_id=entry.workflow_id,
    phase=entry.phase,
    entity_count=entity_count,
    # ... rest of fields from card
)
```

- [ ] **Step 6: CostEstimateHook writes `entity_count`**

In `src/ingestion/cost_hook.py`, add `entity_count` to the schema:

```python
StructField("entity_count", IntegerType(), True),
```

In `_build_row()` (or equivalent row construction), add:
```python
"entity_count": ctx.entity_count,
```

This field gets set in `on_start` and persists through `on_complete`/`on_skip`/`on_error` via MERGE.

- [ ] **Step 7: Run tests to verify GREEN**

Run: `uv run pytest src/tests/test_cost_hook.py -v --tb=short`

Expected: ALL pass, including new `entity_count` tests.

---

## Task 8: Final Verification

- [ ] **Step 1: Full test suite**

Run: `uv run pytest src/tests/ -v --tb=short`

Expected: ALL pass. Zero regressions.

- [ ] **Step 2: Lint and type check**

Run: `uv run ruff check src/ scripts/`
Run: `uv run pyright src/`

Fix any issues.

- [ ] **Step 3: Review changes**

Run: `git diff --stat`

Verify the file list matches the File Map above. No unexpected files modified.

---

## Post-Implementation Notes

### What's NOT in this plan (deferred)

- **D40d** (`for_each_task` fan-out): Requires D40c to be in place first. Separate follow-up.
- **D40e** (guard parallelization): Independent optimization. Separate follow-up.
- **D40f** (`backfill_statsbomb_extra` bottleneck investigation): Investigation, not implementation. Separate.
- **D35** (Workflows drilldown page): Depends on D40b entity_count data being available. Next cycle.
- **Fixing stub guards** (e.g., `player_embeddings_v2`'s always-count=1): The mandatory injection pattern works even with stub guards. Real guard logic is a separate improvement.

### Migration Path for `workflow_cost_live`

The `entity_count` column addition (D40b) requires an `ALTER TABLE` on the existing Delta table:

```sql
ALTER TABLE soccer_analytics.observability.workflow_cost_live
ADD COLUMN entity_count INT;
```

Run this before deploying the updated cost hook. Existing rows will have `NULL` for `entity_count`, which is correct (pre-observability runs).
