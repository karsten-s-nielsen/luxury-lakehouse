"""Guard conformance tests — auto-validates ALL guards and pipelines.

Discovers guards from ``_GUARD_MODULES`` and pipelines from ``@workflow``
decorators. When a developer adds a new workflow, these tests automatically
cover it — no manual test additions needed.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import sys
from pathlib import Path
from typing import ClassVar
from unittest.mock import MagicMock

import pytest

from ingestion.guards import _GUARD_MODULES, FilterResult

# ---------------------------------------------------------------------------
# Guards that are intentionally stubs or don't produce ID-based metadata.
# Exempt from the metadata-quality checks in TestGuardMetadataContract.
# ---------------------------------------------------------------------------
_METADATA_EXEMPT = {
    "wf-statsbomb",  # Live data, internal skip logic
    "wf-metrica",  # Static dataset, count-based guard
    "wf-idsse",  # Static dataset, count-based guard
    "wf-idsse-events",  # Static dataset, count-based guard
    "wf-skillcorner",  # Static dataset, count-based guard
    "wf-wyscout",  # Static dataset, count-based guard
    "wf-import-obso",  # HF SHA guard — metadata is commit_sha string, not ID list
    "wf-import-psxg",  # HF SHA guard — metadata is commit_sha string, not ID list
    "wf-import-space-creation",  # HF SHA guard — metadata is commit_sha string, not ID list
    "wf-model-validation",  # Monitoring, always-run
    "wf-sync-hf-costs",  # Polling sync, always-run
    "wf-hf-sync",  # Orchestrator, always-run stub
    "wf-football2vec-v2",  # HF SHA guard — metadata is commit_sha string, not ID list
    "wf-football2vec-v2-export",  # Count-comparison guard
    "wf-prepare-360-data",  # Count-comparison guard
}

# Guard modules whose pipeline doesn't have its own run_pipeline —
# the run_pipeline lives in an orchestrator (e.g., defcon_lite_360 → defcon_lite).
_NO_OWN_PIPELINE = {
    "ingestion.defcon_lite_360",
    "ingestion.defcon_lite_tracking",
}


# ---------------------------------------------------------------------------
# Spark mock helpers
# ---------------------------------------------------------------------------


class _ColumnMock:
    """Mock PySpark Column that supports comparison operators.

    PySpark Column objects support ``col >= 2`` etc.  Plain MagicMock raises
    ``TypeError`` on ``>=`` in Python 3.10 because MagicMock's metaclass
    explicitly removes comparison dunder methods.  This plain class delegates
    all attribute access to a MagicMock but keeps comparison operators working.
    """

    def __init__(self) -> None:
        self._inner = MagicMock()

    def __getattr__(self, name: str) -> object:
        return getattr(self._inner, name)

    def __ge__(self, other: object) -> _ColumnMock:
        return _ColumnMock()

    def __le__(self, other: object) -> _ColumnMock:
        return _ColumnMock()

    def __gt__(self, other: object) -> _ColumnMock:
        return _ColumnMock()

    def __lt__(self, other: object) -> _ColumnMock:
        return _ColumnMock()

    def __eq__(self, other: object) -> _ColumnMock:  # type: ignore[override]
        return _ColumnMock()

    def __ne__(self, other: object) -> _ColumnMock:  # type: ignore[override]
        return _ColumnMock()


def _mock_pyspark_functions() -> MagicMock:
    """Register a mock pyspark.sql.functions module in sys.modules.

    Must be called before importing any guard module that uses
    ``from pyspark.sql import functions as F``.
    """
    mock_functions = MagicMock()
    mock_functions.col.return_value = _ColumnMock()
    if "pyspark" not in sys.modules:
        sys.modules["pyspark"] = MagicMock()
    mock_sql = MagicMock()
    mock_sql.functions = mock_functions
    sys.modules["pyspark.sql"] = mock_sql
    sys.modules["pyspark.sql.functions"] = mock_functions
    return mock_functions


def _make_permissive_spark_mock() -> MagicMock:
    """Create a Spark mock where all chained calls return MagicMock.

    Mocks are configured so that:
    - table().filter().select().distinct().collect() returns rows with "new" IDs
    - table().filter().select().distinct().count() returns a positive number
    - join() returns a mock whose collect() returns the same rows (anti-join)
    - catalog.tableExists() returns True

    This ensures guards see "new work" and produce metadata.
    """
    _mock_pyspark_functions()
    spark = MagicMock()

    rows: list[dict[str, str]] = [
        {
            "match_id": "m1",
            "matchId": "m1",
            "competition_name": "test",
            "competition_id": "c1",
            "player_id": "p1",
            "_join_id": "m1",
            "last_imported_sha": "mock_sha_1",
        },
        {
            "match_id": "m2",
            "matchId": "m2",
            "competition_name": "test2",
            "competition_id": "c2",
            "player_id": "p2",
            "_join_id": "m2",
            "last_imported_sha": "mock_sha_2",
        },
    ]

    def make_df_mock() -> MagicMock:
        df = MagicMock()
        df.filter.return_value = df
        df.select.return_value = df
        df.distinct.return_value = df
        df.collect.return_value = rows
        df.count.return_value = 2
        df.join.return_value = df  # anti-join returns same mock (simulates "all new")
        df.groupBy.return_value.agg.return_value.filter.return_value.select.return_value = df
        df.limit.return_value = df
        df.limit.return_value.count.return_value = 1
        df.alias.return_value = df
        df.subtract.return_value = df
        return df

    spark.table.side_effect = lambda name: make_df_mock()
    spark.sql.side_effect = lambda query: make_df_mock()
    spark.read.parquet.return_value = make_df_mock()
    spark.catalog.tableExists.return_value = True

    return spark


# ---------------------------------------------------------------------------
# TestGuardRegistry — Registry integrity
# ---------------------------------------------------------------------------


class TestGuardRegistry:
    """Every guard module in _GUARD_MODULES must be importable and well-formed."""

    def test_all_guard_modules_importable(self) -> None:
        """Every entry in _GUARD_MODULES imports and has a skip_guard attribute."""
        for module_path in _GUARD_MODULES:
            mod = importlib.import_module(module_path)
            assert hasattr(mod, "skip_guard"), f"{module_path} missing skip_guard"

    def test_all_guards_have_workflow_id(self) -> None:
        """Every skip_guard must have a non-empty workflow_id starting with 'wf-'."""
        for module_path in _GUARD_MODULES:
            mod = importlib.import_module(module_path)
            guard = mod.skip_guard
            assert isinstance(guard.workflow_id, str), f"{module_path}: workflow_id not a string"
            assert guard.workflow_id.startswith("wf-"), f"{module_path}: workflow_id must start with 'wf-'"

    def test_no_duplicate_workflow_ids(self) -> None:
        """No two guards may share the same workflow_id."""
        ids: list[str] = []
        for module_path in _GUARD_MODULES:
            mod = importlib.import_module(module_path)
            ids.append(mod.skip_guard.workflow_id)
        duplicates = [wid for wid in ids if ids.count(wid) > 1]
        assert not duplicates, f"Duplicate workflow_ids: {set(duplicates)}"

    def test_guard_check_returns_filter_result(self) -> None:
        """Every guard.check() with a mocked Spark returns a FilterResult."""
        for module_path in _GUARD_MODULES:
            mod = importlib.import_module(module_path)
            guard = mod.skip_guard
            spark = _make_permissive_spark_mock()
            result = guard.check(spark, "soccer_analytics", "dev_gold")
            assert isinstance(result, FilterResult), (
                f"{module_path}: check() returned {type(result).__name__}, expected FilterResult"
            )

    def test_guard_check_signature(self) -> None:
        """Every guard.check() must accept (spark, catalog, schema) positional args."""
        for module_path in _GUARD_MODULES:
            mod = importlib.import_module(module_path)
            guard = mod.skip_guard
            sig = inspect.signature(guard.check)
            params = list(sig.parameters.keys())
            # First param is self (bound method), so positional are spark, catalog, schema
            assert len(params) >= 3, (
                f"{module_path}: check() must accept at least (spark, catalog, schema), got {params}"
            )


# ---------------------------------------------------------------------------
# TestGuardMetadataContract — Metadata quality
# ---------------------------------------------------------------------------


class TestGuardMetadataContract:
    """Non-exempt guards that find new work must include structured metadata."""

    def test_real_guards_include_metadata(self) -> None:
        """Non-exempt guards with count>0 must have non-empty metadata."""
        for module_path in _GUARD_MODULES:
            mod = importlib.import_module(module_path)
            guard = mod.skip_guard
            if guard.workflow_id in _METADATA_EXEMPT:
                continue
            spark = _make_permissive_spark_mock()
            result = guard.check(spark, "soccer_analytics", "dev_gold")
            if result.count > 0:
                assert result.metadata, f"{guard.workflow_id}: count={result.count} but metadata is empty"

    def test_metadata_id_lists_are_strings(self) -> None:
        """All ID values in metadata lists must be strings, not ints."""
        for module_path in _GUARD_MODULES:
            mod = importlib.import_module(module_path)
            guard = mod.skip_guard
            if guard.workflow_id in _METADATA_EXEMPT:
                continue
            spark = _make_permissive_spark_mock()
            result = guard.check(spark, "soccer_analytics", "dev_gold")
            for key, val in result.metadata.items():
                if isinstance(val, list):
                    for item in val:
                        assert isinstance(item, str), (
                            f"{guard.workflow_id}: metadata['{key}'] contains non-string: {type(item).__name__}"
                        )


# ---------------------------------------------------------------------------
# TestGuardImportIsolation — Guard module imports
# ---------------------------------------------------------------------------


class TestGuardImportIsolation:
    """Guard modules must not have analytics-extra imports at module level.

    The freshness gate runs in the ``default`` Databricks environment which
    only has the luxury-lakehouse wheel (no analytics extras like scipy,
    xgboost, silly-kicks). Module-level imports of these packages cause
    silent guard failures — the gate swallows the ImportError and treats
    the guard as count=0, so the pipeline bypasses the gate entirely.
    """

    _ANALYTICS_PACKAGES: ClassVar[frozenset[str]] = frozenset(
        {
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
        }
    )

    def test_no_analytics_imports_at_module_level(self) -> None:
        """Top-level imports in guard modules must not pull analytics extras."""
        failures: list[str] = []
        for module_path in _GUARD_MODULES:
            mod = importlib.import_module(module_path)
            source_file = inspect.getfile(mod)
            source = Path(source_file).read_text(encoding="utf-8")
            tree = ast.parse(source)

            for node in ast.iter_child_nodes(tree):
                # Skip TYPE_CHECKING blocks (those are fine — not executed at runtime)
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

    def test_no_transitive_analytics_imports_at_module_level(self) -> None:
        """Runtime: importing a guard must not trigger analytics package loads.

        Runs each probe in a **subprocess** so sentinel patching cannot
        leak into the parent pytest process (sys.modules manipulation
        inside the same process is unreliable — Python's import system
        has internal caches beyond sys.modules).
        """
        import subprocess
        import textwrap

        analytics_csv = ",".join(sorted(self._ANALYTICS_PACKAGES))
        failures: list[str] = []

        for module_path in _GUARD_MODULES:
            script = textwrap.dedent(f"""\
                import sys

                # Plant sentinel modules that raise on attribute access
                class _Sentinel:
                    def __init__(self, name):
                        self._name = name
                    def __getattr__(self, attr):
                        raise ImportError(f"Guard transitively imported {{self._name}}")

                for pkg in "{analytics_csv}".split(","):
                    sys.modules[pkg] = _Sentinel(pkg)

                try:
                    import importlib
                    importlib.import_module("{module_path}")
                    print("OK")
                except ImportError as exc:
                    print(f"FAIL: {{exc}}")
            """)

            result = subprocess.run(  # noqa: S603 — trusted script, not user input
                [sys.executable, "-c", script],
                capture_output=True,
                text=True,
                timeout=30,
            )
            output = (result.stdout + result.stderr).strip()
            if "FAIL:" in output:
                failures.append(f"{module_path}: {output.split('FAIL: ', 1)[1]}")
            elif result.returncode != 0:
                failures.append(f"{module_path}: subprocess exited {result.returncode}: {output}")

        assert not failures, "Guard modules have transitive analytics imports at module level:\n" + "\n".join(
            sorted(failures)
        )


# ---------------------------------------------------------------------------
# TestExceptionPropagation — Silent exception swallowing
# ---------------------------------------------------------------------------


class TestExceptionPropagation:
    """Pipeline functions and their helpers must not silently swallow exceptions.

    ``except Exception`` without a ``raise`` hides failures. D48 showed that
    ``backfill_extra_json`` silently swallowed a protobuf overflow affecting
    12.1M rows, so the MERGE appeared to succeed while writing zero rows.
    """

    _EXEMPT: ClassVar[set[str]] = _NO_OWN_PIPELINE  # No own run_pipeline to scan

    @staticmethod
    def _has_silent_except(
        func_node: ast.FunctionDef,
        *,
        top_level_only: bool = True,
        critical_only: bool = True,
    ) -> list[str]:
        """Return descriptions of ExceptHandler nodes that catch Exception without raise.

        Args:
            func_node: The function AST node to scan.
            top_level_only: If True, only check try/except blocks that are
                direct children of the function body (not nested inside loops,
                conditionals, or inner try blocks). This avoids false positives
                from cleanup operations and optional fallback patterns.
            critical_only: If True (and top_level_only), only flag try/except
                blocks where the handler contains a ``return`` statement or
                the try is the last statement in the function body. Cleanup
                blocks (e.g., DROP TABLE) that just log and let the function
                continue are not flagged.

        Exempt: handlers that specifically catch WorkflowSkippedError.
        """
        issues: list[str] = []

        if top_level_only:
            # Only scan direct children of the function body
            candidates: list[ast.ExceptHandler] = []
            for stmt in func_node.body:
                if isinstance(stmt, ast.Try):
                    candidates.extend(stmt.handlers)
        else:
            # Scan ALL exception handlers in the function tree
            candidates = [n for n in ast.walk(func_node) if isinstance(n, ast.ExceptHandler)]

        for node in candidates:
            # Check if handler catches Exception (or bare except)
            if node.type is not None:
                if isinstance(node.type, ast.Name) and node.type.id != "Exception":
                    continue
                if isinstance(node.type, ast.Attribute):
                    continue  # e.g., some_module.SomeError — not generic
                if isinstance(node.type, ast.Tuple):
                    # except (ValueError, TypeError): — all specific, not generic
                    all_specific = all(
                        (isinstance(elt, ast.Name) and elt.id != "Exception") or isinstance(elt, ast.Attribute)
                        for elt in node.type.elts
                    )
                    if all_specific:
                        continue
            # Check handler body for a raise statement
            has_raise = any(isinstance(child, ast.Raise) for child in ast.walk(node))
            if has_raise:
                continue
            # Exempt WorkflowSkippedError catches
            if node.type and isinstance(node.type, ast.Name) and node.type.id == "WorkflowSkippedError":
                continue
            # In critical_only mode, only flag if the handler contains a
            # ``return`` statement — silently exiting the function instead
            # of propagating the error. Cleanup blocks (e.g., DROP TABLE)
            # that log and let the function fall through are not flagged,
            # even when they are the last statement in the function body.
            if top_level_only and critical_only:
                has_return = any(isinstance(child, ast.Return) for child in ast.walk(node))
                if not has_return:
                    continue
            issues.append(f"{func_node.name}() line {node.lineno}")
        return issues

    def test_no_silent_exception_swallow_in_run_pipeline(self) -> None:
        """run_pipeline functions must not catch Exception without re-raising."""
        failures: list[str] = []
        for module_path in _GUARD_MODULES:
            if module_path in self._EXEMPT:
                continue

            mod = importlib.import_module(module_path)
            source_file = inspect.getfile(mod)
            source = Path(source_file).read_text(encoding="utf-8")
            tree = ast.parse(source)

            for node in ast.iter_child_nodes(tree):
                if isinstance(node, ast.FunctionDef) and node.name.startswith("run_pipeline"):
                    issues = self._has_silent_except(node)
                    for issue in issues:
                        failures.append(f"{module_path}.{issue}")

        assert not failures, "run_pipeline() must not silently swallow exceptions:\n" + "\n".join(sorted(failures))

    def test_helper_functions_propagate_exceptions(self) -> None:
        """Functions directly called by run_pipeline must not silently swallow exceptions.

        Scans run_pipeline for deferred ``from X import Y`` statements, then
        inspects ONLY the imported functions (not the entire module) for
        ``except Exception`` without ``raise``. Uses ``critical_only=False``
        to catch deep-nested silent swallowing (like D48's backfill_extra_json).
        """
        failures: list[str] = []
        for module_path in _GUARD_MODULES:
            if module_path in self._EXEMPT:
                continue

            mod = importlib.import_module(module_path)
            source_file = inspect.getfile(mod)
            source = Path(source_file).read_text(encoding="utf-8")
            tree = ast.parse(source)

            # Find run_pipeline function(s)
            for node in ast.iter_child_nodes(tree):
                if not (isinstance(node, ast.FunctionDef) and node.name.startswith("run_pipeline")):
                    continue

                # Extract ImportFrom nodes: {module: [func_names]}
                imported_funcs: dict[str, set[str]] = {}
                for child in ast.walk(node):
                    if isinstance(child, ast.ImportFrom) and child.module:
                        names = {alias.name for alias in child.names}
                        imported_funcs.setdefault(child.module, set()).update(names)

                # Scan only the specifically imported functions
                for imp_mod_name, func_names in imported_funcs.items():
                    try:
                        imp_mod = importlib.import_module(imp_mod_name)
                        imp_source_file = inspect.getfile(imp_mod)
                    except (ImportError, TypeError):
                        # TypeError: MagicMock modules from pyspark stubs
                        continue

                    imp_source = Path(imp_source_file).read_text(encoding="utf-8")
                    imp_tree = ast.parse(imp_source)

                    for func_node in ast.iter_child_nodes(imp_tree):
                        if not isinstance(func_node, ast.FunctionDef):
                            continue
                        if func_node.name not in func_names:
                            continue
                        issues = self._has_silent_except(
                            func_node,
                            top_level_only=False,
                            critical_only=False,
                        )
                        for issue in issues:
                            failures.append(f"{imp_mod_name}.{issue} (called from {module_path}.run_pipeline)")

        assert not failures, (
            "Helper functions called from run_pipeline silently swallow exceptions "
            "(add 'raise' or use specific exception types):\n" + "\n".join(sorted(failures))
        )


# ---------------------------------------------------------------------------
# TestGuardCountMatchesIds — Count/metadata consistency
# ---------------------------------------------------------------------------


class TestGuardCountMatchesIds:
    """Guard count must equal the total number of entity IDs in metadata."""

    # Guards where count includes non-ID metadata (e.g., boolean flags that
    # add +1 to count). These are tested by TestGuardMetadataContract instead.
    _COUNT_EXEMPT: ClassVar[set[str]] = _METADATA_EXEMPT | {
        "wf-xt-grids",  # count includes +1 for need_global boolean
    }

    def test_count_equals_metadata_id_count(self) -> None:
        """For non-exempt guards, count must equal sum of all ID list lengths in metadata."""
        for module_path in _GUARD_MODULES:
            mod = importlib.import_module(module_path)
            guard = mod.skip_guard
            if guard.workflow_id in self._COUNT_EXEMPT:
                continue

            spark = _make_permissive_spark_mock()
            result = guard.check(spark, "soccer_analytics", "dev_gold")

            if result.count == 0:
                continue

            # Sum lengths of all list[str] values in metadata
            total_ids = 0
            for val in result.metadata.values():
                if isinstance(val, list) and all(isinstance(v, str) for v in val):
                    total_ids += len(val)

            assert result.count == total_ids, (
                f"{guard.workflow_id}: count={result.count} but metadata has "
                f"{total_ids} total IDs across {len(result.metadata)} keys"
            )

    def test_metadata_ids_are_distinct(self) -> None:
        """Each ID list in metadata must contain no duplicates."""
        for module_path in _GUARD_MODULES:
            mod = importlib.import_module(module_path)
            guard = mod.skip_guard
            if guard.workflow_id in self._COUNT_EXEMPT:
                continue

            spark = _make_permissive_spark_mock()
            result = guard.check(spark, "soccer_analytics", "dev_gold")

            for key, val in result.metadata.items():
                if isinstance(val, list):
                    dupes = [v for v in val if val.count(v) > 1]
                    assert not dupes, f"{guard.workflow_id}: metadata['{key}'] has duplicates: {set(dupes)}"


# ---------------------------------------------------------------------------
# TestCostTimeCapture — Cost hook lifecycle
# ---------------------------------------------------------------------------


class TestCostTimeCapture:
    """CostEstimateHook must write cost rows on both success and failure paths."""

    @staticmethod
    def _run_with_hook(*, should_raise: bool = False) -> int:
        """Run a trivial workflow through the lifecycle runner with CostEstimateHook.

        Returns the number of ``spark.createDataFrame`` calls (each MERGE = one call).
        Mocks ``delta.tables.DeltaTable`` to avoid requiring the Delta library.
        """
        from unittest.mock import patch

        import workflows.runner as runner
        from ingestion.cost_hook import CostEstimateHook
        from workflows.registry import WorkflowEntry

        # Save and restore hooks
        saved_hooks = list(runner._hooks)
        runner._hooks.clear()

        # Mock delta.tables module so CostEstimateHook._merge can import it
        delta_mock = MagicMock()
        pyspark_types_mock = MagicMock()

        try:
            # Build a CostEstimateHook with mocked internals
            spark_mock = MagicMock()
            spark_mock.createDataFrame.return_value.alias.return_value = MagicMock()

            hook = CostEstimateHook.__new__(CostEstimateHook)
            hook._spark = spark_mock
            hook._table = "cat.observability.workflow_cost_live"
            hook._rate_usd_per_hour = 0.07
            hook._runtime = "test"

            runner.register_hook(hook)

            # Create a trivial workflow entry
            def trivial_fn(*, filter_result: FilterResult, ctx: object = None) -> int:
                if should_raise:
                    msg = "intentional test failure"
                    raise RuntimeError(msg)
                return 42

            entry = WorkflowEntry(
                workflow_id="wf-cost-test",
                phase="test",
                func=trivial_fn,
            )

            fr = FilterResult(workflow_id="wf-cost-test", count=1)

            with patch.dict(
                sys.modules,
                {
                    "delta": delta_mock,
                    "delta.tables": delta_mock.tables,
                    "pyspark.sql.types": pyspark_types_mock,
                },
            ):
                try:
                    runner.run_workflow(entry, filter_result=fr)
                except RuntimeError:
                    pass  # Expected when should_raise=True

            return spark_mock.createDataFrame.call_count

        finally:
            runner._hooks.clear()
            runner._hooks.extend(saved_hooks)

    def test_completed_workflow_produces_cost_row(self) -> None:
        """Successful workflow: on_start + on_complete each call createDataFrame."""
        call_count = self._run_with_hook(should_raise=False)
        assert call_count >= 2, f"Expected >= 2 createDataFrame calls, got {call_count}"

    def test_failed_workflow_produces_cost_row(self) -> None:
        """Failed workflow: on_start + on_error each call createDataFrame."""
        call_count = self._run_with_hook(should_raise=True)
        assert call_count >= 2, f"Expected >= 2 createDataFrame calls, got {call_count}"


# ---------------------------------------------------------------------------
# TestWorkflowIdConsistency — Guard ID matches decorator ID
# ---------------------------------------------------------------------------


class TestWorkflowIdConsistency:
    """Guard workflow_id must match the @workflow decorator's ID on run_pipeline."""

    _EXEMPT: ClassVar[set[str]] = _NO_OWN_PIPELINE  # No own run_pipeline / decorator to compare

    def test_guard_id_matches_decorator_id(self) -> None:
        """guard.workflow_id must appear among the module's @workflow('wf-xxx') decorators.

        Relaxed 2026-04-15 for D62: a module may legitimately own multiple
        ``@workflow``-decorated pipelines (e.g. ``player_embeddings_v2`` owns
        both ``run_pipeline_v2`` with ``wf-football2vec-v2`` AND
        ``run_pipeline_360`` with ``wf-football2vec-360``). The invariant is
        that the module's ``skip_guard.workflow_id`` must correspond to at
        least one of its decorated pipelines — not that all decorators must
        match the guard. This still catches the typo / copy-paste bugs the
        test was designed to prevent (e.g. a guard that says ``wf-foo-v1``
        in a module whose only pipeline is ``@workflow('wf-foo')``), while
        tolerating multi-workflow modules.
        """
        failures: list[str] = []
        for module_path in _GUARD_MODULES:
            if module_path in self._EXEMPT:
                continue

            mod = importlib.import_module(module_path)
            guard = mod.skip_guard
            guard_id = guard.workflow_id

            source_file = inspect.getfile(mod)
            source = Path(source_file).read_text(encoding="utf-8")
            tree = ast.parse(source)

            decorator_ids: set[str] = set()
            for node in ast.iter_child_nodes(tree):
                if not (isinstance(node, ast.FunctionDef) and node.name.startswith("run_pipeline")):
                    continue

                # Extract @workflow("wf-xxx") decorator ID
                for dec in node.decorator_list:
                    if not _ast_has_name_or_attr(dec, "workflow"):
                        continue
                    if isinstance(dec, ast.Call) and dec.args:
                        first_arg = dec.args[0]
                        if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                            decorator_ids.add(first_arg.value)

            if decorator_ids and guard_id not in decorator_ids:
                failures.append(
                    f"{module_path}: guard.workflow_id={guard_id!r} not in module decorators={sorted(decorator_ids)!r}"
                )

        assert not failures, "Guard workflow_id must match @workflow decorator ID:\n" + "\n".join(sorted(failures))


# ---------------------------------------------------------------------------
# TestMandatoryFilterResult — Pipeline signatures (strict)
# ---------------------------------------------------------------------------


class TestMandatoryFilterResult:
    """run_pipeline() must accept filter_result as a REQUIRED parameter.

    The mandatory injection pattern means pipelines cannot run without a
    FilterResult — they receive it from main() which resolves it from
    either the freshness gate (production) or skip_guard.check() (standalone).
    """

    _EXEMPT: ClassVar[set[str]] = {
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


# ---------------------------------------------------------------------------
# TestNoInlineGuardInPipeline — No guard calls in pipeline functions
# ---------------------------------------------------------------------------


class TestNoInlineGuardInPipeline:
    """Pipeline functions must not run inline guards — IDs come from filter_result.

    ``find_new_ids`` and ``find_incomplete_formation_ids`` must only appear
    inside guard classes (``*Guard``) and ``main*`` functions. Any other
    function calling them duplicates the gate's work.
    """

    _EXEMPT: ClassVar[set[str]] = {
        "ingestion.defcon_lite_360",
        "ingestion.defcon_lite_tracking",
    }

    _GUARD_CALL_MARKERS: ClassVar[frozenset[str]] = frozenset(
        {
            "find_new_ids",
            "find_incomplete_formation_ids",
        }
    )

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
                            failures.append(f"{module_path}.{node.name}() calls {marker}()")

        assert not failures, (
            "Pipeline functions must not run inline guards "
            "(use filter_result.metadata instead):\n" + "\n".join(sorted(failures))
        )


# ---------------------------------------------------------------------------
# TestDirectGuardCall — main() calls skip_guard.check() directly (D52)
# ---------------------------------------------------------------------------


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
        """main() must call a ``*_guard`` directly and must NOT call read_gate_result.

        Relaxed 2026-04-15 for D62: a module may legitimately own multiple
        guards (e.g. ``player_embeddings_v2`` owns both ``skip_guard`` and a
        private ``_football2vec_360_guard`` used by ``main_360()``). The
        invariant is that each main() calls SOME ``*_guard`` symbol
        directly — not that it calls the literal name ``skip_guard``. This
        still catches the original intent (no gate indirection, no
        ``read_gate_result`` calls) while tolerating per-workflow guards.
        """
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
                node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name.startswith("main")
            ]

            for main_fn in main_fns:
                has_guard = _ast_main_references_any_guard(main_fn)
                has_gate = _ast_has_name_or_attr(main_fn, "read_gate_result")

                if not has_guard:
                    failures.append(
                        f"{module_path}.{main_fn.name}() does not reference any ``*_guard`` symbol "
                        "(expected a direct call like ``skip_guard.check(...)`` or ``_foo_guard.check(...)``)"
                    )
                if has_gate:
                    failures.append(
                        f"{module_path}.{main_fn.name}() still references read_gate_result (removed in D52)"
                    )

        assert not failures, "main() guard call conformance failures:\n" + "\n".join(failures)


def _ast_has_name_or_attr(node: ast.AST, name: str) -> bool:
    """Check if an AST node tree contains a Name or Attribute reference to *name*."""
    for child in ast.walk(node):
        if isinstance(child, ast.Name) and child.id == name:
            return True
        if isinstance(child, ast.Attribute) and child.attr == name:
            return True
    return False


def _ast_main_references_any_guard(node: ast.AST) -> bool:
    """Check if an AST node tree references any Name/Attribute ending in ``_guard``.

    Used by ``TestDirectGuardCall`` to accept either the canonical
    ``skip_guard`` symbol or any per-workflow guard alias like
    ``_football2vec_360_guard`` (D62 multi-workflow modules).
    """
    for child in ast.walk(node):
        if isinstance(child, ast.Name) and (child.id == "skip_guard" or child.id.endswith("_guard")):
            return True
        if isinstance(child, ast.Attribute) and (child.attr == "skip_guard" or child.attr.endswith("_guard")):
            return True
    return False


# ---------------------------------------------------------------------------
# TestStaticDatasetGuards — Static guard correctness
# ---------------------------------------------------------------------------


class TestStaticDatasetGuards:
    """Static-dataset guards must return count=0 when data is complete."""

    @pytest.mark.parametrize(
        "module_path,tables_and_counts,source_ids_attr",
        [
            ("ingestion.metrica", [("metrica_tracking", 3), ("metrica_events", 3)], None),
            ("ingestion.idsse", [("idsse_tracking", 7), ("idsse_events", 7)], "IDSSE_MATCH_IDS"),
            ("ingestion.skillcorner", [("skillcorner_tracking", 10)], None),
        ],
        ids=["metrica", "idsse", "skillcorner"],
    )
    def test_static_guard_skips_when_complete(
        self,
        module_path: str,
        tables_and_counts: list[tuple[str, int]],
        source_ids_attr: str | None,
    ) -> None:
        """Mock tables with expected distinct counts, verify count=0.

        ``source_ids_attr`` (e.g. ``"IDSSE_MATCH_IDS"``) is provided for
        guards that use ``.collect()`` for runtime chunk discovery rather
        than ``.count()``. When set, the mock additionally configures
        ``.collect()`` to return rows whose ``match_id`` matches the
        source-of-truth IDs so the guard's anti-join finds zero missing.
        """
        mod = importlib.import_module(module_path)
        guard = mod.skip_guard
        spark = MagicMock()

        # For guards that use .collect() for runtime chunk discovery, build
        # mock rows whose ["match_id"] returns one of the source-of-truth IDs.
        source_ids: list[str] = []
        if source_ids_attr is not None:
            source_ids = list(getattr(mod, source_ids_attr))

        def _make_rows(ids: list[str]) -> list[MagicMock]:
            rows: list[MagicMock] = []
            for mid in ids:
                row = MagicMock()
                row.__getitem__ = lambda self, key, _mid=mid: _mid
                rows.append(row)
            return rows

        def table_side_effect(name: str) -> MagicMock:
            mock_df = MagicMock()
            for table_name, expected_count in tables_and_counts:
                if table_name in name:
                    mock_df.select.return_value.distinct.return_value.count.return_value = expected_count
                    if source_ids:
                        mock_df.select.return_value.distinct.return_value.collect.return_value = _make_rows(source_ids)
                    return mock_df
            # Default: return 0 / empty for unknown tables
            mock_df.select.return_value.distinct.return_value.count.return_value = 0
            mock_df.select.return_value.distinct.return_value.collect.return_value = []
            return mock_df

        spark.table.side_effect = table_side_effect
        result = guard.check(spark, "soccer_analytics", "dev_gold")
        assert result.count == 0, f"{guard.workflow_id}: expected skip but got count={result.count}"

    @pytest.mark.parametrize(
        "module_path",
        ["ingestion.metrica", "ingestion.idsse", "ingestion.skillcorner"],
        ids=["metrica", "idsse", "skillcorner"],
    )
    def test_static_guard_runs_when_incomplete(self, module_path: str) -> None:
        """Mock tables with Exception (table missing), verify count > 0."""
        mod = importlib.import_module(module_path)
        guard = mod.skip_guard
        spark = MagicMock()
        spark.table.side_effect = Exception("[TABLE_OR_VIEW_NOT_FOUND] Table not found")
        result = guard.check(spark, "soccer_analytics", "dev_gold")
        assert result.count > 0, f"{guard.workflow_id}: expected work but got count=0"

    def test_wyscout_guard_skips_when_complete(self) -> None:
        """Wyscout guard: 4 tables (events, matches by competition_name; players + teams by existence).

        PR 5a (ADR-011) extended the guard to require bronze.wyscout_teams
        alongside the pre-existing 3 tables — teams.json ingestion closed a
        pre-existing gap identified at the spec phase. This test's mock was
        extended in lockstep so the skip-complete scenario reflects the new
        bronze reality (4-table completeness, not 3).
        """
        from ingestion.wyscout import skip_guard

        spark = MagicMock()

        def table_side_effect(name: str) -> MagicMock:
            mock_df = MagicMock()
            if "wyscout_events" in name:
                mock_df.select.return_value.distinct.return_value.count.return_value = 7
            elif "wyscout_matches" in name:
                mock_df.select.return_value.distinct.return_value.count.return_value = 7
            elif "wyscout_players" in name:
                mock_df.limit.return_value.count.return_value = 1
            elif "wyscout_teams" in name:
                mock_df.limit.return_value.count.return_value = 1
            else:
                mock_df.select.return_value.distinct.return_value.count.return_value = 0
            return mock_df

        spark.table.side_effect = table_side_effect
        result = skip_guard.check(spark, "soccer_analytics", "dev_gold")
        assert result.count == 0, f"wf-wyscout: expected skip but got count={result.count}"

    def test_wyscout_guard_runs_when_incomplete(self) -> None:
        """Wyscout guard returns count>0 when tables are missing."""
        from ingestion.wyscout import skip_guard

        spark = MagicMock()
        spark.table.side_effect = Exception("[TABLE_OR_VIEW_NOT_FOUND] Table not found")
        result = skip_guard.check(spark, "soccer_analytics", "dev_gold")
        assert result.count > 0, "wf-wyscout: expected work but got count=0"


# ---------------------------------------------------------------------------
# TestEarlyExitStructure — AST-based early exit verification
# ---------------------------------------------------------------------------


class TestEarlyExitStructure:
    """run_pipeline() must reference WorkflowSkippedError for count==0 early exit."""

    _EXEMPT: ClassVar[set[str]] = {
        "ingestion.defcon_lite_360",
        "ingestion.defcon_lite_tracking",
    }

    def test_run_pipeline_references_workflow_skipped_error(self) -> None:
        """Every run_pipeline must raise WorkflowSkippedError on count==0."""
        failures: list[str] = []
        for module_path in _GUARD_MODULES:
            if module_path in self._EXEMPT:
                continue

            mod = importlib.import_module(module_path)
            source_file = inspect.getfile(mod)
            source = Path(source_file).read_text(encoding="utf-8")
            tree = ast.parse(source)

            for node in ast.iter_child_nodes(tree):
                if isinstance(node, ast.FunctionDef) and node.name.startswith("run_pipeline"):
                    if not _ast_has_name_or_attr(node, "WorkflowSkippedError"):
                        failures.append(f"{module_path}.{node.name}()")

        assert not failures, "run_pipeline() must raise WorkflowSkippedError when count==0:\n" + "\n".join(
            sorted(failures)
        )


# ---------------------------------------------------------------------------
# TestEarlyExitBehavior — Behavioral early exit verification
# ---------------------------------------------------------------------------


class TestEarlyExitBehavior:
    """run_pipeline() must actually raise WorkflowSkippedError when count==0."""

    _EXEMPT: ClassVar[set[str]] = {
        "ingestion.defcon_lite_360",
        "ingestion.defcon_lite_tracking",
    }

    # Special cases: pipelines with non-standard signatures
    _SPECIAL_CALL_ARGS: ClassVar[dict[str, dict[str, object]]] = {
        "ingestion.defcon_lite": {
            "filter_360": None,  # will be set per-test
            "filter_tracking": None,
        },
        "ingestion.prepare_360_training_data": {
            "volume_path": "/tmp/test",  # noqa: S108
        },
    }

    def test_raises_on_zero_count(self) -> None:
        """Calling run_pipeline with count=0 must raise WorkflowSkippedError."""
        from workflows.exceptions import WorkflowSkippedError

        _mock_pyspark_functions()
        failures: list[str] = []

        for module_path in _GUARD_MODULES:
            if module_path in self._EXEMPT:
                continue

            mod = importlib.import_module(module_path)

            # Find the run_pipeline function
            pipeline_fn = None
            for name, obj in inspect.getmembers(mod, inspect.isfunction):
                if name.startswith("run_pipeline"):
                    pipeline_fn = obj
                    break

            if pipeline_fn is None:
                continue

            # Unwrap the @workflow decorator
            fn = getattr(pipeline_fn, "__wrapped__", pipeline_fn)

            spark = MagicMock()
            logger = MagicMock()

            # Build kwargs
            sig = inspect.signature(fn)
            kwargs: dict[str, object] = {}

            # Handle special cases
            special = self._SPECIAL_CALL_ARGS.get(module_path, {})
            for key, val in special.items():
                kwargs[key] = val

            # Set filter_result (or special filter params) to count=0
            if "filter_result" in sig.parameters:
                kwargs["filter_result"] = FilterResult(workflow_id="wf-test", count=0)
            if "filter_360" in sig.parameters:
                kwargs["filter_360"] = FilterResult(workflow_id="wf-test-360", count=0)
            if "filter_tracking" in sig.parameters:
                kwargs["filter_tracking"] = FilterResult(workflow_id="wf-test-tracking", count=0)

            # Build positional args from signature
            positional: list[object] = []
            for pname, param in sig.parameters.items():
                if pname in kwargs:
                    continue
                if param.kind in (
                    inspect.Parameter.KEYWORD_ONLY,
                    inspect.Parameter.VAR_KEYWORD,
                ):
                    continue
                if pname == "spark":
                    positional.append(spark)
                elif pname == "logger":
                    positional.append(logger)
                elif pname in ("catalog", "schema"):
                    positional.append("test")
                elif pname == "ctx":
                    continue
                else:
                    positional.append(special.get(pname, MagicMock()))

            try:
                fn(*positional, **kwargs)
                # If it didn't raise, that's a failure
                failures.append(f"{module_path}.{fn.__name__}() did not raise WorkflowSkippedError on count=0")
            except WorkflowSkippedError:
                pass  # Expected — test passes
            except Exception as exc:
                # Some other exception — might be OK if it's before any work
                # But we specifically want WorkflowSkippedError
                failures.append(
                    f"{module_path}.{fn.__name__}() raised {type(exc).__name__} instead of WorkflowSkippedError: {exc}"
                )

        assert not failures, "run_pipeline() must raise WorkflowSkippedError on count=0:\n" + "\n".join(
            sorted(failures)
        )
