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
    "wf-backfill-extra",  # Existence check, no ID metadata
    "wf-backfill-360",  # Set-difference guard, no ID metadata
    "wf-metrica",  # Static dataset, count-based guard
    "wf-idsse",  # Static dataset, count-based guard
    "wf-idsse-events",  # Static dataset, count-based guard
    "wf-skillcorner",  # Static dataset, count-based guard
    "wf-wyscout",  # Static dataset, count-based guard
    "wf-import-obso",  # HF Hub import, always-run
    "wf-import-psxg",  # HF Hub import, always-run
    "wf-import-space-creation",  # HF Hub import, always-run
    "wf-model-validation",  # Monitoring, always-run
    "wf-sync-hf-costs",  # Polling sync, always-run
    "wf-hf-sync",  # Orchestrator, always-run stub
    "wf-football2vec-v2",  # HF Hub import, always-run stub
    "wf-football2vec-v2-export",  # Count-comparison guard
    "wf-prepare-360-data",  # Count-comparison guard
    "wf-entity-resolution",  # Binary existence check
    "wf-tracking-metadata",  # Simple existence check
}

# Guard modules whose pipeline doesn't have its own run_pipeline —
# the run_pipeline lives in an orchestrator (e.g., defcon_lite_360 → defcon_lite).
_NO_OWN_PIPELINE = {
    "ingestion.defcon_lite_360",
    "ingestion.defcon_lite_tracking",
}

# Modules whose main() legitimately omits read_gate_result because
# they are always-run stubs, static-dataset ingestors, or have
# special orchestration that doesn't use the freshness gate.
_READ_GATE_EXEMPT = {
    "ingestion.defcon_lite_360",  # No own main(), orchestrated by defcon_lite
    "ingestion.defcon_lite_tracking",  # No own main(), orchestrated by defcon_lite
}


# ---------------------------------------------------------------------------
# Spark mock helpers
# ---------------------------------------------------------------------------


def _mock_pyspark_functions() -> MagicMock:
    """Register a mock pyspark.sql.functions module in sys.modules.

    Must be called before importing any guard module that uses
    ``from pyspark.sql import functions as F``.
    """
    mock_functions = MagicMock()
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
        {"match_id": "m1", "competition_name": "test", "competition_id": "c1"},
        {"match_id": "m2", "competition_name": "test2", "competition_id": "c2"},
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
# TestMainStandaloneResolution — Gate + fallback in main()
# ---------------------------------------------------------------------------


class TestMainStandaloneResolution:
    """main() must resolve guard result: gate first, skip_guard.check() fallback.

    In production, main() reads FilterResult from Databricks task values
    via read_gate_result(). In standalone mode (no task values), it must
    call skip_guard.check() to compute the result locally. Both paths
    must be present.
    """

    _EXEMPT: ClassVar[set[str]] = {
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
                node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name.startswith("main")
            ]

            for main_fn in main_fns:
                has_gate = _ast_has_name_or_attr(main_fn, "read_gate_result")
                has_fallback = _ast_has_name_or_attr(main_fn, "skip_guard")

                assert has_gate, f"{module_path}.{main_fn.name}() does not call read_gate_result()"
                assert has_fallback, (
                    f"{module_path}.{main_fn.name}() does not call skip_guard.check() as standalone fallback"
                )


def _ast_has_name_or_attr(node: ast.AST, name: str) -> bool:
    """Check if an AST node tree contains a Name or Attribute reference to *name*."""
    for child in ast.walk(node):
        if isinstance(child, ast.Name) and child.id == name:
            return True
        if isinstance(child, ast.Attribute) and child.attr == name:
            return True
    return False


# ---------------------------------------------------------------------------
# TestStaticDatasetGuards — Static guard correctness
# ---------------------------------------------------------------------------


class TestStaticDatasetGuards:
    """Static-dataset guards must return count=0 when data is complete."""

    @pytest.mark.parametrize(
        "module_path,tables_and_counts",
        [
            ("ingestion.metrica", [("metrica_tracking", 3), ("metrica_events", 3)]),
            ("ingestion.idsse", [("idsse_tracking", 7), ("idsse_events", 7)]),
            ("ingestion.skillcorner", [("skillcorner_tracking", 10)]),
        ],
        ids=["metrica", "idsse", "skillcorner"],
    )
    def test_static_guard_skips_when_complete(
        self,
        module_path: str,
        tables_and_counts: list[tuple[str, int]],
    ) -> None:
        """Mock tables with expected distinct counts, verify count=0."""
        mod = importlib.import_module(module_path)
        guard = mod.skip_guard
        spark = MagicMock()

        def table_side_effect(name: str) -> MagicMock:
            mock_df = MagicMock()
            for table_name, expected_count in tables_and_counts:
                if table_name in name:
                    mock_df.select.return_value.distinct.return_value.count.return_value = expected_count
                    return mock_df
            # Default: return 0 for unknown tables
            mock_df.select.return_value.distinct.return_value.count.return_value = 0
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
        spark.table.side_effect = Exception("Table not found")
        result = guard.check(spark, "soccer_analytics", "dev_gold")
        assert result.count > 0, f"{guard.workflow_id}: expected work but got count=0"

    def test_wyscout_guard_skips_when_complete(self) -> None:
        """Wyscout guard: 3 tables (events, matches by competition_name; players by existence)."""
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
        spark.table.side_effect = Exception("Table not found")
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
