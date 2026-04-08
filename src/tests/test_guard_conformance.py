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
    "wf-skillcorner",  # Static dataset, count-based guard
    "wf-wyscout",  # Static dataset, count-based guard
    "wf-import-obso",  # HF Hub import, always-run
    "wf-import-psxg",  # HF Hub import, always-run
    "wf-import-space-creation",  # HF Hub import, always-run
    "wf-model-validation",  # Monitoring, always-run
    "wf-sync-hf-costs",  # Polling sync, always-run
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
    "ingestion.statsbomb",  # Always-run, internal skip logic
    "ingestion.metrica",  # Static dataset, guard is count-based
    "ingestion.idsse",  # Static dataset, guard is count-based
    "ingestion.skillcorner",  # Static dataset, guard is count-based
    "ingestion.wyscout",  # Static dataset, guard is count-based
    "ingestion.import_obso_results",  # HF Hub import, always-run
    "ingestion.import_psxg_predictions",  # HF Hub import, always-run
    "ingestion.import_space_creation",  # HF Hub import, always-run
    "ingestion.tracking_metadata",  # Simple existence check
    "ingestion.model_validation",  # Monitoring, always-run
    "ingestion.sync_hf_costs",  # Polling sync, always-run
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
# TestPipelineAcceptsFilterResult — Pipeline signatures
# ---------------------------------------------------------------------------


class TestPipelineAcceptsFilterResult:
    """Every pipeline's run_pipeline() must accept filter_result kwarg."""

    # Modules where the pipeline function uses non-standard filter params
    _SPECIAL_CASES: ClassVar[dict[str, set[str]]] = {
        "ingestion.defcon_lite": {"filter_360", "filter_tracking"},
    }

    # Modules whose run_pipeline legitimately omits filter_result
    # (always-run stubs, static-dataset ingestors, or HF imports).
    _FILTER_RESULT_EXEMPT: ClassVar[set[str]] = {
        "ingestion.statsbomb",
        "ingestion.metrica",
        "ingestion.idsse",
        "ingestion.skillcorner",
        "ingestion.wyscout",
        "ingestion.import_obso_results",
        "ingestion.import_psxg_predictions",
        "ingestion.import_space_creation",
        "ingestion.tracking_metadata",
        "ingestion.model_validation",
        "ingestion.sync_hf_costs",
        "ingestion.defcon_lite_360",  # Sub-module, no run_pipeline
        "ingestion.defcon_lite_tracking",  # Sub-module, no run_pipeline
    }

    def test_run_pipeline_has_filter_result_param(self) -> None:
        """Inspect the signature of run_pipeline functions for filter_result param."""
        for module_path in _GUARD_MODULES:
            if module_path in self._FILTER_RESULT_EXEMPT:
                continue

            mod = importlib.import_module(module_path)

            # Find the run_pipeline function (may be named run_pipeline_xxx)
            pipeline_fn = None
            for name, obj in inspect.getmembers(mod, inspect.isfunction):
                if name.startswith("run_pipeline"):
                    pipeline_fn = obj
                    break

            if pipeline_fn is None:
                continue  # Sub-modules without their own run_pipeline

            sig = inspect.signature(pipeline_fn)
            param_names = set(sig.parameters.keys())

            # Check for standard filter_result or special-case params
            special = self._SPECIAL_CASES.get(module_path)
            if special:
                for expected_param in special:
                    assert expected_param in param_names, (
                        f"{module_path}.{pipeline_fn.__name__}() missing '{expected_param}' param"
                    )
            else:
                assert "filter_result" in param_names, (
                    f"{module_path}.{pipeline_fn.__name__}() missing 'filter_result' param"
                )


# ---------------------------------------------------------------------------
# TestMainCallsReadGateResult — AST inspection
# ---------------------------------------------------------------------------


class TestMainCallsReadGateResult:
    """Every main() function must call read_gate_result() (unless exempt)."""

    def test_main_calls_read_gate_result(self) -> None:
        """AST-inspect each main() to verify read_gate_result is called."""
        for module_path in _GUARD_MODULES:
            if module_path in _READ_GATE_EXEMPT:
                continue

            mod = importlib.import_module(module_path)

            # Skip modules without main()
            if not hasattr(mod, "main"):
                continue

            # Get the source file
            source_file = inspect.getfile(mod)
            source = Path(source_file).read_text(encoding="utf-8")
            tree = ast.parse(source)

            # Find all main* functions
            main_fns = [
                node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name.startswith("main")
            ]

            for main_fn in main_fns:
                has_read_gate = _ast_has_name_or_attr(main_fn, "read_gate_result")
                assert has_read_gate, f"{module_path}.{main_fn.name}() does not call read_gate_result()"


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
