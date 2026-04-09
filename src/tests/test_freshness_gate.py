"""Tests for the freshness gate orchestrator."""

from __future__ import annotations

import json
import sys
from unittest.mock import MagicMock, patch

from ingestion.freshness_gate import _write_task_values, run_gate
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
            spark,
            "cat",
            "schema",
            guards={"wf-bad": guard_bad, "wf-ok": guard_ok},
        )

        assert results["wf-bad"].count == 0
        assert results["wf-ok"].count == 3

    def test_empty_guards(self) -> None:
        """Gate handles empty guard registry gracefully."""
        spark = MagicMock()
        results = run_gate(spark, "cat", "schema", guards={})
        assert results == {}

    def test_all_guards_skip(self) -> None:
        """When all guards return count=0, all are in results."""
        guard_a = MagicMock()
        guard_a.workflow_id = "wf-a"
        guard_a.check.return_value = FilterResult(workflow_id="wf-a", count=0)

        guard_b = MagicMock()
        guard_b.workflow_id = "wf-b"
        guard_b.check.return_value = FilterResult(workflow_id="wf-b", count=0)

        spark = MagicMock()
        results = run_gate(spark, "cat", "schema", guards={"wf-a": guard_a, "wf-b": guard_b})

        assert all(r.count == 0 for r in results.values())
        assert len(results) == 2

    def test_parallel_execution_faster_than_sequential(self) -> None:
        """4 guards each sleeping 0.1s should complete in <0.3s (parallel)."""
        import time as _time

        def _slow_check(_spark: object, _catalog: str, _schema: str) -> FilterResult:
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
        # With max_workers=4, all 4 guards run in parallel (~0.1s total)
        # Allow 0.3s for CI/thread overhead
        assert elapsed < 0.3, f"Expected <0.3s (parallel), got {elapsed:.2f}s"

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


# ---------------------------------------------------------------------------
# TestFreshnessGateTaskValuePropagation — Task value write fidelity
# ---------------------------------------------------------------------------


def _patch_pyspark_for_task_values() -> tuple[MagicMock, MagicMock, MagicMock]:
    """Set up pyspark mocks so ``_write_task_values`` can import DBUtils.

    Returns (dbutils_mock, dbutils_module, sql_module) for assertion.
    """
    spark_mock = MagicMock()
    dbutils_mock = MagicMock()

    # Mock module for pyspark.dbutils
    dbutils_module = MagicMock()
    dbutils_module.DBUtils.return_value = dbutils_mock

    # Mock module for pyspark.sql — SparkSession.getActiveSession returns our mock
    sql_module = MagicMock()
    sql_module.SparkSession.getActiveSession.return_value = spark_mock

    return dbutils_mock, dbutils_module, sql_module


class TestFreshnessGateTaskValuePropagation:
    """_write_task_values faithfully writes FilterResult data as Databricks task values."""

    def _call_write(self, result: FilterResult, wf_id: str = "wf-test") -> MagicMock:
        """Helper: mock pyspark, call _write_task_values, return dbutils mock."""
        dbutils_mock, dbutils_module, sql_module = _patch_pyspark_for_task_values()

        with patch.dict(
            sys.modules,
            {
                "pyspark": MagicMock(),
                "pyspark.dbutils": dbutils_module,
                "pyspark.sql": sql_module,
            },
        ):
            _write_task_values({wf_id: result})

        return dbutils_mock

    def test_write_task_values_two_keys_per_workflow(self) -> None:
        """Each workflow produces exactly two task-value writes: JSON + count."""
        result = FilterResult(
            workflow_id="wf-test",
            count=5,
            metadata={"new_match_ids": ["m1", "m2", "m3", "m4", "m5"]},
        )
        dbutils = self._call_write(result, "wf-test")

        calls = dbutils.jobs.taskValues.set.call_args_list
        assert len(calls) == 2, f"Expected 2 set() calls, got {len(calls)}"

        keys = {c.kwargs["key"] for c in calls}
        assert "wf-test" in keys
        assert "wf-test-count" in keys

    def test_json_payload_preserves_metadata(self) -> None:
        """JSON task value round-trips metadata faithfully."""
        metadata = {"new_match_ids": ["m1", "m2", "m3"]}
        result = FilterResult(workflow_id="wf-test", count=3, metadata=metadata)
        dbutils = self._call_write(result, "wf-test")

        # Find the JSON call (key without -count suffix)
        for call in dbutils.jobs.taskValues.set.call_args_list:
            if call.kwargs["key"] == "wf-test":
                payload = json.loads(call.kwargs["value"])
                assert payload["metadata"] == metadata
                return

        raise AssertionError("JSON task value not found")

    def test_json_payload_preserves_chunks(self) -> None:
        """JSON task value round-trips chunks faithfully."""
        chunks = [["m1", "m2"], ["m3"]]
        result = FilterResult(workflow_id="wf-test", count=3, chunks=chunks)
        dbutils = self._call_write(result, "wf-test")

        for call in dbutils.jobs.taskValues.set.call_args_list:
            if call.kwargs["key"] == "wf-test":
                payload = json.loads(call.kwargs["value"])
                assert payload["chunks"] == chunks
                return

        raise AssertionError("JSON task value not found")

    def test_count_value_is_integer(self) -> None:
        """The -count task value must be an int, not a string."""
        result = FilterResult(workflow_id="wf-test", count=7)
        dbutils = self._call_write(result, "wf-test")

        for call in dbutils.jobs.taskValues.set.call_args_list:
            if call.kwargs["key"] == "wf-test-count":
                value = call.kwargs["value"]
                assert isinstance(value, int), f"Expected int, got {type(value).__name__}"
                assert value == 7
                return

        raise AssertionError("-count task value not found")

    def test_guard_exception_yields_count_zero_still_written(self) -> None:
        """When a guard raises, run_gate catches it and count=0 is still written."""
        guard_bad = MagicMock()
        guard_bad.workflow_id = "wf-bad"
        guard_bad.check.side_effect = RuntimeError("boom")

        spark = MagicMock()
        results = run_gate(spark, "cat", "schema", guards={"wf-bad": guard_bad})

        # Gate should produce count=0 for the failed guard
        assert results["wf-bad"].count == 0

        # Now write it and verify
        dbutils = self._call_write(results["wf-bad"], "wf-bad")
        for call in dbutils.jobs.taskValues.set.call_args_list:
            if call.kwargs["key"] == "wf-bad-count":
                assert call.kwargs["value"] == 0
                return

        raise AssertionError("-count task value not found for failed guard")

    def test_read_gate_result_round_trip(self) -> None:
        """FilterResult.to_json() -> FilterResult.from_json() preserves all fields."""
        original = FilterResult(
            workflow_id="wf-round-trip",
            count=3,
            chunks=[["a", "b"], ["c"]],
            metadata={"new_match_ids": ["a", "b", "c"]},
        )
        serialized = original.to_json()
        restored = FilterResult.from_json(serialized)

        assert restored.workflow_id == original.workflow_id
        assert restored.count == original.count
        assert restored.chunks == original.chunks
        assert restored.metadata == original.metadata

    def test_standalone_mode_no_crash(self) -> None:
        """When pyspark is not importable, _write_task_values does not raise."""
        # Remove pyspark from sys.modules to simulate standalone
        saved = {}
        pyspark_keys = [k for k in sys.modules if k.startswith("pyspark")]
        for k in pyspark_keys:
            saved[k] = sys.modules.pop(k)

        try:
            with patch.dict(
                sys.modules,
                {
                    "pyspark": None,  # type: ignore[dict-item]
                    "pyspark.dbutils": None,  # type: ignore[dict-item]
                    "pyspark.sql": None,  # type: ignore[dict-item]
                },
            ):
                result = FilterResult(workflow_id="wf-test", count=1)
                # Should not raise — falls into except branch
                _write_task_values({"wf-test": result})
        finally:
            # Restore pyspark modules
            for k, v in saved.items():
                sys.modules[k] = v
