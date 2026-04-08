"""Tests for the freshness gate orchestrator."""

from __future__ import annotations

from unittest.mock import MagicMock

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
