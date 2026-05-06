"""Tests for the combined HF sync task."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ingestion.guards import FilterResult


class TestHfSync:
    """Combined HF task calls all sub-operations."""

    @patch("ingestion.hf_sync._run_sub_workflow")
    def test_calls_all_sub_operations(self, mock_run: MagicMock) -> None:
        from ingestion.hf_sync import run_pipeline

        spark = MagicMock()
        logger_mock = MagicMock()
        filter_result = FilterResult(workflow_id="wf-hf-sync", count=1)

        run_pipeline(spark, "cat", "schema", logger_mock, filter_result=filter_result)

        # PR-Cycle-B (2026-05-01): import_obso_results split out — was 7, now 6.
        # PR-2 (2026-05-05): +4 sub-ops (scoutgpt export, 3 Group 0 publishers) — now 10.
        assert mock_run.call_count == 10

    def test_run_sub_workflow_swallows_failure(self) -> None:
        """_run_sub_workflow logs failure at ERROR and continues (doesn't raise).

        Error level (not warning) so failures surface in error-log queries —
        changed 2026-04-15 after the warm-tier blocker showed warning-level
        logs are invisible.
        """
        from ingestion.hf_sync import _run_sub_workflow

        spark = MagicMock()
        logger_mock = MagicMock()
        failing_op = MagicMock(side_effect=RuntimeError("boom"))

        _run_sub_workflow("test-op", failing_op, spark, "cat", "schema", logger_mock)
        logger_mock.error.assert_called_once()
        logger_mock.warning.assert_not_called()

    def test_run_sub_workflow_calls_op(self) -> None:
        """_run_sub_workflow passes correct args to the callable."""
        from ingestion.hf_sync import _run_sub_workflow

        spark = MagicMock()
        logger_mock = MagicMock()
        mock_op = MagicMock()

        _run_sub_workflow("test-op", mock_op, spark, "cat", "schema", logger_mock)
        mock_op.assert_called_once_with(spark, "cat", "schema", logger_mock)

    def test_sub_operations_count(self) -> None:
        """Verify all 10 sub-operations are registered.

        PR-Cycle-B (2026-05-01): split import_obso_results — was 7, now 6.
        PR-2 (2026-05-05): +4 (scoutgpt export, 3 Group 0 publishers) — now 10.
        """
        from ingestion.hf_sync import _SUB_OPERATIONS

        assert len(_SUB_OPERATIONS) == 10

    def test_sub_operations_all_callable(self) -> None:
        """Every sub-operation has a callable."""
        from ingestion.hf_sync import _SUB_OPERATIONS

        for label, op in _SUB_OPERATIONS:
            assert callable(op), f"{label} op is not callable"
