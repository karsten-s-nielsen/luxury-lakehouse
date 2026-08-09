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
        # PR-2 (2026-05-05): +4 sub-ops (scoutgpt export, 3 Group 0 publishers) — was 10.
        # 2026-07 space_creation retirement: -1 (import_space_creation removed) — was 9.
        # ADR-074 (2026-08-08): import_psxg_predictions + publish_spadl_vaep_hf split
        # into their own tasks (dbt decoupling + driver isolation) — now 7.
        assert mock_run.call_count == 7

    def test_run_sub_workflow_swallows_failure(self) -> None:
        """_run_sub_workflow logs failure at ERROR and continues (doesn't raise).

        Error level (not warning) so failures surface in error-log queries —
        changed 2026-04-15 after the warm-tier blocker showed warning-level
        logs are invisible.

        Returning False is the ADR-073 half: continuing is not passing, and the
        caller uses this to fail the task.
        """
        from ingestion.hf_sync import _run_sub_workflow

        spark = MagicMock()
        logger_mock = MagicMock()
        failing_op = MagicMock(side_effect=RuntimeError("boom"))

        ok = _run_sub_workflow("test-op", failing_op, spark, "cat", "schema", logger_mock)
        assert ok is False
        logger_mock.error.assert_called_once()
        logger_mock.warning.assert_not_called()

    def test_run_sub_workflow_calls_op(self) -> None:
        """_run_sub_workflow passes correct args to the callable."""
        from ingestion.hf_sync import _run_sub_workflow

        spark = MagicMock()
        logger_mock = MagicMock()
        mock_op = MagicMock()

        ok = _run_sub_workflow("test-op", mock_op, spark, "cat", "schema", logger_mock)
        assert ok is True
        mock_op.assert_called_once_with(spark, "cat", "schema", logger_mock)

    def test_run_pipeline_fails_the_task_when_a_sub_workflow_fails(self) -> None:
        """ADR-073 — swallowing a sub-workflow failure must NOT report SUCCESS.

        This is the ADR-067 rule ("a worker that swallows a unit failure must
        still FAIL ITS TASK") applied to hf_sync, where it had never been
        applied. On 2026-08-07 five of nine sub-workflows failed and the task
        reported SUCCESS — the reason six defects stayed invisible for months.
        """
        import pytest

        from ingestion.hf_sync import run_pipeline

        spark = MagicMock()
        logger_mock = MagicMock()
        filter_result = FilterResult(workflow_id="wf-hf-sync", count=1)

        # One op fails, the rest succeed.
        outcomes = [True] * 9
        outcomes[3] = False
        with patch("ingestion.hf_sync._run_sub_workflow", side_effect=outcomes):
            with pytest.raises(RuntimeError, match="FAILED sub-workflow"):
                run_pipeline(spark, "cat", "schema", logger_mock, filter_result=filter_result)

    def test_run_pipeline_still_runs_every_sub_workflow_before_failing(self) -> None:
        """The per-op catch STAYS: one bad publisher must not stop the others."""
        import pytest

        from ingestion.hf_sync import _SUB_OPERATIONS, run_pipeline

        spark = MagicMock()
        logger_mock = MagicMock()
        filter_result = FilterResult(workflow_id="wf-hf-sync", count=1)

        # The FIRST op fails; every later op must still be attempted.
        outcomes = [False] + [True] * (len(_SUB_OPERATIONS) - 1)
        with patch("ingestion.hf_sync._run_sub_workflow", side_effect=outcomes) as mock_run:
            with pytest.raises(RuntimeError):
                run_pipeline(spark, "cat", "schema", logger_mock, filter_result=filter_result)
        assert mock_run.call_count == len(_SUB_OPERATIONS)

    def test_raise_on_failed_sub_workflows_is_silent_when_all_pass(self) -> None:
        """A clean run must not raise — the gate has to stay quiet to stay trusted."""
        from ingestion.hf_sync import raise_on_failed_sub_workflows

        raise_on_failed_sub_workflows([], attempted=9)

    def test_raise_on_failed_sub_workflows_names_every_failure(self) -> None:
        """The operator needs the names — a bare count sends them back to the logs."""
        import pytest

        from ingestion.hf_sync import raise_on_failed_sub_workflows

        with pytest.raises(RuntimeError) as exc:
            raise_on_failed_sub_workflows(["ingestion.a", "ingestion.b"], attempted=9)
        msg = str(exc.value)
        assert "ingestion.a" in msg
        assert "ingestion.b" in msg
        assert "2 of 9" in msg

    def test_sub_operations_count(self) -> None:
        """Verify all 8 sub-operations are registered.

        PR-Cycle-B (2026-05-01): split import_obso_results — was 7, now 6.
        PR-2 (2026-05-05): +4 (scoutgpt export, 3 Group 0 publishers) — was 10.
        2026-07 space_creation retirement: -1 (import_space_creation removed) — was 9.
        ADR-074 (2026-08-08): import_psxg_predictions + publish_spadl_vaep_hf promoted
        to their own tasks — now 7.
        """
        from ingestion.hf_sync import _SUB_OPERATIONS

        assert len(_SUB_OPERATIONS) == 7

    def test_import_psxg_is_no_longer_an_hf_sync_sub_operation(self) -> None:
        """SEC7 / ADR-074 — the ONLY leg dbt needs must not sit behind eight publishers.

        Since ADR-073 hf_sync FAILS its task on any sub-op failure, leaving this here
        meant an HF Hub outage could block dbt_build_output_marts and its two
        dependents (refresh_synced_tables, run_model_validation).
        """
        from ingestion.hf_sync import _SUB_OPERATIONS, _VOLUME_PATHS

        assert "ingestion.import_psxg_predictions" not in [label for label, _ in _SUB_OPERATIONS]
        assert "ingestion.import_psxg_predictions" not in _VOLUME_PATHS

    def test_hf_sync_is_export_only(self) -> None:
        """No sub-operation may write a bronze table dbt reads — that coupling IS SEC7.

        import_obso_results was split for this reason in PR-Cycle-B (2026-05-01) and
        import_psxg_predictions in ADR-074. This asserts the class stays closed rather
        than re-opening the next time an importer looks convenient to co-locate.
        """
        from ingestion.hf_sync import _SUB_OPERATIONS

        assert not [x for x, _ in _SUB_OPERATIONS if "import_" in x]

    def test_publish_spadl_vaep_is_no_longer_an_hf_sync_sub_operation(self) -> None:
        """ADR-074 — measured at 6.97 GB peak alone; it must not share a driver.

        hf_sync runs its sub-operations in ONE process, so memory is cumulative. This
        publisher pulls 9.76M rows via toPandas; alone it uses under half a 16 GB
        driver, but the shared process was OOM-killed (exit 137) on 2026-08-07.
        """
        from ingestion.hf_sync import _SUB_OPERATIONS

        assert "ingestion.publish_spadl_vaep_hf" not in [label for label, _ in _SUB_OPERATIONS]

    def test_sub_operations_all_callable(self) -> None:
        """Every sub-operation has a callable."""
        from ingestion.hf_sync import _SUB_OPERATIONS

        for label, op in _SUB_OPERATIONS:
            assert callable(op), f"{label} op is not callable"
