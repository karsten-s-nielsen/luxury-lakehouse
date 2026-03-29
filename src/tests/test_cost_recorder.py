"""Tests for HFJobsCostRecorder — cost tracking for HF Jobs workflows."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from analytics.cost import (
    HF_RATE_A10G_LARGE,
    HF_RATE_A10G_SMALL,
    HF_RATE_CPU_BASIC,
    HFJobsCostRecorder,
)

# ---------------------------------------------------------------------------
# Rate constant sanity checks
# ---------------------------------------------------------------------------


class TestRateConstants:
    def test_cpu_basic_rate(self) -> None:
        assert HF_RATE_CPU_BASIC == 0.01

    def test_a10g_small_rate(self) -> None:
        assert HF_RATE_A10G_SMALL == 1.00

    def test_a10g_large_rate(self) -> None:
        assert HF_RATE_A10G_LARGE == 1.50


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_valid_workflow_id(self) -> None:
        recorder = HFJobsCostRecorder(
            workflow_id="wf-compute_xg",
            phase="compute",
            rate_usd_per_hour=0.01,
            repo_id="luxury-lakehouse/xg-shot-data",
        )
        assert recorder.workflow_id == "wf-compute_xg"

    def test_invalid_workflow_id_raises(self) -> None:
        with pytest.raises(ValueError, match="workflow_id"):
            HFJobsCostRecorder(
                workflow_id="bad-id",
                phase="compute",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/xg-shot-data",
            )

    def test_hf_job_id_from_environment(self) -> None:
        with patch.dict("os.environ", {"HF_JOB_ID": "job-abc-123"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="compute",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test",
            )
            assert recorder.hf_job_id == "job-abc-123"

    def test_hf_job_id_none_when_unset(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="compute",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test",
            )
            assert recorder.hf_job_id is None


# ---------------------------------------------------------------------------
# start()
# ---------------------------------------------------------------------------


class TestStart:
    @patch("analytics.cost.HfApi")
    def test_start_uploads_running_state(self, mock_hf_api_cls: MagicMock) -> None:
        mock_api = mock_hf_api_cls.return_value
        recorder = HFJobsCostRecorder(
            workflow_id="wf-compute_xg",
            phase="compute",
            rate_usd_per_hour=0.01,
            repo_id="luxury-lakehouse/xg-shot-data",
        )
        recorder.start()

        mock_api.upload_file.assert_called_once()
        call_kwargs = mock_api.upload_file.call_args
        payload = json.loads(call_kwargs.kwargs["path_or_fileobj"])

        assert payload["workflow_id"] == "wf-compute_xg"
        assert payload["phase"] == "compute"
        assert payload["state"] == "RUNNING"
        assert payload["rate_usd_per_hour"] == 0.01
        assert "started_at" in payload
        assert "updated_at" in payload
        # Verify ISO format parse doesn't throw
        datetime.fromisoformat(payload["started_at"])

        assert call_kwargs.kwargs["path_in_repo"] == "_workflow_cost.json"
        assert call_kwargs.kwargs["repo_id"] == "luxury-lakehouse/xg-shot-data"
        assert call_kwargs.kwargs["repo_type"] == "dataset"

    @patch("analytics.cost.time.sleep")
    @patch("analytics.cost.HfApi")
    def test_start_swallows_connection_error(
        self, mock_hf_api_cls: MagicMock, _mock_sleep: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_api = mock_hf_api_cls.return_value
        mock_api.upload_file.side_effect = ConnectionError("network down")

        recorder = HFJobsCostRecorder(
            workflow_id="wf-test",
            phase="compute",
            rate_usd_per_hour=0.01,
            repo_id="luxury-lakehouse/test",
        )
        # Must not raise
        with caplog.at_level(logging.WARNING, logger="analytics.cost"):
            recorder.start()

        assert any("failed" in record.message.lower() for record in caplog.records)

    @patch("analytics.cost.HfApi")
    def test_start_includes_hf_job_id(self, mock_hf_api_cls: MagicMock) -> None:
        mock_api = mock_hf_api_cls.return_value

        with patch.dict("os.environ", {"HF_JOB_ID": "job-xyz"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="compute",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test",
            )
        recorder.start()

        payload = json.loads(mock_api.upload_file.call_args.kwargs["path_or_fileobj"])
        assert payload["hf_job_id"] == "job-xyz"


# ---------------------------------------------------------------------------
# complete()
# ---------------------------------------------------------------------------


class TestComplete:
    @patch("analytics.cost.HfApi")
    def test_complete_returns_new_dict_with_cost_fields(self, mock_hf_api_cls: MagicMock) -> None:
        recorder = HFJobsCostRecorder(
            workflow_id="wf-compute_xg",
            phase="compute",
            rate_usd_per_hour=1.00,
            repo_id="luxury-lakehouse/xg-shot-data",
        )
        recorder.start()

        original: dict[str, object] = {"key": "value"}
        result = recorder.complete(original, row_count=42_000)

        # Original must be unmodified
        assert original == {"key": "value"}

        # Result must contain original keys plus cost fields
        assert result["key"] == "value"
        assert "elapsed_seconds" in result
        assert "rate_usd_per_hour" in result
        assert "estimated_cost_usd" in result
        assert "workflow_id" in result
        assert "workflow_phase" in result
        assert result["row_count"] == 42_000
        assert result["workflow_id"] == "wf-compute_xg"
        assert result["workflow_phase"] == "compute"
        assert result["rate_usd_per_hour"] == 1.00

    @patch("analytics.cost.HfApi")
    def test_complete_uploads_completed_state(self, mock_hf_api_cls: MagicMock) -> None:
        mock_api = mock_hf_api_cls.return_value
        recorder = HFJobsCostRecorder(
            workflow_id="wf-compute_xg",
            phase="compute",
            rate_usd_per_hour=1.00,
            repo_id="luxury-lakehouse/xg-shot-data",
        )
        recorder.start()
        recorder.complete({}, row_count=100)

        # Three uploads: start (_workflow_cost.json) + complete (_workflow_cost.json) + complete (_cost_history/)
        assert mock_api.upload_file.call_count == 3
        complete_call = mock_api.upload_file.call_args_list[1]
        payload = json.loads(complete_call.kwargs["path_or_fileobj"])

        assert payload["state"] == "COMPLETED"
        assert payload["row_count"] == 100
        assert "ended_at" in payload
        assert "duration_seconds" in payload
        assert "estimated_cost_usd" in payload
        assert payload["estimated_cost_usd"] >= 0.0

    @patch("analytics.cost.HfApi")
    def test_complete_cost_calculation(self, mock_hf_api_cls: MagicMock) -> None:
        """Cost = rate_usd_per_hour * duration_seconds / 3600."""
        recorder = HFJobsCostRecorder(
            workflow_id="wf-test",
            phase="compute",
            rate_usd_per_hour=1.00,
            repo_id="luxury-lakehouse/test",
        )
        recorder.start()
        result = recorder.complete({})

        # Duration should be small (< 1 sec), cost proportional
        cost = result["estimated_cost_usd"]
        assert isinstance(cost, float)
        assert cost >= 0.0
        duration = result["elapsed_seconds"]
        assert isinstance(duration, float)
        expected_cost = 1.00 * duration / 3600
        assert abs(cost - expected_cost) < 1e-9

    @patch("analytics.cost.HfApi")
    def test_complete_row_count_none_default(self, mock_hf_api_cls: MagicMock) -> None:
        recorder = HFJobsCostRecorder(
            workflow_id="wf-test",
            phase="compute",
            rate_usd_per_hour=0.01,
            repo_id="luxury-lakehouse/test",
        )
        recorder.start()
        result = recorder.complete({})

        assert result["row_count"] is None


# ---------------------------------------------------------------------------
# fail()
# ---------------------------------------------------------------------------


class TestFail:
    @patch("analytics.cost.HfApi")
    def test_fail_uploads_failed_state(self, mock_hf_api_cls: MagicMock) -> None:
        mock_api = mock_hf_api_cls.return_value
        recorder = HFJobsCostRecorder(
            workflow_id="wf-test",
            phase="compute",
            rate_usd_per_hour=1.00,
            repo_id="luxury-lakehouse/test",
        )
        recorder.start()
        recorder.fail(RuntimeError("OOM killed"))

        # Three uploads: start (_workflow_cost.json) + fail (_workflow_cost.json) + fail (_cost_history/)
        assert mock_api.upload_file.call_count == 3
        fail_call = mock_api.upload_file.call_args_list[1]
        payload = json.loads(fail_call.kwargs["path_or_fileobj"])

        assert payload["state"] == "FAILED"
        assert payload["error"] == "OOM killed"
        assert "ended_at" in payload
        assert "duration_seconds" in payload
        assert "estimated_cost_usd" in payload
        assert payload["estimated_cost_usd"] >= 0.0

    @patch("analytics.cost.HfApi")
    def test_fail_swallows_upload_error(self, mock_hf_api_cls: MagicMock) -> None:
        mock_api = mock_hf_api_cls.return_value
        # First call (start) succeeds, second (fail) raises a non-retryable error
        # to avoid side_effect exhaustion from retry attempts
        mock_api.upload_file.side_effect = [None, PermissionError("access denied")]

        recorder = HFJobsCostRecorder(
            workflow_id="wf-test",
            phase="compute",
            rate_usd_per_hour=0.01,
            repo_id="luxury-lakehouse/test",
        )
        recorder.start()
        # Must not raise
        recorder.fail(RuntimeError("some error"))


# ---------------------------------------------------------------------------
# skip()
# ---------------------------------------------------------------------------


class TestSkip:
    @patch("analytics.cost.HfApi")
    def test_skip_uploads_skipped_state(self, mock_hf_api_cls: MagicMock) -> None:
        mock_api = mock_hf_api_cls.return_value
        recorder = HFJobsCostRecorder(
            workflow_id="wf-test",
            phase="compute",
            rate_usd_per_hour=0.01,
            repo_id="luxury-lakehouse/test",
        )
        recorder.start()
        recorder.skip("all matches already processed")

        # Three uploads: start (_workflow_cost.json) + skip (_workflow_cost.json) + skip (_cost_history/)
        assert mock_api.upload_file.call_count == 3
        skip_call = mock_api.upload_file.call_args_list[1]
        payload = json.loads(skip_call.kwargs["path_or_fileobj"])

        assert payload["state"] == "SKIPPED"
        assert payload["reason"] == "all matches already processed"
        assert payload["duration_seconds"] == 0
        assert payload["estimated_cost_usd"] == 0.0
        assert "ended_at" in payload

    @patch("analytics.cost.HfApi")
    def test_skip_without_start(self, mock_hf_api_cls: MagicMock) -> None:
        """Skip can be called without start — e.g. when skip guard fires early."""
        mock_api = mock_hf_api_cls.return_value
        recorder = HFJobsCostRecorder(
            workflow_id="wf-test",
            phase="compute",
            rate_usd_per_hour=0.01,
            repo_id="luxury-lakehouse/test",
        )
        recorder.skip("nothing to do")

        # Two uploads: skip (_workflow_cost.json) + skip (_cost_history/)
        assert mock_api.upload_file.call_count == 2
        # First call is the _workflow_cost.json upload
        payload = json.loads(mock_api.upload_file.call_args_list[0].kwargs["path_or_fileobj"])
        assert payload["state"] == "SKIPPED"
        assert payload["duration_seconds"] == 0
        assert payload["estimated_cost_usd"] == 0.0


# ---------------------------------------------------------------------------
# Upload retry behaviour
# ---------------------------------------------------------------------------


class TestUploadRetry:
    @patch("analytics.cost.time.sleep")
    @patch("analytics.cost.HfApi")
    def test_upload_retries_on_5xx_error(self, mock_hf_api_cls: MagicMock, _mock_sleep: MagicMock) -> None:
        """5xx errors are transient and should trigger retries."""
        mock_api = MagicMock()
        mock_api.upload_file.side_effect = [Exception("500 Internal Server Error"), None]
        mock_hf_api_cls.return_value = mock_api
        recorder = HFJobsCostRecorder(workflow_id="wf-test", phase="test", rate_usd_per_hour=0.01, repo_id="test/repo")
        recorder.start()
        assert mock_api.upload_file.call_count == 2
