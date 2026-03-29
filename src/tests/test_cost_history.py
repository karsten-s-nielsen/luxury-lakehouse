"""Tests for HFJobsCostRecorder — per-run cost history files.

Verifies that `complete()`, `fail()`, and `skip()` upload history files
to ``_cost_history/{hf_job_id}.json`` in addition to ``_workflow_cost.json``,
and that pruning deletes files older than 90 days.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from huggingface_hub.hf_api import RepoFile

from analytics.cost import HFJobsCostRecorder

# ---------------------------------------------------------------------------
# _cost_history/ upload on complete/fail/skip
# ---------------------------------------------------------------------------


class TestCostHistoryUpload:
    """Test _cost_history/ file upload on complete/fail/skip."""

    @patch("analytics.cost.HfApi")
    def test_complete_uploads_history_file(self, mock_hf_api_cls: MagicMock) -> None:
        mock_api = mock_hf_api_cls.return_value
        with patch.dict("os.environ", {"HF_JOB_ID": "job-abc123"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )
        recorder.start()
        recorder.complete({"key": "val"}, row_count=100)

        # Should have 3 uploads: start (_workflow_cost.json),
        # complete (_workflow_cost.json), complete (_cost_history/job-abc123.json)
        assert mock_api.upload_file.call_count == 3
        history_call = mock_api.upload_file.call_args_list[2]
        assert history_call.kwargs["path_in_repo"] == "_cost_history/job-abc123.json"

        # History file payload must match the complete payload
        body = json.loads(history_call.kwargs["path_or_fileobj"])
        assert body["state"] == "COMPLETED"
        assert body["row_count"] == 100
        assert body["workflow_id"] == "wf-test"

    @patch("analytics.cost.HfApi")
    def test_fail_uploads_history_file(self, mock_hf_api_cls: MagicMock) -> None:
        mock_api = mock_hf_api_cls.return_value
        with patch.dict("os.environ", {"HF_JOB_ID": "job-fail1"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )
        recorder.start()
        recorder.fail(RuntimeError("boom"))

        # 3 uploads: start + fail _workflow_cost.json + fail _cost_history
        assert mock_api.upload_file.call_count == 3
        history_call = mock_api.upload_file.call_args_list[2]
        assert history_call.kwargs["path_in_repo"] == "_cost_history/job-fail1.json"
        body = json.loads(history_call.kwargs["path_or_fileobj"])
        assert body["state"] == "FAILED"
        assert "error" in body

    @patch("analytics.cost.HfApi")
    def test_skip_uploads_history_file(self, mock_hf_api_cls: MagicMock) -> None:
        mock_api = mock_hf_api_cls.return_value
        with patch.dict("os.environ", {"HF_JOB_ID": "job-skip1"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )
        recorder.skip("already computed")

        # 2 uploads: skip _workflow_cost.json + skip _cost_history
        assert mock_api.upload_file.call_count == 2
        history_call = mock_api.upload_file.call_args_list[1]
        assert history_call.kwargs["path_in_repo"] == "_cost_history/job-skip1.json"
        body = json.loads(history_call.kwargs["path_or_fileobj"])
        assert body["state"] == "SKIPPED"
        assert body["reason"] == "already computed"


# ---------------------------------------------------------------------------
# Timestamp slug fallback when HF_JOB_ID is unset
# ---------------------------------------------------------------------------


class TestTimestampSlugFallback:
    """When HF_JOB_ID is not set, use a UTC timestamp slug as filename."""

    @patch("analytics.cost.HfApi")
    def test_no_job_id_uses_timestamp_slug(self, mock_hf_api_cls: MagicMock) -> None:
        mock_api = mock_hf_api_cls.return_value
        with patch.dict("os.environ", {}, clear=True):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )
        recorder.start()
        recorder.complete({}, row_count=0)

        history_call = mock_api.upload_file.call_args_list[2]
        path = history_call.kwargs["path_in_repo"]
        assert path.startswith("_cost_history/")
        assert path.endswith(".json")
        # Must NOT contain "None" — that would mean the fallback is broken
        assert "None" not in path
        # Should be a valid timestamp slug like 20260328T120000Z
        filename = path.removeprefix("_cost_history/").removesuffix(".json")
        assert len(filename) == 16  # YYYYMMDDTHHMMSSz
        assert filename.endswith("Z")


# ---------------------------------------------------------------------------
# Fire-and-forget — history upload failure must not propagate
# ---------------------------------------------------------------------------


class TestHistoryFireAndForget:
    """History upload failures are logged but never propagate."""

    @patch("analytics.cost.HfApi")
    def test_history_upload_failure_does_not_propagate(
        self, mock_hf_api_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_api = mock_hf_api_cls.return_value
        with patch.dict("os.environ", {"HF_JOB_ID": "job-err"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )
        # start succeeds, complete _workflow_cost.json succeeds,
        # history upload fails
        mock_api.upload_file.side_effect = [None, None, ConnectionError("network down")]

        recorder.start()
        with caplog.at_level(logging.WARNING, logger="analytics.cost"):
            result = recorder.complete({"k": "v"})

        # complete() must still return enriched metadata
        assert "elapsed_seconds" in result
        assert any("history" in r.message.lower() or "failed" in r.message.lower() for r in caplog.records)

    @patch("analytics.cost.HfApi")
    def test_fail_history_upload_failure_does_not_propagate(self, mock_hf_api_cls: MagicMock) -> None:
        mock_api = mock_hf_api_cls.return_value
        with patch.dict("os.environ", {"HF_JOB_ID": "job-err2"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )
        # start succeeds, fail _workflow_cost.json succeeds, history upload raises
        mock_api.upload_file.side_effect = [None, None, PermissionError("denied")]

        recorder.start()
        # Must not raise
        recorder.fail(RuntimeError("OOM"))


# ---------------------------------------------------------------------------
# Pruning — deletes _cost_history/ files older than 90 days
# ---------------------------------------------------------------------------


class TestCostHistoryPruning:
    """Test _cost_history/ pruning of old files."""

    @patch("analytics.cost.HfApi")
    def test_prunes_files_older_than_90_days(self, mock_hf_api_cls: MagicMock) -> None:
        mock_api = mock_hf_api_cls.return_value
        with patch.dict("os.environ", {"HF_JOB_ID": "job-new"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )

        # Mock list_repo_tree to return two files with last_commit dates
        old_commit = MagicMock()
        old_commit.date = datetime.now(tz=timezone.utc) - timedelta(days=120)
        old_file = MagicMock(spec=RepoFile)
        old_file.rfilename = "_cost_history/job-old.json"
        old_file.size = 200
        old_file.last_commit = old_commit

        new_commit = MagicMock()
        new_commit.date = datetime.now(tz=timezone.utc) - timedelta(days=5)
        new_file = MagicMock(spec=RepoFile)
        new_file.rfilename = "_cost_history/job-recent.json"
        new_file.size = 200
        new_file.last_commit = new_commit

        mock_api.list_repo_tree.return_value = [old_file, new_file]

        recorder.start()
        recorder.complete({}, row_count=0)

        # Verify delete_file was called for old file only
        delete_calls = [c for c in mock_api.delete_file.call_args_list if "_cost_history/job-old.json" in str(c)]
        assert len(delete_calls) == 1

        # Verify new file was NOT deleted
        delete_new = [c for c in mock_api.delete_file.call_args_list if "_cost_history/job-recent.json" in str(c)]
        assert len(delete_new) == 0

    @patch("analytics.cost.HfApi")
    def test_pruning_failure_does_not_propagate(self, mock_hf_api_cls: MagicMock) -> None:
        """Pruning errors are swallowed — never crashes the pipeline."""
        mock_api = mock_hf_api_cls.return_value
        with patch.dict("os.environ", {"HF_JOB_ID": "job-prune-err"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )

        # list_repo_tree raises
        mock_api.list_repo_tree.side_effect = Exception("repo not found")

        recorder.start()
        # Must not raise despite pruning failure
        result = recorder.complete({"k": "v"})
        assert "elapsed_seconds" in result

    @patch("analytics.cost.HfApi")
    def test_pruning_skips_non_json_files(self, mock_hf_api_cls: MagicMock, tmp_path: MagicMock) -> None:
        """Non-.json files in _cost_history/ are ignored during pruning."""
        mock_api = mock_hf_api_cls.return_value
        with patch.dict("os.environ", {"HF_JOB_ID": "job-filter"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )

        # A non-json file (e.g., .gitkeep) in the directory — still a RepoFile
        non_json = MagicMock(spec=RepoFile)
        non_json.rfilename = "_cost_history/.gitkeep"
        non_json.size = 0
        mock_api.list_repo_tree.return_value = [non_json]

        recorder.start()
        # Must not raise and must not try to download non-json files
        recorder.complete({})

        mock_api.hf_hub_download.assert_not_called()
        mock_api.delete_file.assert_not_called()

    @patch("analytics.cost.HfApi")
    def test_pruning_handles_empty_history_dir(self, mock_hf_api_cls: MagicMock) -> None:
        """Empty _cost_history/ directory does not cause errors."""
        mock_api = mock_hf_api_cls.return_value
        with patch.dict("os.environ", {"HF_JOB_ID": "job-empty"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )

        mock_api.list_repo_tree.return_value = []

        recorder.start()
        # Must not raise
        result = recorder.complete({})
        assert "elapsed_seconds" in result
        mock_api.delete_file.assert_not_called()


# ---------------------------------------------------------------------------
# Existing _workflow_cost.json upload is NOT disrupted
# ---------------------------------------------------------------------------


class TestExistingUploadUnchanged:
    """History feature must not break the existing _workflow_cost.json upload."""

    @patch("analytics.cost.HfApi")
    def test_complete_still_uploads_workflow_cost_json(self, mock_hf_api_cls: MagicMock) -> None:
        mock_api = mock_hf_api_cls.return_value
        with patch.dict("os.environ", {"HF_JOB_ID": "job-x"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="compute",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test",
            )
        recorder.start()
        recorder.complete({}, row_count=50)

        # First two calls write _workflow_cost.json (start + complete)
        for idx in (0, 1):
            call = mock_api.upload_file.call_args_list[idx]
            assert call.kwargs["path_in_repo"] == "_workflow_cost.json"

    @patch("analytics.cost.HfApi")
    def test_start_does_not_upload_history(self, mock_hf_api_cls: MagicMock) -> None:
        """start() only writes _workflow_cost.json — no history file."""
        mock_api = mock_hf_api_cls.return_value
        with patch.dict("os.environ", {"HF_JOB_ID": "job-y"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="compute",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test",
            )
        recorder.start()

        # Only one upload: start _workflow_cost.json
        assert mock_api.upload_file.call_count == 1
        assert mock_api.upload_file.call_args.kwargs["path_in_repo"] == "_workflow_cost.json"
