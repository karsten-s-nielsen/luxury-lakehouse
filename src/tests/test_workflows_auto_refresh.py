"""Tests for Workflows page auto-refresh, HF cost history, and status badges."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Add hf_taipy_app/src to path so we can import the state module
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))

pytest.importorskip("databricks.sdk", reason="databricks-sdk not installed")

from state.workflows import (
    _fetch_hf_cost_history_impl,
    wf_style_status,
)


class TestFetchHfCostHistory:
    """Test _fetch_hf_cost_history_impl function."""

    def test_returns_running_from_live_file(self, tmp_path: Path) -> None:
        """RUNNING state detected from _workflow_cost.json."""
        cost_data = {
            "workflow_id": "wf-xt-grids",
            "phase": "inference",
            "state": "RUNNING",
            "started_at": "2026-03-26T10:00:00+00:00",
            "rate_usd_per_hour": 1.00,
            "hf_job_id": "job-abc",
            "updated_at": "2026-03-26T10:00:00+00:00",
        }
        cost_file = tmp_path / "cost.json"
        cost_file.write_text(json.dumps(cost_data))

        mock_api = MagicMock()
        mock_api.hf_hub_download.return_value = str(cost_file)
        mock_api.list_repo_files.return_value = []

        result = _fetch_hf_cost_history_impl(
            mock_api,
            [("luxury-lakehouse/expected-threat-grids", "dataset", "wf-xt-grids")],
        )
        assert "wf-xt-grids" in result
        assert result["wf-xt-grids"].is_running is True

    def test_returns_completed_runs_from_history(self, tmp_path: Path) -> None:
        """Completed runs loaded from _cost_history/ files."""
        from datetime import datetime, timedelta, timezone

        # Use a recent timestamp so the implementation's 30-day filter
        # (state/workflows.py: end_ts < cutoff -> skip) does not silently
        # drop the fixture and trigger the legacy fallback path. Hardcoded
        # 2026-03-26 dates aged out of the window on 2026-04-25 and made
        # this test a time-bomb; relative timestamps keep it evergreen.
        ended = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        started = ended - timedelta(minutes=5)

        # Live file shows COMPLETED (not RUNNING)
        live_data = {"workflow_id": "wf-xt-grids", "state": "COMPLETED"}
        live_file = tmp_path / "live.json"
        live_file.write_text(json.dumps(live_data))

        # History file
        run_data = {
            "workflow_id": "wf-xt-grids",
            "state": "COMPLETED",
            "started_at": started.isoformat(),
            "ended_at": ended.isoformat(),
            "duration_seconds": 300,
            "estimated_cost_usd": 0.05,
        }
        history_file = tmp_path / "run.json"
        history_file.write_text(json.dumps(run_data))

        mock_api = MagicMock()

        def _download(repo_id: str, filename: str, repo_type: str) -> str:
            if filename == "_workflow_cost.json":
                return str(live_file)
            return str(history_file)

        mock_api.hf_hub_download.side_effect = _download
        mock_api.list_repo_files.return_value = ["_cost_history/run-001.json"]

        result = _fetch_hf_cost_history_impl(
            mock_api,
            [("luxury-lakehouse/expected-threat-grids", "dataset", "wf-xt-grids")],
        )
        assert "wf-xt-grids" in result
        data = result["wf-xt-grids"]
        assert data.is_running is False
        assert len(data.runs) == 1
        assert data.runs[0]["state"] == "COMPLETED"
        assert data.latest_run is not None
        assert data.latest_run["estimated_cost_usd"] == 0.05

    def test_returns_empty_on_network_error(self) -> None:
        """Network errors produce empty result, not exceptions."""
        mock_api = MagicMock()
        mock_api.hf_hub_download.side_effect = ConnectionError("timeout")
        mock_api.list_repo_files.side_effect = ConnectionError("timeout")

        result = _fetch_hf_cost_history_impl(
            mock_api,
            [("luxury-lakehouse/fake-repo", "dataset", "wf-fake")],
        )
        # Should still have the key but empty data
        assert "wf-fake" in result
        assert result["wf-fake"].is_running is False
        assert len(result["wf-fake"].runs) == 0


class TestStyleStatus:
    """Test wf_style_status callback."""

    def test_running_returns_running_class(self) -> None:
        assert "running" in wf_style_status(None, "RUNNING", 0, 0, "Status")

    def test_completed_returns_completed_class(self) -> None:
        assert "completed" in wf_style_status(None, "COMPLETED", 0, 0, "Status")

    def test_failed_returns_failed_class(self) -> None:
        assert "failed" in wf_style_status(None, "FAILED", 0, 0, "Status")

    def test_skipped_returns_skipped_class(self) -> None:
        assert "skipped" in wf_style_status(None, "SKIPPED", 0, 0, "Status")

    def test_unknown_returns_empty(self) -> None:
        assert wf_style_status(None, "UNKNOWN", 0, 0, "Status") == ""
