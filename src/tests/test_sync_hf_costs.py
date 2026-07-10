"""Tests for HF Jobs cost bridge script."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import yaml
from huggingface_hub.hf_api import RepoFile


class TestDiscoverHfRepos:
    """Test workflow card parsing for HF Jobs repo discovery."""

    def test_finds_dataset_repos(self, tmp_path: Path) -> None:
        from ingestion.sync_hf_costs import discover_hf_repos

        card = {
            "id": "wf-xt-grids",
            "execution": {"training": {"runtime": "hf-jobs", "script": "scripts/compute_xt_grid_hf.py"}},
            "outputs": {"datasets": [{"id": "luxury-lakehouse/expected-threat-grids", "destination": "huggingface"}]},
        }
        card_path = tmp_path / "wf-xt-grids.yaml"
        card_path.write_text(yaml.dump(card))

        repos = discover_hf_repos(tmp_path)
        assert ("luxury-lakehouse/expected-threat-grids", "dataset", "wf-xt-grids") in repos

    def test_finds_model_repos(self, tmp_path: Path) -> None:
        from ingestion.sync_hf_costs import discover_hf_repos

        card = {
            "id": "wf-xg-v2",
            "execution": {"training": {"runtime": "hf-jobs", "script": "scripts/train_xg_v3_hf.py"}},
            "outputs": {"models": [{"id": "luxury-lakehouse/xg-v2-model-set-encoder", "destination": "huggingface"}]},
        }
        card_path = tmp_path / "wf-xg-v2.yaml"
        card_path.write_text(yaml.dump(card))

        repos = discover_hf_repos(tmp_path)
        assert ("luxury-lakehouse/xg-v2-model-set-encoder", "model", "wf-xg-v2") in repos

    def test_skips_non_hf_jobs_cards(self, tmp_path: Path) -> None:
        from ingestion.sync_hf_costs import discover_hf_repos

        card = {
            "id": "wf-line-breaking",
            "execution": {"inference": {"runtime": "databricks-workflow", "entry_point": "compute_line_breaking"}},
            "outputs": {"tables": [{"id": "bronze.line_breaking_passes", "destination": "delta-table"}]},
        }
        (tmp_path / "wf-line-breaking.yaml").write_text(yaml.dump(card))

        repos = discover_hf_repos(tmp_path)
        assert len(repos) == 0


class TestFetchCostJson:
    """Test _workflow_cost.json download from HF Hub."""

    def test_returns_parsed_json_on_success(self, tmp_path: Path) -> None:
        from ingestion.sync_hf_costs import fetch_cost_json

        cost_data = {
            "workflow_id": "wf-xt-grids",
            "phase": "inference",
            "state": "COMPLETED",
            "started_at": "2026-03-26T10:00:00+00:00",
            "ended_at": "2026-03-26T10:05:00+00:00",
            "duration_seconds": 300.0,
            "estimated_cost_usd": 0.083,
            "rate_usd_per_hour": 1.00,
            "hf_job_id": "job-abc123",
            "updated_at": "2026-03-26T10:05:00+00:00",
        }
        cost_file = tmp_path / "_workflow_cost.json"
        cost_file.write_text(json.dumps(cost_data))

        api = MagicMock()
        api.hf_hub_download.return_value = str(cost_file)

        result = fetch_cost_json(api, "luxury-lakehouse/expected-threat-grids", "dataset")
        assert result is not None
        assert result["workflow_id"] == "wf-xt-grids"
        assert result["state"] == "COMPLETED"

    def test_returns_none_on_missing_file(self) -> None:
        from ingestion.sync_hf_costs import fetch_cost_json

        api = MagicMock()
        from huggingface_hub.errors import EntryNotFoundError

        api.hf_hub_download.side_effect = EntryNotFoundError("not found")

        result = fetch_cost_json(api, "luxury-lakehouse/fake-repo", "dataset")
        assert result is None

    def test_returns_none_on_network_error(self) -> None:
        from ingestion.sync_hf_costs import fetch_cost_json

        api = MagicMock()
        api.hf_hub_download.side_effect = ConnectionError("timeout")

        result = fetch_cost_json(api, "luxury-lakehouse/fake-repo", "dataset")
        assert result is None


class TestMapToDeltaSchema:
    """Test JSON -> Delta schema mapping."""

    def test_maps_completed_record(self) -> None:
        from ingestion.cost_hook import _COST_LIVE_COLUMNS
        from ingestion.sync_hf_costs import map_to_delta_schema

        cost_data = {
            "workflow_id": "wf-xt-grids",
            "phase": "inference",
            "state": "COMPLETED",
            "started_at": "2026-03-26T10:00:00+00:00",
            "ended_at": "2026-03-26T10:05:00+00:00",
            "duration_seconds": 300.0,
            "estimated_cost_usd": 0.083,
            "rate_usd_per_hour": 1.00,
            "hf_job_id": "job-abc123",
            "updated_at": "2026-03-26T10:05:00+00:00",
        }
        task_key = "compute_xt_grid"
        row = map_to_delta_schema(cost_data, task_key)

        assert row["workflow_id"] == "wf-xt-grids"
        assert row["runtime"] == "hf_jobs"
        assert row["cost_source"] == "hf_hub_sync"
        assert row["run_id"] == "hf-job-abc123"
        assert row["hf_job_id"] == "job-abc123"
        # Schema-drift guard: the row keys must EXACTLY match the canonical
        # _COST_LIVE_COLUMNS list. task_key and job_run_id are orphaned and
        # must NOT appear; entity_count + guard_duration_seconds must appear
        # as None (HF Jobs have no Databricks guard phase / entity count).
        canonical_cols = {name for name, _t, _n in _COST_LIVE_COLUMNS}
        assert set(row.keys()) == canonical_cols, (
            f"sync_hf_costs schema drifted from canonical:\n"
            f"  only in row: {set(row.keys()) - canonical_cols}\n"
            f"  only in canonical: {canonical_cols - set(row.keys())}"
        )
        assert "task_key" not in row
        assert "job_run_id" not in row
        assert row["entity_count"] is None
        assert row["guard_duration_seconds"] is None

    def test_maps_running_record_with_nulls(self) -> None:
        from ingestion.sync_hf_costs import map_to_delta_schema

        cost_data = {
            "workflow_id": "wf-vaep",
            "phase": "training",
            "state": "RUNNING",
            "started_at": "2026-03-26T10:00:00+00:00",
            "rate_usd_per_hour": 0.01,
            "hf_job_id": "job-xyz",
            "updated_at": "2026-03-26T10:00:00+00:00",
        }
        row = map_to_delta_schema(cost_data, "train_vaep")

        assert row["state"] == "RUNNING"
        assert row["ended_at"] is None
        assert row["duration_seconds"] is None
        assert row["estimated_cost_usd"] is None


class TestFetchCostHistory:
    """Test reading _cost_history/ directory from HF Hub."""

    def test_reads_multiple_history_files(self, tmp_path: Path) -> None:
        from ingestion.sync_hf_costs import fetch_cost_history

        # Create two history files
        history_dir = tmp_path / "_cost_history"
        history_dir.mkdir()
        for i, job_id in enumerate(["job-a", "job-b"]):
            data = {
                "workflow_id": "wf-test",
                "phase": "training",
                "state": "COMPLETED",
                "hf_job_id": job_id,
                "started_at": f"2026-03-{20 + i}T10:00:00+00:00",
                "duration_seconds": 300,
                "estimated_cost_usd": 0.001,
            }
            (history_dir / f"{job_id}.json").write_text(json.dumps(data))

        api = MagicMock()
        # list_repo_tree returns file entries (spec=RepoFile for isinstance check)
        file_a = MagicMock(spec=RepoFile)
        file_a.rfilename = "_cost_history/job-a.json"
        file_a.size = 200
        file_b = MagicMock(spec=RepoFile)
        file_b.rfilename = "_cost_history/job-b.json"
        file_b.size = 200
        api.list_repo_tree.return_value = [file_a, file_b]
        api.hf_hub_download.side_effect = lambda repo_id, filename, repo_type: str(tmp_path / filename)

        records = fetch_cost_history(api, "luxury-lakehouse/test", "dataset")
        assert len(records) == 2
        assert {r["hf_job_id"] for r in records} == {"job-a", "job-b"}

    def test_returns_empty_on_missing_directory(self) -> None:
        from ingestion.sync_hf_costs import fetch_cost_history

        api = MagicMock()
        api.list_repo_tree.side_effect = Exception("not found")

        records = fetch_cost_history(api, "luxury-lakehouse/test", "dataset")
        assert records == []

    def test_also_reads_legacy_workflow_cost_json(self, tmp_path: Path) -> None:
        """Falls back to _workflow_cost.json if _cost_history/ is empty."""
        from ingestion.sync_hf_costs import fetch_cost_history

        data = {
            "workflow_id": "wf-test",
            "state": "COMPLETED",
            "hf_job_id": "job-legacy",
            "started_at": "2026-03-25T10:00:00+00:00",
            "duration_seconds": 60,
            "estimated_cost_usd": 0.0002,
        }
        (tmp_path / "_workflow_cost.json").write_text(json.dumps(data))

        api = MagicMock()
        api.list_repo_tree.return_value = []  # empty _cost_history/
        api.hf_hub_download.return_value = str(tmp_path / "_workflow_cost.json")

        records = fetch_cost_history(api, "luxury-lakehouse/test", "dataset")
        assert len(records) == 1
        assert records[0]["hf_job_id"] == "job-legacy"

    def test_legacy_fallback_excludes_running_records(self, tmp_path: Path) -> None:
        """Legacy fallback only includes non-RUNNING records."""
        from ingestion.sync_hf_costs import fetch_cost_history

        data = {
            "workflow_id": "wf-test",
            "state": "RUNNING",
            "hf_job_id": "job-still-running",
            "started_at": "2026-03-25T10:00:00+00:00",
        }
        (tmp_path / "_workflow_cost.json").write_text(json.dumps(data))

        api = MagicMock()
        api.list_repo_tree.return_value = []  # empty _cost_history/
        api.hf_hub_download.return_value = str(tmp_path / "_workflow_cost.json")

        records = fetch_cost_history(api, "luxury-lakehouse/test", "dataset")
        assert records == []

    def test_skips_records_without_hf_job_id(self, tmp_path: Path) -> None:
        """Records missing hf_job_id are skipped."""
        from ingestion.sync_hf_costs import fetch_cost_history

        # Create a history file without hf_job_id
        history_dir = tmp_path / "_cost_history"
        history_dir.mkdir()
        data = {"workflow_id": "wf-test", "state": "COMPLETED"}
        (history_dir / "bad.json").write_text(json.dumps(data))

        api = MagicMock()
        file_entry = MagicMock(spec=RepoFile)
        file_entry.rfilename = "_cost_history/bad.json"
        file_entry.size = 100
        api.list_repo_tree.return_value = [file_entry]
        api.hf_hub_download.side_effect = lambda repo_id, filename, repo_type: str(tmp_path / filename)

        records = fetch_cost_history(api, "luxury-lakehouse/test", "dataset")
        assert records == []

    def test_skips_non_json_files_in_history(self) -> None:
        """Non-.json files in _cost_history/ are ignored."""
        from ingestion.sync_hf_costs import fetch_cost_history

        api = MagicMock()
        gitkeep = MagicMock(spec=RepoFile)
        gitkeep.rfilename = "_cost_history/.gitkeep"
        gitkeep.size = 0
        api.list_repo_tree.return_value = [gitkeep]

        # Legacy fallback also finds nothing
        from huggingface_hub.errors import EntryNotFoundError

        api.hf_hub_download.side_effect = EntryNotFoundError("not found")

        records = fetch_cost_history(api, "luxury-lakehouse/test", "dataset")
        assert records == []
