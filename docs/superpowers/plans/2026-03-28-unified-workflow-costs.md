# Unified Workflow Cost Tracking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Cost (30d), Avg/Run, Last Run, Duration, Status, and Freshness columns work for all workflow runtime types (DB, HF, DB+HF) by unifying on `workflow_id` as the primary key and reading HF Jobs cost history directly from HF Hub.

**Architecture:** `HFJobsCostRecorder` writes per-run files to `_cost_history/{hf_job_id}.json` on HF Hub. The Taipy app reads these directly (60s TTL) and combines with Databricks cold-tier costs (now keyed by `workflow_id` via a dbt model join). `sync_hf_costs.py` runs daily as a catch-all backup to populate Delta.

**Tech Stack:** Python, huggingface_hub, dbt (SQL), Terraform (HCL), pytest

**Spec:** `docs/superpowers/specs/2026-03-28-unified-workflow-costs-design.md`

---

### Task 1: `HFJobsCostRecorder` — per-run cost history files

**Files:**
- Modify: `src/analytics/cost.py`
- Test: `src/tests/test_cost_history.py` (create)

- [ ] **Step 1: Write the failing test for `_upload_history`**

Create `src/tests/test_cost_history.py`:

```python
"""Tests for HFJobsCostRecorder per-run cost history."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, call, patch

import pytest


class TestCostHistoryUpload:
    """Test _cost_history/ file upload on complete/fail/skip."""

    def test_complete_uploads_history_file(self) -> None:
        from analytics.cost import HFJobsCostRecorder

        with patch.dict("os.environ", {"HF_JOB_ID": "job-abc123"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )
        recorder._api = MagicMock()
        recorder.start()
        recorder.complete({"key": "val"}, row_count=100)

        # Should have 3 uploads: start (_workflow_cost.json), complete (_workflow_cost.json), complete (_cost_history/job-abc123.json)
        assert recorder._api.upload_file.call_count == 3
        history_call = recorder._api.upload_file.call_args_list[2]
        assert history_call.kwargs["path_in_repo"] == "_cost_history/job-abc123.json"

    def test_fail_uploads_history_file(self) -> None:
        from analytics.cost import HFJobsCostRecorder

        with patch.dict("os.environ", {"HF_JOB_ID": "job-fail1"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )
        recorder._api = MagicMock()
        recorder.start()
        recorder.fail(RuntimeError("boom"))

        history_call = recorder._api.upload_file.call_args_list[2]
        assert history_call.kwargs["path_in_repo"] == "_cost_history/job-fail1.json"
        body = json.loads(history_call.kwargs["path_or_fileobj"])
        assert body["state"] == "FAILED"

    def test_skip_uploads_history_file(self) -> None:
        from analytics.cost import HFJobsCostRecorder

        with patch.dict("os.environ", {"HF_JOB_ID": "job-skip1"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )
        recorder._api = MagicMock()
        recorder.skip("already computed")

        history_call = recorder._api.upload_file.call_args_list[1]
        assert history_call.kwargs["path_in_repo"] == "_cost_history/job-skip1.json"
        body = json.loads(history_call.kwargs["path_or_fileobj"])
        assert body["state"] == "SKIPPED"

    def test_no_job_id_uses_timestamp_slug(self) -> None:
        from analytics.cost import HFJobsCostRecorder

        with patch.dict("os.environ", {}, clear=True):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )
        recorder._api = MagicMock()
        recorder.start()
        recorder.complete({}, row_count=0)

        history_call = recorder._api.upload_file.call_args_list[2]
        path = history_call.kwargs["path_in_repo"]
        assert path.startswith("_cost_history/")
        assert path.endswith(".json")
        # Should not contain "None"
        assert "None" not in path

    def test_history_upload_failure_does_not_propagate(self) -> None:
        from analytics.cost import HFJobsCostRecorder

        with patch.dict("os.environ", {"HF_JOB_ID": "job-err"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )
        recorder._api = MagicMock()
        # First two uploads succeed (start + complete _workflow_cost.json),
        # third fails (history upload)
        recorder._api.upload_file.side_effect = [None, None, ConnectionError("fail")]
        recorder.start()
        # Should not raise
        result = recorder.complete({"k": "v"})
        assert "elapsed_seconds" in result


class TestCostHistoryPruning:
    """Test _cost_history/ pruning of old files."""

    def test_prunes_files_older_than_90_days(self) -> None:
        from analytics.cost import HFJobsCostRecorder

        with patch.dict("os.environ", {"HF_JOB_ID": "job-new"}):
            recorder = HFJobsCostRecorder(
                workflow_id="wf-test",
                phase="training",
                rate_usd_per_hour=0.01,
                repo_id="luxury-lakehouse/test-repo",
            )
        recorder._api = MagicMock()

        # Mock list_repo_tree to return old + new files
        old_file = MagicMock()
        old_file.rfilename = "_cost_history/job-old.json"
        old_file.size = 200
        new_file = MagicMock()
        new_file.rfilename = "_cost_history/job-recent.json"
        new_file.size = 200
        recorder._api.list_repo_tree.return_value = [old_file, new_file]

        # Mock downloads: old file has started_at > 90 days ago
        old_json = json.dumps({"started_at": "2025-01-01T00:00:00+00:00"})
        new_json = json.dumps({"started_at": "2026-03-27T00:00:00+00:00"})

        def mock_download(repo_id, filename, repo_type):
            if "old" in filename:
                return "/tmp/old.json"
            return "/tmp/new.json"

        recorder._api.hf_hub_download.side_effect = mock_download

        with patch("builtins.open", create=True) as mock_open:
            mock_open.return_value.__enter__ = lambda s: s
            mock_open.return_value.__exit__ = MagicMock(return_value=False)
            mock_open.return_value.read.side_effect = [old_json, new_json]

            recorder.start()
            recorder.complete({})

        # Verify delete_file was called for old file
        delete_calls = [c for c in recorder._api.delete_file.call_args_list
                       if "_cost_history/job-old.json" in str(c)]
        assert len(delete_calls) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_cost_history.py -v`
Expected: FAIL — no `_upload_history` method, no history upload behavior

- [ ] **Step 3: Implement `_upload_history` and `_prune_history` in `HFJobsCostRecorder`**

In `src/analytics/cost.py`, add after the existing constants:

```python
_COST_HISTORY_DIR = "_cost_history"
_HISTORY_RETENTION_DAYS = 90
```

Add these methods to `HFJobsCostRecorder`:

```python
    def _upload_history(self, payload: dict[str, object]) -> None:
        """Upload *payload* to ``_cost_history/{job_id}.json``. Never raises."""
        job_id = self.hf_job_id
        if not job_id:
            job_id = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = f"{_COST_HISTORY_DIR}/{job_id}.json"
        body = json.dumps(payload, indent=2, default=str).encode("utf-8")
        try:
            self._api.upload_file(
                path_or_fileobj=body,
                path_in_repo=path,
                repo_id=self.repo_id,
                repo_type=self.repo_type,
            )
            logger.info("Cost history uploaded: %s/%s", self.repo_id, path)
        except Exception as exc:
            logger.warning("Cost history upload to %s failed: %s", path, exc)

        self._prune_history()

    def _prune_history(self) -> None:
        """Delete _cost_history/ files older than 90 days. Never raises."""
        try:
            from datetime import timedelta
            cutoff = datetime.now(tz=timezone.utc) - timedelta(days=_HISTORY_RETENTION_DAYS)
            items = list(self._api.list_repo_tree(
                self.repo_id, repo_type=self.repo_type,
                path_in_repo=_COST_HISTORY_DIR,
            ))
            for item in items:
                if not hasattr(item, "size") or not item.rfilename.endswith(".json"):
                    continue
                try:
                    local = self._api.hf_hub_download(
                        repo_id=self.repo_id,
                        filename=item.rfilename,
                        repo_type=self.repo_type,
                    )
                    with open(local) as f:
                        data = json.load(f)
                    started = data.get("started_at", "")
                    if started and datetime.fromisoformat(started) < cutoff:
                        self._api.delete_file(
                            path_in_repo=item.rfilename,
                            repo_id=self.repo_id,
                            repo_type=self.repo_type,
                        )
                        logger.info("Pruned old cost history: %s", item.rfilename)
                except Exception:
                    logger.debug("Failed to check/prune %s", item.rfilename, exc_info=True)
        except Exception:
            logger.debug("Cost history pruning failed for %s", self.repo_id, exc_info=True)
```

Modify `complete()` — add history upload after the existing `_upload(payload)` call:

```python
        self._upload(payload)
        self._upload_history(payload)
```

Modify `fail()` — add history upload after the existing `_upload(payload)` call:

```python
        self._upload(payload)
        self._upload_history(payload)
```

Modify `skip()` — add history upload after the existing `_upload(payload)` call:

```python
        self._upload(payload)
        self._upload_history(payload)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_cost_history.py -v`
Expected: PASS

- [ ] **Step 5: Run quality gates**

Run: `uv run ruff check src/analytics/cost.py && uv run pyright src/analytics/cost.py`
Expected: No errors

---

### Task 2: `sync_hf_costs.py` — read `_cost_history/` directory

**Files:**
- Modify: `scripts/sync_hf_costs.py`
- Modify: `src/tests/test_sync_hf_costs.py`

- [ ] **Step 1: Write failing tests for `_cost_history/` reading**

Add to `src/tests/test_sync_hf_costs.py`:

```python
class TestFetchCostHistory:
    """Test reading _cost_history/ directory from HF Hub."""

    def test_reads_multiple_history_files(self, tmp_path: Path) -> None:
        from scripts.sync_hf_costs import fetch_cost_history

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
        # list_repo_tree returns file entries
        file_a = MagicMock()
        file_a.rfilename = "_cost_history/job-a.json"
        file_a.size = 200
        file_b = MagicMock()
        file_b.rfilename = "_cost_history/job-b.json"
        file_b.size = 200
        api.list_repo_tree.return_value = [file_a, file_b]
        api.hf_hub_download.side_effect = lambda repo_id, filename, repo_type: str(
            tmp_path / filename
        )

        records = fetch_cost_history(api, "luxury-lakehouse/test", "dataset")
        assert len(records) == 2
        assert {r["hf_job_id"] for r in records} == {"job-a", "job-b"}

    def test_returns_empty_on_missing_directory(self) -> None:
        from scripts.sync_hf_costs import fetch_cost_history

        api = MagicMock()
        api.list_repo_tree.side_effect = Exception("not found")

        records = fetch_cost_history(api, "luxury-lakehouse/test", "dataset")
        assert records == []

    def test_also_reads_legacy_workflow_cost_json(self, tmp_path: Path) -> None:
        """Falls back to _workflow_cost.json if _cost_history/ is empty."""
        from scripts.sync_hf_costs import fetch_cost_history

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_sync_hf_costs.py::TestFetchCostHistory -v`
Expected: FAIL — `fetch_cost_history` does not exist

- [ ] **Step 3: Implement `fetch_cost_history` and update `sync_costs`**

In `scripts/sync_hf_costs.py`, add after `fetch_cost_json`:

```python
def fetch_cost_history(api: HfApi, repo_id: str, repo_type: str) -> list[dict[str, Any]]:
    """Read all _cost_history/*.json files from an HF Hub repo.

    Falls back to _workflow_cost.json if _cost_history/ is empty or absent.
    Returns list of cost record dicts.
    """
    records: list[dict[str, Any]] = []

    # Try _cost_history/ directory first
    try:
        items = list(api.list_repo_tree(repo_id, repo_type=repo_type, path_in_repo="_cost_history"))
        for item in items:
            if not hasattr(item, "size") or not item.rfilename.endswith(".json"):
                continue
            try:
                local_path = api.hf_hub_download(
                    repo_id=repo_id,
                    filename=item.rfilename,
                    repo_type=repo_type,
                )
                with open(local_path) as f:
                    data = json.load(f)
                if isinstance(data, dict) and data.get("hf_job_id"):
                    records.append(data)
            except Exception:
                logger.debug("Failed to read %s from %s", item.rfilename, repo_id, exc_info=True)
    except Exception:
        logger.debug("No _cost_history/ in %s/%s", repo_type, repo_id, exc_info=True)

    # Fallback: read legacy _workflow_cost.json if no history files found
    if not records:
        legacy = fetch_cost_json(api, repo_id, repo_type)
        if legacy and legacy.get("hf_job_id") and legacy.get("state") != "RUNNING":
            records.append(legacy)

    return records
```

Update `sync_costs` to use `fetch_cost_history` instead of `fetch_cost_json`:

Replace the loop body:
```python
    rows: list[dict[str, Any]] = []
    for repo_id, repo_type, workflow_id in repos:
        history = fetch_cost_history(api, repo_id, repo_type)
        card = cards.get(workflow_id, {})
        task_key = _resolve_task_key(card)
        for cost_data in history:
            row = map_to_delta_schema(cost_data, task_key)
            if row["run_id"]:
                rows.append(row)
                logger.info("Fetched cost record: %s %s -> %s", workflow_id, cost_data.get("state"), row["run_id"])
```

- [ ] **Step 4: Run all sync_hf_costs tests**

Run: `uv run pytest src/tests/test_sync_hf_costs.py -v`
Expected: PASS

- [ ] **Step 5: Run quality gates**

Run: `uv run ruff check scripts/sync_hf_costs.py && uv run pyright scripts/sync_hf_costs.py`
Expected: No errors

---

### Task 3: dbt model — add `workflow_id` to `fct_workflow_costs`

**Files:**
- Modify: `dbt_project/models/marts/fct_workflow_costs.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Add `workflow_id` CTE and column to `fct_workflow_costs.sql`**

Add a new CTE `workflow_ids` after the `tasks` CTE:

```sql
,

workflow_ids AS (
    SELECT DISTINCT
        task_key,
        job_run_id,
        workflow_id
    FROM {{ source('observability', 'workflow_cost_live') }}
    WHERE workflow_id IS NOT NULL
      AND task_key IS NOT NULL
      AND job_run_id IS NOT NULL
)
```

Add LEFT JOIN and `workflow_id` to the final SELECT:

```sql
SELECT
    tasks.task_key,
    billing.usage_date,
    CAST(billing.job_run_id AS BIGINT) AS job_run_id,
    wcl.workflow_id,
    CAST(ROUND(
        billing.dbu * (
            tasks.execution_duration_seconds
            / NULLIF(SUM(tasks.execution_duration_seconds)
                OVER (PARTITION BY billing.job_run_id), 0)
        ),
        4
    ) AS DECIMAL(10, 4)) AS attributed_dbu,
    CAST(ROUND(
        billing.cost_usd * (
            tasks.execution_duration_seconds
            / NULLIF(SUM(tasks.execution_duration_seconds)
                OVER (PARTITION BY billing.job_run_id), 0)
        ),
        4
    ) AS DECIMAL(10, 4)) AS attributed_cost_usd
FROM billing
INNER JOIN tasks ON billing.job_run_id = tasks.job_run_id
LEFT JOIN workflow_ids AS wcl
    ON wcl.job_run_id = CAST(billing.job_run_id AS BIGINT)
    AND wcl.task_key = tasks.task_key
```

- [ ] **Step 2: Add `workflow_id` to the contract in `_marts__models.yml`**

Add after the `task_key` column definition:

```yaml
      - name: workflow_id
        data_type: string
        description: >
          Workflow card identifier (e.g., wf-vaep). Resolved from
          workflow_cost_live via task_key + job_run_id join. NULL for
          runs predating the CostEstimateHook deployment.
```

- [ ] **Step 3: Verify dbt compiles**

Run: `uv run dbt compile --select fct_workflow_costs`
Expected: Compiles without errors (note: full `dbt build` requires warehouse — compile is sufficient for validation)

---

### Task 4: App — `_fetch_hf_cost_history()` and re-keyed `_fetch_job_runs()`

**Files:**
- Modify: `hf_taipy_app/src/state/workflows.py`
- Modify: `src/tests/test_workflows_auto_refresh.py`

- [ ] **Step 1: Write failing tests for `_fetch_hf_cost_history_impl`**

Replace the contents of `src/tests/test_workflows_auto_refresh.py`:

```python
"""Tests for Workflows page auto-refresh, HF cost history, and status badges."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))

from state.workflows import (
    _fetch_hf_cost_history_impl,
    wf_style_status,
)


class TestFetchHfCostHistory:
    """Test _fetch_hf_cost_history_impl function."""

    def test_returns_running_status_from_live_file(self, tmp_path: Path) -> None:
        live_data = {
            "workflow_id": "wf-xt-grids",
            "state": "RUNNING",
            "hf_job_id": "job-abc",
            "started_at": "2026-03-28T10:00:00+00:00",
        }
        live_file = tmp_path / "live.json"
        live_file.write_text(json.dumps(live_data))

        mock_api = MagicMock()
        mock_api.hf_hub_download.return_value = str(live_file)
        mock_api.list_repo_tree.return_value = []  # no history

        result = _fetch_hf_cost_history_impl(
            mock_api,
            [("luxury-lakehouse/expected-threat-grids", "dataset", "wf-xt-grids")],
        )
        assert "wf-xt-grids" in result
        assert result["wf-xt-grids"].is_running is True

    def test_returns_completed_runs_from_history(self, tmp_path: Path) -> None:
        # Live file shows COMPLETED (not running)
        live_data = {"workflow_id": "wf-xt-grids", "state": "COMPLETED", "hf_job_id": "job-b"}
        live_file = tmp_path / "live.json"
        live_file.write_text(json.dumps(live_data))

        # History directory with two completed runs
        history_dir = tmp_path / "_cost_history"
        history_dir.mkdir()
        for job_id, day in [("job-a", "20"), ("job-b", "25")]:
            data = {
                "workflow_id": "wf-xt-grids",
                "state": "COMPLETED",
                "hf_job_id": job_id,
                "started_at": f"2026-03-{day}T10:00:00+00:00",
                "ended_at": f"2026-03-{day}T10:05:00+00:00",
                "duration_seconds": 300,
                "estimated_cost_usd": 0.001,
            }
            (history_dir / f"{job_id}.json").write_text(json.dumps(data))

        mock_api = MagicMock()
        mock_api.hf_hub_download.side_effect = lambda repo_id, filename, repo_type: str(
            tmp_path / filename
        )
        file_a = MagicMock()
        file_a.rfilename = "_cost_history/job-a.json"
        file_a.size = 200
        file_b = MagicMock()
        file_b.rfilename = "_cost_history/job-b.json"
        file_b.size = 200
        mock_api.list_repo_tree.return_value = [file_a, file_b]

        result = _fetch_hf_cost_history_impl(
            mock_api,
            [("luxury-lakehouse/expected-threat-grids", "dataset", "wf-xt-grids")],
        )
        hf_data = result["wf-xt-grids"]
        assert hf_data.is_running is False
        assert len(hf_data.runs) == 2
        assert hf_data.latest_run is not None
        assert hf_data.latest_run["hf_job_id"] == "job-b"

    def test_returns_empty_on_network_error(self) -> None:
        mock_api = MagicMock()
        mock_api.hf_hub_download.side_effect = ConnectionError("timeout")

        result = _fetch_hf_cost_history_impl(
            mock_api,
            [("luxury-lakehouse/fake-repo", "dataset", "wf-fake")],
        )
        assert len(result) == 0


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_workflows_auto_refresh.py::TestFetchHfCostHistory -v`
Expected: FAIL — `_fetch_hf_cost_history_impl` does not exist

- [ ] **Step 3: Implement `HFCostData` dataclass and `_fetch_hf_cost_history_impl`**

In `hf_taipy_app/src/state/workflows.py`, add the dataclass near the top (after imports):

```python
from dataclasses import dataclass, field

@dataclass
class HFCostData:
    """Aggregated HF Jobs cost data for a single workflow_id."""

    runs: list[dict[str, Any]] = field(default_factory=list)
    is_running: bool = False
    latest_run: dict[str, Any] | None = None
```

Replace `_fetch_live_hf_status_impl` with:

```python
def _fetch_hf_cost_history_impl(
    api: Any,
    repos: list[tuple[str, str, str]],
) -> dict[str, HFCostData]:
    """Read _workflow_cost.json (live status) and _cost_history/ (run history) from HF Hub repos.

    Returns dict keyed by workflow_id.
    """
    results: dict[str, HFCostData] = {}

    for repo_id, repo_type, workflow_id in repos:
        hf_data = HFCostData()

        # 1. Check live status from _workflow_cost.json
        try:
            local_path = api.hf_hub_download(
                repo_id=repo_id,
                filename="_workflow_cost.json",
                repo_type=repo_type,
            )
            with open(local_path) as f:
                live = json.load(f)
            if isinstance(live, dict) and live.get("state") == "RUNNING":
                hf_data.is_running = True
        except Exception:
            logger.debug("Live status check failed for %s/%s", repo_type, repo_id, exc_info=True)

        # 2. Read _cost_history/ directory
        try:
            items = list(api.list_repo_tree(repo_id, repo_type=repo_type, path_in_repo="_cost_history"))
            cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=30)
            for item in items:
                if not hasattr(item, "size") or not item.rfilename.endswith(".json"):
                    continue
                try:
                    local = api.hf_hub_download(
                        repo_id=repo_id,
                        filename=item.rfilename,
                        repo_type=repo_type,
                    )
                    with open(local) as f:
                        data = json.load(f)
                    started = data.get("started_at")
                    if started and pd.Timestamp(started) >= cutoff:
                        hf_data.runs.append(data)
                except Exception:
                    logger.debug("Failed to read %s", item.rfilename, exc_info=True)
        except Exception:
            logger.debug("No _cost_history/ in %s/%s", repo_type, repo_id, exc_info=True)

        # 3. Determine latest completed/failed run
        terminal_runs = [r for r in hf_data.runs if r.get("state") in ("COMPLETED", "FAILED")]
        if terminal_runs:
            hf_data.latest_run = max(terminal_runs, key=lambda r: r.get("ended_at", ""))

        if hf_data.is_running or hf_data.runs:
            results[workflow_id] = hf_data

    return results
```

Replace `_fetch_live_hf_status` with:

```python
@ttl_cache(ttl=60)
def _fetch_hf_cost_history() -> dict[str, HFCostData]:
    """Fetch HF Jobs cost history from HF Hub repos. 60s TTL."""
    try:
        from huggingface_hub import HfApi

        api = HfApi()
        repos = _discover_hf_repos_from_cards()
        return _fetch_hf_cost_history_impl(api, repos)
    except Exception:
        logger.debug("HF cost history fetch failed", exc_info=True)
        return {}
```

- [ ] **Step 4: Build `task_key -> workflow_id` reverse lookup**

Add at module level (after `_cards` is loaded):

```python
def _build_task_key_to_wf_id() -> dict[str, str]:
    """Build reverse lookup from Databricks task_key to workflow card id."""
    mapping: dict[str, str] = {}
    for card_id, card in _cards.items():
        exec_cfg = card.get("execution") or {}
        ep = (exec_cfg.get("inference") or {}).get("entry_point", "")
        if ep:
            mapping[ep] = card_id
    return mapping

_task_key_to_wf_id: dict[str, str] = _build_task_key_to_wf_id()
```

Modify `_fetch_job_runs()` — re-key the result before returning:

```python
        logger.info("Fetched run data for %d task keys from Jobs API", len(runs))
        return {_task_key_to_wf_id.get(k, k): v for k, v in runs.items()}
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest src/tests/test_workflows_auto_refresh.py -v`
Expected: PASS

- [ ] **Step 6: Run quality gates**

Run: `uv run ruff check hf_taipy_app/src/state/workflows.py && uv run pyright hf_taipy_app/src/state/workflows.py`
Expected: No errors

---

### Task 5: App — rewrite `_build_table_data()` for unified lookup

**Files:**
- Modify: `hf_taipy_app/src/state/workflows.py`

- [ ] **Step 1: Update `_fetch_cold_costs()` to group by `workflow_id`**

Replace the query in `_fetch_cold_costs()`:

```python
@ttl_cache(ttl=3600)
def _fetch_cold_costs() -> pd.DataFrame:
    """30-day aggregated costs from fct_workflow_costs_synced (cold tier).

    Returns DataFrame with columns: workflow_id, task_key, total_cost_usd, total_dbu, run_count.
    """
    _empty = pd.DataFrame(columns=pd.Index(["workflow_id", "task_key", "total_cost_usd", "total_dbu", "run_count"]))
    try:
        tbl = t("fct_workflow_costs_synced")
        return execute_query(
            f"SELECT COALESCE(workflow_id, task_key) AS workflow_id, "  # noqa: S608
            f"  task_key, "
            f"  SUM(attributed_cost_usd) AS total_cost_usd, "
            f"  SUM(attributed_dbu) AS total_dbu, "
            f"  COUNT(DISTINCT job_run_id) AS run_count "
            f"FROM {tbl} "
            f"WHERE usage_date >= CURRENT_DATE - INTERVAL '30 days' "
            f"GROUP BY COALESCE(workflow_id, task_key), task_key "
            f"ORDER BY total_cost_usd DESC "
            f"LIMIT 100",
        )
    except Exception:
        logger.warning("Cold cost query failed — costs unavailable", exc_info=True)
        return _empty
```

- [ ] **Step 2: Rewrite `_build_table_data()` signature and cost/status logic**

Update the function signature to accept `hf_costs` instead of `live_hf`:

```python
def _build_table_data(
    cards: dict[str, dict[str, Any]],
    cold_costs: pd.DataFrame,
    job_runs: dict[str, dict[str, Any]],
    type_filter: str | None,
    runtime_filter: str | None = "All",
    freshness_filter: str | None = "All",
    hf_costs: dict[str, HFCostData] | None = None,
) -> pd.DataFrame:
```

Replace the cost lookup building block:

```python
    # Build cost lookups: workflow_id -> 30d USD and run count
    db_cost_lookup: dict[str, float] = {}
    db_run_count_lookup: dict[str, int] = {}
    if not cold_costs.empty:
        # Use workflow_id (COALESCE'd with task_key as fallback)
        grouped = cold_costs.groupby("workflow_id").agg(
            total_cost_usd=("total_cost_usd", "sum"),
            run_count=("run_count", "sum"),
        )
        db_cost_lookup = grouped["total_cost_usd"].apply(lambda x: float(x or 0)).to_dict()
        db_run_count_lookup = grouped["run_count"].apply(lambda x: int(x or 0)).to_dict()
        # Also index by task_key for fallback
        for _, row in cold_costs.iterrows():
            tk = row.get("task_key", "")
            wf = _task_key_to_wf_id.get(tk)
            if wf and wf not in db_cost_lookup:
                db_cost_lookup[wf] = float(row.get("total_cost_usd") or 0)
                db_run_count_lookup[wf] = int(row.get("run_count") or 0)
```

Replace the per-card cost + status block inside the loop:

```python
        # Cost: combine Databricks cold tier + HF Hub history
        db_cost = db_cost_lookup.get(card_id, 0.0)
        db_runs = db_run_count_lookup.get(card_id, 0)

        hf_data = (hf_costs or {}).get(card_id)
        hf_cost = sum(float(r.get("estimated_cost_usd") or 0) for r in (hf_data.runs if hf_data else []))
        hf_runs = len(hf_data.runs) if hf_data else 0

        total_cost = db_cost + hf_cost
        total_runs = db_runs + hf_runs
        if total_cost > 0:
            cost_val = f"${total_cost:7.2f}"
            avg_run_val = f"${total_cost / total_runs:7.2f}" if total_runs > 0 else "\u2014"
        else:
            cost_val = "\u2014"
            avg_run_val = "\u2014"

        # Last Run + Duration: best of Jobs API and HF history
        job_run = job_runs.get(card_id, {})
        db_last_run = job_run.get("last_run")
        db_duration = job_run.get("duration_seconds", 0)

        hf_latest = hf_data.latest_run if hf_data else None
        hf_last_run = pd.Timestamp(hf_latest["ended_at"]) if hf_latest and hf_latest.get("ended_at") else None
        hf_duration = int(hf_latest.get("duration_seconds", 0)) if hf_latest else 0

        # Pick whichever is more recent
        if db_last_run and hf_last_run:
            if hf_last_run > db_last_run:
                last_run_ts = hf_last_run
                duration_secs = hf_duration
            else:
                last_run_ts = db_last_run
                duration_secs = db_duration
        elif hf_last_run:
            last_run_ts = hf_last_run
            duration_secs = hf_duration
        else:
            last_run_ts = db_last_run
            duration_secs = db_duration

        last_run_str = "\u2014"
        duration_str = "\u2014"
        if last_run_ts is not None:
            last_run_str = last_run_ts.strftime("%Y-%m-%d %H:%M")
            if duration_secs > 0:
                mins, secs = divmod(duration_secs, 60)
                duration_str = f"{mins}m {secs}s" if mins else f"{secs}s"

        # Freshness
        freshness_str = "\u2014"
        sla_hours = (card.get("monitoring") or {}).get("freshness_sla_hours")
        if sla_hours and last_run_ts is not None:
            age_hours = (pd.Timestamp.now(tz="UTC") - last_run_ts).total_seconds() / 3600
            freshness_str = _classify_freshness(age_hours, sla_hours)

        # Status: RUNNING from either source, else most recent terminal state
        is_hf_running = hf_data.is_running if hf_data else False
        if is_hf_running:
            status_str = "RUNNING"
        elif job_run:
            run_state = job_run.get("state", "")
            if run_state in ("RUNNING", "PENDING"):
                status_str = "RUNNING"
            elif run_state in ("SUCCESS", "COMPLETED"):
                status_str = "COMPLETED"
            elif run_state in ("FAILED", "ERROR", "TIMEDOUT", "CANCELED"):
                status_str = "FAILED"
            elif run_state in ("SKIPPED", "DISABLED", "EXCLUDED"):
                status_str = "SKIPPED"
            else:
                status_str = "\u2014"
        elif hf_latest:
            hf_state = hf_latest.get("state", "")
            if hf_state == "COMPLETED":
                status_str = "COMPLETED"
            elif hf_state == "FAILED":
                status_str = "FAILED"
            elif hf_state == "SKIPPED":
                status_str = "SKIPPED"
            else:
                status_str = "\u2014"
        else:
            status_str = "\u2014"
```

- [ ] **Step 3: Update all callers of `_build_table_data` and `_fetch_live_hf_status`**

Search for all call sites of `_fetch_live_hf_status` and `_build_table_data` in `workflows.py`. Replace `_fetch_live_hf_status()` calls with `_fetch_hf_cost_history()` and pass as `hf_costs=` instead of `live_hf=`.

The primary call site is in the refresh callback. Update it:

```python
        hf_costs = _fetch_hf_cost_history()
        # ... pass to _build_table_data
        df = _build_table_data(
            _cards, cold_costs, job_runs, type_filter,
            runtime_filter=runtime_filter,
            freshness_filter=freshness_filter,
            hf_costs=hf_costs,
        )
```

- [ ] **Step 4: Remove old `_fetch_live_hf_status_impl` and `_fetch_live_hf_status`**

Delete the old functions. Keep `_discover_hf_repos_from_cards()` (still used by the new function).

- [ ] **Step 5: Run all tests**

Run: `uv run pytest src/tests/test_workflows_auto_refresh.py src/tests/test_taipy_workflows_styling.py -v`
Expected: PASS

- [ ] **Step 6: Run quality gates**

Run: `uv run ruff check hf_taipy_app/src/state/workflows.py && uv run pyright hf_taipy_app/src/state/workflows.py`
Expected: No errors

---

### Task 6: Terraform — daily cron job for `sync_hf_costs`

**Files:**
- Modify: `terraform/modules/workflows/main.tf`
- Modify: `terraform/environments/dev/main.tf`

- [ ] **Step 1: Add `sync_hf_costs` as a pre-task in the existing workflow**

In `terraform/modules/workflows/main.tf`, add a new task block before the existing tasks (inside the `databricks_job.data_ingestion` resource):

```hcl
  # ── Task: Sync HF Jobs costs to Delta (pre-task, runs before compute tasks) ─
  task {
    task_key = "sync_hf_costs"

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "sync-hf-costs"
      parameters   = ["--catalog", var.catalog_name, "--cards-dir", "/Workspace/Repos/luxury-lakehouse/workflow-cards"]
    }

    new_cluster {
      spark_version       = var.spark_version
      num_workers         = 0
      node_type_id        = var.node_type_id
      data_security_mode  = "SINGLE_USER"
    }
  }
```

- [ ] **Step 2: Add daily cron job resource**

In `terraform/environments/dev/main.tf`, add a new job resource:

```hcl
# ── Daily HF costs sync (catch-all backup) ────────────────────────────────
resource "databricks_job" "sync_hf_costs_daily" {
  name                = "sync-hf-costs-daily-${var.environment}"
  max_concurrent_runs = 1

  schedule {
    quartz_cron_expression = "0 0 6 * * ?"
    timezone_id            = "UTC"
    pause_status           = var.environment == "dev" ? "PAUSED" : "UNPAUSED"
  }

  task {
    task_key = "sync_hf_costs"

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "sync-hf-costs"
      parameters   = ["--catalog", var.catalog_name, "--cards-dir", "/Workspace/Repos/luxury-lakehouse/workflow-cards"]
    }

    new_cluster {
      spark_version       = var.spark_version
      num_workers         = 0
      node_type_id        = var.node_type_id
      data_security_mode  = "SINGLE_USER"
    }
  }
}
```

- [ ] **Step 3: Add `sync-hf-costs` entry point to `pyproject.toml`**

Verify the entry point exists. If not, add:

```toml
sync-hf-costs = "scripts.sync_hf_costs:main"
```

- [ ] **Step 4: Validate Terraform syntax**

Run: `cd terraform/environments/dev && terraform validate`
Expected: Configuration is valid

---

### Task 7: Integration test — run EPV job and verify display

**Files:** No new files — validation only.

- [ ] **Step 1: Run the EPV job to generate `_cost_history/` data**

```bash
hf jobs uv run scripts/compute_epv_transition_hf.py --flavor cpu-basic --timeout 30m --secrets HF_TOKEN=$HF_TOKEN
```

Expected: Job completes, `_cost_history/{job_id}.json` appears in `luxury-lakehouse/obso-trained-grids` repo.

- [ ] **Step 2: Verify `_cost_history/` file exists on HF Hub**

```python
from huggingface_hub import HfApi
api = HfApi()
items = list(api.list_repo_tree("luxury-lakehouse/obso-trained-grids", repo_type="dataset", path_in_repo="_cost_history"))
for item in items:
    print(item.rfilename, getattr(item, "size", "?"))
```

Expected: At least one `.json` file in `_cost_history/`

- [ ] **Step 3: Start local Taipy server and verify Workflows page**

Navigate to the Workflows page. Verify that `wf-epv-reachability` shows:
- Status: COMPLETED (not "—")
- Last Run: timestamp of the job run
- Last Duration: ~30-40s
- Cost (30d): ~$0.00 (CPU basic rate)

- [ ] **Step 4: Run full quality gate suite**

```bash
uv run ruff check src/ scripts/ hf_taipy_app/src/ && uv run ruff format --check src/ scripts/ hf_taipy_app/src/ && uv run pyright src/ scripts/ && uv run pytest src/tests/ -v
```

Expected: All pass with zero violations
