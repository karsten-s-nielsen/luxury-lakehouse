# HF Buckets & Workflow Auto-Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate demo data to HF Storage Bucket, bridge HF Jobs costs into the Taipy Workflows page, and add 2-minute auto-refresh with visual state indicators.

**Architecture:** D27 creates a `luxury-lakehouse/demo-data` bucket for parquet files (wheel stays in model repo — pip can't install from buckets). D34 adds a cost bridge script that reads `_workflow_cost.json` from HF Hub repos and MERGEs into `workflow_cost_live` Delta, plus a live HF Hub status reader and auto-refresh timer in the Taipy app.

**Tech Stack:** `huggingface_hub >= 1.5.0` (bucket API + fsspec), Taipy 4.1.1, `threading.Timer`, Databricks SQL, PySpark Delta MERGE.

**Spec:** `docs/superpowers/specs/2026-03-26-hf-buckets-auto-refresh-design.md`

**Scope revision:** HF Buckets do not expose HTTPS download URLs for pip. The wheel stays in the `luxury-lakehouse/build-artifacts` model repo. Only demo data moves to a bucket. See spec for original design; this plan reflects the revised scope.

---

## File Map

### New Files

| File | Responsibility |
|------|---------------|
| `scripts/setup_hf_buckets.py` | Idempotent bucket provisioning — creates `luxury-lakehouse/demo-data`, uploads parquet files |
| `scripts/sync_hf_costs.py` | Cost bridge — reads `_workflow_cost.json` from HF Hub repos, MERGEs into `workflow_cost_live` Delta |
| `src/tests/test_setup_hf_buckets.py` | Unit tests for bucket provisioning |
| `src/tests/test_sync_hf_costs.py` | Unit tests for cost bridge |
| `src/tests/test_workflows_auto_refresh.py` | Unit tests for live status, auto-refresh timer, status badges |
| `workflow-cards/wf-sync-hf-costs.yaml` | Workflow card for the cost bridge Databricks task |

### Modified Files

| File | Change |
|------|--------|
| `pyproject.toml` | Bump `huggingface_hub>=1.5.0` |
| `scripts/*_hf.py` (8 files) | Bump `huggingface-hub>=1.5.0` in PEP 723 headers |
| `demo_space/app.py` | Replace local parquet reads with `hf://buckets/` fsspec paths |
| `hf_taipy_app/requirements.txt` | Add `huggingface-hub>=1.5.0` |
| `hf_taipy_app/src/state/workflows.py` | Live HF status fetch, auto-refresh timer, Status column, TTL adjustment |
| `hf_taipy_app/src/pages/workflows.py` | Add Status column to table ContentBlock |

### Removed Files

| File | Reason |
|------|--------|
| `demo_space/data/*.parquet` (6 files) | Migrated to `luxury-lakehouse/demo-data` bucket |

---

## Task 1: Bump `huggingface_hub` and PEP 723 headers

**Files:**
- Modify: `pyproject.toml:52-55` (embeddings extra)

- [ ] **Step 1: Bump huggingface_hub version in embeddings extra**

In `pyproject.toml`, change the embeddings extra:

```toml
embeddings = [
    "gensim>=4.3.0",
    "huggingface_hub>=1.5.0",
]
```

Note: No entry points added for the new scripts. `scripts/` is not a wheel package — existing scripts (`deploy_wheel.py`, `deploy_taipy.py`) are invoked directly via `python scripts/<name>.py`, not through entry points.

- [ ] **Step 2: Bump PEP 723 headers in all 8 HF Jobs scripts**

In each of these files, find `huggingface-hub>=0.25.0` and replace with `huggingface-hub>=1.5.0`:

- `scripts/compute_xt_grid_hf.py`
- `scripts/train_xg_model_hf.py`
- `scripts/train_xg_v2_hf.py`
- `scripts/train_vaep_model_hf.py`
- `scripts/compute_obso_hf.py`
- `scripts/compute_space_creation_hf.py`
- `scripts/compute_epv_transition_hf.py`
- `scripts/publish_xg_shots_hf.py`

- [ ] **Step 3: Run uv sync to update lockfile**

Run: `uv sync --extra embeddings`
Expected: lockfile updated, `huggingface_hub >= 1.5.0` resolved

---

## Task 2: Create `scripts/setup_hf_buckets.py` — bucket provisioning

**Files:**
- Create: `scripts/setup_hf_buckets.py`
- Test: `src/tests/test_setup_hf_buckets.py`

- [ ] **Step 1: Write the failing test**

Create `src/tests/test_setup_hf_buckets.py`:

```python
"""Tests for HF Bucket provisioning script."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest


class TestCreateDemoBucket:
    """Test bucket creation logic."""

    def test_creates_bucket_when_not_exists(self) -> None:
        from scripts.setup_hf_buckets import create_demo_bucket

        api = MagicMock()
        create_demo_bucket(api)
        api.create_bucket.assert_called_once_with(
            "luxury-lakehouse/demo-data",
            private=False,
            exist_ok=True,
        )

    def test_exist_ok_prevents_error_on_duplicate(self) -> None:
        from scripts.setup_hf_buckets import create_demo_bucket

        api = MagicMock()
        # exist_ok=True means no error even if bucket exists
        create_demo_bucket(api)
        _, kwargs = api.create_bucket.call_args
        assert kwargs.get("exist_ok") is True or api.create_bucket.call_args[1].get("exist_ok") is True


class TestUploadDemoData:
    """Test parquet upload logic."""

    def test_uploads_all_six_parquet_files(self, tmp_path: Path) -> None:
        from scripts.setup_hf_buckets import DEMO_FILES, upload_demo_data

        # Create fake parquet files
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        for name in DEMO_FILES:
            (data_dir / name).write_bytes(b"fake-parquet")

        api = MagicMock()
        upload_demo_data(api, data_dir)

        api.batch_bucket_files.assert_called_once()
        call_args = api.batch_bucket_files.call_args
        assert call_args[1]["bucket_id"] == "luxury-lakehouse/demo-data"
        add_list = call_args[1]["add"]
        assert len(add_list) == 6
        remote_paths = {item[1] for item in add_list}
        assert remote_paths == set(DEMO_FILES)

    def test_skips_missing_files(self, tmp_path: Path) -> None:
        from scripts.setup_hf_buckets import upload_demo_data

        # Empty dir — no parquet files
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        api = MagicMock()
        upload_demo_data(api, data_dir)

        api.batch_bucket_files.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_setup_hf_buckets.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'scripts.setup_hf_buckets'`

- [ ] **Step 3: Write the implementation**

Create `scripts/setup_hf_buckets.py`:

```python
#!/usr/bin/env python3
"""Idempotent HF Bucket provisioning — creates demo-data bucket and uploads parquet files.

Usage:
    python scripts/setup_hf_buckets.py [--data-dir demo_space/data]

Requires HF_TOKEN with write access to luxury-lakehouse org.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from huggingface_hub import HfApi

logger = logging.getLogger(__name__)

BUCKET_ID = "luxury-lakehouse/demo-data"

DEMO_FILES = [
    "career_embeddings.parquet",
    "sample_shots.parquet",
    "sample_passes.parquet",
    "sample_tracking.parquet",
    "defcon_pressure.parquet",
    "sample_pausa.parquet",
]


def create_demo_bucket(api: HfApi) -> None:
    """Create the demo-data bucket if it doesn't exist."""
    logger.info("Creating bucket %s (exist_ok=True)", BUCKET_ID)
    api.create_bucket(BUCKET_ID, private=False, exist_ok=True)
    logger.info("Bucket %s ready", BUCKET_ID)


def upload_demo_data(api: HfApi, data_dir: Path) -> None:
    """Upload all demo parquet files to the bucket."""
    add_list: list[tuple[str, str]] = []
    for name in DEMO_FILES:
        path = data_dir / name
        if path.exists():
            add_list.append((str(path), name))
            logger.info("Queued %s (%d bytes)", name, path.stat().st_size)
        else:
            logger.warning("Skipping %s — file not found at %s", name, path)

    if not add_list:
        logger.warning("No files to upload — data_dir may be empty")
        return

    logger.info("Uploading %d files to %s", len(add_list), BUCKET_ID)
    api.batch_bucket_files(bucket_id=BUCKET_ID, add=add_list)
    logger.info("Upload complete — %d files in %s", len(add_list), BUCKET_ID)


def main() -> None:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(description="Provision HF demo-data bucket")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("demo_space/data"),
        help="Directory containing demo parquet files (default: demo_space/data)",
    )
    args = parser.parse_args()

    if not args.data_dir.is_dir():
        logger.error("Data directory not found: %s", args.data_dir)
        sys.exit(1)

    api = HfApi()
    create_demo_bucket(api)
    upload_demo_data(api, args.data_dir)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_setup_hf_buckets.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Run ruff + pyright**

Run: `uv run ruff check scripts/setup_hf_buckets.py && uv run ruff format --check scripts/setup_hf_buckets.py`
Expected: PASS

---

## Task 3: Update `demo_space/app.py` to read from bucket

**Files:**
- Modify: `demo_space/app.py:23,30-43`

- [ ] **Step 1: Replace local parquet helper with fsspec reads**

In `demo_space/app.py`, replace the `DATA_DIR` constant and `_load_parquet` helper (lines 23, 30-35) with:

```python
_BUCKET = "hf://buckets/luxury-lakehouse/demo-data"


def _load_parquet(name: str) -> pd.DataFrame:
    """Load a Parquet file from the HF demo-data bucket, returning empty DataFrame if missing."""
    try:
        return pd.read_parquet(f"{_BUCKET}/{name}")
    except Exception:
        return pd.DataFrame()
```

The six call sites (lines 38-43) remain unchanged — they already call `_load_parquet("filename.parquet")`.

- [ ] **Step 2: Remove the DATA_DIR import if Path is no longer used**

Check if `Path` is still used elsewhere in the file. If not, remove the `from pathlib import Path` import.

- [ ] **Step 3: Delete the local parquet files**

Remove all 6 files from `demo_space/data/`:

```bash
rm demo_space/data/career_embeddings.parquet
rm demo_space/data/sample_shots.parquet
rm demo_space/data/sample_passes.parquet
rm demo_space/data/sample_tracking.parquet
rm demo_space/data/defcon_pressure.parquet
rm demo_space/data/sample_pausa.parquet
```

If the `data/` directory is now empty, either remove it or add a `.gitkeep` with a comment:

```
# Demo data lives in HF Bucket: luxury-lakehouse/demo-data
# Run: python scripts/setup_hf_buckets.py to provision
```

- [ ] **Step 4: Run ruff on demo_space/app.py**

Run: `uv run ruff check demo_space/app.py && uv run ruff format --check demo_space/app.py`
Expected: PASS

---

## Task 4: Create `scripts/sync_hf_costs.py` — cost bridge

**Files:**
- Create: `scripts/sync_hf_costs.py`
- Test: `src/tests/test_sync_hf_costs.py`

- [ ] **Step 1: Write the failing tests**

Create `src/tests/test_sync_hf_costs.py`:

```python
"""Tests for HF Jobs cost bridge script."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestDiscoverHfRepos:
    """Test workflow card parsing for HF Jobs repo discovery."""

    def test_finds_dataset_repos(self, tmp_path: Path) -> None:
        from scripts.sync_hf_costs import discover_hf_repos

        card = {
            "id": "wf-xt-grids",
            "execution": {"training": {"runtime": "hf-jobs", "script": "scripts/compute_xt_grid_hf.py"}},
            "outputs": {"datasets": [{"id": "luxury-lakehouse/expected-threat-grids", "destination": "huggingface"}]},
        }
        card_path = tmp_path / "wf-xt-grids.yaml"
        import yaml
        card_path.write_text(yaml.dump(card))

        repos = discover_hf_repos(tmp_path)
        assert ("luxury-lakehouse/expected-threat-grids", "dataset", "wf-xt-grids") in repos

    def test_finds_model_repos(self, tmp_path: Path) -> None:
        from scripts.sync_hf_costs import discover_hf_repos

        card = {
            "id": "wf-xg-v2",
            "execution": {"training": {"runtime": "hf-jobs", "script": "scripts/train_xg_v2_hf.py"}},
            "outputs": {"models": [{"id": "luxury-lakehouse/xg-model-v2", "destination": "huggingface"}]},
        }
        card_path = tmp_path / "wf-xg-v2.yaml"
        import yaml
        card_path.write_text(yaml.dump(card))

        repos = discover_hf_repos(tmp_path)
        assert ("luxury-lakehouse/xg-model-v2", "model", "wf-xg-v2") in repos

    def test_skips_non_hf_jobs_cards(self, tmp_path: Path) -> None:
        from scripts.sync_hf_costs import discover_hf_repos

        card = {
            "id": "wf-line-breaking",
            "execution": {"inference": {"runtime": "databricks-workflow", "entry_point": "compute_line_breaking"}},
            "outputs": {"tables": [{"id": "bronze.line_breaking_passes", "destination": "delta-table"}]},
        }
        import yaml
        (tmp_path / "wf-line-breaking.yaml").write_text(yaml.dump(card))

        repos = discover_hf_repos(tmp_path)
        assert len(repos) == 0


class TestFetchCostJson:
    """Test _workflow_cost.json download from HF Hub."""

    def test_returns_parsed_json_on_success(self, tmp_path: Path) -> None:
        from scripts.sync_hf_costs import fetch_cost_json

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
        from scripts.sync_hf_costs import fetch_cost_json

        api = MagicMock()
        from huggingface_hub.errors import EntryNotFoundError
        api.hf_hub_download.side_effect = EntryNotFoundError("not found")

        result = fetch_cost_json(api, "luxury-lakehouse/fake-repo", "dataset")
        assert result is None

    def test_returns_none_on_network_error(self) -> None:
        from scripts.sync_hf_costs import fetch_cost_json

        api = MagicMock()
        api.hf_hub_download.side_effect = ConnectionError("timeout")

        result = fetch_cost_json(api, "luxury-lakehouse/fake-repo", "dataset")
        assert result is None


class TestMapToDeltaSchema:
    """Test JSON -> Delta schema mapping."""

    def test_maps_completed_record(self) -> None:
        from scripts.sync_hf_costs import map_to_delta_schema

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
        assert row["task_key"] == "compute_xt_grid"
        assert row["run_id"] == "hf-job-abc123"
        assert row["job_run_id"] is None
        assert row["hf_job_id"] == "job-abc123"

    def test_maps_running_record_with_nulls(self) -> None:
        from scripts.sync_hf_costs import map_to_delta_schema

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_sync_hf_costs.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'scripts.sync_hf_costs'`

- [ ] **Step 3: Write the implementation**

Create `scripts/sync_hf_costs.py`:

```python
#!/usr/bin/env python3
"""Cost bridge — reads _workflow_cost.json from HF Hub repos and MERGEs into workflow_cost_live.

Parses workflow-cards/*.yaml to discover HF Jobs repos. Designed to run as a
Databricks scheduled task every 15 minutes.

Usage:
    python scripts/sync_hf_costs.py --catalog soccer_analytics [--cards-dir workflow-cards]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from huggingface_hub import HfApi

logger = logging.getLogger(__name__)

_ID_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def discover_hf_repos(cards_dir: Path) -> list[tuple[str, str, str]]:
    """Parse workflow cards to find HF Jobs repos that may contain _workflow_cost.json.

    Returns list of (repo_id, repo_type, workflow_id) tuples.
    """
    repos: list[tuple[str, str, str]] = []
    for card_path in sorted(cards_dir.glob("wf-*.yaml")):
        try:
            card = yaml.safe_load(card_path.read_text())
        except Exception:
            logger.warning("Failed to parse %s", card_path.name, exc_info=True)
            continue

        if not card or not isinstance(card, dict):
            continue

        workflow_id = card.get("id", "")
        execution = card.get("execution") or {}

        # Check if any execution phase uses hf-jobs
        has_hf_jobs = False
        for phase in ("training", "inference"):
            phase_cfg = execution.get(phase) or {}
            rt = (phase_cfg.get("runtime") or "").lower().replace("_", "-")
            if rt == "hf-jobs":
                has_hf_jobs = True
                break

        if not has_hf_jobs:
            continue

        outputs = card.get("outputs") or {}

        # Check datasets first (preferred — most HF Jobs write to dataset repos)
        for ds in outputs.get("datasets") or []:
            if ds.get("destination") == "huggingface" and ds.get("id"):
                repos.append((ds["id"], "dataset", workflow_id))

        # Check models (some HF Jobs training writes to model repos)
        for model in outputs.get("models") or []:
            if model.get("destination") == "huggingface" and model.get("id"):
                repos.append((model["id"], "model", workflow_id))

    logger.info("Discovered %d HF repos from %s", len(repos), cards_dir)
    return repos


def fetch_cost_json(api: HfApi, repo_id: str, repo_type: str) -> dict[str, Any] | None:
    """Download _workflow_cost.json from an HF Hub repo. Returns None on failure."""
    try:
        local_path = api.hf_hub_download(
            repo_id=repo_id,
            filename="_workflow_cost.json",
            repo_type=repo_type,
        )
        with open(local_path) as f:
            return json.load(f)
    except Exception:
        logger.debug("No _workflow_cost.json in %s/%s", repo_type, repo_id, exc_info=True)
        return None


def map_to_delta_schema(cost_data: dict[str, Any], task_key: str) -> dict[str, Any]:
    """Map HF Jobs cost JSON to workflow_cost_live Delta schema."""
    hf_job_id = cost_data.get("hf_job_id")
    return {
        "workflow_id": cost_data.get("workflow_id"),
        "phase": cost_data.get("phase"),
        "run_id": f"hf-{hf_job_id}" if hf_job_id else None,
        "runtime": "hf_jobs",
        "job_run_id": None,
        "task_key": task_key,
        "hf_job_id": hf_job_id,
        "state": cost_data.get("state"),
        "started_at": cost_data.get("started_at"),
        "ended_at": cost_data.get("ended_at"),
        "duration_seconds": cost_data.get("duration_seconds"),
        "row_count": cost_data.get("row_count"),
        "rate_usd_per_hour": cost_data.get("rate_usd_per_hour"),
        "estimated_cost_usd": cost_data.get("estimated_cost_usd"),
        "cost_source": "hf_hub_sync",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _resolve_task_key(card: dict[str, Any]) -> str:
    """Extract the task_key from a workflow card's execution config.

    Prefers execution.training.script stem (e.g., 'train_xg_v2_hf' from
    'scripts/train_xg_v2_hf.py'), falls back to execution.inference.entry_point.
    """
    execution = card.get("execution") or {}
    for phase in ("training", "inference"):
        phase_cfg = execution.get(phase) or {}
        script = phase_cfg.get("script")
        if script:
            return Path(script).stem
        entry_point = phase_cfg.get("entry_point")
        if entry_point:
            return entry_point
    return ""


def sync_costs(
    catalog: str,
    cards_dir: Path,
) -> int:
    """Main sync logic. Returns number of records synced."""
    if not _ID_PATTERN.match(catalog):
        msg = f"Invalid catalog name: {catalog}"
        raise ValueError(msg)

    api = HfApi()
    repos = discover_hf_repos(cards_dir)

    if not repos:
        logger.info("No HF Jobs repos found — nothing to sync")
        return 0

    # Load workflow cards for task_key resolution
    cards: dict[str, dict[str, Any]] = {}
    for card_path in cards_dir.glob("wf-*.yaml"):
        try:
            card = yaml.safe_load(card_path.read_text())
            if card and isinstance(card, dict) and card.get("id"):
                cards[card["id"]] = card
        except Exception:
            pass

    rows: list[dict[str, Any]] = []
    for repo_id, repo_type, workflow_id in repos:
        cost_data = fetch_cost_json(api, repo_id, repo_type)
        if cost_data is None:
            continue

        card = cards.get(workflow_id, {})
        task_key = _resolve_task_key(card)
        row = map_to_delta_schema(cost_data, task_key)
        if row["run_id"]:
            rows.append(row)
            logger.info("Fetched cost record: %s %s -> %s", workflow_id, cost_data.get("state"), row["run_id"])

    if not rows:
        logger.info("No cost records to sync")
        return 0

    # MERGE into workflow_cost_live via PySpark
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    target_table = f"{catalog}.observability.workflow_cost_live"
    source_df = spark.createDataFrame(rows)

    from delta.tables import DeltaTable

    dt = DeltaTable.forName(spark, target_table)
    (
        dt.alias("target")
        .merge(source_df.alias("source"), "target.run_id = source.run_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info("Synced %d HF Jobs cost records into %s", len(rows), target_table)
    return len(rows)


def main() -> None:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(description="Sync HF Jobs costs to Delta")
    parser.add_argument("--catalog", default="soccer_analytics", help="Unity Catalog name")
    parser.add_argument("--cards-dir", type=Path, default=Path("workflow-cards"), help="Workflow cards directory")
    args = parser.parse_args()

    count = sync_costs(args.catalog, args.cards_dir)
    logger.info("Done — %d records synced", count)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_sync_hf_costs.py -v`
Expected: All 8 tests PASS

- [ ] **Step 5: Run ruff + pyright**

Run: `uv run ruff check scripts/sync_hf_costs.py && uv run ruff format --check scripts/sync_hf_costs.py && uv run pyright scripts/sync_hf_costs.py`
Expected: PASS

---

## Task 5: Create `workflow-cards/wf-sync-hf-costs.yaml`

**Files:**
- Create: `workflow-cards/wf-sync-hf-costs.yaml`

- [ ] **Step 1: Create the workflow card**

Create `workflow-cards/wf-sync-hf-costs.yaml`:

```yaml
---
name: HF Jobs Cost Bridge
id: wf-sync-hf-costs
version: "1.0"
status: production
type: infrastructure
domain: observability
owners:
  - karsten
tags:
  - cost-tracking
  - observability
  - hf-jobs

description: >
  Reads _workflow_cost.json from HF Hub repos (written by HFJobsCostRecorder
  during HF Jobs runs) and MERGEs into workflow_cost_live Delta table.
  Bridges the gap between HF Jobs cost recording and the Taipy Workflows
  page display.

inputs:
  datasets:
    - id: "_workflow_cost.json in HF Jobs output repos"
      source: huggingface
      description: "Cost metadata written by HFJobsCostRecorder during HF Jobs execution"

outputs:
  tables:
    - id: "{catalog}.observability.workflow_cost_live"
      destination: delta-table
      description: "Unified cost tracking table (Databricks + HF Jobs)"

execution:
  inference:
    trigger: scheduled
    runtime: databricks-workflow
    entry_point: sync_hf_costs
    module: scripts.sync_hf_costs
    schedule: "every 15 minutes"
    timeout: "300s"

cost:
  inference:
    runtime: databricks
    sku: "jobs_serverless_compute_run_dbus"
    typical_dbu: 5
    typical_cost_usd: 0.35
    notes: "Minimal compute — reads JSON files from HF Hub, single MERGE"

monitoring:
  freshness_sla_hours: 1

dependencies:
  - wf-vaep
  - wf-xg-v1
  - wf-xg-v2
  - wf-xt-grids
  - wf-obso-pausa
  - wf-space-creation
  - wf-epv-reachability

links:
  source_code:
    - "scripts/sync_hf_costs.py"
```

- [ ] **Step 2: Validate the workflow card**

Run: `uv run validate_workflow_cards`
Expected: PASS — all cards valid including the new one

---

## Task 6: Add `huggingface-hub` to Taipy requirements and add live HF status fetch

**Files:**
- Modify: `hf_taipy_app/requirements.txt`
- Modify: `hf_taipy_app/src/state/workflows.py` (imports, new function, TTL change)
- Test: `src/tests/test_workflows_auto_refresh.py`

- [ ] **Step 1: Add huggingface-hub to Taipy requirements**

Add to `hf_taipy_app/requirements.txt`:

```
huggingface-hub>=1.5.0
```

- [ ] **Step 2: Write the failing test for live HF status**

Create `src/tests/test_workflows_auto_refresh.py`:

```python
"""Tests for Workflows page auto-refresh, live HF status, and status badges."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestFetchLiveHfStatus:
    """Test _fetch_live_hf_status function."""

    def test_returns_running_records_only(self, tmp_path: Path) -> None:
        # Import after patches are in place
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

        from state.workflows import _fetch_live_hf_status_impl

        result = _fetch_live_hf_status_impl(
            mock_api,
            [("luxury-lakehouse/expected-threat-grids", "dataset", "wf-xt-grids")],
        )
        assert len(result) == 1
        assert result[0]["state"] == "RUNNING"
        assert result[0]["workflow_id"] == "wf-xt-grids"

    def test_skips_completed_records(self, tmp_path: Path) -> None:
        cost_data = {
            "workflow_id": "wf-xt-grids",
            "state": "COMPLETED",
            "hf_job_id": "job-abc",
        }
        cost_file = tmp_path / "cost.json"
        cost_file.write_text(json.dumps(cost_data))

        mock_api = MagicMock()
        mock_api.hf_hub_download.return_value = str(cost_file)

        from state.workflows import _fetch_live_hf_status_impl

        result = _fetch_live_hf_status_impl(
            mock_api,
            [("luxury-lakehouse/expected-threat-grids", "dataset", "wf-xt-grids")],
        )
        assert len(result) == 0

    def test_returns_empty_on_network_error(self) -> None:
        mock_api = MagicMock()
        mock_api.hf_hub_download.side_effect = ConnectionError("timeout")

        from state.workflows import _fetch_live_hf_status_impl

        result = _fetch_live_hf_status_impl(
            mock_api,
            [("luxury-lakehouse/fake-repo", "dataset", "wf-fake")],
        )
        assert len(result) == 0


class TestStyleStatus:
    """Test wf_style_status callback."""

    def test_running_returns_running_class(self) -> None:
        from state.workflows import wf_style_status

        assert wf_style_status("RUNNING") == "ll-cell-status-running"

    def test_completed_returns_completed_class(self) -> None:
        from state.workflows import wf_style_status

        assert wf_style_status("COMPLETED") == "ll-cell-status-completed"

    def test_failed_returns_failed_class(self) -> None:
        from state.workflows import wf_style_status

        assert wf_style_status("FAILED") == "ll-cell-status-failed"

    def test_skipped_returns_skipped_class(self) -> None:
        from state.workflows import wf_style_status

        assert wf_style_status("SKIPPED") == "ll-cell-status-skipped"

    def test_stale_returns_stale_class(self) -> None:
        from state.workflows import wf_style_status

        assert wf_style_status("STALE") == "ll-cell-status-stale"

    def test_unknown_returns_empty(self) -> None:
        from state.workflows import wf_style_status

        assert wf_style_status("UNKNOWN") == ""
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_workflows_auto_refresh.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 4: Add live HF status fetch to workflows.py**

In `hf_taipy_app/src/state/workflows.py`, add to imports section (after existing imports):

```python
import json as _json
from pathlib import Path as _Path
```

Add after `_fetch_job_runs()` function (around line 790):

```python
def _discover_hf_repos_from_cards() -> list[tuple[str, str, str]]:
    """Parse workflow cards to find HF Jobs repos for live status checking.

    Returns list of (repo_id, repo_type, workflow_id) tuples.
    """
    repos: list[tuple[str, str, str]] = []
    for card in _cards.values():
        execution = card.get("execution") or {}
        has_hf_jobs = False
        for phase in ("training", "inference"):
            phase_cfg = execution.get(phase) or {}
            rt = (phase_cfg.get("runtime") or "").lower().replace("_", "-")
            if rt == "hf-jobs":
                has_hf_jobs = True
                break
        if not has_hf_jobs:
            continue
        outputs = card.get("outputs") or {}
        for ds in outputs.get("datasets") or []:
            if ds.get("destination") == "huggingface" and ds.get("id"):
                repos.append((ds["id"], "dataset", card.get("id", "")))
        for model in outputs.get("models") or []:
            if model.get("destination") == "huggingface" and model.get("id"):
                repos.append((model["id"], "model", card.get("id", "")))
    return repos


def _fetch_live_hf_status_impl(
    api: Any,
    repos: list[tuple[str, str, str]],
) -> list[dict[str, Any]]:
    """Read _workflow_cost.json from HF Hub repos, return RUNNING records only.

    Separated from the cached wrapper for testability.
    """
    running: list[dict[str, Any]] = []
    for repo_id, repo_type, workflow_id in repos:
        try:
            local_path = api.hf_hub_download(
                repo_id=repo_id,
                filename="_workflow_cost.json",
                repo_type=repo_type,
            )
            with open(local_path) as f:
                data = _json.load(f)
            if isinstance(data, dict) and data.get("state") == "RUNNING":
                running.append(data)
        except Exception:
            logger.debug("Live status check failed for %s/%s", repo_type, repo_id, exc_info=True)
    return running


@ttl_cache(ttl=60)
def _fetch_live_hf_status() -> list[dict[str, Any]]:
    """Fetch live RUNNING status from HF Hub repos. 60s TTL."""
    try:
        from huggingface_hub import HfApi

        api = HfApi()
        repos = _discover_hf_repos_from_cards()
        return _fetch_live_hf_status_impl(api, repos)
    except Exception:
        logger.debug("Live HF status fetch failed", exc_info=True)
        return []
```

- [ ] **Step 5: Change warm tier TTL from 1800s to 120s**

In `hf_taipy_app/src/state/workflows.py`, change:

```python
@ttl_cache(ttl=1800)
def _fetch_warm_costs() -> pd.DataFrame:
```

to:

```python
@ttl_cache(ttl=120)
def _fetch_warm_costs() -> pd.DataFrame:
```

Also change `_fetch_job_runs` TTL:

```python
@ttl_cache(ttl=1800)
def _fetch_job_runs() -> dict[str, dict[str, Any]]:
```

to:

```python
@ttl_cache(ttl=120)
def _fetch_job_runs() -> dict[str, dict[str, Any]]:
```

- [ ] **Step 6: Add wf_style_status callback**

In `hf_taipy_app/src/state/workflows.py`, add after the existing `wf_style_freshness` callback:

```python
_STATUS_CLASSES: dict[str, str] = {
    "RUNNING": "ll-cell-status-running",
    "COMPLETED": "ll-cell-status-completed",
    "FAILED": "ll-cell-status-failed",
    "SKIPPED": "ll-cell-status-skipped",
    "STALE": "ll-cell-status-stale",
}


def wf_style_status(state: Any, value: Any, *_args: Any) -> str:
    """Table cell class for Status column."""
    return _STATUS_CLASSES.get(str(value), "")
```

Note: Check the existing `wf_style_freshness` signature pattern to match the exact Taipy callback signature (it may be `(state, value, index, row, col)` or just `(value)` depending on how the existing callbacks are defined). Match the same pattern.

- [ ] **Step 7: Add wf_style_status and new state vars to __all__**

Add to the `__all__` list:

```python
"wf_style_status",
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_workflows_auto_refresh.py -v`
Expected: All 9 tests PASS

---

## Task 7: Add Status column to table and integrate live status into stats

**Files:**
- Modify: `hf_taipy_app/src/state/workflows.py` (`_WF_TABLE_COLS`, `_build_table_data`, `_compute_stats`)
- Modify: `hf_taipy_app/src/pages/workflows.py` (table ContentBlock)

- [ ] **Step 1: Add "Status" to table columns**

In `hf_taipy_app/src/state/workflows.py`, modify `_WF_TABLE_COLS`:

```python
_WF_TABLE_COLS = [
    "Name",
    "Type",
    "Runtime",
    "Status",
    "Last Run",
    "Last Duration",
    "Cost (30d)",
    "Avg/Run",
    "Freshness",
]
```

Update the default `wf_table_data` declaration to match.

- [ ] **Step 2: Add Status to table row construction in _build_table_data**

Modify `_build_table_data()` to accept the live HF status data and the jobs dict. For each row, determine status:

1. Check live HF status — if a RUNNING record exists for this workflow_id, status = "RUNNING"
2. Check Databricks Jobs API — if a run exists with `state` in ("RUNNING", "PENDING"), status = "RUNNING"
3. If last run was COMPLETED/SUCCEEDED, status = "COMPLETED"
4. If last run was FAILED/ERROR, status = "FAILED"
5. If last run was SKIPPED, status = "SKIPPED"
6. If no run in last 30 days, status = "STALE"

Add `"Status": status_str` to each row dict.

- [ ] **Step 3: Update wf_refresh to pass live status through the pipeline**

In `wf_refresh()`, add the live status fetch:

```python
# Query costs + job runs + live HF status
cold = _fetch_cold_costs()
warm = _fetch_warm_costs()
jobs = _fetch_job_runs()
live_hf = _fetch_live_hf_status()
```

Pass `live_hf` to `_build_table_data()` and `_compute_stats()`.

- [ ] **Step 4: Update _compute_stats to show combined running count**

In `_compute_stats()`, after the existing run volume calculation, add:

```python
# Count currently running jobs (both runtimes)
running_db = sum(1 for r in jobs.values() if r.get("state") == "RUNNING")
running_hf = len(live_hf)
total_running = running_db + running_hf
if total_running > 0:
    state.wf_run_volume_detail = f"{total_running} running now · " + state.wf_run_volume_detail
```

Also update freshness: a workflow with a RUNNING job should show as fresh:

```python
# In the freshness loop, check live_hf before classifying as stale
live_wf_ids = {r.get("workflow_id") for r in live_hf}
# ... if card_id in live_wf_ids: fresh_count += 1; continue
```

- [ ] **Step 5: Add Status to page config table ContentBlock**

In `hf_taipy_app/src/pages/workflows.py`, modify the table ContentBlock:

```python
ContentBlock(
    "table",
    "wf_table_data",
    table_page_size=20,
    table_cell_class_name={
        "Type":      "wf_style_type",
        "Runtime":   "wf_style_runtime",
        "Status":    "wf_style_status",
        "Freshness": "wf_style_freshness",
    },
)
```

- [ ] **Step 6: Add CSS for status badges**

In the Taipy app's CSS file (find the existing stylesheet that defines `ll-cell-rt-db`, `ll-cell-rt-hf`, etc.), add:

```css
.ll-cell-status-running {
    color: #58a6ff;
    font-weight: 600;
    animation: ll-pulse 2s ease-in-out infinite;
}
.ll-cell-status-completed { color: #3fb950; font-weight: 600; }
.ll-cell-status-failed    { color: #f85149; font-weight: 600; }
.ll-cell-status-skipped   { color: #8b949e; }
.ll-cell-status-stale     { color: #d29922; font-weight: 600; }

@keyframes ll-pulse {
    0%, 100% { opacity: 1; }
    50%      { opacity: 0.5; }
}
```

- [ ] **Step 7: Run all workflow tests**

Run: `uv run pytest src/tests/test_workflows_auto_refresh.py src/tests/test_taipy_workflows_styling.py src/tests/test_taipy_workflows_perf.py -v`
Expected: All tests PASS

- [ ] **Step 8: Run ruff + pyright on modified files**

Run: `uv run ruff check hf_taipy_app/src/state/workflows.py hf_taipy_app/src/pages/workflows.py && uv run ruff format --check hf_taipy_app/src/state/workflows.py hf_taipy_app/src/pages/workflows.py`
Expected: PASS

---

## Task 8: Add auto-refresh timer

**Files:**
- Modify: `hf_taipy_app/src/state/workflows.py` (timer logic)

- [ ] **Step 1: Add timer imports and state**

At the top of `hf_taipy_app/src/state/workflows.py`, add to imports:

```python
import threading
```

Add module-level timer state:

```python
_refresh_timer: threading.Timer | None = None
_REFRESH_INTERVAL_SECONDS = 120  # 2 minutes
```

- [ ] **Step 2: Add timer start/stop functions**

Add after the timer state variables:

```python
def _start_auto_refresh(state: Any) -> None:
    """Start the 2-minute auto-refresh timer for the Workflows page."""
    global _refresh_timer
    _stop_auto_refresh()

    def _tick() -> None:
        global _refresh_timer
        try:
            state.invoke_callback("_wf_auto_refresh_tick", [])
        except Exception:
            logger.debug("Auto-refresh tick failed", exc_info=True)
        # Schedule next tick
        _refresh_timer = threading.Timer(_REFRESH_INTERVAL_SECONDS, _tick)
        _refresh_timer.daemon = True
        _refresh_timer.start()

    _refresh_timer = threading.Timer(_REFRESH_INTERVAL_SECONDS, _tick)
    _refresh_timer.daemon = True
    _refresh_timer.start()
    logger.info("Auto-refresh started (%ds interval)", _REFRESH_INTERVAL_SECONDS)


def _stop_auto_refresh() -> None:
    """Cancel the auto-refresh timer."""
    global _refresh_timer
    if _refresh_timer is not None:
        _refresh_timer.cancel()
        _refresh_timer = None
        logger.info("Auto-refresh stopped")


def _wf_auto_refresh_tick(state: Any) -> None:
    """Callback invoked by the timer — re-fetches data and updates state."""
    logger.debug("Auto-refresh tick")
    cold = _fetch_cold_costs()
    warm = _fetch_warm_costs()
    jobs = _fetch_job_runs()
    live_hf = _fetch_live_hf_status()

    # Rebuild table with current filters
    state.wf_table_data = _build_table_data(
        _cards, cold, jobs,
        state.wf_type_filter, state.wf_runtime_filter, state.wf_freshness_filter,
        live_hf=live_hf,
    )
    _compute_stats(state, cold, warm, jobs, live_hf=live_hf)
```

- [ ] **Step 3: Wire timer into wf_refresh (start) and page navigation (stop)**

At the end of `wf_refresh()`, after the existing logic, add:

```python
_start_auto_refresh(state)
```

Add `_wf_auto_refresh_tick` to `__all__` so Taipy can find the callback.

- [ ] **Step 4: Add stop logic on navigate-away**

The timer's daemon thread will die with the process, but for clean behavior, add stop logic. In `hf_taipy_app/src/state/shared.py`, the `_refresh_current_page` function calls the page refresher on navigation. The cleanest approach: check if the user is navigating *away* from the Workflows page.

In `wf_refresh()`, the timer starts. When `_refresh_current_page` fires for a *different* page, the Workflows timer should stop. Add a module-level function in workflows.py:

```python
def wf_on_navigate_away() -> None:
    """Called when user navigates away from the Workflows page."""
    _stop_auto_refresh()
```

In `hf_taipy_app/src/state/shared.py`, in `_refresh_current_page()`, add before `fn = _page_refreshers.get(...)`:

```python
# Stop Workflows auto-refresh when navigating away
if state.current_page != "AI-ML-Workflows":
    from state.workflows import _stop_auto_refresh
    _stop_auto_refresh()
```

This ensures the timer stops on navigate-away. `_start_auto_refresh` also cancels any existing timer before starting, so re-entering the page is safe.

- [ ] **Step 5: Add _wf_auto_refresh_tick to __all__**

```python
"_wf_auto_refresh_tick",
```

- [ ] **Step 6: Run all tests**

Run: `uv run pytest src/tests/test_workflows_auto_refresh.py src/tests/test_taipy_workflows_perf.py -v`
Expected: All tests PASS

---

## Task 9: Update spec with scope revision and run full quality gate

**Files:**
- Modify: `docs/superpowers/specs/2026-03-26-hf-buckets-auto-refresh-design.md`

- [ ] **Step 1: Add scope revision note to the spec**

At the top of the spec, after the header, add:

```markdown
**Scope revision (implementation):** HF Buckets do not expose HTTPS download URLs
for pip. The wheel stays in the `luxury-lakehouse/build-artifacts` model repo.
Only demo data moves to the `luxury-lakehouse/demo-data` bucket. CI workflow,
`deploy_wheel.py`, and PEP 723 wheel URLs are unchanged. PEP 723 scripts get
a `huggingface-hub` version bump only.
```

- [ ] **Step 2: Run full quality gate**

Run all checks:

```bash
uv run ruff check src/ scripts/ demo_space/ hf_taipy_app/src/
uv run ruff format --check src/ scripts/ demo_space/ hf_taipy_app/src/
uv run pyright src/
uv run pytest src/tests/ -v
```

Expected: All PASS

- [ ] **Step 3: Run the provisioning script (manual verification)**

This is a manual step — requires HF_TOKEN. Run:

```bash
python scripts/setup_hf_buckets.py --data-dir demo_space/data
```

Verify at https://huggingface.co/buckets/luxury-lakehouse/demo-data that all 6 parquet files are present.

- [ ] **Step 4: Verify demo_space reads from bucket**

After bucket provisioning, test the demo app locally:

```bash
cd demo_space && python -c "from app import embeddings_df; print(len(embeddings_df))"
```

Expected: Non-zero row count (confirms fsspec reads from bucket work)

---

## Task 10: Local Taipy verification and staging deploy

**Files:** No new changes — verification only.

- [ ] **Step 1: Run Taipy app locally**

```bash
cd hf_taipy_app && uv run python src/main.py
```

Navigate to AI/ML Workflows page. Verify:
- Status column appears in the table
- Stat cards render (Total Cost shows HF costs if warm tier has data)
- No errors in console

- [ ] **Step 2: Puppeteer screenshot of Workflows page**

Use Chrome (not Chromium) to capture the Workflows page with the new Status column visible. Verify:
- Status badges have correct colors
- Table has 9 columns (Name, Type, Runtime, Status, Last Run, Last Duration, Cost, Avg/Run, Freshness)
- Stat cards show combined running count

- [ ] **Step 3: Deploy to staging**

```bash
python scripts/deploy_taipy.py staging
```

Wait for staging Space to reach RUNNING status. Puppeteer-verify the staging deployment matches local.

- [ ] **Step 4: User approval for commit**

Present all changes for user review. Wait for explicit commit approval before proceeding.
