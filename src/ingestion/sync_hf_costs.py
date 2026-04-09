#!/usr/bin/env python3
"""Cost bridge — reads per-run cost history from HF Hub repos and MERGEs into workflow_cost_live.

Reads ``_cost_history/*.json`` per-run files first (written by HFJobsCostRecorder).
Falls back to ``_workflow_cost.json`` for repos that haven't adopted per-run history yet.

Parses workflow-cards/*.yaml to discover HF Jobs repos. Designed to run as a
Databricks scheduled task every 15 minutes.

Usage:
    python scripts/sync_hf_costs.py --catalog soccer_analytics [--cards-dir workflow-cards]
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from huggingface_hub import HfApi
from huggingface_hub.hf_api import RepoFile

from ingestion.guards import FilterResult
from shared.constants import COST_TABLE_NAME, DEFAULT_OBSERVABILITY_SCHEMA, IDENTIFIER_RE
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class _SyncHfCostsGuard:
    workflow_id = "wf-sync-hf-costs"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return FilterResult(workflow_id=self.workflow_id, count=1)


skip_guard = _SyncHfCostsGuard()


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

        # Check datasets (most HF Jobs write to dataset repos)
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


def fetch_cost_history(api: HfApi, repo_id: str, repo_type: str) -> list[dict[str, Any]]:
    """Read all ``_cost_history/*.json`` files from an HF Hub repo.

    Falls back to ``_workflow_cost.json`` if ``_cost_history/`` is empty or absent.
    Only records with ``hf_job_id`` are included. The legacy fallback excludes
    RUNNING records (those are transient and will be superseded by history files
    once the job completes).

    Returns list of cost record dicts.
    """
    records: list[dict[str, Any]] = []

    # Try _cost_history/ directory first
    try:
        items = list(api.list_repo_tree(repo_id, repo_type=repo_type, path_in_repo="_cost_history"))
        for item in items:
            if not isinstance(item, RepoFile) or not item.rfilename.endswith(".json"):
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


@workflow("wf-sync-hf-costs", phase="sync")
def run_pipeline(
    catalog: str,
    cards_dir: Path,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Main sync logic. Returns number of records synced."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")
    if not IDENTIFIER_RE.match(catalog):
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
            logger.debug("Failed to load card %s for task_key resolution", card_path.name, exc_info=True)

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

    if not rows:
        logger.info("No cost records to sync")
        return 0

    # MERGE into workflow_cost_live via PySpark
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    target_table = f"{catalog}.{DEFAULT_OBSERVABILITY_SCHEMA}.{COST_TABLE_NAME}"
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
    """CLI entry point.

    Note: bootstrap_hooks is intentionally omitted here. This module writes
    directly to ``workflow_cost_live`` — the same table that CostEstimateHook
    targets. Adding the hook would create a circular write (the cost bridge
    recording its own cost to the cost table it just merged into).
    The ``@workflow`` decorator on ``run_pipeline`` still provides registry
    tracking without the cost hook.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(description="Sync HF Jobs costs to Delta")
    parser.add_argument("--catalog", default="soccer_analytics", help="Unity Catalog name")
    parser.add_argument("--cards-dir", type=Path, default=Path("workflow-cards"), help="Workflow cards directory")
    args = parser.parse_args()

    # No Spark available yet for guard check — create a standalone FilterResult
    filter_result = skip_guard.check(None, args.catalog, "")  # type: ignore[arg-type]

    count = run_pipeline(args.catalog, args.cards_dir, filter_result=filter_result)
    logger.info("Done — %d records synced", count)


if __name__ == "__main__":
    main()
