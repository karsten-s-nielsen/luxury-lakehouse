#!/usr/bin/env python3
"""Post-apply patch: enforce max_retries on Databricks job tasks.

The Databricks Terraform provider (Go SDK) uses `omitempty` on the
`max_retries` int field.  Go's zero-value for int is 0, so
`max_retries = 0` is silently omitted from the API payload.  The
platform then applies its default (1 retry), meaning every task gets
one retry regardless of what Terraform declares.

This script patches the job via the REST API after `terraform apply`
to enforce the intended retry policy:

  - **Ingestion tasks** (external API calls): max_retries = 1
    Transient network errors, rate limits, and provider outages
    benefit from a single retry.

  - **Compute tasks** (deterministic): max_retries = 0
    If a compute task fails (schema mismatch, data quality, OOM),
    retrying wastes a full task timeout worth of DBU before producing
    the same error.

Usage (CI — post terraform apply):
    python scripts/patch_job_retries.py

Usage (manual):
    python scripts/patch_job_retries.py --dry-run
    python scripts/patch_job_retries.py --job-name soccer-analytics-ingestion-dev

Environment:
    DATABRICKS_HOST   Workspace hostname (no https://)
    DATABRICKS_TOKEN  PAT or OAuth token (CI uses OIDC-generated token)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("patch_job_retries")

# ── Task classification ──────────────────────────────────────────────────────
# Ingestion tasks make external API/network calls where transient failures
# (rate limits, timeouts, provider outages) benefit from a single retry.
# Everything else is deterministic compute — retry wastes time.

_INGESTION_TASK_KEYS: frozenset[str] = frozenset(
    {
        "backfill_statsbomb_360",
        "backfill_statsbomb_extra",
        "hf_sync",
        "import_obso_results",
        # ingest_gradientsports is a for_each_task parent (no max_retries of its own)
        # ingest_idsse is a for_each_task parent (no max_retries of its own)
        "ingest_gradientsports_iteration",
        "ingest_idsse_iteration",
        "ingest_idsse_events",
        "ingest_metrica",
        "ingest_skillcorner",
        "ingest_statsbomb",
        "ingest_wyscout",
    }
)

_INGESTION_MAX_RETRIES = 1
_COMPUTE_MAX_RETRIES = 0


def _get_base_url() -> str:
    host = os.environ.get("DATABRICKS_HOST", "")
    if not host:
        logger.error("DATABRICKS_HOST not set")
        sys.exit(1)
    host = host.rstrip("/")
    if not host.startswith("https://"):
        host = f"https://{host}"
    return host


def _get_headers() -> dict[str, str]:
    """A fresh bearer, resolved through the SDK at the point of use.

    Deliberately duplicates ``ingestion.databricks_auth.auth_headers`` rather than importing
    it: terraform-apply.yml runs this with a bare ``python`` and a pip-installed
    ``databricks-sdk``, without the project wheel on sys.path. Keep the two in sync — the
    canonical version, with tests, is the ``ingestion`` one.

    ``Config.authenticate()`` dispatches on whatever is configured, so this works for a
    static ``DATABRICKS_TOKEN`` locally and for ``github-oidc`` in CI, where it mints per
    call. The previous version read ``DATABRICKS_TOKEN`` directly, which required the
    workflow to materialise a bearer into ``$GITHUB_OUTPUT`` (ADR-071 amendment).
    """
    from databricks.sdk import WorkspaceClient

    value = (WorkspaceClient().config.authenticate() or {}).get("Authorization", "")
    if not value.startswith("Bearer "):
        logger.error(
            "Databricks SDK returned no Bearer authorization header (got %r). Set "
            "DATABRICKS_HOST and either DATABRICKS_TOKEN or DATABRICKS_AUTH_TYPE=github-oidc.",
            value[:24],
        )
        sys.exit(1)
    return {"Authorization": value, "Content-Type": "application/json"}


def _find_job_id(base_url: str, headers: dict[str, str], job_name: str) -> int:
    """Find the job ID by name via the Jobs API."""
    resp = requests.get(
        f"{base_url}/api/2.1/jobs/list",
        headers=headers,
        params={"name": job_name, "limit": 1},
        timeout=(10, 30),
        verify=True,
    )
    resp.raise_for_status()
    jobs = resp.json().get("jobs", [])
    if not jobs:
        logger.error("No job found with name '%s'", job_name)
        sys.exit(1)
    job_id: int = jobs[0]["job_id"]
    logger.info("Found job '%s' → job_id=%d", job_name, job_id)
    return job_id


def _get_job(base_url: str, headers: dict[str, str], job_id: int) -> dict:
    """GET /api/2.1/jobs/get."""
    resp = requests.get(
        f"{base_url}/api/2.1/jobs/get",
        headers=headers,
        params={"job_id": job_id},
        timeout=(10, 30),
        verify=True,
    )
    resp.raise_for_status()
    return resp.json()


def _classify_task(task_key: str) -> int:
    """Return the intended max_retries for a task based on its classification."""
    if task_key in _INGESTION_TASK_KEYS:
        return _INGESTION_MAX_RETRIES
    return _COMPUTE_MAX_RETRIES


def _patch_tasks(settings: dict) -> list[str]:
    """Mutate task settings in-place, return list of changes made."""
    changes: list[str] = []
    tasks = settings.get("tasks", [])

    for task in tasks:
        task_key = task.get("task_key", "")
        current = task.get("max_retries", 0)
        intended = _classify_task(task_key)

        if current != intended:
            changes.append(f"  {task_key}: {current} → {intended}")
            task["max_retries"] = intended

        # Handle for_each_task nested tasks
        for_each = task.get("for_each_task", {})
        inner_task = for_each.get("task", {})
        if inner_task:
            inner_key = inner_task.get("task_key", "")
            inner_current = inner_task.get("max_retries", 0)
            inner_intended = _classify_task(inner_key)

            if inner_current != inner_intended:
                changes.append(f"  {task_key}/{inner_key}: {inner_current} → {inner_intended}")
                inner_task["max_retries"] = inner_intended

    return changes


def _reset_job(base_url: str, headers: dict[str, str], job_id: int, new_settings: dict) -> None:
    """POST /api/2.1/jobs/reset — replace job settings."""
    resp = requests.post(
        f"{base_url}/api/2.1/jobs/reset",
        headers=headers,
        json={"job_id": job_id, "new_settings": new_settings},
        timeout=(10, 30),
        verify=True,
    )
    resp.raise_for_status()
    logger.info("Job %d updated successfully", job_id)


def _verify_post_apply(base_url: str, headers: dict[str, str], job_id: int) -> None:
    """GET /api/2.1/jobs/get + assert every task's max_retries matches classifier intent.

    Without this verifier the patch is fire-and-forget — if Databricks ever
    silently rewrites max_retries on its side (e.g. policy mutation, future
    SDK regression, or the same Go omitempty issue resurfacing in a different
    code path), the next prod failure would be the first signal.

    Raises SystemExit on mismatch so the CI step (terraform-apply.yml) fails
    loud and the PR cannot ship a partially-patched job.
    """
    job = _get_job(base_url, headers, job_id)
    settings = job.get("settings", {})
    tasks = settings.get("tasks", [])

    mismatches: list[str] = []
    for task in tasks:
        task_key = task.get("task_key", "")
        actual = task.get("max_retries", 0)
        intended = _classify_task(task_key)
        if actual != intended:
            mismatches.append(f"  {task_key}: actual={actual} intended={intended}")

        for_each = task.get("for_each_task", {})
        inner_task = for_each.get("task", {})
        if inner_task:
            inner_key = inner_task.get("task_key", "")
            inner_actual = inner_task.get("max_retries", 0)
            inner_intended = _classify_task(inner_key)
            if inner_actual != inner_intended:
                mismatches.append(f"  {task_key}/{inner_key}: actual={inner_actual} intended={inner_intended}")

    if mismatches:
        logger.error("Post-apply verification FAILED — job %d has tasks with wrong max_retries:", job_id)
        for m in mismatches:
            logger.error(m)
        logger.error(
            "The reset POST succeeded but the verification GET shows drift. "
            "Either a) Databricks rewrote max_retries server-side, b) the Go omitempty "
            "regression has resurfaced in another code path, or c) the classifier disagrees "
            "with the platform's saved state. Investigate before re-running."
        )
        sys.exit(1)

    logger.info("Post-apply verification OK — %d task(s) match classifier intent", len(tasks))


def main() -> None:
    parser = argparse.ArgumentParser(description="Patch Databricks job task retry settings")
    parser.add_argument(
        "--job-name",
        default="soccer-analytics-ingestion-dev",
        help="Job name to patch (default: soccer-analytics-ingestion-dev)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show changes without applying",
    )
    args = parser.parse_args()

    base_url = _get_base_url()
    headers = _get_headers()

    job_id = _find_job_id(base_url, headers, args.job_name)
    job = _get_job(base_url, headers, job_id)
    settings = job.get("settings", {})

    changes = _patch_tasks(settings)

    if not changes:
        logger.info("All tasks already at intended max_retries — no changes needed")
        return

    logger.info("Retry policy changes (%d tasks):", len(changes))
    for change in changes:
        logger.info(change)

    if args.dry_run:
        logger.info("Dry run — no changes applied")
        print(json.dumps({"changes": changes, "dry_run": True}, indent=2))
        return

    _reset_job(base_url, headers, job_id, settings)
    _verify_post_apply(base_url, headers, job_id)
    logger.info("Done — %d task(s) patched + verified", len(changes))


if __name__ == "__main__":
    main()
