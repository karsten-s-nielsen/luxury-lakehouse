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
from typing import Any

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
        "import_psxg_predictions",
        "publish_spadl_vaep",
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

    RESTORED 2026-08-11 after the ADR-075 auth consolidation broke it. That sweep rewrote this
    to ``ingestion.databricks_auth.workspace_client()`` and left the paragraph above — which
    states exactly why it must not — standing three lines up, so Terraform Apply died on
    ``ModuleNotFoundError: No module named 'ingestion'`` (the apply itself succeeded; the
    ADR-025 retry patch did not run). The bare client is correct HERE and only here: this runs
    solely under ``DATABRICKS_AUTH_TYPE=github-oidc``, where there is no ``~/.databrickscfg``
    and the profile ambiguity ``workspace_client()`` exists to explain cannot arise. Recorded
    in ``_ALLOWED_BARE`` (test_workspace_client_construction.py) and enforced from the other
    side by test_ci_script_environment.py.
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
    """Mutate task settings in-place, return list of changes made.

    ``max_retries`` ABSENT is NOT the same as explicit ``0``: the Databricks TF provider
    (Go omitempty) drops ``max_retries = 0`` from the payload, and the platform then applies
    its DEFAULT (1 retry) for an absent field — see the module docstring. So ``.get(..., 0)``
    would read absent as 0, decide "already correct", and make NO change (the no-op bug that
    shipped 1-retry compute tasks and burned a full drain's DBU on GATE 0, run 941047587677694).
    Read absent as ``None`` so it compares unequal to the intended explicit int and is patched.
    """
    changes: list[str] = []
    tasks = settings.get("tasks", [])

    for task in tasks:
        task_key = task.get("task_key", "")
        current = task.get("max_retries")  # None if absent; absent != explicit 0 (platform default = 1)
        intended = _classify_task(task_key)

        if current != intended:
            changes.append(f"  {task_key}: {current} → {intended}")
            task["max_retries"] = intended

        # Handle for_each_task nested tasks
        for_each = task.get("for_each_task", {})
        inner_task = for_each.get("task", {})
        if inner_task:
            inner_key = inner_task.get("task_key", "")
            inner_current = inner_task.get("max_retries")
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
    """GET /api/2.1/jobs/get + assert the retry policy landed, to the extent the API can confirm it.

    **The GET cannot echo ``max_retries = 0``** — Databricks omits it on the response (server-side Go
    ``omitempty``, not only the TF provider), so a stored 0 and an absent field are indistinguishable, and
    absent behaves as retry (proven: GATE-0 run 941047587677694 had compute iterations at attempt_number=1
    with max_retries=None). A GET-based ``actual == 0`` assertion therefore can NEVER pass for a compute task
    (ADR-025 amendment). So this verifier asserts only GET-returnable facts:

    1. every NON-ZERO intent (ingestion = 1) must round-trip via GET — catches a dead/partial reset;
    2. no task may show an UNINTENDED POSITIVE retry count (a compute/intended-0 task reading >= 1);
    3. intended-0 reading absent/None/0 is ACCEPTED (the API cannot distinguish a stored 0 from absent);
    4. the round-trip guard (1) must have run on >= 1 non-zero task, else it proved nothing (vacuous) — a job
       with no ingestion task or a classifier regression to all-0 would otherwise pass silently.

    Whether a reset-sent 0 actually SUPPRESSES the retry is NOT confirmable here (write side only, guarded by
    ``_patch_tasks`` unit tests); it is settled once by the ADR-025 effectiveness probe. A green verify means
    "honest + the non-zero intents applied", NOT "compute retries suppressed". Raises SystemExit on failure so
    terraform-apply.yml fails loud.
    """
    job = _get_job(base_url, headers, job_id)
    settings = job.get("settings", {})
    tasks = settings.get("tasks", [])

    mismatches: list[str] = []
    n_nonzero_verified = 0  # the round-trip guard (1) must run on >= 1 non-zero task (non-vacuity, point 4)

    def _check(key: str, actual: Any, label: str) -> None:
        nonlocal n_nonzero_verified
        intended = _classify_task(key)
        if intended != 0:
            # non-zero intent (ingestion = 1) MUST round-trip via GET (catches a dead/partial reset).
            n_nonzero_verified += 1
            if actual != intended:
                mismatches.append(f"  {label}: actual={actual} intended={intended} (non-zero intent must round-trip)")
        elif actual is not None and int(actual) > 0:
            # intended 0: absent/None/0 accepted (GET omits a stored 0); only a POSITIVE value is a defect.
            mismatches.append(f"  {label}: actual={actual} intended=0 (unintended positive retries)")

    for task in tasks:
        task_key = task.get("task_key", "")
        _check(task_key, task.get("max_retries"), task_key)
        inner_task = task.get("for_each_task", {}).get("task", {})
        if inner_task:
            inner_key = inner_task.get("task_key", "")
            _check(inner_key, inner_task.get("max_retries"), f"{task_key}/{inner_key}")

    if n_nonzero_verified == 0:
        logger.error(
            "Post-apply verification VACUOUS — the round-trip guard checked 0 non-zero-intent tasks "
            "(no ingestion max_retries=1 task found). The classifier or job shape changed; the honest verify "
            "must not pass while proving nothing. Investigate the classifier / job definition before re-running."
        )
        sys.exit(1)

    if mismatches:
        logger.error("Post-apply verification FAILED — job %d has tasks with the wrong max_retries:", job_id)
        for m in mismatches:
            logger.error(m)
        logger.error(
            "Non-zero intents (ingestion=1) must round-trip via GET, and no task may show a POSITIVE retry "
            "count it should not. NOTE: Databricks omits max_retries=0 on GET (server-side omitempty), so an "
            "intended-0 task reading absent/None is EXPECTED + accepted here — the effectiveness of the 0 is "
            "settled by the ADR-025 probe, not this GET. Investigate the listed mismatches before re-running."
        )
        sys.exit(1)

    logger.info(
        "Post-apply verification OK — %d non-zero-intent task(s) round-tripped; no unintended positive retries "
        "(intended-0 tasks read absent per Databricks GET omitempty).",
        n_nonzero_verified,
    )


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
