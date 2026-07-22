"""GitHub-Actions OIDC auth-method conflict guard.

Catches the exact class of defect shipped by the #449 PAT->OIDC migration and
observed failing on *every* ``lakebase-grants`` run for a day (2026-07-21/22):

A job sets ``DATABRICKS_CLIENT_ID`` at **job level** (visible to every step)
*and* an OIDC mint step exports ``DATABRICKS_TOKEN`` into ``$GITHUB_ENV``
(visible to every subsequent step). A downstream step that constructs a bare
``WorkspaceClient()`` then sees two configured auth methods -- a bearer token
(pat) and a client id (oauth) -- and the Databricks SDK refuses:

    ValueError: validate: more than one authorization method configured:
                oauth and pat

The first casualty was ``scripts/fix_event_log_ownership.py`` (bare
``WorkspaceClient()``); the earlier heal step only *looked* green because it
carried ``continue-on-error: true``.

The invariant this test enforces:

    A job that (a) sets DATABRICKS_CLIENT_ID at job level AND (b) mints
    DATABRICKS_TOKEN into $GITHUB_ENV must ALSO pin DATABRICKS_AUTH_TYPE at
    job level (any value -- 'pat' or 'github-oidc' -- disambiguates).

Workflows that instead confine ``DATABRICKS_CLIENT_ID`` to the mint step
(python-ci, data-quality-ci) never expose the pair to a downstream bare
client, so they are correctly *not* flagged.

Offline, deterministic, PR-gating -- no live workspace required.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

_REPO = Path(__file__).resolve().parents[2]
_WORKFLOWS_DIR = _REPO / ".github" / "workflows"


def _iter_jobs() -> list[tuple[str, str, dict[str, Any]]]:
    """Yield ``(workflow_filename, job_name, job_dict)`` for every job in every workflow."""
    jobs: list[tuple[str, str, dict[str, Any]]] = []
    for wf in sorted(_WORKFLOWS_DIR.glob("*.yml")):
        doc = yaml.safe_load(wf.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            continue
        for job_name, job in (doc.get("jobs") or {}).items():
            if isinstance(job, dict):
                jobs.append((wf.name, job_name, job))
    return jobs


def _job_env(job: dict[str, Any]) -> dict[str, Any]:
    env = job.get("env") or {}
    return env if isinstance(env, dict) else {}


def _mints_token_globally(job: dict[str, Any]) -> bool:
    """True if any step writes ``DATABRICKS_TOKEN`` into ``$GITHUB_ENV`` (global export)."""
    for step in job.get("steps") or []:
        if not isinstance(step, dict):
            continue
        run = step.get("run")
        if isinstance(run, str) and "DATABRICKS_TOKEN=" in run and "GITHUB_ENV" in run:
            return True
    return False


def _violates(job: dict[str, Any]) -> bool:
    """The bug: job-level CLIENT_ID + a global TOKEN mint, with no job-level AUTH_TYPE."""
    env = _job_env(job)
    has_job_client_id = "DATABRICKS_CLIENT_ID" in env
    has_job_auth_type = "DATABRICKS_AUTH_TYPE" in env
    return has_job_client_id and _mints_token_globally(job) and not has_job_auth_type


def test_no_oidc_auth_method_conflict() -> None:
    """No workflow job may expose both a bearer token and a client id to a bare client."""
    offenders = [f"{wf}:{job_name}" for wf, job_name, job in _iter_jobs() if _violates(job)]
    assert not offenders, (
        "OIDC auth-method conflict: these jobs set DATABRICKS_CLIENT_ID at job level "
        "AND mint DATABRICKS_TOKEN into $GITHUB_ENV, but do not pin a job-level "
        "DATABRICKS_AUTH_TYPE. A downstream bare WorkspaceClient() will raise "
        "'more than one authorization method configured: oauth and pat'. Fix: add "
        "'DATABRICKS_AUTH_TYPE: pat' to the job env (the mint step overrides it with a "
        f"step-level github-oidc). Offenders: {offenders}"
    )


def test_detector_flags_the_pre_fix_pattern() -> None:
    """Self-test: the detector must flag the #449 bug and clear both valid fixes.

    Without this, a refactor that quietly breaks ``_violates`` would let the guard
    pass vacuously on a healthy tree while no longer catching a reintroduced bug.
    """
    mint_step = {"run": 'echo "DATABRICKS_TOKEN=$TOKEN" >> "$GITHUB_ENV"'}

    buggy = {
        "env": {"DATABRICKS_HOST": "x", "DATABRICKS_CLIENT_ID": "x"},
        "steps": [mint_step],
    }
    assert _violates(buggy), "detector failed to flag the #449 job-level-CLIENT_ID + global-mint bug"

    fixed_option_a = {  # pin auth_type at job level (this PR's fix)
        "env": {"DATABRICKS_HOST": "x", "DATABRICKS_CLIENT_ID": "x", "DATABRICKS_AUTH_TYPE": "pat"},
        "steps": [mint_step],
    }
    assert not _violates(fixed_option_a), "auth_type=pat at job level must clear the guard"

    fixed_option_b = {  # confine CLIENT_ID to the mint step (python-ci pattern)
        "env": {"DATABRICKS_HOST": "x"},
        "steps": [{"env": {"DATABRICKS_CLIENT_ID": "x"}, **mint_step}],
    }
    assert not _violates(fixed_option_b), "CLIENT_ID confined to the mint step must clear the guard"


def test_guard_actually_scans_workflows() -> None:
    """Fail loudly if the workflow glob ever resolves to nothing (moved/renamed dir)."""
    jobs = _iter_jobs()
    assert jobs, f"no workflow jobs discovered under {_WORKFLOWS_DIR} - guard would pass vacuously"
