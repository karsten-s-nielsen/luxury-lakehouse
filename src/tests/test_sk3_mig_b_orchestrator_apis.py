"""SK3-MIG-B orchestrator API-drift + wiring tests.

Catches huggingface_hub API drift and orchestrator wiring bugs at CI time
instead of at the start of a $40-80 retrain cycle. Origin: 2026-05-04 Phase 9
attempt halted on `api.run_jobs` / `job.job_id` / `hardware=` / missing
polling + missing `scripts/refresh_synced_tables.py`. Each test below maps to
one of those gaps.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ORCHESTRATOR = _REPO_ROOT / "scripts" / "sk3_mig_b_retrain.py"


def test_huggingface_hub_run_uv_job_exists() -> None:
    """HfApi.run_uv_job is the dispatch entrypoint the orchestrator calls."""
    from huggingface_hub import HfApi

    assert hasattr(HfApi, "run_uv_job"), "HF Hub renamed run_uv_job — orchestrator dispatch needs update"


def test_huggingface_hub_inspect_job_exists() -> None:
    """HfApi.inspect_job is the polling entrypoint the orchestrator calls."""
    from huggingface_hub import HfApi

    assert hasattr(HfApi, "inspect_job"), "HF Hub renamed inspect_job — orchestrator polling needs update"


def test_huggingface_hub_jobinfo_id_field() -> None:
    """JobInfo.id is the canonical attribute (NOT .job_id)."""
    from huggingface_hub._jobs_api import JobInfo

    field_names = {f.name for f in dataclasses.fields(JobInfo)}
    assert "id" in field_names, "JobInfo.id field missing — was it renamed?"
    assert "job_id" not in field_names, "JobInfo.job_id appeared (was previously .id) — orchestrator needs update"


def test_jobstage_terminal_values_present() -> None:
    """Polling depends on these terminal stages being defined."""
    from huggingface_hub._jobs_api import JobStage

    names = {s.name for s in JobStage}
    assert "COMPLETED" in names, "JobStage.COMPLETED missing"
    assert "CANCELED" in names, "JobStage.CANCELED missing"
    assert "ERROR" in names, "JobStage.ERROR missing"
    assert "RUNNING" in names, "JobStage.RUNNING missing"


def test_orchestrator_uses_run_uv_job_not_run_jobs() -> None:
    """Orchestrator must not call non-existent api.run_jobs."""
    src = _ORCHESTRATOR.read_text(encoding="utf-8")
    assert "run_uv_job" in src, "Orchestrator must call api.run_uv_job(...)"
    assert "api.run_jobs(" not in src, (
        "Orchestrator still calls non-existent api.run_jobs() — must use api.run_uv_job(...)"
    )


def test_orchestrator_reads_jobinfo_id_attribute() -> None:
    """Orchestrator must read JobInfo.id, not .job_id (which doesn't exist)."""
    src = _ORCHESTRATOR.read_text(encoding="utf-8")
    assert "job.job_id" not in src, "Orchestrator reads non-existent JobInfo.job_id — use job.id"


def test_orchestrator_uses_flavor_param_not_hardware() -> None:
    """run_uv_job takes flavor=, not hardware=."""
    src = _ORCHESTRATOR.read_text(encoding="utf-8")
    assert "hardware=flavor" not in src, "Orchestrator still passes run_jobs hardware= param — run_uv_job uses flavor="


def test_orchestrator_polls_hf_job_until_terminal() -> None:
    """Orchestrator must poll HF Job to terminal state before promoting Champion.

    Without polling, dispatch returns immediately and downstream Champion / mart
    checks run before the trainer has finished.
    """
    src = _ORCHESTRATOR.read_text(encoding="utf-8")
    assert "_poll_hf_job_until_terminal" in src, (
        "Orchestrator must define _poll_hf_job_until_terminal — without it, "
        "_promote_champion runs before the trainer finishes."
    )
    assert "inspect_job" in src, "Orchestrator must call api.inspect_job() to poll HF Job status"


def test_orchestrator_does_not_reference_nonexistent_refresh_script() -> None:
    """scripts/refresh_synced_tables.py does not exist; use scripts/maintain_synced_tables.py."""
    src = _ORCHESTRATOR.read_text(encoding="utf-8")
    assert "refresh_synced_tables.py" not in src, (
        "Orchestrator references non-existent scripts/refresh_synced_tables.py. "
        "Use scripts/maintain_synced_tables.py (existing wrapper) instead."
    )


def test_orchestrator_main_reconfigures_stdout_for_utf8() -> None:
    """Win11 cp1252 stdout breaks on Greek alpha. main() must force utf-8."""
    src = _ORCHESTRATOR.read_text(encoding="utf-8")
    assert "sys.stdout.reconfigure" in src, (
        "main() must call sys.stdout.reconfigure(encoding='utf-8') so the Greek alpha "
        "in 'PR-alpha' status lines prints on Windows cp1252 default codepage"
    )


def test_orchestrator_polling_handles_str_stage_at_runtime() -> None:
    """huggingface_hub returns JobStatus.stage as `str` at runtime, not JobStage enum.

    The dataclass annotation says `stage: JobStage` but `inspect_job` returns the
    raw API string ("RUNNING" / "COMPLETED" / "CANCELED" / ...). The polling
    helper must use a defensive accessor — direct `.value` on a str raises
    `AttributeError: 'str' object has no attribute 'value'`.

    Origin: 2026-05-04 Phase 9 first dispatch halted on this exact line. Sentinel
    catches reversion to direct `.value` access.
    """
    src = _ORCHESTRATOR.read_text(encoding="utf-8")
    assert "getattr(stage" in src or "hasattr(stage" in src or "isinstance(stage" in src, (
        "_poll_hf_job_until_terminal must defensively extract stage value — "
        "huggingface_hub returns JobStatus.stage as str at runtime, not JobStage enum. "
        "Use `getattr(stage_obj, 'value', stage_obj)` or equivalent guard."
    )


def test_orchestrator_uses_loaded_at_for_gold_marts() -> None:
    """Gold marts use _loaded_at (dbt convention); only bronze uses _ingested_at.

    Pre-flight queries `dev_gold.fct_action_values` — must use `_loaded_at`.
    Belt-and-suspenders test: catches reversion of the 2026-05-04 fix.
    """
    src = _ORCHESTRATOR.read_text(encoding="utf-8")
    assert "MAX(_loaded_at) FROM" in src and "fct_action_values" in src, (
        "Pre-flight gold mart freshness query must use _loaded_at, not _ingested_at"
    )
