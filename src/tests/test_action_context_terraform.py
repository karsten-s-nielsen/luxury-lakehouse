"""Terraform wiring guards for the AC-1 worker-drain fan-out (ADR-037).

Offline (text parsing) — no Terraform runtime needed.
"""

from __future__ import annotations

import re
from pathlib import Path

from ingestion.action_context import _ActionContextGuard

_TF = Path(__file__).resolve().parents[2] / "terraform" / "modules" / "workflows" / "main.tf"


def _tf() -> str:
    return _TF.read_text(encoding="utf-8")


def test_terraform_concurrency_matches_n_workers() -> None:
    """M4: the for-each concurrency must equal _N_DRAIN_WORKERS (single source of truth)."""
    text = _tf()
    block = text[text.index('task_key = "compute_action_context"') :][:1400]
    m = re.search(r"concurrency\s*=\s*(\d+)", block)
    assert m is not None, "no concurrency in compute_action_context for_each"
    assert int(m.group(1)) == _ActionContextGuard._N_DRAIN_WORKERS


def test_terraform_drain_worker_entry_point_and_params() -> None:
    text = _tf()
    block = text[text.index('task_key = "compute_action_context"') :][:1400]
    assert 'entry_point  = "compute_action_context_drain_worker"' in block
    assert "action_context_worker_ids" in block  # for-each input is the worker-id list
    assert "action_context_run_id" in block  # run-id passed to the worker (B1)
    assert "timeout_seconds = 28800" in block  # 8 h drain budget
    # the old per-game chunk fan-out must be gone
    assert "action_context_chunks" not in text


def test_terraform_preflight_passes_job_run_id() -> None:
    """B1: preflight receives the job-level run id to write into the task value."""
    text = _tf()
    block = text[text.index('task_key        = "preflight_action_context"') :][:1800]
    assert '"--run-id", "{{job.run_id}}"' in block
