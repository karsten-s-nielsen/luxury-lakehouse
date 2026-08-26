"""Terraform wiring guards for the tracking-marts worker-drain fan-out (ADR-037/068 reuse).

Sibling of ``test_action_context_terraform.py`` — offline text parsing, no Terraform runtime.
The tracking-marts drain replaces the three driver-sequential grain-mart writers
(``off_ball_runs_writer`` / ``defensive_credit_writer`` / ``gkdv_writer``) with a single
``preflight_tracking_marts`` -> ``compute_tracking_marts`` (for_each) -> ``verify_tracking_marts_drain``
-> ``compute_gkdv_pool`` chain.
"""

from __future__ import annotations

import re
from pathlib import Path

from ingestion.drain_adapters import _N_EVENT_WORKERS
from ingestion.tracking_marts_drain import _N_TRACKING_MARTS_WORKERS

_TF = Path(__file__).resolve().parents[2] / "terraform" / "modules" / "workflows" / "main.tf"


def _tf() -> str:
    return _TF.read_text(encoding="utf-8")


def _block(text: str, needle: str, size: int = 2000) -> str:
    idx = text.index(needle)
    return text[idx : idx + size]


def test_terraform_concurrency_matches_n_workers() -> None:
    """The for-each concurrency MUST equal ``_N_TRACKING_MARTS_WORKERS`` — the single source of truth
    that is itself pinned == the event-worker count (``_N_EVENT_WORKERS``)."""
    block = _block(_tf(), 'task_key = "compute_tracking_marts"')
    m = re.search(r"concurrency\s*=\s*(\d+)", block)
    assert m is not None, "no concurrency in compute_tracking_marts for_each"
    assert int(m.group(1)) == _N_TRACKING_MARTS_WORKERS
    # The three constants are welded: TF concurrency == drain worker count == event-table worker count.
    assert _N_TRACKING_MARTS_WORKERS == _N_EVENT_WORKERS == 8


def test_terraform_drain_worker_entry_point_and_params() -> None:
    block = _block(_tf(), 'task_key = "compute_tracking_marts"')
    assert 'entry_point  = "compute_tracking_marts_drain_worker"' in block
    assert "tracking_marts_worker_ids" in block  # for-each input is the constant worker-id list
    assert "tracking_marts_run_id" in block  # run-id passed to the worker (from preflight task value)
    assert "timeout_seconds = 28800" in block  # 8 h drain budget
    assert '"--watchdog-budget-s", "{{job.parameters.watchdog_budget_s}}"' in block


def test_terraform_preflight_passes_job_run_id_and_full_flag() -> None:
    """Preflight receives the JOB run id (not a task value) + the tracking_marts_full job parameter."""
    block = _block(_tf(), 'task_key        = "preflight_tracking_marts"')
    assert 'entry_point  = "preflight_tracking_marts"' in block
    assert '"--run-id", "{{job.run_id}}"' in block
    assert '"--full", "{{job.parameters.tracking_marts_full}}"' in block
    # Depends on BOTH AC arms + the xG scorer (its inputs), listed alphabetically.
    for dep in ("compute_action_context", "compute_action_context_statsbomb", "compute_xg_shot_scores"):
        assert f'task_key = "{dep}"' in block


def test_terraform_job_parameter_tracking_marts_full_exists() -> None:
    text = _tf()
    assert 'name    = "tracking_marts_full"' in text, "job parameter tracking_marts_full missing"


def test_terraform_verify_gate_and_pool_wiring() -> None:
    text = _tf()
    verify = _block(text, 'task_key        = "verify_tracking_marts_drain"')
    assert 'entry_point  = "verify_tracking_marts_drain"' in verify
    assert 'run_if = "ALL_DONE"' in verify
    assert 'task_key = "compute_tracking_marts"' in verify  # depends on the drain
    assert '"--run-id", "{{job.run_id}}"' in verify
    assert "timeout_seconds = 3600" in verify

    pool = _block(text, 'task_key        = "compute_gkdv_pool"')
    assert 'entry_point  = "compute_gkdv_pool"' in pool
    assert 'task_key = "verify_tracking_marts_drain"' in pool  # runs after the gate


def test_terraform_old_writer_tasks_removed() -> None:
    """The three driver-sequential writer tasks must be GONE — replaced by the drain."""
    text = _tf()
    for writer in ("defensive_credit_writer", "gkdv_writer", "off_ball_runs_writer"):
        assert f'task_key        = "{writer}"' not in text, f"stale writer task {writer} still in main.tf"


def test_terraform_dbt_output_marts_depends_on_drain_not_writers() -> None:
    """dbt_build_output_marts now waits on the drain gate + gkdv pool, not the removed writers."""
    block = _block(_tf(), 'task_key        = "dbt_build_output_marts"', size=3000)
    assert 'task_key = "verify_tracking_marts_drain"' in block
    assert 'task_key = "compute_gkdv_pool"' in block
    for writer in ("defensive_credit_writer", "gkdv_writer", "off_ball_runs_writer"):
        assert f'task_key = "{writer}"' not in block, f"dbt_build_output_marts still depends on removed {writer}"
