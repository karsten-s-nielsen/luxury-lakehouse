"""Parity test: patch_job_retries.py task classification matches main.tf intent.

The Databricks TF provider silently drops max_retries=0 (Go omitempty).
scripts/patch_job_retries.py patches compute tasks to 0 post-apply.  This
test ensures the script's _INGESTION_TASK_KEYS stays in sync with the TF
file's declared max_retries values.

Invariants:
  1. Every TF task with max_retries=1 must be in _INGESTION_TASK_KEYS
  2. Every TF task with max_retries=0 must NOT be in _INGESTION_TASK_KEYS
  3. _INGESTION_TASK_KEYS must not contain task keys absent from the TF file

Pure parse — no Databricks connection.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
_TF_FILE = _REPO / "terraform" / "modules" / "workflows" / "main.tf"

# Regex patterns for extracting task_key and max_retries from HCL.
# Handles both top-level tasks and nested for_each_task iteration tasks.
_TASK_KEY_RE = re.compile(r'task_key\s*=\s*"(\w+)"')
_MAX_RETRIES_RE = re.compile(r"max_retries\s*=\s*(\d+)")


def _parse_task_retries() -> dict[str, int]:
    """Parse all task_key → max_retries pairs from the TF file.

    Returns a dict mapping task_key to declared max_retries.
    Only includes tasks that have an explicit max_retries declaration.
    """
    text = _TF_FILE.read_text()
    result: dict[str, int] = {}

    # Split into task blocks by finding `task {` or `task {` inside for_each_task
    # Simple approach: find all (task_key, max_retries) pairs by proximity
    lines = text.splitlines()
    current_task_key: str | None = None

    for line in lines:
        key_match = _TASK_KEY_RE.search(line)
        if key_match:
            current_task_key = key_match.group(1)

        retry_match = _MAX_RETRIES_RE.search(line)
        if retry_match and current_task_key:
            result[current_task_key] = int(retry_match.group(1))
            current_task_key = None  # Reset — next max_retries belongs to a new task

    return result


def test_ingestion_keys_match_tf_max_retries_1() -> None:
    """Every TF task with max_retries=1 must be in _INGESTION_TASK_KEYS."""
    from scripts.patch_job_retries import _INGESTION_TASK_KEYS

    task_retries = _parse_task_retries()
    tf_ingestion = {k for k, v in task_retries.items() if v == 1}

    missing = tf_ingestion - _INGESTION_TASK_KEYS
    assert not missing, (
        f"TF tasks with max_retries=1 not in _INGESTION_TASK_KEYS: {sorted(missing)}. "
        f"Add them to scripts/patch_job_retries.py::_INGESTION_TASK_KEYS."
    )


def test_compute_keys_not_in_ingestion_set() -> None:
    """Every TF task with max_retries=0 must NOT be in _INGESTION_TASK_KEYS."""
    from scripts.patch_job_retries import _INGESTION_TASK_KEYS

    task_retries = _parse_task_retries()
    tf_compute = {k for k, v in task_retries.items() if v == 0}

    misclassified = tf_compute & _INGESTION_TASK_KEYS
    assert not misclassified, (
        f"TF tasks with max_retries=0 found in _INGESTION_TASK_KEYS: {sorted(misclassified)}. "
        f"Remove them — compute tasks must not be retried."
    )


def test_ingestion_keys_exist_in_tf() -> None:
    """_INGESTION_TASK_KEYS must not contain phantom task keys."""
    from scripts.patch_job_retries import _INGESTION_TASK_KEYS

    task_retries = _parse_task_retries()
    all_tf_tasks = set(task_retries.keys())

    phantom = _INGESTION_TASK_KEYS - all_tf_tasks
    assert not phantom, (
        f"_INGESTION_TASK_KEYS contains task keys not in TF: {sorted(phantom)}. "
        f"Remove stale entries from scripts/patch_job_retries.py."
    )


def test_all_tf_tasks_classified() -> None:
    """Every TF task with max_retries declared must be either 0 or 1."""
    task_retries = _parse_task_retries()

    invalid = {k: v for k, v in task_retries.items() if v not in (0, 1)}
    assert not invalid, (
        f"TF tasks with unexpected max_retries values: {invalid}. Expected 0 (compute) or 1 (ingestion)."
    )


# ── absent != explicit 0 (GATE 0 run 941047587677694 regression) ──
# The TF provider drops max_retries=0 (Go omitempty) -> the deployed task has the field ABSENT,
# and the platform applies its default (1 retry) for absent. patch_job_retries.py used
# `.get("max_retries", 0)`, reading absent as 0, so it decided compute tasks were "already 0" and
# made NO change -> the explicit 0 was never sent -> the drain ran with 1 retry and burned a full
# re-drain of DBU. These guard that absent is treated as needing an explicit set.


def test_absent_compute_max_retries_is_patched_to_explicit_zero() -> None:
    from scripts.patch_job_retries import _patch_tasks

    settings = {"tasks": [{"task_key": "compute_tracking_marts_iteration"}]}  # max_retries ABSENT
    changes = _patch_tasks(settings)
    assert settings["tasks"][0]["max_retries"] == 0, "absent compute max_retries must be set to explicit 0"
    assert changes, "absent max_retries must register as a change, not a no-op"


def test_absent_for_each_inner_max_retries_is_patched_to_explicit_zero() -> None:
    from scripts.patch_job_retries import _patch_tasks

    settings = {
        "tasks": [
            {
                "task_key": "compute_tracking_marts",
                "for_each_task": {"task": {"task_key": "compute_tracking_marts_iteration"}},  # inner ABSENT
            }
        ]
    }
    _patch_tasks(settings)
    inner = settings["tasks"][0]["for_each_task"]["task"]
    assert inner["max_retries"] == 0, "absent for_each inner (drain worker) max_retries must be set to explicit 0"


def test_absent_ingestion_max_retries_is_patched_to_one() -> None:
    from scripts.patch_job_retries import _INGESTION_TASK_KEYS, _patch_tasks

    key = next(iter(_INGESTION_TASK_KEYS))
    settings = {"tasks": [{"task_key": key}]}  # ABSENT; ingestion => 1
    _patch_tasks(settings)
    assert settings["tasks"][0]["max_retries"] == 1, f"absent ingestion ({key}) max_retries must be set to explicit 1"


# ── _verify_post_apply detector is NON-VACUOUS (guards-that-cannot-fail) ──
# The post-apply verifier is the fail-loud backstop: if the reset POST silently did not persist the
# explicit 0 (the GATE-0 class — the field stays absent => platform default 1), the verifier MUST raise.
# Watch it FAIL on the exact GATE-0 condition (absent compute max_retries) before trusting the GREEN below;
# a verifier that passes on absence would be vacuous (it must read absent as None != intended 0).


def test_verify_post_apply_raises_on_absent_compute_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.patch_job_retries as p

    # compute task with max_retries ABSENT (== the GATE-0 deployed state: platform default 1).
    job = {"settings": {"tasks": [{"task_key": "compute_tracking_marts_iteration"}]}}
    monkeypatch.setattr(p, "_get_job", lambda base_url, headers, job_id: job)
    with pytest.raises(SystemExit):
        p._verify_post_apply("https://host", {}, 123)


def test_verify_post_apply_passes_on_explicit_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.patch_job_retries as p

    # the FIXED state: compute task carries an explicit 0 → verifier must NOT raise (non-vacuity: it can pass).
    job = {"settings": {"tasks": [{"task_key": "compute_tracking_marts_iteration", "max_retries": 0}]}}
    monkeypatch.setattr(p, "_get_job", lambda base_url, headers, job_id: job)
    p._verify_post_apply("https://host", {}, 123)  # no raise
