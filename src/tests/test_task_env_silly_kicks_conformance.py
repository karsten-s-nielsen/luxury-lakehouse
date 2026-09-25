"""Guard: a mega-job task whose entry-point module imports silly_kicks MUST run on an sk-bearing env.

sk4118 regression (ADR-085): Phase G re-pointed `compute_expected_threat` + `compute_off_ball_xt` to
fit/read a silly-kicks `ExpectedThreat`, but their TF tasks stayed on `environment_key = "default"`
(which has NO silly-kicks — only `analytics` does). The card<->TF env-parity test passed (both said
"default"), and nothing checked that a task's actual imports resolve in its declared env, so both
shipped and failed at runtime with `ModuleNotFoundError: No module named 'silly_kicks'`.

This test closes that gap: it maps every mega-job task -> entry-point module (via pyproject
[project.scripts]) -> module source, and asserts that any task whose module references `silly_kicks`
runs on an sk-bearing environment_key. Non-vacuous: it fails if the scan checks zero sk tasks, and it
pins the two just-fixed producers as positive controls.
"""

from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_TF = _ROOT / "terraform" / "modules" / "workflows" / "main.tf"
_PYPROJECT = _ROOT / "pyproject.toml"
_SRC = _ROOT / "src"

# Environments whose spec declares silly-kicks (see main.tf environment blocks).
_SK_ENVS = frozenset({"analytics"})

# Tasks whose entry-point MODULE mentions silly_kicks but whose ENTRY POINT does not execute it,
# or whose sk use is pre-existing and out of the sk4118 scope. Each needs a documented reason.
_EXEMPT: dict[str, str] = {
    # main_preflight only imports numpy + guards; the module's sk import lives in the compute
    # entry (compute_action_context, which IS on analytics), not the preflight path.
    "preflight_action_context": "preflight entry (main_preflight) is numpy-only; sk import is on the compute path",
    "preflight_idsse": "preflight entry does not run the DFL sk-parse path",
    # NOTE: ingest_idsse_iteration (DFL parse via silly_kicks.providers.sportec, ADR-055) was ALSO on
    # the default env — fixed to analytics in the sk4118 env sweep, so it is no longer exempt (the
    # general offenders check now covers it).
}


def _scripts() -> dict[str, str]:
    text = _PYPROJECT.read_text(encoding="utf-8")
    m = re.search(r"\[project\.scripts\](.*?)(\n\[|\Z)", text, re.DOTALL)
    out: dict[str, str] = {}
    for line in m.group(1).splitlines() if m else []:
        mm = re.match(r'\s*([A-Za-z0-9_]+)\s*=\s*"([^"]+)"', line)
        if mm:
            out[mm.group(1)] = mm.group(2)
    return out


def _tasks() -> list[tuple[str, str, str]]:
    """(task_key, entry_point, environment_key) for each mega-job task definition."""
    tf = _TF.read_text(encoding="utf-8")
    # Split on the aligned task-definition form only (depends_on uses single-space `task_key = "`).
    parts = re.split(r'\n\s+task_key\s{2,}=\s+"', tf)
    rows: list[tuple[str, str, str]] = []
    for b in parts[1:]:
        tk = b[: b.index('"')]
        ep = re.search(r'entry_point\s+=\s+"([^"]+)"', b)
        ek = re.search(r'environment_key\s*=\s*"([^"]+)"', b)
        if ep and ek:
            rows.append((tk, ep.group(1), ek.group(1)))
    return rows


def _module_uses_sk(entry_point: str, scripts: dict[str, str]) -> bool:
    target = scripts.get(entry_point, "")
    module = target.split(":")[0] if target else ""
    if not module:
        return False
    path = _SRC / Path(module.replace(".", "/") + ".py")
    return path.exists() and "silly_kicks" in path.read_text(encoding="utf-8")


def test_sk_tasks_run_on_sk_bearing_env() -> None:
    scripts = _scripts()
    tasks = _tasks()
    assert len(tasks) >= 40, f"task parse looks broken — only {len(tasks)} tasks found"

    checked_sk = 0
    offenders: list[tuple[str, str]] = []
    for tk, ep, ek in tasks:
        if not _module_uses_sk(ep, scripts):
            continue
        checked_sk += 1
        if tk in _EXEMPT:
            continue
        if ek not in _SK_ENVS:
            offenders.append((tk, ek))

    assert checked_sk >= 5, f"non-vacuity: expected several sk-using tasks, checked {checked_sk}"
    assert not offenders, (
        "silly_kicks tasks on a non-sk env (add silly-kicks to that env, move to 'analytics', "
        f"or exempt with a reason): {offenders}"
    )


def test_positive_controls_on_analytics() -> None:
    """The two sk4118-fixed producers + a known sk writer must be on analytics (pins the fix)."""
    env = {tk: ek for tk, _ep, ek in _tasks()}
    for tk in ("compute_expected_threat", "compute_off_ball_xt", "territory_writer"):
        assert env.get(tk) in _SK_ENVS, f"{tk} must run on an sk-bearing env, got {env.get(tk)!r}"
