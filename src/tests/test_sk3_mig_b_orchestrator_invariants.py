"""SK3-MIG-B orchestrator + trainer constant-parity sentinels.

Per spec §2.10 — importlib-based introspection (no regex on dict literals or
docstrings). Catches drift between:

- orchestrator's `_FLAVOR_MAP` and each trainer's `VALIDATED_HF_FLAVOR` (§2.10.1)
- orchestrator's `_TASK_KEY_MAP` values and the dbt seed task list (§2.10.2)
- the dbt seed task list and the live mega-job task_keys (§2.10.3, env-gated)
- trainer-side `silly-kicks` PEP 723 pins (§2.10.4)
- trainer-side `_REQUIRED_SK_MIN` runtime-assertion constants (§2.10.5)

Origin: 2026-05-04 SK3-MIG-B Phase 9 hardening (PR-1). The 2026-05-04 cycle
halted on each of the divergences these sentinels cover; once green, they
prevent reversion.
"""

from __future__ import annotations

import csv
import importlib.util
import os
import re
import sys
from pathlib import Path
from types import ModuleType

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ORCHESTRATOR_PATH = _REPO_ROOT / "scripts" / "sk3_mig_b_retrain.py"
_SEED_PATH = _REPO_ROOT / "dbt_project" / "seeds" / "task_workflow_mapping.csv"
_TRAINER_PATHS: dict[str, Path] = {
    "vaep": _REPO_ROOT / "scripts" / "train_vaep_model_hf.py",
    # xg_v2 (scripts/train_xg_v2_hf.py) retired 2026-07-10 with the v2 producer chain (ADR-066).
    "f2v_v1": _REPO_ROOT / "scripts" / "train_football2vec.py",
    "f2v_v2": _REPO_ROOT / "scripts" / "train_football2vec_v2.py",
    "f2v_360": _REPO_ROOT / "scripts" / "train_football2vec_360.py",
    "scoutgpt": _REPO_ROOT / "scripts" / "train_scoutgpt_hf.py",
}


_SCRIPTS_DIR = _REPO_ROOT / "scripts"


def _load_script_module(path: Path, mod_name: str) -> ModuleType:
    """Load a PEP 723 script as a Python module.

    Idempotent across pytest collection — re-uses sys.modules entry if present.
    Adds `scripts/` to sys.path during the load so that PEP 723 trainers using
    sibling helper modules (e.g. train_football2vec_360_helpers) resolve
    correctly. The `if __name__ == "__main__"` guard in our scripts makes
    import-without-main side-effect-free.
    """
    cached = sys.modules.get(mod_name)
    if cached is not None:
        return cached
    spec = importlib.util.spec_from_file_location(mod_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not build importlib spec for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    scripts_str = str(_SCRIPTS_DIR)
    added = scripts_str not in sys.path
    if added:
        sys.path.insert(0, scripts_str)
    try:
        spec.loader.exec_module(module)
    finally:
        if added:
            sys.path.remove(scripts_str)
    return module


def _seed_task_keys() -> set[str]:
    with _SEED_PATH.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return {row["task_key"] for row in reader}


_TF_TASK_KEY_RE = re.compile(r'task_key\s*=\s*"([^"]+)"')
_TF_WORKFLOW_MODULE = _REPO_ROOT / "terraform" / "modules" / "workflows" / "main.tf"


def _terraform_task_keys() -> set[str]:
    """Extract top-level task_keys from the TF daily-job resource.

    Used to tolerate the merge window where seed + TF ship together but
    TF Apply has not yet run. The regex is intentionally loose (matches
    depends_on refs too) — the superset is harmless because we only
    subtract tf_keys from orphan candidates.
    """
    if not _TF_WORKFLOW_MODULE.exists():
        return set()
    return set(_TF_TASK_KEY_RE.findall(_TF_WORKFLOW_MODULE.read_text(encoding="utf-8")))


# ── §2.10.2 — orchestrator _TASK_KEY_MAP values ⊆ seed task_keys ────────────


def test_orchestrator_task_keys_present_in_seed() -> None:
    """Every value in `_TASK_KEY_MAP` must be a task_key in the dbt seed.

    Catches:
    - typos in mappings (e.g. `compute_defcon` vs `compute_defcon_lite`)
    - stale entries pointing at deleted/never-existed mega-job tasks
      (e.g. `wf_scoutgpt_export` was never a live task)
    """
    orch = _load_script_module(_ORCHESTRATOR_PATH, "sk3_mig_b_retrain")
    assert hasattr(orch, "_TASK_KEY_MAP"), (
        "Orchestrator must expose `_TASK_KEY_MAP` as a module-level dict — "
        "introspectable from CI without regex on a dict literal inside a function body."
    )
    task_key_map: dict[str, str] = orch._TASK_KEY_MAP
    assert isinstance(task_key_map, dict), "_TASK_KEY_MAP must be a dict"
    seed_keys = _seed_task_keys()
    missing = {item: tk for item, tk in task_key_map.items() if tk not in seed_keys}
    assert not missing, (
        f"_TASK_KEY_MAP values not present in dbt seed task_workflow_mapping.csv: {missing}. "
        f"Either fix the mapping or add the task to the seed (and confirm it exists in the live mega-job)."
    )


# ── §2.10.1 — orchestrator _FLAVOR_MAP[item] == trainer.VALIDATED_HF_FLAVOR ──


def test_orchestrator_flavor_map_matches_trainer_constants() -> None:
    """Each `_FLAVOR_MAP[item]` must equal the trainer's `VALIDATED_HF_FLAVOR`.

    Single source of truth per trainer; orchestrator inherits from trainers
    rather than dictating. Catches PR-alpha-style downsizing where the orchestrator
    silently overrode a validated GPU flavor with a smaller one.
    """
    # train_football2vec.py imports databricks-sdk at module level; guard so
    # the test skips when only the default extras are installed (sdk is an
    # optional extra in pyproject.toml).
    pytest.importorskip("databricks.sdk", reason="databricks-sdk not installed (optional 'sdk' extra)")
    orch = _load_script_module(_ORCHESTRATOR_PATH, "sk3_mig_b_retrain")
    assert hasattr(orch, "_FLAVOR_MAP"), (
        "Orchestrator must expose `_FLAVOR_MAP` as a module-level dict (per spec §2.2)."
    )
    flavor_map: dict[str, str] = orch._FLAVOR_MAP
    mismatches: dict[str, tuple[str, str]] = {}
    for item, trainer_path in _TRAINER_PATHS.items():
        trainer = _load_script_module(trainer_path, f"_sk3_trainer_{item}")
        assert hasattr(trainer, "VALIDATED_HF_FLAVOR"), (
            f"Trainer {trainer_path.name} must declare module-level `VALIDATED_HF_FLAVOR: str` per spec §2.3."
        )
        trainer_flavor = trainer.VALIDATED_HF_FLAVOR
        assert item in flavor_map, f"_FLAVOR_MAP missing entry for cycle item {item!r}"
        if flavor_map[item] != trainer_flavor:
            mismatches[item] = (flavor_map[item], trainer_flavor)
    assert not mismatches, (
        f"_FLAVOR_MAP / VALIDATED_HF_FLAVOR mismatches (item: orchestrator vs trainer): {mismatches}. "
        f"Trainer constants are the validated source of truth — sync the orchestrator."
    )


# ── §2.10.3 — seed task_keys ⊆ live mega-job task_keys (env-gated) ──────────


@pytest.mark.skipif(
    not (os.environ.get("DATABRICKS_TOKEN") and os.environ.get("DATABRICKS_HOST")),
    reason="DATABRICKS_TOKEN+DATABRICKS_HOST required to query live mega-job task list",
)
def test_seed_csv_subset_of_live_mega_job() -> None:
    """Seed task_keys must be a subset of the live mega-job's task_keys.

    Catches seed drift after a workflow-card rename / removal that hasn't been
    reflected in the seed yet. Skipped on forks / no-secrets PRs.

    Design: checks ``seed <= (live_keys | tf_keys)`` rather than the stricter
    ``seed <= live_keys`` because seed + TF ship in the same commit while
    TF Apply runs only after merge. A task_key present in TF but not yet
    in the live job is pending deployment, not orphaned. Post-merge, TF
    Apply promotes the key from tf_keys into live_keys — the tolerance is
    self-healing.
    """
    databricks_sdk = pytest.importorskip("databricks.sdk", reason="databricks-sdk not installed (optional 'sdk' extra)")
    WorkspaceClient = databricks_sdk.WorkspaceClient  # noqa: N806

    w = WorkspaceClient()
    jobs = list(w.jobs.list(name="soccer-analytics-ingestion-dev"))
    assert jobs and jobs[0].job_id is not None, "Mega-job 'soccer-analytics-ingestion-dev' not found in workspace"
    job = w.jobs.get(job_id=jobs[0].job_id)
    settings = job.settings
    assert settings is not None, "Mega-job has no settings — workspace API drift?"
    live_keys = {t.task_key for t in (settings.tasks or []) if t.task_key}
    seed_keys = _seed_task_keys()
    # Tasks defined in TF but not yet applied to the live job are expected
    # during the merge window (seed + TF ship together, TF Apply runs after
    # merge). Tolerate them by also accepting TF-defined task_keys.
    tf_keys = _terraform_task_keys()
    orphan = sorted(seed_keys - live_keys - tf_keys)
    assert not orphan, (
        f"Seed task_keys not present in live mega-job or TF: {orphan}. "
        f"Either re-derive the seed from `WorkspaceClient.jobs.get(...).settings.tasks` "
        f"or document the divergence as 'sub_operation_of'."
    )


# ── §2.10.4 — no trainer pins silly-kicks explicitly in PEP 723 deps ────────


_PEP723_HEADER_RE = re.compile(r"^# /// script\s*$", re.MULTILINE)
_PEP723_FOOTER_RE = re.compile(r"^# ///\s*$", re.MULTILINE)
_PEP723_SILLY_KICKS_RE = re.compile(
    r'^#\s+"silly-kicks[^"]*"',  # any "silly-kicks..." dep line
    re.MULTILINE,
)


def _extract_pep723_block(src: str) -> str:
    """Return the PEP 723 metadata block contents (between # /// script and # ///).

    Returns an empty string when no PEP 723 block is present (e.g. non-script
    file). Raises if header found without footer.
    """
    header = _PEP723_HEADER_RE.search(src)
    if header is None:
        return ""
    footer = _PEP723_FOOTER_RE.search(src, pos=header.end())
    if footer is None:
        raise RuntimeError("PEP 723 block opened with `# /// script` but never closed with `# ///`")
    return src[header.end() : footer.start()]


def test_no_trainer_pins_silly_kicks_explicitly() -> None:
    """No trainer may pin `silly-kicks` in its PEP 723 deps.

    The wheel's ``[spadl]`` extra (silly-kicks>=4.43.0,<5) is the single source
    of truth. Trainers install ``luxury-lakehouse[spadl] @ ...wheel`` which
    resolves silly-kicks transitively. uv silently picks a conflicting
    top-level pin over the wheel's transitive pin (verified empirically
    2026-05-04). An explicit ``"silly-kicks..."`` pin in PEP 723 deps is
    therefore an active footgun, not a safety net.

    NOTE: the regex ``r'"silly-kicks'`` intentionally does not match the
    ``luxury-lakehouse[spadl]`` wheel line — the token ``"silly-kicks``
    never appears there.
    """
    offenders: dict[str, str] = {}
    for item, path in _TRAINER_PATHS.items():
        if not path.exists():
            continue
        src = path.read_text(encoding="utf-8")
        block = _extract_pep723_block(src)
        match = _PEP723_SILLY_KICKS_RE.search(block)
        if match is not None:
            offenders[item] = match.group(0).strip()
    assert not offenders, (
        f"Trainers with explicit silly-kicks PEP 723 pins (must be removed): {offenders}. "
        f"Defense against the silent uv-downgrade footgun = runtime version assertion "
        f"in main(), not a PEP 723 dep pin."
    )


# ── §2.10.5 — every trainer declares _REQUIRED_SK_MIN = (4, 43, 0) ──────────
# Floor advanced 4.39.0 -> 4.43.0 (2026-07-11) for the silly-kicks gk_distribution_mask public
# API (F1 — the GK-distribution domain marker `is_gk_distribution` on fct_action_context; goal-kick
# OR acting-GK open-play pass). Additive: no xt_gk/VAEP value change, no retrain (the AC mini-golden
# must NOT move). The floor advances so a stale <4.43.0 worker cannot silently NaN/absent-fill the
# new AC bronze column (build_output missing-col fallback) — the runtime guard catches that drift.
# Floor advanced 4.38.0 -> 4.39.0 (2026-07-01) for the silly-kicks goal-kick actor
# resolver (acting_gk_from_frames): credits goal-kicks to the acting keeper via the
# lakehouse set-piece synthesis override; supersedes 4.38.0 —
# feat/silly-kicks-4-39-0-goalkick-actor-override.
# Floor advanced 4.37.0 -> 4.38.0 (2026-07-01) for the silly-kicks SkillCorner
# GK-identification fix (trust the native roster is_goalkeeper flag; supersedes
# 4.37.0 — feat/silly-kicks-4-38-0-skillcorner-gk-identification).
# Floor advanced 4.36.0 -> 4.37.0 (2026-06-30) for the silly-kicks SkillCorner
# keeper-origin adoption (feat/skillcorner-keeper-origin-access-tier).
# Floor advanced 4.34.0 -> 4.35.0 (2026-06-27) for the silly-kicks xT-GK PEV/DZV
# fidelity fix (ADR-024 amendment upstream; Eyestone Q1-Q3), then 4.35.0 -> 4.36.0
# (2026-06-29) for the xT-GK resolved-coordinate audit columns. 4.36.0 ADDS the four
# `xt_gk_{origin,dest}_{x,y}` audit cols (additive — no xt_gk_* value change). The floor
# advances because the AC bronze contract now REQUIRES those coords: a stale 4.35.0
# worker would silently NaN-fill them (build_output missing-col fallback), defeating the
# audit. The runtime guard's job is to catch exactly that stale-install drift, so 4.36.0
# is the true minimum. See lakehouse ADR-062 (4.35.0) + the 4.36.0 coord migration.
# Floor advanced 4.31.0 -> 4.32.0 (2026-06-17) for the silly-kicks add_* input-purity
# CI gate + the add_gk_distribution_metrics in-place-mutation fix (identity/row-order
# only, no value miscompute, no recompute) + the pitch_control_at_action ->
# pitch_control_at_target FUNCTION rename (emitted column base unchanged). See sk ADR-033.
# Floor advanced 4.30.0 -> 4.31.0 (2026-06-16) for the silly-kicks pitch-control
# rename: pitch_control_at_ball__<method> -> pitch_control_at_target__<method>
# (sampled at the action destination, with the ADR-028 away-team query
# re-projection). BREAKING + atomic with the column migration (ADR-056); the dead
# ~0.5 at-ball fallback is retired for a live at-destination feature.
# Floor advanced 4.27.0 -> 4.30.0 (2026-06-16) for the silly-kicks DFL parse-port
# adoption (ADR-055) + Metrica builder y-fix; adds the `[parse-dfl]` extra.
# Floor advanced 3.30.0 -> 4.0.0 (2026-05-30) to force silly-kicks 4.0.0 everywhere
# (ET-direction symmetric guard via require_et_direction across all 5
# per-period-absolute converters; breaking only for ET matches without the flag).
# Floor advanced 4.0.0 -> 4.1.1 (2026-06-01) to force the @njit(cache=...) default
# change everywhere (cache OFF by default so JIT works on serverless' read-only
# ephemeral wheel path without the "no locator available" import crash; full native
# speed retained).
# Floor advanced 4.1.1 -> 4.2.0 (2026-06-01) to adopt the vectorized ghost-GK KDE
# backend (default kde_backend="vectorized"; ~1.24x faster than the scipy reference
# on CPU, value-equivalent to rtol<=1e-7) + the DAS offside carrier-forwarding fix
# (kills the per-call player_in_possession_col warning flood) + the de-iloc'd
# elastic-sync (~23x faster). Validated locally via scripts/profile_ac1_local.py
# (see ADR-035).
# Floor advanced 4.2.0 -> 4.4.0 (2026-06-02) to adopt silly-kicks 4.4.0 (skips 4.3.0):
# cpu-numba ghost-GK KDE backend + closed-form vectorized whitening (4.3.0) plus the
# 4.4.0 changes. NOTE: the DAS offside carrier-forwarding (shipped 4.2.0) is a confirmed
# correctness fix — the 4.0 golden encoded the pre-fix bug (on-ball carrier mis-flagged
# offside); golden re-baselined to 4.4.0.
# Floor advanced 4.4.0 -> 4.6.0 (2026-06-02) to adopt silly-kicks 4.6.0: the FFT/binned-
# convolution ghost-GK KDE backend (NGP binning; CIC not yet) — the O(grid log grid) lever
# beyond cpu-numba — plus 4.4.1 (DAS "value-neutral" record corrected) and 4.5.0 (cacheable
# infer_ball_carrier).
# Floor advanced 4.6.0 -> 4.9.0 (2026-06-03) to adopt the fft-cic (CIC bilinear binning) ghost-GK
# KDE backend (silly-kicks 4.8.0/4.9.0) as the AC-1 production default — the FFT lever that makes
# a full metrica tracking game finish inside the per-game watchdog (cpu-numba cannot). fft-cic
# approximates the scipy-oracle argmax (95% mode-exact on J03WMX_p1, mean Δ 97mm, entropy err
# <0.3%), so BOTH AC-1 goldens were re-baselined to fft-cic. See ADR-035 (2nd amendment).
# Floor advanced 4.9.0 -> 4.9.1 (2026-06-03) to adopt silly-kicks 4.9.1's DAS empty-frame-batch
# fix (guards the accessible-space None simulation_result on a zero-frame subset; GS-10502 class)
# alongside the xShotOccurrence + gk_influence-zone + SB360-coverage feature work. See ADR-039.
# Floor advanced 4.9.1 -> 4.11.0 (2026-06-03) to adopt the selectable ghost-GK kde_backend +
# ghost_gk_method provenance + period work-units work, absorbing silly-kicks 4.10.0's ghost-GK
# serve-carrier re-baseline (ghost_gk_x/y shift on ~0.4% of frames; both goldens regenerated).
# 4.11.0's xCrossAttempt (TF-17) ships untrained and is NOT consumed. See ADR-035/037 amendments.
# Floor advanced 4.11.0 -> 4.12.0 (2026-06-04) to adopt silly-kicks 4.12.0's period-relative
# time_seconds contract + per-period link-coverage guard (validate_time_base / on_low_coverage);
# purely additive (no AC-1 enrichment value change; goldens unchanged). See ADR-040.
# Floor advanced 4.12.0 -> 4.19.2 (2026-06-04) to adopt silly-kicks 4.19.2's GS goal-capture
# correctness (own goals RE+G -> bad_touch+owngoal geometry-tripwire-guarded; cross-goals CR+G ->
# cross + synthetic shot; nonEvent voided-event exclusion; is_synthetic provenance column) AND the
# codebase-wide VAEP owngoal-label fix (own goals counted in scores/concedes/xG for all providers).
# See silly-kicks ADR-018; lakehouse adoption tracked in project_silly_kicks_413_adoption.
# Floor advanced 4.19.2 -> 4.20.1 (2026-06-09) to adopt the SkillCorner converter fixes
# (period-relative time_seconds; goalkick result via same_team_next), the sportec/IDSSE
# play_evaluation-driven pass/set-piece results + cross-label fix, and the SGM eps-floor.
# Floor advanced 4.20.1 -> 4.27.0 (2026-06-13) to adopt orient_frames_to_ltr (the
# metrica/skillcorner bronze-frame LTR-orientation helper; ADR-029) plus the 4.21-4.26
# adoptions already shipped (space-creation lean contract, GS null-actor NaN identifiers,
# tracking-geometry action-LTR frame unification). See ADR-052 / 4.27.0 adoption.
# Matches the pyproject [spadl] pin `silly-kicks>=4.43.0,<5`.


def test_all_trainers_assert_silly_kicks_runtime_min() -> None:
    """Each trainer must declare module-level `_REQUIRED_SK_MIN = (4, 39, 0)`.

    Per spec §2.10.5: the runtime check inside `main()` is not directly
    introspectable post-hoc, so we assert the constant. Code review covers
    that the constant is actually consulted in `main()`. (Honest about what's
    mechanically testable — Q18 commitment.) The value tracks the pyproject
    [spadl] floor so trainers refuse to run on anything older than the pinned
    silly-kicks.
    """
    # train_football2vec.py imports databricks-sdk at module level.
    pytest.importorskip("databricks.sdk", reason="databricks-sdk not installed (optional 'sdk' extra)")
    missing: list[str] = []
    wrong_value: dict[str, object] = {}
    for item, path in _TRAINER_PATHS.items():
        trainer = _load_script_module(path, f"_sk3_trainer_{item}")
        if not hasattr(trainer, "_REQUIRED_SK_MIN"):
            missing.append(item)
            continue
        expected = (4, 43, 0)
        actual = trainer._REQUIRED_SK_MIN
        if actual != expected:
            wrong_value[item] = actual
    assert not missing, (
        f"Trainers missing module-level `_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 43, 0)`: {missing}"
    )
    assert not wrong_value, f"Trainers with `_REQUIRED_SK_MIN` not equal to (4, 43, 0): {wrong_value}"
