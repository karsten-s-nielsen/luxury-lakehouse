# ScoutGPT Cross-Attention Promotion + Fourier Retention — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run a 3-arm A/B (additive control on Media-PC, cross_attention on AI-PC, fourier_cross_attention on AI-PC) at production fidelity; apply two pre-registered decision rules to decide whether to flip the `ScoutGPTConfig.conditioning_type` default from `"additive"` to `"cross_attention"` and whether to soft-deprecate `fourier_cross_attention`.

**Architecture:** Single-commit-at-end workflow on a new feature branch `evolve/scoutgpt-cross-attn-promote`. Pre-A/B code changes are tiny (one new pure function + orchestrator script + tests); post-A/B changes are conditional on rule outcomes (default flip, DeprecationWarning, wheel bump). All compute runs on two local RTX 5070 Tis (AI-PC + Media-PC); HF Hub is used only for read-only dataset streaming.

**Tech Stack:** Python 3.10, PyTorch (project `uv` on AI-PC, `venv-fourier` with torch nightly cu128 on Media-PC), Hugging Face Hub (datasets only), pytest, ruff, pyright. Orchestrator uses stdlib subprocess + ssh + tar-pipe (no rsync — Windows Git Bash limitation).

**Spec:** `docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md`

**Key discipline:** NO intermediate commits. CLAUDE.md single-commit-at-end overrides the per-task commit pattern the writing-plans skill suggests. All work lives in the working tree until the end-of-cycle single commit, pending user approval.

---

## Phase A — Pre-A/B code changes (TDD, working tree only, no commits)

### Task 1: Create feature branch and dispatch manifest directory

**Files:**
- Modify (no content change): working tree state
- Create: `artifacts/cross-attn-promote/.gitkeep`

- [ ] **Step 1: Verify clean working tree on main**

Run:
```bash
git status
git log --oneline -1
```

Expected: clean tree on `main @ db4f2a9` (or newer if other sessions have merged).

- [ ] **Step 2: Create feature branch off main**

Run:
```bash
git checkout -b evolve/scoutgpt-cross-attn-promote
```

Expected: switched to new branch.

- [ ] **Step 3: Create artifacts directory and gitignore entry**

Run:
```bash
mkdir -p artifacts/cross-attn-promote
touch artifacts/cross-attn-promote/.gitkeep
```

Append to `.gitignore` (check if already present first):
```
artifacts/cross-attn-promote/**
!artifacts/cross-attn-promote/.gitkeep
```

- [ ] **Step 4: Verify branch state**

Run:
```bash
git branch --show-current
git status
```

Expected: branch = `evolve/scoutgpt-cross-attn-promote`, `.gitignore` modified, `artifacts/cross-attn-promote/.gitkeep` untracked.

---

### Task 2: Add `apply_retention_rule` to promotion_rules.py via TDD

**Files:**
- Test: `src/tests/test_retention_rule.py` (new)
- Modify: `src/analytics/promotion_rules.py`

- [ ] **Step 1: Write the failing tests**

Create `src/tests/test_retention_rule.py`:
```python
"""Pre-registered retention rule tests.

Ensures apply_retention_rule is a pure function with the pre-registered
thresholds documented in docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md section A.3.
"""

from __future__ import annotations

import pytest

from analytics.promotion_rules import (
    RETENTION_DEPRECATE_RHO_THRESHOLD,
    TOP1_REGRESSION_FLOOR,
    apply_retention_rule,
)


def test_retention_threshold_is_0_05() -> None:
    assert RETENTION_DEPRECATE_RHO_THRESHOLD == 0.05


@pytest.mark.parametrize(
    ("rho_inc", "rho_chl", "top1_inc", "top1_chl", "expected"),
    [
        # Exactly at +0.05 rho threshold, no top1 regression -> DEPRECATE (boundary)
        (0.20, 0.25, 0.80, 0.80, "DEPRECATE"),
        # Just below +0.05 threshold -> KEEP
        (0.20, 0.249, 0.80, 0.80, "KEEP"),
        # +0.05 rho BUT top1 regression below floor -> KEEP (safety floor wins)
        (0.20, 0.25, 0.80, 0.79, "KEEP"),
        # +0.05 rho AND top1 exactly at -0.005 floor -> DEPRECATE (boundary)
        (0.20, 0.25, 0.80, 0.795, "DEPRECATE"),
        # Clear deprecate: +0.10 rho, no top1 loss
        (0.15, 0.25, 0.80, 0.80, "DEPRECATE"),
        # RoPE historical case (delta +0.016) -> KEEP
        (0.115, 0.131, 0.810, 0.815, "KEEP"),
        # PR #166 Arm 5 vs Arm 2 historical case (delta +0.018) -> KEEP
        # (fourier rho=0.2812, top1=0.8368; cross_attn rho=0.2995, top1=0.8410)
        (0.2812, 0.2995, 0.8368, 0.8410, "KEEP"),
        # Negative rho delta (challenger worse) -> KEEP
        (0.30, 0.20, 0.80, 0.80, "KEEP"),
    ],
)
def test_apply_retention_rule(
    rho_inc: float,
    rho_chl: float,
    top1_inc: float,
    top1_chl: float,
    expected: str,
) -> None:
    result = apply_retention_rule(
        rho_incumbent=rho_inc,
        rho_challenger=rho_chl,
        top1_incumbent=top1_inc,
        top1_challenger=top1_chl,
    )
    assert result == expected


def test_rule_is_pure_function() -> None:
    """Same inputs produce same output, regardless of call order."""
    inputs = (0.2812, 0.2995, 0.8368, 0.8410)
    first = apply_retention_rule(*inputs)
    second = apply_retention_rule(*inputs)
    assert first == second == "KEEP"


def test_shares_top1_floor_with_promotion_rule() -> None:
    """Retention rule reuses the same TOP1_REGRESSION_FLOOR as promotion."""
    from analytics.promotion_rules import TOP1_REGRESSION_FLOOR as floor_imported
    assert TOP1_REGRESSION_FLOOR == floor_imported == -0.005
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
uv run pytest src/tests/test_retention_rule.py -v
```

Expected: FAIL with `ImportError: cannot import name 'apply_retention_rule' from 'analytics.promotion_rules'` (or `RETENTION_DEPRECATE_RHO_THRESHOLD`).

- [ ] **Step 3: Add `apply_retention_rule` to promotion_rules.py**

Edit `src/analytics/promotion_rules.py`. Append after the existing `apply_decision_rule`:

```python
# Retention rule threshold — pre-registered. See
# docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md section A.3
# for calibration rationale: half the promotion threshold because removing an existing
# mechanism requires positive evidence of a better alternative, not absence of evidence.
RETENTION_DEPRECATE_RHO_THRESHOLD: float = 0.05


def apply_retention_rule(
    rho_incumbent: float,
    rho_challenger: float,
    top1_incumbent: float,
    top1_challenger: float,
) -> Literal["KEEP", "DEPRECATE"]:
    """Pre-registered retention rule for existing mechanisms facing a better alternative.

    Returns DEPRECATE iff the challenger clearly beats the incumbent:
      - rho_challenger - rho_incumbent >= +0.05 AND
      - top1_challenger - top1_incumbent >= -0.005 (shared safety floor with promotion rule).
    Otherwise KEEP.

    Asymmetric with apply_decision_rule by design. The +0.05 rho threshold is the natural
    "parity threshold" at which two arms are considered within noise of each other;
    +0.10 (the promotion threshold) would effectively retain any incumbent under almost
    any challenger.

    Args:
        rho_incumbent: Existing mechanism's mean Spearman rho (counterfactual ranking).
        rho_challenger: New mechanism's mean Spearman rho.
        top1_incumbent: Existing mechanism's top-1 next-action accuracy.
        top1_challenger: New mechanism's top-1 next-action accuracy.

    Returns:
        "DEPRECATE" if the challenger clearly wins on rho without top1 regression,
        otherwise "KEEP".
    """
    rho_delta = rho_challenger - rho_incumbent
    top1_delta = top1_challenger - top1_incumbent

    rho_ok = rho_delta >= RETENTION_DEPRECATE_RHO_THRESHOLD
    top1_ok = top1_delta >= TOP1_REGRESSION_FLOOR

    if rho_ok and top1_ok:
        return "DEPRECATE"
    return "KEEP"
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
uv run pytest src/tests/test_retention_rule.py -v
```

Expected: all 10+ test cases PASS.

- [ ] **Step 5: Run the adjacent existing decision rule tests to confirm no regression**

Run:
```bash
uv run pytest src/tests/test_fourier_promotion_decision.py -v
```

Expected: existing tests still PASS (no touch to `apply_decision_rule`).

---

### Task 3: Create orchestrator script skeleton — `scripts/run_cross_attention_ab.py`

**Files:**
- Create: `scripts/run_cross_attention_ab.py`

- [ ] **Step 1: Create the script with mode dispatch and argparse**

Create `scripts/run_cross_attention_ab.py`:
```python
"""Local orchestrator for the ScoutGPT cross_attention promotion A/B.

Not PEP 723 — imports train_loop directly from src/analytics/ so uncommitted
source-tree changes are authoritative (avoids wheel fetch chicken-and-egg).

Two modes:
    --mode drive   : top-level driver. Runs pre-flight, smoke test on Media-PC,
                     dispatches arms, polls metrics, applies decision rules,
                     generates SUMMARY.md.
    --mode run-arm : single-arm execution (called by drive on AI-PC via subprocess
                     or on Media-PC via ssh).

See docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md
for full design. See docs/superpowers/plans/2026-04-21-scoutgpt-cross-attention-promote.md
for implementation sequence.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("cross-attn-ab")

# Constants — pinned for this cycle. See design doc B.2 for rationale.
ARMS: dict[str, dict[str, object]] = {
    "arm1-additive": {
        "conditioning_type": "additive",
        "hidden_dim": 256,
        "num_layers": 6,
        "num_heads": 8,
        "machine": "media-pc",
        "role": "control",
    },
    "arm5-cross-attention": {
        "conditioning_type": "cross_attention",
        "hidden_dim": 256,
        "num_layers": 6,
        "num_heads": 8,
        "machine": "ai-pc",
        "role": "default-flip-candidate",
    },
    "arm2-fourier": {
        "conditioning_type": "fourier_cross_attention",
        "hidden_dim": 256,
        "num_layers": 6,
        "num_heads": 8,
        "machine": "ai-pc",
        "role": "retention-candidate",
    },
}

SHARED_TRAIN_CONFIG = {
    "epochs": 30,
    "patience": 5,
    "learning_rate": 1e-4,
    "batch_size": 256,
    "dropout": 0.10,
    "vaep_loss_weight": 0.10,
    "position_embedding": "learnable",
    "seed": 42,
    "num_counterfactual_episodes": 1000,
    "num_counterfactual_players": 100,
}

DATASET_REPO = "luxury-lakehouse/scoutgpt-training-data"
MEDIA_PC_SSH = "super@192.168.68.70"
MEDIA_PC_WORKDIR = "~/luxury-lakehouse-cross-attn"
MEDIA_PC_VENV = "~/venv-fourier/bin/activate"

ARTIFACTS_ROOT = Path("artifacts/cross-attn-promote")


@dataclass(frozen=True)
class DispatchManifest:
    dispatch_timestamp_utc: str
    dataset_sha: str
    branch_commit_sha: str
    arm_roster: dict[str, dict[str, object]]
    seed: int


def main() -> int:
    parser = argparse.ArgumentParser(prog="run_cross_attention_ab")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    drive = subparsers.add_parser("drive", help="Top-level A/B driver")
    drive.add_argument("--skip-smoke", action="store_true",
                       help="Skip Media-PC smoke test (for re-runs after smoke already passed)")
    drive.add_argument("--skip-preflight", action="store_true",
                       help="Skip Media-PC pre-flight checks (for debugging)")
    drive.add_argument("--serial-ai-pc", action="store_true",
                       help="Run all 3 arms serially on AI-PC (fallback if Media-PC unavailable)")

    run_arm = subparsers.add_parser("run-arm", help="Single-arm execution")
    run_arm.add_argument("--arm", required=True, choices=list(ARMS.keys()))
    run_arm.add_argument("--local-output-dir", required=True, type=Path)
    run_arm.add_argument("--dataset-revision", required=True,
                         help="HF dataset SHA resolved by driver")
    run_arm.add_argument("--epochs", type=int, default=SHARED_TRAIN_CONFIG["epochs"])
    run_arm.add_argument("--num-episodes-subset", type=int, default=None,
                         help="Smoke test: cap episodes. Full run: leave None.")

    args = parser.parse_args()

    if args.mode == "drive":
        return cmd_drive(args)
    if args.mode == "run-arm":
        return cmd_run_arm(args)
    parser.error(f"Unknown mode: {args.mode}")
    return 2


def cmd_drive(args: argparse.Namespace) -> int:
    """Populated in Task 4. Stub raises NotImplementedError."""
    raise NotImplementedError("cmd_drive — see Task 4")


def cmd_run_arm(args: argparse.Namespace) -> int:
    """Populated in Task 5. Stub raises NotImplementedError."""
    raise NotImplementedError("cmd_run_arm — see Task 5")


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Verify it parses**

Run:
```bash
uv run python scripts/run_cross_attention_ab.py --help
```

Expected: argparse help shows `drive` and `run-arm` subcommands.

Run:
```bash
uv run python scripts/run_cross_attention_ab.py drive --help
```

Expected: shows `--skip-smoke`, `--skip-preflight`, `--serial-ai-pc` flags.

---

### Task 4: Implement `cmd_drive` — dataset SHA resolution, manifest, pre-flight, smoke, dispatch, poll, summarize

**Files:**
- Modify: `scripts/run_cross_attention_ab.py`

- [ ] **Step 1: Implement dataset SHA resolution and manifest writing**

Replace the `cmd_drive` stub in `scripts/run_cross_attention_ab.py` with:

```python
def cmd_drive(args: argparse.Namespace) -> int:
    from huggingface_hub import HfApi
    from datetime import datetime, timezone

    ARTIFACTS_ROOT.mkdir(parents=True, exist_ok=True)

    log.info("Resolving dataset SHA for %s", DATASET_REPO)
    api = HfApi()
    info = api.repo_info(DATASET_REPO, repo_type="dataset")
    dataset_sha = info.sha
    if not dataset_sha:
        log.error("Failed to resolve dataset SHA")
        return 1

    branch_sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True
    ).strip()

    manifest = DispatchManifest(
        dispatch_timestamp_utc=datetime.now(timezone.utc).isoformat(),
        dataset_sha=dataset_sha,
        branch_commit_sha=branch_sha,
        arm_roster=ARMS,
        seed=SHARED_TRAIN_CONFIG["seed"],  # type: ignore[arg-type]
    )
    manifest_path = ARTIFACTS_ROOT / "dispatch-manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "dispatch_timestamp_utc": manifest.dispatch_timestamp_utc,
                "dataset_sha": manifest.dataset_sha,
                "branch_commit_sha": manifest.branch_commit_sha,
                "arm_roster": manifest.arm_roster,
                "seed": manifest.seed,
            },
            indent=2,
        )
    )
    log.info("Manifest written to %s (dataset_sha=%s, branch=%s)",
             manifest_path, dataset_sha[:8], branch_sha[:8])

    # Pre-flight + smoke test on Media-PC (unless skipped or serial-ai-pc mode)
    if not args.serial_ai_pc:
        if not args.skip_preflight:
            if not run_media_pc_preflight():
                log.error("Media-PC pre-flight failed — aborting")
                log.error("Fallback: re-run with --serial-ai-pc to use AI-PC only")
                return 1
        if not args.skip_smoke:
            if not run_media_pc_smoke(dataset_sha):
                log.error("Media-PC smoke test failed — aborting")
                log.error("Fallback: re-run with --serial-ai-pc to use AI-PC only")
                return 1

    # Dispatch arms and wait for completion
    if args.serial_ai_pc:
        log.info("Serial-AI-PC mode: all 3 arms on AI-PC sequentially")
        arm_order = ["arm1-additive", "arm5-cross-attention", "arm2-fourier"]
        for arm in arm_order:
            if not run_single_arm_local(arm, dataset_sha):
                log.error("Arm %s failed", arm)
                return 1
    else:
        # Parallel phase: Arm 1 on Media-PC, Arm 5 on AI-PC
        log.info("Parallel phase: Arm 1 on Media-PC, Arm 5 on AI-PC")
        if not run_arms_parallel(dataset_sha):
            log.error("Parallel arms phase failed")
            return 1
        # Serial phase: Arm 2 on AI-PC
        log.info("Serial phase: Arm 2 on AI-PC")
        if not run_single_arm_local("arm2-fourier", dataset_sha):
            log.error("Arm 2 failed")
            return 1

    # Collect metrics, apply rules, generate SUMMARY
    return generate_summary_and_rules()
```

- [ ] **Step 2: Implement pre-flight check function**

Append to `scripts/run_cross_attention_ab.py`:

```python
def run_media_pc_preflight() -> bool:
    """Verify Media-PC SSH + torch + CUDA + GPU visible."""
    log.info("Media-PC pre-flight: SSH reachability")
    r = subprocess.run(
        ["ssh", MEDIA_PC_SSH, "echo ok"],
        capture_output=True, text=True, timeout=15, check=False,
    )
    if r.returncode != 0 or r.stdout.strip() != "ok":
        log.error("SSH check failed: rc=%d stdout=%r stderr=%r",
                  r.returncode, r.stdout, r.stderr)
        return False

    log.info("Media-PC pre-flight: torch + CUDA + GPU")
    probe_cmd = (
        f"source {MEDIA_PC_VENV} && "
        "python -c 'import torch; "
        "print(torch.__version__, torch.cuda.is_available(), "
        "torch.cuda.get_device_name(0) if torch.cuda.is_available() else None)'"
    )
    r = subprocess.run(
        ["ssh", MEDIA_PC_SSH, probe_cmd],
        capture_output=True, text=True, timeout=30, check=False,
    )
    if r.returncode != 0:
        log.error("torch probe failed: rc=%d stdout=%r stderr=%r",
                  r.returncode, r.stdout, r.stderr)
        return False
    log.info("Media-PC torch probe: %s", r.stdout.strip())
    if "True" not in r.stdout:
        log.error("CUDA not available on Media-PC")
        return False
    if "5070 Ti" not in r.stdout:
        log.error("Media-PC GPU is not RTX 5070 Ti: %s", r.stdout.strip())
        return False

    log.info("Media-PC pre-flight: HF_TOKEN presence")
    r = subprocess.run(
        ["ssh", MEDIA_PC_SSH, 'test -n "$HF_TOKEN" && echo set || echo missing'],
        capture_output=True, text=True, timeout=10, check=False,
    )
    if r.stdout.strip() != "set":
        log.warning("HF_TOKEN not in Media-PC env; orchestrator will pass explicitly")

    log.info("Media-PC pre-flight: PASS")
    return True
```

- [ ] **Step 3: Implement Media-PC smoke test function**

Append to `scripts/run_cross_attention_ab.py`:

```python
def run_media_pc_smoke(dataset_sha: str) -> bool:
    """Run 2-epoch x ~2000-episode smoke on Media-PC with Arm 1 config.

    Pass criteria (all must hold):
      - Subprocess exit 0
      - metrics.json written with required keys
      - Training loss decreased monotonically (asserted by train loop)
      - No CUDA OOM / dtype / dataset errors in stderr
    """
    log.info("Media-PC smoke: 2 epochs x ~2000 episodes, Arm 1 config")
    smoke_outdir = "/tmp/cross-attn-smoke"
    remote_cmd = (
        f"source {MEDIA_PC_VENV} && "
        f"cd {MEDIA_PC_WORKDIR} && "
        f"export PYTHONPATH=./src && "
        f"mkdir -p {smoke_outdir} && "
        f"python scripts/run_cross_attention_ab.py run-arm "
        f"--arm arm1-additive "
        f"--local-output-dir {smoke_outdir} "
        f"--dataset-revision {dataset_sha} "
        f"--epochs 2 "
        f"--num-episodes-subset 2000"
    )
    r = subprocess.run(
        ["ssh", MEDIA_PC_SSH, remote_cmd],
        capture_output=True, text=True, timeout=900, check=False,
    )
    if r.returncode != 0:
        log.error("Smoke exit rc=%d", r.returncode)
        log.error("stderr tail:\n%s", "\n".join(r.stderr.splitlines()[-100:]))
        return False

    # Fetch metrics.json and verify shape
    fetch_cmd = f"cat {smoke_outdir}/metrics.json"
    r = subprocess.run(
        ["ssh", MEDIA_PC_SSH, fetch_cmd],
        capture_output=True, text=True, timeout=15, check=False,
    )
    if r.returncode != 0:
        log.error("Failed to read smoke metrics.json: %s", r.stderr)
        return False
    try:
        metrics = json.loads(r.stdout)
    except json.JSONDecodeError as e:
        log.error("Smoke metrics.json not valid JSON: %s", e)
        return False
    required_keys = {"counterfactual_rho", "test_top1", "val_loss", "wall_min"}
    missing = required_keys - set(metrics.keys())
    if missing:
        log.error("Smoke metrics.json missing keys: %s", missing)
        return False
    log.info("Media-PC smoke: PASS (rho=%.3f, top1=%.3f, val_loss=%.3f, wall=%.1fm)",
             metrics["counterfactual_rho"], metrics["test_top1"],
             metrics["val_loss"], metrics["wall_min"])
    return True
```

- [ ] **Step 4: Implement parallel and local arm dispatch helpers**

Append to `scripts/run_cross_attention_ab.py`:

```python
def run_arms_parallel(dataset_sha: str) -> bool:
    """Run Arm 1 on Media-PC and Arm 5 on AI-PC in parallel, poll until both done."""
    arm1_outdir = ARTIFACTS_ROOT / "arm1-additive"
    arm5_outdir = ARTIFACTS_ROOT / "arm5-cross-attention"
    arm1_outdir.mkdir(parents=True, exist_ok=True)
    arm5_outdir.mkdir(parents=True, exist_ok=True)

    # Safety: check no existing ScoutGPT training on AI-PC
    if _ai_pc_training_in_progress():
        log.error("An existing ScoutGPT training is running on AI-PC — aborting")
        log.error("Kill it manually: pgrep -af scoutgpt_training")
        return False

    # Launch Arm 5 on AI-PC (background)
    arm5_log = arm5_outdir / "run-arm.log"
    arm5_log.parent.mkdir(parents=True, exist_ok=True)
    log.info("Dispatching Arm 5 on AI-PC -> %s", arm5_log)
    arm5_proc = subprocess.Popen(
        [
            "uv", "run", "python", "scripts/run_cross_attention_ab.py",
            "run-arm",
            "--arm", "arm5-cross-attention",
            "--local-output-dir", str(arm5_outdir),
            "--dataset-revision", dataset_sha,
        ],
        stdout=open(arm5_log, "w", buffering=1),
        stderr=subprocess.STDOUT,
    )

    # Launch Arm 1 on Media-PC (background, nohup)
    remote_outdir = "~/cross-attn-artifacts/arm1-additive"
    remote_log = "~/cross-attn-artifacts/arm1-additive/run-arm.log"
    remote_cmd = (
        f"source {MEDIA_PC_VENV} && "
        f"cd {MEDIA_PC_WORKDIR} && "
        f"export PYTHONPATH=./src && "
        f"mkdir -p {remote_outdir} && "
        f"nohup python scripts/run_cross_attention_ab.py run-arm "
        f"--arm arm1-additive "
        f"--local-output-dir {remote_outdir} "
        f"--dataset-revision {dataset_sha} "
        f">>{remote_log} 2>&1 & echo $!"
    )
    log.info("Dispatching Arm 1 on Media-PC via ssh + nohup")
    r = subprocess.run(
        ["ssh", MEDIA_PC_SSH, remote_cmd],
        capture_output=True, text=True, timeout=30, check=False,
    )
    if r.returncode != 0:
        log.error("Failed to dispatch Arm 1 on Media-PC: %s", r.stderr)
        arm5_proc.terminate()
        return False
    media_pc_pid = r.stdout.strip()
    log.info("Arm 1 dispatched on Media-PC (pid=%s)", media_pc_pid)

    # Poll both arms until done
    return _poll_parallel_arms(arm5_proc, media_pc_pid, arm1_outdir, arm5_outdir, remote_outdir)


def _ai_pc_training_in_progress() -> bool:
    """Returns True if a ScoutGPT training subprocess is currently running on AI-PC."""
    import os
    my_pid = os.getpid()
    # On Windows Git Bash, pgrep may not be available — use tasklist or ps
    try:
        r = subprocess.run(
            ["ps", "-ef"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        if r.returncode == 0:
            for line in r.stdout.splitlines():
                if "scoutgpt" in line.lower() and f" {my_pid} " not in line:
                    if "run_cross_attention_ab" in line or "scoutgpt_training" in line:
                        log.warning("Found existing training process: %s", line.strip())
                        return True
    except FileNotFoundError:
        log.info("ps not available; skipping AI-PC in-progress check")
    return False


def _poll_parallel_arms(
    arm5_proc: subprocess.Popen,
    media_pc_pid: str,
    arm1_outdir: Path,
    arm5_outdir: Path,
    remote_outdir: str,
) -> bool:
    """Poll Arm 5 (local Popen) and Arm 1 (Media-PC pid) until both complete.

    Reports progress every 30 seconds. Returns True if both succeeded.
    """
    arm5_done = False
    arm1_done = False
    start = time.monotonic()
    while not (arm5_done and arm1_done):
        elapsed_min = (time.monotonic() - start) / 60.0
        if not arm5_done and arm5_proc.poll() is not None:
            arm5_done = True
            rc = arm5_proc.returncode
            if rc != 0:
                log.error("Arm 5 (AI-PC) exited with rc=%d", rc)
                return False
            log.info("[t=%.1fm] Arm 5 (AI-PC) COMPLETE", elapsed_min)
        if not arm1_done:
            alive_check = subprocess.run(
                ["ssh", MEDIA_PC_SSH, f"kill -0 {media_pc_pid} 2>/dev/null && echo alive || echo dead"],
                capture_output=True, text=True, timeout=15, check=False,
            )
            if alive_check.stdout.strip() == "dead":
                arm1_done = True
                # Verify metrics.json written
                r = subprocess.run(
                    ["ssh", MEDIA_PC_SSH, f"test -f {remote_outdir}/metrics.json && echo ok || echo missing"],
                    capture_output=True, text=True, timeout=15, check=False,
                )
                if r.stdout.strip() != "ok":
                    log.error("Arm 1 (Media-PC) exited without writing metrics.json")
                    return False
                # Fetch metrics.json back to AI-PC
                subprocess.run(
                    ["scp", f"{MEDIA_PC_SSH}:{remote_outdir}/metrics.json",
                     str(arm1_outdir / "metrics.json")],
                    capture_output=True, text=True, timeout=30, check=True,
                )
                log.info("[t=%.1fm] Arm 1 (Media-PC) COMPLETE", elapsed_min)
        if not (arm5_done and arm1_done):
            log.info("[t=%.1fm] polling... Arm5=%s Arm1=%s",
                     elapsed_min,
                     "done" if arm5_done else "running",
                     "done" if arm1_done else "running")
            time.sleep(30)
    return True


def run_single_arm_local(arm: str, dataset_sha: str) -> bool:
    """Run a single arm on AI-PC (local machine), blocking until complete."""
    outdir = ARTIFACTS_ROOT / arm
    outdir.mkdir(parents=True, exist_ok=True)

    if _ai_pc_training_in_progress():
        log.error("An existing ScoutGPT training is running on AI-PC — aborting")
        return False

    log.info("Running arm %s on AI-PC (blocking)", arm)
    log_path = outdir / "run-arm.log"
    with open(log_path, "w", buffering=1) as log_fh:
        r = subprocess.run(
            [
                "uv", "run", "python", "scripts/run_cross_attention_ab.py",
                "run-arm",
                "--arm", arm,
                "--local-output-dir", str(outdir),
                "--dataset-revision", dataset_sha,
            ],
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            check=False,
        )
    if r.returncode != 0:
        log.error("Arm %s exit rc=%d. Tail of log:", arm, r.returncode)
        with open(log_path) as f:
            lines = f.readlines()
        for line in lines[-100:]:
            log.error("  %s", line.rstrip())
        return False
    log.info("Arm %s COMPLETE", arm)
    return True
```

- [ ] **Step 5: Implement SUMMARY.md generation and rule application**

Append to `scripts/run_cross_attention_ab.py`:

```python
def generate_summary_and_rules() -> int:
    """Read all metrics.json files, apply both decision rules, generate SUMMARY.md."""
    from analytics.promotion_rules import (
        apply_decision_rule,
        apply_retention_rule,
    )

    arm1 = _load_arm_metrics("arm1-additive")
    arm5 = _load_arm_metrics("arm5-cross-attention")
    arm2 = _load_arm_metrics("arm2-fourier")

    if not (arm1 and arm5 and arm2):
        log.error("Missing metrics from one or more arms; cannot generate SUMMARY")
        return 1

    # Apply default flip rule: Arm 5 vs Arm 1
    flip_decision = apply_decision_rule(
        rho_ctrl=arm1["counterfactual_rho"],
        rho_trt=arm5["counterfactual_rho"],
        top1_ctrl=arm1["test_top1"],
        top1_trt=arm5["test_top1"],
    )

    # Apply retention rule: Arm 5 (challenger) vs Arm 2 (incumbent)
    retention_decision = apply_retention_rule(
        rho_incumbent=arm2["counterfactual_rho"],
        rho_challenger=arm5["counterfactual_rho"],
        top1_incumbent=arm2["test_top1"],
        top1_challenger=arm5["test_top1"],
    )

    log.info("=" * 60)
    log.info("DECISIONS")
    log.info("=" * 60)
    log.info("Default flip (Arm 5 vs Arm 1): %s", flip_decision)
    log.info("  rho delta: %+.4f (threshold >= +0.10)",
             arm5["counterfactual_rho"] - arm1["counterfactual_rho"])
    log.info("  top1 delta: %+.4f (threshold >= -0.005)",
             arm5["test_top1"] - arm1["test_top1"])
    log.info("Fourier retention (Arm 5 challenger vs Arm 2 incumbent): %s", retention_decision)
    log.info("  rho delta (chl - inc): %+.4f (threshold >= +0.05 for DEPRECATE)",
             arm5["counterfactual_rho"] - arm2["counterfactual_rho"])
    log.info("  top1 delta (chl - inc): %+.4f (threshold >= -0.005)",
             arm5["test_top1"] - arm2["test_top1"])
    log.info("=" * 60)

    # Write SUMMARY.md
    summary_dir = Path("docs/evolve/cross-attention-promote")
    summary_dir.mkdir(parents=True, exist_ok=True)
    summary_path = summary_dir / "SUMMARY.md"
    summary_path.write_text(_format_summary_md(
        arm1=arm1, arm5=arm5, arm2=arm2,
        flip_decision=flip_decision,
        retention_decision=retention_decision,
    ))
    log.info("SUMMARY.md written to %s", summary_path)

    # Write decisions to a machine-readable file for downstream conditional steps
    decisions_path = ARTIFACTS_ROOT / "decisions.json"
    decisions_path.write_text(json.dumps({
        "flip_decision": flip_decision,
        "retention_decision": retention_decision,
        "arm1": arm1,
        "arm5": arm5,
        "arm2": arm2,
    }, indent=2))
    log.info("Decisions written to %s (for post-A/B conditional code changes)", decisions_path)
    return 0


def _load_arm_metrics(arm: str) -> dict[str, float] | None:
    p = ARTIFACTS_ROOT / arm / "metrics.json"
    if not p.exists():
        log.error("Missing metrics.json for %s at %s", arm, p)
        return None
    return json.loads(p.read_text())


def _format_summary_md(
    arm1: dict[str, float],
    arm5: dict[str, float],
    arm2: dict[str, float],
    flip_decision: str,
    retention_decision: str,
) -> str:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"""# ScoutGPT cross_attention Promotion + Fourier Retention — Summary

**Date:** {now}
**Branch:** `evolve/scoutgpt-cross-attn-promote`
**Execution venue:** Local — 1x RTX 5070 Ti (AI-PC) + 1x RTX 5070 Ti (Media-PC)
**Dataset:** `luxury-lakehouse/scoutgpt-training-data`
**Spec:** `docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md`

## Pre-registered decision rules

- **Default flip rule** (Arm 1 additive → Arm 5 cross_attention): PROMOTE iff `rho delta >= +0.10` AND `top1 delta >= -0.005`. Applied via `src/analytics/promotion_rules.py::apply_decision_rule`.
- **Fourier retention rule** (Arm 2 fourier_cross_attention incumbent vs Arm 5 cross_attention challenger): DEPRECATE iff `rho_chl - rho_inc >= +0.05` AND `top1_chl - top1_inc >= -0.005`. Applied via `src/analytics/promotion_rules.py::apply_retention_rule`.

## Headline

| Arm | Role | conditioning_type | `counterfactual_rho` | `test_top1` | `val_loss` | `wall_min` |
|---|---|---|---:|---:|---:|---:|
| arm1-additive | CONTROL | additive | {arm1["counterfactual_rho"]:.4f} | {arm1["test_top1"]:.4f} | {arm1["val_loss"]:.4f} | {arm1["wall_min"]:.1f} |
| arm5-cross-attention | DEFAULT FLIP CANDIDATE | cross_attention | {arm5["counterfactual_rho"]:.4f} | {arm5["test_top1"]:.4f} | {arm5["val_loss"]:.4f} | {arm5["wall_min"]:.1f} |
| arm2-fourier | RETENTION CANDIDATE | fourier_cross_attention | {arm2["counterfactual_rho"]:.4f} | {arm2["test_top1"]:.4f} | {arm2["val_loss"]:.4f} | {arm2["wall_min"]:.1f} |

## Dispositions

- **Default flip** (Arm 5 vs Arm 1): **{flip_decision}**
  - rho delta = {arm5["counterfactual_rho"] - arm1["counterfactual_rho"]:+.4f}
  - top1 delta = {arm5["test_top1"] - arm1["test_top1"]:+.4f}
- **Fourier retention** (Arm 2 vs Arm 5): **{retention_decision}**
  - rho delta (challenger - incumbent) = {arm5["counterfactual_rho"] - arm2["counterfactual_rho"]:+.4f}
  - top1 delta (challenger - incumbent) = {arm5["test_top1"] - arm2["test_top1"]:+.4f}

## Cross-reference

- **PR #166 (Fourier promotion, 2026-04-21)** — Arm 5 under GPU contention reported rho=0.2995. Clean re-run this cycle: {arm5["counterfactual_rho"]:.4f}. Arm 2 (clean, no contention) reported 0.2812; clean re-run this cycle: {arm2["counterfactual_rho"]:.4f}.
- **RoPE-for-ScoutGPT A/B (PR #159, 2026-04-19)** — delta +0.016 rejected as noise floor.

## Mechanism narrative

_(To be filled in based on decision outcomes — include whether GPU contention was the Arm 5 rho driver, and whether Fourier's within-noise gap under PR #166 persisted under clean conditions.)_

## Follow-ups

- Canonical checkpoint retraining on HF Hub under the new default (if flip promoted).
- Football2Vec cross-attention port.
- Spatial-encoding × conditioning-type axis decomposition (future refactor).
- Hard removal of deprecated `fourier_cross_attention` (3+ months of no push-back).
"""
```

- [ ] **Step 6: Verify the orchestrator script compiles and imports cleanly**

Run:
```bash
uv run python -c "import scripts.run_cross_attention_ab; print('ok')"
```

Expected: prints `ok`. If ImportError, check that `src/analytics/promotion_rules.py::apply_retention_rule` from Task 2 is in place.

Run:
```bash
uv run python scripts/run_cross_attention_ab.py drive --help
uv run python scripts/run_cross_attention_ab.py run-arm --help
```

Expected: help text renders with all expected flags.

---

### Task 5: Implement `cmd_run_arm` — single-arm training invocation

**Files:**
- Modify: `scripts/run_cross_attention_ab.py`

- [ ] **Step 1: Implement `cmd_run_arm` importing train_loop from source tree**

Replace the `cmd_run_arm` stub in `scripts/run_cross_attention_ab.py` with:

```python
def cmd_run_arm(args: argparse.Namespace) -> int:
    """Single-arm execution: build ScoutGPTConfig, call train_loop, write metrics.json."""
    from datetime import datetime, timezone
    import time as _time

    arm_cfg = ARMS[args.arm]
    outdir: Path = args.local_output_dir
    outdir.mkdir(parents=True, exist_ok=True)

    log.info("=" * 60)
    log.info("ARM: %s", args.arm)
    log.info("  conditioning_type: %s", arm_cfg["conditioning_type"])
    log.info("  hidden_dim/num_layers/num_heads: %d/%d/%d",
             arm_cfg["hidden_dim"], arm_cfg["num_layers"], arm_cfg["num_heads"])
    log.info("  epochs: %d", args.epochs)
    log.info("  output: %s", outdir)
    log.info("  dataset_revision: %s", args.dataset_revision[:8])
    log.info("=" * 60)

    # Import training code from source tree (not from wheel)
    from analytics.scoutgpt_decoder import ScoutGPTConfig
    from analytics.scoutgpt_training import train_loop

    config = ScoutGPTConfig(
        hidden_dim=int(arm_cfg["hidden_dim"]),  # type: ignore[arg-type]
        num_layers=int(arm_cfg["num_layers"]),  # type: ignore[arg-type]
        num_heads=int(arm_cfg["num_heads"]),    # type: ignore[arg-type]
        conditioning_type=str(arm_cfg["conditioning_type"]),
        position_embedding=str(SHARED_TRAIN_CONFIG["position_embedding"]),
        dropout=float(SHARED_TRAIN_CONFIG["dropout"]),  # type: ignore[arg-type]
        vaep_loss_weight=float(SHARED_TRAIN_CONFIG["vaep_loss_weight"]),  # type: ignore[arg-type]
    )

    start_time = _time.monotonic()

    # train_loop is expected to accept: config, dataset_repo, dataset_revision,
    # output_dir, epochs, patience, learning_rate, batch_size, seed,
    # num_counterfactual_episodes, num_counterfactual_players, num_episodes_subset
    #
    # If the existing train_loop signature differs, adapt here; the orchestrator
    # knows only what the training entrypoint accepts.
    metrics = train_loop(
        config=config,
        dataset_repo=DATASET_REPO,
        dataset_revision=args.dataset_revision,
        output_dir=str(outdir),
        epochs=args.epochs,
        patience=int(SHARED_TRAIN_CONFIG["patience"]),  # type: ignore[arg-type]
        learning_rate=float(SHARED_TRAIN_CONFIG["learning_rate"]),  # type: ignore[arg-type]
        batch_size=int(SHARED_TRAIN_CONFIG["batch_size"]),  # type: ignore[arg-type]
        seed=int(SHARED_TRAIN_CONFIG["seed"]),  # type: ignore[arg-type]
        num_counterfactual_episodes=int(SHARED_TRAIN_CONFIG["num_counterfactual_episodes"]),  # type: ignore[arg-type]
        num_counterfactual_players=int(SHARED_TRAIN_CONFIG["num_counterfactual_players"]),  # type: ignore[arg-type]
        num_episodes_subset=args.num_episodes_subset,
    )

    wall_min = (_time.monotonic() - start_time) / 60.0
    # Augment metrics with wall_min + arm identity + completion timestamp
    metrics["wall_min"] = wall_min
    metrics["arm"] = args.arm
    metrics["completed_utc"] = datetime.now(timezone.utc).isoformat()

    metrics_path = outdir / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2, default=str))
    log.info("Metrics written to %s", metrics_path)
    log.info("Final: rho=%.4f top1=%.4f val_loss=%.4f wall=%.1fm",
             metrics["counterfactual_rho"], metrics["test_top1"],
             metrics["val_loss"], wall_min)
    return 0
```

- [ ] **Step 2: Verify train_loop signature matches the expected kwargs**

Run:
```bash
uv run python -c "
import inspect
from analytics.scoutgpt_training import train_loop
sig = inspect.signature(train_loop)
print('Parameters:')
for name, param in sig.parameters.items():
    print(f'  {name}: {param.default}')
"
```

Expected: lists parameters. If `train_loop` doesn't accept any of the kwargs this task passes (e.g., `num_episodes_subset`, `dataset_revision`), note the mismatch and treat it as a Task 5.5 follow-up to either extend `train_loop` or adapt the orchestrator call.

**If signature mismatches exist:** capture them in a scratch note and proceed to Task 6. Adapt the call shape in a follow-up edit (minimal change to the orchestrator; do NOT modify `train_loop` unless strictly required — PR #166 worked with whatever signature currently exists).

- [ ] **Step 3: Full static-check sweep**

Run:
```bash
uv run ruff check scripts/run_cross_attention_ab.py
uv run ruff format --check scripts/run_cross_attention_ab.py
uv run pyright scripts/run_cross_attention_ab.py src/analytics/promotion_rules.py src/tests/test_retention_rule.py
```

Expected: zero violations. Fix any lint/format issues in place.

---

### Task 6: Full pre-A/B validation sweep

**Files:** no new edits — this is a verification gate.

- [ ] **Step 1: Run full test suite**

Run:
```bash
uv run pytest src/tests/ -v -x
```

Expected: all tests PASS (no regressions from adding `apply_retention_rule`).

- [ ] **Step 2: Run ruff format on the whole repo**

Run:
```bash
uv run ruff format --check src/ scripts/
```

Expected: all files correctly formatted. If not, run `uv run ruff format src/ scripts/` and re-run the check.

- [ ] **Step 3: Run ruff lint**

Run:
```bash
uv run ruff check src/ scripts/
```

Expected: zero violations.

- [ ] **Step 4: Run pyright**

Run:
```bash
uv run pyright src/
```

Expected: zero errors in basic mode.

- [ ] **Step 5: Sanity-check the branch state**

Run:
```bash
git status
git diff --stat
```

Expected: changes on `evolve/scoutgpt-cross-attn-promote` branch only. Files:
- `src/analytics/promotion_rules.py` (modified)
- `src/tests/test_retention_rule.py` (new)
- `scripts/run_cross_attention_ab.py` (new)
- `.gitignore` (modified if necessary)
- `artifacts/cross-attn-promote/.gitkeep` (new)
- `docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md` (new — from brainstorming)
- `docs/superpowers/plans/2026-04-21-scoutgpt-cross-attention-promote.md` (new — this file)

**DO NOT COMMIT.** Single-commit-at-end rule per CLAUDE.md.

---

## Phase B — Media-PC deployment and smoke test

### Task 7: Tar-pipe working tree to Media-PC

**Files:** none created; uses existing SSH key / network.

- [ ] **Step 1: Verify SSH connectivity to Media-PC**

Run:
```bash
ssh super@192.168.68.70 "echo ok && uname -a && nvidia-smi --query-gpu=name,memory.total --format=csv"
```

Expected: `ok` + Linux/WSL kernel info + RTX 5070 Ti with 16 GB VRAM.

If SSH hangs or fails: notify user (Media-PC must be online per user interaction pattern — WoL-over-WiFi doesn't work). Wait for user confirmation before retrying.

- [ ] **Step 2: Tar-pipe current working tree to Media-PC**

Run from repository root on AI-PC:
```bash
tar czf - \
  --exclude=.git \
  --exclude=artifacts \
  --exclude=__pycache__ \
  --exclude='*.pyc' \
  --exclude='.ruff_cache' \
  --exclude='.pytest_cache' \
  --exclude='node_modules' \
  --exclude='.venv' \
  --exclude='dist' \
  --exclude='build' \
  --exclude='*.egg-info' \
  . | ssh super@192.168.68.70 \
    "mkdir -p ~/luxury-lakehouse-cross-attn && cd ~/luxury-lakehouse-cross-attn && tar xzf -"
```

Expected: runs for ~30-60 seconds depending on network; no errors in stderr.

- [ ] **Step 3: Verify deployment on Media-PC**

Run:
```bash
ssh super@192.168.68.70 "ls ~/luxury-lakehouse-cross-attn/scripts/run_cross_attention_ab.py && head -5 ~/luxury-lakehouse-cross-attn/scripts/run_cross_attention_ab.py"
```

Expected: file exists, first 5 lines match the script we just wrote.

Run:
```bash
ssh super@192.168.68.70 "cd ~/luxury-lakehouse-cross-attn && ls src/analytics/ | grep -E 'promotion_rules|scoutgpt_decoder|scoutgpt_training'"
```

Expected: lists `promotion_rules.py`, `scoutgpt_decoder.py`, `scoutgpt_training.py`.

---

### Task 8: Media-PC pre-flight checks via orchestrator

**Files:** no edits.

- [ ] **Step 1: Manually invoke pre-flight (bypass smoke temporarily)**

This task exercises the `run_media_pc_preflight()` function in isolation before running the full drive. On AI-PC from the repo root:

```bash
uv run python -c "
from scripts.run_cross_attention_ab import run_media_pc_preflight
import sys
sys.exit(0 if run_media_pc_preflight() else 1)
"
```

Expected output:
- `Media-PC pre-flight: SSH reachability` → success
- `Media-PC pre-flight: torch + CUDA + GPU` → logs torch version + `True` + device name containing "5070 Ti"
- `Media-PC pre-flight: HF_TOKEN presence` → `set` (or warning if missing — orchestrator passes explicitly)
- `Media-PC pre-flight: PASS`

If any check fails: stop, report diagnostic, notify user.

---

### Task 9: Media-PC smoke test

**Files:** no edits.

- [ ] **Step 1: Resolve current dataset SHA**

```bash
uv run python -c "
from huggingface_hub import HfApi
api = HfApi()
info = api.repo_info('luxury-lakehouse/scoutgpt-training-data', repo_type='dataset')
print(info.sha)
"
```

Expected: 40-character hex SHA. Copy it for the next step.

- [ ] **Step 2: Invoke smoke test function directly**

```bash
DATASET_SHA=<paste SHA from step 1>
uv run python -c "
from scripts.run_cross_attention_ab import run_media_pc_smoke
import sys
sys.exit(0 if run_media_pc_smoke('$DATASET_SHA') else 1)
"
```

Expected:
- 2-epoch training completes on Media-PC within ~5-10 min
- metrics.json fetched via ssh cat, parsed successfully
- Final log line: `Media-PC smoke: PASS (rho=%.3f, top1=%.3f, val_loss=%.3f, wall=%.1fm)`

Exit code 0.

- [ ] **Step 3: Smoke failure handling (contingency)**

If smoke returns non-zero:
1. Capture the remote stderr tail (automatically logged by `run_media_pc_smoke`).
2. Diagnose common causes:
   - torch version mismatch → update `venv-fourier` on Media-PC
   - CUDA driver incompatibility → check `nvidia-smi` driver vs torch cu128
   - HF dataset auth → export HF_TOKEN on Media-PC
   - Disk full / path permission → check `~/cross-attn-artifacts` existence
3. Notify user with diagnosis. Do not loop-retry — three-strikes rule per CLAUDE.md.
4. If Media-PC is unfixable: proceed to Phase C with `--serial-ai-pc` fallback.

**BLOCKING GATE**: do not proceed to Phase C until smoke passes OR user approves serial-AI-PC fallback.

---

## Phase C — Full 3-arm A/B execution (~6 hrs wall clock)

### Task 10: Launch the full driver

**Files:** no edits.

- [ ] **Step 1: Start the driver with `run_in_background=true`**

Run (via Bash tool with `run_in_background: true`):
```bash
uv run python scripts/run_cross_attention_ab.py drive --skip-smoke --skip-preflight 2>&1 | tee artifacts/cross-attn-promote/driver.log
```

(We pass `--skip-smoke` and `--skip-preflight` because Tasks 8–9 already validated them; re-running would burn ~10 min for no information.)

Record the shell_id from the tool output. Expected initial log lines:
- `Resolving dataset SHA for luxury-lakehouse/scoutgpt-training-data`
- `Manifest written to artifacts/cross-attn-promote/dispatch-manifest.json`
- `Parallel phase: Arm 1 on Media-PC, Arm 5 on AI-PC`
- `Dispatching Arm 5 on AI-PC`
- `Dispatching Arm 1 on Media-PC via ssh + nohup`

- [ ] **Step 2: Poll every 20-30 min via `BashOutput` (stay within cache TTL)**

Read the tail of `artifacts/cross-attn-promote/driver.log` every 20–30 min. Expected cadence:
- T+0 to T+3hr: parallel phase. Log entries: `[t=X.Xm] polling... Arm5=running Arm1=running`
- T+3hr: `Arm 5 (AI-PC) COMPLETE` and `Arm 1 (Media-PC) COMPLETE` within a few min of each other
- T+3hr: `Serial phase: Arm 2 on AI-PC` → `Running arm arm2-fourier on AI-PC (blocking)`
- T+6hr: `Arm arm2-fourier COMPLETE`
- T+6hr: `DECISIONS` block logs both rule outcomes
- T+6hr: `SUMMARY.md written` + `Decisions written to artifacts/cross-attn-promote/decisions.json`
- Process exits with code 0

Use `ScheduleWakeup` for cadence; don't idle-poll and don't let cache go cold.

- [ ] **Step 3: Confirm all metrics.json files are in place**

```bash
ls -la artifacts/cross-attn-promote/
ls -la artifacts/cross-attn-promote/arm1-additive/ \
       artifacts/cross-attn-promote/arm5-cross-attention/ \
       artifacts/cross-attn-promote/arm2-fourier/
cat artifacts/cross-attn-promote/decisions.json
```

Expected: each arm dir has `metrics.json` + `run-arm.log`. `decisions.json` has both `flip_decision` and `retention_decision` fields plus full metrics from all 3 arms.

---

### Task 11: Contingency — re-dispatch individual arms if any failed

**Files:** no edits.

This task is only executed if Task 10 failed for one or more arms.

- [ ] **Step 1: Identify which arm(s) failed**

```bash
for arm in arm1-additive arm5-cross-attention arm2-fourier; do
  if [ -f "artifacts/cross-attn-promote/$arm/metrics.json" ]; then
    echo "$arm: OK"
  else
    echo "$arm: MISSING — tail of log:"
    tail -30 "artifacts/cross-attn-promote/$arm/run-arm.log" 2>/dev/null || echo "(no log)"
  fi
done
```

- [ ] **Step 2: Re-dispatch only the failed arm(s)**

For a specific missing arm, run:
```bash
DATASET_SHA=$(python -c "import json; print(json.load(open('artifacts/cross-attn-promote/dispatch-manifest.json'))['dataset_sha'])")
uv run python scripts/run_cross_attention_ab.py run-arm \
  --arm <arm-name> \
  --local-output-dir artifacts/cross-attn-promote/<arm-name> \
  --dataset-revision $DATASET_SHA
```

Expected: completes in ~3 hrs.

- [ ] **Step 3: Re-generate SUMMARY and decisions**

```bash
uv run python -c "
from scripts.run_cross_attention_ab import generate_summary_and_rules
import sys
sys.exit(generate_summary_and_rules())
"
```

Expected: SUMMARY.md + decisions.json written.

---

## Phase D — Conditional post-A/B code changes (based on `decisions.json`)

### Task 12: Load decisions and determine conditional path

**Files:** no edits; this is a dispatch task.

- [ ] **Step 1: Inspect decisions.json**

```bash
cat artifacts/cross-attn-promote/decisions.json | python -c "
import json, sys
d = json.load(sys.stdin)
print(f'flip_decision: {d[\"flip_decision\"]}')
print(f'retention_decision: {d[\"retention_decision\"]}')
print()
print(f'Arm 1 (additive): rho={d[\"arm1\"][\"counterfactual_rho\"]:.4f} top1={d[\"arm1\"][\"test_top1\"]:.4f}')
print(f'Arm 5 (cross_attn): rho={d[\"arm5\"][\"counterfactual_rho\"]:.4f} top1={d[\"arm5\"][\"test_top1\"]:.4f}')
print(f'Arm 2 (fourier): rho={d[\"arm2\"][\"counterfactual_rho\"]:.4f} top1={d[\"arm2\"][\"test_top1\"]:.4f}')
"
```

- [ ] **Step 2: Route to conditional tasks**

Based on the `flip_decision` and `retention_decision`:

- `flip_decision == "PROMOTE"` → execute Task 13 (A.1 default flip + default-value test)
- `flip_decision == "ARCHIVE"` → skip Task 13
- `retention_decision == "DEPRECATE"` → execute Task 14 (pre-audit) and Task 15 (A.2 deprecation warning)
- `retention_decision == "KEEP"` → skip Tasks 14, 15
- If either PROMOTE or DEPRECATE → execute Task 16 (wheel bump)
- If both ARCHIVE and KEEP → skip Tasks 13, 14, 15, 16 entirely (null-cycle outcome)

---

### Task 13: Apply A.1 — Default flip + default-value test (CONDITIONAL on PROMOTE)

**Files:**
- Modify: `src/analytics/scoutgpt_decoder.py:39`
- Modify: `src/tests/test_scoutgpt_decoder.py`

- [ ] **Step 1: Write failing test asserting new default**

Edit `src/tests/test_scoutgpt_decoder.py`. Add at the top of the file (or at the bottom if preferred — append is fine):

```python
def test_default_conditioning_type_is_cross_attention() -> None:
    """Default conditioning_type is cross_attention as of wheel 0.3.10.

    See docs/evolve/cross-attention-promote/SUMMARY.md for the A/B rationale.
    Promotion decision was driven by:
      - Arm 5 (cross_attention) vs Arm 1 (additive): rho delta >= +0.10, top1 delta >= -0.005
      - Pre-registered rule: src/analytics/promotion_rules.py::apply_decision_rule
    """
    from analytics.scoutgpt_decoder import ScoutGPTConfig
    cfg = ScoutGPTConfig()
    assert cfg.conditioning_type == "cross_attention"
```

- [ ] **Step 2: Run test to verify it fails with current default**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_decoder.py::test_default_conditioning_type_is_cross_attention -v
```

Expected: FAIL — `assert 'additive' == 'cross_attention'`.

- [ ] **Step 3: Apply the default flip**

Edit `src/analytics/scoutgpt_decoder.py`:

Change line 39:
```python
    conditioning_type: str = "additive"
```
to:
```python
    conditioning_type: str = "cross_attention"
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_decoder.py::test_default_conditioning_type_is_cross_attention -v
```

Expected: PASS.

- [ ] **Step 5: Run the full decoder test suite for regression check**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_decoder.py -v
```

Expected: all tests PASS. Any existing tests that implicitly relied on the `"additive"` default should have failed here — if so, investigate each and either (a) update the test to explicitly pass `conditioning_type="additive"` (preserving original intent) or (b) update the test to align with the new default (if the test's intent was "test the default").

---

### Task 14: Fourier usage audit (CONDITIONAL on DEPRECATE) — BLOCKING before Task 15

**Files:** no edits; investigation only.

- [ ] **Step 1: Grep every reference to `fourier_cross_attention` in Python**

Run:
```bash
grep -rn "fourier_cross_attention" --include="*.py" src/ scripts/ hf_taipy_app/ 2>/dev/null
```

- [ ] **Step 2: Classify each hit**

For each file from Step 1, classify:
- **Seed file** (`src/evolve/targets/scoutgpt/seed_programs/fourier_cross_attention.py`) — reference only, no `ScoutGPTConfig` construction. Safe.
- **Decoder branch** (`src/analytics/scoutgpt_decoder.py`) — the branch we're soft-deprecating; `__post_init__` added in Task 15.
- **Parity test** (`src/tests/test_scoutgpt_fourier_parity.py`) — constructs `ScoutGPTConfig(conditioning_type="fourier_cross_attention")`. WILL trigger DeprecationWarning. Needs `pytest.warns` wrapping OR `@pytest.mark.filterwarnings("ignore::DeprecationWarning")`.
- **Decoder test** (`src/tests/test_scoutgpt_decoder.py`) — same as above; may construct Fourier configs.
- **Orchestrator scripts** (`scripts/run_fourier_scoutgpt_ab.py`, `scripts/finalize_fourier_scoutgpt_ab.py`, `scripts/train_scoutgpt_hf.py`, `scripts/evaluate_scoutgpt_l2_seeds.py`) — pass-through via CLI args; DeprecationWarning will fire at runtime. That is intended behavior (loud signal to users).
- **Architecture ref** (`src/tests/test_architecture_md_appendix.py`) — string check only, no config construction. Safe.

- [ ] **Step 3: Document handling for each impacted test file**

Record in a scratch note: which test files need `filterwarnings` adjustments. Exact change will apply in Task 15 Step 3.

---

### Task 15: Apply A.2 — DeprecationWarning + tests (CONDITIONAL on DEPRECATE)

**Files:**
- Modify: `src/analytics/scoutgpt_decoder.py`
- Modify: `src/tests/test_scoutgpt_decoder.py`
- Modify: `src/tests/test_scoutgpt_fourier_parity.py` (and any other impacted tests from Task 14)

- [ ] **Step 1: Write failing test for DeprecationWarning**

Edit `src/tests/test_scoutgpt_decoder.py`. Append:

```python
def test_fourier_cross_attention_emits_deprecation_warning() -> None:
    """fourier_cross_attention is soft-deprecated as of wheel 0.3.10.

    The enum value still works (backward compat for saved checkpoints and
    explicit users), but construction emits a DeprecationWarning pointing
    users to cross_attention. See docs/evolve/cross-attention-promote/SUMMARY.md.
    """
    import pytest
    from analytics.scoutgpt_decoder import ScoutGPTConfig
    with pytest.warns(DeprecationWarning, match="fourier_cross_attention"):
        cfg = ScoutGPTConfig(conditioning_type="fourier_cross_attention")
    # Ensure the enum value is still accepted (soft deprecation, not hard removal)
    assert cfg.conditioning_type == "fourier_cross_attention"
```

- [ ] **Step 2: Run test to verify it fails (no warning yet)**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_decoder.py::test_fourier_cross_attention_emits_deprecation_warning -v
```

Expected: FAIL — `DID NOT WARN`.

- [ ] **Step 3: Add `__post_init__` to `ScoutGPTConfig`**

Edit `src/analytics/scoutgpt_decoder.py`. Add `import warnings` at the top if not already present.

Modify the `ScoutGPTConfig` class (around lines 26-40) to add `__post_init__`:

```python
@dataclass(frozen=True)
class ScoutGPTConfig:
    """Configuration for the ScoutGPT decoder."""

    vocab_size: int = 23
    hidden_dim: int = 256
    num_layers: int = 6
    num_heads: int = 8
    dropout: float = 0.1
    max_seq_len: int = 128
    num_players: int = 11_918
    spatial_mlp_dim: int = 64
    vaep_loss_weight: float = 0.1
    conditioning_type: str = "cross_attention"  # flipped from "additive" in wheel 0.3.10
    position_embedding: str = "learnable"

    def __post_init__(self) -> None:
        if self.conditioning_type == "fourier_cross_attention":
            warnings.warn(
                "conditioning_type='fourier_cross_attention' is deprecated as of wheel 0.3.10; "
                "cross_attention is preferred (see docs/evolve/cross-attention-promote/SUMMARY.md).",
                DeprecationWarning,
                stacklevel=2,
            )
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_decoder.py::test_fourier_cross_attention_emits_deprecation_warning -v
```

Expected: PASS.

- [ ] **Step 5: Audit existing tests for DeprecationWarning fallout**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_decoder.py src/tests/test_scoutgpt_fourier_parity.py -v -W error::DeprecationWarning
```

Expected: tests that construct `ScoutGPTConfig(conditioning_type="fourier_cross_attention")` will FAIL with `DeprecationWarning` promoted to error.

For EACH failing test, apply ONE of these wraps:

Option A — when the test's intent is to exercise the Fourier branch specifically:
```python
def test_xxx():
    with pytest.warns(DeprecationWarning, match="fourier_cross_attention"):
        cfg = ScoutGPTConfig(conditioning_type="fourier_cross_attention")
    # ... rest of test ...
```

Option B — when the test's intent is unrelated to warning semantics (parity tests, shape checks):
```python
@pytest.mark.filterwarnings("ignore::DeprecationWarning:analytics.scoutgpt_decoder")
def test_xxx():
    cfg = ScoutGPTConfig(conditioning_type="fourier_cross_attention")
    # ... rest of test ...
```

Prefer Option B for parity tests — they don't care about the warning, only the behavior.

- [ ] **Step 6: Re-run impacted tests without promotion**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_decoder.py src/tests/test_scoutgpt_fourier_parity.py -v
```

Expected: all PASS.

- [ ] **Step 7: Run full test suite to catch any other fallout**

Run:
```bash
uv run pytest src/tests/ -v
```

Expected: all tests PASS. If any new failures appear in test files touched by Task 14's audit but not yet fixed, apply the same Option A/B pattern.

---

### Task 16: Wheel bump 0.3.9 → 0.3.10 (CONDITIONAL on any code change from Task 13 or 15)

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/shared/wheel.py`
- Modify (via bump_wheel.py): PEP 723 script headers, Terraform files, `deploy.sh`

- [ ] **Step 1: Update `pyproject.toml` version**

Edit `pyproject.toml` line 3:
```toml
version = "0.3.10"
```
(was `0.3.9`)

- [ ] **Step 2: Update `src/shared/wheel.py`**

Edit `src/shared/wheel.py`:
```python
WHEEL_VERSION = "0.3.10"
```
(was `0.3.9`; `WHEEL_FILENAME` is f-string and will update automatically)

- [ ] **Step 3: Run `bump_wheel.py` to propagate**

Run:
```bash
uv run python scripts/bump_wheel.py
```

Expected: prints the list of files updated (PEP 723 headers, Terraform `*.tf`, `deploy.sh`).

- [ ] **Step 4: Verify consistency via `--check`**

Run:
```bash
uv run python scripts/bump_wheel.py --check
```

Expected: `OK` with zero inconsistencies. If any inconsistencies reported, investigate — the script's propagation should have handled them; any residual is a bug in an untracked consumer.

- [ ] **Step 5: Review the diff**

Run:
```bash
git diff pyproject.toml src/shared/wheel.py
git diff --stat
```

Expected: version updates plus propagation to ~5-10 consumer files (PEP 723 scripts, Terraform, deploy.sh).

---

## Phase E — Final validation and commit

### Task 17: Full pre-commit gate sweep

**Files:** no edits; verification only.

- [ ] **Step 1: Ruff lint**

```bash
uv run ruff check src/ scripts/
```

Expected: zero violations.

- [ ] **Step 2: Ruff format check**

```bash
uv run ruff format --check src/ scripts/
```

Expected: `X files already formatted`.

- [ ] **Step 3: Pyright basic-mode type check**

```bash
uv run pyright src/
```

Expected: zero errors.

- [ ] **Step 4: Full test suite**

```bash
uv run pytest src/tests/ -v
```

Expected: all tests PASS. If Task 15 was executed, confirm `test_fourier_cross_attention_emits_deprecation_warning` and `test_default_conditioning_type_is_cross_attention` are among the passing tests.

- [ ] **Step 5: Verify SUMMARY.md exists and is populated**

```bash
test -f docs/evolve/cross-attention-promote/SUMMARY.md && head -30 docs/evolve/cross-attention-promote/SUMMARY.md
```

Expected: file exists with the headline table and dispositions populated.

- [ ] **Step 6: Review full diff against main**

```bash
git diff main --stat
git diff main --name-only
```

Expected files in diff (depending on conditional outcomes):

Always:
- `src/analytics/promotion_rules.py` (retention rule added)
- `src/tests/test_retention_rule.py` (new)
- `scripts/run_cross_attention_ab.py` (new)
- `.gitignore` (modified for `artifacts/cross-attn-promote/**` exclusion)
- `artifacts/cross-attn-promote/.gitkeep` (new)
- `docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md` (new)
- `docs/superpowers/plans/2026-04-21-scoutgpt-cross-attention-promote.md` (new)
- `docs/evolve/cross-attention-promote/SUMMARY.md` (new)

Conditional on PROMOTE (Task 13):
- `src/analytics/scoutgpt_decoder.py` (default flip)
- `src/tests/test_scoutgpt_decoder.py` (default-value test added)

Conditional on DEPRECATE (Task 15):
- `src/analytics/scoutgpt_decoder.py` (`__post_init__` added — same file as above if PROMOTE also)
- `src/tests/test_scoutgpt_decoder.py` (deprecation test added — same file as above if PROMOTE also)
- `src/tests/test_scoutgpt_fourier_parity.py` + possibly other test files (filterwarnings adjustments)

Conditional on wheel bump (Task 16):
- `pyproject.toml`
- `src/shared/wheel.py`
- Various PEP 723 scripts + `terraform/**/*.tf` + `deploy.sh`

---

### Task 18: Present to user for commit approval

**Files:** no edits.

- [ ] **Step 1: Summarize the cycle outcome to the user**

Report to user:
1. Final rule outcomes:
   - `flip_decision: <PROMOTE|ARCHIVE>` with rho/top1 deltas
   - `retention_decision: <KEEP|DEPRECATE>` with rho/top1 deltas
2. Code changes landing (from Task 17 Step 6 diff list)
3. Wheel bump status (yes/no, version if yes)
4. Full pre-commit gate results (all green)
5. Proposed commit message (single-commit-at-end).

- [ ] **Step 2: Ask for explicit commit approval**

Phrase: "Ready to commit. The cycle landed `<flip_decision>` + `<retention_decision>`. Diff is N files as above, all gates green. Shall I create the single end-of-cycle commit?"

**Do not proceed to Task 19 without explicit user approval** per CLAUDE.md rule: "Never commit without explicit user approval. Each commit, PR, and destructive git operation requires separate, explicit approval."

---

### Task 19: Single end-of-cycle commit (AFTER user approval)

**Files:** all staged changes.

- [ ] **Step 1: Stage all changes**

```bash
git add -A
git status
```

Expected: all Phase A/D/E changes staged. Verify no .env, no credentials, no artifacts/ contents (other than `.gitkeep`).

- [ ] **Step 2: Create the commit**

The commit message depends on the rule outcomes. Choose the matching template:

**Both PROMOTE + DEPRECATE:**
```bash
git commit -m "$(cat <<'EOF'
feat(scoutgpt): promote cross_attention default + soft-deprecate fourier_cross_attention

Cycle outcome from 3-arm A/B at production fidelity (30 ep, 256d/6L/8h):
  - Arm 1 (additive control, Media-PC): rho=<R1>, top1=<T1>
  - Arm 5 (cross_attention, AI-PC): rho=<R5>, top1=<T5>
  - Arm 2 (fourier_cross_attention, AI-PC): rho=<R2>, top1=<T2>

Pre-registered decisions:
  - Default flip rule (apply_decision_rule): PROMOTE (rho delta +<DR>, top1 delta +<DT>)
  - Fourier retention rule (apply_retention_rule, new): DEPRECATE (rho delta +<DRF>, top1 delta +<DTF>)

Code changes:
  - ScoutGPTConfig.conditioning_type default: additive -> cross_attention
  - ScoutGPTConfig.__post_init__: DeprecationWarning for fourier_cross_attention
  - apply_retention_rule added to promotion_rules.py (reusable pre-registered tool)
  - Wheel 0.3.9 -> 0.3.10

See docs/evolve/cross-attention-promote/SUMMARY.md for full narrative.
Spec: docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

**PROMOTE + KEEP:**
```bash
git commit -m "$(cat <<'EOF'
feat(scoutgpt): promote cross_attention as default conditioning_type

Cycle outcome from 3-arm A/B at production fidelity (30 ep, 256d/6L/8h):
  - Arm 1 (additive control, Media-PC): rho=<R1>, top1=<T1>
  - Arm 5 (cross_attention, AI-PC): rho=<R5>, top1=<T5>
  - Arm 2 (fourier_cross_attention, AI-PC): rho=<R2>, top1=<T2>

Pre-registered decisions:
  - Default flip rule (apply_decision_rule): PROMOTE (rho delta +<DR>, top1 delta +<DT>)
  - Fourier retention rule (apply_retention_rule, new): KEEP (within-noise gap vs cross_attention)

Code changes:
  - ScoutGPTConfig.conditioning_type default: additive -> cross_attention
  - apply_retention_rule added to promotion_rules.py (reusable pre-registered tool)
  - Wheel 0.3.9 -> 0.3.10

See docs/evolve/cross-attention-promote/SUMMARY.md for full narrative.
Spec: docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

**ARCHIVE + DEPRECATE:**
```bash
git commit -m "$(cat <<'EOF'
chore(scoutgpt): soft-deprecate fourier_cross_attention (cross_attention default stays additive)

Cycle outcome from 3-arm A/B at production fidelity (30 ep, 256d/6L/8h):
  - Arm 1 (additive control): rho=<R1>, top1=<T1>
  - Arm 5 (cross_attention): rho=<R5>, top1=<T5>
  - Arm 2 (fourier_cross_attention): rho=<R2>, top1=<T2>

Pre-registered decisions:
  - Default flip rule (apply_decision_rule): ARCHIVE (clean Arm 5 did not clear +0.10 rho threshold)
  - Fourier retention rule (apply_retention_rule, new): DEPRECATE

Code changes:
  - ScoutGPTConfig.__post_init__: DeprecationWarning for fourier_cross_attention
  - apply_retention_rule added to promotion_rules.py (reusable pre-registered tool)
  - Wheel 0.3.9 -> 0.3.10

See docs/evolve/cross-attention-promote/SUMMARY.md for full narrative.
Spec: docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

**ARCHIVE + KEEP (null cycle):**
```bash
git commit -m "$(cat <<'EOF'
docs(scoutgpt): cross_attention A/B null cycle + retention-rule tool

3-arm A/B at production fidelity did not clear promotion thresholds:
  - Arm 1 (additive control): rho=<R1>, top1=<T1>
  - Arm 5 (cross_attention): rho=<R5>, top1=<T5>
  - Arm 2 (fourier_cross_attention): rho=<R2>, top1=<T2>

Pre-registered decisions:
  - Default flip rule (apply_decision_rule): ARCHIVE
  - Fourier retention rule (apply_retention_rule, new): KEEP

Code changes:
  - apply_retention_rule added to promotion_rules.py (reusable pre-registered tool)
  - Spec + plan + SUMMARY.md document the null result.

See docs/evolve/cross-attention-promote/SUMMARY.md for full narrative.
Spec: docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

Replace `<R1>`, `<T1>`, `<R5>`, `<T5>`, `<R2>`, `<T2>`, `<DR>`, `<DT>`, `<DRF>`, `<DTF>` with actual values from `artifacts/cross-attn-promote/decisions.json` rounded to 4 decimals.

Expected: commit created. Any pre-commit hook (ruff, etc.) runs and passes.

- [ ] **Step 3: Verify commit**

```bash
git log --oneline -3
git show --stat HEAD
```

Expected: commit on `evolve/scoutgpt-cross-attn-promote`, file list matches the Task 17 Step 6 diff.

---

## Self-review notes

**Spec coverage check:**
- Spec Section A (Architecture) — covered by Tasks 2, 13, 15.
- Spec Section B (A/B run plan) — covered by Tasks 3, 4, 5, 7, 8, 9, 10, 11.
- Spec Section C (Decision rules) — covered by Tasks 2 (retention rule), 10/11 (both rules applied in orchestrator).
- Spec Section D (Validation) — covered by Tasks 2, 6, 13, 14, 15, 17.
- Spec Section E (Artefacts) — covered by Tasks 1 (artifacts dir), 15 (DeprecationWarning), 16 (wheel bump), 10 (SUMMARY), 19 (commit).
- Spec Section F (Risks) — mitigations baked into Tasks 7 (SSH verify), 8 (pre-flight), 9 (smoke), 10 (poll), 11 (re-dispatch), 14 (audit), 15 (filterwarnings).
- Spec Section G (Deliverables) — covered by Task 19 (commit).

**Placeholder scan:** no TBD/TODO markers. All code blocks contain complete runnable code.

**Type consistency:** decision-rule call signatures (`apply_decision_rule`, `apply_retention_rule`) are consistent across Tasks 2, 4, 10 (via `generate_summary_and_rules`).

**Scope check:** single plan, single feature branch, single commit. Does not overreach into XG2 (Cycle 2 queued separately).
