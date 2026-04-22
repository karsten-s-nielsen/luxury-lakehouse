"""Local orchestrator for the ScoutGPT cross_attention promotion + Fourier retention A/B.

3-arm A/B at production fidelity, local-only execution:
  - Arm 1 (additive, control)        → Media-PC (super@192.168.68.70, ~/venv-fourier)
  - Arm 5 (cross_attention, flip)    → AI-PC (this workstation, project uv)
  - Arm 2 (fourier_cross_attention)  → AI-PC (serial after Arm 5)

Each arm runs ``scripts/train_scoutgpt_hf.py --local-mode`` as a subprocess (AI-PC) or via SSH
(Media-PC). Training code matches the production pipeline exactly — no duplicated logic.

Follows the PR #166 orchestrator pattern (scripts/run_fourier_scoutgpt_ab.py) adapted for:
  - 3 arms instead of 5
  - Media-PC (Ubuntu via SSH, fresh setup) instead of DGX Spark
  - Media-PC smoke test as a BLOCKING gate before full A/B (Media-PC is new; AI-PC is tested)
  - Two pre-registered decision rules applied to the A/B results:
      * apply_decision_rule(Arm 1, Arm 5)  -> default flip (PROMOTE/ARCHIVE)
      * apply_retention_rule(Arm 2, Arm 5) -> Fourier fate (KEEP/DEPRECATE)

See:
  - docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md
  - docs/superpowers/plans/2026-04-21-scoutgpt-cross-attention-promote.md
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

logging.basicConfig(
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
TRAINING_DATASET = f"{HF_ORG}/scoutgpt-training-data"

LOCAL_MACHINE = "local"  # AI-PC (this workstation)
MEDIA_MACHINE = "media"  # Media-PC (super@192.168.68.70)
MEDIA_SSH = "super@192.168.68.70"
MEDIA_WORKSPACE = "~/luxury-lakehouse-cross-attn"
MEDIA_ENV_ACTIVATE = "source ~/venv-fourier/bin/activate"
MEDIA_REMOTE_ARTIFACTS = f"{MEDIA_WORKSPACE}/artifacts/cross-attn-promote"

REPO_ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS_DIR = REPO_ROOT / "artifacts" / "cross-attn-promote"
SUMMARY_PATH = REPO_ROOT / "docs" / "evolve" / "cross-attention-promote" / "SUMMARY.md"
DECISIONS_PATH = ARTIFACTS_DIR / "decisions.json"

POLL_INTERVAL_SECONDS = 30
SMOKE_SSH_TIMEOUT = 900  # 15 min hard cap for smoke subprocess


@dataclass(frozen=True)
class ArmSpec:
    """Specification for a single A/B arm."""

    name: str
    conditioning_type: str
    hidden_dim: int
    num_layers: int
    num_heads: int
    machine: Literal["local", "media"]
    role: str  # CONTROL | DEFAULT-FLIP-CANDIDATE | RETENTION-CANDIDATE


# Arm numbering matches PR #166 for cross-reference. See design doc B.1.
ARMS: list[ArmSpec] = [
    ArmSpec(
        name="arm1-additive",
        conditioning_type="additive",
        hidden_dim=256,
        num_layers=6,
        num_heads=8,
        machine=MEDIA_MACHINE,
        role="CONTROL",
    ),
    ArmSpec(
        name="arm5-cross-attention",
        conditioning_type="cross_attention",
        hidden_dim=256,
        num_layers=6,
        num_heads=8,
        machine=LOCAL_MACHINE,
        role="DEFAULT-FLIP-CANDIDATE",
    ),
    ArmSpec(
        name="arm2-fourier",
        conditioning_type="fourier_cross_attention",
        hidden_dim=256,
        num_layers=6,
        num_heads=8,
        machine=LOCAL_MACHINE,
        role="RETENTION-CANDIDATE",
    ),
]

SHARED_TRAINING_CONFIG: dict[str, object] = {
    "position_embedding": "learnable",
    "dropout": 0.10,
    "learning_rate": 1e-4,
    "batch_size": 256,
    "vaep_loss_weight": 0.10,
    "epochs": 30,
    "patience": 5,
    "seed": 42,
}


def resolve_dataset_revision() -> str:
    """Resolve the current HF dataset SHA once at dispatch start."""
    from huggingface_hub import HfApi

    api = HfApi()
    info = api.repo_info(repo_id=TRAINING_DATASET, repo_type="dataset")
    sha = info.sha or ""
    if not sha:
        msg = f"Could not resolve HF dataset SHA for {TRAINING_DATASET}"
        raise RuntimeError(msg)
    return sha


def build_dispatch_manifest(dataset_revision: str) -> dict[str, object]:
    """Serialize the dispatch plan so arms see the same pinning."""
    return {
        "dispatch_start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset_revision": dataset_revision,
        "dataset_repo": TRAINING_DATASET,
        "shared_config": SHARED_TRAINING_CONFIG,
        "arms": [asdict(a) for a in ARMS],
    }


def run_media_pc_preflight() -> bool:
    """Verify Media-PC SSH + torch + CUDA + GPU + HF_TOKEN. Returns True on pass."""
    logger.info("Media-PC pre-flight: SSH reachability")
    r = subprocess.run(  # noqa: S603
        ["ssh", MEDIA_SSH, "echo ok"],  # noqa: S607
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if r.returncode != 0 or r.stdout.strip() != "ok":
        logger.error("SSH check failed: rc=%d stdout=%r stderr=%r", r.returncode, r.stdout, r.stderr)
        return False

    logger.info("Media-PC pre-flight: torch + CUDA + GPU")
    probe = (
        f"{MEDIA_ENV_ACTIVATE} && "
        "python -c 'import torch; "
        "print(torch.__version__, torch.cuda.is_available(), "
        "torch.cuda.get_device_name(0) if torch.cuda.is_available() else None)'"
    )
    r = subprocess.run(  # noqa: S603
        ["ssh", MEDIA_SSH, probe],  # noqa: S607
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if r.returncode != 0:
        logger.error("torch probe failed: rc=%d stdout=%r stderr=%r", r.returncode, r.stdout, r.stderr)
        return False
    logger.info("Media-PC torch probe: %s", r.stdout.strip())
    if "True" not in r.stdout:
        logger.error("CUDA not available on Media-PC")
        return False
    if "5070 Ti" not in r.stdout:
        logger.error("Media-PC GPU is not RTX 5070 Ti: %s", r.stdout.strip())
        return False

    logger.info("Media-PC pre-flight: HF_TOKEN presence")
    r = subprocess.run(  # noqa: S603
        ["ssh", MEDIA_SSH, 'test -n "$HF_TOKEN" && echo set || echo missing'],  # noqa: S607
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if r.stdout.strip() != "set":
        logger.warning("HF_TOKEN not in Media-PC env — orchestrator will forward explicitly at dispatch")

    logger.info("Media-PC pre-flight: PASS")
    return True


def _transfer_branch_to_media() -> None:
    """Tar-pipe the current working tree (incl uncommitted changes) to Media-PC.

    Tight scope: only the directories/files training actually needs — src/, scripts/,
    pyproject.toml. Training on Media-PC imports via PYTHONPATH=./src and invokes
    scripts/train_scoutgpt_hf.py; nothing else is referenced at runtime. This is
    ~4 MB vs the 2.9 GB a full-tree transfer produces (.terraform provider caches
    dominate the full tree and caused a WSL disk-fill incident 2026-04-21).

    Uses ``tar cz | ssh tar xz`` rather than rsync because rsync is not available in
    Windows Git Bash by default.
    """
    logger.info("Transferring branch to Media-PC (tight scope: src/ + scripts/ + pyproject.toml)...")
    subprocess.run(  # noqa: S603
        ["ssh", MEDIA_SSH, f"mkdir -p {MEDIA_WORKSPACE}"],  # noqa: S607
        check=True,
        capture_output=True,
        timeout=30,
    )
    tar_cmd = [
        "tar",
        "czf",
        "-",
        "--exclude=__pycache__",
        "--exclude=*.pyc",
        "--exclude=.ruff_cache",
        "--exclude=.pytest_cache",
        "--exclude=.benchmarks",
        "src",
        "scripts",
        "pyproject.toml",
    ]
    ssh_extract_cmd = ["ssh", MEDIA_SSH, f"cd {MEDIA_WORKSPACE} && tar xzf -"]

    tar_proc = subprocess.Popen(tar_cmd, stdout=subprocess.PIPE, cwd=str(REPO_ROOT))  # noqa: S603
    ssh_proc = subprocess.Popen(ssh_extract_cmd, stdin=tar_proc.stdout)  # noqa: S603
    if tar_proc.stdout is not None:
        tar_proc.stdout.close()
    ssh_rc = ssh_proc.wait()
    tar_rc = tar_proc.wait()
    if tar_rc != 0 or ssh_rc != 0:
        msg = f"Branch transfer to Media-PC failed: tar_rc={tar_rc} ssh_rc={ssh_rc}"
        raise RuntimeError(msg)
    logger.info("Transfer complete → %s", MEDIA_WORKSPACE)


def run_media_pc_smoke(dataset_revision: str) -> bool:
    """Run 2-epoch x 1000-episode smoke on Media-PC with Arm 1 config.

    Pass criteria:
      - Subprocess exits 0
      - metrics.json written with the required rho/top1/val_loss keys
    """
    logger.info("Media-PC smoke: 2 epochs x 1000 episodes, Arm 1 (additive) config")
    smoke_outdir = f"{MEDIA_REMOTE_ARTIFACTS}/smoke"
    env_prefix = _media_remote_env_prefix(dataset_revision)
    remote_cmd = (
        f"cd {MEDIA_WORKSPACE} && "
        f"{MEDIA_ENV_ACTIVATE} && "
        f"mkdir -p {smoke_outdir} && "
        f"{env_prefix} && "
        f"python scripts/train_scoutgpt_hf.py "
        f"--local-mode "
        f"--local-output-dir {smoke_outdir} "
        f"--variant learnable "
        f"--conditioning-type additive "
        f"--hidden-dim 256 "
        f"--num-layers 6 "
        f"--num-heads 8 "
        f"--epochs 2 "
        f"--batch-size 256 "
        f"--lr 1e-4 "
        f"--patience 5 "
        f"--max-episodes 1000"
    )
    r = subprocess.run(  # noqa: S603
        ["ssh", MEDIA_SSH, remote_cmd],  # noqa: S607
        capture_output=True,
        text=True,
        timeout=SMOKE_SSH_TIMEOUT,
        check=False,
    )
    if r.returncode != 0:
        logger.error("Smoke exit rc=%d", r.returncode)
        stderr_tail = "\n".join(r.stderr.splitlines()[-100:])
        logger.error("stderr tail:\n%s", stderr_tail)
        return False

    fetch = subprocess.run(  # noqa: S603
        ["ssh", MEDIA_SSH, f"cat {smoke_outdir}/metrics.json"],  # noqa: S607
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if fetch.returncode != 0:
        logger.error("Failed to read smoke metrics.json: %s", fetch.stderr)
        return False
    try:
        metrics = json.loads(fetch.stdout)
    except json.JSONDecodeError as exc:
        logger.error("Smoke metrics.json not valid JSON: %s", exc)
        return False

    # Accept either metric naming convention. Current train_scoutgpt_hf.py (2026-04-21
    # post PR #166) emits `mean_spearman_rho` and `test_top1_accuracy`; PR #166's own
    # orchestrator looked for the older `counterfactual_spearman_rho` / `next_action_top1_accuracy`.
    rho_key = _first_present(metrics, ["mean_spearman_rho", "counterfactual_spearman_rho", "counterfactual_rho"])
    top1_key = _first_present(metrics, ["test_top1_accuracy", "next_action_top1_accuracy", "test_top1"])
    if rho_key is None or top1_key is None:
        logger.error("Smoke metrics.json missing expected rho/top1 keys. keys=%s", list(metrics.keys()))
        return False
    logger.info(
        "Media-PC smoke: PASS (rho=%.3f, top1=%.3f, wall_min=%s)",
        metrics[rho_key],
        metrics[top1_key],
        metrics.get("wall_clock_minutes", metrics.get("wall_min", "?")),
    )
    return True


def _first_present(d: dict[str, object], keys: list[str]) -> str | None:
    for k in keys:
        if k in d:
            return k
    return None


def _build_train_cli_args(arm: ArmSpec, local_output_dir: str) -> list[str]:
    """Build the CLI argument list for scripts/train_scoutgpt_hf.py --local-mode."""
    return [
        "--local-mode",
        "--local-output-dir",
        local_output_dir,
        "--variant",
        str(SHARED_TRAINING_CONFIG["position_embedding"]),
        "--conditioning-type",
        arm.conditioning_type,
        "--hidden-dim",
        str(arm.hidden_dim),
        "--num-layers",
        str(arm.num_layers),
        "--num-heads",
        str(arm.num_heads),
        "--epochs",
        str(SHARED_TRAINING_CONFIG["epochs"]),
        "--batch-size",
        str(SHARED_TRAINING_CONFIG["batch_size"]),
        "--lr",
        str(SHARED_TRAINING_CONFIG["learning_rate"]),
        "--patience",
        str(SHARED_TRAINING_CONFIG["patience"]),
    ]


def _env_with_dataset_pinned(dataset_revision: str) -> dict[str, str]:
    """Return a copy of os.environ with DATASET_PINNED_SHA + UTF-8 encoding set."""
    import os

    env = dict(os.environ)
    env["DATASET_PINNED_SHA"] = dataset_revision
    env.setdefault("PYTHONIOENCODING", "utf-8")
    return env


def _get_hf_token() -> str:
    """Read HF_TOKEN from the AI-PC env for forwarding to Media-PC.

    train_scoutgpt_hf.py raises `RuntimeError("HF_TOKEN required for dataset streaming
    even in local mode")` without it. HF_TOKEN must reach Media-PC's subprocess env
    via either (a) explicit forwarding in the SSH command (this function), or (b)
    persistence on Media-PC (not done; Media-PC is a new env). This function is
    called at dispatch time so the token isn't embedded in logs.
    """
    import os

    token = os.environ.get("HF_TOKEN", "")
    if not token:
        # Fallback: try huggingface-hub's saved token
        try:
            from huggingface_hub import get_token

            token = get_token() or ""
        except ImportError:
            pass
    if not token:
        msg = "HF_TOKEN not found in AI-PC env and no saved HuggingFace token — cannot forward to Media-PC"
        raise RuntimeError(msg)
    return token


def _media_remote_env_prefix(dataset_revision: str) -> str:
    """Env-export prefix for remote SSH commands. Includes HF_TOKEN forwarded from AI-PC.

    Returns a string of bash `export` statements that must precede the training
    invocation inside the single-quoted SSH command. HF_TOKEN is quoted with
    shlex.quote for shell safety.
    """
    import shlex

    hf_token = _get_hf_token()
    return (
        f"export DATASET_PINNED_SHA={shlex.quote(dataset_revision)} && "
        f"export PYTHONPATH=./src && "
        f"export PYTHONIOENCODING=utf-8 && "
        f"export HF_TOKEN={shlex.quote(hf_token)}"
    )


def _dispatch_local(arm: ArmSpec, dataset_revision: str) -> subprocess.Popen[bytes]:
    """Dispatch an arm to AI-PC (this workstation) as a subprocess."""
    arm_out_dir = ARTIFACTS_DIR / arm.name
    arm_out_dir.mkdir(parents=True, exist_ok=True)
    log_path = arm_out_dir / "run-arm.log"
    logger.info("Dispatching %s on AI-PC → %s", arm.name, log_path)

    cli_args = _build_train_cli_args(arm, str(arm_out_dir))
    cmd = [sys.executable, str(REPO_ROOT / "scripts" / "train_scoutgpt_hf.py"), *cli_args]

    env = _env_with_dataset_pinned(dataset_revision)
    log_fh = log_path.open("wb")
    return subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT, env=env)  # noqa: S603


def _dispatch_media(arm: ArmSpec, dataset_revision: str) -> subprocess.Popen[bytes]:
    """Dispatch an arm to Media-PC via SSH."""
    arm_out_dir_local = ARTIFACTS_DIR / arm.name
    arm_out_dir_local.mkdir(parents=True, exist_ok=True)
    log_path = arm_out_dir_local / "run-arm.log"
    logger.info("Dispatching %s on Media-PC → %s", arm.name, log_path)

    media_out_dir = f"{MEDIA_REMOTE_ARTIFACTS}/{arm.name}"
    cli_args = _build_train_cli_args(arm, media_out_dir)
    cli_str = " ".join(cli_args)
    env_prefix = _media_remote_env_prefix(dataset_revision)

    remote_script = (
        f"cd {MEDIA_WORKSPACE} && "
        f"{MEDIA_ENV_ACTIVATE} && "
        f"mkdir -p {media_out_dir} && "
        f"{env_prefix} && "
        f"python scripts/train_scoutgpt_hf.py {cli_str}"
    )

    log_fh = log_path.open("wb")
    return subprocess.Popen(  # noqa: S603
        ["ssh", MEDIA_SSH, remote_script],  # noqa: S607
        stdout=log_fh,
        stderr=subprocess.STDOUT,
    )


def _collect_metrics(arm: ArmSpec) -> dict[str, object]:
    """Collect metrics.json from either local disk or Media-PC (via scp)."""
    local_path = ARTIFACTS_DIR / arm.name / "metrics.json"

    if arm.machine == MEDIA_MACHINE:
        media_metrics_path = f"{MEDIA_REMOTE_ARTIFACTS}/{arm.name}/metrics.json"
        logger.info("Fetching metrics from Media-PC: %s", media_metrics_path)
        subprocess.run(  # noqa: S603
            ["scp", f"{MEDIA_SSH}:{media_metrics_path}", str(local_path)],  # noqa: S607
            check=True,
            capture_output=True,
            timeout=60,
        )

    if not local_path.exists():
        msg = f"metrics.json missing for arm {arm.name} at {local_path}"
        raise FileNotFoundError(msg)

    return json.loads(local_path.read_text(encoding="utf-8"))


def _extract_metric(m: dict[str, object], keys: list[str]) -> float | None:
    for k in keys:
        v = m.get(k)
        if isinstance(v, (int, float)):
            return float(v)
    return None


def _apply_rules_and_write_summary(
    all_metrics: dict[str, dict[str, object]],
    dataset_revision: str,
) -> tuple[str, str]:
    """Apply both pre-registered rules and write SUMMARY.md + decisions.json.

    Returns (flip_decision, retention_decision). Raises RuntimeError if metrics missing.
    """
    from analytics.promotion_rules import apply_decision_rule, apply_retention_rule

    arm1 = all_metrics["arm1-additive"]
    arm5 = all_metrics["arm5-cross-attention"]
    arm2 = all_metrics["arm2-fourier"]

    # Current train_scoutgpt_hf.py emits `mean_spearman_rho` + `test_top1_accuracy`;
    # PR #166 emitted `counterfactual_spearman_rho` + `next_action_top1_accuracy`.
    # Accept either so the orchestrator is robust to future renames.
    rho_keys = ["mean_spearman_rho", "counterfactual_spearman_rho", "counterfactual_rho"]
    top1_keys = ["test_top1_accuracy", "next_action_top1_accuracy", "test_top1"]

    rho1 = _extract_metric(arm1, rho_keys)
    rho5 = _extract_metric(arm5, rho_keys)
    rho2 = _extract_metric(arm2, rho_keys)
    top1_1 = _extract_metric(arm1, top1_keys)
    top1_5 = _extract_metric(arm5, top1_keys)
    top1_2 = _extract_metric(arm2, top1_keys)

    if None in (rho1, rho5, rho2, top1_1, top1_5, top1_2):
        raise RuntimeError(
            f"Missing rho/top1 in metrics. rho1={rho1} rho5={rho5} rho2={rho2} "
            f"top1_1={top1_1} top1_5={top1_5} top1_2={top1_2}"
        )

    flip_decision = apply_decision_rule(
        rho_ctrl=float(rho1),  # type: ignore[arg-type]
        rho_trt=float(rho5),  # type: ignore[arg-type]
        top1_ctrl=float(top1_1),  # type: ignore[arg-type]
        top1_trt=float(top1_5),  # type: ignore[arg-type]
    )
    retention_decision = apply_retention_rule(
        rho_incumbent=float(rho2),  # type: ignore[arg-type]
        rho_challenger=float(rho5),  # type: ignore[arg-type]
        top1_incumbent=float(top1_2),  # type: ignore[arg-type]
        top1_challenger=float(top1_5),  # type: ignore[arg-type]
    )

    logger.info("=" * 60)
    logger.info("DECISIONS")
    logger.info("=" * 60)
    logger.info("Default flip (Arm 5 vs Arm 1): %s", flip_decision)
    logger.info("  rho delta: %+.4f (threshold >= +0.10)", float(rho5) - float(rho1))  # type: ignore[arg-type]
    logger.info("  top1 delta: %+.4f (threshold >= -0.005)", float(top1_5) - float(top1_1))  # type: ignore[arg-type]
    logger.info("Fourier retention (Arm 5 challenger vs Arm 2 incumbent): %s", retention_decision)
    logger.info(
        "  rho delta (chl - inc): %+.4f (threshold >= +0.05 for DEPRECATE)",
        float(rho5) - float(rho2),  # type: ignore[arg-type]
    )
    logger.info(
        "  top1 delta (chl - inc): %+.4f (threshold >= -0.005)",
        float(top1_5) - float(top1_2),  # type: ignore[arg-type]
    )
    logger.info("=" * 60)

    # Write decisions.json for downstream conditional code-change steps
    DECISIONS_PATH.write_text(
        json.dumps(
            {
                "flip_decision": flip_decision,
                "retention_decision": retention_decision,
                "arm1-additive": arm1,
                "arm5-cross-attention": arm5,
                "arm2-fourier": arm2,
                "dataset_revision": dataset_revision,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    logger.info("Decisions written to %s", DECISIONS_PATH)

    # Write SUMMARY.md
    _write_summary_md(
        all_metrics=all_metrics,
        dataset_revision=dataset_revision,
        flip_decision=flip_decision,
        retention_decision=retention_decision,
        rho1=float(rho1),  # type: ignore[arg-type]
        rho5=float(rho5),  # type: ignore[arg-type]
        rho2=float(rho2),  # type: ignore[arg-type]
        top1_1=float(top1_1),  # type: ignore[arg-type]
        top1_5=float(top1_5),  # type: ignore[arg-type]
        top1_2=float(top1_2),  # type: ignore[arg-type]
    )

    return flip_decision, retention_decision


def _write_summary_md(
    all_metrics: dict[str, dict[str, object]],
    dataset_revision: str,
    flip_decision: str,
    retention_decision: str,
    rho1: float,
    rho5: float,
    rho2: float,
    top1_1: float,
    top1_5: float,
    top1_2: float,
) -> None:
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)

    def _fmt(v: object) -> str:
        if isinstance(v, (int, float)):
            return f"{v:.4f}"
        return "—"

    lines: list[str] = [
        "# ScoutGPT cross_attention Promotion + Fourier Retention — Summary",
        "",
        f"**Date:** {time.strftime('%Y-%m-%d', time.gmtime())}",
        "**Branch:** `evolve/scoutgpt-cross-attn-promote`",
        "**Execution venue:** Local — 1x RTX 5070 Ti (AI-PC) + 1x RTX 5070 Ti (Media-PC, SSH)",
        f"**Dataset:** `{TRAINING_DATASET}` revision `{dataset_revision}`",
        "**Spec:** `docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md`",
        "",
        "## Pre-registered decision rules",
        "",
        "- **Default flip rule** (Arm 1 additive → Arm 5 cross_attention): "
        "PROMOTE iff `rho_trt - rho_ctrl >= +0.10` AND `top1_trt >= top1_ctrl - 0.005`. "
        "Applied via `src/analytics/promotion_rules.py::apply_decision_rule`.",
        "- **Fourier retention rule** (Arm 2 incumbent vs Arm 5 challenger): "
        "DEPRECATE iff `rho_challenger - rho_incumbent >= +0.05` AND "
        "`top1_challenger - top1_incumbent >= -0.005`. "
        "Applied via `src/analytics/promotion_rules.py::apply_retention_rule`.",
        "",
        "## Headline",
        "",
        (
            "| Arm | Role | conditioning_type | hd/L/H | "
            "`counterfactual_rho` | `test_top1` | `val_loss` | `wall_clock_min` |"
        ),
        "|---|---|---|---|---:|---:|---:|---:|",
    ]
    for arm in ARMS:
        m = all_metrics.get(arm.name, {}) or {}
        rho = _fmt(m.get("mean_spearman_rho") or m.get("counterfactual_spearman_rho") or m.get("counterfactual_rho"))
        top1 = _fmt(m.get("test_top1_accuracy") or m.get("next_action_top1_accuracy") or m.get("test_top1"))
        vloss = _fmt(m.get("test_loss") or m.get("val_loss"))
        wc = _fmt(m.get("wall_clock_minutes") or m.get("wall_min"))
        lines.append(
            f"| {arm.name} | {arm.role} | {arm.conditioning_type} | "
            f"{arm.hidden_dim}/{arm.num_layers}/{arm.num_heads} | "
            f"{rho} | {top1} | {vloss} | {wc} |"
        )

    lines.extend(
        [
            "",
            "## Dispositions",
            "",
            f"- **Default flip** (Arm 5 vs Arm 1): **{flip_decision}**",
            f"  - rho delta = {rho5 - rho1:+.4f}",
            f"  - top1 delta = {top1_5 - top1_1:+.4f}",
            f"- **Fourier retention** (Arm 2 vs Arm 5): **{retention_decision}**",
            f"  - rho delta (challenger - incumbent) = {rho5 - rho2:+.4f}",
            f"  - top1 delta (challenger - incumbent) = {top1_5 - top1_2:+.4f}",
            "",
            "## Cross-reference",
            "",
            f"- **PR #166 (Fourier promotion, 2026-04-21)** — Arm 5 under GPU contention reported "
            f"rho=0.2995. Clean re-run this cycle: {rho5:.4f}. Arm 2 (clean, no contention) "
            f"reported 0.2812; clean re-run this cycle: {rho2:.4f}.",
            "- **RoPE-for-ScoutGPT A/B (PR #159, 2026-04-19)** — rho delta +0.016 rejected as noise floor.",
            "",
            "## Mechanism narrative",
            "",
            "_(To be filled in during review — include whether GPU contention was the Arm 5 rho "
            "driver, and whether Fourier's within-noise gap under PR #166 persisted under clean "
            "conditions.)_",
            "",
            "## Follow-ups",
            "",
            "- Canonical checkpoint retraining on HF Hub under the new default (if flip promoted).",
            "- Football2Vec cross-attention port.",
            "- Spatial-encoding x conditioning-type axis decomposition (future refactor).",
            "- Hard removal of deprecated `fourier_cross_attention` (after 3+ months of no push-back).",
            "",
            "See `docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md` "
            "Section I for the full follow-up list.",
            "",
        ]
    )

    SUMMARY_PATH.write_text("\n".join(lines), encoding="utf-8")
    logger.info("SUMMARY written to %s", SUMMARY_PATH)


def cmd_drive(skip_smoke: bool, skip_preflight: bool) -> int:
    """Execute the 3-arm A/B with Media-PC smoke-gate first, then pair-parallel dispatch."""
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    dataset_revision = resolve_dataset_revision()
    manifest = build_dispatch_manifest(dataset_revision)
    (ARTIFACTS_DIR / "dispatch-manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    logger.info("Dispatch manifest pinned dataset @ %s", dataset_revision)

    # Transfer branch to Media-PC (tar-pipe; includes uncommitted changes)
    if any(a.machine == MEDIA_MACHINE for a in ARMS):
        _transfer_branch_to_media()

    # Media-PC pre-flight + smoke — BLOCKING gate before full A/B
    if not skip_preflight:
        if not run_media_pc_preflight():
            logger.error("Media-PC pre-flight failed — aborting")
            logger.error("Fallback: fix Media-PC env and re-run, or use --skip-preflight after manual verification")
            return 1

    if not skip_smoke:
        if not run_media_pc_smoke(dataset_revision):
            logger.error("Media-PC smoke test failed — aborting before full A/B")
            return 1

    # Pair-parallel dispatch: one arm per machine at a time, iterate until all done.
    # With 3 arms (Arm 1 on Media, Arms 5 + 2 on AI-PC), Arm 1 runs in parallel with Arm 5,
    # then Arm 2 runs serially on AI-PC after Arm 5 finishes.
    pending = list(ARMS)
    running: dict[str, tuple[ArmSpec, subprocess.Popen[bytes]]] = {}
    failed: list[str] = []

    while pending or running:
        busy_machines = {arm.machine for arm, _ in running.values()}
        still_pending: list[ArmSpec] = []
        for arm in pending:
            if arm.machine in busy_machines:
                still_pending.append(arm)
                continue
            proc = (
                _dispatch_local(arm, dataset_revision)
                if arm.machine == LOCAL_MACHINE
                else _dispatch_media(arm, dataset_revision)
            )
            running[arm.name] = (arm, proc)
            busy_machines.add(arm.machine)
        pending = still_pending

        time.sleep(POLL_INTERVAL_SECONDS)

        for arm_name in list(running.keys()):
            _arm_spec, proc = running[arm_name]
            if proc.poll() is not None:
                rc = proc.returncode
                if rc == 0:
                    logger.info("Arm %s completed (rc=0)", arm_name)
                else:
                    logger.error(
                        "Arm %s FAILED (rc=%d); log at %s",
                        arm_name,
                        rc,
                        ARTIFACTS_DIR / arm_name / "run-arm.log",
                    )
                    failed.append(arm_name)
                del running[arm_name]

    if failed:
        logger.error("Cycle halted: %d arm(s) failed: %s", len(failed), failed)
        return 1

    # Collect metrics from all arms
    all_metrics = {arm.name: _collect_metrics(arm) for arm in ARMS}
    (ARTIFACTS_DIR / "results.json").write_text(json.dumps(all_metrics, indent=2, default=str), encoding="utf-8")
    logger.info("All %d arms complete; results at %s", len(ARMS), ARTIFACTS_DIR / "results.json")

    # Apply decision rules + write SUMMARY.md + decisions.json
    try:
        _apply_rules_and_write_summary(all_metrics, dataset_revision)
    except RuntimeError as exc:
        logger.error("Rule application failed: %s", exc)
        return 1

    return 0


def cmd_drive_dry_run() -> int:
    """Print the dispatch plan without executing."""
    try:
        dataset_revision = resolve_dataset_revision()
    except (RuntimeError, ConnectionError) as exc:
        logger.warning("Dataset SHA resolution failed (%s); using placeholder", exc)
        dataset_revision = "<unresolved-network-failure>"

    manifest = build_dispatch_manifest(dataset_revision)
    print(json.dumps(manifest, indent=2, default=str))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="ScoutGPT cross_attention + Fourier A/B orchestrator.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the dispatch plan without executing. Useful for reviewing arm roster and dataset SHA.",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip Media-PC SSH/torch/CUDA/GPU probe. Use only after manual verification.",
    )
    parser.add_argument(
        "--skip-smoke",
        action="store_true",
        help="Skip Media-PC 2-epoch smoke training. Use only after smoke already passed once.",
    )
    args = parser.parse_args()

    if args.dry_run:
        return cmd_drive_dry_run()
    return cmd_drive(skip_smoke=args.skip_smoke, skip_preflight=args.skip_preflight)


if __name__ == "__main__":
    sys.exit(main())
