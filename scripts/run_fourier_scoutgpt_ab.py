"""Local A/B orchestrator for the fourier_cross_attention + swiglu promotion cycle.

Runs 5 arms locally across 1x RTX 5070 Ti + 1x DGX Spark (SSH). No HF Jobs,
no HF Hub for artefacts. HF Hub is used only for read-only dataset streaming
with revision pinning (matches the L2 harvest baseline).

Each arm runs ``scripts/train_scoutgpt_hf.py --local-mode`` as a subprocess
(local machine) or via SSH (Spark). Training code is the same for all arms
and matches the production pipeline exactly — no duplicated logic.

See docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md
and docs/superpowers/plans/2026-04-20-scoutgpt-fourier-cross-attention-promote.md.
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

LOCAL_MACHINE = "local"
SPARK_MACHINE = "spark"
SPARK_SSH = "karsten@192.168.68.73"
SPARK_WORKSPACE = "~/Development/luxury-lakehouse-fourier-promote"
SPARK_ENV_ACTIVATE = "source ~/Development/evolve-env/bin/activate"

REPO_ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS_DIR = REPO_ROOT / "artifacts" / "fourier-scoutgpt"
SUMMARY_PATH = REPO_ROOT / "docs" / "evolve" / "fourier-scoutgpt" / "SUMMARY.md"

POLL_INTERVAL_SECONDS = 30


@dataclass(frozen=True)
class ArmSpec:
    """Specification for a single A/B arm."""

    name: str
    conditioning_type: str
    hidden_dim: int
    num_layers: int
    num_heads: int
    machine: Literal["local", "spark"]
    role: str  # "CONTROL" | "TREATMENT" | "ABLATION" | "ISOLATION"


ARMS: list[ArmSpec] = [
    ArmSpec(
        name="arm1-control-additive",
        conditioning_type="additive",
        hidden_dim=256,
        num_layers=6,
        num_heads=8,
        machine=SPARK_MACHINE,
        role="CONTROL",
    ),
    ArmSpec(
        name="arm2-fourier-prod",
        conditioning_type="fourier_cross_attention",
        hidden_dim=256,
        num_layers=6,
        num_heads=8,
        machine=LOCAL_MACHINE,
        role="TREATMENT",
    ),
    ArmSpec(
        name="arm3-fourier-seed",
        conditioning_type="fourier_cross_attention",
        hidden_dim=192,
        num_layers=3,
        num_heads=6,
        machine=LOCAL_MACHINE,
        role="ABLATION",
    ),
    ArmSpec(
        name="arm4-swiglu",
        conditioning_type="swiglu",
        hidden_dim=256,
        num_layers=6,
        num_heads=8,
        machine=SPARK_MACHINE,
        role="TREATMENT",
    ),
    ArmSpec(
        name="arm5-cross-attention",
        conditioning_type="cross_attention",
        hidden_dim=256,
        num_layers=6,
        num_heads=8,
        machine=SPARK_MACHINE,
        role="ISOLATION",
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


def _build_train_cli_args(arm: ArmSpec, local_output_dir: str, dataset_revision: str, smoke_test: bool) -> list[str]:
    """Build the CLI argument list for scripts/train_scoutgpt_hf.py --local-mode."""
    args = [
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
        "2" if smoke_test else str(SHARED_TRAINING_CONFIG["epochs"]),
        "--batch-size",
        str(SHARED_TRAINING_CONFIG["batch_size"]),
        "--lr",
        str(SHARED_TRAINING_CONFIG["learning_rate"]),
        "--patience",
        str(SHARED_TRAINING_CONFIG["patience"]),
    ]
    if smoke_test:
        args.extend(["--max-episodes", "1000"])
    # DATASET_PINNED_SHA is read by train_scoutgpt_hf.py via env var, not CLI.
    return args


def _dispatch_local(arm: ArmSpec, dataset_revision: str, smoke_test: bool) -> subprocess.Popen[bytes]:
    """Dispatch an arm to the local 5070 Ti as a subprocess."""
    arm_out_dir = ARTIFACTS_DIR / arm.name
    arm_out_dir.mkdir(parents=True, exist_ok=True)
    log_path = arm_out_dir / "run-arm.log"
    logger.info("Dispatching %s on local 5070 Ti → %s", arm.name, log_path)

    cli_args = _build_train_cli_args(arm, str(arm_out_dir), dataset_revision, smoke_test)
    cmd = [sys.executable, str(REPO_ROOT / "scripts" / "train_scoutgpt_hf.py"), *cli_args]

    env = _env_with_dataset_pinned(dataset_revision)
    log_fh = log_path.open("wb")
    # S603, S607: orchestrator dispatch with internal config only; partial path OK (python in PATH).
    return subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT, env=env)  # noqa: S603


def _dispatch_spark(arm: ArmSpec, dataset_revision: str, smoke_test: bool) -> subprocess.Popen[bytes]:
    """Dispatch an arm to DGX Spark via SSH."""
    arm_out_dir_local = ARTIFACTS_DIR / arm.name
    arm_out_dir_local.mkdir(parents=True, exist_ok=True)
    log_path = arm_out_dir_local / "run-arm.log"
    logger.info("Dispatching %s on DGX Spark → %s", arm.name, log_path)

    spark_out_dir = f"{SPARK_WORKSPACE}/artifacts/fourier-scoutgpt/{arm.name}"
    cli_args = _build_train_cli_args(arm, spark_out_dir, dataset_revision, smoke_test)
    cli_str = " ".join(cli_args)

    remote_script = (
        f"cd {SPARK_WORKSPACE} && "
        f"{SPARK_ENV_ACTIVATE} && "
        f"mkdir -p {spark_out_dir} && "
        f"export DATASET_PINNED_SHA={dataset_revision} && "
        # PYTHONPATH=./src makes Python import from the synced source tree, not
        # from the installed wheel (which is 0.3.4 and missing the new
        # conditioning_type values + load_training_data(revision=) kwarg).
        "export PYTHONPATH=./src && "
        "export PYTHONIOENCODING=utf-8 && "
        f"python scripts/train_scoutgpt_hf.py {cli_str}"
    )

    log_fh = log_path.open("wb")
    # S603, S607: SSH dispatch to known Spark host; remote script built from internal config.
    return subprocess.Popen(  # noqa: S603
        ["ssh", SPARK_SSH, remote_script],  # noqa: S607
        stdout=log_fh,
        stderr=subprocess.STDOUT,
    )


def _env_with_dataset_pinned(dataset_revision: str) -> dict[str, str]:
    """Return a copy of os.environ with DATASET_PINNED_SHA + UTF-8 encoding set."""
    import os

    env = dict(os.environ)
    env["DATASET_PINNED_SHA"] = dataset_revision
    env.setdefault("PYTHONIOENCODING", "utf-8")
    return env


def _collect_metrics(arm: ArmSpec) -> dict[str, object]:
    """Collect metrics.json from either local disk or Spark (via scp)."""
    local_path = ARTIFACTS_DIR / arm.name / "metrics.json"

    if arm.machine == SPARK_MACHINE:
        spark_metrics_path = f"{SPARK_WORKSPACE}/artifacts/fourier-scoutgpt/{arm.name}/metrics.json"
        logger.info("Fetching metrics from Spark: %s", spark_metrics_path)
        # S603, S607: scp from known Spark host to internal local path.
        subprocess.run(  # noqa: S603
            ["scp", f"{SPARK_SSH}:{spark_metrics_path}", str(local_path)],  # noqa: S607
            check=True,
            capture_output=True,
        )

    if not local_path.exists():
        msg = f"metrics.json missing for arm {arm.name} at {local_path}"
        raise FileNotFoundError(msg)

    return json.loads(local_path.read_text(encoding="utf-8"))


def _transfer_branch_to_spark() -> None:
    """Transfer the current working tree (incl uncommitted changes) to Spark.

    Uses ``tar cz | ssh tar xz`` rather than rsync because rsync is not
    available in Windows Git Bash by default. Excludes artifacts/, .venv/,
    __pycache__/, .git/, build caches — but includes uncommitted changes
    (critical: this cycle's decoder+promotion_rules code is pre-commit).
    """
    logger.info("Transferring branch to Spark (tar + ssh)...")
    # Ensure destination dir exists
    # S603, S607: mkdir on known Spark host; internal paths only.
    subprocess.run(  # noqa: S603
        ["ssh", SPARK_SSH, f"mkdir -p {SPARK_WORKSPACE}"],  # noqa: S607
        check=True,
        capture_output=True,
    )
    # Stream tar over ssh. Using subprocess pipe so tar output never touches disk.
    tar_cmd = [
        "tar",
        "czf",
        "-",
        "--exclude=.git",
        "--exclude=artifacts",
        "--exclude=.venv",
        "--exclude=__pycache__",
        "--exclude=.benchmarks",
        "--exclude=node_modules",
        "--exclude=.pytest_cache",
        "--exclude=dist",
        "--exclude=build",
        ".",
    ]
    ssh_extract_cmd = ["ssh", SPARK_SSH, f"cd {SPARK_WORKSPACE} && tar xzf -"]

    # S603: both ends are orchestrator-internal.
    tar_proc = subprocess.Popen(tar_cmd, stdout=subprocess.PIPE, cwd=str(REPO_ROOT))  # noqa: S603
    ssh_proc = subprocess.Popen(ssh_extract_cmd, stdin=tar_proc.stdout)  # noqa: S603
    if tar_proc.stdout is not None:
        tar_proc.stdout.close()  # Let tar SIGPIPE on ssh exit.
    ssh_rc = ssh_proc.wait()
    tar_rc = tar_proc.wait()
    if tar_rc != 0 or ssh_rc != 0:
        msg = f"Branch transfer to Spark failed: tar_rc={tar_rc} ssh_rc={ssh_rc}"
        raise RuntimeError(msg)
    logger.info("Transfer complete")


def _write_summary(all_metrics: dict[str, dict[str, object]], dataset_revision: str) -> None:
    """Apply apply_decision_rule and write SUMMARY.md."""
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from analytics.promotion_rules import apply_decision_rule

    def _metric_or_none(arm_name: str, key: str) -> float | None:
        m = all_metrics.get(arm_name, {}) or {}
        v = m.get(key)
        if isinstance(v, (int, float)):
            return float(v)
        return None

    rho_ctrl = _metric_or_none("arm1-control-additive", "counterfactual_spearman_rho") or _metric_or_none(
        "arm1-control-additive", "counterfactual_rho"
    )
    rho_fourier = _metric_or_none("arm2-fourier-prod", "counterfactual_spearman_rho") or _metric_or_none(
        "arm2-fourier-prod", "counterfactual_rho"
    )
    rho_swiglu = _metric_or_none("arm4-swiglu", "counterfactual_spearman_rho") or _metric_or_none(
        "arm4-swiglu", "counterfactual_rho"
    )
    top1_ctrl = _metric_or_none("arm1-control-additive", "next_action_top1_accuracy") or _metric_or_none(
        "arm1-control-additive", "test_top1"
    )
    top1_fourier = _metric_or_none("arm2-fourier-prod", "next_action_top1_accuracy") or _metric_or_none(
        "arm2-fourier-prod", "test_top1"
    )
    top1_swiglu = _metric_or_none("arm4-swiglu", "next_action_top1_accuracy") or _metric_or_none(
        "arm4-swiglu", "test_top1"
    )

    if None in (rho_ctrl, rho_fourier, rho_swiglu, top1_ctrl, top1_fourier, top1_swiglu):
        logger.warning("Some metrics missing — disposition will be 'UNKNOWN' for missing arms")
        fourier_disposition = "UNKNOWN"
        swiglu_disposition = "UNKNOWN"
    else:
        # Type narrowing: the None-check above guarantees all 6 values are floats.
        fourier_disposition = apply_decision_rule(
            float(rho_ctrl),  # type: ignore[arg-type]
            float(rho_fourier),  # type: ignore[arg-type]
            float(top1_ctrl),  # type: ignore[arg-type]
            float(top1_fourier),  # type: ignore[arg-type]
        )
        swiglu_disposition = apply_decision_rule(
            float(rho_ctrl),  # type: ignore[arg-type]
            float(rho_swiglu),  # type: ignore[arg-type]
            float(top1_ctrl),  # type: ignore[arg-type]
            float(top1_swiglu),  # type: ignore[arg-type]
        )

    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = [
        "# ScoutGPT Fourier / Swiglu Promotion A/B — Summary",
        "",
        f"**Date:** {time.strftime('%Y-%m-%d', time.gmtime())}",
        "**Branch:** `evolve/scoutgpt-fourier-promote`",
        "**Execution venue:** Local (1x RTX 5070 Ti + 1x DGX Spark via SSH)",
        f"**Dataset:** `{TRAINING_DATASET}` revision `{dataset_revision}`",
        "**Spec:** `docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md`",
        "",
        "## Pre-registered decision rule",
        "",
        "PROMOTE iff `rho_trt - rho_ctrl >= +0.10` AND `top1_trt >= top1_ctrl - 0.005`.",
        "Applied via `src/analytics/promotion_rules.py::apply_decision_rule`.",
        "",
        "## Headline",
        "",
        (
            "| Arm | Role | conditioning_type | hd/L/H | `counterfactual_rho` | "
            "`test_top1` | `val_loss` | `wall_clock_min` |"
        ),
        "|---|---|---|---|---:|---:|---:|---:|",
    ]
    for arm in ARMS:
        m = all_metrics.get(arm.name, {}) or {}

        def _fmt(v: object) -> str:
            if isinstance(v, (int, float)):
                return f"{v:.4f}"
            return "—"

        rho = _fmt(m.get("counterfactual_spearman_rho") or m.get("counterfactual_rho"))
        top1 = _fmt(m.get("next_action_top1_accuracy") or m.get("test_top1"))
        vloss = _fmt(m.get("val_loss"))
        wc = _fmt(m.get("wall_clock_minutes"))
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
            f"- **Fourier** (Arm 2 vs Arm 1): **{fourier_disposition}**",
            f"- **Swiglu** (Arm 4 vs Arm 1): **{swiglu_disposition}**",
            "",
            "## Cross-reference",
            "",
            "- L2 harvest (2026-04-20): fourier_cross_attention rho=+0.3799 at 15-epoch evolve-scale.",
            "- RoPE-for-ScoutGPT (2026-04-19): rho delta +0.016 rejected.",
            "",
            "## Informational arms",
            "",
            ("- Arm 3 (`fourier-seed` at 192d/3L/6h): capacity ablation. Compare to Arm 2 for capacity effect."),
            (
                "- Arm 5 (`cross-attention` at 256d/6L/8h): mechanism isolation. "
                "Compare Arm 2 vs Arm 5 to isolate the Fourier spatial contribution."
            ),
            "",
            "## Follow-ups",
            "",
            "See `docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md` Section I.",
            "",
        ]
    )

    SUMMARY_PATH.write_text("\n".join(lines), encoding="utf-8")
    logger.info("SUMMARY written to %s", SUMMARY_PATH)


def cmd_drive(smoke_test: bool) -> int:
    """Execute the 5-arm A/B (or smoke subset if smoke_test=True)."""
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    dataset_revision = resolve_dataset_revision()
    manifest = build_dispatch_manifest(dataset_revision)
    (ARTIFACTS_DIR / "dispatch-manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    logger.info("Dispatch manifest pinned dataset @ %s", dataset_revision)

    # Transfer branch only if any Spark arms are scheduled.
    if any(a.machine == SPARK_MACHINE for a in ARMS):
        _transfer_branch_to_spark()

    # Pair-parallel dispatch. Group arms by machine, iterate until all done.
    pending = list(ARMS)
    running: dict[str, tuple[ArmSpec, subprocess.Popen[bytes]]] = {}
    failed: list[str] = []

    while pending or running:
        # Dispatch next arm per idle machine (one per machine at a time).
        busy_machines = {arm.machine for arm, _ in running.values()}
        still_pending: list[ArmSpec] = []
        for arm in pending:
            if arm.machine in busy_machines:
                still_pending.append(arm)
                continue
            proc = (
                _dispatch_local(arm, dataset_revision, smoke_test)
                if arm.machine == LOCAL_MACHINE
                else _dispatch_spark(arm, dataset_revision, smoke_test)
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

    # Collect metrics
    all_metrics = {arm.name: _collect_metrics(arm) for arm in ARMS}

    (ARTIFACTS_DIR / "results.json").write_text(json.dumps(all_metrics, indent=2, default=str), encoding="utf-8")
    logger.info("All %d arms complete; results at %s", len(ARMS), ARTIFACTS_DIR / "results.json")

    _write_summary(all_metrics, dataset_revision)
    return 0


def cmd_drive_dry_run() -> int:
    """Dry-run: print the dispatch plan without executing."""
    try:
        dataset_revision = resolve_dataset_revision()
    except Exception as exc:  # noqa: BLE001 — dry-run falls back to placeholder if network fails
        logger.warning("Dataset SHA resolution failed (%s); using placeholder for dry-run", exc)
        dataset_revision = "<unresolved-network-failure>"

    manifest = build_dispatch_manifest(dataset_revision)
    print(json.dumps(manifest, indent=2, default=str))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Fourier/Swiglu promotion A/B orchestrator.")
    parser.add_argument(
        "--mode",
        choices=["drive"],
        default="drive",
        help="Only mode supported: drive (top-level dispatch).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the dispatch plan without executing. Useful for reviewing arm roster and dataset SHA.",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Reduce epochs to 2 and dataset to 1000 episodes for smoke-testing each arm end-to-end.",
    )
    args = parser.parse_args()

    if args.dry_run:
        return cmd_drive_dry_run()
    return cmd_drive(smoke_test=args.smoke_test)


if __name__ == "__main__":
    sys.exit(main())
