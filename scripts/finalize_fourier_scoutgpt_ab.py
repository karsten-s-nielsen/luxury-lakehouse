"""Finalize the Fourier/Swiglu A/B: collect all 5 arms' metrics and write SUMMARY.

Used when the main orchestrator was interrupted mid-run (e.g. to redirect arms
across machines). Does NOT dispatch any arms — only collects already-written
metrics.json files and generates docs/evolve/fourier-scoutgpt/SUMMARY.md.

Per-arm paths (machine-specific):
  arm1-control-additive: Spark (scp from ~/Development/luxury-lakehouse-fourier-promote/...)
  arm2-fourier-prod: local (artifacts/fourier-scoutgpt/arm2-fourier-prod/metrics.json)
  arm3-fourier-seed: local
  arm4-swiglu: Spark
  arm5-cross-attention: local (redirected from original Spark plan)
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Literal

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from analytics.promotion_rules import apply_decision_rule

REPO_ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS_DIR = REPO_ROOT / "artifacts" / "fourier-scoutgpt"
SUMMARY_PATH = REPO_ROOT / "docs" / "evolve" / "fourier-scoutgpt" / "SUMMARY.md"

SPARK_SSH = "karsten@192.168.68.73"
SPARK_WORKSPACE = "~/Development/luxury-lakehouse-fourier-promote"

HF_ORG = "luxury-lakehouse"
TRAINING_DATASET = f"{HF_ORG}/scoutgpt-training-data"

# Arm roster for this cycle. (Arm 5 redirected from Spark to local.)
ARMS = [
    ("arm1-control-additive", "additive", 256, 6, 8, "spark", "CONTROL"),
    ("arm2-fourier-prod", "fourier_cross_attention", 256, 6, 8, "local", "TREATMENT"),
    ("arm3-fourier-seed", "fourier_cross_attention", 192, 3, 6, "local", "ABLATION"),
    ("arm4-swiglu", "swiglu", 256, 6, 8, "spark", "TREATMENT"),
    ("arm5-cross-attention", "cross_attention", 256, 6, 8, "local", "ISOLATION"),
]


def _collect_metrics(arm_name: str, machine: Literal["local", "spark"]) -> dict[str, object]:
    local_path = ARTIFACTS_DIR / arm_name / "metrics.json"
    if machine == "spark":
        remote_path = f"{SPARK_WORKSPACE}/artifacts/fourier-scoutgpt/{arm_name}/metrics.json"
        # S603, S607: scp from known Spark host to internal local path.
        subprocess.run(  # noqa: S603
            ["scp", f"{SPARK_SSH}:{remote_path}", str(local_path)],  # noqa: S607
            check=True,
            capture_output=True,
        )
    if not local_path.exists():
        msg = f"metrics.json missing for arm {arm_name} at {local_path}"
        raise FileNotFoundError(msg)
    return json.loads(local_path.read_text(encoding="utf-8"))


def main() -> int:
    all_metrics: dict[str, dict[str, object]] = {}
    for arm_name, _, _, _, _, machine, _ in ARMS:
        all_metrics[arm_name] = _collect_metrics(arm_name, machine)  # type: ignore[arg-type]

    (ARTIFACTS_DIR / "results.json").write_text(json.dumps(all_metrics, indent=2, default=str), encoding="utf-8")

    # Decision rule: Fourier (Arm 2 vs Arm 1) and Swiglu (Arm 4 vs Arm 1).
    ctrl = all_metrics["arm1-control-additive"]
    fourier = all_metrics["arm2-fourier-prod"]
    swiglu = all_metrics["arm4-swiglu"]

    rho_ctrl = float(ctrl["mean_spearman_rho"])  # type: ignore[arg-type]
    top1_ctrl = float(ctrl["test_top1_accuracy"])  # type: ignore[arg-type]
    rho_fourier = float(fourier["mean_spearman_rho"])  # type: ignore[arg-type]
    top1_fourier = float(fourier["test_top1_accuracy"])  # type: ignore[arg-type]
    rho_swiglu = float(swiglu["mean_spearman_rho"])  # type: ignore[arg-type]
    top1_swiglu = float(swiglu["test_top1_accuracy"])  # type: ignore[arg-type]

    fourier_disposition = apply_decision_rule(rho_ctrl, rho_fourier, top1_ctrl, top1_fourier)
    swiglu_disposition = apply_decision_rule(rho_ctrl, rho_swiglu, top1_ctrl, top1_swiglu)

    dataset_revision = str(ctrl.get("dataset_commit", "unknown"))

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
            "| Arm | Role | conditioning_type | hd/L/H | epochs | `counterfactual_rho` | "
            "`test_top1` | `val_loss` | `wall_min` |"
        ),
        "|---|---|---|---|---:|---:|---:|---:|---:|",
    ]

    def _fmt_float(v: object) -> str:
        if isinstance(v, (int, float)):
            return f"{v:.4f}"
        return "—"

    def _fmt_int(v: object) -> str:
        if isinstance(v, (int, float)):
            return f"{int(v)}"
        return "—"

    for arm_name, cond_type, hd, nl, nh, _machine, role in ARMS:
        m = all_metrics.get(arm_name, {}) or {}
        rho = _fmt_float(m.get("mean_spearman_rho"))
        top1 = _fmt_float(m.get("test_top1_accuracy"))
        vloss = _fmt_float(m.get("val_loss") or m.get("test_loss"))
        wc = _fmt_float(m.get("wall_clock_minutes"))
        epochs = _fmt_int(m.get("actual_epochs"))
        lines.append(
            f"| {arm_name} | {role} | {cond_type} | {hd}/{nl}/{nh} | {epochs} | {rho} | {top1} | {vloss} | {wc} |"
        )

    lines.extend(
        [
            "",
            "## Dispositions",
            "",
            (
                f"- **Fourier** (Arm 2 vs Arm 1): **{fourier_disposition}** "
                f"(rho delta = {rho_fourier - rho_ctrl:+.4f}, top1 delta = {top1_fourier - top1_ctrl:+.4f})"
            ),
            (
                f"- **Swiglu** (Arm 4 vs Arm 1): **{swiglu_disposition}** "
                f"(rho delta = {rho_swiglu - rho_ctrl:+.4f}, top1 delta = {top1_swiglu - top1_ctrl:+.4f})"
            ),
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
            "## Execution note",
            "",
            (
                "The main orchestrator was interrupted mid-run to redirect Arm 5 from DGX Spark "
                "to the local RTX 5070 Ti (the Spark critical path was the bottleneck; local "
                "5070 Ti was idle after Arms 2+3 finished early). Arms 1+4 ran on Spark, "
                "Arms 2+3+5 ran on local. This finalizer script collected all 5 arms' metrics "
                "post-hoc and wrote this SUMMARY."
            ),
            "",
            "## Follow-ups",
            "",
            "See `docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md` Section I.",
            "",
        ]
    )

    SUMMARY_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"SUMMARY written to {SUMMARY_PATH}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
