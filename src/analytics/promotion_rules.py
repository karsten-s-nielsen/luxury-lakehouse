"""Pre-registered promotion decision rule for evolve cycles.

The rule is a pure function so SUMMARY.md generation cannot be
motivated-reasoned post-hoc. Thresholds are calibrated from the
ScoutGPT RoPE A/B (session 50) rejection margin — 6x above it — and
from the L2 harvest's observed rho_std (~0.30), targeting 0.33 sigma.

See docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md section C.
"""

from __future__ import annotations

from typing import Literal

# Threshold constants — pre-registered. Changing these after an A/B runs
# is motivated reasoning. If the rule ever needs to change, do it in a
# separate PR before the next cycle.
RHO_PROMOTE_THRESHOLD: float = 0.10
TOP1_REGRESSION_FLOOR: float = -0.005


def apply_decision_rule(
    rho_ctrl: float,
    rho_trt: float,
    top1_ctrl: float,
    top1_trt: float,
) -> Literal["PROMOTE", "ARCHIVE"]:
    """Return PROMOTE iff rho gain >= +0.10 AND top1 regression >= -0.005.

    Args:
        rho_ctrl: Control arm's mean Spearman rho (counterfactual ranking).
        rho_trt: Treatment arm's mean Spearman rho.
        top1_ctrl: Control arm's top-1 next-action accuracy on test set.
        top1_trt: Treatment arm's top-1 next-action accuracy.

    Returns:
        "PROMOTE" if both conditions hold, otherwise "ARCHIVE".
    """
    rho_delta = rho_trt - rho_ctrl
    top1_delta = top1_trt - top1_ctrl

    rho_ok = rho_delta >= RHO_PROMOTE_THRESHOLD
    top1_ok = top1_delta >= TOP1_REGRESSION_FLOOR

    if rho_ok and top1_ok:
        return "PROMOTE"
    return "ARCHIVE"
