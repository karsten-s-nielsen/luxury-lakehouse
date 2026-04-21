"""Tests for the pre-registered Fourier/Swiglu promotion decision rule.

The rule is locked in code so that SUMMARY.md generation cannot be
motivated-reasoned post-hoc. See
docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md
section C.
"""

from __future__ import annotations

import pytest

from analytics.promotion_rules import apply_decision_rule


@pytest.mark.parametrize(
    ("rho_ctrl", "rho_trt", "top1_ctrl", "top1_trt", "expected"),
    [
        # Boundary — rho delta exactly +0.10, top1 regression exactly -0.005 → PROMOTE
        (0.030, 0.130, 0.815, 0.810, "PROMOTE"),
        # Clear promote — both metrics comfortably above thresholds
        (0.030, 0.400, 0.815, 0.820, "PROMOTE"),
        # Rho gain just below +0.10 → ARCHIVE
        (0.030, 0.129, 0.815, 0.815, "ARCHIVE"),
        # Top1 regression just beyond -0.005 → ARCHIVE (even with big rho gain)
        (0.030, 0.400, 0.815, 0.809, "ARCHIVE"),
        # RoPE historical case: rho delta +0.016, top1 delta +0.00009 → ARCHIVE
        (0.02986, 0.04547, 0.81535, 0.81526, "ARCHIVE"),
        # Clear archive — no signal
        (0.030, 0.025, 0.815, 0.815, "ARCHIVE"),
        # Rho regression, top1 stable → ARCHIVE
        (0.030, -0.100, 0.815, 0.815, "ARCHIVE"),
    ],
    ids=[
        "boundary_both_at_threshold",
        "clear_promote",
        "rho_gain_just_below_threshold",
        "top1_regression_too_large",
        "rope_historical_case",
        "clear_archive_no_signal",
        "rho_regression",
    ],
)
def test_apply_decision_rule(rho_ctrl: float, rho_trt: float, top1_ctrl: float, top1_trt: float, expected: str) -> None:
    assert apply_decision_rule(rho_ctrl, rho_trt, top1_ctrl, top1_trt) == expected


def test_apply_decision_rule_returns_literal_strings() -> None:
    """The return type is Literal['PROMOTE', 'ARCHIVE'] — no other strings allowed."""
    result = apply_decision_rule(0.0, 0.5, 0.8, 0.8)
    assert result in {"PROMOTE", "ARCHIVE"}
