"""Tests for the pre-registered retention rule.

Ensures apply_retention_rule is a pure function with the pre-registered thresholds
documented in docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md
section A.3. The rule is locked in code so that SUMMARY.md disposition of existing
mechanisms cannot be motivated-reasoned post-hoc.
"""

from __future__ import annotations

import pytest

from analytics.promotion_rules import (
    RETENTION_DEPRECATE_RHO_THRESHOLD,
    TOP1_REGRESSION_FLOOR,
    apply_retention_rule,
)


def test_retention_threshold_is_0_05() -> None:
    """The +0.05 rho threshold is the pre-registered parity-threshold constant.

    Changing this value after data is collected is motivated reasoning. If the rule
    needs to change, do it in a separate PR before the next cycle.
    """
    assert RETENTION_DEPRECATE_RHO_THRESHOLD == 0.05


def test_shares_top1_floor_with_promotion_rule() -> None:
    """Retention rule shares TOP1_REGRESSION_FLOOR with apply_decision_rule."""
    assert TOP1_REGRESSION_FLOOR == -0.005


@pytest.mark.parametrize(
    ("rho_inc", "rho_chl", "top1_inc", "top1_chl", "expected"),
    [
        # Exactly at +0.05 rho threshold, no top1 regression → DEPRECATE (boundary).
        # Using delta of exactly 0.05 (literal) to sidestep float subtraction drift
        # at the decision boundary.
        (0.0, 0.05, 0.0, 0.0, "DEPRECATE"),
        # Just below +0.05 threshold → KEEP
        (0.20, 0.249, 0.80, 0.80, "KEEP"),
        # Rho gain passes BUT top1 regression below floor → KEEP (safety floor wins)
        (0.10, 0.20, 0.80, 0.79, "KEEP"),
        # Clear deprecate on rho AND top1 exactly at -0.005 floor → DEPRECATE (boundary).
        # Using literal -0.005 for top1 delta to sidestep float subtraction drift.
        (0.0, 0.06, 0.0, -0.005, "DEPRECATE"),
        # Clear deprecate: +0.10 rho, no top1 loss
        (0.15, 0.25, 0.80, 0.80, "DEPRECATE"),
        # RoPE historical case (rho delta +0.016) → KEEP
        (0.115, 0.131, 0.810, 0.815, "KEEP"),
        # PR #166 Arm 5 vs Arm 2 historical case (rho delta +0.018, top1 delta +0.004) → KEEP
        # fourier: rho=0.2812, top1=0.8368; cross_attn: rho=0.2995, top1=0.8410
        (0.2812, 0.2995, 0.8368, 0.8410, "KEEP"),
        # Negative rho delta (challenger worse than incumbent) → KEEP
        (0.30, 0.20, 0.80, 0.80, "KEEP"),
    ],
    ids=[
        "boundary_rho_at_threshold",
        "just_below_rho_threshold",
        "top1_regression_too_large",
        "top1_at_floor_exactly",
        "clear_deprecate",
        "rope_historical_case",
        "pr166_fourier_vs_cross_attn",
        "negative_rho_delta",
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


def test_apply_retention_rule_is_pure() -> None:
    """Same inputs produce same output, no matter the call order."""
    inputs = {
        "rho_incumbent": 0.2812,
        "rho_challenger": 0.2995,
        "top1_incumbent": 0.8368,
        "top1_challenger": 0.8410,
    }
    first = apply_retention_rule(**inputs)
    second = apply_retention_rule(**inputs)
    assert first == second == "KEEP"


def test_apply_retention_rule_returns_literal_strings() -> None:
    """The return type is Literal['KEEP', 'DEPRECATE'] — no other strings allowed."""
    result = apply_retention_rule(
        rho_incumbent=0.20,
        rho_challenger=0.40,
        top1_incumbent=0.80,
        top1_challenger=0.80,
    )
    assert result in {"KEEP", "DEPRECATE"}
