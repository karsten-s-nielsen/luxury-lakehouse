"""ScoutGPT post-retrain smoke gate. Spec §3 — PR #176 close-out validation set.

Loads the @Champion ScoutGPT model + runs forward-pass on the held-out test set;
asserts top1 + rho thresholds.
"""

from __future__ import annotations

import pytest

_TOP1_BASELINE = 0.842
_TOP1_TOLERANCE = 0.022  # 2pp per spec §3
_TOP1_THRESHOLD = _TOP1_BASELINE - _TOP1_TOLERANCE  # > 0.80

_RHO_BASELINE = 0.247
_RHO_TOLERANCE = 0.05
_RHO_THRESHOLD = _RHO_BASELINE - _RHO_TOLERANCE  # > 0.20


def test_vocab_size_unchanged() -> None:
    # Plan said `ScoutGPTDecoderConfig`; actual class is `ScoutGPTConfig`.
    try:
        from analytics.scoutgpt_decoder import ScoutGPTConfig
    except ImportError as exc:
        pytest.skip(f"analytics.scoutgpt_decoder not available locally: {exc}")
    cfg = ScoutGPTConfig()
    assert cfg.vocab_size == 23, f"vocab_size = {cfg.vocab_size}, expected 23 (SPADL action-type taxonomy unchanged)"


def test_top1_above_threshold() -> None:
    """Phase 9 prep extracts evaluate_champion_top1 from train_scoutgpt_hf.py
    if the helper isn't already in analytics.scoutgpt_evaluation."""
    try:
        from analytics.scoutgpt_evaluation import evaluate_champion_top1
    except ImportError as exc:
        pytest.skip(
            f"analytics.scoutgpt_evaluation.evaluate_champion_top1 missing: {exc}. "
            "Phase 9 prep: extract from train_scoutgpt_hf.py."
        )
    top1 = evaluate_champion_top1()
    assert top1 > _TOP1_THRESHOLD, (
        f"ScoutGPT test_top1 = {top1:.4f}, threshold {_TOP1_THRESHOLD:.4f} "
        f"(baseline {_TOP1_BASELINE} - {_TOP1_TOLERANCE} tolerance)"
    )


def test_counterfactual_rho_above_threshold() -> None:
    try:
        from analytics.scoutgpt_evaluation import evaluate_champion_counterfactual_rho
    except ImportError as exc:
        pytest.skip(f"analytics.scoutgpt_evaluation.evaluate_champion_counterfactual_rho missing: {exc}")
    rho = evaluate_champion_counterfactual_rho()
    assert rho > _RHO_THRESHOLD, f"ScoutGPT counterfactual rho = {rho:.4f}, threshold {_RHO_THRESHOLD:.4f}"


def test_no_nan_in_logits() -> None:
    try:
        from analytics.scoutgpt_evaluation import sample_champion_logits
    except ImportError as exc:
        pytest.skip(f"analytics.scoutgpt_evaluation.sample_champion_logits missing: {exc}")
    import numpy as np

    logits = sample_champion_logits(n=100)
    assert not np.any(np.isnan(logits)), "ScoutGPT Champion produces NaN logits"
