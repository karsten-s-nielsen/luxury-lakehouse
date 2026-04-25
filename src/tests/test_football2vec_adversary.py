"""Tests for src/analytics/football2vec_adversary.py."""

from __future__ import annotations

import math

import pytest


def test_adversary_config_defaults():
    """AdversaryConfig() produces the pinned production defaults."""
    from analytics.football2vec_adversary import AdversaryConfig

    cfg = AdversaryConfig()
    assert cfg.architecture == "linear"
    assert cfg.lambda_schedule_shape == "linear"
    assert cfg.lambda_max == pytest.approx(0.2)
    assert cfg.lambda_warmup_epochs == 5


def test_adversary_config_is_frozen():
    """Mutating a frozen dataclass should raise."""
    from dataclasses import FrozenInstanceError

    from analytics.football2vec_adversary import AdversaryConfig

    cfg = AdversaryConfig()
    with pytest.raises(FrozenInstanceError):
        cfg.lambda_max = 0.5  # type: ignore[misc]


def test_lambda_schedule_linear_matches_production():
    """Linear shape at production defaults reproduces current hardcoded ramp."""
    from analytics.football2vec_adversary import AdversaryConfig, lambda_schedule

    cfg = AdversaryConfig()  # linear, lambda_max=0.2, warmup=5
    # Ramp epochs 0..4 linearly from 0 to lambda_max, then hold.
    expected_by_epoch = {0: 0.0, 1: 0.04, 2: 0.08, 3: 0.12, 4: 0.16, 5: 0.2, 10: 0.2, 29: 0.2}
    for epoch, expected in expected_by_epoch.items():
        actual = lambda_schedule(cfg, epoch, total_epochs=30)
        assert actual == pytest.approx(expected, abs=1e-9), f"epoch={epoch}"


def test_lambda_schedule_sigmoid_monotonic_to_max():
    """Sigmoid shape: monotonic from 0 at epoch 0 to approximately lambda_max at or near warmup."""
    from analytics.football2vec_adversary import AdversaryConfig, lambda_schedule

    cfg = AdversaryConfig(lambda_schedule_shape="sigmoid")
    values = [lambda_schedule(cfg, e, total_epochs=30) for e in range(31)]
    # Sigmoid at progress=0 gives ~0.0067 * lambda_max (not exactly zero). Accept <1% of lambda_max.
    assert values[0] < 0.01 * cfg.lambda_max
    # Monotonic non-decreasing.
    for i in range(1, len(values)):
        assert values[i] >= values[i - 1] - 1e-9, f"non-monotonic at epoch {i}"
    # Reaches lambda_max (within 2%) by epoch 10 (2x warmup) and holds.
    assert values[10] == pytest.approx(cfg.lambda_max, rel=0.02)
    assert values[29] == pytest.approx(cfg.lambda_max, rel=0.02)


def test_lambda_schedule_cosine_monotonic_to_max():
    """Cosine shape: monotonic from 0 at epoch 0 to lambda_max at warmup_epochs, then holds."""
    from analytics.football2vec_adversary import AdversaryConfig, lambda_schedule

    cfg = AdversaryConfig(lambda_schedule_shape="cosine")
    values = [lambda_schedule(cfg, e, total_epochs=30) for e in range(31)]
    assert values[0] == pytest.approx(0.0, abs=1e-9)
    for i in range(1, len(values)):
        assert values[i] >= values[i - 1] - 1e-9, f"non-monotonic at epoch {i}"
    # Reaches lambda_max exactly at warmup_epochs (cosine from 0 to pi/2).
    assert values[cfg.lambda_warmup_epochs] == pytest.approx(cfg.lambda_max, abs=1e-9)
    assert values[29] == pytest.approx(cfg.lambda_max, abs=1e-9)
    # Cosine is smoother than linear: at the midpoint of warmup, cosine ramp is below linear.
    # progress=0.5 -> cosine: 0.2 * 0.5 * (1 - cos(pi*0.5)) = 0.2 * 0.5 * 1.0 = 0.1
    # linear at same progress: 0.2 * 0.5 = 0.1. Actually equal at midpoint.
    # Better check: at progress=0.25 (epoch 1.25), cosine < linear.
    # We test epoch 2 (progress=0.4) - cosine should be below linear here.
    cfg_linear = AdversaryConfig(lambda_schedule_shape="linear")
    assert values[2] < lambda_schedule(cfg_linear, 2, total_epochs=30) or math.isclose(
        values[2], lambda_schedule(cfg_linear, 2, total_epochs=30)
    )


def test_lambda_schedule_rejects_unknown_shape():
    """Unknown shape raises ValueError."""
    from analytics.football2vec_adversary import AdversaryConfig, lambda_schedule

    cfg = AdversaryConfig()
    # Bypass dataclass validation by object.__setattr__ so lambda_schedule itself raises.
    object.__setattr__(cfg, "lambda_schedule_shape", "bogus")
    with pytest.raises(ValueError, match="unknown lambda_schedule_shape"):
        lambda_schedule(cfg, 0, total_epochs=30)


def test_build_adversary_linear_matches_team_classifier():
    """build_adversary(AdversaryConfig(), ...) has the same non-grl state_dict key structure
    as TeamClassifierHead from football2vec_transformer.py, and a compatible forward shape."""
    import torch

    from analytics.football2vec_adversary import AdversaryConfig, build_adversary
    from analytics.football2vec_transformer import TeamClassifierHead

    hd, num_comp = 192, 22
    torch.manual_seed(42)
    ours = build_adversary(AdversaryConfig(), hd, num_comp)
    torch.manual_seed(42)
    theirs = TeamClassifierHead(hd, num_comp, lambda_val=0.2)

    # Same non-grl state_dict key structure (both have classifier.weight + classifier.bias).
    our_keys = {k for k in ours.state_dict() if not k.startswith("grl.")}
    their_keys = {k for k in theirs.state_dict() if not k.startswith("grl.")}
    assert our_keys == their_keys, f"state_dict key mismatch: {our_keys} vs {their_keys}"

    # Forward-shape compatibility.
    x = torch.randn(4, 8, hd)  # (B, S, hd) — EV2 adversary takes per-token + mask.
    mask = torch.ones(4, 8, dtype=torch.bool)
    # Our adversary signature: (encoder_output, attention_mask). Linear head uses CLS pool internally.
    our_logits = ours(x, mask)
    # TeamClassifierHead signature is (pooled,); call with CLS pool of same input.
    their_logits = theirs(x[:, 0])

    assert our_logits.shape == their_logits.shape == (4, num_comp)


def test_build_adversary_rejects_unknown_architecture():
    """Unknown architecture raises ValueError with the registry listed."""
    from analytics.football2vec_adversary import AdversaryConfig, build_adversary

    cfg = AdversaryConfig()
    object.__setattr__(cfg, "architecture", "does_not_exist")
    with pytest.raises(ValueError, match="unknown architecture"):
        build_adversary(cfg, hidden_dim=192, num_competitions=22)


def test_stage2_loop_injection_backcompat():
    """Refactored _train_stage2_loop with defaults produces byte-equivalent training
    trajectory vs explicit injection of the matching adversary + linear schedule.

    Asserts: adversary_module=None + lambda_schedule_fn=None reproduces
    the pre-refactor production behavior at 1-epoch fidelity on a 10-sample fixture.
    """
    import sys

    import torch

    sys.path.insert(0, "scripts")
    try:
        from train_football2vec_v2 import _train_stage2_loop
    finally:
        sys.path.pop(0)

    from analytics.football2vec_adversary import LinearAdversaryHead
    from analytics.football2vec_transformer import (
        Football2VecConfig,
        Football2VecEncoder,
    )
    from ingestion.football2vec_v2_training import (
        ADVERSARIAL_LAMBDA_MAX,
        ADVERSARIAL_WARMUP_EPOCHS,
        VOCAB_SIZE,
        Football2VecDataset,
    )

    device = torch.device("cpu")
    hd, num_comp = 64, 4  # tiny config for speed
    cfg = Football2VecConfig(hidden_dim=hd, num_layers=2, num_heads=4, max_seq_len=16, spatial_mlp_dim=16)

    torch.manual_seed(42)
    action_ids = [[1, 2, 3, 4, 5] for _ in range(10)]
    x_coords = [[0.1, 0.2, 0.3, 0.4, 0.5] for _ in range(10)]
    y_coords = [[0.5, 0.4, 0.3, 0.2, 0.1] for _ in range(10)]
    competition_ids = [i % num_comp for i in range(10)]

    train_ds = Football2VecDataset(
        action_ids,
        x_coords,
        y_coords,
        max_seq_len=16,
        mask_prob=0.15,
        mlm=True,
        competition_ids=competition_ids,
    )
    val_ds = Football2VecDataset(
        action_ids,
        x_coords,
        y_coords,
        max_seq_len=16,
        mask_prob=0.15,
        mlm=True,
        competition_ids=competition_ids,
    )

    def _make_stage1_encoder() -> Football2VecEncoder:
        """Build a Football2VecEncoder with the token embedding expanded to include
        MASK+PAD tokens, matching how stage-1 training mutates the encoder before
        stage-2 receives it."""
        import torch.nn as nn

        enc = Football2VecEncoder(cfg)
        expanded = nn.Embedding(VOCAB_SIZE + 2, cfg.hidden_dim)
        with torch.no_grad():
            expanded.weight[:VOCAB_SIZE] = enc.token_embedding.weight
        enc.token_embedding = expanded
        return enc

    # Reference path: refactored loop with defaults (backward-compat).
    torch.manual_seed(42)
    encoder_ref = _make_stage1_encoder()
    _, _, hist_ref = _train_stage2_loop(
        encoder_ref,
        train_ds,
        val_ds,
        num_comp,
        cfg,
        device,
        epochs=1,
        batch_size=5,
        lr=1e-3,
        patience=3,
    )

    # Explicit-injection path: LinearAdversaryHead (the same module the refactored
    # default constructs internally) + production linear schedule function.
    torch.manual_seed(42)
    encoder_explicit = _make_stage1_encoder()
    adversary = LinearAdversaryHead(hidden_dim=hd, num_competitions=num_comp)

    def linear_schedule(epoch: int, total_epochs: int) -> float:
        del total_epochs
        return ADVERSARIAL_LAMBDA_MAX * min(epoch / ADVERSARIAL_WARMUP_EPOCHS, 1.0)

    _, _, hist_explicit = _train_stage2_loop(
        encoder_explicit,
        train_ds,
        val_ds,
        num_comp,
        cfg,
        device,
        epochs=1,
        batch_size=5,
        lr=1e-3,
        patience=3,
        adversary_module=adversary,
        lambda_schedule_fn=linear_schedule,
    )

    # Trajectories must match byte-for-byte (deterministic seed + same structure).
    for key in ("train_mlm_loss", "train_adv_loss", "val_mlm_loss", "val_adv_accuracy", "lambda_val"):
        for i, (a, b) in enumerate(zip(hist_ref[key], hist_explicit[key], strict=True)):
            assert a == pytest.approx(b, abs=1e-6), f"{key}[{i}]: ref={a} vs explicit={b}"


def test_football2vec_adversary_public_surface():
    """football2vec_adversary public API surface is stable — guard against
    inadvertent refactors. Catches broken re-exports and accidental renames."""
    from analytics import football2vec_adversary as mod

    for name in (
        "AdversaryConfig",
        "LinearAdversaryHead",
        "build_adversary",
        "lambda_schedule",
        "_ADVERSARY_REGISTRY",
    ):
        assert hasattr(mod, name), f"missing {name} in public API"

    # Registry always has at least the baseline.
    assert "linear" in mod._ADVERSARY_REGISTRY
