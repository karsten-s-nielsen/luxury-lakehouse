"""Tests for Football2Vec v2 transformer encoder, GRL, and team classifier."""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("torch")

import torch

from analytics.football2vec_transformer import (
    Football2VecConfig,
    Football2VecEncoder,
    GradientReversalLayer,
    TeamClassifierHead,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_batch(
    batch_size: int = 4,
    seq_len: int = 50,
    vocab_size: int = 23,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    """Create a synthetic batch of SPADL action sequences.

    Returns:
        (action_ids, x_coords, y_coords, attention_mask) tensors.
    """
    gen = torch.Generator().manual_seed(42)
    action_ids = torch.randint(0, vocab_size, (batch_size, seq_len), generator=gen)
    x_coords = torch.rand(batch_size, seq_len, generator=gen)
    y_coords = torch.rand(batch_size, seq_len, generator=gen)
    attention_mask = torch.ones(batch_size, seq_len, dtype=torch.bool)
    return action_ids, x_coords, y_coords, attention_mask


# ---------------------------------------------------------------------------
# Football2VecConfig
# ---------------------------------------------------------------------------


class TestFootball2VecConfig:
    """Frozen transformer config defaults and custom overrides."""

    def test_default_config(self) -> None:
        cfg = Football2VecConfig()
        assert cfg.vocab_size == 23
        assert cfg.hidden_dim == 128
        assert cfg.num_layers == 4
        assert cfg.num_heads == 4
        assert cfg.dropout == 0.1
        assert cfg.max_seq_len == 512
        assert cfg.mask_prob == 0.15
        assert cfg.spatial_mlp_dim == 64

    def test_custom_config(self) -> None:
        cfg = Football2VecConfig(
            vocab_size=30,
            hidden_dim=64,
            num_layers=2,
            num_heads=2,
            dropout=0.2,
            max_seq_len=256,
            mask_prob=0.20,
            spatial_mlp_dim=32,
        )
        assert cfg.vocab_size == 30
        assert cfg.hidden_dim == 64
        assert cfg.num_layers == 2
        assert cfg.num_heads == 2
        assert cfg.dropout == 0.2
        assert cfg.max_seq_len == 256
        assert cfg.mask_prob == 0.20
        assert cfg.spatial_mlp_dim == 32

    def test_frozen(self) -> None:
        cfg = Football2VecConfig()
        with pytest.raises(AttributeError):
            cfg.hidden_dim = 256  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Football2VecEncoder
# ---------------------------------------------------------------------------


class TestFootball2VecEncoder:
    """Transformer encoder forward pass and MLM head shape validation."""

    def test_forward_pass_shape(self) -> None:
        """forward() produces (batch=4, hidden_dim=128) via mean pooling."""
        config = Football2VecConfig()
        model = Football2VecEncoder(config)
        model.eval()

        action_ids, x_coords, y_coords, attention_mask = _make_batch()
        with torch.no_grad():
            output = model(action_ids, x_coords, y_coords, attention_mask)

        assert output.shape == (4, 128)

    def test_mlm_head_shape(self) -> None:
        """mlm_forward() produces (batch=4, seq_len=50, vocab_size=23) logits."""
        config = Football2VecConfig()
        model = Football2VecEncoder(config)
        model.eval()

        action_ids, x_coords, y_coords, attention_mask = _make_batch()
        with torch.no_grad():
            logits = model.mlm_forward(action_ids, x_coords, y_coords, attention_mask)

        assert logits.shape == (4, 50, 23)

    def test_spatial_encoding_contributes(self) -> None:
        """Same action_ids but different positions produce different embeddings."""
        config = Football2VecConfig()
        model = Football2VecEncoder(config)
        model.eval()

        gen = torch.Generator().manual_seed(42)
        action_ids = torch.randint(0, 23, (2, 30), generator=gen)
        # Duplicate action_ids for both samples
        action_ids = action_ids[:1].expand(2, -1).clone()

        # Different spatial coordinates
        x_a = torch.zeros(1, 30)
        y_a = torch.zeros(1, 30)
        x_b = torch.ones(1, 30)
        y_b = torch.ones(1, 30)

        x_coords = torch.cat([x_a, x_b], dim=0)
        y_coords = torch.cat([y_a, y_b], dim=0)

        with torch.no_grad():
            output = model(action_ids, x_coords, y_coords)

        emb_a = output[0]
        emb_b = output[1]
        # Embeddings should differ because spatial positions differ
        assert not torch.allclose(emb_a, emb_b, atol=1e-6)

    def test_forward_without_attention_mask(self) -> None:
        """forward() works when attention_mask is None (all tokens valid)."""
        config = Football2VecConfig()
        model = Football2VecEncoder(config)
        model.eval()

        action_ids, x_coords, y_coords, _ = _make_batch()
        with torch.no_grad():
            output = model(action_ids, x_coords, y_coords, attention_mask=None)

        assert output.shape == (4, 128)

    def test_attention_mask_affects_output(self) -> None:
        """Masking out tokens changes the mean-pooled embedding."""
        config = Football2VecConfig()
        model = Football2VecEncoder(config)
        model.eval()

        action_ids, x_coords, y_coords, full_mask = _make_batch(batch_size=1, seq_len=20)

        # Partial mask: only first 10 tokens valid
        partial_mask = torch.zeros(1, 20, dtype=torch.bool)
        partial_mask[0, :10] = True

        with torch.no_grad():
            out_full = model(action_ids, x_coords, y_coords, full_mask)
            out_partial = model(action_ids, x_coords, y_coords, partial_mask)

        assert not torch.allclose(out_full, out_partial, atol=1e-6)

    def test_default_config_when_none(self) -> None:
        """Passing config=None uses default Football2VecConfig."""
        model = Football2VecEncoder(config=None)
        assert model.config.hidden_dim == 128
        assert model.config.vocab_size == 23

    def test_custom_config_dimensions(self) -> None:
        """Custom config propagates to model dimensions."""
        config = Football2VecConfig(hidden_dim=64, vocab_size=30, num_layers=2, num_heads=2)
        model = Football2VecEncoder(config)
        model.eval()

        gen = torch.Generator().manual_seed(42)
        action_ids = torch.randint(0, 30, (2, 20), generator=gen)
        x_coords = torch.rand(2, 20, generator=gen)
        y_coords = torch.rand(2, 20, generator=gen)

        with torch.no_grad():
            output = model(action_ids, x_coords, y_coords)

        assert output.shape == (2, 64)

    def test_mlm_forward_without_mask(self) -> None:
        """mlm_forward() works when attention_mask is None."""
        config = Football2VecConfig()
        model = Football2VecEncoder(config)
        model.eval()

        action_ids, x_coords, y_coords, _ = _make_batch(seq_len=30)
        with torch.no_grad():
            logits = model.mlm_forward(action_ids, x_coords, y_coords, attention_mask=None)

        assert logits.shape == (4, 30, 23)


# ---------------------------------------------------------------------------
# GradientReversalLayer
# ---------------------------------------------------------------------------


class TestGradientReversalLayer:
    """Forward identity and backward gradient negation."""

    def test_forward_identity(self) -> None:
        """Forward pass is identity (output equals input)."""
        grl = GradientReversalLayer(lambda_val=0.5)
        x = torch.tensor([1.0, 2.0, 3.0])
        y = grl(x)
        assert torch.allclose(y, x)

    def test_backward_negated(self) -> None:
        """Backward pass negates gradient scaled by lambda.

        With lambda=1.0 and upstream grad = ones, the gradient through
        the GRL should be -1.0 * ones.
        """
        grl = GradientReversalLayer(lambda_val=1.0)
        x = torch.ones(4, requires_grad=True)
        y = grl(x)
        loss = y.sum()
        loss.backward()
        assert x.grad is not None
        expected = -1.0 * torch.ones(4)
        assert torch.allclose(x.grad, expected)

    def test_backward_scaled(self) -> None:
        """Backward pass scales negated gradient by lambda_val."""
        grl = GradientReversalLayer(lambda_val=0.5)
        x = torch.ones(3, requires_grad=True)
        y = grl(x)
        loss = y.sum()
        loss.backward()
        assert x.grad is not None
        expected = -0.5 * torch.ones(3)
        assert torch.allclose(x.grad, expected)

    def test_default_lambda(self) -> None:
        """Default lambda_val is 0.2."""
        grl = GradientReversalLayer()
        assert grl.lambda_val == 0.2


# ---------------------------------------------------------------------------
# TeamClassifierHead
# ---------------------------------------------------------------------------


class TestTeamClassifierHead:
    """Team classifier with gradient reversal output shape."""

    def test_output_shape(self) -> None:
        """(batch=4, hidden_dim=128) → (batch=4, num_teams=50)."""
        head = TeamClassifierHead(hidden_dim=128, num_teams=50)
        x = torch.randn(4, 128)
        with torch.no_grad():
            logits = head(x)
        assert logits.shape == (4, 50)

    def test_gradient_flows_reversed(self) -> None:
        """Gradient from classifier loss is reversed through GRL."""
        head = TeamClassifierHead(hidden_dim=16, num_teams=5, lambda_val=1.0)
        x = torch.randn(2, 16, requires_grad=True)
        logits = head(x)
        loss = logits.sum()
        loss.backward()

        assert x.grad is not None
        # Verify gradient is non-zero (GRL doesn't zero it, it negates it)
        assert x.grad.abs().sum() > 0

    def test_custom_lambda(self) -> None:
        """Custom lambda_val propagates to GRL."""
        head = TeamClassifierHead(hidden_dim=64, num_teams=20, lambda_val=0.7)
        assert head.grl.lambda_val == 0.7


# ---------------------------------------------------------------------------
# Architectural enum variants (EV1)
# ---------------------------------------------------------------------------


def _dummy_batch(batch_size: int = 2, seq_len: int = 16) -> dict[str, torch.Tensor]:
    """Build a dummy batch for forward-pass testing."""
    return {
        "action_ids": torch.randint(0, 23, (batch_size, seq_len)),
        "x_coords": torch.rand(batch_size, seq_len),
        "y_coords": torch.rand(batch_size, seq_len),
        "attention_mask": torch.ones(batch_size, seq_len, dtype=torch.bool),
    }


@pytest.mark.parametrize("pooling_type", ["mean", "attention", "cls"])
def test_football2vec_encoder_pooling_variants(pooling_type: str) -> None:
    """Encoder forward pass returns (batch, hidden_dim) for every pooling variant."""
    cfg = Football2VecConfig(hidden_dim=32, num_layers=1, num_heads=4, max_seq_len=64, pooling_type=pooling_type)
    model = Football2VecEncoder(cfg)
    model.eval()
    batch = _dummy_batch()
    with torch.no_grad():
        out = model(batch["action_ids"], batch["x_coords"], batch["y_coords"], batch["attention_mask"])
    assert out.shape == (2, 32), f"pooling_type={pooling_type!r} produced shape {out.shape}"


@pytest.mark.parametrize("spatial_injection", ["additive", "concat", "film"])
def test_football2vec_encoder_spatial_variants(spatial_injection: str) -> None:
    """Encoder forward pass works for every spatial_injection variant."""
    cfg = Football2VecConfig(
        hidden_dim=32,
        num_layers=1,
        num_heads=4,
        max_seq_len=64,
        spatial_mlp_dim=8,
        spatial_injection=spatial_injection,
    )
    model = Football2VecEncoder(cfg)
    model.eval()
    batch = _dummy_batch()
    with torch.no_grad():
        out = model(batch["action_ids"], batch["x_coords"], batch["y_coords"], batch["attention_mask"])
    assert out.shape == (2, 32), f"spatial_injection={spatial_injection!r} produced shape {out.shape}"


def test_football2vec_encoder_concat_guard() -> None:
    """concat injection rejects spatial_mlp_dim > hidden_dim/2 (memory guard)."""
    cfg = Football2VecConfig(
        hidden_dim=32,
        num_layers=1,
        num_heads=4,
        max_seq_len=64,
        spatial_mlp_dim=20,
        spatial_injection="concat",
    )
    with pytest.raises(ValueError, match="spatial_mlp_dim"):
        Football2VecEncoder(cfg)


@pytest.mark.parametrize("position_embedding", ["learnable", "sinusoidal", "rope"])
def test_football2vec_encoder_position_variants(position_embedding: str) -> None:
    """Encoder forward pass works for every position_embedding variant."""
    cfg = Football2VecConfig(
        hidden_dim=32,
        num_layers=1,
        num_heads=4,
        max_seq_len=64,
        position_embedding=position_embedding,
    )
    model = Football2VecEncoder(cfg)
    model.eval()
    batch = _dummy_batch()
    with torch.no_grad():
        out = model(batch["action_ids"], batch["x_coords"], batch["y_coords"], batch["attention_mask"])
    assert out.shape == (2, 32), f"position_embedding={position_embedding!r} produced shape {out.shape}"


def test_football2vec_encoder_backward_compat() -> None:
    """Default Football2VecConfig() produces the same module structure as before EV1."""
    cfg = Football2VecConfig()
    model = Football2VecEncoder(cfg)

    assert cfg.pooling_type == "mean"
    assert cfg.spatial_injection == "additive"
    assert cfg.position_embedding == "learnable"

    expected_modules = {
        "token_embedding",
        "spatial_x",
        "spatial_y",
        "position_embedding",
        "embedding_dropout",
        "encoder",
        "mlm_head",
    }
    actual_modules = {name for name, _ in model.named_children()}
    missing = expected_modules - actual_modules
    assert not missing, f"backward-compat regression: missing modules {missing}"

    forbidden_modules = {"pool_attn", "spatial_concat_proj", "film_scale", "film_shift", "cls_token"}
    extra = forbidden_modules & actual_modules
    assert not extra, f"backward-compat regression: unexpected modules {extra}"

    # Also assert EV1-only buffers are absent (named_children excludes buffers).
    buffer_names = {name for name, _ in model.named_buffers()}
    forbidden_buffers = {"_sin_pos", "_rope_cos", "_rope_sin"}
    extra_buffers = forbidden_buffers & buffer_names
    assert not extra_buffers, f"backward-compat regression: unexpected buffers {extra_buffers}"

    batch = _dummy_batch()
    model.eval()
    with torch.no_grad():
        out = model(batch["action_ids"], batch["x_coords"], batch["y_coords"], batch["attention_mask"])
    assert out.shape == (2, 128)


@pytest.mark.parametrize(
    ("field", "bad_value", "valid_values"),
    [
        ("pooling_type", "max", "mean|attention|cls"),
        ("spatial_injection", "cross_attention", "additive|concat|film"),
        ("position_embedding", "alibi", "learnable|sinusoidal|rope"),
    ],
)
def test_football2vec_encoder_rejects_unknown_enum(field: str, bad_value: str, valid_values: str) -> None:
    """Unknown enum values raise ValueError at construction with an actionable message."""
    kwargs: dict[str, Any] = {
        "hidden_dim": 32,
        "num_layers": 1,
        "num_heads": 4,
        "max_seq_len": 64,
        field: bad_value,
    }
    cfg = Football2VecConfig(**kwargs)
    with pytest.raises(ValueError, match=valid_values):
        Football2VecEncoder(cfg)
