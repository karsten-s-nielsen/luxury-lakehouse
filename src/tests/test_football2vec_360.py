"""Tests for 360-enriched Football2Vec encoder."""

from __future__ import annotations

import pytest

pytest.importorskip("torch")

import torch

from analytics.football2vec_360 import (
    Football2Vec360Config,
    Football2Vec360Encoder,
)


class TestFootball2Vec360Config:
    """Config inheritance and default values."""

    def test_config_extends_base(self) -> None:
        cfg = Football2Vec360Config()
        # Inherits Football2VecConfig defaults (promoted to EV1 iter-15 2026-04-19).
        assert cfg.hidden_dim == 192
        assert cfg.context_dim == 16
        assert cfg.vocab_size == 23

    def test_default_deep_sets_fields(self) -> None:
        cfg = Football2Vec360Config()
        assert cfg.deep_sets_hidden == 32
        assert cfg.player_feature_dim == 4
        assert cfg.use_pretrained_encoder is True

    def test_custom_context_dim(self) -> None:
        cfg = Football2Vec360Config(context_dim=32, deep_sets_hidden=64)
        assert cfg.context_dim == 32
        assert cfg.deep_sets_hidden == 64

    def test_frozen(self) -> None:
        cfg = Football2Vec360Config()
        with pytest.raises(AttributeError):
            cfg.context_dim = 32  # type: ignore[misc]


class TestFootball2Vec360Encoder:
    """Forward pass shape validation for 360-enriched encoder."""

    def test_output_dimension_is_hidden_plus_context(self) -> None:
        """Output dim is hidden_dim + context_dim (defaults to iter-15's 192 + 16 = 208)."""
        cfg = Football2Vec360Config()
        model = Football2Vec360Encoder(cfg)
        model.eval()
        batch, seq_len = 4, 10
        action_ids = torch.randint(0, 23, (batch, seq_len))
        x_coords = torch.rand(batch, seq_len)
        y_coords = torch.rand(batch, seq_len)
        mask = torch.ones(batch, seq_len, dtype=torch.bool)
        context_360 = torch.rand(batch, seq_len, cfg.context_dim)
        with torch.no_grad():
            out = model(action_ids, x_coords, y_coords, mask, context_360)
        assert out.shape == (batch, cfg.hidden_dim + cfg.context_dim)

    def test_output_without_360_uses_zeros(self) -> None:
        cfg = Football2Vec360Config()
        model = Football2Vec360Encoder(cfg)
        model.eval()
        batch, seq_len = 2, 5
        action_ids = torch.randint(0, 23, (batch, seq_len))
        x_coords = torch.rand(batch, seq_len)
        y_coords = torch.rand(batch, seq_len)
        mask = torch.ones(batch, seq_len, dtype=torch.bool)
        with torch.no_grad():
            out = model(action_ids, x_coords, y_coords, mask, context_360=None)
        assert out.shape == (batch, cfg.hidden_dim + cfg.context_dim)

    def test_raw_player_features_input(self) -> None:
        """Deep Sets branch accepts raw (batch, seq_len, max_players, 4) input."""
        cfg = Football2Vec360Config()
        model = Football2Vec360Encoder(cfg)
        model.eval()
        batch, seq_len, max_players = 2, 8, 22
        action_ids = torch.randint(0, 23, (batch, seq_len))
        x_coords = torch.rand(batch, seq_len)
        y_coords = torch.rand(batch, seq_len)
        mask = torch.ones(batch, seq_len, dtype=torch.bool)
        context_360 = torch.rand(batch, seq_len, max_players, cfg.player_feature_dim)
        with torch.no_grad():
            out = model(action_ids, x_coords, y_coords, mask, context_360)
        assert out.shape == (batch, cfg.hidden_dim + cfg.context_dim)

    def test_custom_dimensions(self) -> None:
        """Custom hidden_dim and context_dim produce correct output size."""
        cfg = Football2Vec360Config(
            hidden_dim=64,
            context_dim=32,
            num_layers=2,
            num_heads=2,
            deep_sets_hidden=48,
        )
        model = Football2Vec360Encoder(cfg)
        model.eval()
        batch, seq_len = 3, 12
        action_ids = torch.randint(0, 23, (batch, seq_len))
        x_coords = torch.rand(batch, seq_len)
        y_coords = torch.rand(batch, seq_len)
        mask = torch.ones(batch, seq_len, dtype=torch.bool)
        context_360 = torch.rand(batch, seq_len, 32)
        with torch.no_grad():
            out = model(action_ids, x_coords, y_coords, mask, context_360)
        assert out.shape == (batch, 64 + 32)

    def test_without_attention_mask(self) -> None:
        """Forward works when attention_mask is None."""
        cfg = Football2Vec360Config()
        model = Football2Vec360Encoder(cfg)
        model.eval()
        batch, seq_len = 2, 6
        action_ids = torch.randint(0, 23, (batch, seq_len))
        x_coords = torch.rand(batch, seq_len)
        y_coords = torch.rand(batch, seq_len)
        context_360 = torch.rand(batch, seq_len, cfg.context_dim)
        with torch.no_grad():
            out = model(action_ids, x_coords, y_coords, None, context_360)
        assert out.shape == (batch, cfg.hidden_dim + cfg.context_dim)

    def test_context_360_affects_output(self) -> None:
        """Non-zero 360 context produces different embedding than zeros."""
        cfg = Football2Vec360Config()
        model = Football2Vec360Encoder(cfg)
        model.eval()
        batch, seq_len = 2, 10
        gen = torch.Generator().manual_seed(42)
        action_ids = torch.randint(0, 23, (batch, seq_len), generator=gen)
        x_coords = torch.rand(batch, seq_len, generator=gen)
        y_coords = torch.rand(batch, seq_len, generator=gen)
        mask = torch.ones(batch, seq_len, dtype=torch.bool)
        context_360 = torch.rand(batch, seq_len, 16, generator=gen)
        with torch.no_grad():
            out_with = model(action_ids, x_coords, y_coords, mask, context_360)
            out_without = model(action_ids, x_coords, y_coords, mask, None)
        # The 360 context branch should make the outputs differ
        assert not torch.allclose(out_with, out_without, atol=1e-6)
