"""Tests for ScoutGPT decoder architecture."""

from __future__ import annotations

import pytest

torch = pytest.importorskip("torch")

from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder  # noqa: E402


def _make_batch(
    batch_size: int = 4,
    seq_len: int = 30,
    vocab_size: int = 23,
    num_players: int = 100,
):
    """Create synthetic inputs for decoder testing."""
    g = torch.Generator().manual_seed(42)
    action_ids = torch.randint(0, vocab_size, (batch_size, seq_len), generator=g)
    start_x = torch.rand(batch_size, seq_len, generator=g)
    start_y = torch.rand(batch_size, seq_len, generator=g)
    end_x = torch.rand(batch_size, seq_len, generator=g)
    end_y = torch.rand(batch_size, seq_len, generator=g)
    result = torch.randint(0, 2, (batch_size, seq_len), generator=g)
    time_delta = torch.rand(batch_size, seq_len, generator=g) * 10.0
    player_ids = torch.randint(0, num_players, (batch_size, seq_len), generator=g)
    attention_mask = torch.ones(batch_size, seq_len, dtype=torch.bool)
    return action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids, attention_mask


class TestScoutGPTConfig:
    def test_default_config(self) -> None:
        cfg = ScoutGPTConfig()
        assert cfg.vocab_size == 23
        assert cfg.hidden_dim == 256
        assert cfg.num_layers == 6
        assert cfg.num_heads == 8
        assert cfg.dropout == 0.1
        assert cfg.max_seq_len == 128
        assert cfg.num_players == 11_918
        assert cfg.spatial_mlp_dim == 64
        assert cfg.vaep_loss_weight == 0.1

    def test_custom_config(self) -> None:
        cfg = ScoutGPTConfig(
            vocab_size=10,
            hidden_dim=64,
            num_layers=2,
            num_heads=4,
            dropout=0.2,
            max_seq_len=64,
            num_players=500,
            spatial_mlp_dim=32,
            vaep_loss_weight=0.05,
        )
        assert cfg.vocab_size == 10
        assert cfg.hidden_dim == 64
        assert cfg.num_layers == 2
        assert cfg.num_heads == 4
        assert cfg.dropout == 0.2
        assert cfg.max_seq_len == 64
        assert cfg.num_players == 500
        assert cfg.spatial_mlp_dim == 32
        assert cfg.vaep_loss_weight == 0.05

    def test_frozen(self) -> None:
        cfg = ScoutGPTConfig()
        with pytest.raises(AttributeError):
            cfg.hidden_dim = 64  # type: ignore[misc]


class TestScoutGPTDecoder:
    def test_forward_pass_shape(self) -> None:
        cfg = ScoutGPTConfig(num_players=100, hidden_dim=64, num_layers=2, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, mask = _make_batch(num_players=100)
        with torch.no_grad():
            out = model(action_ids, sx, sy, ex, ey, result, td, pids, mask)
        assert out.shape == (4, 64)

    def test_predict_shape(self) -> None:
        cfg = ScoutGPTConfig(num_players=100, hidden_dim=64, num_layers=2, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, mask = _make_batch(num_players=100)
        with torch.no_grad():
            action_logits, vaep_preds = model.predict(action_ids, sx, sy, ex, ey, result, td, pids, mask)
        assert action_logits.shape == (4, 30, 23)
        assert vaep_preds.shape == (4, 30, 1)

    def test_forward_without_attention_mask(self) -> None:
        cfg = ScoutGPTConfig(num_players=100, hidden_dim=64, num_layers=2, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, _ = _make_batch(num_players=100)
        with torch.no_grad():
            out = model(action_ids, sx, sy, ex, ey, result, td, pids)
        assert out.shape == (4, 64)

    def test_default_config_when_none(self) -> None:
        model = ScoutGPTDecoder(config=None)
        assert model.config.hidden_dim == 256
        assert model.config.num_players == 11_918

    def test_custom_config_dimensions(self) -> None:
        cfg = ScoutGPTConfig(num_players=50, hidden_dim=32, num_layers=1, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, mask = _make_batch(batch_size=2, seq_len=10, num_players=50)
        with torch.no_grad():
            out = model(action_ids, sx, sy, ex, ey, result, td, pids, mask)
        assert out.shape == (2, 32)

    def test_spatial_encoding_contributes(self) -> None:
        cfg = ScoutGPTConfig(num_players=100, hidden_dim=64, num_layers=2, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, mask = _make_batch(num_players=100)
        sx2 = torch.ones_like(sx) * 0.9
        sy2 = torch.ones_like(sy) * 0.1
        with torch.no_grad():
            out_a = model(action_ids, sx, sy, ex, ey, result, td, pids, mask)
            out_b = model(action_ids, sx2, sy2, ex, ey, result, td, pids, mask)
        assert not torch.allclose(out_a, out_b, atol=1e-6)

    def test_player_conditioning_contributes(self) -> None:
        cfg = ScoutGPTConfig(num_players=100, hidden_dim=64, num_layers=2, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, mask = _make_batch(num_players=100)
        pids2 = torch.zeros_like(pids)  # All player 0
        with torch.no_grad():
            out_a = model(action_ids, sx, sy, ex, ey, result, td, pids, mask)
            out_b = model(action_ids, sx, sy, ex, ey, result, td, pids2, mask)
        assert not torch.allclose(out_a, out_b, atol=1e-6)

    def test_attention_mask_affects_output(self) -> None:
        cfg = ScoutGPTConfig(num_players=100, hidden_dim=64, num_layers=2, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, mask = _make_batch(num_players=100)
        partial_mask = mask.clone()
        partial_mask[:, 15:] = False
        with torch.no_grad():
            out_full = model(action_ids, sx, sy, ex, ey, result, td, pids, mask)
            out_partial = model(action_ids, sx, sy, ex, ey, result, td, pids, partial_mask)
        assert not torch.allclose(out_full, out_partial, atol=1e-6)

    def test_predict_without_mask(self) -> None:
        cfg = ScoutGPTConfig(num_players=100, hidden_dim=64, num_layers=2, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, _ = _make_batch(num_players=100)
        with torch.no_grad():
            logits, vaep = model.predict(action_ids, sx, sy, ex, ey, result, td, pids)
        assert logits.shape == (4, 30, 23)
        assert vaep.shape == (4, 30, 1)
