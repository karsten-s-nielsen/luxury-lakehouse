"""Tests for ScoutGPT conditioning mechanisms (additive, cross_attention, film, gated)."""

from __future__ import annotations

from typing import Any

import pytest

try:
    import torch

    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False

pytestmark = pytest.mark.skipif(not HAS_TORCH, reason="torch not installed")


def _make_config(**overrides: Any) -> Any:
    from analytics.scoutgpt_decoder import ScoutGPTConfig

    defaults: dict[str, Any] = {
        "hidden_dim": 32,
        "num_layers": 1,
        "num_heads": 2,
        "num_players": 100,
        "max_seq_len": 16,
        "spatial_mlp_dim": 16,
    }
    defaults.update(overrides)
    return ScoutGPTConfig(**defaults)


def _make_batch(batch_size: int = 2, seq_len: int = 8) -> dict[str, Any]:
    g = torch.Generator().manual_seed(42)
    return {
        "action_ids": torch.randint(0, 23, (batch_size, seq_len), generator=g),
        "start_x": torch.rand(batch_size, seq_len, generator=g),
        "start_y": torch.rand(batch_size, seq_len, generator=g),
        "end_x": torch.rand(batch_size, seq_len, generator=g),
        "end_y": torch.rand(batch_size, seq_len, generator=g),
        "result": torch.randint(0, 2, (batch_size, seq_len), generator=g),
        "time_delta": torch.rand(batch_size, seq_len, generator=g),
        "player_ids": torch.randint(0, 100, (batch_size, seq_len), generator=g),
        "attention_mask": torch.ones(batch_size, seq_len, dtype=torch.bool),
    }


@pytest.mark.parametrize("conditioning_type", ["additive", "cross_attention", "film", "gated"])
def test_predict_shape(conditioning_type: str) -> None:
    from analytics.scoutgpt_decoder import ScoutGPTDecoder

    config = _make_config(conditioning_type=conditioning_type)
    model = ScoutGPTDecoder(config)
    model.eval()
    batch = _make_batch()
    with torch.no_grad():
        logits, vaep = model.predict(**batch)
    assert logits.shape == (2, 8, 23)
    assert vaep.shape == (2, 8, 1)


@pytest.mark.parametrize("conditioning_type", ["additive", "cross_attention", "film", "gated"])
def test_forward_shape(conditioning_type: str) -> None:
    from analytics.scoutgpt_decoder import ScoutGPTDecoder

    config = _make_config(conditioning_type=conditioning_type)
    model = ScoutGPTDecoder(config)
    model.eval()
    batch = _make_batch()
    with torch.no_grad():
        emb = model(**batch)
    assert emb.shape == (2, 32)


@pytest.mark.parametrize("conditioning_type", ["cross_attention", "film", "gated"])
def test_player_swap_changes_output(conditioning_type: str) -> None:
    from analytics.scoutgpt_decoder import ScoutGPTDecoder

    config = _make_config(conditioning_type=conditioning_type)
    model = ScoutGPTDecoder(config)
    model.eval()
    batch = _make_batch()
    with torch.no_grad():
        logits_a, _ = model.predict(**batch)
        batch["player_ids"] = batch["player_ids"].clone()
        batch["player_ids"][:, 0] = 99
        logits_b, _ = model.predict(**batch)
    assert not torch.allclose(logits_a, logits_b, atol=1e-5)


def test_additive_is_default() -> None:
    from analytics.scoutgpt_decoder import ScoutGPTConfig

    config = ScoutGPTConfig()
    assert config.conditioning_type == "additive"


def test_unknown_conditioning_type_raises() -> None:
    from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder

    config = ScoutGPTConfig(conditioning_type="unknown_type")
    with pytest.raises(ValueError, match="Unknown conditioning_type"):
        ScoutGPTDecoder(config)
