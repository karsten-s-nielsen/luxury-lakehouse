"""Unit tests for ScoutGPTDecoder RoPE config option."""

from __future__ import annotations

import pytest
import torch

from analytics.rotary_attention import RotaryTransformerEncoder
from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder

# Captured from ScoutGPTDecoder() state_dict on 2026-04-22 (post cross_attention default flip).
# The player_cross_attn.* + player_cross_norm.* keys were added in wheel 0.3.10 when the default
# conditioning_type flipped from "additive" to "cross_attention". See docs/evolve/cross-attention-promote/SUMMARY.md.
# Regression guard: any parameter rename by accident breaks this set.
EXPECTED_KEYS_LEARNABLE_DEFAULT: frozenset[str] = frozenset(
    {
        "_causal_mask",
        "_pos_ids",
        "action_head.bias",
        "action_head.weight",
        "end_x_mlp.net.0.bias",
        "end_x_mlp.net.0.weight",
        "end_x_mlp.net.2.bias",
        "end_x_mlp.net.2.weight",
        "end_y_mlp.net.0.bias",
        "end_y_mlp.net.0.weight",
        "end_y_mlp.net.2.bias",
        "end_y_mlp.net.2.weight",
        "player_cross_attn.in_proj_bias",
        "player_cross_attn.in_proj_weight",
        "player_cross_attn.out_proj.bias",
        "player_cross_attn.out_proj.weight",
        "player_cross_norm.bias",
        "player_cross_norm.weight",
        "player_embedding.weight",
        "position_embedding.weight",
        "result_embedding.weight",
        "start_x_mlp.net.0.bias",
        "start_x_mlp.net.0.weight",
        "start_x_mlp.net.2.bias",
        "start_x_mlp.net.2.weight",
        "start_y_mlp.net.0.bias",
        "start_y_mlp.net.0.weight",
        "start_y_mlp.net.2.bias",
        "start_y_mlp.net.2.weight",
        "time_delta_mlp.net.0.bias",
        "time_delta_mlp.net.0.weight",
        "time_delta_mlp.net.2.bias",
        "time_delta_mlp.net.2.weight",
        "token_embedding.weight",
        "transformer.layers.0.linear1.bias",
        "transformer.layers.0.linear1.weight",
        "transformer.layers.0.linear2.bias",
        "transformer.layers.0.linear2.weight",
        "transformer.layers.0.norm1.bias",
        "transformer.layers.0.norm1.weight",
        "transformer.layers.0.norm2.bias",
        "transformer.layers.0.norm2.weight",
        "transformer.layers.0.self_attn.in_proj_bias",
        "transformer.layers.0.self_attn.in_proj_weight",
        "transformer.layers.0.self_attn.out_proj.bias",
        "transformer.layers.0.self_attn.out_proj.weight",
        "transformer.layers.1.linear1.bias",
        "transformer.layers.1.linear1.weight",
        "transformer.layers.1.linear2.bias",
        "transformer.layers.1.linear2.weight",
        "transformer.layers.1.norm1.bias",
        "transformer.layers.1.norm1.weight",
        "transformer.layers.1.norm2.bias",
        "transformer.layers.1.norm2.weight",
        "transformer.layers.1.self_attn.in_proj_bias",
        "transformer.layers.1.self_attn.in_proj_weight",
        "transformer.layers.1.self_attn.out_proj.bias",
        "transformer.layers.1.self_attn.out_proj.weight",
        "transformer.layers.2.linear1.bias",
        "transformer.layers.2.linear1.weight",
        "transformer.layers.2.linear2.bias",
        "transformer.layers.2.linear2.weight",
        "transformer.layers.2.norm1.bias",
        "transformer.layers.2.norm1.weight",
        "transformer.layers.2.norm2.bias",
        "transformer.layers.2.norm2.weight",
        "transformer.layers.2.self_attn.in_proj_bias",
        "transformer.layers.2.self_attn.in_proj_weight",
        "transformer.layers.2.self_attn.out_proj.bias",
        "transformer.layers.2.self_attn.out_proj.weight",
        "transformer.layers.3.linear1.bias",
        "transformer.layers.3.linear1.weight",
        "transformer.layers.3.linear2.bias",
        "transformer.layers.3.linear2.weight",
        "transformer.layers.3.norm1.bias",
        "transformer.layers.3.norm1.weight",
        "transformer.layers.3.norm2.bias",
        "transformer.layers.3.norm2.weight",
        "transformer.layers.3.self_attn.in_proj_bias",
        "transformer.layers.3.self_attn.in_proj_weight",
        "transformer.layers.3.self_attn.out_proj.bias",
        "transformer.layers.3.self_attn.out_proj.weight",
        "transformer.layers.4.linear1.bias",
        "transformer.layers.4.linear1.weight",
        "transformer.layers.4.linear2.bias",
        "transformer.layers.4.linear2.weight",
        "transformer.layers.4.norm1.bias",
        "transformer.layers.4.norm1.weight",
        "transformer.layers.4.norm2.bias",
        "transformer.layers.4.norm2.weight",
        "transformer.layers.4.self_attn.in_proj_bias",
        "transformer.layers.4.self_attn.in_proj_weight",
        "transformer.layers.4.self_attn.out_proj.bias",
        "transformer.layers.4.self_attn.out_proj.weight",
        "transformer.layers.5.linear1.bias",
        "transformer.layers.5.linear1.weight",
        "transformer.layers.5.linear2.bias",
        "transformer.layers.5.linear2.weight",
        "transformer.layers.5.norm1.bias",
        "transformer.layers.5.norm1.weight",
        "transformer.layers.5.norm2.bias",
        "transformer.layers.5.norm2.weight",
        "transformer.layers.5.self_attn.in_proj_bias",
        "transformer.layers.5.self_attn.in_proj_weight",
        "transformer.layers.5.self_attn.out_proj.bias",
        "transformer.layers.5.self_attn.out_proj.weight",
        "vaep_head.bias",
        "vaep_head.weight",
    }
)


def _small_config(position_embedding: str = "learnable") -> ScoutGPTConfig:
    """Tiny config — fast construction, runs on CPU.

    Explicitly pins ``conditioning_type="additive"`` (not the cross_attention default from
    wheel 0.3.10) because these tests validate RoPE's position-embedding invariants, which
    are orthogonal to player conditioning. cross_attention conditioning does not currently
    receive ``attention_mask``, so padding leaks through the K/V over player_emb — that is
    a known limitation of cross_attention conditioning, filed as a follow-up after the
    default flip cycle (2026-04-22). Isolating these tests to additive conditioning keeps
    the RoPE assertion clean and unconfounded.
    """
    return ScoutGPTConfig(
        vocab_size=23,
        hidden_dim=32,
        num_layers=2,
        num_heads=4,
        dropout=0.0,
        max_seq_len=16,
        num_players=50,
        spatial_mlp_dim=8,
        position_embedding=position_embedding,
        conditioning_type="additive",
    )


def _dummy_batch(batch: int = 2, seq_len: int = 8) -> dict[str, torch.Tensor]:
    """Build a valid dummy batch at the shape ScoutGPTDecoder expects."""
    return {
        "action_ids": torch.randint(0, 23, (batch, seq_len)),
        "start_x": torch.rand(batch, seq_len),
        "start_y": torch.rand(batch, seq_len),
        "end_x": torch.rand(batch, seq_len),
        "end_y": torch.rand(batch, seq_len),
        "result": torch.randint(0, 2, (batch, seq_len)),
        "time_delta": torch.rand(batch, seq_len),
        "player_ids": torch.randint(0, 50, (batch, seq_len)),
        "attention_mask": torch.ones(batch, seq_len, dtype=torch.bool),
    }


def test_learnable_default_unchanged() -> None:
    """Default config still builds the stdlib transformer stack + learned pos emb."""
    import torch.nn as nn

    m = ScoutGPTDecoder()
    assert isinstance(m.transformer, nn.TransformerEncoder)
    assert hasattr(m, "position_embedding")
    assert isinstance(m.position_embedding, nn.Embedding)
    # _causal_mask and _pos_ids live as buffers on the module.
    assert "_causal_mask" in dict(m.named_buffers())
    assert "_pos_ids" in dict(m.named_buffers())


def test_state_dict_keys_stable_for_learnable() -> None:
    """Regression guard: accidental parameter renames break this test."""
    m = ScoutGPTDecoder()
    observed = frozenset(m.state_dict().keys())
    missing = EXPECTED_KEYS_LEARNABLE_DEFAULT - observed
    extra = observed - EXPECTED_KEYS_LEARNABLE_DEFAULT
    assert not missing, f"missing expected keys: {sorted(missing)}"
    assert not extra, f"unexpected new keys: {sorted(extra)}"


def test_unknown_position_embedding_raises() -> None:
    """position_embedding must be 'learnable' or 'rope'; anything else raises."""
    for bad in ("sinusoidal", "garbage", ""):
        with pytest.raises(ValueError, match="position_embedding"):
            ScoutGPTDecoder(_small_config(position_embedding=bad))


def test_rope_config_constructs() -> None:
    """RoPE variant builds RotaryTransformerEncoder and skips learned pos + causal mask."""
    m = ScoutGPTDecoder(_small_config(position_embedding="rope"))
    assert isinstance(m.transformer, RotaryTransformerEncoder)
    assert not hasattr(m, "position_embedding")
    buffers = dict(m.named_buffers())
    assert "_causal_mask" not in buffers
    assert "_pos_ids" not in buffers


def test_rope_forward_shape() -> None:
    """forward(...) returns (batch, hidden_dim); predict(...) returns expected shapes."""
    m = ScoutGPTDecoder(_small_config(position_embedding="rope"))
    m.eval()
    b = _dummy_batch(batch=2, seq_len=8)
    with torch.no_grad():
        pooled = m(**b)
        action_logits, vaep_preds = m.predict(**b)
    assert pooled.shape == (2, 32)
    assert action_logits.shape == (2, 8, 23)
    assert vaep_preds.shape == (2, 8, 1)


def test_rope_causal_property_preserved() -> None:
    """Perturbing token at position t must not change outputs at positions < t.

    The single most important correctness guard for swapping the causal mechanism
    from an explicit triu mask to is_causal=True in SDPA.
    """
    torch.manual_seed(0)
    m = ScoutGPTDecoder(_small_config(position_embedding="rope"))
    m.eval()
    b = _dummy_batch(batch=1, seq_len=8)
    perturb_pos = 5

    with torch.no_grad():
        orig_logits, _ = m.predict(**b)
        b_perturbed = dict(b)
        b_perturbed["action_ids"] = b["action_ids"].clone()
        # Flip the token at perturb_pos to a different valid action id.
        current = b["action_ids"][0, perturb_pos].item()
        b_perturbed["action_ids"][0, perturb_pos] = (current + 1) % 23
        perturbed_logits, _ = m.predict(**b_perturbed)

    # Positions 0..perturb_pos-1 must be bit-identical.
    for pos in range(perturb_pos):
        assert torch.equal(orig_logits[0, pos], perturbed_logits[0, pos]), (
            f"causal leak at position {pos} (< perturbed position {perturb_pos})"
        )
    # Sanity: the perturbed position itself must differ.
    assert not torch.equal(orig_logits[0, perturb_pos], perturbed_logits[0, perturb_pos])


def test_rope_padding_mask_preserved() -> None:
    """Scrambling padded positions must not change outputs at valid positions."""
    torch.manual_seed(0)
    m = ScoutGPTDecoder(_small_config(position_embedding="rope"))
    m.eval()
    batch, seq_len = 1, 8
    n_valid = 5  # positions 0..4 valid, 5..7 padded
    b = _dummy_batch(batch=batch, seq_len=seq_len)
    mask = torch.zeros(batch, seq_len, dtype=torch.bool)
    mask[:, :n_valid] = True
    b["attention_mask"] = mask

    with torch.no_grad():
        orig_logits, _ = m.predict(**b)

        # Scramble the padded positions — everything we can scramble.
        # Per-column vocab ranges must match _small_config: action_ids<23, result<2,
        # player_ids<50. Out-of-range indices would raise IndexError in nn.Embedding,
        # so the scramble is bounded by each embedding table's true vocab size.
        b_scrambled = {k: v.clone() if isinstance(v, torch.Tensor) else v for k, v in b.items()}
        b_scrambled["action_ids"][:, n_valid:] = torch.randint(0, 23, (batch, seq_len - n_valid))
        b_scrambled["result"][:, n_valid:] = torch.randint(0, 2, (batch, seq_len - n_valid))
        b_scrambled["player_ids"][:, n_valid:] = torch.randint(0, 50, (batch, seq_len - n_valid))
        for k in ("start_x", "start_y", "end_x", "end_y", "time_delta"):
            b_scrambled[k][:, n_valid:] = torch.rand(batch, seq_len - n_valid)
        scrambled_logits, _ = m.predict(**b_scrambled)

    # Outputs at valid positions (0..n_valid-1) must be bit-identical.
    for pos in range(n_valid):
        assert torch.equal(orig_logits[0, pos], scrambled_logits[0, pos]), (
            f"padding leak: position {pos} changed when positions >= {n_valid} were scrambled"
        )


def test_rope_backward_produces_finite_gradients() -> None:
    """loss.backward() runs and produces finite gradients on all trainable params."""
    torch.manual_seed(0)
    m = ScoutGPTDecoder(_small_config(position_embedding="rope"))
    m.train()
    b = _dummy_batch(batch=2, seq_len=8)
    action_logits, vaep_preds = m.predict(**b)
    labels = torch.randint(0, 23, (2, 8))
    loss = torch.nn.functional.cross_entropy(action_logits.reshape(-1, 23), labels.reshape(-1))
    loss = loss + vaep_preds.pow(2).mean() * 0.1
    loss.backward()
    for name, p in m.named_parameters():
        if p.requires_grad:
            assert p.grad is not None, f"{name}: no grad"
            assert torch.isfinite(p.grad).all(), f"{name}: non-finite grad"
