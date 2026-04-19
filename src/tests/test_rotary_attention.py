"""Tests for src/analytics/rotary_attention.py — RoPE-enabled attention + encoder.

Covers:
    - RotaryMultiheadAttention: shape parity, key-padding mask, causal mask.
    - RotaryTransformerEncoderLayer: shape parity (both norm_first modes).
    - RotaryTransformerEncoder: multi-layer stack, eval determinism,
      backward pass, key-padding mask propagation, interface parity with
      ``nn.TransformerEncoder`` (accepts ``src, src_key_padding_mask=...``).
"""

from __future__ import annotations

import pytest

pytest.importorskip("torch")

import torch

from analytics.rope import RotaryEmbedding
from analytics.rotary_attention import (
    RotaryMultiheadAttention,
    RotaryTransformerEncoder,
    RotaryTransformerEncoderLayer,
)


class TestRotaryMultiheadAttention:
    """Self-attention with rotary position embedding on Q/K."""

    def test_forward_preserves_shape(self) -> None:
        """(batch, seq_len, d_model) in → (batch, seq_len, d_model) out."""
        torch.manual_seed(0)
        d_model, num_heads, seq_len = 32, 4, 16
        attn = RotaryMultiheadAttention(d_model=d_model, num_heads=num_heads)
        attn.eval()
        rope = RotaryEmbedding(head_dim=d_model // num_heads, max_seq_len=seq_len)
        cos, sin = rope(seq_len=seq_len)
        x = torch.randn(2, seq_len, d_model)
        with torch.no_grad():
            out = attn(x, cos, sin)
        assert out.shape == (2, seq_len, d_model)

    def test_key_padding_mask_isolates_padded_positions(self) -> None:
        """Corrupting the input at masked positions must not change output at valid positions.

        This is the defining property of key_padding_mask — padded tokens
        should contribute zero attention weight to any query.
        """
        torch.manual_seed(1)
        d_model, num_heads, seq_len = 32, 4, 12
        attn = RotaryMultiheadAttention(d_model=d_model, num_heads=num_heads)
        attn.eval()
        rope = RotaryEmbedding(head_dim=d_model // num_heads, max_seq_len=seq_len)
        cos, sin = rope(seq_len=seq_len)

        x = torch.randn(1, seq_len, d_model)
        # Mask positions [8, seq_len) — True = ignore.
        key_padding_mask = torch.zeros(1, seq_len, dtype=torch.bool)
        key_padding_mask[0, 8:] = True

        x_corrupted = x.clone()
        x_corrupted[0, 8:, :] = 1e6  # massive values at masked positions
        with torch.no_grad():
            out_clean = attn(x, cos, sin, key_padding_mask=key_padding_mask)
            out_corrupt = attn(x_corrupted, cos, sin, key_padding_mask=key_padding_mask)
        # Output at unmasked query positions must be identical across clean and corrupt inputs.
        assert torch.allclose(out_clean[0, :8], out_corrupt[0, :8], atol=1e-5)

    def test_causal_mask_blocks_future_positions(self) -> None:
        """With is_causal=True, future tokens do not affect earlier-position outputs.

        Corrupt the input at positions ``>= k`` and verify output at positions
        ``< k`` is unchanged.
        """
        torch.manual_seed(2)
        d_model, num_heads, seq_len = 32, 4, 12
        attn = RotaryMultiheadAttention(d_model=d_model, num_heads=num_heads)
        attn.eval()
        rope = RotaryEmbedding(head_dim=d_model // num_heads, max_seq_len=seq_len)
        cos, sin = rope(seq_len=seq_len)
        x = torch.randn(1, seq_len, d_model)
        x_future_corrupt = x.clone()
        k = 6
        x_future_corrupt[0, k:, :] = 1e6
        with torch.no_grad():
            out_clean = attn(x, cos, sin, is_causal=True)
            out_corrupt = attn(x_future_corrupt, cos, sin, is_causal=True)
        # Positions [0, k) must not see the corruption at positions [k, seq_len).
        assert torch.allclose(out_clean[0, :k], out_corrupt[0, :k], atol=1e-5)


class TestRotaryTransformerEncoderLayer:
    """Single encoder layer: RoPE self-attention + FFN + LayerNorms + dropouts."""

    @pytest.mark.parametrize("norm_first", [False, True])
    def test_forward_preserves_shape(self, norm_first: bool) -> None:
        """(batch, seq_len, d_model) in → (batch, seq_len, d_model) out, both norm schedules."""
        torch.manual_seed(0)
        d_model, nhead, seq_len = 32, 4, 16
        layer = RotaryTransformerEncoderLayer(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=d_model * 4,
            dropout=0.0,
            activation="gelu",
            norm_first=norm_first,
        )
        layer.eval()
        rope = RotaryEmbedding(head_dim=d_model // nhead, max_seq_len=seq_len)
        cos, sin = rope(seq_len=seq_len)
        x = torch.randn(2, seq_len, d_model)
        with torch.no_grad():
            out = layer(x, cos, sin)
        assert out.shape == (2, seq_len, d_model)


class TestRotaryTransformerEncoder:
    """Multi-layer RoPE encoder stack with nn.TransformerEncoder-style forward API."""

    def test_stack_preserves_shape(self) -> None:
        """4-layer stack: (batch, seq_len, d_model) in → same shape out."""
        torch.manual_seed(0)
        d_model, nhead, num_layers, seq_len = 32, 4, 4, 16
        encoder = RotaryTransformerEncoder(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=d_model * 4,
            dropout=0.0,
            activation="gelu",
            num_layers=num_layers,
            max_seq_len=seq_len,
        )
        encoder.eval()
        x = torch.randn(2, seq_len, d_model)
        with torch.no_grad():
            out = encoder(x)
        assert out.shape == (2, seq_len, d_model)

    def test_eval_mode_is_deterministic(self) -> None:
        """Same input, two calls in eval mode → identical output (no dropout noise)."""
        torch.manual_seed(0)
        d_model, nhead, num_layers, seq_len = 32, 4, 3, 12
        encoder = RotaryTransformerEncoder(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=d_model * 4,
            dropout=0.3,  # nonzero; eval mode should still zero dropout
            activation="gelu",
            num_layers=num_layers,
            max_seq_len=seq_len,
        )
        encoder.eval()
        x = torch.randn(1, seq_len, d_model)
        with torch.no_grad():
            out1 = encoder(x)
            out2 = encoder(x)
        assert torch.equal(out1, out2)

    def test_src_key_padding_mask_propagates_to_layers(self) -> None:
        """Corrupting masked positions must not change output at unmasked positions."""
        torch.manual_seed(1)
        d_model, nhead, num_layers, seq_len = 32, 4, 2, 12
        encoder = RotaryTransformerEncoder(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=d_model * 4,
            dropout=0.0,
            activation="gelu",
            num_layers=num_layers,
            max_seq_len=seq_len,
        )
        encoder.eval()
        x = torch.randn(1, seq_len, d_model)
        mask = torch.zeros(1, seq_len, dtype=torch.bool)
        mask[0, 8:] = True
        x_corrupt = x.clone()
        x_corrupt[0, 8:, :] = 1e6
        with torch.no_grad():
            out_clean = encoder(x, src_key_padding_mask=mask)
            out_corrupt = encoder(x_corrupt, src_key_padding_mask=mask)
        # Unmasked positions must be identical across the two runs.
        assert torch.allclose(out_clean[0, :8], out_corrupt[0, :8], atol=1e-4)

    def test_backward_pass_produces_finite_grads(self) -> None:
        """Gradients flow through every learnable parameter; no NaN / inf."""
        torch.manual_seed(2)
        d_model, nhead, num_layers, seq_len = 32, 4, 2, 8
        encoder = RotaryTransformerEncoder(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=d_model * 4,
            dropout=0.0,
            activation="gelu",
            num_layers=num_layers,
            max_seq_len=seq_len,
        )
        x = torch.randn(2, seq_len, d_model, requires_grad=True)
        out = encoder(x)
        out.sum().backward()
        assert x.grad is not None and torch.isfinite(x.grad).all()
        for name, p in encoder.named_parameters():
            assert p.grad is not None, f"{name} has no grad"
            assert torch.isfinite(p.grad).all(), f"{name} grad contains non-finite values"
