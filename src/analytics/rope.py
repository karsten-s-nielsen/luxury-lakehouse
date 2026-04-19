"""Rotary Position Embedding (RoPE) primitive — Su et al. 2021.

Reusable across every transformer in the codebase. The `apply_rotary_pos_emb`
function rotates Q/K tensors element-wise so that the dot product
<q_m, k_n> depends only on the relative position (m - n), not on absolute
positions m or n individually.

Convention: LLaMA-style first-half / second-half pairing. Each pair
(d_i, d_{i + d/2}) rotates together at frequency θ_i = base^(-2i/d). This is
mathematically equivalent to Su et al.'s original interleaved (2i, 2i+1)
pairing but maps cleanly onto ``torch.chunk(2, dim=-1)`` and matches every
modern reference implementation (LLaMA, Mistral, HF Transformers).

References:
    Su, J. et al. (2021). "RoFormer: Enhanced Transformer with Rotary Position
        Embedding." arXiv:2104.09864.
"""

from __future__ import annotations

import torch
import torch.nn as nn


class RotaryEmbedding(nn.Module):
    """Precomputed (cos, sin) tables for rotary position embedding.

    Args:
        head_dim: Per-head dimension.
        max_seq_len: Maximum sequence length to precompute for.
        base: Base frequency. Default 10000.0 per Su et al. (2021).
    """

    def __init__(self, head_dim: int, max_seq_len: int, base: float = 10000.0) -> None:
        super().__init__()
        if head_dim % 2 != 0:
            msg = f"RotaryEmbedding requires head_dim to be even; got {head_dim}"
            raise ValueError(msg)
        self.head_dim = head_dim
        self.max_seq_len = max_seq_len

        inv_freq = 1.0 / (base ** (torch.arange(0, head_dim, 2, dtype=torch.float32) / head_dim))
        positions = torch.arange(max_seq_len, dtype=torch.float32)
        freqs = torch.outer(positions, inv_freq)
        emb = torch.cat([freqs, freqs], dim=-1)

        self.register_buffer("_cos", emb.cos(), persistent=False)
        self.register_buffer("_sin", emb.sin(), persistent=False)

    def forward(self, seq_len: int) -> tuple[torch.Tensor, torch.Tensor]:
        """Return ``(cos, sin)`` tables sliced to the current sequence length."""
        if seq_len > self.max_seq_len:
            msg = f"seq_len={seq_len} exceeds RotaryEmbedding.max_seq_len={self.max_seq_len}"
            raise ValueError(msg)
        return self._cos[:seq_len], self._sin[:seq_len]  # type: ignore[index]


def rotate_half(x: torch.Tensor) -> torch.Tensor:
    """LLaMA-style half-rotation: ``concat(-second_half, first_half)`` along last dim.

    Combined with the (cos, sin) tables from :class:`RotaryEmbedding`, this
    implements the per-pair rotation
    ``(a, b) → (a cos(mθ) - b sin(mθ), a sin(mθ) + b cos(mθ))``
    where the pair is ``(x_first[i], x_second[i])``.
    """
    x_first, x_second = x.chunk(2, dim=-1)
    return torch.cat([-x_second, x_first], dim=-1)


def apply_rotary_pos_emb(
    q: torch.Tensor,
    k: torch.Tensor,
    cos: torch.Tensor,
    sin: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Apply RoPE to Q and K tensors.

    The rotation is ``x_rot = x * cos + rotate_half(x) * sin``, applied per
    (first-half, second-half) pair at the frequency appropriate for each
    position.

    Args:
        q: Query tensor of shape ``(..., seq_len, head_dim)``. Typically
            ``(batch, num_heads, seq_len, head_dim)``.
        k: Key tensor of the same shape as ``q``.
        cos: Cosine table from :class:`RotaryEmbedding`, shape
            ``(seq_len, head_dim)``.
        sin: Sine table, same shape as ``cos``.

    Returns:
        ``(q_rot, k_rot)`` with rotation applied. Shapes unchanged.
    """
    # Broadcast (seq_len, head_dim) over leading batch/head dims of q/k.
    while cos.ndim < q.ndim:
        cos = cos.unsqueeze(0)
        sin = sin.unsqueeze(0)
    q_rot = q * cos + rotate_half(q) * sin
    k_rot = k * cos + rotate_half(k) * sin
    return q_rot, k_rot
