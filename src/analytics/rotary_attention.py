"""Rotary Position Embedding attention + transformer encoder layers.

Provides drop-in-style replacements for ``torch.nn.MultiheadAttention`` and
``torch.nn.TransformerEncoderLayer`` that rotate Q and K by RoPE before the
scaled-dot-product attention, rather than adding a positional embedding
to the input tokens.

Reusable across every transformer in the codebase. Built on top of
``torch.nn.functional.scaled_dot_product_attention`` so flash attention and
memory-efficient kernels are engaged automatically when available.

References:
    Su, J. et al. (2021). "RoFormer: Enhanced Transformer with Rotary Position
        Embedding." arXiv:2104.09864.
"""

from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F  # noqa: N812 — PyTorch convention

from analytics.rope import RotaryEmbedding, apply_rotary_pos_emb


class RotaryMultiheadAttention(nn.Module):
    """Multi-head self-attention with rotary position embedding applied to Q and K.

    The ``(cos, sin)`` rotation tables are supplied at forward time rather than
    stored internally — this lets the enclosing encoder share a single
    :class:`~analytics.rope.RotaryEmbedding` across all its layers.

    Args:
        d_model: Total embedding dimension. Must be divisible by ``num_heads``.
        num_heads: Number of attention heads.
        dropout: Attention dropout rate (active in training only).
        bias: Whether Q / K / V / output projections have bias. Default ``True``
            to match ``torch.nn.MultiheadAttention``.
    """

    def __init__(
        self,
        d_model: int,
        num_heads: int,
        dropout: float = 0.0,
        bias: bool = True,
    ) -> None:
        super().__init__()
        if d_model % num_heads != 0:
            msg = f"d_model={d_model} must be divisible by num_heads={num_heads}"
            raise ValueError(msg)
        self.d_model = d_model
        self.num_heads = num_heads
        self.head_dim = d_model // num_heads
        self.dropout = dropout

        self.q_proj = nn.Linear(d_model, d_model, bias=bias)
        self.k_proj = nn.Linear(d_model, d_model, bias=bias)
        self.v_proj = nn.Linear(d_model, d_model, bias=bias)
        self.out_proj = nn.Linear(d_model, d_model, bias=bias)

    def forward(
        self,
        x: torch.Tensor,
        cos: torch.Tensor,
        sin: torch.Tensor,
        key_padding_mask: torch.Tensor | None = None,
        is_causal: bool = False,
    ) -> torch.Tensor:
        """Self-attention forward with RoPE applied to Q and K.

        Args:
            x: Input tensor of shape ``(batch, seq_len, d_model)``.
            cos: Rotation table ``(seq_len, head_dim)`` from
                :class:`~analytics.rope.RotaryEmbedding`.
            sin: Rotation table, same shape as ``cos``.
            key_padding_mask: ``(batch, seq_len)`` bool tensor where ``True``
                marks positions to ignore (PyTorch convention).
            is_causal: If ``True``, apply causal (lower-triangular) mask.

        Returns:
            Tensor of shape ``(batch, seq_len, d_model)``.
        """
        batch, seq_len, _ = x.shape

        # Project and reshape to (batch, num_heads, seq_len, head_dim).
        q = self.q_proj(x).view(batch, seq_len, self.num_heads, self.head_dim).transpose(1, 2)
        k = self.k_proj(x).view(batch, seq_len, self.num_heads, self.head_dim).transpose(1, 2)
        v = self.v_proj(x).view(batch, seq_len, self.num_heads, self.head_dim).transpose(1, 2)

        q, k = apply_rotary_pos_emb(q, k, cos, sin)

        # Build additive attention mask from key_padding_mask.
        attn_mask: torch.Tensor | None = None
        if key_padding_mask is not None:
            # Convert (batch, seq_len) bool → (batch, 1, 1, seq_len) float with -inf at padded positions.
            attn_mask = torch.zeros(batch, 1, 1, seq_len, dtype=x.dtype, device=x.device)
            attn_mask = attn_mask.masked_fill(key_padding_mask.view(batch, 1, 1, seq_len), float("-inf"))

        attn_out = F.scaled_dot_product_attention(
            q,
            k,
            v,
            attn_mask=attn_mask,
            dropout_p=self.dropout if self.training else 0.0,
            is_causal=is_causal,
        )
        # (batch, num_heads, seq_len, head_dim) → (batch, seq_len, d_model)
        attn_out = attn_out.transpose(1, 2).contiguous().view(batch, seq_len, self.d_model)
        return self.out_proj(attn_out)


class RotaryTransformerEncoderLayer(nn.Module):
    """Drop-in encoder layer with RoPE self-attention, matching ``nn.TransformerEncoderLayer``.

    Behaviour mirrors PyTorch's reference layer for both the post-norm default
    (``norm_first=False``) and the pre-norm variant (``norm_first=True``).

    Args:
        d_model: Embedding dimension.
        nhead: Number of attention heads.
        dim_feedforward: Hidden dimension of the feed-forward block.
        dropout: Dropout rate applied after attention, inside FFN, and on the
            FFN output before the residual.
        activation: ``"gelu"`` or ``"relu"``.
        norm_first: If ``True`` use pre-norm (apply LayerNorm before attention
            / FFN then residual). Default ``False`` to match
            ``nn.TransformerEncoderLayer``.
        bias: Whether projections and FFN linears carry bias.
    """

    def __init__(
        self,
        d_model: int,
        nhead: int,
        dim_feedforward: int,
        dropout: float = 0.1,
        activation: str = "gelu",
        norm_first: bool = False,
        bias: bool = True,
    ) -> None:
        super().__init__()
        self.self_attn = RotaryMultiheadAttention(
            d_model=d_model,
            num_heads=nhead,
            dropout=dropout,
            bias=bias,
        )
        self.linear1 = nn.Linear(d_model, dim_feedforward, bias=bias)
        self.linear2 = nn.Linear(dim_feedforward, d_model, bias=bias)
        self.dropout_ffn = nn.Dropout(dropout)
        self.dropout1 = nn.Dropout(dropout)
        self.dropout2 = nn.Dropout(dropout)
        self.norm1 = nn.LayerNorm(d_model)
        self.norm2 = nn.LayerNorm(d_model)
        self.norm_first = norm_first

        if activation == "gelu":
            self.activation = F.gelu
        elif activation == "relu":
            self.activation = F.relu
        else:
            msg = f"unsupported activation {activation!r}; expected gelu|relu"
            raise ValueError(msg)

    def forward(
        self,
        src: torch.Tensor,
        cos: torch.Tensor,
        sin: torch.Tensor,
        src_key_padding_mask: torch.Tensor | None = None,
        is_causal: bool = False,
    ) -> torch.Tensor:
        """Encoder-layer forward: self-attention + residual, FFN + residual."""
        if self.norm_first:
            src = src + self._sa_block(self.norm1(src), cos, sin, src_key_padding_mask, is_causal)
            src = src + self._ff_block(self.norm2(src))
        else:
            src = self.norm1(src + self._sa_block(src, cos, sin, src_key_padding_mask, is_causal))
            src = self.norm2(src + self._ff_block(src))
        return src

    def _sa_block(
        self,
        x: torch.Tensor,
        cos: torch.Tensor,
        sin: torch.Tensor,
        key_padding_mask: torch.Tensor | None,
        is_causal: bool,
    ) -> torch.Tensor:
        attn = self.self_attn(x, cos, sin, key_padding_mask=key_padding_mask, is_causal=is_causal)
        return self.dropout1(attn)

    def _ff_block(self, x: torch.Tensor) -> torch.Tensor:
        return self.dropout2(self.linear2(self.dropout_ffn(self.activation(self.linear1(x)))))


class RotaryTransformerEncoder(nn.Module):
    """Stack of :class:`RotaryTransformerEncoderLayer` with a shared RoPE table.

    Owns a single :class:`~analytics.rope.RotaryEmbedding` and passes its
    ``(cos, sin)`` outputs into every layer on each forward pass. The
    ``forward`` signature mirrors ``nn.TransformerEncoder`` — it accepts
    ``(src, src_key_padding_mask=None, is_causal=False)`` — so it can swap
    in anywhere a standard encoder stack sits.

    Args:
        d_model: Embedding dimension.
        nhead: Number of attention heads per layer. ``d_model`` must be
            divisible by ``nhead``.
        dim_feedforward: Hidden dim of each layer's FFN.
        dropout: Dropout rate (attention + FFN + residual).
        activation: ``"gelu"`` or ``"relu"``.
        num_layers: Number of stacked layers.
        max_seq_len: Maximum supported sequence length (RoPE precomputation).
        norm_first: If ``True``, pre-norm; else post-norm (default).
        rope_base: Base for RoPE frequencies. Default 10000.0.
    """

    def __init__(
        self,
        d_model: int,
        nhead: int,
        dim_feedforward: int,
        dropout: float,
        activation: str,
        num_layers: int,
        max_seq_len: int,
        norm_first: bool = False,
        rope_base: float = 10000.0,
    ) -> None:
        super().__init__()
        head_dim = d_model // nhead
        self.rope = RotaryEmbedding(head_dim=head_dim, max_seq_len=max_seq_len, base=rope_base)
        self.layers = nn.ModuleList(
            [
                RotaryTransformerEncoderLayer(
                    d_model=d_model,
                    nhead=nhead,
                    dim_feedforward=dim_feedforward,
                    dropout=dropout,
                    activation=activation,
                    norm_first=norm_first,
                )
                for _ in range(num_layers)
            ]
        )

    def forward(
        self,
        src: torch.Tensor,
        src_key_padding_mask: torch.Tensor | None = None,
        is_causal: bool = False,
    ) -> torch.Tensor:
        """Run the stack, sharing ``(cos, sin)`` from the single RoPE table across layers."""
        cos, sin = self.rope(src.size(1))
        x = src
        for layer in self.layers:
            x = layer(x, cos, sin, src_key_padding_mask=src_key_padding_mask, is_causal=is_causal)
        return x
