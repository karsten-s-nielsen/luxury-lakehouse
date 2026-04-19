"""Encoder-only transformer for SPADL action sequence embeddings (Football2Vec v2).

Replaces Doc2Vec (Theiner et al. 2022) with a tiny transformer encoder
that learns contextual player embeddings via masked language modeling (MLM)
on SPADL action sequences. Spatial coordinates are injected through learned
MLP projections summed with token embeddings.

A gradient reversal layer (Ganin et al. 2016) enables domain-adversarial
training to remove team identity from embeddings, encouraging the model to
learn style-invariant representations.

Architecture:
    Token embedding (23-type SPADL vocab)
    + Spatial encoding (x MLP + y MLP)
    + Positional embedding (learnable)
    → TransformerEncoder (GELU, 4x FFN)
    → Mean pooling over valid tokens → (batch, hidden_dim)

References:
    Danesi, P. (2025). "Football2Vec: Transformer-Based Player Embeddings."
    Ganin, Y. et al. (2016). "Domain-Adversarial Training of Neural Networks." JMLR 17(1).
    Theiner, J. et al. (2022). "Extraction of Positional Player Data from Broadcast
        Soccer Videos." WACV.
    Decroos, T. et al. (2019). "Actions Speak Louder than Goals: Valuing Player
        Actions in Soccer." KDD.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import cast

import torch
import torch.nn as nn
from torch.autograd import Function

from analytics.rotary_attention import RotaryTransformerEncoder

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Football2VecConfig:
    """Immutable configuration for the Football2Vec transformer encoder.

    Attributes:
        vocab_size: SPADL 23-type action vocabulary size.
        hidden_dim: Embedding and transformer hidden dimension.
        num_layers: Number of transformer encoder layers.
        num_heads: Number of attention heads.
        dropout: Dropout rate.
        max_seq_len: Maximum sequence length for positional embedding.
        mask_prob: MLM mask probability.
        spatial_mlp_dim: Intermediate dimension for spatial coordinate MLPs.
        pooling_type: How to reduce per-token embeddings to a sequence embedding.
            "mean" (default — current behaviour), "attention" (learned attention pool),
            or "cls" (prepended CLS token).
        spatial_injection: How spatial coordinates are injected into the token stream.
            "additive" (default — tok + spatial_x + spatial_y + pos), "concat" (concat
            then project back), or "film" (per-channel scale + shift).
        position_embedding: Position encoding scheme. "learnable" (default —
            nn.Embedding), "sinusoidal" (fixed table), or "rope" (rotary).
    """

    # Defaults promoted to EV1 iter-15 on 2026-04-19 after HF Jobs L40S production
    # validation reproduced the local val_acc_15ep=0.5865 number
    # (reproduced=0.5850, +1.6 pp over the prior PR #124 baseline of 0.569).
    # See docs/evolve/ev1-football2vec/SUMMARY.md.
    vocab_size: int = 23
    hidden_dim: int = 192
    num_layers: int = 4
    num_heads: int = 6
    dropout: float = 0.1
    max_seq_len: int = 512
    mask_prob: float = 0.22
    spatial_mlp_dim: int = 64
    pooling_type: str = "cls"
    spatial_injection: str = "additive"
    position_embedding: str = "learnable"


# ---------------------------------------------------------------------------
# Spatial MLP
# ---------------------------------------------------------------------------


class SpatialMLP(nn.Module):
    """Project a scalar spatial coordinate to hidden_dim via a two-layer MLP.

    Takes normalized (x, y) scalars and maps them to the same dimensionality
    as the token embeddings so they can be summed element-wise.

    Args:
        hidden_dim: Output dimension (must match token embedding dim).
        intermediate_dim: Width of the intermediate layer.
    """

    def __init__(self, hidden_dim: int, intermediate_dim: int) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(1, intermediate_dim),
            nn.GELU(),
            nn.Linear(intermediate_dim, hidden_dim),
        )

    def forward(self, coords: torch.Tensor) -> torch.Tensor:
        """Project scalar coordinates to hidden_dim.

        Args:
            coords: (batch, seq_len) float tensor of normalized coordinates.

        Returns:
            (batch, seq_len, hidden_dim) spatial encoding.
        """
        return self.net(coords.unsqueeze(-1))


# ---------------------------------------------------------------------------
# Transformer Encoder
# ---------------------------------------------------------------------------


class Football2VecEncoder(nn.Module):
    """Encoder-only transformer for SPADL action sequence embeddings.

    Combines token embeddings, learned spatial encodings (x and y MLPs),
    and learnable positional embeddings. The encoder uses GELU activation
    and 4x feedforward expansion. Mean pooling over valid (non-masked) tokens
    produces a fixed-length sequence embedding.

    Args:
        config: Frozen configuration dataclass.
    """

    def __init__(self, config: Football2VecConfig | None = None) -> None:
        super().__init__()
        cfg = config or Football2VecConfig()
        self.config = cfg

        # Token embedding: 23-type SPADL vocabulary
        self.token_embedding = nn.Embedding(cfg.vocab_size, cfg.hidden_dim)

        # Spatial encoding: project normalized (x, y) scalars → hidden_dim
        self.spatial_x = SpatialMLP(cfg.hidden_dim, cfg.spatial_mlp_dim)
        self.spatial_y = SpatialMLP(cfg.hidden_dim, cfg.spatial_mlp_dim)

        # Position embedding variants (EV1). RoPE is applied inside attention
        # (see encoder construction below), so no additive position signal is
        # registered for the rope variant.
        if cfg.position_embedding == "learnable":
            self.position_embedding = nn.Embedding(cfg.max_seq_len, cfg.hidden_dim)
        elif cfg.position_embedding == "sinusoidal":
            # Fixed sinusoidal table; not a parameter.
            pe = torch.zeros(cfg.max_seq_len, cfg.hidden_dim)
            position = torch.arange(0, cfg.max_seq_len, dtype=torch.float).unsqueeze(1)
            div_term = torch.exp(
                torch.arange(0, cfg.hidden_dim, 2, dtype=torch.float) * (-math.log(10000.0) / cfg.hidden_dim)
            )
            pe[:, 0::2] = torch.sin(position * div_term)
            pe[:, 1::2] = torch.cos(position * div_term)
            self.register_buffer("_sin_pos", pe.unsqueeze(0))  # (1, max_seq_len, hidden_dim)
        elif cfg.position_embedding != "rope":
            msg = f"unknown position_embedding {cfg.position_embedding!r}; expected learnable|sinusoidal|rope"
            raise ValueError(msg)

        # Dropout after embedding sum
        self.embedding_dropout = nn.Dropout(cfg.dropout)

        # Spatial injection variants (EV1)
        if cfg.spatial_injection == "concat":
            if cfg.spatial_mlp_dim > cfg.hidden_dim // 2:
                msg = (
                    f"spatial_mlp_dim={cfg.spatial_mlp_dim} too large for concat "
                    f"injection (must be <= hidden_dim/2 = {cfg.hidden_dim // 2})"
                )
                raise ValueError(msg)
            self.spatial_concat_proj = nn.Linear(3 * cfg.hidden_dim, cfg.hidden_dim)
        elif cfg.spatial_injection == "film":
            self.film_scale = nn.Linear(2 * cfg.hidden_dim, cfg.hidden_dim)
            self.film_shift = nn.Linear(2 * cfg.hidden_dim, cfg.hidden_dim)
        elif cfg.spatial_injection != "additive":
            msg = f"unknown spatial_injection {cfg.spatial_injection!r}; expected additive|concat|film"
            raise ValueError(msg)

        # Transformer encoder. For the rope variant the encoder owns its own
        # RotaryEmbedding and applies rotation to Q/K inside scaled dot-product
        # attention — there is no additive position signal on the input tokens.
        self.encoder: nn.Module
        if cfg.position_embedding == "rope":
            # +1 so the rope tables cover the CLS-prepended sequence length.
            rope_max_seq_len = cfg.max_seq_len + (1 if cfg.pooling_type == "cls" else 0)
            self.encoder = RotaryTransformerEncoder(
                d_model=cfg.hidden_dim,
                nhead=cfg.num_heads,
                dim_feedforward=cfg.hidden_dim * 4,
                dropout=cfg.dropout,
                activation="gelu",
                num_layers=cfg.num_layers,
                max_seq_len=rope_max_seq_len,
            )
        else:
            encoder_layer = nn.TransformerEncoderLayer(
                d_model=cfg.hidden_dim,
                nhead=cfg.num_heads,
                dim_feedforward=cfg.hidden_dim * 4,
                dropout=cfg.dropout,
                activation="gelu",
                batch_first=True,
            )
            self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=cfg.num_layers)

        # Pooling variants (EV1)
        if cfg.pooling_type == "attention":
            self.pool_attn = nn.Linear(cfg.hidden_dim, 1)
        elif cfg.pooling_type == "cls":
            self.cls_token = nn.Parameter(torch.zeros(1, 1, cfg.hidden_dim))
            nn.init.normal_(self.cls_token, std=0.02)
        elif cfg.pooling_type != "mean":
            msg = f"unknown pooling_type {cfg.pooling_type!r}; expected mean|attention|cls"
            raise ValueError(msg)

        # MLM head: Linear → GELU → LayerNorm → Linear(hidden_dim, vocab_size)
        self.mlm_head = nn.Sequential(
            nn.Linear(cfg.hidden_dim, cfg.hidden_dim),
            nn.GELU(),
            nn.LayerNorm(cfg.hidden_dim),
            nn.Linear(cfg.hidden_dim, cfg.vocab_size),
        )

        # Pre-computed positional indices (avoids torch.arange allocation per forward pass)
        self.register_buffer("_pos_ids", torch.arange(cfg.max_seq_len).unsqueeze(0))

        # Initialize weights
        self.apply(self._init_weights)

    @staticmethod
    def _init_weights(module: nn.Module) -> None:
        """Xavier uniform initialization for linear layers and embeddings."""
        if isinstance(module, nn.Linear):
            nn.init.xavier_uniform_(module.weight)
            if module.bias is not None:
                nn.init.zeros_(module.bias)
        elif isinstance(module, nn.Embedding):
            nn.init.xavier_uniform_(module.weight)
        elif isinstance(module, nn.LayerNorm):
            nn.init.ones_(module.weight)
            nn.init.zeros_(module.bias)

    def _embed(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
    ) -> torch.Tensor:
        """Compute combined embedding per the configured spatial_injection + position_embedding strategy."""
        seq_len = action_ids.size(1)

        tok_emb = self.token_embedding(action_ids)
        x_emb = self.spatial_x(x_coords)
        y_emb = self.spatial_y(y_coords)

        # Position embedding (variant-dependent). For rope, no additive signal is
        # added here — rotation is applied to Q/K inside RotaryTransformerEncoder.
        pos_emb: torch.Tensor | None
        if self.config.position_embedding == "learnable":
            pos_emb = self.position_embedding(self._pos_ids[:, :seq_len])  # type: ignore[index]
        elif self.config.position_embedding == "sinusoidal":
            pos_emb = self._sin_pos[:, :seq_len, :]  # type: ignore[index]
        else:  # rope
            pos_emb = None

        # Spatial injection (variant-dependent)
        if self.config.spatial_injection == "concat":
            stacked = torch.cat([tok_emb, x_emb, y_emb], dim=-1)
            combined = self.spatial_concat_proj(stacked)
        elif self.config.spatial_injection == "film":
            spatial = torch.cat([x_emb, y_emb], dim=-1)
            scale = self.film_scale(spatial)
            shift = self.film_shift(spatial)
            combined = tok_emb * (1.0 + scale) + shift
        else:  # additive — default
            combined = tok_emb + x_emb + y_emb

        if pos_emb is not None:
            combined = combined + pos_emb

        return self.embedding_dropout(combined)

    def _encode(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Run the transformer encoder on embedded inputs."""
        embedded = self._embed(action_ids, x_coords, y_coords)

        # Prepend CLS token for cls pooling variant
        if self.config.pooling_type == "cls":
            batch_size = embedded.size(0)
            cls = self.cls_token.expand(batch_size, -1, -1)  # (batch, 1, hidden_dim)
            embedded = torch.cat([cls, embedded], dim=1)
            if attention_mask is not None:
                cls_mask = torch.ones(batch_size, 1, dtype=torch.bool, device=attention_mask.device)
                attention_mask = torch.cat([cls_mask, attention_mask], dim=1)

        src_key_padding_mask: torch.Tensor | None = None
        if attention_mask is not None:
            src_key_padding_mask = ~attention_mask

        return self.encoder(embedded, src_key_padding_mask=src_key_padding_mask)

    def forward(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Compute sequence-level embedding via the configured pooling strategy."""
        encoded = self._encode(action_ids, x_coords, y_coords, attention_mask)

        # The mask inside _encode was extended for cls prepending; reconstruct the
        # pooling mask here to match encoded's shape.
        pooling_mask = attention_mask
        if self.config.pooling_type == "cls" and attention_mask is not None:
            batch_size = attention_mask.size(0)
            cls_mask = torch.ones(batch_size, 1, dtype=torch.bool, device=attention_mask.device)
            pooling_mask = torch.cat([cls_mask, attention_mask], dim=1)

        if self.config.pooling_type == "cls":
            return encoded[:, 0, :]

        if self.config.pooling_type == "attention":
            scores = self.pool_attn(encoded).squeeze(-1)  # (batch, seq_len)
            if pooling_mask is not None:
                # Guard: a row with zero valid tokens would softmax (-inf, ..., -inf) → NaN.
                # Replace the scores row with zeros so softmax yields uniform weights;
                # the upstream training loader should filter empty sequences, but we
                # refuse to propagate NaN if one slips through.
                any_valid = pooling_mask.any(dim=1, keepdim=True)  # (batch, 1)
                scores = scores.masked_fill(~pooling_mask, float("-inf"))
                scores = torch.where(any_valid, scores, torch.zeros_like(scores))
            weights = torch.softmax(scores, dim=1).unsqueeze(-1)  # (batch, seq_len, 1)
            return (encoded * weights).sum(dim=1)

        # Default: mean pooling
        if pooling_mask is not None:
            mask_expanded = pooling_mask.unsqueeze(-1).float()
            summed = (encoded * mask_expanded).sum(dim=1)
            lengths = mask_expanded.sum(dim=1).clamp(min=1)
            return summed / lengths
        return encoded.mean(dim=1)

    def mlm_forward(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Compute per-token MLM logits for masked language modeling."""
        encoded = self._encode(action_ids, x_coords, y_coords, attention_mask)
        # For CLS variant: drop the prepended CLS position so logits align with input action_ids.
        if self.config.pooling_type == "cls":
            encoded = encoded[:, 1:, :]
        return self.mlm_head(encoded)


# ---------------------------------------------------------------------------
# Gradient Reversal Layer (Ganin et al. 2016)
# ---------------------------------------------------------------------------


class _GradientReversalFunction(Function):
    """Autograd function: identity forward, negated+scaled gradient backward.

    Used in domain-adversarial training to learn domain-invariant features
    by reversing the gradient flowing from the domain classifier.
    """

    @staticmethod
    def forward(ctx: torch.autograd.function.FunctionCtx, x: torch.Tensor, lambda_val: float) -> torch.Tensor:
        """Store lambda and pass input unchanged."""
        ctx.lambda_val = lambda_val  # type: ignore[attr-defined]
        return x.clone()

    @staticmethod
    def backward(ctx: torch.autograd.function.FunctionCtx, grad_output: torch.Tensor) -> tuple[torch.Tensor, None]:
        """Negate and scale the gradient by lambda."""
        return -ctx.lambda_val * grad_output, None  # type: ignore[attr-defined]


class GradientReversalLayer(nn.Module):
    """Gradient reversal layer for domain-adversarial training.

    During the forward pass, acts as identity. During the backward pass,
    negates and scales the gradient by ``lambda_val``. This encourages the
    upstream encoder to produce representations that are uninformative for
    the downstream domain classifier.

    Args:
        lambda_val: Gradient scaling factor. Higher values impose stronger
            domain-invariance pressure. Default 0.2 per Ganin et al. (2016).

    References:
        Ganin, Y. et al. (2016). "Domain-Adversarial Training of Neural
        Networks." JMLR 17(1), pp. 1-35.
    """

    def __init__(self, lambda_val: float = 0.2) -> None:
        super().__init__()
        self.lambda_val = lambda_val

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Apply gradient reversal (identity forward, negated backward)."""
        return cast(torch.Tensor, _GradientReversalFunction.apply(x, self.lambda_val))


# ---------------------------------------------------------------------------
# Team Classifier Head
# ---------------------------------------------------------------------------


class TeamClassifierHead(nn.Module):
    """Adversarial team classifier head with gradient reversal.

    Placed after the encoder, this head predicts team identity. The gradient
    reversal layer ensures that the encoder learns to *not* encode team-specific
    features, producing style-invariant embeddings.

    Args:
        hidden_dim: Input dimension (must match encoder hidden_dim).
        num_teams: Number of teams (output classes).
        lambda_val: Gradient reversal scaling factor.
    """

    def __init__(self, hidden_dim: int, num_teams: int, lambda_val: float = 0.2) -> None:
        super().__init__()
        self.grl = GradientReversalLayer(lambda_val)
        self.classifier = nn.Linear(hidden_dim, num_teams)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Classify team from encoder embedding with gradient reversal.

        Args:
            x: (batch, hidden_dim) encoder output.

        Returns:
            (batch, num_teams) unnormalized logits.
        """
        return self.classifier(self.grl(x))
