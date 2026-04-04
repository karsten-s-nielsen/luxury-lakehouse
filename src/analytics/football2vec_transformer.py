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

from dataclasses import dataclass
from typing import cast

import torch
import torch.nn as nn
from torch.autograd import Function

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
    """

    vocab_size: int = 23
    hidden_dim: int = 128
    num_layers: int = 4
    num_heads: int = 4
    dropout: float = 0.1
    max_seq_len: int = 512
    mask_prob: float = 0.15
    spatial_mlp_dim: int = 64


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

        # Positional encoding: learnable
        self.position_embedding = nn.Embedding(cfg.max_seq_len, cfg.hidden_dim)

        # Dropout after embedding sum
        self.embedding_dropout = nn.Dropout(cfg.dropout)

        # Transformer encoder
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=cfg.hidden_dim,
            nhead=cfg.num_heads,
            dim_feedforward=cfg.hidden_dim * 4,
            dropout=cfg.dropout,
            activation="gelu",
            batch_first=True,
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=cfg.num_layers)

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
        """Compute combined embedding: token + spatial_x + spatial_y + position.

        Args:
            action_ids: (batch, seq_len) long tensor of SPADL action type indices.
            x_coords: (batch, seq_len) float tensor of normalized x coordinates.
            y_coords: (batch, seq_len) float tensor of normalized y coordinates.

        Returns:
            (batch, seq_len, hidden_dim) combined embedding tensor.
        """
        seq_len = action_ids.size(1)

        # Token embeddings
        tok_emb = self.token_embedding(action_ids)

        # Spatial encodings
        x_emb = self.spatial_x(x_coords)
        y_emb = self.spatial_y(y_coords)

        # Positional embeddings (pre-computed buffer, sliced to seq_len)
        pos_emb = self.position_embedding(self._pos_ids[:, :seq_len])  # type: ignore[index]

        # Sum all embedding components
        combined = tok_emb + x_emb + y_emb + pos_emb
        return self.embedding_dropout(combined)

    def _encode(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Run the transformer encoder on embedded inputs.

        Args:
            action_ids: (batch, seq_len) long tensor of SPADL action type indices.
            x_coords: (batch, seq_len) float tensor of normalized x coordinates.
            y_coords: (batch, seq_len) float tensor of normalized y coordinates.
            attention_mask: (batch, seq_len) bool tensor. True = valid token,
                False = padding. Converted to src_key_padding_mask (True = ignore).

        Returns:
            (batch, seq_len, hidden_dim) encoder output tensor.
        """
        embedded = self._embed(action_ids, x_coords, y_coords)

        # TransformerEncoder expects src_key_padding_mask where True = ignore
        src_key_padding_mask: torch.Tensor | None = None
        if attention_mask is not None:
            src_key_padding_mask = ~attention_mask  # invert: True → ignore

        return self.encoder(embedded, src_key_padding_mask=src_key_padding_mask)

    def forward(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Compute sequence-level embedding via mean pooling over valid tokens.

        Args:
            action_ids: (batch, seq_len) long tensor of SPADL action type indices.
            x_coords: (batch, seq_len) float tensor of normalized x coordinates.
            y_coords: (batch, seq_len) float tensor of normalized y coordinates.
            attention_mask: (batch, seq_len) bool tensor. True = valid token.
                If None, all tokens are treated as valid.

        Returns:
            (batch, hidden_dim) sequence embedding tensor (mean-pooled).
        """
        encoded = self._encode(action_ids, x_coords, y_coords, attention_mask)

        # Mean pooling over valid tokens
        if attention_mask is not None:
            # Expand mask for broadcasting: (batch, seq_len, 1)
            mask_expanded = attention_mask.unsqueeze(-1).float()
            # Zero out padding positions and compute mean over valid tokens
            summed = (encoded * mask_expanded).sum(dim=1)
            lengths = mask_expanded.sum(dim=1).clamp(min=1)  # avoid division by zero
            return summed / lengths
        else:
            return encoded.mean(dim=1)

    def mlm_forward(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Compute per-token MLM logits for masked language modeling.

        Args:
            action_ids: (batch, seq_len) long tensor of SPADL action type indices.
            x_coords: (batch, seq_len) float tensor of normalized x coordinates.
            y_coords: (batch, seq_len) float tensor of normalized y coordinates.
            attention_mask: (batch, seq_len) bool tensor. True = valid token.

        Returns:
            (batch, seq_len, vocab_size) logits for masked token prediction.
        """
        encoded = self._encode(action_ids, x_coords, y_coords, attention_mask)
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
