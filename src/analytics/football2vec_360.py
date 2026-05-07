"""Football2Vec 360-enriched encoder -- transformer + Deep Sets -> 208d.

Extends the Football2Vec v2 transformer encoder with a Deep Sets branch
that encodes StatsBomb 360 freeze frame context per action. Output is
192d (transformer) + 16d (Deep Sets) = 208d.

References:
    Theiner, J. et al. (2022). "Extraction of Positional Player Data from
        Broadcast Soccer Videos." WACV.
    Zaheer, M. et al. (2017). "Deep Sets." NeurIPS.
    Danesi, P. (2025). "Football2Vec: Transformer-Based Player Embeddings."
"""

from __future__ import annotations

from dataclasses import dataclass

import torch
import torch.nn as nn

from analytics.football2vec_transformer import Football2VecConfig, Football2VecEncoder

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Football2Vec360Config(Football2VecConfig):
    """Immutable configuration for the 360-enriched Football2Vec encoder.

    Extends :class:`Football2VecConfig` with Deep Sets parameters for
    encoding StatsBomb 360 freeze frame context.

    Attributes:
        context_dim: Output dimension of the Deep Sets encoder.
        deep_sets_hidden: Hidden dimension of the per-player MLP.
        player_feature_dim: Number of raw features per player (x, y, is_keeper, is_teammate).
        use_pretrained_encoder: Whether to load pretrained transformer weights.
    """

    context_dim: int = 16
    deep_sets_hidden: int = 32
    player_feature_dim: int = 4
    use_pretrained_encoder: bool = True


# ---------------------------------------------------------------------------
# Deep Sets Encoder
# ---------------------------------------------------------------------------


class _DeepSetsEncoder(nn.Module):
    """Deep Sets encoder for variable-size player sets (Zaheer et al. 2017).

    Architecture:
        Linear(player_feature_dim -> deep_sets_hidden) -> ReLU
        -> Linear(deep_sets_hidden -> context_dim) -> ReLU
        -> sum aggregation over players

    Handles two input formats:
        - Raw player features: (batch, seq_len, max_players, player_feature_dim)
          Applies per-player MLP then sum aggregation over the player axis.
        - Pre-encoded context: (batch, seq_len, context_dim)
          Passed through unchanged (already aggregated).

    Args:
        config: Frozen 360 encoder configuration.
    """

    def __init__(self, config: Football2Vec360Config) -> None:
        super().__init__()
        self.config = config
        self.fc1 = nn.Linear(config.player_feature_dim, config.deep_sets_hidden)
        self.fc2 = nn.Linear(config.deep_sets_hidden, config.context_dim)
        self.relu = nn.ReLU()

    def forward(self, context: torch.Tensor) -> torch.Tensor:
        """Encode 360 freeze frame context.

        Args:
            context: Either (batch, seq_len, max_players, player_feature_dim)
                for raw player features, or (batch, seq_len, context_dim)
                for pre-encoded context vectors.

        Returns:
            (batch, seq_len, context_dim) encoded context.
        """
        if context.ndim == 4:
            # Raw player features: apply per-player MLP + sum aggregation
            # (batch, seq_len, max_players, player_feature_dim)
            h = self.relu(self.fc1(context))
            h = self.relu(self.fc2(h))
            # Sum over player axis → (batch, seq_len, context_dim)
            return h.sum(dim=2)
        # Pre-encoded: (batch, seq_len, context_dim) — pass through
        return context


# ---------------------------------------------------------------------------
# 360-Enriched Encoder
# ---------------------------------------------------------------------------


class Football2Vec360Encoder(nn.Module):
    """Football2Vec encoder enriched with 360 freeze frame context.

    Combines the transformer encoder (SPADL action sequences -> 192d)
    with a Deep Sets branch (360 freeze frames -> 16d) via concatenation
    to produce 208d embeddings.

    When ``context_360`` is not provided, the Deep Sets branch outputs
    zeros, producing a graceful fallback to transformer-only embeddings.

    Args:
        config: Frozen 360 encoder configuration.
    """

    def __init__(self, config: Football2Vec360Config | None = None) -> None:
        super().__init__()
        cfg = config or Football2Vec360Config()
        self.config = cfg

        # Compose the base transformer encoder (DRY — reuse, don't duplicate).
        self.base_encoder = Football2VecEncoder(cfg)

        # --- Deep Sets branch ---
        self.deep_sets = _DeepSetsEncoder(cfg)

        # Zero fallback for missing 360 data (avoids allocation per forward pass)
        self.register_buffer("_zero_context", torch.zeros(1, cfg.context_dim))

    def forward(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
        context_360: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Compute 360-enriched sequence embedding.

        Encodes actions via the transformer branch (mean-pooled to hidden_dim),
        encodes 360 context via the Deep Sets branch (sum-aggregated per action,
        then mean-pooled across the sequence to context_dim), and concatenates
        to produce (hidden_dim + context_dim) output.

        Args:
            action_ids: (batch, seq_len) long tensor of SPADL action type indices.
            x_coords: (batch, seq_len) float tensor of normalized x coordinates.
            y_coords: (batch, seq_len) float tensor of normalized y coordinates.
            attention_mask: (batch, seq_len) bool tensor. True = valid token.
                If None, all tokens are treated as valid.
            context_360: Optional 360 freeze frame context. Either
                (batch, seq_len, context_dim) pre-encoded or
                (batch, seq_len, max_players, player_feature_dim) raw.
                If None, zeros are used for the Deep Sets branch.

        Returns:
            (batch, hidden_dim + context_dim) concatenated embedding tensor.
        """
        batch_size = action_ids.size(0)

        # --- Transformer branch (delegated to composed base encoder) ---
        transformer_out = self.base_encoder(
            action_ids,
            x_coords,
            y_coords,
            attention_mask,
        )  # (batch, hidden_dim)

        # --- Deep Sets branch ---
        if context_360 is not None:
            # Encode 360 context -> (batch, seq_len, context_dim)
            context_encoded = self.deep_sets(context_360)

            # Mean pooling over sequence positions -> (batch, context_dim)
            if attention_mask is not None:
                mask_ctx = attention_mask.unsqueeze(-1).float()
                ctx_summed = (context_encoded * mask_ctx).sum(dim=1)
                ctx_lengths = mask_ctx.sum(dim=1).clamp(min=1)
                deep_sets_out = ctx_summed / ctx_lengths
            else:
                deep_sets_out = context_encoded.mean(dim=1)
        else:
            # No 360 data: expand pre-allocated zero buffer to batch size
            deep_sets_out = self._zero_context.expand(batch_size, -1)  # type: ignore[union-attr]

        # Concatenate: (batch, hidden_dim + context_dim)
        return torch.cat([transformer_out, deep_sets_out], dim=-1)
