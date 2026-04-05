"""ScoutGPT: Player-conditioned causal decoder over SPADL possession episodes.

Architecture follows Hong et al. (2025), arXiv:2512.17266 — a GPT-style transformer
with player ID conditioning for counterfactual substitution. Per-action player
attribution and VAEP auxiliary regression head.

Reuses SpatialMLP from football2vec_transformer for coordinate encoding.
"""

from __future__ import annotations

from dataclasses import dataclass

import torch
import torch.nn as nn

from analytics.football2vec_transformer import SpatialMLP

# Special tokens - action vocab is 0-22 (23 types)
PAD_TOKEN_ID = 23
BOS_TOKEN_ID = 24
EXPANDED_VOCAB_SIZE = 25  # 23 actions + PAD + BOS


@dataclass(frozen=True)
class ScoutGPTConfig:
    """Configuration for the ScoutGPT decoder."""

    vocab_size: int = 23
    hidden_dim: int = 256
    num_layers: int = 6
    num_heads: int = 8
    dropout: float = 0.1
    max_seq_len: int = 128
    num_players: int = 11_918
    spatial_mlp_dim: int = 64
    vaep_loss_weight: float = 0.1
    conditioning_type: str = "additive"


class ScoutGPTDecoder(nn.Module):
    """Player-conditioned GPT-style causal decoder.

    Input features per token: action_type, start_x, start_y, end_x, end_y,
    action_result, time_delta, player_id. Position 0 is the focal player
    conditioning token (BOS + player embedding).

    Prediction heads:
      - action_head: next action type (23-class, cross-entropy)
      - vaep_head: current action VAEP (regression, MSE, weight config.vaep_loss_weight)
    """

    def __init__(self, config: ScoutGPTConfig | None = None) -> None:
        super().__init__()
        self.config = config or ScoutGPTConfig()
        c = self.config
        hd = c.hidden_dim

        # Token and player embeddings
        self.token_embedding = nn.Embedding(EXPANDED_VOCAB_SIZE, hd)
        self.player_embedding = nn.Embedding(c.num_players, hd)

        # Conditioning mechanism for player identity
        self._conditioning_type = c.conditioning_type
        if c.conditioning_type == "additive":
            pass  # Player embedding summed directly with action embedding
        elif c.conditioning_type == "cross_attention":
            self.player_cross_attn = nn.MultiheadAttention(hd, c.num_heads, dropout=c.dropout, batch_first=True)
            self.player_cross_norm = nn.LayerNorm(hd)
        elif c.conditioning_type == "film":
            self.film_scale = nn.Sequential(nn.Linear(hd, hd), nn.Sigmoid())
            self.film_shift = nn.Linear(hd, hd)
        elif c.conditioning_type == "gated":
            self.player_gate = nn.Sequential(nn.Linear(hd, hd), nn.Sigmoid())
        else:
            msg = f"Unknown conditioning_type: {c.conditioning_type!r}"
            raise ValueError(msg)

        # Spatial encoders (4 for coordinates + 1 for time delta)
        self.start_x_mlp = SpatialMLP(hd, c.spatial_mlp_dim)
        self.start_y_mlp = SpatialMLP(hd, c.spatial_mlp_dim)
        self.end_x_mlp = SpatialMLP(hd, c.spatial_mlp_dim)
        self.end_y_mlp = SpatialMLP(hd, c.spatial_mlp_dim)
        self.time_delta_mlp = SpatialMLP(hd, c.spatial_mlp_dim)

        # Result embedding (binary: 0=fail, 1=success)
        self.result_embedding = nn.Embedding(2, hd)

        # Positional embedding
        self.position_embedding = nn.Embedding(c.max_seq_len, hd)

        self.embedding_dropout = nn.Dropout(c.dropout)

        # Causal transformer (nn.TransformerEncoder + is_causal = GPT pattern)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=hd,
            nhead=c.num_heads,
            dim_feedforward=hd * 4,
            dropout=c.dropout,
            activation="gelu",
            batch_first=True,
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=c.num_layers)

        # Prediction heads
        self.action_head = nn.Linear(hd, c.vocab_size)
        self.vaep_head = nn.Linear(hd, 1)

        # Pre-computed buffers (avoids per-forward-pass GPU allocation)
        self.register_buffer(
            "_causal_mask", torch.triu(torch.ones(c.max_seq_len, c.max_seq_len, dtype=torch.bool), diagonal=1)
        )
        self.register_buffer("_pos_ids", torch.arange(c.max_seq_len).unsqueeze(0))

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
        start_x: torch.Tensor,
        start_y: torch.Tensor,
        end_x: torch.Tensor,
        end_y: torch.Tensor,
        result: torch.Tensor,
        time_delta: torch.Tensor,
        player_ids: torch.Tensor,
    ) -> torch.Tensor:
        """Compute input embeddings with configurable player conditioning.

        All inputs are (batch, seq_len). Returns (batch, seq_len, hidden_dim).

        Conditioning types:
          - additive: player_emb summed with action features (original behavior)
          - cross_attention: action attends to player embedding via multi-head attention
          - film: Feature-wise Linear Modulation — player controls scale and shift
          - gated: learned sigmoid gate weights the action signal, plus player residual
        """
        seq_len = action_ids.size(1)

        # Player embedding computed separately for conditioning
        player_emb = self.player_embedding(player_ids)

        # Action embedding: all components EXCEPT player
        action_emb = (
            self.token_embedding(action_ids)
            + self.start_x_mlp(start_x)
            + self.start_y_mlp(start_y)
            + self.end_x_mlp(end_x)
            + self.end_y_mlp(end_y)
            + self.result_embedding(result)
            + self.time_delta_mlp(time_delta)
            + self.position_embedding(self._pos_ids[:, :seq_len])  # type: ignore[index]
        )

        # Apply conditioning
        if self._conditioning_type == "additive":
            emb = action_emb + player_emb
        elif self._conditioning_type == "cross_attention":
            attn_out, _ = self.player_cross_attn(query=action_emb, key=player_emb, value=player_emb)
            emb = self.player_cross_norm(action_emb + attn_out)
        elif self._conditioning_type == "film":
            scale = self.film_scale(player_emb)
            shift = self.film_shift(player_emb)
            emb = scale * action_emb + shift
        elif self._conditioning_type == "gated":
            gate = self.player_gate(player_emb)
            emb = gate * action_emb + player_emb
        else:
            msg = f"Unknown conditioning_type: {self._conditioning_type!r}"
            raise ValueError(msg)

        return self.embedding_dropout(emb)

    def _encode(
        self,
        action_ids: torch.Tensor,
        start_x: torch.Tensor,
        start_y: torch.Tensor,
        end_x: torch.Tensor,
        end_y: torch.Tensor,
        result: torch.Tensor,
        time_delta: torch.Tensor,
        player_ids: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Run causal transformer. Returns (batch, seq_len, hidden_dim)."""
        emb = self._embed(action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids)
        seq_len = emb.size(1)

        # Pre-computed causal mask (register_buffer), sliced to seq_len
        causal_mask = self._causal_mask[:seq_len, :seq_len]  # type: ignore[index]

        # Padding mask: TransformerEncoder uses True = ignore
        src_key_padding_mask: torch.Tensor | None = None
        if attention_mask is not None:
            src_key_padding_mask = ~attention_mask

        return self.transformer(emb, mask=causal_mask, src_key_padding_mask=src_key_padding_mask)

    def forward(
        self,
        action_ids: torch.Tensor,
        start_x: torch.Tensor,
        start_y: torch.Tensor,
        end_x: torch.Tensor,
        end_y: torch.Tensor,
        result: torch.Tensor,
        time_delta: torch.Tensor,
        player_ids: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Mean-pooled sequence representation. Returns (batch, hidden_dim).

        Used for embedding extraction and future adversarial debiasing stage.
        """
        hidden = self._encode(
            action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids, attention_mask
        )
        if attention_mask is not None:
            mask_expanded = attention_mask.unsqueeze(-1).float()
            hidden = hidden * mask_expanded
            return hidden.sum(dim=1) / mask_expanded.sum(dim=1).clamp(min=1)
        return hidden.mean(dim=1)

    def predict(
        self,
        action_ids: torch.Tensor,
        start_x: torch.Tensor,
        start_y: torch.Tensor,
        end_x: torch.Tensor,
        end_y: torch.Tensor,
        result: torch.Tensor,
        time_delta: torch.Tensor,
        player_ids: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        """Per-position predictions. Returns (action_logits, vaep_preds).

        action_logits: (batch, seq_len, vocab_size)
        vaep_preds: (batch, seq_len, 1)
        """
        hidden = self._encode(
            action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids, attention_mask
        )
        return self.action_head(hidden), self.vaep_head(hidden)
