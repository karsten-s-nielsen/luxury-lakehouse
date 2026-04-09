"""Seed 8: Fourier spatial features + cross-attention (Level 2 — code evolution).

Standard MLPs suffer from spectral bias — they struggle to learn
high-frequency spatial functions from raw scalar coordinates. This seed
applies Random Fourier Feature (RFF) projections to all four spatial
coordinates before fusing with the action embedding. The sin/cos mapping
lifts each scalar into a rich frequency space, allowing the model to
capture rapid spatial variations (e.g., near-goal vs midfield).

The Fourier projection matrix B is initialized randomly and learned
during training (learnable Fourier features). The downstream cross-
attention layers learn how to use the enriched spatial signal.

Reference: Tancik et al. (2020), "Fourier Features Let Networks Learn
High Frequency Functions in Low Dimensional Domains".
"""

config = {
    "conditioning_type": "cross_attention",  # overridden to "additive" by evaluator when custom_embed present
    "hidden_dim": 192,
    "num_layers": 3,
    "num_heads": 6,
    "dropout": 0.15,
    "max_seq_len": 128,
    "spatial_mlp_dim": 64,
    "vaep_loss_weight": 0.32,
    "player_prediction_weight": 0.18,
    "learning_rate": 2e-4,
    "weight_decay": 0.01,
    "batch_size": 384,
}

# Number of Fourier frequencies per spatial coordinate
_N_FREQS = 32


def custom_layers(hidden_dim):
    """Cross-attention + Fourier spatial projection."""
    num_heads = config["num_heads"]
    dropout = config["dropout"]
    return {
        # Frozen random projection matrix for 4 spatial coords -> Fourier features
        # Each coord produces 2 * _N_FREQS features (sin + cos), 4 coords total
        "fourier_B": torch.nn.Linear(4, _N_FREQS * 4, bias=False),
        # Project Fourier features to hidden_dim
        "fourier_proj": torch.nn.Linear(_N_FREQS * 4 * 2, hidden_dim),
        # Cross-attention for player conditioning
        "fourier_cross_attn": torch.nn.MultiheadAttention(
            hidden_dim,
            num_heads,
            dropout=dropout,
            batch_first=True,
        ),
        "fourier_cross_norm": torch.nn.LayerNorm(hidden_dim),
    }


def custom_embed(self, action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids):
    """Fourier spatial features + cross-attention player conditioning."""
    player_emb = self.player_embedding(player_ids)

    # Stack spatial coordinates: (B, S, 4)
    spatial = torch.stack([start_x, start_y, end_x, end_y], dim=-1)

    # Random Fourier features: project then sin/cos
    projected = self.fourier_B(spatial)  # (B, S, N_FREQS*4)
    fourier_feats = torch.cat([torch.sin(projected), torch.cos(projected)], dim=-1)
    spatial_emb = self.fourier_proj(fourier_feats)  # (B, S, hd)

    # Combine token + spatial + temporal + result
    action_emb = (
        self.token_embedding(action_ids) + spatial_emb + self.result_embedding(result) + self.time_delta_mlp(time_delta)
    )

    # Cross-attention: action queries attend to player keys/values
    attn_out, _ = self.fourier_cross_attn(
        query=action_emb,
        key=player_emb,
        value=player_emb,
    )
    emb = self.fourier_cross_norm(action_emb + attn_out)

    return self.embedding_dropout(emb)
