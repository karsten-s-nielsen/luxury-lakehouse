"""Seed 7: Gated cross-attention with orthogonal projection (Level 2 — code evolution).

Extends standard cross-attention by projecting the attention output onto
the orthogonal complement of the action embedding before fusion. This
forces the attention mechanism to inject strictly novel, non-redundant
information rather than amplifying existing features. A learned gate
controls the injection strength.

The orthogonal projection is computed inline via the Gram-Schmidt
rejection: proj_orth = attn_out - (dot(attn_out, action) / dot(action, action)) * action.
This geometric constraint improves parameter efficiency and head diversity.

Reference: Du et al. (2023), cross-domain sequential recommendation
with orthogonal alignment constraints.
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


def custom_layers(hidden_dim):
    """Cross-attention + orthogonal gate for non-redundant player conditioning."""
    num_heads = config["num_heads"]
    dropout = config["dropout"]
    return {
        "orth_cross_attn": torch.nn.MultiheadAttention(
            hidden_dim,
            num_heads,
            dropout=dropout,
            batch_first=True,
        ),
        "orth_gate": torch.nn.Sequential(
            torch.nn.Linear(hidden_dim * 2, hidden_dim),
            torch.nn.Sigmoid(),
        ),
        "orth_norm": torch.nn.LayerNorm(hidden_dim),
    }


def custom_embed(self, action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids):
    """Orthogonal cross-attention: inject only novel information from player context."""
    player_emb = self.player_embedding(player_ids)
    action_emb = (
        self.token_embedding(action_ids)
        + self.start_x_mlp(start_x)
        + self.start_y_mlp(start_y)
        + self.end_x_mlp(end_x)
        + self.end_y_mlp(end_y)
        + self.result_embedding(result)
        + self.time_delta_mlp(time_delta)
    )

    # Cross-attention: action queries attend to player keys/values
    attn_out, _ = self.orth_cross_attn(
        query=action_emb,
        key=player_emb,
        value=player_emb,
    )

    # Orthogonal projection: remove the component parallel to action_emb
    # proj_orth = attn_out - (dot(attn_out, action) / dot(action, action)) * action
    dot_aa = (action_emb * action_emb).sum(dim=-1, keepdim=True).clamp(min=1e-8)
    dot_oa = (attn_out * action_emb).sum(dim=-1, keepdim=True)
    attn_orth = attn_out - (dot_oa / dot_aa) * action_emb  # (B, S, hd)

    # Learned gate controls injection strength
    gate = self.orth_gate(torch.cat([action_emb, player_emb], dim=-1))
    emb = self.orth_norm(action_emb + gate * attn_orth)

    return self.embedding_dropout(emb)
