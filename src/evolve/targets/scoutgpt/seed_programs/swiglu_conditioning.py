"""Seed 6: SwiGLU conditioning (Level 2 — code evolution).

Replaces standard sigmoid gating with Swish-Gated Linear Units (SwiGLU),
the activation mechanism behind LLaMA and PaLM. Concatenates action and
player embeddings, projects into a high-dimensional space, then splits
into a data path and a Swish-gated control path. The smooth, non-monotonic
Swish activation avoids dead neurons and enables dynamic, data-dependent
filtering that sigmoid gating cannot express.

Reference: Shazeer (2020), "GLU Variants Improve Transformer".
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
    """SwiGLU projection: concat(action, player) -> split -> swish-gate -> project back."""
    return {
        "swiglu_w1": torch.nn.Linear(hidden_dim * 2, hidden_dim, bias=False),
        "swiglu_w2": torch.nn.Linear(hidden_dim * 2, hidden_dim, bias=False),
        "swiglu_proj": torch.nn.Linear(hidden_dim, hidden_dim, bias=False),
        "swiglu_norm": torch.nn.LayerNorm(hidden_dim),
    }


def custom_embed(self, action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids):
    """SwiGLU conditioning: non-monotonic multiplicative fusion of player and action."""
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

    # Concat player + action, split into data path and gating path
    combined = torch.cat([action_emb, player_emb], dim=-1)  # (B, S, 2*hd)
    data_path = self.swiglu_w1(combined)                     # (B, S, hd)
    gate_path = torch.nn.functional.silu(self.swiglu_w2(combined))  # (B, S, hd) — Swish
    fused = data_path * gate_path                            # (B, S, hd) — Hadamard product

    # Project back and residual connection with action embedding
    emb = self.swiglu_norm(action_emb + self.swiglu_proj(fused))

    return self.embedding_dropout(emb)
