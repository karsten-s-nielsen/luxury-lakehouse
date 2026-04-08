"""Seed 5: Hybrid gated cross-attention (Level 2 — code evolution).

Demonstrates the custom_layers + custom_embed format. Combines cross-attention
with a learned sigmoid gate that controls how much player identity information
flows into the action embeddings. The gate is a separate MLP declared via
custom_layers, allowing the evolution engine to modify or replace it.

This is a starting point for Level 2 evolution — the LLM can modify both
the gating mechanism and the attention pattern.
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
    """Cross-attention + learned gate for hybrid conditioning.

    Must register player_cross_attn and player_cross_norm here because
    code_evolution=true forces conditioning_type="additive", so the base
    model will not create them.
    """
    num_heads = config["num_heads"]
    dropout = config["dropout"]
    return {
        "player_cross_attn": torch.nn.MultiheadAttention(
            hidden_dim, num_heads, dropout=dropout, batch_first=True,
        ),
        "player_cross_norm": torch.nn.LayerNorm(hidden_dim),
        "hybrid_gate": torch.nn.Sequential(
            torch.nn.Linear(hidden_dim, hidden_dim // 4),
            torch.nn.ReLU(),
            torch.nn.Linear(hidden_dim // 4, hidden_dim),
            torch.nn.Sigmoid(),
        ),
    }


def custom_embed(self, action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids):
    """Gated cross-attention: attention output is modulated by a learned gate."""
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
    attn_out, _ = self.player_cross_attn(
        query=action_emb,
        key=player_emb,
        value=player_emb,
    )
    attn_out = self.player_cross_norm(action_emb + attn_out)

    # Learned gate modulates how much player info flows through
    gate = self.hybrid_gate(player_emb)
    emb = gate * attn_out + (1 - gate) * action_emb

    return self.embedding_dropout(emb)
