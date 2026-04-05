"""Seed 4: Gated player conditioning with auxiliary player prediction loss.

Learned gate: sigmoid(W * player_emb) . action_emb — player selectively
amplifies/suppresses action features. Includes an auxiliary loss that
predicts the player from the hidden state, forcing representations to
retain player identity throughout the network.
"""

config = {
    "conditioning_type": "gated",
    "hidden_dim": 256,
    "num_layers": 6,
    "num_heads": 8,
    "dropout": 0.1,
    "max_seq_len": 128,
    "spatial_mlp_dim": 64,
    "vaep_loss_weight": 0.1,
    "player_prediction_weight": 0.05,
    "learning_rate": 1e-4,
    "weight_decay": 0.01,
    "batch_size": 256,
}
