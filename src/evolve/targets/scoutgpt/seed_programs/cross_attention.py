"""Seed 2: Cross-attention player conditioning (evolved).

Player embedding as K/V in a dedicated cross-attention layer.
Compact 3-layer architecture with multi-task regularisation
(VAEP + player prediction auxiliary loss).

Evolved from the original hand-crafted seed by Stage 3 evolution
(150 iterations, early-stopped at 114, 2026-04-06/07).
Key discoveries: smaller hidden_dim (192 vs 256), heavier VAEP
loss weight (0.32 vs 0.1), and activated player_prediction_weight
(0.18) together yield +35% spearman_rho with 41% fewer parameters.
"""

config = {
    "conditioning_type": "cross_attention",
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
