"""Seed 1: Additive player conditioning (current baseline).

Sum player embedding with all other embeddings. This is the existing
ScoutGPT architecture that achieves 81.5% top-1 but only 0.094 rho.
"""

config = {
    "conditioning_type": "additive",
    "hidden_dim": 256,
    "num_layers": 6,
    "num_heads": 8,
    "dropout": 0.1,
    "max_seq_len": 128,
    "spatial_mlp_dim": 64,
    "vaep_loss_weight": 0.1,
    "learning_rate": 1e-4,
    "weight_decay": 0.01,
    "batch_size": 256,
}
