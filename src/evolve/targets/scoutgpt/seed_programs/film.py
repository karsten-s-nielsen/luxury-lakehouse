"""Seed 3: Feature-wise Linear Modulation (FiLM) player conditioning.

Player embedding predicts per-channel scale + shift applied to the
action embedding (Perez et al. 2018). Multiplicative interaction gives
the player stronger control over the representation.
"""

config = {
    "conditioning_type": "film",
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
