"""Seed 4: Heavier mask — higher mask_prob and learning rate."""

config = {
    "hidden_dim": 128,
    "num_layers": 4,
    "num_heads": 4,
    "dropout": 0.1,
    "mask_prob": 0.20,
    "spatial_mlp_dim": 64,
    "pooling_type": "mean",
    "spatial_injection": "additive",
    "position_embedding": "learnable",
    "learning_rate": 2e-4,
    "batch_size": 256,
}
