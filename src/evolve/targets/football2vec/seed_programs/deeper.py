"""Seed 3: Deeper — more layers at the baseline width."""

config = {
    "hidden_dim": 128,
    "num_layers": 6,
    "num_heads": 4,
    "dropout": 0.1,
    "mask_prob": 0.15,
    "spatial_mlp_dim": 64,
    "pooling_type": "mean",
    "spatial_injection": "additive",
    "position_embedding": "learnable",
    "learning_rate": 1e-4,
    "batch_size": 256,
}
