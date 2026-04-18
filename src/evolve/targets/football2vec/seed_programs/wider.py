"""Seed 2: Wider — increased hidden_dim and num_heads."""

config = {
    "hidden_dim": 192,
    "num_layers": 4,
    "num_heads": 6,
    "dropout": 0.1,
    "mask_prob": 0.15,
    "spatial_mlp_dim": 64,
    "pooling_type": "mean",
    "spatial_injection": "additive",
    "position_embedding": "learnable",
    "learning_rate": 1e-4,
    "batch_size": 256,
}
