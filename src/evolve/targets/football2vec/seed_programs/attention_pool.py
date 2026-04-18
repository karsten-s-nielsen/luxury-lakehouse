"""Seed 5: Attention pooling instead of mean."""

config = {
    "hidden_dim": 128,
    "num_layers": 4,
    "num_heads": 4,
    "dropout": 0.1,
    "mask_prob": 0.15,
    "spatial_mlp_dim": 64,
    "pooling_type": "attention",
    "spatial_injection": "additive",
    "position_embedding": "learnable",
    "learning_rate": 1e-4,
    "batch_size": 256,
}
