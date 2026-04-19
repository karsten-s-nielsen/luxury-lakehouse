"""EV1 winning config — iter 11 — val_accuracy=0.5693, param_count=1,295,255.

LLM-mutated from the wider.py seed. Key changes from the seed:
- hidden_dim 192 -> 128 (narrower)
- num_layers 4 -> 6 (deeper)
- num_heads 6 -> 8
- dropout 0.1 -> 0.2 (more regularization for the deeper stack)
- spatial_injection "additive" -> "film"
- position_embedding "learnable" -> "sinusoidal" (parameter-free)
- learning_rate 1e-4 -> 5e-4 (5x higher to compensate for depth in 5-epoch budget)
"""

config = {
    "hidden_dim": 128,
    "num_layers": 6,
    "num_heads": 8,
    "dropout": 0.2,
    "mask_prob": 0.15,
    "spatial_mlp_dim": 64,
    "pooling_type": "mean",
    "spatial_injection": "film",
    "position_embedding": "sinusoidal",
    "learning_rate": 5e-4,
    "batch_size": 256,
}
