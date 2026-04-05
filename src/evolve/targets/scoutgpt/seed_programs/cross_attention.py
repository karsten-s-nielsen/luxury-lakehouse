"""Seed 2: Cross-attention player conditioning.

Player embedding as K/V in a dedicated cross-attention layer.
Hypothesis: separating player signal from action signal prevents dilution.
Fewer transformer layers to compensate for added cross-attention compute.
"""

config = {
    "conditioning_type": "cross_attention",
    "hidden_dim": 256,
    "num_layers": 4,
    "num_heads": 8,
    "dropout": 0.1,
    "max_seq_len": 128,
    "spatial_mlp_dim": 64,
    "vaep_loss_weight": 0.1,
    "learning_rate": 3e-4,
    "weight_decay": 0.01,
    "batch_size": 256,
}
