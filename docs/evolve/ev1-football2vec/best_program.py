"""EV1 winning config — iter 15 — val_accuracy=0.5865 at 15 epochs.

Rank 1 at 15-epoch fidelity across the top-10 EV1 candidates (see the updated
leaderboard in SUMMARY.md). The earlier 5-epoch signal ranked this candidate
at position 8 of 10; the 15-epoch retrain moved it to rank 1, beating iter-11
(0.5824) by +0.41 pp.

Key traits:
- Wide-shallow (192x4) beats narrow-deep (128x6) at 15 epochs.
- CLS pooling over mean pooling.
- Higher mask_prob (0.22 vs default 0.15) — harder pretext task.
- Low learning rate (3e-4 vs 5e-4) — more stable across 15 epochs.
- Default spatial+position scheme (additive + learnable) beat FiLM + sinusoidal
  once epochs were sufficient.
"""

config = {
    "hidden_dim": 192,
    "num_layers": 4,
    "num_heads": 6,
    "dropout": 0.1,
    "mask_prob": 0.22,
    "spatial_mlp_dim": 64,
    "pooling_type": "cls",
    "spatial_injection": "additive",
    "position_embedding": "learnable",
    "learning_rate": 3e-4,
    "batch_size": 256,
}
