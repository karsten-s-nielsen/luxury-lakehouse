---
language:
  - en
license: cc-by-nc-4.0
library_name: pytorch
tags:
  - sports-analytics
  - soccer
  - football
  - player-embeddings
  - transformer
  - scoutgpt
  - rope
  - research-artifact
datasets:
  - luxury-lakehouse/scoutgpt-training-data
pipeline_tag: feature-extraction
---

# ScoutGPT — RoPE Variant (Research Artifact)

Ablation checkpoint of the [ScoutGPT](https://huggingface.co/luxury-lakehouse/scoutgpt) transformer decoder with **Rotary Positional Embedding (RoPE)** replacing the default learnable absolute positional embedding. Produced by the `rope-scoutgpt` A/B cycle (2026-04-19 → 2026-04-22) and retained for reproducibility of the ablation comparison against the `learnable` baseline.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Status

- **Status**: Research artifact — **not** the canonical production ScoutGPT model
- **Canonical model**: [`luxury-lakehouse/scoutgpt`](https://huggingface.co/luxury-lakehouse/scoutgpt)
- **Retained for**: Reproducibility of the RoPE A/B experiment documented in [`docs/evolve/rope-scoutgpt/SUMMARY.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/evolve/rope-scoutgpt/SUMMARY.md)

## Architecture

Identical to canonical ScoutGPT *except*:

- **Position encoding**: RoPE (Su et al. 2021) applied to Q/K vectors in every attention layer, replacing learnable absolute positions
- **All other hyperparameters**: unchanged (depth, heads, hidden dim, SwiGLU FFN, dropout)

## Training

- **Dataset**: [`luxury-lakehouse/scoutgpt-training-data`](https://huggingface.co/datasets/luxury-lakehouse/scoutgpt-training-data)
- **Optimiser**: AdamW, cosine LR schedule
- **Hardware**: HF Jobs L40S single-GPU
- **Epochs**: 15 (best-of-15 checkpoint published)
- **Publishing script**: [`scripts/run_rope_scoutgpt_ab.py`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/scripts/run_rope_scoutgpt_ab.py)

Full training metrics (loss curves, ablation deltas vs the `learnable` variant) are in `metrics.json` alongside the weights.

## Artefacts in the repo

- `pytorch_model.bin` — best-of-15-epoch checkpoint
- `metrics.json` — per-epoch train / val loss + ablation comparison snapshot

## Use Cases

- **Ablation reproduction**: input for papers / posts comparing RoPE vs learnable positions on short-sequence soccer data
- **Not for downstream inference**: production downstream consumers should pull the canonical [`luxury-lakehouse/scoutgpt`](https://huggingface.co/luxury-lakehouse/scoutgpt) instead

## EU AI Act — Intended Use and Non-Use

This model is **not** intended for, validated for, or supplied to any Annex III §4 use (employment, worker management, access-to-employment decisions). It is a research ablation artefact. Any deployer who wishes to repurpose it for a high-risk use must perform their own conformity assessment under the EU AI Act.

## License

CC-BY-NC 4.0 (inherited from the training-data licensing via Wyscout).

## Citation

```bibtex
@software{scoutgpt_variant_rope_2026,
  title={ScoutGPT RoPE Variant},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://huggingface.co/luxury-lakehouse/scoutgpt-variant-rope}
}
```

RoPE paper:

```bibtex
@article{su2021roformer,
  title={RoFormer: Enhanced Transformer with Rotary Position Embedding},
  author={Su, Jianlin and Lu, Yu and Pan, Shengfeng and Murtadha, Ahmed and Wen, Bo and Liu, Yunfeng},
  journal={arXiv preprint arXiv:2104.09864},
  year={2021}
}
```
