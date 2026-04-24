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
  - research-artifact
datasets:
  - luxury-lakehouse/scoutgpt-training-data
pipeline_tag: feature-extraction
---

# ScoutGPT — Learnable Positions Variant (Research Artifact)

Ablation checkpoint of the [ScoutGPT](https://huggingface.co/luxury-lakehouse/scoutgpt) transformer decoder with **learnable absolute positional embeddings** — the baseline against which the [`rope`](https://huggingface.co/luxury-lakehouse/scoutgpt-variant-rope) variant was compared. Produced by the same `rope-scoutgpt` A/B cycle (2026-04-19 → 2026-04-22).

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Status

- **Status**: Research artifact — **not** the canonical production ScoutGPT model
- **Canonical model**: [`luxury-lakehouse/scoutgpt`](https://huggingface.co/luxury-lakehouse/scoutgpt)
- **Retained for**: Reproducibility of the learnable-vs-RoPE A/B experiment documented in [`docs/evolve/rope-scoutgpt/SUMMARY.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/evolve/rope-scoutgpt/SUMMARY.md)

## Architecture

ScoutGPT transformer decoder with learnable absolute positional embeddings added to the token embedding at every position (Vaswani et al. 2017 baseline). All other hyperparameters identical to the canonical ScoutGPT.

## Training

- **Dataset**: [`luxury-lakehouse/scoutgpt-training-data`](https://huggingface.co/datasets/luxury-lakehouse/scoutgpt-training-data)
- **Optimiser**: AdamW, cosine LR schedule
- **Hardware**: HF Jobs L40S single-GPU
- **Epochs**: 14 (best-of-14 checkpoint published)
- **Publishing script**: [`scripts/run_rope_scoutgpt_ab.py`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/scripts/run_rope_scoutgpt_ab.py)

Full training metrics in `metrics.json`.

## Artefacts in the repo

- `pytorch_model.bin` — best-of-14-epoch checkpoint
- `metrics.json` — per-epoch train / val loss + ablation comparison snapshot

## Use Cases

- **Ablation baseline**: the comparison point for the `rope` variant
- **Not for downstream inference**: downstream consumers should pull the canonical [`luxury-lakehouse/scoutgpt`](https://huggingface.co/luxury-lakehouse/scoutgpt)

## EU AI Act — Intended Use and Non-Use

This model is **not** intended for, validated for, or supplied to any Annex III §4 use (employment, worker management, access-to-employment decisions). It is a research ablation artefact. Any deployer who wishes to repurpose it for a high-risk use must perform their own conformity assessment.

## License

CC-BY-NC 4.0 (inherited from the training-data licensing).

## Citation

```bibtex
@software{scoutgpt_variant_learnable_2026,
  title={ScoutGPT Learnable Positions Variant},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://huggingface.co/luxury-lakehouse/scoutgpt-variant-learnable}
}
```
