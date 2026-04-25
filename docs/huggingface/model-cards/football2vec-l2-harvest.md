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
  - football2vec
  - evolve
  - l2-harvest
  - research-artifact
datasets:
  - luxury-lakehouse/football2vec-training-data
pipeline_tag: feature-extraction
---

# Football2Vec — L2 Harvest (Research Artifact)

Repository collecting seed-program evaluation outputs from the [OpenEvolve](https://github.com/codelion/openevolve) Level-2 architecture-evolution cycles on the [Football2Vec v2](https://huggingface.co/luxury-lakehouse/football2vec-v2) target. Each harvest run uploads a `metrics.json` capturing a candidate architecture's evaluated fitness against the production Football2Vec v2 baseline.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Status

- **Status**: Research artifact — L2 evolve harvest outputs, **not** a trained deployable model
- **Canonical model**: [`luxury-lakehouse/football2vec-v2`](https://huggingface.co/luxury-lakehouse/football2vec-v2)
- **Retained for**: Audit trail of evolve-engine seed evaluations; reproduction of promotion decisions

## What L2 Harvesting Is

The evolve engine's Level-2 mode proposes new architectural vocabularies (custom attention kernels, position encodings, adversary schedules, etc.) as Python programs. Each proposal is evaluated on HF Jobs L40S GPUs and the resulting metrics are uploaded to this repo for audit. Seeds that beat the baseline by a pre-registered threshold are candidates for promotion to a full training run; the rest are archived here for transparency.

## Repo Layout

- `<variant-name>/metrics.json` — per-variant evaluation metrics (val MLM loss, adversary accuracy, debias score, mlm score, fitness, param count, wall-clock). Variant names match the seed program filename (e.g. `cross_attention_adversary/metrics.json` from `cross_attention_adversary.py`).
- `results.json` — combined harvest result with shared_config provenance, dataset SHA, stage-1 SHA, fitness formula, baseline `L_0`, and the full sorted variant list. Overwritten by each orchestrator run; the per-variant files are the authoritative per-variant records.

## Publishing Provenance

Harvest uploads are written by `scripts/evaluate_football2vec_l2_adversary_seeds.py` (the multi-backend orchestrator) after each seed's evaluation completes on the configured compute backend (AI-PC local CUDA, Media-PC or DGX Spark via SSH, or HF Jobs L40S). The README itself (this card) is pushed separately via `scripts/publish_hf_cards.py --name football2vec-l2-harvest.md --kind model` per [ADR-014](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/superpowers/adrs/ADR-014-hf-card-inventory-parity.md) — an orphan card with no training-script publisher. See the EV2 cycle documentation at [`docs/evolve/ev2-football2vec-l2-adversarial/SUMMARY.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/evolve/ev2-football2vec-l2-adversarial/SUMMARY.md) and `docs/engineering/orchestration.md` for the debugging history and the rules that govern future cycles.

## EV2 Phase 1 — Adversary Architecture Harvest (2026-04-23)

First L2 cycle for Football2Vec v2, targeting the adversary-architecture axis. Six architecturally distinct adversary seeds + the linear baseline, all on stage-1 SHA `bf102a57c9575cbfddf7661ba7a3ebe29de3c124` and dataset SHA `5eb1bfc3be549c56fc1256936aa53fd7f2393d8f`. Fitness formula: `0.4 × mlm_score + 0.6 × debias_score`. Baseline `L_0 = 0.7413`. WIN threshold: fitness ≥ baseline + 0.02 = 0.960.

| Variant | val_mlm | val_adv_acc | debias | mlm_score | fitness | Disposition |
|---|---|---|---|---|---|---|
| `linear` (baseline) | 0.7413 | 0.1411 | 0.900 | 1.000 | **0.940** | baseline |
| `cross_attention_adversary` | 0.8817 | 0.0973 | **0.946** | 0.841 | 0.904 | ARCHIVE |
| `residual_mlp` | 0.7491 | 0.2116 | 0.826 | 0.990 | 0.891 | ARCHIVE |
| `deep_mlp_3layer` | 0.7732 | 0.2116 | 0.826 | 0.959 | 0.879 | ARCHIVE |
| `dual_head_ensemble` | 0.7802 | 0.2116 | 0.826 | 0.950 | 0.876 | ARCHIVE |
| `deep_mlp_2layer` | 0.7839 | 0.2116 | 0.826 | 0.946 | 0.874 | ARCHIVE |
| `attention_pool_head` | 0.9479 (ep6/30) | 0.1796 (ep6/30) | 0.859 (ep6/30) | 0.782 (ep6/30) | 0.828 (ep6/30) | ARCHIVE — interrupted |

**Outcome:** no promotions. The linear adversary baseline Pareto-dominates or ties every tested architecture on the combined fitness metric. `cross_attention_adversary` is the single variant that improves debias (0.946 vs baseline 0.900) AND reduces `val_adv_accuracy` (0.0973 vs 0.1411), but pays an MLM cost that drags net fitness below threshold. Flagged for possible Phase 2 mechanism probe.

**`attention_pool_head` disposition.** Phase 1f on AI-PC (`LocalCudaBackend`, no wall-clock timeout) was interrupted by an unapproved Windows auto-restart at Epoch 6/30 (2026-04-24 11:50, PID 71172 killed mid-training; no checkpoint/resume wired). The Epoch 1-6 trajectory shows no convergence toward the WIN threshold: `val_mlm` oscillates 0.92-1.07 with no downward trend toward the 0.7413 baseline, and `val_adv_acc` oscillates 0.01-0.20 without settling at a low-leakage equilibrium. Interim fitness 0.828 (Epoch 6 snapshot) is below every completed non-baseline variant. **ARCHIVE by trajectory**; no re-run (~16h of additional compute for a confirmed-ARCHIVE result). Full per-epoch log preserved in-repo at [`docs/evolve/ev2-football2vec-l2-adversarial/phase1f.log`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/evolve/ev2-football2vec-l2-adversarial/phase1f.log).

Full narrative (including the Phase 1a-1e orchestration debugging that consumed ~4 h of active investigation across 5 sequential re-fires) in [`SUMMARY.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/evolve/ev2-football2vec-l2-adversarial/SUMMARY.md).

## Use Cases

- **Evolve-engine audit**: trace which seed programs were evaluated, their scores, and when the promotion threshold was met
- **Architecture research**: mine the seed programs for architectural ideas that scored well but weren't promoted

## EU AI Act — Intended Use and Non-Use

This repository contains **research evaluations of proposed model architectures**, not a trained deployable model. It is not intended for, validated for, or supplied to any Annex III §4 use. Any deployer who wishes to fine-tune or train a promoted seed into a production model must perform their own conformity assessment.

## License

- **Metrics + seed programs**: CC-BY-NC 4.0 (inherited from training-data licensing)
- **Seed program Python source**: research-use licensed, not redistributable as a standalone library

## Citation

```bibtex
@software{football2vec_l2_harvest_2026,
  title={Football2Vec L2 Evolve Harvest},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://huggingface.co/luxury-lakehouse/football2vec-l2-harvest}
}
```
