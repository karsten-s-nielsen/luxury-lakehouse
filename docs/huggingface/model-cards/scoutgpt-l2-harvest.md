---
language:
  - en
license: cc-by-nc-4.0
library_name: pytorch
tags:
  - sports-analytics
  - soccer
  - football
  - transformer
  - scoutgpt
  - evolve
  - l2-harvest
  - research-artifact
datasets:
  - luxury-lakehouse/scoutgpt-training-data
pipeline_tag: feature-extraction
---

# ScoutGPT — L2 Harvest (Research Artifact)

Repository collecting seed-program evaluation outputs from the [OpenEvolve](https://github.com/codelion/openevolve) Level-2 architecture-evolution cycles on the [ScoutGPT](https://huggingface.co/luxury-lakehouse/scoutgpt) target. Each harvest run uploads a `metrics.json` that captures a single seed program's evaluated fitness against the production ScoutGPT baseline.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Status

- **Status**: Research artifact — L2 evolve harvest outputs, **not** a trained deployable model
- **Canonical model**: [`luxury-lakehouse/scoutgpt`](https://huggingface.co/luxury-lakehouse/scoutgpt)
- **Retained for**: Audit trail of evolve-engine seed evaluations; reproduction of promotion decisions

## What L2 Harvesting Is

The evolve engine's Level-2 mode proposes new architectural vocabularies (e.g., custom attention kernels, position-encoding variants, adversary schedules) as Python programs. Each proposal is "harvested" by evaluating it against a held-out validation set and logging the resulting metrics. Seeds that beat the baseline by a pre-registered threshold are candidates for promotion to a full training run; the rest are archived here for transparency.

## Repo Layout

- `seeds/<iter>/<seed-id>/metrics.json` — per-seed evaluation metrics (val loss, val accuracy, baseline delta, wall-clock)
- `seeds/<iter>/<seed-id>/program.py` — the candidate architecture (research-licence, see below)

## Publishing Script

[`scripts/evaluate_scoutgpt_l2_seeds.py`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/scripts/evaluate_scoutgpt_l2_seeds.py) — reads a batch of proposed seeds from the evolve engine, evaluates each on HF Jobs L40S GPUs, and uploads the per-seed `metrics.json` to this repo.

## Use Cases

- **Evolve-engine audit**: trace which seed programs were evaluated, their scores, and when the promotion threshold was met
- **Architecture research**: mine the seed programs for architectural ideas that scored well but weren't promoted (e.g., sub-threshold improvements that aggregate interestingly)

## EU AI Act — Intended Use and Non-Use

This repository contains **research evaluations of proposed model architectures**, not a trained deployable model. It is not intended for, validated for, or supplied to any Annex III §4 use. Any deployer who wishes to fine-tune or train a promoted seed into a production model must perform their own conformity assessment.

## License

- **Metrics + seed programs**: CC-BY-NC 4.0 for evaluation artefacts (inherited from training-data licensing)
- **Seed program Python source**: research-use licensed, not redistributable as a standalone library

## Citation

```bibtex
@software{scoutgpt_l2_harvest_2026,
  title={ScoutGPT L2 Evolve Harvest},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://huggingface.co/luxury-lakehouse/scoutgpt-l2-harvest}
}
```
