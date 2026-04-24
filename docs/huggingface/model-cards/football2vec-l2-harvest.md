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

- `seeds/<iter>/<seed-id>/metrics.json` — per-seed evaluation metrics (val MLM loss, adversary accuracy, baseline delta, wall-clock)
- `seeds/<iter>/<seed-id>/program.py` — the candidate architecture (research-licence)

## Publishing Provenance

Harvest uploads are written by the evolve engine's Football2Vec target backend (`src/evolve/targets/football2vec/evaluator.py`) after each seed's evaluation completes on HF Jobs. See [EV1 documentation](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/evolve/ev1-football2vec/README.md) for the full cycle history.

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
