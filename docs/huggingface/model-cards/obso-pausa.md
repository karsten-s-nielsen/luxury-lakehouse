---
language:
  - en
license: cc-by-4.0
library_name: numpy
tags:
  - sports-analytics
  - soccer
  - football
  - pass-timing
  - obso
  - pausa
  - expected-possession-value
  - tracking-data
datasets:
  - luxury-lakehouse/obso-pausa-inputs
  - luxury-lakehouse/obso-pausa-values
  - luxury-lakehouse/obso-trained-grids
pipeline_tag: other
---

# OBSO + PAUSA — Off-Ball Scoring Opportunity and Pass Timing

Computes two related tracking-based metrics on a GPU-accelerated pipeline: (1) **OBSO** (Off-Ball Scoring Opportunity) surfaces, quantifying the scoring value of space regardless of ball location, and (2) **PAUSA** (pass timing), measuring whether passes are released at the optimal moment by comparing actual pass timestamps to counterfactual alternatives. **Heuristic combination** of three published models (Spearman 2018, Fernández & Bornn 2018, Lee et al. 2026); no trained weights for the inference step.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Method Description

### OBSO

OBSO combines three pre-computed ingredients:

1. **Pitch control** — probability that the attacking team controls each pitch cell (from `wf-pitch-control`).
2. **Ball transition probability** — probability that the ball reaches each cell given its current position (pre-trained grid on HF Hub: [`luxury-lakehouse/obso-trained-grids`](https://huggingface.co/datasets/luxury-lakehouse/obso-trained-grids)).
3. **Expected possession value (EPV)** — probability that possession at a given cell leads to a goal (pre-trained grid on HF Hub).

For every frame and every pitch cell, `OBSO(cell) = PitchControl(cell) × Transition(cell) × EPV(cell)`. The surface is computed on GPU via HF Jobs `l40sx1` for the full match in a single pass using the "factor-out loop-invariant computation" optimisation (the transition × EPV product is a match-level constant, computed once outside the per-frame loop).

### PAUSA

PAUSA extends OBSO to pass-timing analysis. For each actual pass, it computes OBSO surfaces at the moment of release and at a set of counterfactual timestamps before and after. The metric is `PAUSA = max(OBSO at alternative timestamps) − OBSO at actual timestamp`: positive values mean the passer could have waited for a better moment; zero means the timing was optimal; negative means the passer got lucky or made a value-increasing early decision.

### References

- Spearman, W. (2018). **Beyond Expected Goals.** MIT Sloan Sports Analytics Conference. <https://www.sloansportsconference.com/research-papers/beyond-expected-goals>
- Fernández, J. & Bornn, L. (2018). **Wide Open Spaces: A Statistical Technique for Measuring Space Creation in Professional Soccer.** MIT Sloan Sports Analytics Conference.
- Lee, M., Jo, S., Hong, S., Bauer, P., & Ko, Y. (2026). **Valuing La Pausa: Quantifying Optimal Pass Timing Beyond Speed.** MIT Sloan Sports Analytics Conference 2026.

The implementation is an independent Python translation of the published equations; PAUSA is based on the reference code at <https://github.com/leemingo/mitssac-pausa> (Apache License 2.0), with adaptations for the (Right! Luxury!) Lakehouse tracking schema.

## Inputs

**No training data for the inference step.** Upstream EPV transition and reachability grids ARE trained, but they are published separately as [`luxury-lakehouse/obso-trained-grids`](https://huggingface.co/datasets/luxury-lakehouse/obso-trained-grids) and consumed as a static input here.

Runtime inputs:

| Input | Source |
|---|---|
| Tracking frames (player + ball positions, velocities) | [`luxury-lakehouse/pitch-control-tracking`](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) |
| Pitch control surfaces | From `wf-pitch-control` |
| EPV transition grid | [`luxury-lakehouse/obso-trained-grids`](https://huggingface.co/datasets/luxury-lakehouse/obso-trained-grids) |
| EPV reachability grid | Same as above |
| Pass events (for PAUSA) | [`luxury-lakehouse/obso-pausa-inputs`](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-inputs) |

## Execution

**OBSO surface computation**: HF Jobs GPU (`l40sx1`), script `scripts/compute_obso_hf.py`. Typical duration: 60 minutes for full corpus, ≈$1.50 per run.

**PAUSA inference**: Daily Databricks serverless workflow `compute_pausa` (module `ingestion.pausa`). Distribution: `applyInPandas` grouped by `match_id`. Output table: `{catalog}.dev_gold.fct_pausa_values`. Output dataset: [`luxury-lakehouse/obso-pausa-values`](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-values).

See [`workflow-cards/wf-obso-pausa.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-obso-pausa.yaml) for the full operational contract.

## Intended Use

- **Pass-timing analysis**: Per-passer metric on the dashboard's Pass Timing page, identifying players who consistently find the optimal release moment
- **Tactical profiling**: Team-level PAUSA distributions to compare passing tempo across opponents
- **Research**: Reproducible OBSO + PAUSA implementation on open tracking and event data

## EU AI Act — Intended Use and Non-Use

This method is published for **research and reproducibility** purposes on public, open-licensed tracking and event data. It is **not intended for, not validated for, and not supplied to** any use that would fall within Annex III §4 (Employment, workers management and access to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of natural persons, decisions affecting work-related contractual relationships, promotion, termination, task allocation based on individual traits, or the monitoring and evaluation of performance and behaviour of workers for employment decisions.

Any deployer who wishes to use this method for such a purpose is responsible for performing their own conformity assessment under Article 43, for drawing up the technical documentation required by Article 11 and Annex IV, for implementing the human oversight measures required by Article 14, for declaring accuracy metrics under Article 15, and for ensuring the data governance obligations of Article 10 are met. Note specifically that the training data contains no protected attributes and therefore cannot support the group-fairness audits required by Article 10(2)(g) without ingesting additional personal data.

See the [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis in the source repository for the project's full risk classification, re-classification triggers, and governance posture.

## Limitations

- **Upstream grid dependence.** OBSO outputs inherit any bias in the pre-trained EPV transition and reachability grids (see [`luxury-lakehouse/obso-trained-grids`](https://huggingface.co/datasets/luxury-lakehouse/obso-trained-grids) dataset card).
- **Counterfactual timing assumption**: PAUSA compares the actual pass timestamp against ±N frame offsets. It assumes the surrounding trajectory would have unfolded identically at the counterfactual timestamp, which over-credits early/late decisions that would have reshaped play.
- **Coverage gap**: PAUSA is computed only on matches with both event and tracking data at the pass moment.
- **No intent model**: A "badly timed" pass may have been the only option given pressing pressure; PAUSA does not distinguish sub-optimal timing from risk management.
- **Inherits pitch-control limits.** All limitations of pitch control (2-D, no ball physics, no defender intent model) propagate.

## Files

No persisted model weights. The method is implemented in source:

- `src/analytics/obso.py`
- `src/ingestion/pausa.py`
- `scripts/compute_obso_hf.py`

Outputs are published as the Delta table `{catalog}.dev_gold.fct_pausa_values` and as the HF Hub dataset [`luxury-lakehouse/obso-pausa-values`](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-values).

## Citation

```bibtex
@inproceedings{spearman2018beyond,
  title={Beyond Expected Goals},
  author={Spearman, William},
  booktitle={MIT Sloan Sports Analytics Conference},
  year={2018}
}
```

```bibtex
@inproceedings{fernandez2018wideopenspaces,
  title={Wide Open Spaces: A Statistical Technique for Measuring Space Creation in Professional Soccer},
  author={Fern{\'a}ndez, Javier and Bornn, Luke},
  booktitle={MIT Sloan Sports Analytics Conference},
  year={2018}
}
```

```bibtex
@inproceedings{lee2026pausa,
  title={Valuing La Pausa: Quantifying Optimal Pass Timing Beyond Speed},
  author={Lee, Mingo and Jo, Sangho and Hong, Seungwoo and Bauer, Paul and Ko, Youngwoong},
  booktitle={MIT Sloan Sports Analytics Conference},
  year={2026}
}
```

```bibtex
@software{nielsen2026obsopausa,
  title={OBSO + PAUSA: Off-Ball Scoring Opportunity and Pass Timing on Open Tracking Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Companion Resources

| Resource | Description |
|---|---|
| [OBSO + PAUSA Inputs](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-inputs) | Pre-processed pass events used for PAUSA |
| [OBSO + PAUSA Values](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-values) | Computed OBSO surfaces and PAUSA timing values |
| [OBSO Trained Grids](https://huggingface.co/datasets/luxury-lakehouse/obso-trained-grids) | Pre-trained EPV transition and reachability grids |
| [Pitch Control](pitch-control.md) | Upstream method card |

## More Information

- **License**: [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/) — inherited from open tracking sources
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-obso-pausa.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-obso-pausa.yaml)
- **Reference implementation (PAUSA authors)**: <https://github.com/leemingo/mitssac-pausa> (Apache License 2.0)
