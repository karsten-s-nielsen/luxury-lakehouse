---
language:
  - en
license: cc-by-4.0
library_name: numpy
tags:
  - sports-analytics
  - soccer
  - football
  - space-creation
  - counterfactual
  - tracking-data
  - pitch-control
  - heuristic
datasets:
  - luxury-lakehouse/pitch-control-tracking
  - luxury-lakehouse/obso-trained-grids
  - luxury-lakehouse/space-creation-values
pipeline_tag: other
---

# Space Creation — Per-Player Counterfactual Space-Creation Value

Quantifies each player's contribution to opening valuable space for teammates through off-ball movement. For every tracking frame, pitch control is computed with and without each off-ball player; the EPV-weighted difference in controlled space measures how much valuable territory each player creates through their positioning and movement. **Heuristic counterfactual** following Fernández & Bornn (2018); no trained weights for the per-player attribution.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Method Description

### Algorithm

For each tracking frame:

1. Compute the baseline pitch-control surface with all players present (from `wf-pitch-control`).
2. For each off-ball player `p`:
   - Recompute pitch control with player `p` removed.
   - Compute the EPV-weighted difference between the baseline and the counterfactual surface.
   - The difference is player `p`'s space-creation value at this frame.
3. Aggregate per-player across all frames in a match.

The EPV weighting (from the OBSO trained grids) ensures that space opened in high-value areas (e.g., inside the penalty box) counts more than space opened on one's own goal line.

The implementation uses the "factor-out loop-invariant computation" optimisation: the transition × EPV grid product is a match-level constant, so the counterfactual surfaces differ only in their pitch-control component. This collapses what would be N sequential pitch-control evaluations into a single vectorised NumPy broadcast, cutting GPU runtime by over an order of magnitude.

### Reference

- Fernández, J. & Bornn, L. (2018). **Wide Open Spaces: A Statistical Technique for Measuring Space Creation in Professional Soccer.** MIT Sloan Sports Analytics Conference. <https://www.sloansportsconference.com/research-papers/wide-open-spaces-a-statistical-technique-for-measuring-space-creation-in-professional-soccer>

## Inputs

**No training data** — this is a heuristic counterfactual. Upstream EPV transition and reachability grids are trained, but they are consumed as a static input here.

Runtime inputs:

| Input | Source |
|---|---|
| Tracking frames | [`luxury-lakehouse/pitch-control-tracking`](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) |
| EPV transition × reachability grid | [`luxury-lakehouse/obso-trained-grids`](https://huggingface.co/datasets/luxury-lakehouse/obso-trained-grids) |
| Pitch control surfaces | From `wf-pitch-control` |

## Execution

**Batch computation**: HF Jobs GPU (`l40sx1`), script `scripts/compute_space_creation_hf.py`. Typical duration: 45 minutes for full corpus. Output dataset: [`luxury-lakehouse/space-creation-values`](https://huggingface.co/datasets/luxury-lakehouse/space-creation-values).

See [`workflow-cards/wf-space-creation.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-space-creation.yaml) for the full operational contract.

## Intended Use

- **Off-ball contribution ranking**: Identify players who open the most EPV-weighted space for teammates
- **Tactical profiling**: Surface run patterns and decoy movements that create value without touching the ball
- **Research**: Reproducible implementation of Fernández & Bornn (2018) on open tracking data

## EU AI Act — Intended Use and Non-Use

This method is published for **research and reproducibility** purposes on public, open-licensed tracking data. It is **not intended for, not validated for, and not supplied to** any use that would fall within Annex III §4 (Employment, workers management and access to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of natural persons, decisions affecting work-related contractual relationships, promotion, termination, task allocation based on individual traits, or the monitoring and evaluation of performance and behaviour of workers for employment decisions.

Any deployer who wishes to use this method for such a purpose is responsible for performing their own conformity assessment under Article 43, for drawing up the technical documentation required by Article 11 and Annex IV, for implementing the human oversight measures required by Article 14, for declaring accuracy metrics under Article 15, and for ensuring the data governance obligations of Article 10 are met. Note specifically that the tracking corpus contains no protected attributes and therefore cannot support the group-fairness audits required by Article 10(2)(g) without ingesting additional personal data.

See the [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis in the source repository for the project's full risk classification, re-classification triggers, and governance posture.

## Limitations

- **One-player counterfactual only.** Removing player `p` does not model the defensive reorganisation that their absence would trigger. The metric therefore slightly under-credits players who attract multiple defenders.
- **No ball-trajectory adjustment.** The ball position is held fixed across the counterfactual; in reality, removing an attacker would change the decision space of the ball carrier.
- **Tracking-data dependence.** Event-only matches do not produce space-creation values. Only matches with tracking coverage are processed.
- **Upstream grid dependence.** Inherits biases in the pre-trained EPV transition and reachability grids.
- **Inherits pitch-control limits.** All limitations of pitch control (2-D, no ball physics, no defender intent model) propagate.

## Files

No persisted model weights. The method is implemented in source:

- `scripts/compute_space_creation_hf.py`

Outputs are published as the HF Hub dataset [`luxury-lakehouse/space-creation-values`](https://huggingface.co/datasets/luxury-lakehouse/space-creation-values).

## Citation

```bibtex
@inproceedings{fernandez2018wideopenspaces,
  title={Wide Open Spaces: A Statistical Technique for Measuring Space Creation in Professional Soccer},
  author={Fern{\'a}ndez, Javier and Bornn, Luke},
  booktitle={MIT Sloan Sports Analytics Conference},
  year={2018}
}
```

```bibtex
@software{nielsen2026spacecreation,
  title={Space Creation: Per-Player Counterfactual Space-Creation Value on Open Tracking Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Companion Resources

| Resource | Description |
|---|---|
| [Space Creation Values](https://huggingface.co/datasets/luxury-lakehouse/space-creation-values) | Per-player per-frame space-creation values |
| [OBSO Trained Grids](https://huggingface.co/datasets/luxury-lakehouse/obso-trained-grids) | Upstream EPV grids |
| [Pitch Control](pitch-control.md) | Upstream method card |

## More Information

- **License**: [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/) — inherited from open tracking sources
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-space-creation.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-space-creation.yaml)
