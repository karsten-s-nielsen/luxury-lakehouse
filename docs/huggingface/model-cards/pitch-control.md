---
language:
  - en
license: cc-by-4.0
library_name: numpy
tags:
  - sports-analytics
  - soccer
  - football
  - tracking-data
  - pitch-control
  - physics-model
  - heuristic
datasets:
  - luxury-lakehouse/pitch-control-tracking
pipeline_tag: other
---

# Pitch Control — Physics-Based Team-Control Probability Surface

Computes a per-frame probability surface indicating which team controls each location on the pitch. Physics-based heuristic following Spearman (2017). **No trained weights** — the method is an independent Python translation of the published equations.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform. Pitch Control is the load-bearing substrate for Off-Ball xT, Space Creation, OBSO, and PAUSA.

## Method Description

Pitch Control estimates each player's influence over every grid cell on the pitch using a time-to-intercept (TTI) function parameterised by player position and velocity. The probability that a team controls a cell is the sum of its players' influence values divided by the total influence across both teams, producing a pair of complementary probabilities at every cell (home-team and away-team, summing to 1).

### Algorithm

For each tracking frame:

1. For each player, compute time-to-intercept for every target cell given their current position, velocity vector, and a maximum-acceleration constraint.
2. Convert TTI to a logistic influence function: lower TTI → higher influence; the logistic slope is a model parameter.
3. Sum influence per team; normalise across teams to produce a probability bounded in [0, 1] per cell per team.
4. Output: a 2-D probability surface per frame, one grid per team.

### Reference

- Spearman, W. (2017). **Physics-Based Modeling of Pass Probabilities in Soccer.** MIT Sloan Sports Analytics Conference. <https://www.sloansportsconference.com/research-papers/physics-based-modeling-of-pass-probabilities-in-soccer>

## Inputs

**No training data** — this is a heuristic, not a learned model.

Runtime inputs per frame:

| Field | Source | Description |
|---|---|---|
| Player positions (x, y) | Tracking vendor (Metrica / IDSSE / SkillCorner) | 105×68 m canonical pitch, metric units |
| Player velocities (vx, vy) | Tracking vendor | Differenced positions smoothed over a short window |
| Team attribution | Tracking metadata | `home` / `away` per player |
| Grid resolution | Configuration | Default: 68×52 cells (~1.5 m resolution) |

The canonical tracking corpus used by the project is published as [`luxury-lakehouse/pitch-control-tracking`](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) on HuggingFace Hub and attributed in [`NOTICE`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/NOTICE).

## Execution

Computed daily via Databricks serverless workflow `compute_pitch_control` (module `ingestion.pitch_control_batch`). Distribution: `applyInPandas` grouped by synthetic `frame_batch_id` partitions to stay within the 1 GB UDF memory limit. Output table: `{catalog}.bronze.pitch_control_values`.

**PR 7 (ADR-011 close-out):** writer schema widened with `data_source STRING` + `match_key BIGINT` natively, so the per-row Kimball surrogate FK is emitted at write time rather than re-derived in `stg_pitch_control__values`. Collapses the PR 6 prefix-CASE-then-dim_matches-JOIN bridge to a passthrough.

A batched Numba-accelerated implementation is available for off-line surface generation. Benchmark target: ≤ 5 ms per frame for 22 targets.

See [`workflow-cards/wf-pitch-control.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-pitch-control.yaml) for the full operational contract.

## Intended Use

- **Substrate for downstream analytics**: Off-Ball xT, Space Creation, OBSO, PAUSA
- **Tactical visualisation**: Team-control regions at key moments on the dashboard's Pitch Control page
- **Research**: Reproducible implementation of Spearman's model on open tracking data

## EU AI Act — Intended Use and Non-Use

This method is published for **research and reproducibility** purposes on public, open-licensed tracking data. It is **not intended for, not validated for, and not supplied to** any use that would fall within Annex III §4 (Employment, workers management and access to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of natural persons, decisions affecting work-related contractual relationships, promotion, termination, task allocation based on individual traits, or the monitoring and evaluation of performance and behaviour of workers for employment decisions.

Any deployer who wishes to use this method for such a purpose is responsible for performing their own conformity assessment under Article 43, for drawing up the technical documentation required by Article 11 and Annex IV, for implementing the human oversight measures required by Article 14, for declaring accuracy metrics under Article 15, and for ensuring the data governance obligations of Article 10 are met. Note specifically that this is an unparameterised physics heuristic with no training data — so representativity, bias, and protected-attribute audits are not directly applicable to the method itself, but the downstream player-level metrics derived from it (Off-Ball xT, Space Creation, OBSO, PAUSA) inherit the representativity of the tracking corpus on which they are evaluated.

See the [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis in the source repository for the project's full risk classification, re-classification triggers, and governance posture.

## Limitations

- **Heuristic, not learned.** Parameters (TTI logistic slope, maximum acceleration) are set by published values and have not been fit to any dataset. The method does not adapt to playing style, pitch conditions, or data-source characteristics.
- **No defender intent model.** All players react identically. Defenders with off-ball tracking or engagement intent are not distinguished.
- **Tracking-data quality dependence.** Missing players, skipped frames, or velocity-smoothing artefacts propagate directly into the surface.
- **2-D only.** Aerial play (headers, clearances) is ignored.
- **No ball physics.** Ball trajectory is not explicitly modelled; the surface describes *player* control, not *ball* control.

## Files

No model weights. The method is implemented in source:

- `src/analytics/pitch_control.py`
- `src/ingestion/pitch_control_batch.py`

Outputs are published as the Delta table `{catalog}.bronze.pitch_control_values` and in Parquet form as [`luxury-lakehouse/pitch-control-tracking`](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking).

## Citation

```bibtex
@inproceedings{spearman2017physicsbased,
  title={Physics-Based Modeling of Pass Probabilities in Soccer},
  author={Spearman, William},
  booktitle={MIT Sloan Sports Analytics Conference},
  year={2017}
}
```

```bibtex
@software{nielsen2026pitchcontrol,
  title={Pitch Control: Physics-Based Team-Control Probability Surface on Open Tracking Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Companion Resources

| Resource | Description |
|---|---|
| [Pitch Control Tracking](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) | Canonical tracking corpus formatted for pitch control computation |
| [OBSO Trained Grids](https://huggingface.co/datasets/luxury-lakehouse/obso-trained-grids) | Downstream EPV transition and reachability grids |

## More Information

- **License**: [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/) — inherited from open tracking data sources
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-pitch-control.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-pitch-control.yaml)
- **Demo**: [Soccer Analytics Explorer](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) — visualise pitch-control surfaces on open tracking data
