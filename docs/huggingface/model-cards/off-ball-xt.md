---
language:
  - en
license: cc-by-4.0
library_name: numpy
tags:
  - sports-analytics
  - soccer
  - football
  - expected-threat
  - off-ball
  - tracking-data
  - heuristic
datasets:
  - luxury-lakehouse/pitch-control-tracking
  - luxury-lakehouse/expected-threat-grids
pipeline_tag: other
---

# Off-Ball xT — Off-Ball Expected Threat Attribution

Attributes expected-threat (xT) value to off-ball players based on their positioning relative to the xT surface, weighted by pitch control. Quantifies each player's spatial contribution to the team's attacking threat even when they are not directly involved in the play. **Heuristic combination** of Karun Singh's xT (2018) and Spearman's Pitch Control (2017); no trained weights.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Method Description

For each tracking frame, the xT grid value at each off-ball player's position is combined with a pitch-control weighting to produce an attributed off-ball-xT score per player. Players who position themselves in high-value areas while maintaining receivability (pitch-control advantage) earn higher off-ball-xT credits.

### Algorithm

For each frame:

1. Retrieve the pre-computed xT grid value at every pitch cell.
2. Compute pitch control at every cell (from `wf-pitch-control`).
3. For each off-ball attacker, look up the grid cell containing their position.
4. Their off-ball-xT contribution is `xT(cell) × PitchControl(cell, attacker_team)`.
5. Aggregate per-player per-frame into a running total.

### References

- Singh, K. (2018). **Introducing Expected Threat (xT).** <https://karun.in/blog/expected-threat.html>
- Spearman, W. (2017). **Physics-Based Modeling of Pass Probabilities in Soccer.** MIT Sloan Sports Analytics Conference.

The xT grid itself is a data-driven 12×8-to-68×52 surface computed from 2.2M SPADL actions in the (Right! Luxury!) Lakehouse pipeline, seeded from the published Karun Singh values. The pitch-control component is described in [`pitch-control.md`](pitch-control.md).

## Inputs

**No training data** — this is a heuristic built on two pre-computed inputs.

Runtime inputs per frame:

| Field | Source |
|---|---|
| xT grid values | `{catalog}.bronze.expected_threat_grids` (also published as [`luxury-lakehouse/expected-threat-grids`](https://huggingface.co/datasets/luxury-lakehouse/expected-threat-grids)) |
| Pitch control surface | From `wf-pitch-control` (see [`pitch-control.md`](pitch-control.md)) |
| Player positions (x, y) | Tracking vendor (Metrica / IDSSE / SkillCorner) |
| Team attribution | Tracking metadata |
| Off-ball filter | Players not holding the ball in the current frame |

## Execution

Daily Databricks serverless workflow `compute_off_ball_xt` (module `ingestion.off_ball_xt`). Distribution: `applyInPandas` grouped by synthetic frame-batch partitions. Output table: `{catalog}.bronze.off_ball_xt_results`.

See [`workflow-cards/wf-off-ball-xt.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-off-ball-xt.yaml) for the full operational contract.

Benchmark target: off-ball xT frame computation ≤ 5 ms for 22 targets.

## Intended Use

- **Off-ball contribution analysis**: Surface players whose positioning generates attacking value beyond direct ball involvement
- **Tactical profiling**: Identify overlap runs, channel threats, and half-space occupancy quantitatively
- **Research**: Reproducible off-ball-xT implementation combining open xT grids and physics-based pitch control

## EU AI Act — Intended Use and Non-Use

This method is published for **research and reproducibility** purposes on public, open-licensed tracking data. It is **not intended for, not validated for, and not supplied to** any use that would fall within Annex III §4 (Employment, workers management and access to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of natural persons, decisions affecting work-related contractual relationships, promotion, termination, task allocation based on individual traits, or the monitoring and evaluation of performance and behaviour of workers for employment decisions.

Any deployer who wishes to use this method for such a purpose is responsible for performing their own conformity assessment under Article 43, for drawing up the technical documentation required by Article 11 and Annex IV, for implementing the human oversight measures required by Article 14, for declaring accuracy metrics under Article 15, and for ensuring the data governance obligations of Article 10 are met. Note specifically that the tracking corpus contains no protected attributes and therefore cannot support the group-fairness audits required by Article 10(2)(g) without ingesting additional personal data.

See the [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis in the source repository for the project's full risk classification, re-classification triggers, and governance posture.

## Limitations

- **Heuristic multiplication.** Off-ball-xT is `xT × pitch_control` — not a probabilistic credit assignment. It does not account for sequence-level receivability or teammate spacing effects.
- **Grid resolution.** The xT grid is defined at ~1.5 m resolution; fine spatial distinctions (inside-vs-outside the six-yard box) are washed out at the grid edge.
- **Tracking-data dependence.** Matches without tracking data do not get off-ball-xT attribution. Event-only matches fall through to zero.
- **No temporal weighting.** Every frame contributes equally; players who maintain a threatening position for 5 seconds and for 0.5 seconds receive additive credit in proportion to frame count, which may not match how coaches value sustained vs. fleeting threats.
- **Inherits pitch-control limits.** All limitations of pitch control (2-D, no ball physics, no defender intent model) propagate.

## Files

No model weights. The method is implemented in source:

- `src/analytics/off_ball_xt.py`
- `src/ingestion/off_ball_xt.py`

Outputs are persisted as the Delta table `{catalog}.bronze.off_ball_xt_results`.

## Citation

```bibtex
@misc{singh2018expectedthreat,
  title={Introducing Expected Threat (xT)},
  author={Singh, Karun},
  year={2018},
  howpublished={\url{https://karun.in/blog/expected-threat.html}}
}
```

```bibtex
@inproceedings{spearman2017physicsbased,
  title={Physics-Based Modeling of Pass Probabilities in Soccer},
  author={Spearman, William},
  booktitle={MIT Sloan Sports Analytics Conference},
  year={2017}
}
```

```bibtex
@software{nielsen2026offballxt,
  title={Off-Ball Expected Threat: Attribution via Pitch Control and xT on Open Tracking Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Companion Resources

| Resource | Description |
|---|---|
| [Pitch Control](pitch-control.md) | Upstream method card |
| [Expected Threat Grids](https://huggingface.co/datasets/luxury-lakehouse/expected-threat-grids) | Data-driven xT surface used as input |
| [Pitch Control Tracking](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) | Tracking corpus |

## More Information

- **License**: [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/) — inherited from open tracking sources
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-off-ball-xt.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-off-ball-xt.yaml)
