---
language:
  - en
license: cc-by-4.0
library_name: numpy
tags:
  - sports-analytics
  - soccer
  - football
  - passing-valuation
  - packing
  - action-context
  - tracking-data
pipeline_tag: other
---

# Packing — Bypassed-Defender Valuation

Quantifies the number of opposing defenders a pass or carry takes out of the game
("bypasses") between the ball's origin and destination. A per-action attacking-progression
metric that originated commercially as **Packing** at IMPECT GmbH (Reinartz & Hegeler, c. 2015)
and was formalized academically by Goes, Kempe, Meerhoff & Lemmink (2019). **Deterministic**
frame-geometry computation — no trained weights.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform. Implemented via the
silly-kicks action-context pipeline (4.87.0).

## Method Description

For each on-ball action, packing counts the opposing defenders positioned in the geometric
region between the ball's start and end locations relative to the defending goal.
`packing_net` is the net count of defenders taken out of play by the action. The
computation is deterministic and depends on the resolved tracking frame at the moment of
the action — no learned parameters.

### Reference

Per silly-kicks' `NOTICE` (the canonical bibliography for the implementation), packing has two
attributed sources:

- Reinartz, S., & Hegeler, J. (c. 2015). **"Packing"** (Impect GmbH). — the commercial-origin practitioner concept: opponents removed from the defensive phase by a completed pass or carry (1 point per bypassed opponent).
- Goes, F. R., Kempe, M., Meerhoff, L. A., & Lemmink, K. A. P. M. (2019). **"Not Every Pass Can Be an Assist: A Data-Driven Model to Measure Pass Effectiveness in Professional Soccer Matches."** *Big Data*, 7(1). doi:10.1089/big.2018.0067. — the peer-reviewed longitudinal outplayed-defender formalization (`start_x < d_x <= end_x`) this implementation realizes.

This is an independent, deterministic frame-geometry computation in the silly-kicks toolkit; the
net-packing direction multipliers additionally follow Varadharajan's open-source `football-packing`.

## Inputs

**No training data** — this is a deterministic metric, not a learned model.

| Input | Source |
|---|---|
| SPADL action geometry (start/end) | `{catalog}.bronze.spadl_actions` |
| Resolved tracking frame (defender positions) | `{catalog}.bronze.spadl_action_context` |

## Execution

Computed as a drain-native column of the action-context pipeline (module
`src/analytics/action_context/enrich.py`), materialised per-action in
`{catalog}.bronze.spadl_action_context` and surfaced in `dev_gold.fct_action_context`.
The operational contract is owned by the action-context drain — see
[`workflow-cards/wf-packing.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-packing.yaml)
and `wf-action-context.yaml`.

## Intended Use

- **Progression analysis**: per-action attacking progression on the dashboard
- **Tactical profiling**: identify players who consistently bypass defensive lines
- **Research**: reproducible packing computation on open tracking data

## EU AI Act — Intended Use and Non-Use

This method is published for **research and reproducibility** purposes on public,
open-licensed tracking data. It is **not intended for, not validated for, and not supplied
to** any use that would fall within Annex III §4 (Employment, workers management and access
to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of
natural persons, decisions affecting work-related contractual relationships, promotion,
termination, task allocation based on individual traits, or the monitoring and evaluation
of performance and behaviour of workers for employment decisions.

Any deployer who wishes to use this method for such a purpose is responsible for performing
their own conformity assessment under Article 43, for drawing up the technical
documentation required by Article 11 and Annex IV, for implementing the human oversight
measures required by Article 14, for declaring accuracy metrics under Article 15, and for
ensuring the data governance obligations of Article 10 are met. Note specifically that the
tracking corpus contains no protected attributes and therefore cannot support the
group-fairness audits required by Article 10(2)(g) without ingesting additional personal
data.

This posture is the project's remediation record for the internal audit finding
`SEC-AUDIT-v1.12.0 REG-01`. See the
[`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md)
gap analysis in the source repository for the project's full risk classification,
re-classification triggers, and governance posture.

## Limitations

- **Deterministic geometry.** Packing counts defenders in a geometric region; it does not
  model defender intent, recovery ability, or whether a bypassed defender was actually
  relevant to the play.
- **Tracking-data dependence.** Requires a resolved tracking frame; event-only matches do
  not receive packing.
- **No difficulty weighting.** All bypassed defenders count equally regardless of the
  difficulty of the pass or the danger of the space entered.

## Files

No model weights. The method is implemented in source:

- `src/analytics/action_context/enrich.py`

Output is the per-action column `packing_net` in `{catalog}.bronze.spadl_action_context`,
surfaced in `{catalog}.dev_gold.fct_action_context`.

## Citation

```bibtex
@article{goes2019notevery,
  title={Not Every Pass Can Be an Assist: A Data-Driven Model to Measure Pass Effectiveness in Professional Soccer Matches},
  author={Goes, Floris R. and Kempe, Matthias and Meerhoff, Laurentius A. and Lemmink, Koen A. P. M.},
  journal={Big Data},
  volume={7},
  number={1},
  year={2019},
  doi={10.1089/big.2018.0067}
}
```

```bibtex
@software{nielsen2026packing,
  title={Packing: Bypassed-Defender Valuation on Open Tracking Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## More Information

- **License**: [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/) — inherited from open tracking sources
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-packing.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-packing.yaml)
