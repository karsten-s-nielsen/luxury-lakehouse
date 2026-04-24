---
language:
  - en
license: cc-by-nc-4.0
library_name: xgboost
tags:
  - sports-analytics
  - soccer
  - football
  - defensive-valuation
  - credit-assignment
  - xgboost
  - counterfactual
datasets:
  - luxury-lakehouse/spadl-vaep
pipeline_tag: tabular-regression
---

# DEFCON — Defensive Credit Assignment via Counterfactual Value

Assigns defensive credit to individual players by estimating the counterfactual value change caused by each defender's presence at the moment of a defensive action. An XGBoost value estimator is trained inline during each inference run on VAEP action values and spatial features from 360 freeze-frame and tracking data.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform. Implements the DEFCON-lite framework (Kim et al. 2025).

## Method Description

For each defensive action (intercept, tackle, disturb, deter, concede), DEFCON computes the value difference between the observed game state and a counterfactual state where the defender is absent. The value estimator is a gradient-boosted regressor trained on VAEP action values and spatial features (defender-to-ball distance, goal-side angle, pressure indicators) extracted from 360 freeze-frames and tracking data.

### Algorithm

1. **Event-stream classification.** Defensive events are classified into four tiers (intercept/tackle/disturb/deter) and their SPADL context is extracted.
2. **Spatial feature extraction.** For each defensive action, the defender's position relative to the ball, the attacker's trajectory, and the goal-side angle are computed from the 360 freeze-frame or tracking-frame snapshot at event time.
3. **Value estimator training.** An XGBoost regressor is fitted to predict VAEP value from the spatial features, using all defensive-action rows in the current batch.
4. **Counterfactual assignment.** For each defender-action, the value difference `V(observed) - V(counterfactual)` is assigned as the defender's credit for that action.
5. **Aggregation.** Per-player credit is summed across a match for reporting.

### Reference

- Kim, H. S., Bierens, N., Sofie Van Roy, Dossche, M., Schulze, F., Bransen, L., Davis, J. (2025). **Better Prevent than Tackle: Valuing Defense in Soccer Based on Graph Neural Networks.** arXiv:2512.10355. <https://arxiv.org/abs/2512.10355>

This project implements the published DEFCON-lite heuristic (tier classification + spatial XGBoost regressor), not the graph-neural-network variant from the same paper. The implementation is an independent Python translation of the published equations, not derived from the authors' source code.

## Inputs

Inline training: the XGBoost value estimator is trained from scratch on each inference run, using the following inputs for every defensive action in the current batch:

| Field | Source | Description |
|---|---|---|
| VAEP action value | [`luxury-lakehouse/spadl-vaep`](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep) — `fct_action_values` | Per-action offensive/defensive value delta |
| Defender position (x, y) | StatsBomb 360 freeze-frames + tracking data | Pitch-relative coordinates |
| Ball-to-defender distance | Computed | Derived at event time |
| Goal-side angle | Computed | Angle between defender and the goal line |
| Tier classification | Event-stream derivation | `intercept` / `tackle` / `disturb` / `deter` |

**No external model weights** — each inference run produces a fresh XGBoost regressor scoped to the batch of matches being processed.

## Execution

Daily Databricks serverless workflow `compute_defcon_lite` (module `ingestion.defcon_lite`). Distribution: `applyInPandas` grouped by `match_id` with partition-level idempotency keyed by `(match_id, data_source)`. Output table: `{catalog}.bronze.defcon_results`.

See [`workflow-cards/wf-defcon.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-defcon.yaml) for the full operational contract.

## Intended Use

- **Defensive-value reporting**: Per-player defensive contribution on the dashboard's Defensive Impact page
- **Tactical analysis**: Identifying which defenders generate the most counterfactual value under different pressing systems
- **Research**: Reproducible DEFCON-lite implementation on open event + freeze-frame data

## EU AI Act — Intended Use and Non-Use

This method is published for **research and reproducibility** purposes on public, open-licensed match data. It is **not intended for, not validated for, and not supplied to** any use that would fall within Annex III §4 (Employment, workers management and access to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of natural persons, decisions affecting work-related contractual relationships, promotion, termination, task allocation based on individual traits, or the monitoring and evaluation of performance and behaviour of workers for employment decisions.

Any deployer who wishes to use this method for such a purpose is responsible for performing their own conformity assessment under Article 43, for drawing up the technical documentation required by Article 11 and Annex IV, for implementing the human oversight measures required by Article 14, for declaring accuracy metrics under Article 15, and for ensuring the data governance obligations of Article 10 are met. Note specifically that the training data contains no protected attributes and therefore cannot support the group-fairness audits required by Article 10(2)(g) without ingesting additional personal data.

See the [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis in the source repository for the project's full risk classification, re-classification triggers, and governance posture.

## Limitations

- **Inline training**: The value estimator is re-fit on every inference run, which means calibration drifts across runs as the underlying data volume grows. The dashboard surfaces per-run credit values, not a longitudinally-comparable metric.
- **No permanent weights**: This card does not publish XGBoost weights because they are a per-run artefact. The trained booster is held only in Delta alongside the results.
- **Event-defender matching**: Spatial features require that the defender identified in the event stream be locatable in the 360 freeze-frame or tracking frame at event time. Matches without tracking or 360 coverage fall back to event-only features, reducing discrimination.
- **Tier classification is heuristic**: The four-tier taxonomy is rule-based (following Kim et al. 2025 Tier 1-3) and does not adapt to idiosyncratic coaching vocabulary or league-specific refereeing conventions.
- **Not the full DEFCON GNN**: The published paper describes a graph-neural-network variant with richer temporal context; this implementation is the "lite" heuristic alternative.

## Files

No persisted model weights. The method is implemented in source:

- `src/ingestion/defcon_lite.py`
- `src/ingestion/defcon_lite_common.py`

Per-run XGBoost boosters live transiently in `_model_cache` on executors. Outputs are persisted as the Delta table `{catalog}.bronze.defcon_results`.

## Citation

```bibtex
@article{kim2025defcon,
  title={Better Prevent than Tackle: Valuing Defense in Soccer Based on Graph Neural Networks},
  author={Kim, Hyun Sung and Bierens, Nathan and Van Roy, Sofie and Dossche, Manu and Schulze, Friedrich and Bransen, Lotte and Davis, Jesse},
  journal={arXiv preprint arXiv:2512.10355},
  year={2025}
}
```

```bibtex
@software{nielsen2026defcon,
  title={DEFCON-lite: Defensive Credit Assignment on Open Event Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) — inherited from Wyscout training data via the upstream VAEP values
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-defcon.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-defcon.yaml)
- **Reference implementation (paper authors)**: <https://github.com/hyunsungkim-ds/defcon> (Apache License 2.0)
