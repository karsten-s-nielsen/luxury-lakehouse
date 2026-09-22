---
language:
  - en
license: cc-by-nc-4.0
library_name: numpy
tags:
  - sports-analytics
  - soccer
  - football
  - player-valuation
  - defender-analytics
  - territorial-dominance
pipeline_tag: other
---

# Territorial Dominance — Per-Defender xT Conceded / Prevented

The per-`(match, defender)` measure of expected threat (xT) a defender concedes vs prevents
inside the convex hull of their own defensive actions, in two methods: **completed_failed**
(v1) and **counterfactual** (threat-prevented-above-expectation). **Deterministic** over an
injected fitted Expected Threat surface and, for the counterfactual method, an injected pass
completion model — no trained weights of its own.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform. Implemented via the
silly-kicks `territory.compute_territorial_dominance` family (4.120.0, TF-54 / TF-54b).

## Method Description

`compute_territorial_dominance` builds the convex hull of a defender's defensive actions
(tackle / interception / clearance) and integrates the injected xT surface over opponent passes
into that hull. The **v1 (completed_failed)** method emits the 12 territory metric columns
(`territory_xt_conceded` / `territory_xt_prevented` / `territory_xt_net` and their forward,
rate, hull-geometry companions) + a `territory_hull_source` provenance. The **counterfactual**
method (TF-54b) adds 5 columns — `territory_expected_threat_faced`,
`territory_xt_prevented_above_expectation`, `territory_passes_aimed_into_hull`,
`territory_mean_completion_faced`, and a `territory_target_source` provenance — by injecting
`PassCompletionModel.bundled()` (the 4.120.0 full-open-data re-fit) to score what a baseline
defender would have conceded. A defender with `<3` defensive actions cannot form a hull and is
emitted as a `territory_hull_source='degenerate'` NaN row (counted). Output grain is one row
per `(match, defender)`; it is **event-only** but requires an injected fitted `ExpectedThreat`.

### Reference

The territorial-dominance / defensive-hull framing follows the Twelve match-report practitioner
vocabulary of David Sumpter / twelve.football (the 'Earpiece' match-report series). The
Expected Threat surface it integrates follows Singh (2018). The counterfactual pass-completion
model is silly-kicks-native.

## Inputs

**No training data** — this is a deterministic aggregate over injected models, not a learned model.

| Input | Source |
|---|---|
| SPADL actions | `{catalog}.bronze.spadl_actions` |
| Fitted Expected Threat surface | fit at the driver over the provider action corpus |
| Pass completion model (counterfactual only) | `PassCompletionModel.bundled()` (silly-kicks) |

## Execution

Materialised by the ADR-013 writer `src/ingestion/territory_writer.py`, which fits an
`ExpectedThreat` once at the driver, broadcasts its grid + transition matrix into the
`applyInPandas` closure, runs BOTH methods per match, emits native `(match_id, player_id,
team_id)`, and lands the 20-column counterfactual union in bronze for dbt to join into the
player-match mart `fct_player_match_metrics`. It is scheduled into the daily job — see
[`workflow-cards/wf-territory.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-territory.yaml).

## Intended Use

- **Defender territorial-dominance reporting**: xT conceded / prevented on the Defender
  Analytics page
- **Ranking**: territory is **ranking-licensed** per the sk defender-ranking census
  (intraclass-correlation lower bound 0.121 > 0, power 1.0), so a per-defender ranking is
  supported — distinct from gk_decision's instrument-not-ranking limit
- **Research**: reproducible territorial-dominance computation on open match data

## EU AI Act — Intended Use and Non-Use

This method is published for **research and reproducibility** purposes on public,
open-licensed match data. It is **not intended for, not validated for, and not supplied
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
match data contains no protected attributes and therefore cannot support the group-fairness
audits required by Article 10(2)(g) without ingesting additional personal data.

This posture is the project's remediation record for the internal audit finding
`SEC-AUDIT-v1.12.0 REG-01`. See the
[`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md)
gap analysis in the source repository for the project's full risk classification,
re-classification triggers, and governance posture.

## Limitations

- **Under-count of off-hull contribution.** The hull is only the defender's OWN defensive
  actions, so territorial dominance undercounts a defender who contributes outside their
  action footprint (positional deterrence with no logged action).
- **Injected-model dependence.** The metric is only as good as the injected xT surface; the
  counterfactual method additionally depends on the injected pass-completion model.
- **Pre-4.120 correlation caveat.** Any PassCompletion correlation cited for the counterfactual
  method is a **pre-4.120** measure — the 4.120.0 full-open-data `PassCompletionModel.bundled()`
  re-fit shifts the counterfactual values, so re-measure against the shipped bundle before
  displaying a correlation number.

## Files

No model weights. The method is implemented in source:

- `src/ingestion/territory_writer.py`

Output is landed in the Delta mart `{catalog}.dev_gold.fct_player_match_metrics`.

## Citation

```bibtex
@software{nielsen2026territory,
  title={Territorial Dominance: Per-Defender xT Conceded / Prevented on Open Match Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) — inherited from Wyscout non-commercial event data in the multi-provider corpus
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-territory.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-territory.yaml)
