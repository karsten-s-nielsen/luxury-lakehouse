---
language:
  - en
license: cc-by-nc-4.0
library_name: numpy
tags:
  - sports-analytics
  - soccer
  - football
  - goalkeeper-valuation
  - gk-decision
  - distribution
pipeline_tag: other
---

# GK Decision Value — Distribution Selection

Scores a goalkeeper's build-up DISTRIBUTION DECISIONS as chosen-vs-available: given a GK
distribution action (goalkick / keeper pass), how good was the option the keeper chose versus the
reachable alternatives, per `(match, keeper)`. **An instrument, not a ranking** — see Limitations.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform. Implemented via the
silly-kicks `compute_gk_decision_value` family (4.120.0, TF-62), **reconstruction tier only**.

## Method Description

For each GK distribution action, the option set (the reachable teammates the keeper could have played)
is reconstructed and each option scored for expected value; the chosen option is compared against the
available set. Outputs: `decision_value` (chosen EV minus the counterfactual mean available EV),
`chosen_ev`, `best_ev`, `sel_efficiency` (chosen/best, 0-1), and `decision_pct` (the chosen option's
percentile among available, 0-1, ~0.5 = random).

Two tiers exist; **only the reconstruction tier is built here**:

- **Reconstruction** (`ReconstructedOptionSet`): reconstructs the option set from SB360 freeze-frames
  (+ full-tracking) with the bundled `PassCompletionModel`. Emits `option_set_source = 'reconstructed'`.
- **Native SkillCorner game-intelligence** (`SkillCornerGIOptionSet`): reads SkillCorner GI
  `passing_option` rows. **Not yet sourced** in the lakehouse (no `parse_passing_options` ingestion) — a
  tracked follow-up. When it lands, its rows carry `option_set_source = 'native'`.

Because the reconstruction tier reads tracking frames, it is **tracking-consuming**: scored per work
unit inside the consolidated tracking-marts worker-drain (not a standalone scheduled task).

### Reference

The goalkeeper distribution-value framing draws on the xT-GK (Expected Threat for Goalkeepers)
collaboration (Eyestone) and the pass risk/reward valuation of Power, Ruiz, Wei & Lucey (2017, KDD '17)
for the option expected-pass modelling.

## Inputs

**No training data of its own** — a deterministic scorer over an injected `PassCompletionModel`.

| Input | Source |
|---|---|
| GK distribution actions | `{catalog}.bronze.spadl_actions` |
| Tracking frames per action (SB360 + full-tracking) | `{catalog}.bronze.spadl_action_context` |
| Bundled `PassCompletionModel` (4.120.0 full-open-data re-fit) | silly-kicks wheel |

## Execution

Materialised by the ADR-013 writer `src/ingestion/gk_decision_writer.py`. Its per-unit core
(`score_gk_decision_unit`) is invoked by the tracking-marts drain (P1 Phase E drain-wiring); it emits
native `(match_id, period_id, decision_id, keeper, team_id)` + the 5 metric cols + `n_options` /
`option_set_source`, landed in `bronze.gk_decision` for dbt to resolve into the **per-decision** mart
`fct_gk_decision` (`silly_kicks.compute_gk_decision_value` returns one row per GK decision, `decision_id`
== the SPADL action_id; a per-keeper-match summary is a downstream aggregate). See
[`workflow-cards/wf-gk-decision.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-gk-decision.yaml).

## Intended Use

- **Goalkeeper distribution-decision reporting**: was the chosen option good relative to what was on?
- **Coaching context**: identify systematically sub-optimal or conservative distribution selection
- **Research**: reproducible chosen-vs-available decision scoring on open match data

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

- **An INSTRUMENT, not a ranking.** GK Decision Value tells you whether a given decision beat its
  available alternatives; it is NOT calibrated as a cross-keeper leaderboard. Do not rank keepers by it.
- **Reconstruction-tier correlation to ground truth is only MODERATE, and the cited numbers are
  PRE-4.120.** TF-62 measured ρ≈0.24–0.25 on the WC2022 `PassCompletionModel` bundle (AUC 0.811); the
  4.120.0 bundle this cycle ships is a full-open-data re-fit (all-comp AUC 0.794), so that correlation
  MUST be re-measured against the 4.120.0 bundle before any number is printed here or in the UI. No
  correlation figure is displayed on this card until that re-measurement is done.
- **Reachability floor + FOV.** Options are pruned by a reachability floor (`reachability_min_xpass`,
  default 0.85) and by field-of-view gating on SB360; decisions with too few options or a cropped FOV are
  dropped (and counted in the conservation census), not scored.
- **Native GI tier not yet sourced.** Only reconstruction-tier rows exist today; SkillCorner
  game-intelligence option sets are a tracked follow-up.

## Files

No model weights of its own (the injected `PassCompletionModel` ships bundled in the silly-kicks wheel).
The method is implemented in source:

- `src/ingestion/gk_decision_writer.py`

Output is resolved into the Delta mart `{catalog}.dev_gold.fct_gk_decision`.

## Citation

```bibtex
@software{nielsen2026gkdecision,
  title={GK Decision Value: Chosen-vs-Available Distribution Selection on Open Match Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) — inherited from Wyscout non-commercial event data in the multi-provider corpus
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-gk-decision.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-gk-decision.yaml)
```
