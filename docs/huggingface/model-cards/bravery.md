---
language:
  - en
license: cc-by-nc-4.0
library_name: numpy
tags:
  - sports-analytics
  - soccer
  - football
  - defensive-valuation
  - bravery
  - match-summary
pipeline_tag: other
---

# Bravery — Defending-Team Block Willingness

The defending team's willingness to put a body in the way: the percentage of the
opponent's final actions (shots + open-play crosses) that the defending team blocks,
computed per `(match, defending team)`. **Deterministic** event metric — no trained
weights.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform. Implemented via the
silly-kicks `compute_bravery` family (4.87.0, TF-51).

## Method Description

`compute_bravery` groups the opponent's shots and open-play crosses by defending team and
computes the blocked share from the `shot_blocked` / `cross_blocked` SPADL enrichments
(baked into every `convert_to_actions` since silly-kicks 4.56). It is **event-only** and
covers every event provider (StatsBomb / Wyscout / IDSSE / Metrica / SkillCorner /
GradientSports) — no tracking frames or xT are required. Output grain is one row per
`(match, defending team)`.

### Reference

Silly-kicks-native deterministic event metric (TF-51). It implements no single published
academic methodology.

## Inputs

**No training data** — this is a deterministic metric, not a learned model.

| Input | Source |
|---|---|
| SPADL actions + `shot_blocked` / `cross_blocked` | `{catalog}.bronze.spadl_actions` |

## Execution

Materialised by the ADR-013 writer `src/ingestion/bravery_writer.py`, which dispatches per
match via `applyInPandas`, emits native `(match_id, team_id)`, and lands the aggregate in
bronze for dbt to join into the existing `fct_match_summary` mart (one row per match, with
`home_bravery_*` / `away_bravery_*` pivots resolved through a double LEFT-JOIN on the
defending team). Its operational scheduling is a pending operator decision — see
[`workflow-cards/wf-bravery.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-bravery.yaml).

## Intended Use

- **Defensive-profile reporting**: team block willingness on the match-summary page
- **Tactical analysis**: compare defensive commitment across teams and matches
- **Research**: reproducible bravery computation on open event data

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

- **Team-level, not per-player.** Bravery is a defending-team aggregate; it does not
  attribute the block willingness to individual defenders.
- **Block-share only.** It measures the share of final actions blocked, not the danger of
  the chances or the difficulty of the blocks.
- **Event completeness dependence.** The blocked share depends on the completeness and
  consistency of `shot_blocked` / `cross_blocked` tagging across providers.

## Files

No model weights. The method is implemented in source:

- `src/ingestion/bravery_writer.py`

Output is joined into the Delta mart `{catalog}.dev_gold.fct_match_summary`
(`home_bravery_*` / `away_bravery_*`).

## Citation

```bibtex
@software{nielsen2026bravery,
  title={Bravery: Defending-Team Block Willingness on Open Match Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) — inherited from Wyscout non-commercial event data in the multi-provider corpus
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-bravery.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-bravery.yaml)
