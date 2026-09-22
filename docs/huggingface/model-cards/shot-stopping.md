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
  - shot-stopping
  - goals-prevented
  - gsaa
pipeline_tag: other
---

# Shot Stopping — Goals Prevented / GSAA per Keeper

The goalkeeper's Goals Prevented (== GSAA — Goals Saved Above Average): the sum of Post-Shot
xG faced minus goals conceded, per `(match, defending keeper)`, reported with and without
in-play penalties. **Deterministic** aggregate over an injected Post-Shot xG — no trained
weights of its own.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform. Implemented via the
silly-kicks `compute_shot_stopping` family (4.120.0, TF-59).

## Method Description

`compute_shot_stopping` gates shots on Post-Shot xG presence (the on-target gate), excludes
own goals / blocked shots / the penalty shootout, and groups on the CANONICAL keeper id (so a
keeper whose id appears in mixed dtypes across a match — int `88` vs str `"88"` — is not
fragmented into two rows with split GSAA). Goals Prevented = `psxg_faced - goals_conceded`; the
`_excl_penalties` companions re-run the aggregate excluding in-play penalties. Output grain is
one row per `(match, defending keeper)`. It is **event-only** but requires an injected per-shot
Post-Shot xG (silly-kicks ships no xG model) — supplied here from the unified `fct_shot_psxg`
fact, so it covers every provider whose shots carry a Post-Shot xG (tracking providers via the
trajectory model, StatsBomb via the freeze-frame model).

### Reference

Post-Shot Expected Goals (PSxG) and the derived Goals Prevented / Goals Saved Above Average
(GSAA) shot-stopping metric follow the StatsBomb lineage. The Post-Shot xG this metric consumes
is governed under `wf-goalkeeper`.

## Inputs

**No training data** — this is a deterministic aggregate, not a learned model.

| Input | Source |
|---|---|
| SPADL actions + `defending_gk_player_id` | `{catalog}.bronze.spadl_actions` |
| Per-shot Post-Shot xG (`psxg_recalibrated`) | `{catalog}.dev_gold.fct_shot_psxg` |

## Execution

Materialised by the ADR-013 writer `src/ingestion/shot_stopping_writer.py`, which dispatches per
match via `applyInPandas`, LEFT-joins the gold Post-Shot xG fact per shot, derives the defending
keeper's team from a per-match `player_id -> team_id_native` map, emits native `(match_id,
player_id, team_id)`, and lands the aggregate in bronze for dbt to re-source the three GK marts
(`fct_gk_shot_stopping` / `fct_gk_shot_stopping_pooled` / `fct_goalkeeper_stats`). It is scheduled
into the daily job — see
[`workflow-cards/wf-shot-stopping.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-shot-stopping.yaml).

## Intended Use

- **Goalkeeper shot-stopping reporting**: Goals Prevented / GSAA on the Goalkeeper Analytics page
- **Tactical analysis**: compare keeper shot-stopping over a pooled (competition, season) window
- **Research**: reproducible GSAA computation on open match data

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

- **Low sample per match — the per-match grain is a drill-down, NOT an evaluative number.** A
  keeper faces ~1 on-target shot per match on average, so the per-match Goals Prevented is noise.
  The evaluative surface is the POOLED layer (`fct_gk_shot_stopping_pooled`), which ships raw Goals
  Prevented with a closed-form Poisson-binomial band and DEFERS the percentile leaderboard until a
  cohort clears the ranking floor (≥20 keepers clearing ≥20 shots — currently false everywhere).
- **Injected-xG dependence.** Goals Prevented is only as good as the injected Post-Shot xG; a
  gate-failed shot (trajectory-fit failure) is excluded, which is visible in `shots_faced_total`
  (pre-gate) vs `shots_faced` (post-gate).
- **Attribution.** GSAA credits the defending keeper stamped by `add_pre_shot_gk_context`; a
  sub-window keeper change within a match is attributed to the identified keeper.

## Files

No model weights. The method is implemented in source:

- `src/ingestion/shot_stopping_writer.py`

Output is re-sourced into the Delta marts `{catalog}.dev_gold.fct_gk_shot_stopping` /
`fct_gk_shot_stopping_pooled` / `fct_goalkeeper_stats`.

## Citation

```bibtex
@software{nielsen2026shotstopping,
  title={Shot Stopping: Goals Prevented / GSAA per Keeper on Open Match Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) — inherited from Wyscout non-commercial event data in the multi-provider corpus
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-shot-stopping.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-shot-stopping.yaml)
```
