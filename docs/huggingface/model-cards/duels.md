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
  - ground-duels
  - glicko-2
pipeline_tag: other
---

# Ground Duels — Per-Player Glicko-2 Rating

A per-`(match, player)` Glicko-2 rating of a player's ground-duel win/loss record: a stateful
skill rating (rating / deviation / volatility) that carries forward across matches, plus the
per-match contested / won / lost counts. **Deterministic** — the Glicko-2 update is a fixed
algorithm, no trained weights.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform. Implemented via the
silly-kicks `duels.compute_duel_ratings` family (4.120.0, TF-55).

## Method Description

`compute_duel_ratings` extracts ground duels — from the native sportec `tackle_winner/loser`
columns where present, else the derived `tackle`/`take_on` adjacency (a cross-team pair within
5 seconds where exactly one action succeeds; both-succeed / both-fail is indeterminate and
excluded) — and runs the Glicko-2 rating system over them in ascending `game_id` order. Because
Glicko-2 is stateful (each match is one rating period and a player's rating carries forward),
the writer processes the whole corpus in a **single ordered driver pass**, never a per-match
parallel dispatch. Each row carries the carried-forward `duel_rating` / `duel_rating_deviation`
/ `duel_volatility` plus the per-match `duels_contested` / `duels_won` / `duels_lost` and a
`duel_winner_source` labeling provenance. Output grain is one row per `(match, player)`; it is
**event-only**.

### Reference

The rating system is Glicko-2: Glickman, M.E. (2012). "Example of the Glicko-2 system." Boston
University. It is an independent Python implementation of the published algorithm.

## Inputs

**No training data** — this is a deterministic rating computation, not a learned model.

| Input | Source |
|---|---|
| SPADL actions (tackle / take_on adjacency + native winner/loser where present) | `{catalog}.bronze.spadl_actions` |

## Execution

Materialised by the ADR-013 writer `src/ingestion/duels_writer.py`, which pulls each provider's
whole action corpus to the driver (bounded to the narrow duel-input columns), runs
`compute_duel_ratings` in one ordered pass, emits native `(match_id, player_id, team_id)`, and
lands the ratings in bronze for dbt to join into the player-match mart
`fct_player_match_metrics`. It is scheduled into the daily job — see
[`workflow-cards/wf-duels.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-duels.yaml).

## Intended Use

- **Ground-duel skill reporting**: Glicko-2 rating on the Player Analytics page
- **Tactical analysis**: compare players' ground-duel win records over a match window
- **Research**: reproducible Glicko-2 duel-rating computation on open match data

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

- **Ground duels only.** The SPADL taxonomy has no aerial-duel type, so aerial contests are not
  rated — the rating reflects only ground-duel skill.
- **Derived-adjacency proxy.** Where native winner/loser labels are absent, duels fall back to
  the derived `tackle`/`take_on` adjacency (~11 candidate duels per match), a lower-signal proxy
  than a native duel outcome. `duel_winner_source` records the labeling provenance so consumers
  can filter on it.
- **Low per-match sample.** A player contests few duels per match, so the per-match counts are
  noisy; the carried-forward Glicko-2 rating (with its deviation) is the stabilised surface.

## Files

No model weights. The method is implemented in source:

- `src/ingestion/duels_writer.py`

Output is landed in the Delta mart `{catalog}.dev_gold.fct_player_match_metrics`.

## Citation

```bibtex
@software{nielsen2026duels,
  title={Ground Duels: Per-Player Glicko-2 Rating on Open Match Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) — inherited from Wyscout non-commercial event data in the multi-provider corpus
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-duels.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-duels.yaml)
