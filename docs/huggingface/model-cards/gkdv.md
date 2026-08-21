---
language:
  - en
license: cc-by-4.0
library_name: numpy
tags:
  - sports-analytics
  - soccer
  - football
  - goalkeeper-valuation
  - gkdv
  - counterfactual
  - tracking-data
datasets:
  - luxury-lakehouse/pitch-control-tracking
pipeline_tag: other
---

# GKDV — Goalkeeper Deterrent Value (Ghost-Frame Counterfactual)

Estimates a keeper's off-line deterrent effect via a counterfactual: for each
scored-and-defending tracking frame it builds a "ghost" frame with the keeper removed, and
measures the change in Dangerous-Attacks-Space (`delta_das`, accessible-space) and in
fitted-xT threat (`delta_threat_suppression`) that the keeper's positioning suppresses. The
per-frame deltas are pooled per keeper over `(competition, season)`. The counterfactual composes a
ghost-substitution engine (Le, Yue, Carr & Lucey 2017) with two physics arms — accessible space
(`delta_das`, Bischofberger & Baca 2026) and threat-suppression (`delta_threat_suppression`,
Spearman 2018 + Shaw & Sudarshan 2020); the ghost-frame construction and per-keeper pooling are
silly-kicks-native.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform. Implemented via the
silly-kicks gkdv family (4.87.0, `[das]` extra).

## Method Description

Per tracking work unit: infer the ball carrier, derive the team in possession (DAS requires
it), then `build_ghost_frames(home_team_id, carrier=)` produces the counterfactual frames.
For each scored-and-defending frame, `delta_das` (accessible space) and
`delta_threat_suppression` (fitted-xT threat against the goal map) are computed as the
difference between the actual and ghost legs, then `aggregate_by_keeper` pools per keeper,
partitioned by `(competition, season)`.

**Drop-reason exclusion (critical).** `build_ghost_frames` returns the full counterfactual
frames; a dropped frame (missing/NaN GK, off-domain) is byte-identical across the
actual/ghost legs, so differencing it yields `delta == 0` and would bias every keeper
aggregate toward the null. The writer restricts to frames whose
`provenance["drop_reason"].isna()` (and the defending keeper) **before** any differencing.

### Reference

Per silly-kicks' `NOTICE`, GKDV is a counterfactual difference taken over three published
methodologies, one per sub-component:

- **Ghost-substitution engine** (`build_ghost_frames`): Le, H. M., Yue, Y., Carr, P., & Lucey, P. (2017). **"Data-Driven Ghosting Using Deep Imitation Learning."** MIT Sloan Sports Analytics Conference.
- **Accessible-space arm** (`delta_das`): Bischofberger, J., & Baca, A. (2026). **"Dangerous accessible space: a unified model of space and value in team sports."** *Journal of Big Data*, 13, 76 (package: `accessible-space`).
- **Threat-suppression arm** (`delta_threat_suppression`): Spearman, W. (2018). **"Beyond Expected Goals."** MIT Sloan SAC — plus Shaw, L., & Sudarshan, M. (2020). **"A Framework for Tactical Analysis and Individual Offensive Production Assessment in Soccer Using Markov Models"** (source of the `lambda_gk = 3 * lambda_outfield` constant).

The ghost-frame construction and per-keeper pooling are silly-kicks-native and pinned to the
4.87.0 signatures (`build_ghost_frames` / `delta_das` / `delta_threat_suppression` /
`aggregate_by_keeper`) as the surface is still evolving upstream. DEFCON-GNN (Kim et al. 2025) is a
*comparator only* per `NOTICE` — it is **not** implemented here and is not GKDV's methodology.

## Inputs

**No training data for the counterfactual step** (the xT surface used by
`delta_threat_suppression` is fitted separately).

| Input | Source |
|---|---|
| Resolved tracking frames (players + ball) | `{catalog}.bronze.spadl_action_context` |
| Fitted xT grid (goal map) | Project xT surface |

## Execution

Materialised by the ADR-013 writer `src/ingestion/gkdv_writer.py` (requires the `[das]`
extra), which resolves native `(player_id, competition_id, season_id)` to Kimball
surrogates in dbt staging and lands the pooled result in bronze for dbt to join into the
existing `fct_gk_shot_stopping_pooled` mart. Its operational scheduling is a pending
operator decision — see
[`workflow-cards/wf-gkdv.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-gkdv.yaml).

## Intended Use

- **Goalkeeper positioning analysis**: quantify a keeper's space/threat deterrent
- **Tactical analysis**: compare keepers' off-line contribution pooled over a season
- **Research**: reproducible ghost-frame counterfactual on open tracking data

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

- **Evolving upstream API.** The gkdv surface is still developing in silly-kicks; this card
  is pinned to the 4.87.0 signatures.
- **Tracking-and-DAS dependence.** Requires tracking frames and the `[das]` accessible-space
  computation; event-only matches receive no gkdv.
- **Counterfactual assumption.** Removing the keeper assumes the rest of the frame is
  unchanged, which over- or under-states the deterrent when other defenders would have
  reorganised.
- **Small keeper cohort.** Pooling over `(competition, season)` with `min_nonzero` / `min_games`
  gates means low-volume keepers may be gated out.

## Files

No model weights. The method is implemented in source:

- `src/ingestion/gkdv_writer.py`

Output is joined into the Delta mart `{catalog}.dev_gold.fct_gk_shot_stopping_pooled`
(`gkdv_delta_das_*` / `gkdv_delta_threat_*`).

## Citation

```bibtex
@inproceedings{le2017ghosting,
  title={Data-Driven Ghosting Using Deep Imitation Learning},
  author={Le, Hoang M. and Yue, Yisong and Carr, Peter and Lucey, Patrick},
  booktitle={MIT Sloan Sports Analytics Conference},
  year={2017}
}
```

```bibtex
@article{bischofberger2026das,
  title={Dangerous accessible space: a unified model of space and value in team sports},
  author={Bischofberger, Jonas and Baca, Arnold},
  journal={Journal of Big Data},
  volume={13},
  pages={76},
  year={2026}
}
```

```bibtex
@inproceedings{shaw2020markov,
  title={A Framework for Tactical Analysis and Individual Offensive Production Assessment in Soccer Using Markov Models},
  author={Shaw, Laurie and Sudarshan, Mallesh},
  year={2020}
}
```

```bibtex
@software{nielsen2026gkdv,
  title={GKDV: Goalkeeper Deterrent Value via Ghost-Frame Counterfactual on Open Tracking Data},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## More Information

- **License**: [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/) — inherited from open tracking sources
- **Source repository**: <https://github.com/karsten-s-nielsen/luxury-lakehouse>
- **Workflow card**: [`workflow-cards/wf-gkdv.yaml`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-gkdv.yaml)
