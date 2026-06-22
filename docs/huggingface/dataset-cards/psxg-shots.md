---
license: cc-by-nc-4.0
tags:
  - soccer
  - football
  - analytics
  - psxg
  - xgot
  - goalkeeping
  - shot-quality
  - tracking
size_categories:
  - 10K<n<100K
# NOTE: the per-provider `configs:` block is injected at publish time from the providers
# actually present (ingestion.hf_publish.build_provider_configs / inject_frontmatter_configs),
# so it never drifts from the data. See the Quick Start for the resulting configs.
---

# Post-Shot Expected Goals (PSxG / xGOT) — Shot Grain

Post-shot expected goals for on-target shots from the [luxury-lakehouse](https://github.com/karsten-s-nielsen/luxury-lakehouse) analytics platform. **One row per on-target shot, all providers** — the provider is the `data_source` column, not a separate file format. This is the atomic shot-grain fact (`fct_shot_psxg`) that the goalkeeper shot-stopping aggregates (`goals_prevented`) are built from.

PSxG answers "given where and how this shot arrived at goal, what was the probability it scored?" — so a keeper's **goals prevented** = Σ(PSxG of shots faced) − goals conceded.

## Modality coverage

| `psxg_input_source` | Providers | Goalmouth geometry |
|---------------------|-----------|--------------------|
| `tracking_trajectory` | GradientSports, SkillCorner, IDSSE | **Measured** ball crossing point (`shot_crossing_y/z`) from tracking; Platt-recalibrated per modality |
| `statsbomb_freeze_frame` | StatsBomb | **Projected** crossing: the shot trajectory `location → end_location` is projected onto the goal line (x=120), because `end_location` is the save point (not the goal-line crossing) for non-goals |

Metrica is excluded (its bronze has no ball-height signal — `shot_crossing_z IS NULL`).

## Model (ADR-060)

A 4-feature logistic model unified across modalities: `goalmouth_dist_from_centre` (symmetric distance of the crossing point from goal centre), `goalmouth_z` (crossing height), `distance_to_goal_m`, and `shot_angle`. Out-of-sample (GroupKFold by match, n=32,698 StatsBomb on-target) **AUC 0.818 / Brier 0.153**. The naive 2-feature model on raw `end_location` was ≈ random (AUC 0.525) because raw end-location is the goal line only for goals — see [`ADR-060`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/superpowers/adrs/ADR-060-psxg-projected-goalmouth-four-feature-model.md).

## Key columns

| Column | Meaning |
|--------|---------|
| `psxg` | Raw model post-shot xG (0–1) |
| `psxg_recalibrated` | Platt-recalibrated PSxG (tracking; equals `psxg` for StatsBomb, `psxg_calibration='none'`) |
| `psxg_gated` | `true` if the goalmouth fit failed quality gates — `psxg` is NULL, row kept so coverage is computable |
| `is_goal` | Outcome label (the shot was scored) |
| `shot_crossing_y` / `shot_crossing_z` | Measured crossing point (tracking only; NULL for StatsBomb) |
| `player_key` / `defending_gk_player_key` | Kimball surrogates — shooter and the defending keeper (lineup-attributed) |
| `match_key` / `action_id` | Shot identity (join key to `fct_action_values` / `fct_action_context`) |
| `model_version` / `platt_version` / `normalization_version` | Provenance |

## Restricted partitions

GradientSports partitions are license-restricted: they publish to a private org-members-only companion repo (`psxg-shots-restricted`) rather than this public dataset, per lakehouse [ADR-049](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/superpowers/adrs/ADR-049-restricted-hf-dataset-companion-repos.md). A partition migrates here automatically once its license permits public redistribution.

## Quick start

```python
from datasets import load_dataset

# All public providers
ds = load_dataset("luxury-lakehouse/psxg-shots", "all", split="train")

# A single provider (the card's injected `configs:` block — e.g. statsbomb / skillcorner / idsse)
sb = load_dataset("luxury-lakehouse/psxg-shots", "statsbomb", split="train")
```

## Related artifacts

- [`psxg-model`](https://huggingface.co/luxury-lakehouse/psxg-model) — the trained model + card
- [`spadl-action-context`](https://huggingface.co/datasets/luxury-lakehouse/spadl-action-context) — the tracking features the tracking-modality crossings come from
- [`spadl-vaep-action-values`](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) — action-level value
