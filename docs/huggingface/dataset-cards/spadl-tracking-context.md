---
language: [en]
license: mit
task_categories:
  - tabular-classification
  - tabular-regression
tags:
  - football
  - soccer
  - tracking
  - spadl
  - pitch-control
  - team-shape
  - line-breaking
  - expected-threat
size_categories:
  - 10K<n<100K
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
---

# SPADL Tracking Context

Unified action-coupled tracking features for football matches. One row per SPADL action, enriched with 66 tracking-derived features from the [silly-kicks](https://github.com/ML-KULeuven/silly-kicks) library.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/spadl-tracking-context")
df = ds["train"].to_pandas()
print(df.columns.tolist())  # 83 columns
```

## Feature Groups

| Group | Columns | Reference |
|-------|---------|-----------|
| Action context | nearest_defender_distance, actor_speed, receiver_zone_density, defenders_in_triangle_to_goal | — |
| Actor pre-window | actor_arc_length_pre_window, actor_displacement_pre_window | — |
| Pressure | pressure_on_actor__andrienko_oval, __link_zones, __bekkers_pi | Andrienko 2017, Bekkers 2023 |
| Pitch control | pitch_control_at_ball__spearman, __fernandez_bornn, __voronoi | Spearman 2018, Fernandez & Bornn 2018 |
| Defensive line | defensive_line_x, back_line_high_x, compactness_x, lateral_width, max_lateral_gap, back_n_count | — |
| Off-ball context | line_break, n_attackers_behind_line, n_off_ball_runners_*, ... | Power 2017 |
| Ward line-breaking | line_break__ward, lines_broken__ward, line_breaking_type__ward | Karakus & Arkadas 2025 |
| Team shape | team_shape_{metric}_{attacking/defending} (14 cols) | Clemente 2013 |
| DAS | das_team, das_opponent, das_diff | Bischofberger & Baca 2026 |
| GK influence | gk_pitch_control_share_weighted, gk_reachable_area_m2, gk_closing_time_* | Anzer & Bauer 2021 |
| Cover shadows | n_blocked_receivers, blocking_score, blocked_threat_fraction, ... | — |
| Sync score | sync_score_min, sync_score_mean, sync_score_high_quality_frac | — |
| GK context | defending_gk_player_id, gk_was_distributing, gk_was_engaged, gk_actions_in_possession | — |

## Providers

| Provider | Matches |
|----------|---------|
| IDSSE (Bundesliga) | 7 |
| Metrica (open data) | 3 |
| SkillCorner (open data) | 10 |

## Data Fields

All feature columns are `float64` (nullable NaN) unless noted. Identity columns: `data_source` (string), `match_id` (string), `action_id` (int), `period_id` (int), `time_seconds` (float), `team_id` (string), `player_id` (string), `type_name` (string), `start_x`/`start_y`/`end_x`/`end_y` (float, SPADL 105x68).

## License

MIT — see repository for details.
