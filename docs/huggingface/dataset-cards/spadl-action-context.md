---
license: cc-by-nc-4.0
tags:
  - soccer
  - football
  - analytics
  - spadl
  - action-context
  - tracking
  - pitch-control
  - expected-threat
size_categories:
  - 100K<n<1M
---

# SPADL Action Context Features

Unified per-action context features for football matches from the [luxury-lakehouse](https://github.com/karsten-s-nielsen/luxury-lakehouse) analytics platform. One row per SPADL action, with ~111 columns for tracking providers and ~5 for event-only providers. Covers all 6 data sources.

## Provider Tiers

| Tier | Providers | Feature Coverage |
|------|-----------|-----------------|
| Event-only | StatsBomb, Wyscout | Identity + game state (~5 columns) |
| SB360 | StatsBomb (with 360 freeze-frames) | Event-only + freeze-frame-derived context |
| Tracking | IDSSE, Metrica, SkillCorner | Full ~111 columns including pitch control, team shape, line-breaking, OBSO, PAUSA, gk_influence zones, xShotOccurrence |

GradientSports data is computed but excluded from HF publication per licensing restrictions.

## Pitch-control method provenance & SB360 coverage

The `pitch_control_method` column records which pitch-control model produced each row's
pitch-control-derived metrics (OBSO, PAUSA, `gk_influence`/`gk_closing_time_*`): **`spearman`**
(velocity-aware) on the tracking providers, **`voronoi`** (position-only) on SB360 freeze-frames
(which carry no velocity), and `null` on event-only rows. SB360 and tracking values for these
columns are therefore produced by different estimators and are **not directly comparable** —
segment on `pitch_control_method`.

The `ghost_gk_method` column records which ghost-GK **KDE backend** produced each row's `ghost_gk_x/y/density_spread`
(one of `scipy`, `vectorized`, `cpu-numba`, `fft`, `fft-cic`; `null` on event-only rows). The default is
`fft-cic` (a ~95% mode-exact fast approximation); a run may select an exact backend for higher accuracy,
which yields *different* `ghost_gk_*` values. This column scopes **only** to `ghost_gk_*` and is independent
of `pitch_control_method` — **segment on `ghost_gk_method` before comparing `ghost_gk_*` across rows.**

SB360 freeze-frame coverage is **partial and sparse**: each metric only populates the subset of
actions whose freeze-frame contains the needed players, and `xshot_occurrence` in particular is
non-null for only ~4% of SB360 actions (a non-random subsample). **Do not compute naive
provider-level averages over SB360 metrics** — filter on non-null and account for the sampling.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/spadl-action-context")
df = ds["train"].to_pandas()
print(df.columns.tolist())
```

## Column Categories

| Category | Example Columns |
|----------|----------------|
| Identity | data_source, match_id, action_id, period_id, team_id, player_id |
| Game State | time_seconds, type_name, start_x, start_y, end_x, end_y |
| Frame Linkage | frame_id, timestamp_utc |
| GK Resolution | defending_gk_player_id, gk_was_distributing, gk_was_engaged |
| GK Spatial | gk_x, gk_y, gk_distance_to_goal |
| Action Context | nearest_defender_distance, receiver_zone_density, defenders_in_triangle_to_goal |
| Actor Pre-Window | actor_arc_length_pre_window, actor_displacement_pre_window |
| Pressure | pressure_on_actor__andrienko_oval, __link_zones, __bekkers_pi |
| Pitch Control | pitch_control_at_ball__spearman, __fernandez_bornn, __voronoi |
| Defensive Line | defensive_line_x, back_line_high_x, compactness_x, lateral_width |
| Off-Ball Context | line_break, n_attackers_behind_line, n_off_ball_runners_* |
| Ward Line-Breaking | line_break__ward, lines_broken__ward, line_breaking_type__ward |
| Team Shape | team_shape_{metric}_{attacking/defending} (14 cols) |
| DAS | das_team, das_opponent, das_diff |
| GK Influence | gk_pitch_control_share_weighted, gk_reachable_area_m2, gk_closing_time_* |
| Cover Shadows | n_blocked_receivers, blocking_score, blocked_threat_fraction |
| Sync Score | sync_score_min, sync_score_mean, sync_score_high_quality_frac |
| OBSO | obso_value, obso_total_threat |
| PAUSA | pausa_value, pausa_added_threat |
| Space Creation | space_created_m2, space_exploited |
| ELASTIC Sync | elastic_sync_score, elastic_compactness |
| Shape Graph | shape_graph_centrality, shape_graph_clustering |
| Ghost-GK | ghost_gk_x, ghost_gk_y, ghost_gk_density_spread (served = boosted-HGBR mean; density_spread = conditional-density dispersion) |
| Structural Pass | structural_lbs, structural_sgm, structural_sdi (Line Bypass Score / Space Gain / Disruption Index; Karakus & Arkadas 2026, arXiv:2603.28916; NaN for non-pass/cross) |
| Player Influence | actor_reachable_area_m2, off_ball_xt_{team,opponent,diff}, reachable_area_{team,opponent,diff} (Spearman 2018 + Singh 2018) |
| xCrossAttempt | xcross_attempt (P(cross attempt) for the in-possession team; Cao et al. 2025, arXiv:2505.11841; tracking-only — NaN on SB360) |
| xShotOccurrence | xshot_occurrence (P(shot attempted); Pipping-Gamón, Feng & Sabin 2026, arXiv:2512.00203) |
| Pitch-Control Provenance | pitch_control_method (spearman=tracking / voronoi=SB360 / null=event-only) |
| Ghost-GK Backend Provenance | ghost_gk_method (scipy/vectorized/cpu-numba/fft/fft-cic; null=event-only) |

## Data Fields

All feature columns are `float64` (nullable NaN) unless noted. Identity columns: `data_source` (string), `match_id` (string), `action_id` (int), `period_id` (int), `time_seconds` (float), `team_id` (string), `player_id` (string), `type_name` (string), `start_x`/`start_y`/`end_x`/`end_y` (float, SPADL 105x68).

## Citation

```bibtex
@software{luxury_lakehouse,
  title  = {Luxury Lakehouse — Serverless Soccer Analytics Platform},
  url    = {https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## License

CC-BY-NC-4.0 — see repository for details.
