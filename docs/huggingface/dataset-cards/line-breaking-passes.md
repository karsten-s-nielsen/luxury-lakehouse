---
language: [en]
license: mit
task_categories: [tabular-classification]
tags: [sports-analytics, soccer, football, line-breaking-passes, defensive-line, tracking-data, statsbomb]
size_categories: [1M-10M]
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
---

# Line-Breaking Passes

Every pass from **~5 million** professional soccer events, annotated with whether each pass breaks through opponent defensive lines. Built from StatsBomb Open Data and Wyscout event data, with line-breaking labels derived from StatsBomb 360 freeze-frame and Metrica tracking data. The `is_line_breaking` flag enables binary classification models that learn to predict line-breaking potential from pass context alone.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/line-breaking-passes")
df = ds["train"].to_pandas()

# Filter to confirmed line-breaking passes
lb_passes = df[df["is_line_breaking"] == True]
print(f"{len(lb_passes)} line-breaking passes out of {len(df)} total")
```

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

## What is a Line-Breaking Pass?

A **line-breaking pass** is a pass that travels through a compact defensive shape, bypassing one or more lines of defenders. They are high-value attacking actions because they disrupt defensive organization and create goal-scoring opportunities in the space behind the defensive block.

Line-breaking detection is computed using **Ward hierarchical clustering** on opponent positions at the moment of the pass (sourced from StatsBomb 360 freeze frames, Metrica tracking data, and IDSSE Bundesliga tracking data), followed by a **cross-product straddle test** to determine whether the pass vector intersects the cluster boundary. Three defensive lines (low-block, mid-block, high-press) are identified per frame; a pass may break through one, two, or all three.

The dataset includes **all passes** &mdash; both line-breaking and non-line-breaking &mdash; to provide balanced positive and negative examples for machine learning. The `is_line_breaking` column is the binary target label.

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `pass_id` | `string` | Unique pass identifier (surrogate key) |
| `match_key` | `bigint` | **Canonical Kimball match FK** (PR 7, ADR-011). BIGINT surrogate, collision-free across providers. |
| `team_key` | `bigint` | **Canonical Kimball team FK** (PR 7, ADR-011). BIGINT surrogate. |
| `passer_player_key` | `bigint` | **Canonical Kimball player FK** for the passer (PR 7, ADR-011). BIGINT surrogate. |
| `recipient_player_key` | `bigint` | **Canonical Kimball player FK** for the recipient (PR 7, ADR-011). NULL for Wyscout and incomplete passes. |
| `match_id` | `bigint` | Match identifier (provider-native; coexists with `match_key` until 2026-07-22 sunset) |
| `player_id` | `int` | Passer player identifier (provider-native) |
| `team_id` | `int` | Team identifier (provider-native) |
| `pass_recipient_id` | `int` | Recipient identifier (provider-native; NULL for Wyscout and incomplete passes) |
| `competition_id` | `int` | Competition identifier |
| `season_id` | `int` | Season identifier |
| `period` | `bigint` | Match period (1&ndash;5) |
| `minute` | `bigint` | Match minute |
| `second` | `bigint` | Match second |
| `start_x` | `double` | Starting X coordinate (StatsBomb 120-yard scale) |
| `start_y` | `double` | Starting Y coordinate (StatsBomb 80-yard scale) |
| `end_x` | `double` | Ending X coordinate |
| `end_y` | `double` | Ending Y coordinate |
| `pass_type` | `string` | Pass type classification |
| `pass_height` | `string` | Pass height (Ground Pass, Low Pass, High Pass) |
| `body_part` | `string` | Body part used |
| `pass_length` | `double` | Pass length in yards |
| `pass_angle_radians` | `double` | Pass angle in radians |
| `pass_outcome` | `string` | Pass result (Complete, Incomplete, Out, etc.) |
| `is_cross` | `boolean` | Whether the pass was a cross |
| `is_switch` | `boolean` | Whether the pass was a switch of play |
| `is_through_ball` | `boolean` | Whether the pass was a through ball |
| `is_complete` | `boolean` | Whether the pass was completed |
| `is_progressive` | `boolean` | Whether the pass moved the ball significantly closer to goal |
| `pass_direction` | `string` | Pass direction (forward, backward, lateral) |
| `is_line_breaking` | `boolean` | Whether the pass breaks through opponent defensive lines |
| `lines_broken` | `int` | Number of defensive lines broken (0&ndash;3) |
| `line_breaking_type` | `string` | Type of line break (through, around) |
| `data_source` | `string` | Data provider (statsbomb, wyscout) |

### Coordinate System

All coordinates use the **StatsBomb standard**: 120&times;80 yards. The origin (0, 0) is at the bottom-left corner from the perspective of the attacking team. The x-axis runs along the length of the pitch (0&ndash;120 yards), and the y-axis runs along the width (0&ndash;80 yards).

## Detection Method

Line-breaking detection runs as a distributed compute pipeline via `applyInPandas` on Databricks, grouped by `match_id`. For each pass in a match:

1. **Cluster opponent positions** using Ward hierarchical clustering on the Y-axis to identify defensive lines.
2. **Straddle test**: compute the cross product of the pass vector against each cluster boundary segment. A sign change indicates the pass crosses the line.
3. **Label assignment**: `is_line_breaking = true` if the pass straddles at least one defensive line cluster; `lines_broken` counts how many.

This method does not require labeled training data &mdash; it is a deterministic geometric rule over freeze-frame positions.

## Data Sources

| Source | Matches | License |
|--------|---------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~3,000 | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | ~1,900 | CC-BY-NC 4.0 |

Coverage includes the Premier League, La Liga, Serie A, Bundesliga, Ligue 1, Champions League, World Cup, and more.

## Limitations

- **Sparse line-breaking labels**: Line-breaking detection requires opponent positional data (StatsBomb 360 freeze frames, Metrica tracking, or IDSSE tracking data). For matches without this data &mdash; the majority of Wyscout matches and StatsBomb matches without 360 &mdash; `is_line_breaking`, `lines_broken`, and `line_breaking_type` are NULL. These passes are still included in the dataset as unlabeled examples.
- **StatsBomb 360 coverage**: Approximately 323 StatsBomb matches include 360 freeze-frame data. Line-breaking labels are densest for these matches. IDSSE Bundesliga tracking provides an additional 7 matches with full positional data.
- **Geometric approximation**: The Ward clustering + straddle test is a rule-based approximation. It does not model defensive intent, pressing triggers, or tactical context beyond raw position.
- **Coordinate system**: All coordinates are in StatsBomb 120&times;80-yard scale. Wyscout passes are normalized to this scale; minor rounding differences may exist near pitch boundaries.
- **Open data only**: Commercial datasets with richer tracking coverage may yield different label distributions and model generalizations.

## Citation

There is no formal paper for this dataset. If you use it, please cite the repository:

```bibtex
@misc{luxury_lakehouse_line_breaking,
  title={Line-Breaking Passes: A Soccer Pass Dataset with Defensive Line Labels},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse},
  note={Data sourced from StatsBomb Open Data (CC-BY 4.0) and Wyscout Public Dataset (CC-BY-NC 4.0)}
}
```

## More Information

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

## PR 7 changelog (2026-04-27)

**Downstream (2026-07 mart consolidation):** the gold mart `fct_line_breaking_results` was retired. Line-breaking flags are consumed directly from the surviving `stg_line_breaking__results` staging view by `fct_passes`, where Kimball FK resolution (via `int_unified_passes` JOIN on (event_id, data_source) → dim_teams/dim_players) now happens.

- **License**: [MIT](https://opensource.org/licenses/MIT)
