---
language: [en]
license: cc-by-nc-4.0
task_categories:
  - tabular-classification
tags:
  - sports-analytics
  - soccer
  - football
  - expected-goals
  - xg
  - shots
  - statsbomb
  - wyscout
size_categories:
  - 100K<n<1M
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/**/*.parquet"
---

# xG Shot Data &mdash; StatsBomb + Wyscout

**~131K professional soccer shots** from [StatsBomb Open Data](https://github.com/statsbomb/open-data) (~95K) and [Wyscout](https://figshare.com/collections/Soccer_match_event_dataset/4415000) (~43K), with geometric features, categorical context, and goal labels. Partitioned by `data_source` for selective loading.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## ⚠️ Schema change (cut-over 2026-07-22)

This dataset emits both legacy and canonical Kimball key columns side-by-side. The legacy `match_id` column will be removed on **2026-07-22** (90-day dual-column window opened at the PR 3 ship on 2026-04-22, per [ADR-011](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md)):

| Legacy column | Canonical replacement | Notes |
|---|---|---|
| `match_id` | `match_key` | BIGINT Kimball surrogate; collision-free across providers |

Both columns are populated during the window. Update consumer code to read `match_key` before the cut-over date; after cut-over the legacy `match_id` column is removed.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/xg-shot-data")
df = ds["train"].to_pandas()

# Goal conversion rate by body part
df.groupby("shot_body_part")["is_goal"].mean().sort_values(ascending=False)
```

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## What Is This Dataset?

This dataset contains every shot from the StatsBomb and Wyscout open data collections, enriched with pre-computed geometric features (distance to goal, shot angle) and unified to a common coordinate system. It serves as the primary training input for both xG models in the platform:

| Model | Repo | Architecture |
|-------|------|-------------|
| **xG v1** | [`xg-model-statsbomb-wyscout`](https://huggingface.co/luxury-lakehouse/xg-model-statsbomb-wyscout) | Calibrated XGBoost (13 tabular features) |
| **xG v2** | [`xg-v2-model-set-encoder`](https://huggingface.co/luxury-lakehouse/xg-v2-model-set-encoder) | Deep Sets encoder + MLP with MC Dropout (tabular + freeze-frame context) |

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `shot_id` | `string` | Surrogate key (deterministic hash via `dbt_utils.generate_surrogate_key`) |
| `match_key` | `Int64` | **Canonical Kimball match FK** (ADR-011). BIGINT surrogate, collision-free across providers. |
| `match_id` | `Int64` | LEGACY provider-native match identifier; sunset 2026-07-22 (see top-of-card). |
| `competition_id` | `Int64` | Competition identifier (NULL for Wyscout) |
| `season_id` | `Int64` | Season identifier (NULL for Wyscout) |
| `player_id` | `Int64` | Player identifier |
| `team_id` | `Int64` | Team identifier |
| `period` | `Int64` | Match period (1 = first half, 2 = second half, 3+ = extra time) |
| `minute` | `Int64` | Match minute |
| `second` | `Int64` | Second within the minute |
| `location_x` | `float64` | Shot x-coordinate (StatsBomb: 0&ndash;120 yards, attacking direction) |
| `location_y` | `float64` | Shot y-coordinate (StatsBomb: 0&ndash;80 yards) |
| `end_location_x` | `float64` | Shot destination x-coordinate |
| `end_location_y` | `float64` | Shot destination y-coordinate |
| `shot_outcome` | `string` | Categorical outcome: Goal, Saved, Blocked, Off T, Wayward, Post |
| `shot_body_part` | `string` | Body part used: Right Foot, Left Foot, Head, Other |
| `shot_technique` | `string` | Technique: Normal, Volley, Half Volley, Lob, Overhead Kick, Backheel, Diving Header |
| `shot_type` | `string` | Context: Open Play, Free Kick, Corner, Penalty, Kick Off |
| `is_goal` | `bool` | Target variable &mdash; `true` if `shot_outcome = 'Goal'` |
| `distance_to_goal` | `float64` | Euclidean distance from shot location to goal center (yards) |
| `shot_angle` | `float64` | Angle subtended by the goal posts from the shot location (radians) |
| `is_first_time` | `bool` | Shot taken first-time (no prior control touch) |
| `play_pattern` | `string` | Build-up pattern: Regular Play, From Counter, From Corner, From Free Kick, From Keeper, etc. |
| `statsbomb_xg` | `float64` | StatsBomb proprietary xG (NULL for Wyscout shots; useful as a benchmark label) |
| `data_source` | `string` | Partition key: `statsbomb` or `wyscout` |

### Coordinate System

All spatial features use the **StatsBomb coordinate system**:

- Pitch dimensions: 120 yards (length) &times; 80 yards (width)
- Origin: bottom-left corner of the pitch
- Attacking direction: left to right (x increases toward opponent goal)
- Goal center: approximately (120, 40)

Wyscout coordinates (0&ndash;100% scale) are converted to StatsBomb coordinates at the dbt staging layer.

## Data Sources

| Source | Shots | Matches | License |
|--------|-------|---------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~95K | ~3,000 | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | ~43K | ~1,900 | CC-BY-NC 4.0 |
| **Total** | **~131K** | | CC-BY-NC 4.0 (most restrictive applies) |

Coverage includes the Premier League, La Liga, Serie A, Bundesliga, Ligue 1, Champions League, World Cup, and more.

### Partitioning

Data is Hive-partitioned by `data_source`:

```
data/
  data_source=statsbomb/data.parquet
  data_source=wyscout/data.parquet
```

Load a single source efficiently:

```python
import pandas as pd
from huggingface_hub import hf_hub_download

# Load only StatsBomb shots
path = hf_hub_download(
    "luxury-lakehouse/xg-shot-data",
    "data/data_source=statsbomb/data.parquet",
    repo_type="dataset",
)
df_sb = pd.read_parquet(path)
```

## Use Cases

- **xG model training**: Primary input for training expected goals models (logistic regression, XGBoost, Deep Sets, or custom architectures)
- **Shot analysis**: Visualize shot maps, compare conversion rates by body part, technique, or play pattern
- **Benchmarking**: Compare custom xG models against the included `statsbomb_xg` column on the StatsBomb subset
- **Feature engineering**: Pre-computed `distance_to_goal` and `shot_angle` ready for modeling; categorical columns ready for one-hot encoding
- **Cross-source research**: Study differences in shot event classification between StatsBomb and Wyscout

## Limitations

- **Open data only**: Contains only publicly available StatsBomb and Wyscout shots. Commercial datasets cover additional leagues and seasons.
- **No freeze frames**: This dataset contains tabular shot features only. For player positions at the moment of each shot (used by xG v2), see the companion [xG Freeze Frame Data](https://huggingface.co/datasets/luxury-lakehouse/xg-freeze-frame-data) dataset.
- **Wyscout NULL columns**: `competition_id`, `season_id`, and `statsbomb_xg` are NULL for all Wyscout shots due to the absence of a cross-source match join for those fields.
- **Coordinate conversion**: Wyscout coordinates are converted from percentage-based (0&ndash;100) to StatsBomb yards (0&ndash;120, 0&ndash;80) at the dbt staging layer. Subtle conversion artifacts may exist at pitch boundaries.
- **Class imbalance**: Goals are relatively rare (~9&ndash;10% of shots). Account for this imbalance during training using class weights (`scale_pos_weight` in XGBoost) or stratified sampling.

## Citation

If you use this dataset, please cite the data providers:

```bibtex
@misc{statsbomb2024opendata,
  title={StatsBomb Open Data},
  author={{StatsBomb}},
  year={2024},
  url={https://github.com/statsbomb/open-data},
  note={CC-BY 4.0}
}
```

```bibtex
@misc{pappalardo2019public,
  title={A public data set of spatio-temporal match events in soccer competitions},
  author={Pappalardo, Luca and Cintia, Paolo and Rossi, Alessio and Massucco, Emanuele
          and Ferragina, Paolo and Pedreschi, Dino and Giannotti, Fosca},
  journal={Scientific Data},
  volume={6},
  number={1},
  pages={1--15},
  year={2019},
  publisher={Nature Publishing Group}
}
```

```bibtex
@software{nielsen2026xgshotdata,
  title={xG Shot Data: StatsBomb + Wyscout Open Data Shot Features},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Companion Resources

| Resource | Type | Description |
|----------|------|-------------|
| [xG Model v1](https://huggingface.co/luxury-lakehouse/xg-model-statsbomb-wyscout) | Model | Calibrated XGBoost + logistic baseline (13 features) |
| [xG v2 Set Encoder](https://huggingface.co/luxury-lakehouse/xg-v2-model-set-encoder) | Model | Deep Sets + MLP with MC Dropout uncertainty |
| [xG Freeze Frame Data](https://huggingface.co/datasets/luxury-lakehouse/xg-freeze-frame-data) | Dataset | Player positions at shot time (15.58M rows, 323 matches) |
| [SPADL/VAEP Action Values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Dataset | Per-action VAEP valuations (~9.5M actions) |
| [VAEP Model](https://huggingface.co/luxury-lakehouse/vaep-model-statsbomb-wyscout) | Model | P(scores) + P(concedes) XGBClassifiers |

## Demo

Try the interactive [Soccer Analytics Explorer](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) &mdash; explore shot maps with xG overlays, filter by competition, and compare custom xG against StatsBomb.

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) (inherited from Wyscout data)
- **Publishing script**: `scripts/publish_xg_shots_hf.py` (PEP 723 standalone)
- **Platform**: [Luxury Lakehouse Soccer Analytics](https://github.com/karsten-s-nielsen/luxury-lakehouse)

## PR 7 changelog (2026-04-27)

The upstream gold mart `fct_shots` now carries Kimball surrogate FKs (`team_key`, `player_key`) alongside the existing `match_key` (PR 3) and the legacy `team_id`/`player_id` INT columns during the 2026-07-22 dual-column window per ADR-011. SB+WS native IDs are real BIGINTs cast to string for the dim JOINs. PR 8 will sunset the legacy `*_id` columns post-2026-07-22.
