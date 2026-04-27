---
language: [en]
license: cc-by-4.0
task_categories:
  - tabular-classification
tags:
  - sports-analytics
  - soccer
  - football
  - expected-goals
  - freeze-frames
  - statsbomb
  - deep-sets
size_categories:
  - 10M<n<100M
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/**/*.parquet"
---

# xG Freeze-Frame Data &mdash; StatsBomb 360

**~15.58M freeze-frame rows** from 323 StatsBomb 360 matches, capturing player positions at the moment of each shot. Each row represents one visible player in one shot event.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/xg-freeze-frame-data")
df = ds["train"].to_pandas()

# Average number of visible players per shot
df.groupby("event_id").size().describe()
```

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## What Is This Dataset?

StatsBomb 360 data includes inline freeze frames for each event &mdash; a snapshot of every visible player's position at the instant of the event. This dataset extracts freeze-frame rows specifically for shot events, providing the spatial context that the [xG v2 set encoder](https://huggingface.co/luxury-lakehouse/xg-v2-model-set-encoder) uses to condition expected goals predictions on defensive positioning.

Each row represents one player visible in one shot. A single shot typically has 10&ndash;22 freeze-frame rows (one per visible player). The set encoder aggregates these into a fixed-length context vector using permutation-invariant sum pooling.

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | `string` | Shot event ID (FK to xG Shot Data) |
| `match_id` | `bigint` | Match identifier |
| `competition_id` | `bigint` | Competition identifier |
| `season_id` | `bigint` | Season identifier |
| `player_x_norm` | `double` | Player x position normalized to [0, 1] from StatsBomb 120-yard pitch |
| `player_y_norm` | `double` | Player y position normalized to [0, 1] from StatsBomb 80-yard pitch |
| `is_keeper` | `boolean` | Whether the player is the goalkeeper |
| `is_teammate` | `boolean` | Whether the player is on the shooting team |

### Coordinate System

Raw positions are in the **StatsBomb coordinate system** (120 &times; 80 yards, origin at bottom-left, attacking direction left to right). The `player_x_norm` and `player_y_norm` columns are normalized:

```
x_norm = location_x / 120.0
y_norm = location_y / 80.0
```

## Data Sources

| Source | Coverage | License |
|--------|----------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) (360 subset) | ~323 matches with inline freeze frames | CC-BY 4.0 |

Only StatsBomb 360 matches include freeze-frame data. Non-360 StatsBomb matches and all Wyscout matches do not contribute to this dataset. The xG v2 model handles missing freeze frames by falling back to a zero context vector.

## Companion Resources

| Resource | Type | Description |
|----------|------|-------------|
| [xG v2 Set Encoder](https://huggingface.co/luxury-lakehouse/xg-v2-model-set-encoder) | Model | Deep Sets encoder + MLP that consumes this dataset as spatial context |
| [xG Shot Data](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data) | Dataset | Tabular shot features (joins on `event_id`) |
| [xG Model v1](https://huggingface.co/luxury-lakehouse/xg-model-statsbomb-wyscout) | Model | Tabular-only XGBoost baseline (no freeze-frame context) |

## Limitations

- **Partial visibility**: StatsBomb 360 captures only *visible* players. Players behind the camera or in crowded areas may be absent. The set encoder handles this gracefully (fewer rows per shot), but predictions may underestimate defensive pressure when defenders are occluded.
- **StatsBomb 360 only**: Covers ~323 of ~3,000 StatsBomb matches. The majority of shots in the xG Shot Data dataset do not have corresponding freeze-frame rows.
- **Shot events only**: This dataset extracts freeze frames for shots specifically. Freeze frames for other event types (passes, tackles) are not included.
- **No player identity**: The dataset includes spatial position and role flags only. Player name, jersey number, height, and other attributes are not captured.

## Citation

If you use this dataset, please cite StatsBomb and the Deep Sets architecture:

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
@inproceedings{zaheer2017deep,
  title={Deep Sets},
  author={Zaheer, Manzil and Kottur, Satwik and Ravanbakhsh, Siamak
          and P{\'o}czos, Barnab{\'a}s and Salakhutdinov, Ruslan
          and Smola, Alexander J.},
  booktitle={Advances in Neural Information Processing Systems (NeurIPS)},
  volume={30},
  year={2017}
}
```

## More Information

- **License**: [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- **Publishing script**: `scripts/publish_xg_shots_hf.py`
- **Platform**: [Luxury Lakehouse Soccer Analytics](https://github.com/karsten-s-nielsen/luxury-lakehouse)

## PR 7 changelog (2026-04-27)

The lineage mart `fct_shots` now carries Kimball surrogate FKs (`team_key`, `player_key`) alongside the existing `match_key` (PR 3) and the legacy `team_id`/`player_id` INT columns during the 2026-07-22 dual-column window per ADR-011. The freeze-frame payload itself is unchanged at this grain; the keys flow through `fct_xg_predictions_v2` (the v2 set-encoder mart) via INNER JOIN to `fct_shots`. PR 8 will sunset legacy IDs post-2026-07-22.
