---
language: [en]
license: cc-by-4.0
task_categories:
  - tabular-classification
tags:
  - sports-analytics
  - soccer
  - football
  - goalkeeper
  - expected-goals
  - psxg
  - shots
  - statsbomb
size_categories:
  - 10K-100K
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
    default: true
---

# StatsBomb On-Target Shots &mdash; Goalmouth Coordinates

**~15K on-target shots** from [StatsBomb Open Data](https://github.com/statsbomb/open-data) with goalmouth coordinates (`end_location_y`, `end_location_z`). Primary training input for the [PSxG model](https://huggingface.co/luxury-lakehouse/psxg-model) used in goalkeeper shot-stopping evaluation.

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

ds = load_dataset("luxury-lakehouse/statsbomb-shots-on-target")
df = ds["train"].to_pandas()
print(f"{len(df)} on-target shots")

# Goal rate by goalmouth zone
df["height_zone"] = df["end_location_z"].apply(
    lambda z: "high" if z > 0.6 else ("mid" if z > 0.3 else "low")
)
df.groupby("height_zone")["is_goal"].mean()
```

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## What Is This Dataset?

This dataset contains every on-target shot from the StatsBomb open data collection that includes goalmouth coordinates. It is the training corpus for the PSxG (Post-Shot Expected Goals) model, which estimates the probability that an on-target shot becomes a goal given where it was headed.

Only shots with `shot_outcome` in `{Saved, Goal, Post}` are included. Blocked shots and wayward shots are excluded because they never reach the goalkeeper.

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | `string` | Unique StatsBomb event identifier |
| `match_key` | `Int64` | **Canonical Kimball match FK** (ADR-011). BIGINT surrogate, collision-free across providers. |
| `match_id` | `Int64` | LEGACY provider-native match identifier; sunset 2026-07-22 (see top-of-card). |
| `player_id` | `Int64` | Shooter player identifier |
| `end_location_y` | `float64` | Normalized horizontal goalmouth position [0, 1] (0 = left post, 1 = right post) |
| `end_location_z` | `float64` | Normalized vertical goalmouth position [0, 1] (0 = ground level, 1 = crossbar) |
| `shot_outcome` | `string` | Outcome: `Saved`, `Goal`, or `Post` |
| `is_goal` | `bool` | Target variable &mdash; `true` if `shot_outcome = 'Goal'` |

### Coordinate System

Goalmouth coordinates use the **StatsBomb 360 coordinate system**, normalized to [0, 1]:

- **`end_location_y`**: Raw StatsBomb y is 36&ndash;44 yards (goalpost to goalpost, 8 yards wide). Normalized: (y &minus; 36) / 8.
- **`end_location_z`**: Raw StatsBomb z is 0&ndash;2.44 meters (ground to crossbar). Normalized: z / 2.44.

Values outside [0, 1] (shots that miss via height or width but are still classified as "Saved") are clipped at 0 and 1.

## Data Sources

| Source | On-Target Shots | License |
|--------|----------------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~15K | CC-BY 4.0 |

Coverage includes the Premier League, La Liga, Serie A, Bundesliga, Ligue 1, Champions League, World Cup, and more (StatsBomb 360-enabled competitions only, as `end_location_z` requires 360 data).

## Use Cases

- **PSxG model training**: Primary training input for the [PSxG model](https://huggingface.co/luxury-lakehouse/psxg-model)
- **Goalkeeper benchmarking**: Analyze shot difficulty distributions faced by individual goalkeepers
- **Shot analysis**: Visualize goalmouth heatmaps by outcome, competition, or position
- **Custom models**: Train alternative PSxG architectures (e.g., kernel density, neural nets) on the same standardized dataset

## Limitations

- **StatsBomb only**: `end_location_z` is not available in Wyscout or other open providers. This dataset is StatsBomb-only.
- **360-enabled competitions**: Only StatsBomb competitions with 360 data have goalmouth z-coordinates. Earlier StatsBomb open data lacks the z-dimension.
- **Open data only**: Contains only publicly available StatsBomb shots. Commercial datasets cover more competitions and seasons.
- **No keeper position**: This dataset does not include the goalkeeper's starting position or movement. For freeze-frame context, see [xG Freeze Frame Data](https://huggingface.co/datasets/luxury-lakehouse/xg-freeze-frame-data).
- **Clipped coordinates**: A small fraction of "Saved" shots have raw coordinates outside the goalmouth geometry (e.g., diving saves). These are clipped to [0, 1].

## Citation

If you use this dataset, please cite:

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
@article{butcher2025xgot,
  title={An Expected Goals On Target (xGOT) Model},
  author={Butcher, J. and others},
  journal={Big Data and Cognitive Computing},
  volume={9},
  number={3},
  pages={64},
  year={2025},
  publisher={MDPI},
  url={https://www.mdpi.com/2504-2289/9/3/64}
}
```

```bibtex
@software{nielsen2026psxg,
  title={PSxG Model: Post-Shot Expected Goals for Goalkeeper Evaluation},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Companion Resources

| Resource | Description |
|----------|-------------|
| [PSxG Model](https://huggingface.co/luxury-lakehouse/psxg-model) | Logistic regression PSxG model trained on this dataset |
| [PSxG Predictions](https://huggingface.co/datasets/luxury-lakehouse/psxg-predictions) | Per-shot PSxG predictions with player and match identifiers |
| [xG Shot Data](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data) | Full shot dataset with pre-shot features (StatsBomb + Wyscout) |
| [xG Freeze Frame Data](https://huggingface.co/datasets/luxury-lakehouse/xg-freeze-frame-data) | Player positions at shot time for context-conditioned xG models |

## More Information

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

- **Model repo**: [`luxury-lakehouse/psxg-model`](https://huggingface.co/luxury-lakehouse/psxg-model)
- **License**: [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/) (StatsBomb Open Data)
- **Platform**: [Luxury Lakehouse Soccer Analytics](https://github.com/karsten-s-nielsen/luxury-lakehouse)
