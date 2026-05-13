---
language: [en]
license: cc-by-nc-4.0
task_categories:
  - tabular-classification
tags:
  - sports-analytics
  - soccer
  - football
  - goalkeeper
  - expected-goals
  - psxg
  - predictions
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

# PSxG Predictions &mdash; Post-Shot Expected Goals per Shot

Per-shot PSxG scores produced by the [PSxG model](https://huggingface.co/luxury-lakehouse/psxg-model) (logistic regression on goalmouth coordinates). Used as the primary shot-stopping input to `fct_goalkeeper_stats` in the goalkeeper evaluation framework.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/psxg-predictions")
df = ds["train"].to_pandas()
print(f"{len(df)} on-target shot predictions")

# PSxG faced per goalkeeper (join with shots dataset for goals_conceded)
gp = df.groupby("player_id").agg(
    psxg_faced=("psxg", "sum"),
    shots_faced=("event_id", "count"),
)

print(gp.sort_values("psxg_faced", ascending=False).head(10))
```

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

## What Is This Dataset?

This dataset contains one row per on-target shot, with the PSxG score assigned by the [PSxG model](https://huggingface.co/luxury-lakehouse/psxg-model). PSxG is the estimated probability that a given on-target shot becomes a goal, conditioned on goalmouth position.

**Goals prevented** = sum(PSxG over shots faced) &minus; actual goals conceded.

- **Positive value** indicates the goalkeeper saved more goals than expected given shot difficulty.
- **Negative value** indicates the goalkeeper conceded more goals than expected.

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | `string` | Unique StatsBomb event identifier (join key to source shot events) |
| `match_id` | `Int64` | Match identifier |
| `player_id` | `Int64` | Goalkeeper player identifier (the keeper who faced the shot) |
| `psxg` | `float64` | Post-Shot Expected Goals: probability the shot becomes a goal [0, 1] |

### Interpreting PSxG

| PSxG Range | Meaning |
|-----------|---------|
| 0.80&ndash;1.00 | Near-certain goal (top corner, unstoppable) |
| 0.40&ndash;0.80 | Difficult save required |
| 0.10&ndash;0.40 | Moderate difficulty |
| 0.00&ndash;0.10 | Routine save (central, low, slow) |

## Data Sources

Predictions are generated from the [PSxG model](https://huggingface.co/luxury-lakehouse/psxg-model) applied to on-target shots in [StatsBomb Open Data](https://github.com/statsbomb/open-data).

| Source | On-Target Shots | License |
|--------|----------------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~15K | CC-BY 4.0 |

## Use Cases

- **Goalkeeper benchmarking**: Aggregate `goals_prevented = sum(psxg) - actual_goals` per goalkeeper to rank shot-stopping performance
- **Season analysis**: Track a goalkeeper's PSxG performance over a season to distinguish form from underlying difficulty
- **Squad analysis**: Compare squad goalkeepers on shot-stopping contribution beyond raw save percentage
- **Research**: Evaluate custom PSxG models against this logistic regression baseline

## Limitations

- **StatsBomb only**: Predictions are generated only for shots with StatsBomb goalmouth coordinates (`end_location_z`). No Wyscout coverage.
- **Two-feature model**: PSxG is conditioned only on `end_location_y` and `end_location_z`. Shot speed, trajectory, and defensive pressure are not modeled.
- **No keeper position conditioning**: The model does not observe the goalkeeper's starting position or reaction. Saves from unconventional positions may appear easier than they were.
- **Goalkeeper attribution**: `player_id` identifies the goalkeeper who faced the shot, derived from the StatsBomb event data keeper field.

## Citation

If you use this dataset, please cite:

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
| [PSxG Model](https://huggingface.co/luxury-lakehouse/psxg-model) | Logistic regression PSxG model that produced these predictions |
| [On-Target Shot Data](https://huggingface.co/datasets/luxury-lakehouse/statsbomb-shots-on-target) | Input dataset: ~15K StatsBomb on-target shots with goalmouth coordinates |
| [xG Shot Data](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data) | Full shot dataset with pre-shot xG features (StatsBomb + Wyscout) |

## More Information

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

- **Model repo**: [`luxury-lakehouse/psxg-model`](https://huggingface.co/luxury-lakehouse/psxg-model)
- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/)
- **Platform**: [Luxury Lakehouse Soccer Analytics](https://github.com/karsten-s-nielsen/luxury-lakehouse)
