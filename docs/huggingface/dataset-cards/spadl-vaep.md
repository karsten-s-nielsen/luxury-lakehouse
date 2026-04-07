---
language: [en]
license: mit
task_categories: [tabular-classification, tabular-regression]
tags: [sports-analytics, soccer, football, spadl, vaep, action-valuation, statsbomb, wyscout]
size_categories: [1M-10M]
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
---

# SPADL/VAEP Action Values

Every on-ball action from **~9.5 million** professional soccer events, converted to the [SPADL](https://github.com/karsten-s-nielsen/silly-kicks) unified format and scored with offensive, defensive, and net VAEP values. Built with the [silly-kicks](https://github.com/karsten-s-nielsen/silly-kicks) library &mdash; enabling player ranking by total contribution beyond goals and assists.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/spadl-vaep-action-values")
df = ds["train"].to_pandas()

# Top 10 players by total VAEP contribution
top_players = df.groupby("player_id")["vaep_value"].sum().nlargest(10)
```

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## What is SPADL/VAEP?

**SPADL** (Soccer Player Action Description Language) is a unified event representation that converts vendor-specific event streams into 23 canonical action types with standardized coordinates (105&times;68 meters). This enables cross-source analysis that would otherwise require bespoke adapters per data provider.

**VAEP** (Valuing Actions by Estimating Probabilities) scores each action by its impact on scoring and conceding probabilities, as described in:

> Decroos, T., Bransen, L., Van Haaren, J., & Davis, J. (2019). **Actions Speak Louder than Goals: Valuing Player Actions in Soccer**. *Proceedings of the 25th ACM SIGKDD International Conference on Knowledge Discovery & Data Mining*.

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `action_value_id` | `string` | Surrogate key (deterministic hash of match_id + period + time_seconds + player_id + type_id + data_source) |
| `match_id` | `string` | Unique match identifier |
| `player_id` | `string` | Player identifier (source-native) |
| `team_id` | `string` | Team identifier |
| `competition_id` | `string` | Competition identifier |
| `season_id` | `string` | Season identifier |
| `period` | `int` | Match period (1 = first half, 2 = second half, 3+ = extra time) |
| `time_seconds` | `double` | Elapsed time within the period in seconds |
| `minute` | `int` | Match minute |
| `second` | `int` | Second within the minute |
| `start_x` | `double` | Action start x-coordinate (meters, 0&ndash;105) |
| `start_y` | `double` | Action start y-coordinate (meters, 0&ndash;68) |
| `end_x` | `double` | Action end x-coordinate (meters, 0&ndash;105) |
| `end_y` | `double` | Action end y-coordinate (meters, 0&ndash;68) |
| `action_type` | `string` | SPADL action type (see vocabulary below) |
| `action_result` | `string` | Action outcome (success, fail, offside, owngoal, yellow_card, red_card) |
| `bodypart` | `string` | Body part used (foot, head, other, head/other) |
| `offensive_value` | `double` | VAEP offensive value &mdash; change in scoring probability |
| `defensive_value` | `double` | VAEP defensive value &mdash; change in conceding probability |
| `vaep_value` | `double` | Net VAEP value (offensive_value + defensive_value) |
| `data_source` | `string` | Origin data provider (`statsbomb` or `wyscout`) |
| `original_event_id` | `string` | Event ID from the source provider |

### Coordinate System

All coordinates use the **SPADL academic standard**: 105&times;68 meters representing real-world pitch dimensions. The origin (0, 0) is at the bottom-left corner of the attacking team's half. The x-axis runs along the length of the pitch (0&ndash;105m), and the y-axis runs along the width (0&ndash;68m).

### Action Type Vocabulary

SPADL defines 23 canonical action types:

`pass`, `cross`, `throw_in`, `freekick_crossed`, `freekick_short`, `corner_crossed`, `corner_short`, `take_on`, `foul`, `tackle`, `interception`, `shot`, `shot_penalty`, `shot_freekick`, `keeper_save`, `keeper_claim`, `keeper_punch`, `keeper_pick_up`, `clearance`, `bad_touch`, `non_action`, `dribble`, `goalkick`

## Data Sources

| Source | Matches | License |
|--------|---------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~3,000 | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | ~1,900 | CC-BY-NC 4.0 |

Coverage includes the Premier League, La Liga, Serie A, Bundesliga, Ligue 1, Champions League, World Cup, and more.

## Limitations

- **Open data only**: Trained on publicly available StatsBomb and Wyscout data. Commercial datasets with richer event annotations may yield different VAEP scores.
- **No tracking data**: VAEP is event-based. Off-ball positioning, pressing intensity, and space creation are not captured.
- **Competition-agnostic model**: The underlying XGBoost model is trained across all competitions jointly. League-specific models may produce more calibrated probabilities.
- **Cross-source alignment**: StatsBomb and Wyscout use different event taxonomies. The SPADL adapter normalizes them, but subtle differences in event definitions (e.g., duel classification) remain.

## Citation

If you use this dataset, please cite the original VAEP paper:

```bibtex
@inproceedings{decroos2019actions,
  title={Actions Speak Louder than Goals: Valuing Player Actions in Soccer},
  author={Decroos, Tom and Bransen, Lotte and Van Haaren, Jan and Davis, Jesse},
  booktitle={Proceedings of the 25th ACM SIGKDD International Conference on Knowledge Discovery and Data Mining},
  pages={1851--1861},
  year={2019},
  publisher={ACM}
}
```

And the silly-kicks library:

```bibtex
@article{silly-kicks,
  title={silly-kicks: A Python library for valuing soccer actions},
  author={Decroos, Tom and Van Haaren, Jan and Davis, Jesse},
  year={2020},
  url={https://github.com/karsten-s-nielsen/silly-kicks}
}
```

## Companion Resources

| Resource | Type | Description |
|----------|------|-------------|
| [VAEP Model](https://huggingface.co/luxury-lakehouse/vaep-model-statsbomb-wyscout) | Model | P(scores) + P(concedes) XGBClassifiers trained on this dataset |
| [Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) | Dataset | Behavioral + statistical vectors derived from SPADL actions |

## More Information

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

- **License**: [MIT](https://opensource.org/licenses/MIT)
