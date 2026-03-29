---
language: [en]
license: mit
task_categories: [tabular-regression]
tags: [sports-analytics, soccer, football, pitch-control, tracking-data, spearman-2017, physics-model]
size_categories: [10M-100M]
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
---

# Pitch Control Tracking Data

Per-player per-frame pitch control values from **~38 million rows** of professional soccer tracking data across **20 matches** from three providers. Computed using the [Spearman (2017)](https://www.researchgate.net/publication/315166647_Beyond_Expected_Goals) physics-based model &mdash; each row contains one player's position, velocity, and the home-team control probability at that location.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/pitch-control-tracking")
df = ds["train"].to_pandas()

# Average home-team pitch control per match
home_control = df.groupby("match_id")["pitch_control_value"].mean()
print(home_control.describe())
```

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## What is Pitch Control?

**Pitch control** quantifies which team controls each location on the pitch at every moment in a match. The Spearman (2017) physics-based model estimates control by computing the time for each player to intercept a given point, accounting for reaction time and maximum acceleration kinematics. A logistic influence function converts time-to-intercept into a control probability, and each player's contribution is the fraction of their team's total influence at that location.

The result is a per-player control value: the home-team control probability [0, 1] at that player's current position at that instant in time.

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `tracking_id` | `string` | Primary key &mdash; unique player-frame identifier |
| `match_id` | `string` | Match identifier (prefixed by provider) |
| `player_id` | `string` | Player identifier |
| `team` | `string` | Team affiliation (home or away) |
| `period` | `int` | Match period (1 or 2) |
| `frame` | `int` | Frame number within the period |
| `timestamp_seconds` | `double` | Timestamp in seconds from period start |
| `x` | `double` | Player X coordinate (StatsBomb 120-yard scale) |
| `y` | `double` | Player Y coordinate (StatsBomb 80-yard scale) |
| `ball_x` | `double` | Ball X coordinate |
| `ball_y` | `double` | Ball Y coordinate |
| `velocity_x` | `double` | Player velocity X (coordinate-units/second) |
| `velocity_y` | `double` | Player velocity Y (coordinate-units/second) |
| `speed_ms` | `double` | Player speed in m/s |
| `pitch_control_value` | `double` | Home-team control probability [0, 1] at this player's position |
| `source_provider` | `string` | Tracking data provider (metrica, idsse, skillcorner) |
| `frame_rate` | `bigint` | Frame rate in fps (25 for Metrica/IDSSE, 10 for SkillCorner) |

### Coordinate System

All coordinates use the **StatsBomb 120&times;80 yards** scale. All three tracking providers are normalized to this coordinate system during ingestion. The origin (0, 0) is at the bottom-left corner of the pitch; x runs along the length (0&ndash;120 yards), y along the width (0&ndash;80 yards).

## Model

The pitch control values are computed using the **Spearman (2017) physics-based model**:

1. **Time-to-intercept**: For each player and each target location, compute the minimum time to reach that point given a reaction time plus kinematics under maximum acceleration.
2. **Logistic influence function**: Convert time-to-intercept to a control probability via a sigmoid function.
3. **Per-player control**: Each player's pitch control value is the fraction of their team's total logistic influence at the player's current position, relative to all players on the pitch.

The result is bounded [0, 1] where 1 = home team has full control and 0 = away team has full control.

## Data Sources

| Provider | Matches | Frame Rate | Competition |
|----------|---------|------------|-------------|
| [Metrica Sports](https://github.com/metrica-sports/sample-data) | 3 | 25 fps | Open sample matches |
| IDSSE Bundesliga | 7 | 25 fps | German Bundesliga |
| SkillCorner A-League | 10 | 10 fps | Australian A-League |

All providers use standardized tracking formats normalized to the StatsBomb coordinate scale.

## Limitations

- **Small sample**: Only 20 matches have full tracking data. Patterns may not generalize across leagues or tactical systems.
- **Variable frame rate**: Metrica and IDSSE track at 25 fps; SkillCorner at 10 fps. Time-series analyses should account for this difference.
- **Velocity estimation**: Velocity vectors are estimated from positional differences and may differ in smoothing method between providers.
- **Physics model approximation**: The Spearman (2017) model assumes constant maximum acceleration and uniform reaction time. It does not account for player fatigue, directional momentum, or tactical intent.
- **No goalkeeper distinction**: Goalkeepers are treated identically to outfield players in the control model.

## Citation

If you use this dataset, please cite the original pitch control paper:

```bibtex
@inproceedings{spearman2017beyond,
  title={Beyond Expected Goals},
  author={Spearman, William},
  booktitle={MIT Sloan Sports Analytics Conference},
  year={2017}
}
```

## Companion Resources

| Resource | Type | Description |
|----------|------|-------------|
| [OBSO/PAUSA Values](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-values) | Dataset | Off-ball scoring opportunities computed from pitch control surfaces |
| [Space Creation Values](https://huggingface.co/datasets/luxury-lakehouse/space-creation-values) | Dataset | Per-player space creation using differential pitch control |

## More Information

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

- **License**: [MIT](https://opensource.org/licenses/MIT)
