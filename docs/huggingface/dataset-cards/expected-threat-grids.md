---
language: [en]
license: mit
task_categories: [tabular-regression]
tags:
  - sports-analytics
  - soccer
  - football
  - expected-threat
  - xT
  - markov-chain
  - analytics
size_categories: [n<1K]
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
---

# Expected Threat (xT) Grids

Markov chain expected threat grids computed via value iteration &mdash; **96 cells per competition** on a 12&times;8 grid. Each cell quantifies the probability that a possession starting in that zone will end in a goal, derived from observed transition and shot frequencies across ~4,900 matches.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset
import pandas as pd

ds = load_dataset("luxury-lakehouse/expected-threat-grids")
df = ds["train"].to_pandas()

# Pivot the global grid into a 12x8 matrix (attacking direction left-to-right)
global_grid = df[df["competition_id"] == "global"]
xt_matrix = global_grid.pivot(index="zone_y", columns="zone_x", values="xt_value")
print(xt_matrix)
```

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

## What Is This Dataset?

**Expected Threat (xT)** is a Markov chain model that assigns a goal-scoring threat value to every zone on the pitch. The pitch is discretized into a 12&times;8 grid (96 cells), and transition probabilities between zones are estimated from observed event data. Value iteration propagates goal-scoring probability backward from the opponent goal to produce an expected threat surface.

Each cell represents a ~8.75m &times; 8.5m pitch zone. A player who receives the ball in a zone with xT = 0.05 is in a location from which possessions historically result in a goal 5% of the time.

> Singh, K. (2018). **Introducing Expected Threat (xT).** [karun.in/blog/expected-threat.html](https://karun.in/blog/expected-threat.html)

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `zone_x` | `int` | Grid x-coordinate (0&ndash;11, left-to-right in attacking direction) |
| `zone_y` | `int` | Grid y-coordinate (0&ndash;7, pitch width) |
| `xt_value` | `float` | Expected threat value (0&ndash;1, higher = more threatening) |
| `competition_id` | `string` | Competition identifier, or `"global"` for the cross-competition aggregate |

### Coordinate System

The grid maps to the **SPADL academic standard**: 105&times;68 meters. Each of the 96 cells covers approximately 8.75m (length) &times; 8.5m (width). `zone_x = 0` is the defending goal line; `zone_x = 11` is nearest the attacking goal. `zone_y = 0` is the left touchline; `zone_y = 7` is the right touchline.

## Data Sources

| Source | Matches | License |
|--------|---------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~3,000 | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | ~1,900 | CC-BY-NC 4.0 |

All event data is converted to SPADL format before transition/shot frequency estimation.

## Companion Resources

| Resource | Description |
|----------|-------------|
| [SPADL/VAEP Action Values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Per-action VAEP scores from the same source events |
| [OBSO Trained Grids](https://huggingface.co/datasets/luxury-lakehouse/obso-trained-grids) | Reachability, EPV, and completion grids for OBSO computation |

## Limitations

- **Spatial resolution**: The 12&times;8 grid is deliberately coarse. Sub-zone variation (e.g., center vs. wing within a cell) is averaged away.
- **Competition-agnostic global grid**: The `"global"` grid pools all competitions. Tactical differences between leagues (e.g., Bundesliga pressing vs. Serie A low-block) are smoothed out.
- **Static model**: xT values are computed from full-season aggregates. They do not adapt to in-game state (score, time, personnel).
- **Open data only**: Trained on publicly available StatsBomb and Wyscout data. Commercial datasets with richer coverage may yield different threat surfaces.

## Citation

If you use this dataset, please cite the original xT blog post:

```bibtex
@misc{singh2018expected,
  title={Introducing Expected Threat (xT)},
  author={Singh, Karun},
  year={2018},
  url={https://karun.in/blog/expected-threat.html}
}
```

## More Information

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

- **License**: [MIT](https://opensource.org/licenses/MIT)
- **Publish script**: `scripts/compute_xt_grid_hf.py`
