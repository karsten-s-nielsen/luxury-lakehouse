---
language: [en]
license: mit
task_categories: [tabular-regression]
tags:
  - sports-analytics
  - soccer
  - football
  - obso
  - epv
  - transition
  - reachability
  - analytics
size_categories: [10K<n<100K]
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
---

# OBSO Trained Grids &mdash; Reachability, EPV, and Completion Matrices

Pre-trained grid artifacts for **Off-Ball Scoring Opportunity (OBSO)** computation: ball reachability surfaces, expected possession value (EPV) grids, and pass completion probability matrices. These are the static lookup tables that power real-time OBSO evaluation &mdash; derived from observed passing, shooting, and transition patterns across ~4,900 open-data matches.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from huggingface_hub import hf_hub_download
import pandas as pd

repo = "luxury-lakehouse/obso-trained-grids"

# Load the global reachability grid (100x64)
reach_path = hf_hub_download(repo, "data/reachability_grid_global.parquet", repo_type="dataset")
reach_df = pd.read_parquet(reach_path)
reach_matrix = reach_df.pivot(index="zone_y", columns="zone_x", values="reachability")
print(f"Reachability grid shape: {reach_matrix.shape}")  # (100, 64)

# Load the global EPV grid (50x32)
epv_path = hf_hub_download(repo, "data/epv_grid_global.parquet", repo_type="dataset")
epv_df = pd.read_parquet(epv_path)
epv_matrix = epv_df.pivot(index="zone_y", columns="zone_x", values="epv_value")
print(f"EPV grid shape: {epv_matrix.shape}")  # (50, 32)
```

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

## What Is This Dataset?

The OBSO model evaluates off-ball scoring opportunities by combining three components:

1. **Reachability** &mdash; the probability that a ball played to a given zone can be controlled by the receiving team, based on observed reception patterns.
2. **Expected Possession Value (EPV)** &mdash; the probability that a possession in a given zone will result in a goal, estimated via value iteration over transition and shot frequencies.
3. **Pass Completion Probability** &mdash; the likelihood that a pass from one zone to another is completed, estimated from observed pass outcomes.

These grids are pre-computed from event data and serve as static inputs to the real-time OBSO pipeline, which combines them with dynamic pitch control surfaces.

## Data Fields

The dataset contains three types of grid artifacts at different resolutions, plus per-competition variants.

### Reachability Grid (`reachability_grid_global.parquet`)

| Column | Type | Description |
|--------|------|-------------|
| `zone_y` | `int` | Grid row index (0&ndash;99) |
| `zone_x` | `int` | Grid column index (0&ndash;63) |
| `reachability` | `float` | Ball reachability probability (0&ndash;1) |

Grid resolution: **100&times;64** (105m &divide; 100 = 1.05m per row, 68m &divide; 64 = 1.0625m per column).

### EPV Grid (`epv_grid_global.parquet`)

| Column | Type | Description |
|--------|------|-------------|
| `zone_y` | `int` | Grid row index (0&ndash;49) |
| `zone_x` | `int` | Grid column index (0&ndash;31) |
| `epv_value` | `float` | Expected possession value (0&ndash;1, higher = closer to goal) |

Grid resolution: **50&times;32** (105m &divide; 50 = 2.1m per row, 68m &divide; 32 = 2.125m per column).

### Completion Matrix (`completion_matrix_global.parquet`)

| Column | Type | Description |
|--------|------|-------------|
| `origin_zone` | `int` | Flat index of the origin zone (0&ndash;399) |
| `target_zone` | `int` | Flat index of the target zone (0&ndash;399) |
| `probability` | `float` | Pass completion probability (row-normalized, 0&ndash;1) |

Zone grid: **25&times;16 = 400 zones** (105m &divide; 25 = 4.2m per row, 68m &divide; 16 = 4.25m per column).

### Per-Competition Variants

Per-competition files (`*_all.parquet`) include an additional `competition_id` column. These allow competition-specific OBSO computation where sufficient data exists.

### Coordinate System

All grids map to the **SPADL 105&times;68 meters** pitch coordinate space. Grid resolution varies by artifact type (see tables above). The origin (0, 0) is at the bottom-left corner of the attacking team's half.

## Data Sources

| Source | Matches | License |
|--------|---------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~3,000 | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | ~1,900 | CC-BY-NC 4.0 |

All event data is converted to SPADL format before grid estimation.

## Companion Resources

| Resource | Description |
|----------|-------------|
| [OBSO/PAUSA Values](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-values) | Per-pass OBSO and PAUSA scores computed using these grids |
| [Expected Threat (xT) Grids](https://huggingface.co/datasets/luxury-lakehouse/expected-threat-grids) | Markov chain xT grids from the same source data |
| [SPADL/VAEP Action Values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Per-action VAEP scores from the same source events |

## Limitations

- **Open data only**: Grids are trained on publicly available StatsBomb and Wyscout data. Commercial datasets with denser event coverage may yield different surfaces.
- **Competition-agnostic global grids**: The global variants pool all competitions. League-specific tactical patterns (e.g., high-press vs. low-block) are averaged away.
- **Static estimates**: Grids are computed from full-season aggregates. They do not adapt to in-game state (score, time, fatigue, personnel).
- **Resolution trade-offs**: Each grid uses a different resolution optimized for its purpose. Interpolation is required when combining grids of different resolutions.
- **No goalkeeper modeling**: Reachability and EPV grids do not distinguish goalkeeper positioning from outfield player patterns.

## Citation

If you use this dataset, please cite the underlying models:

```bibtex
@misc{singh2018expected,
  title={Introducing Expected Threat (xT)},
  author={Singh, Karun},
  year={2018},
  url={https://karun.in/blog/expected-threat.html}
}
```

```bibtex
@inproceedings{spearman2018beyond,
  title={Beyond Expected Goals},
  author={Spearman, William},
  booktitle={MIT Sloan Sports Analytics Conference},
  year={2018}
}
```

```bibtex
@inproceedings{fernandez2018wide,
  title={Wide Open Spaces: A statistical technique for measuring space creation in professional soccer},
  author={Fernandez, Javier and Bornn, Luke},
  booktitle={MIT Sloan Sports Analytics Conference},
  year={2018}
}
```

```bibtex
@inproceedings{lee2026pausa,
  title={Valuing La Pausa: Quantifying the Timing and Quality of Soccer Passes Using Off-Ball Scoring Opportunities},
  author={Lee, Minho and Jo, Hyunsung and Hong, Seungwon and Bauer, Pascal and Ko, Sangkuk},
  booktitle={MIT Sloan Sports Analytics Conference},
  year={2026}
}
```

## More Information

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

- **License**: [MIT](https://opensource.org/licenses/MIT)
- **Publish script**: `scripts/compute_epv_transition_hf.py`
