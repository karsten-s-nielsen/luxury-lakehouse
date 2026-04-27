---
language: [en]
license: mit
task_categories: [tabular-regression]
tags:
  - sports-analytics
  - soccer
  - football
  - space-creation
  - pitch-control
  - obso
  - analytics
  - idsse
  - bundesliga
size_categories: [100K<n<1M]
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
---

# Space Creation Values &mdash; ELASTIC/OBSO

Per-player per-frame space creation quantification &mdash; measuring each player's contribution to off-ball scoring opportunities via **differential OBSO**. For every sampled frame, the model computes OBSO with and without each player, yielding the area of scoring opportunity that player creates (or destroys) by their positioning.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/space-creation-values")
df = ds["train"].to_pandas()

# Top 10 players by average net space created per frame
avg_space = df.groupby("player_id")["net_space_m2"].mean().nlargest(10)
print(avg_space)
```

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## What Is This Dataset?

**Space creation** quantifies each player's off-ball contribution by measuring the change in OBSO (Off-Ball Scoring Opportunity) caused by their presence on the pitch. For each frame and each player:

1. Compute the OBSO surface with all players present.
2. Remove the player and recompute the OBSO surface.
3. The difference yields two quantities: **space created** (zones where OBSO increased due to the player) and **space destroyed** (zones where OBSO decreased).

This approach operationalizes the concept from Fernandez & Bornn (2018) &mdash; players who intelligently position themselves create exploitable space for teammates even without touching the ball.

The grid resolution is **52&times;34 cells** over a 105&times;68m pitch, giving a cell area of approximately 4.04 m&sup2;. Frames are sampled at 1fps (every 25th frame from the 25fps tracking source) to keep computation tractable.

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | `string` | Match identifier |
| `frame_id` | `int` | Tracking frame number (25fps source, sampled every 25th frame) |
| `player_id` | `string` | DFL PersonId |
| `team` | `string` | Team affiliation (`home` or `away`) |
| `period` | `int` | Match period (1 or 2) |
| `space_created_m2` | `double` | OBSO area added by this player's presence (&ge; 0, in m&sup2;) |
| `space_destroyed_m2` | `double` | OBSO area removed by this player's presence (&le; 0, in m&sup2;) |
| `net_space_m2` | `double` | Net OBSO contribution (positive = beneficial, in m&sup2;) |

### Coordinate System

The underlying grid maps to the **SPADL 105&times;68 meters** pitch. Grid resolution: **52&times;34 cells** (~2.02m &times; 2.0m per cell, ~4.04 m&sup2; per cell). Frame sampling: every 25th frame (1fps from 25fps source).

## Data Sources

| Source | Matches | License |
|--------|---------|---------|
| [IDSSE Open Data](https://www.nature.com/articles/s41597-025-04507-0) | ~3 | CC-BY 4.0 |

Computed from IDSSE 25fps tracking data via the ELASTIC event-tracking synchronization pipeline and differential OBSO computation.

## Companion Resources

| Resource | Description |
|----------|-------------|
| [OBSO/PAUSA Values](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-values) | Per-pass OBSO and PAUSA scores |
| [Pitch Control Tracking](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) | Per-player per-frame pitch control values |
| [OBSO/PAUSA Inputs](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-inputs) | Source events and ELASTIC sync data |

## Limitations

- **Small sample**: Only ~3 Bundesliga matches from the IDSSE open data release. Player-level aggregates should not be interpreted as career profiles.
- **1fps sampling**: Frames are sampled at 1fps (every 25th frame). Rapid positional changes between samples are not captured.
- **Differential assumption**: Removing a player and recomputing OBSO does not account for how teammates would reposition in that player's absence. The counterfactual is "same formation minus one player," not "adjusted formation."
- **No opponent reaction modeling**: The model assumes opponents do not adjust their positioning in response to the removed player.
- **Grid resolution**: At 52&times;34 cells, each cell covers ~4 m&sup2;. Sub-cell positioning differences are averaged away.
- **Computational cost**: Differential OBSO requires N+1 pitch control computations per frame (one baseline plus one per player). This limits the practical frame rate for large-scale computation.

## Citation

If you use this dataset, please cite the following:

```bibtex
@inproceedings{fernandez2018wide,
  title={Wide Open Spaces: A statistical technique for measuring space creation in professional soccer},
  author={Fernandez, Javier and Bornn, Luke},
  booktitle={MIT Sloan Sports Analytics Conference},
  year={2018}
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
@article{bassek2025idsse,
  title={An integrated dataset of spatiotemporal and event data in elite soccer},
  author={Bassek, Manuel and Weber, Henrik and Rein, Robert and Memmert, Daniel},
  journal={Scientific Data},
  volume={12},
  pages={283},
  year={2025},
  publisher={Nature Publishing Group}
}
```

## More Information

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

- **License**: [MIT](https://opensource.org/licenses/MIT)
- **Publish script**: `scripts/compute_space_creation_hf.py`

## PR 7 changelog (2026-04-27)

The upstream gold mart `fct_space_creation` now carries Kimball surrogate FKs (`match_key`, `player_key`, `data_source`) alongside the legacy native columns during the 2026-07-22 dual-column window per ADR-011. Per-row `team` remains a `home`/`away` role string — team-level resolution is deferred until a use case demands it. PR 8 will sunset the legacy `*_id` columns post-2026-07-22.
