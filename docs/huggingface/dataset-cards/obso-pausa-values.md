---
language: [en]
license: mit
task_categories: [tabular-regression]
tags:
  - sports-analytics
  - soccer
  - football
  - obso
  - pausa
  - pitch-control
  - pass-timing
  - analytics
  - idsse
  - bundesliga
size_categories: [1K<n<10K]
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
---

# OBSO/PAUSA Values &mdash; Pass Timing and Target Quality

Per-pass **Off-Ball Scoring Opportunity (OBSO)** and **PAUSA** scores measuring pass timing quality and spatial target selection. Each row captures the actual, peak, and optimal OBSO at the moment of a pass, plus two derived ratios: **temporal judgment** (did the passer release at the right moment?) and **spatial selection** (did the passer choose the best available target?).

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/obso-pausa-values")
df = ds["train"].to_pandas()

# Top 10 passes by temporal judgment (best-timed releases)
best_timed = df.nlargest(10, "temporal_judgment")[
    ["match_id", "player_id", "temporal_judgment", "spatial_selection", "actual_obso"]
]
print(best_timed)
```

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## What Is This Dataset?

**OBSO** (Off-Ball Scoring Opportunity) quantifies the goal-scoring threat created by off-ball positioning at the moment a pass is released. The model computes pitch control over a grid, multiplies by expected possession value (EPV), and sums the resulting surface to produce a single scalar threat value.

**PAUSA** ("La Pausa") extends OBSO by evaluating pass timing and target selection:

- **Temporal judgment** = `actual_obso / peak_obso` &mdash; how close to the optimal release moment the passer acted. A value of 1.0 means the pass was released at the exact peak of opportunity.
- **Spatial selection** = `actual_obso / optimal_obso` &mdash; how well the chosen target compares to the best available teammate. A value of 1.0 means the passer selected the highest-value target.

These metrics are described in Lee, Jo, Hong, Bauer, & Ko (2026), building on the pitch control and EPV frameworks of Spearman (2018) and Fernandez & Bornn (2018).

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | `string` | Match identifier |
| `pass_id` | `string` | Composite key uniquely identifying the pass |
| `event_id` | `string` | Source event identifier |
| `player_id` | `string` | DFL PersonId of the passer |
| `team` | `string` | Team affiliation (`home` or `away`) |
| `period` | `int` | Match period (1 or 2) |
| `timestamp_seconds` | `double` | Elapsed time in seconds from period start |
| `frame_id` | `int` | Tracking frame at the moment of the pass |
| `ball_x` | `double` | Ball x-coordinate at release (StatsBomb 120-yard scale) |
| `ball_y` | `double` | Ball y-coordinate at release (StatsBomb 80-yard scale) |
| `receiver_x` | `double` | Target x-coordinate (StatsBomb 120-yard scale) |
| `receiver_y` | `double` | Target y-coordinate (StatsBomb 80-yard scale) |
| `actual_obso` | `double` | OBSO at the moment of pass release |
| `peak_obso` | `double` | Maximum OBSO observed in the ghost trajectory window |
| `optimal_obso` | `double` | Maximum OBSO across all teammate target locations |
| `temporal_judgment` | `double` | Timing quality (0&ndash;1, higher = better; actual/peak) |
| `spatial_selection` | `double` | Target quality (0&ndash;1, higher = better; actual/optimal) |
| `alignment_confidence` | `double` | ELASTIC sync confidence (0&ndash;1) for the event-tracking alignment |

### Coordinate System

All coordinates use the **StatsBomb 120&times;80 yards** scale. The origin (0, 0) is at the bottom-left corner of the pitch. The pitch control grid resolution is **104&times;68 cells**. Ghost trajectory window: **3.0 seconds before** to **1.0 second after** the event, sampled at 25fps.

## Data Sources

| Source | Matches | License |
|--------|---------|---------|
| [IDSSE Open Data](https://www.nature.com/articles/s41597-025-04507-0) | ~3 | CC-BY 4.0 |

Computed from IDSSE tracking data via ELASTIC event-tracking synchronization. Event coordinates are normalized to the StatsBomb scale during the OBSO pipeline.

**PR 7 (ADR-013 second application — 2026-04-27):** the gold mart `fct_pausa_values` is now built by dbt with `contract: enforced: true` from `bronze.pausa_values` (the writer `src/ingestion/pausa.py` retargets bronze; previously wrote gold directly). Mart inherits Kimball surrogate FKs (`match_key`, `team_key`, `player_key`) via `INNER JOIN fct_passes ON pass_id`. The published HF dataset payload gains the new key columns alongside the legacy native IDs during the 2026-07-22 dual-column window; consumers should migrate joins from `pass_id`/`match_id` to `match_key` for cross-provider stability.

## Companion Resources

| Resource | Description |
|----------|-------------|
| [OBSO/PAUSA Inputs](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-inputs) | Source events and ELASTIC sync used as inputs |
| [OBSO Trained Grids](https://huggingface.co/datasets/luxury-lakehouse/obso-trained-grids) | Reachability, EPV, and completion grids powering the OBSO model |
| [Pitch Control Tracking](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) | Per-player per-frame pitch control from tracking data |

## Limitations

- **IDSSE matches only**: Approximately 3 Bundesliga matches from the IDSSE open data release. The sample is too small to draw league-wide conclusions.
- **ELASTIC alignment accuracy**: OBSO values depend on correct event-tracking synchronization. Events with low `alignment_confidence` may have pitch control computed at the wrong frame.
- **Relative scores**: PAUSA temporal judgment and spatial selection are ratios, not absolute quality measures. A temporal judgment of 0.8 means 80% of the peak opportunity was captured &mdash; whether 0.8 is "good" depends on the difficulty of the pass context.
- **Ghost trajectory assumption**: The ghost window assumes players continue their current velocity for 3 seconds before the event. Sudden direction changes or stops are not modeled.
- **No opponent intent modeling**: Pitch control treats defenders as physics objects. Tactical intent (e.g., deliberate pressing traps) is not captured.

## Citation

If you use this dataset, please cite the following:

```bibtex
@inproceedings{lee2026pausa,
  title={Valuing La Pausa: Quantifying the Timing and Quality of Soccer Passes Using Off-Ball Scoring Opportunities},
  author={Lee, Minho and Jo, Hyunsung and Hong, Seungwon and Bauer, Pascal and Ko, Sangkuk},
  booktitle={MIT Sloan Sports Analytics Conference},
  year={2026}
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
@inproceedings{kim2025elastic,
  title={ELASTIC: Event-Tracking Data Synchronization in Soccer Without Annotated Event Locations},
  author={Kim, Hyunsung and Theis, Fabian and Kim, Hanjun},
  booktitle={ECML-PKDD MLSA Workshop},
  year={2025},
  eprint={2508.09238},
  archiveprefix={arXiv}
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
- **Publish script**: `scripts/compute_obso_hf.py`
