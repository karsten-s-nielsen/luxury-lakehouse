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
  - elastic-sync
  - event-data
  - idsse
  - bundesliga
size_categories: [1K<n<10K]
configs:
  - config_name: events
    data_files:
      - split: train
        path: "data/events/**/*.parquet"
  - config_name: elastic_sync
    data_files:
      - split: train
        path: "data/elastic_sync/**/*.parquet"
---

# OBSO/PAUSA Input Data &mdash; IDSSE Events + ELASTIC Sync

Input event data and ELASTIC event-tracking synchronization results for OBSO/PAUSA computation &mdash; **two configs** covering DFL match events and their frame-level alignment to 25fps IDSSE tracking data. These inputs feed the Off-Ball Scoring Opportunity (OBSO) and PAUSA pass-timing pipelines.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset

# Load events
events = load_dataset("luxury-lakehouse/obso-pausa-inputs", "events")
df_events = events["train"].to_pandas()

# Load ELASTIC sync results
sync = load_dataset("luxury-lakehouse/obso-pausa-inputs", "elastic_sync")
df_sync = sync["train"].to_pandas()

# Join events to their best-matching tracking frame
merged = df_events.merge(df_sync, on=["match_id", "event_id"], how="inner")
print(f"{len(merged)} events with frame alignment")
```

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

## What Is This Dataset?

This dataset provides the **input layer** for OBSO and PAUSA computation. It contains two configs:

1. **`events`** &mdash; Match events extracted from IDSSE open data in DFL format (Play, KickOff, TacklingGame, etc.) with pitch coordinates in meters.
2. **`elastic_sync`** &mdash; The output of the ELASTIC algorithm (Kim et al. 2025) that synchronizes each event to its best-matching tracking frame, enabling event-tracking fusion without annotated event locations.

Together, these allow downstream pipelines to compute pitch control at the exact moment of each event by looking up the corresponding tracking frame.

## Data Fields &mdash; `events` Config

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | `string` | Match identifier |
| `event_id` | `string` | Unique event identifier |
| `event_type` | `string` | DFL event type (Play, KickOff, TacklingGame, etc.) |
| `timestamp_seconds` | `double` | Elapsed time in seconds from period start |
| `period` | `int` | Match period (1 or 2) |
| `player_id` | `string` | DFL PersonId of the acting player |
| `team` | `string` | Team affiliation (`home` or `away`) |
| `x` | `double` | Event x-coordinate (meters, 0&ndash;105) |
| `y` | `double` | Event y-coordinate (meters, 0&ndash;68) |

## Data Fields &mdash; `elastic_sync` Config

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | `string` | Match identifier |
| `event_id` | `string` | Event identifier (join key to `events` config) |
| `frame_id` | `int` | Best-matching tracking frame number |
| `alignment_confidence` | `double` | ELASTIC alignment confidence (0&ndash;1, higher = more confident) |
| `alignment_error_seconds` | `double` | Estimated temporal error of the alignment in seconds |

### Coordinate System

**Events**: DFL pitch-origin meters (105&times;68m). The origin (0, 0) is at the center of one goal line; x runs along the pitch length (0&ndash;105m), y along the width (0&ndash;68m).

**ELASTIC sync**: Frame IDs reference 25fps IDSSE tracking data. Each `frame_id` maps to a specific instant in the tracking timeline.

## Data Sources

| Source | Matches | License |
|--------|---------|---------|
| [IDSSE Open Data](https://www.nature.com/articles/s41597-025-04507-0) | ~3 | CC-BY 4.0 |

The IDSSE (Integrated Dataset of Spatiotemporal and Event Data in Elite Soccer) provides synchronized event and tracking data from German Bundesliga matches.

## Companion Resources

| Resource | Description |
|----------|-------------|
| [OBSO/PAUSA Values](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-values) | Computed OBSO and PAUSA scores per pass |
| [Pitch Control Tracking](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) | Per-player per-frame pitch control from tracking data |

## Limitations

- **Small sample**: Only ~3 Bundesliga matches from the IDSSE open data release. Results may not generalize across leagues or tactical systems.
- **ELASTIC alignment accuracy**: Synchronization quality depends on event timing precision in the source data. Events with ambiguous timestamps may have lower `alignment_confidence`.
- **DFL event taxonomy**: Event types follow DFL conventions, which differ from StatsBomb or Opta taxonomies. Cross-provider comparisons require mapping.
- **No ball tracking in events**: Event coordinates represent player position at the moment of action, not ball trajectory.

## Citation

If you use this dataset, please cite the IDSSE and ELASTIC papers:

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

## More Information

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

- **License**: [MIT](https://opensource.org/licenses/MIT)
- **Publish script**: `notebooks/publish_obso_data.py` (Databricks notebook)
