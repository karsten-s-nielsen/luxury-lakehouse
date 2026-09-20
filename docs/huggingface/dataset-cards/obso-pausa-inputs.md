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
  - action-data
  - idsse
  - bundesliga
size_categories: [1K<n<10K]
---

# OBSO/PAUSA Input Data &mdash; IDSSE SPADL Actions + ELASTIC Action-Frame Alignment

Input data for OBSO/PAUSA computation: one denormalized row per IDSSE SPADL action that the ELASTIC algorithm anchored to a tracking frame, carrying the action's start location plus its ELASTIC start-frame and reception-frame alignment against 25fps IDSSE tracking data. These inputs feed the Off-Ball Scoring Opportunity (OBSO) and PAUSA pass-timing pipelines.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

> **Grain change (2026-09):** this dataset moved from event grain (one row per DFL event, with a separate `elastic_sync` config) to **action grain** (one row per SPADL action). The alignment is now produced by the lakehouse Action-Context drain's ELASTIC v2 pass (silly-kicks TF-57), which anchors both the action's start frame and its reception frame &mdash; there is no longer a separate producer or a two-config split.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/obso-pausa-inputs", split="train")
df = ds.to_pandas()
# Each row is one ELASTIC-aligned SPADL action; look up the tracking frame directly.
print(f"{len(df)} actions with frame alignment across {df['match_id'].nunique()} matches")
```

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

## What Is This Dataset?

This dataset provides the **input layer** for OBSO and PAUSA computation: IDSSE SPADL actions denormalized with their ELASTIC frame alignment. The alignment is the output of the ELASTIC algorithm (Kim et al. 2025) as implemented in silly-kicks' ELASTIC v2 (TF-57), which synchronizes each action to its best-matching tracking frame &mdash; both the frame where the action starts and the frame where it is received &mdash; enabling event-tracking fusion without annotated event locations. Downstream pipelines compute pitch control at the exact moment of each action by looking up the corresponding tracking frame. Only actions that ELASTIC anchored to a frame are included.

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | `string` | Match identifier (partition key; native IDSSE match id) |
| `action_id` | `bigint` | SPADL action identifier within the match |
| `type_name` | `string` | SPADL action type (`pass`, `shot`, `cross`, etc.) |
| `period` | `bigint` | Match period (1 or 2) |
| `timestamp_seconds` | `double` | Action time in seconds from period start |
| `player_id` | `string` | Native DFL PersonId of the acting player |
| `team` | `string` | Native DFL team identifier of the acting player |
| `start_x` | `double` | Action start x-coordinate (SPADL meters, 0&ndash;105, home-LTR) |
| `start_y` | `double` | Action start y-coordinate (SPADL meters, 0&ndash;68, home-LTR) |
| `elastic_frame_id` | `int` | Best-matching tracking frame for the action's start |
| `elastic_confidence` | `double` | ELASTIC start-frame confidence (0&ndash;1, higher = more confident) |
| `elastic_error_seconds` | `double` | Estimated temporal error of the start-frame alignment (seconds) |
| `elastic_receive_frame_id` | `int` | Best-matching tracking frame for the action's reception (NULL if none) |
| `elastic_receive_confidence` | `double` | ELASTIC reception-frame confidence (0&ndash;1) |
| `elastic_receive_error_seconds` | `double` | Estimated temporal error of the reception-frame alignment (seconds) |
| `access_tier` | `string` | Per-row redistribution tier (`public` for this open-data IDSSE dataset) |

### Coordinate System

Action coordinates are canonical SPADL meters on a 105&times;68m pitch, oriented home left-to-right. `elastic_frame_id` / `elastic_receive_frame_id` reference 25fps IDSSE tracking data &mdash; each maps to a specific instant in the tracking timeline.

## Data Sources

| Source | Matches | License |
|--------|---------|---------|
| [IDSSE Open Data](https://www.nature.com/articles/s41597-025-04507-0) | 7 | CC-BY 4.0 |

The IDSSE (Integrated Dataset of Spatiotemporal and Event Data in Elite Soccer) provides synchronized event and tracking data from German Bundesliga matches.

## Companion Resources

| Resource | Description |
|----------|-------------|
| [OBSO/PAUSA Values](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-values) | Computed OBSO and PAUSA scores per pass |
| [Pitch Control Tracking](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) | Per-player per-frame pitch control from tracking data |

## Limitations

- **Small sample**: Only 7 Bundesliga matches from the IDSSE open data release. Results may not generalize across leagues or tactical systems.
- **ELASTIC alignment accuracy**: Synchronization quality depends on event timing precision in the source data. Events with ambiguous timestamps may have lower `alignment_confidence`.
- **SPADL action taxonomy**: `type_name` follows the canonical SPADL vocabulary derived from DFL events; cross-provider comparisons that assume raw DFL or StatsBomb/Opta taxonomies require mapping.
- **Aligned actions only**: rows exist only for actions ELASTIC anchored to a tracking frame (`elastic_frame_id` non-NULL); actions with no resolvable frame are absent.
- **Action start coordinates**: `start_x`/`start_y` are the acting player's position at the action start, not ball trajectory.

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
- **Publish script**: `scripts/publish_obso_pausa_inputs_hf.py`
