---
language: [en]
license: cc-by-nc-4.0
task_categories: [feature-extraction]
tags: [sports-analytics, soccer, football, player-embeddings, transformer, spadl, training-data]
size_categories: [10K-100K]
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
    default: true
---

# Football2Vec Training Data &mdash; SPADL Action Sequences

Tokenized SPADL action sequences for training the [Football2Vec v2](https://huggingface.co/luxury-lakehouse/football2vec-v2) transformer encoder. One row per player-match, covering **~87,000 sequences** across ~3,000 professional soccer matches from StatsBomb Open Data and Wyscout.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/football2vec-training-data")
df = ds["train"].to_pandas()
print(f"{len(df)} player-match sequences")

# Inspect one sequence
row = df.iloc[0]
print(f"Player: {row['canonical_player_id']}, Match: {row['match_id']}")
print(f"Actions: {len(row['actions'])} events")
print(f"First action: {row['actions'][0]}")  # {'action_type': 0, 'x': 0.52, 'y': 0.34, 'result': 1}
```

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

## What Is This Dataset?

Each row represents one player's actions in one match, serialized as a struct array of SPADL-tokenized events. The 23-type SPADL vocabulary provides a unified action taxonomy across StatsBomb and Wyscout data sources. Continuous spatial coordinates (x, y) are normalized to [0, 1] on a 105&times;68m pitch.

This dataset is the **training corpus** for Football2Vec v2. It is exported from the platform's `fct_action_values` Delta table via the `export_embeddings_training_data` entry point and published here for reproducibility.

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `canonical_player_id` | `string` | Unified player identifier (from entity resolution across data sources) |
| `match_id` | `string` | Match identifier |
| `competition_id` | `int` | Competition identifier (used as adversarial target in Stage 2 training) |
| `season_id` | `int` | Season identifier |
| `position_group` | `string` (nullable) | Player position group: GK, Def, Mid, Fwd (from `dim_players`) |
| `actions` | `array<struct>` | Ordered sequence of tokenized SPADL actions |

### Action Struct Schema

Each element in the `actions` array:

| Field | Type | Description |
|-------|------|-------------|
| `action_type` | `int` | SPADL action type ID (0&ndash;22, 23 action types) |
| `x` | `float` | Normalized x coordinate [0, 1] on 105m pitch |
| `y` | `float` | Normalized y coordinate [0, 1] on 68m pitch |
| `result` | `int` | Binary outcome: 1 = success, 0 = failure |

### SPADL Action Vocabulary (23 types)

| ID | Action | ID | Action | ID | Action |
|----|--------|----|--------|----|--------|
| 0 | pass | 8 | foul | 16 | keeper_punch |
| 1 | cross | 9 | tackle | 17 | keeper_pick_up |
| 2 | throw_in | 10 | interception | 18 | clearance |
| 3 | freekick_crossed | 11 | shot | 19 | bad_touch |
| 4 | freekick_short | 12 | shot_penalty | 20 | non_action |
| 5 | corner_crossed | 13 | shot_freekick | 21 | dribble |
| 6 | corner_short | 14 | keeper_save | 22 | goalkick |
| 7 | take_on | 15 | keeper_claim | | |

## Schema Migration &mdash; Dual-Column Window (2026-04-25 &rarr; 2026-07-22)

PR 5b of the lakehouse Kimball migration (ADR-011) adds the BIGINT surrogate `player_key` to the upstream `fct_player_embeddings*` marts. The training-data export script (`src/ingestion/export_embeddings_training_data.py`) **continues to read `canonical_player_id` only** &mdash; this dataset's payload is unchanged in PR 5b. PR 8 (planned 2026-07-22) will add `player_key` to the payload in a backwards-compatible way and announce a sunset for `canonical_player_id`.

Recommended consumer behaviour during this window:

- **No change required.** Continue to read `canonical_player_id` from this dataset.
- If you maintain your own join to a `dim_players` clone, you may pre-compute `player_key = xxhash64(provider || '|' || cast(player_id as string))` to align with the lakehouse Kimball convention ahead of the payload change.
- After 2026-07-22 the dataset will carry both columns for at least one HF dataset version, then `canonical_player_id` will be deprecated. Migrate at your convenience inside that window.

If you depend on this dataset and need extra notice before the column drop, open an issue on the [lakehouse repo](https://github.com/karsten-s-nielsen/luxury-lakehouse).

## Data Sources

| Source | Matches | License |
|--------|---------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~3,000 | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | ~1,900 | CC-BY-NC 4.0 |

Coverage includes the Premier League, La Liga, Serie A, Bundesliga, Ligue 1, Champions League, World Cup, and more.

## Freshness

| Metric | Value |
|--------|-------|
| **Freshness SLA** | 168 hours (7 days) |
| **Refresh trigger** | Re-exported when upstream `fct_action_values` is updated with new match data |
| **Publish script** | `src/ingestion/export_embeddings_training_data.py` (entry point: `export_embeddings_training_data`) |

## Use Cases

- **Transformer training**: Primary training corpus for Football2Vec v2 (masked language modeling + adversarial debiasing)
- **Custom embedding models**: Train your own player embedding model on standardized SPADL sequences
- **Sequence analysis**: Study per-player action patterns, spatial tendencies, and decision sequences
- **Vocabulary research**: Compare action distributions across competitions, positions, or eras

## Limitations

- **Event-based only**: Contains on-ball action sequences. Off-ball movement, pressing, and positioning are not represented.
- **Open data only**: Derived from publicly available StatsBomb and Wyscout data. Coverage is uneven across leagues and seasons.
- **Coordinate normalization**: All coordinates are normalized to [0, 1] on a 105&times;68m pitch (SPADL standard). Original provider-specific coordinate systems are not preserved.
- **NULL position_group**: Players not matched via entity resolution or lacking position metadata have `position_group = NULL`.

## Citation

If you use this dataset, please cite the SPADL framework and the Football2Vec v2 model:

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

```bibtex
@software{nielsen2026football2vec_v2,
  title={Football2Vec v2: Transformer Player Embeddings with Adversarial Team Debiasing},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Companion Resources

| Resource | Description |
|----------|-------------|
| [Football2Vec v2 Model](https://huggingface.co/luxury-lakehouse/football2vec-v2) | 192-dim transformer encoder trained on this data |
| [Football2Vec v1 Model](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout) | 32-dim Doc2Vec baseline |
| [Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) | Pre-computed vectors (career/season/match) |
| [SPADL/VAEP Action Values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Per-action offensive/defensive VAEP valuations |

## More Information

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

- **Model repo**: [`luxury-lakehouse/football2vec-v2`](https://huggingface.co/luxury-lakehouse/football2vec-v2)
- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) (inherited from Wyscout data)

## PR 7 changelog (2026-04-27)

PR 5b (2026-04-25) added `player_key` to the upstream training data lineage. The HF dataset payload republish was deferred at PR 5b and is absorbed into PR 7's scope. Payload now carries `player_key` (BIGINT) alongside the legacy `player_id` and `canonical_player_id` columns during the 2026-07-22 dual-column window. PR 8 will sunset the legacy ID columns post-2026-07-22.
