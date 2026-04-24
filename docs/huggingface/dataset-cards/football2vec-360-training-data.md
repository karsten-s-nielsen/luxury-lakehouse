---
language: [en]
license: cc-by-4.0
task_categories: [feature-extraction]
tags: [sports-analytics, soccer, football, player-embeddings, transformer, deep-sets, spadl, 360-data, training-data]
size_categories: [1M-10M]
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
    default: true
---

# Football2Vec 360 Training Data &mdash; SPADL Sequences with Freeze-Frame Context

Tokenized SPADL action sequences with StatsBomb 360 freeze-frame context for training the [Football2Vec 360-Enriched](https://huggingface.co/luxury-lakehouse/football2vec-360) model. One row per player-match, covering **~2M actions** across **323 professional soccer matches** from StatsBomb 360 Open Data.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/football2vec-360-training-data")
df = ds["train"].to_pandas()
print(f"{len(df)} player-match sequences")

# Inspect one sequence
row = df.iloc[0]
print(f"Player: {row['canonical_player_id']}, Match: {row['match_id']}")
print(f"Actions: {len(row['actions'])} events")
print(f"First action: {row['actions'][0]}")
print(f"First freeze frame: {len(row['freeze_frames'][0]['players'])} visible players")
# action: {'action_type': 0, 'x': 0.52, 'y': 0.34, 'result': 1}
# freeze_frames and actions are parallel arrays — freeze_frames[i] corresponds to actions[i]
```

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## What Is This Dataset?

Each row represents one player's actions in one match, serialized as a struct array of SPADL-tokenized events. Each action includes the 23-type SPADL token, normalized spatial coordinates, and the StatsBomb 360 freeze-frame: the (x, y) positions of all visible opponents and teammates at the moment of that action.

The freeze-frame context is the key differentiator from the standard Football2Vec v2 training data. It enables the Deep Sets encoder to learn how players behave relative to surrounding players, capturing spatial decision-making beyond what action sequences alone reveal.

This dataset is the **training corpus** for Football2Vec 360-Enriched. It is exported from the platform's `fct_action_values` and StatsBomb 360 freeze-frame Delta tables via the `export_embeddings_training_data` entry point and published here for reproducibility.

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `canonical_player_id` | `string` | Unified player identifier (from entity resolution across data sources) |
| `match_id` | `string` | Match identifier |
| `competition_id` | `int` | Competition identifier (used as adversarial target in Stage 2 training) |
| `season_id` | `int` | Season identifier |
| `position_group` | `string` (nullable) | Player position group: GK, Def, Mid, Fwd (from `dim_players`) |
| `actions` | `array<struct>` | Ordered sequence of tokenized SPADL actions |
| `freeze_frames` | `array<struct>` | Parallel array of freeze-frame player positions (aligned with `actions` by index) |

### Action Struct Schema

Each element in the `actions` array:

| Field | Type | Description |
|-------|------|-------------|
| `action_type` | `int` | SPADL action type ID (0&ndash;22, 23 action types) |
| `x` | `float` | Normalized x coordinate [0, 1] on 105m pitch |
| `y` | `float` | Normalized y coordinate [0, 1] on 68m pitch |
| `result` | `int` | Binary outcome: 1 = success, 0 = failure |

### Freeze-Frame Struct Schema

Each element in the `freeze_frames` array contains a `players` field with an array of visible player positions:

| Field | Type | Description |
|-------|------|-------------|
| `players` | `array<struct>` | Array of player positions at this action |

Each player struct:

| Field | Type | Description |
|-------|------|-------------|
| `x` | `float` | Normalized x coordinate [0, 1] on 120m pitch (StatsBomb) |
| `y` | `float` | Normalized y coordinate [0, 1] on 80m pitch (StatsBomb) |
| `is_keeper` | `bool` | True if this player is a goalkeeper |
| `is_teammate` | `bool` | True if this player is a teammate of the acting player |

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

## Data Sources

| Source | Matches | License |
|--------|---------|---------|
| [StatsBomb 360 Open Data](https://github.com/statsbomb/open-data) | 323 | CC-BY 4.0 |

The 323-match corpus is the complete StatsBomb 360 open-data release. Coverage includes La Liga (multiple seasons), Premier League, Champions League, Euro 2020, Women's World Cup, and Copa America matches with freeze-frame annotations.

## Freshness

| Metric | Value |
|--------|-------|
| **Freshness SLA** | 168 hours (7 days) |
| **Refresh trigger** | Re-exported when StatsBomb releases new 360 open-data matches |
| **Publish script** | `src/ingestion/export_embeddings_training_data.py` (entry point: `export_embeddings_training_data`) |

## Use Cases

- **Transformer + Deep Sets training**: Primary training corpus for Football2Vec 360-Enriched (masked language modeling + adversarial debiasing with freeze-frame context)
- **Spatial context research**: Study how player spatial environment correlates with action choice and outcome
- **Custom embedding models**: Train your own context-aware player embedding model on standardized SPADL + 360 sequences
- **Ablation studies**: Compare model performance with vs. without freeze-frame context against the Football2Vec v2 training data

## Limitations

- **360 matches only**: Covers 323 StatsBomb 360 matches. Players with appearances only in non-360 matches are not represented.
- **Uneven freeze-frame coverage**: Not every action in a 360-annotated match has a freeze-frame. Actions without freeze-frame context have `freeze_frames = []` and fall back to transformer-only representation.
- **Open data only**: Derived from publicly available StatsBomb 360 data. Coverage is uneven across leagues and seasons; some competitions have more 360 annotations than others.
- **Coordinate normalization**: All coordinates are normalized to [0, 1] on a 105&times;68m pitch (SPADL standard). Original StatsBomb coordinate system is not preserved.
- **NULL position_group**: Players not matched via entity resolution or lacking position metadata have `position_group = NULL`.

## Citation

If you use this dataset, please cite the SPADL framework, the Football2Vec 360-Enriched model, and the Deep Sets architecture:

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
@inproceedings{zaheer2017deep,
  title={Deep Sets},
  author={Zaheer, Manzil and Kottur, Satwik and Ravanbakhsh, Siamak and Poczos, Barnabas and Salakhutdinov, Ruslan and Smola, Alexander},
  booktitle={Advances in Neural Information Processing Systems},
  volume={30},
  year={2017}
}
```

```bibtex
@software{nielsen2026football2vec_360,
  title={Football2Vec 360-Enriched: Transformer + Deep Sets Player Embeddings},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Companion Resources

| Resource | Description |
|----------|-------------|
| [Football2Vec 360 Model](https://huggingface.co/luxury-lakehouse/football2vec-360) | 144-dim model trained on this data |
| [360 Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-360-embeddings) | Pre-computed 144-dim vectors per player-match |
| [Football2Vec v2 Training Data](https://huggingface.co/datasets/luxury-lakehouse/football2vec-training-data) | Event-only SPADL sequences (~87K player-matches, ~3,000 matches) |
| [Football2Vec v2 Model](https://huggingface.co/luxury-lakehouse/football2vec-v2) | 128-dim event-only model with broader coverage |
| [SPADL/VAEP Action Values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Per-action offensive/defensive VAEP valuations |

## More Information

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

- **Model repo**: [`luxury-lakehouse/football2vec-360`](https://huggingface.co/luxury-lakehouse/football2vec-360)
- **License**: [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/) (StatsBomb Open Data)
