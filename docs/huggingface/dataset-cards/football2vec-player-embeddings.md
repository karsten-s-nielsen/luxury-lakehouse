---
language: [en]
license: mit
task_categories: [feature-extraction]
tags: [sports-analytics, soccer, football, player-embeddings, doc2vec, football2vec, similarity-search]
size_categories: [10K-100K]
configs:
  - config_name: career
    data_files:
      - split: train
        path: "data/career/*.parquet"
    default: true
  - config_name: season
    data_files:
      - split: train
        path: "data/season/*.parquet"
  - config_name: per_match
    data_files:
      - split: train
        path: "data/per_match/*.parquet"
---

# football2vec Player Embeddings

Pre-computed player embedding vectors from the [football2vec v2](https://huggingface.co/luxury-lakehouse/football2vec-v2) transformer model &mdash; ready to use without loading model weights. Covers **~87,000** per-match vectors, **~8,950** career vectors, and season-level aggregates across professional soccer competitions. V2 uses a 128-dim transformer encoder with adversarial team debiasing (Ganin GRL) to prevent team identity from confounding player style representations.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset
import numpy as np

ds = load_dataset("luxury-lakehouse/football2vec-player-embeddings")
df = ds["train"].to_pandas()

# Extract behavioral vectors as a NumPy matrix
vectors = np.array(df["behavioral_vector"].tolist())
print(f"{vectors.shape[0]} players, {vectors.shape[1]}-dim embeddings")
```

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## What Are These Embeddings?

Each embedding is composed of two complementary vectors trained or derived from open event data:

- **Behavioral vector** (128-dimensional): A transformer encoder embedding with adversarial team debiasing (Ganin GRL). Trained on tokenized SPADL action streams (23-type vocabulary), it captures *how* a player plays &mdash; their movement patterns, decision sequences, and positional tendencies &mdash; while removing team-specific confounds. V1 (32-dim Doc2Vec) is retained as a baseline.
- **Statistical vector** (13-dimensional, may be NULL): Per-90 statistics z-score normalized **within position group** (GK, Def, Mid, Fwd). This position-aware normalization prevents goalkeeper contamination in similarity search and provides fairer cross-position comparisons.

For model architecture details, training methodology, and the full vocabulary, see the companion model repositories: [`luxury-lakehouse/football2vec-v2`](https://huggingface.co/luxury-lakehouse/football2vec-v2) (v2 transformer) and [`luxury-lakehouse/football2vec-statsbomb-wyscout`](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout) (v1 Doc2Vec baseline).

## Configs

### `career` (default) &mdash; ~8,950 rows

One row per player. The behavioral vector is the element-wise mean of all per-match embeddings across the player's career. Useful for overall style profiling and cross-era similarity search.

### `season`

One row per player per competition-season. Captures style evolution across seasons and enables within-season similarity comparisons.

### `per_match` &mdash; ~87,000 rows

One row per player per match. The most granular config; suitable for match-level clustering and fine-grained style analysis.

## Data Fields

### Career Config (`fct_player_embeddings_career`)

| Column | Type | Description |
|--------|------|-------------|
| `canonical_player_id` | `string` | Unified player identifier (see [dim_players](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings)) |
| `behavioral_vector` | `array<double>` | 128-dim embedding (element-wise mean across career) |
| `stat_vector` | `array<double>` | 13-dim z-score normalized per-90 stats (may be NULL) |
| `total_matches` | `bigint` | Number of matches contributing to this career embedding |
| `data_sources` | `array<string>` | Data providers contributing (e.g., `["statsbomb", "wyscout"]`) |

### Season Config (`fct_player_embeddings_season`)

| Column | Type | Description |
|--------|------|-------------|
| `embedding_season_id` | `string` | Surrogate key |
| `canonical_player_id` | `string` | Unified player identifier |
| `competition_id` | `int` | Competition identifier |
| `season_id` | `int` | Season identifier |
| `behavioral_vector` | `array<double>` | 128-dim embedding (season-level mean across matches) |
| `stat_vector` | `array<double>` | 13-dim stat vector (may be NULL) |
| `matches_in_sample` | `bigint` | Number of matches contributing in this competition-season |
| `data_sources` | `array<string>` | Data providers contributing |

### Per-Match Config (`fct_player_embeddings`)

| Column | Type | Description |
|--------|------|-------------|
| `embedding_id` | `string` | Surrogate key |
| `canonical_player_id` | `string` | Unified player identifier |
| `match_id` | `string` | Match identifier |
| `data_source` | `string` | Data provider (`statsbomb` or `wyscout`) |
| `behavioral_vector` | `array<double>` | 128-dim embedding for this player-match |
| `stat_vector` | `array<double>` | 13-dim stat vector (may be NULL) |

## Schema Migration &mdash; Dual-Column Window (2026-04-25 &rarr; 2026-07-22)

PR 5b of the lakehouse Kimball migration (ADR-011) adds the BIGINT surrogate `player_key` to the underlying `fct_player_embeddings*` marts. **This dataset payload is NOT yet modified** &mdash; the parquet files continue to ship `canonical_player_id` only. PR 8 (planned 2026-07-22) will add `player_key` to the payloads in a backwards-compatible way and announce a sunset for `canonical_player_id`.

Recommended consumer behaviour during this window:

- **No change required.** Continue to read `canonical_player_id` from this dataset.
- If you maintain your own join to a `dim_players` clone, you may pre-compute `player_key = xxhash64(provider || '|' || cast(player_id as string))` to align with the lakehouse Kimball convention ahead of the payload change.
- After 2026-07-22 the dataset will carry both columns for at least one HF dataset version, then `canonical_player_id` will be deprecated. Migrate at your convenience inside that window.

If you depend on this dataset and need extra notice before the column drop, open an issue on the [lakehouse repo](https://github.com/karsten-s-nielsen/luxury-lakehouse).

## Use Cases

- **Similarity search**: Find players with the most similar style to a given player using cosine similarity on `behavioral_vector`. Four HNSW indexes are maintained on the platform for sub-millisecond ANN queries.
- **Clustering**: Group players by behavioral archetype (e.g., deep-lying playmaker vs. box-to-box midfielder) without labeled data.
- **Transfer market analysis**: Identify stylistically equivalent players across leagues and data sources for scouting.
- **Style evolution tracking**: Use the `season` config to monitor how a player's behavioral profile changes across seasons or after a transfer.
- **Hybrid ranking**: Combine `behavioral_vector` (style match) with `stat_vector` (volume/efficiency) for multi-objective player ranking.

## Data Sources

| Source | Matches | License |
|--------|---------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~3,000 | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | ~1,900 | CC-BY-NC 4.0 |

Coverage includes the Premier League, La Liga, Serie A, Bundesliga, Ligue 1, Champions League, World Cup, and more.

## Limitations

- **Event-based only**: The behavioral vector is derived from action sequences, not tracking data. Off-ball movement, pressing intensity, and spatial positioning are not directly encoded.
- **Action sequence style**: The transformer model captures the *type* and *order* of actions a player takes, not physical attributes (pace, strength) or tactical context (formation, instructions).
- **Open data only**: Model trained on publicly available StatsBomb and Wyscout data. Players with few appearances in these datasets may have noisy embeddings.
- **NULL stat vectors**: Players with insufficient on-ball volume for reliable per-90 computation have `stat_vector = NULL`. Downstream models should handle this case explicitly.
- **Cross-source alignment**: Player identity is unified via the entity resolution pipeline (`dim_players`), but subtle cross-source differences in event definitions may affect behavioral vector comparability.

## Citation

If you use these embeddings, please reference the companion model repository:

```
luxury-lakehouse/football2vec-statsbomb-wyscout
https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout
```

And the underlying SPADL/VAEP framework:

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

## More Information

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

- **Model repo**: [`luxury-lakehouse/football2vec-statsbomb-wyscout`](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout)
- **License**: [MIT](https://opensource.org/licenses/MIT)
