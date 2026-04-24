---
language: [en]
license: cc-by-nc-4.0
task_categories:
  - feature-extraction
tags:
  - sports-analytics
  - soccer
  - football
  - player-embeddings
  - transformer
  - football2vec
  - statsbomb
  - wyscout
size_categories:
  - 1K<n<10K
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
---

# Football2Vec v2 Embeddings &mdash; StatsBomb + Wyscout

Per-player 128-dimensional embeddings produced by [Football2Vec v2](https://huggingface.co/luxury-lakehouse/football2vec-v2) &mdash; a transformer encoder with adversarial competition debiasing. Trained on ~3,000 StatsBomb + ~1,900 Wyscout open-data matches; the dataset here is the post-training *embeddings* output, one row per unique canonical player.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Naming note

This dataset is named `football2vec-statsbomb-wyscout` to match the historical v1 model repo name. The v2 model weights live at [`luxury-lakehouse/football2vec-v2`](https://huggingface.co/luxury-lakehouse/football2vec-v2); the legacy v1 model at [`luxury-lakehouse/football2vec-statsbomb-wyscout`](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout) is **deprecated** and kept only for traceability. The *embeddings* in this dataset come from v2; the repo name is retained for backward compatibility with downstream consumers that read embeddings from this path.

## Quick Start

```python
from datasets import load_dataset
import numpy as np

ds = load_dataset("luxury-lakehouse/football2vec-statsbomb-wyscout")
df = ds["train"].to_pandas()
print(f"{len(df):,} players, dim={len(df.loc[0, 'embedding'])}")
```

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `canonical_player_id` | `Int64` | Cross-source-resolved canonical player identifier |
| `player_name` | `string` | Player display name at time of training |
| `embedding` | `list<float32>` | 128-dimensional vector produced by Football2Vec v2 |
| `total_matches` | `Int64` | Number of matches the player appeared in across both sources |
| `data_sources` | `list<string>` | Sources where the player has appearances (`statsbomb`, `wyscout`) |

## Training Provenance

- **Producer model**: [`luxury-lakehouse/football2vec-v2`](https://huggingface.co/luxury-lakehouse/football2vec-v2)
- **Training corpus**: [`luxury-lakehouse/football2vec-training-data`](https://huggingface.co/datasets/luxury-lakehouse/football2vec-training-data)
- **Adversarial debiasing**: gradient-reversal against a competition-prediction head (Ganin et al. 2016) so embeddings encode player behaviour rather than league identity
- **Publishing script**: [`scripts/train_football2vec_v2.py`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/scripts/train_football2vec_v2.py)

## Use Cases

- **Player similarity search**: cosine similarity over `embedding` returns behaviourally similar players across competitions
- **Counterfactual substitution** ("what would Player X do in Team Y's possessions?"): input for downstream visual analytics
- **Role clustering**: UMAP / PCA projections reveal role archetypes decoupled from competition identity (v1 baselines tended to cluster by league; v2 does not)

## Limitations

- **Open data only**: commercial datasets cover additional leagues and seasons
- **Training-time snapshot**: embeddings are recomputed only when the v2 model is retrained; between training runs, new players are absent from this dataset
- **Position-agnostic**: no role tag — downstream consumers apply role classifiers separately

## License

CC-BY-NC 4.0 (inherited from Wyscout training-data licensing).

## Companion Resources

| Resource | Type | Description |
|----------|------|-------------|
| [Football2Vec v2 Model](https://huggingface.co/luxury-lakehouse/football2vec-v2) | Model | Transformer encoder that produced these embeddings |
| [Football2Vec Training Data](https://huggingface.co/datasets/luxury-lakehouse/football2vec-training-data) | Dataset | Upstream training corpus |
| [Football2Vec v1 (deprecated)](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout) | Model | Legacy Doc2Vec model — superseded by v2 |
| [Football2Vec Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) | Dataset | Multi-granularity embeddings (career/season/per-match) |
