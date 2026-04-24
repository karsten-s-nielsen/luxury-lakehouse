---
language: [en]
license: cc-by-4.0
task_categories: [feature-extraction]
tags: [sports-analytics, soccer, football, player-embeddings, transformer, deep-sets, 360-data, similarity-search]
size_categories: [1K-10K]
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
    default: true
---

# Football2Vec 360 Player Embeddings &mdash; 144-Dim Transformer + Deep Sets Vectors

Pre-computed 144-dimensional player embedding vectors from the [Football2Vec 360-Enriched](https://huggingface.co/luxury-lakehouse/football2vec-360) model &mdash; ready to use without loading model weights. Covers **~4K player-match records** across 323 StatsBomb 360 open-data matches. Each vector encodes both action sequences and spatial freeze-frame context via a transformer encoder (128d) combined with a Deep Sets encoder (16d).

This dataset occupies a **separate embedding space** from [Football2Vec v2 Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) (128-dim). Vectors from the two models are not directly comparable and must not be mixed in the same similarity index.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset
import numpy as np

ds = load_dataset("luxury-lakehouse/football2vec-360-embeddings")
df = ds["train"].to_pandas()

# Extract behavioral vectors as a NumPy matrix
vectors = np.array(df["behavioral_vector"].tolist())
print(f"{vectors.shape[0]} player-matches, {vectors.shape[1]}-dim embeddings")  # (~4K, 144)

# Cosine similarity between two players
from sklearn.metrics.pairwise import cosine_similarity
player_a = vectors[0:1]
player_b = vectors[1:2]
sim = cosine_similarity(player_a, player_b)[0, 0]
print(f"Cosine similarity: {sim:.4f}")
```

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## What Are These Embeddings?

Each embedding is a 144-dimensional vector combining two complementary representations:

- **Transformer stream** (128 dimensions): A transformer encoder embedding capturing action sequences and spatial patterns from SPADL-tokenized events. Same architecture as Football2Vec v2 but trained exclusively on 360-annotated matches.
- **Deep Sets stream** (16 dimensions): A permutation-invariant encoder (Zaheer et al. 2017) processing the unordered set of visible player positions (freeze-frame) at each action, aggregated via sum-pooling. Captures how a player behaves relative to surrounding opponents and teammates.

Both streams are combined via concatenation and jointly trained with adversarial team debiasing (Ganin et al. 2016) to remove team-identity confounds.

For model architecture details and training methodology, see the companion model: [`luxury-lakehouse/football2vec-360`](https://huggingface.co/luxury-lakehouse/football2vec-360).

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `canonical_player_id` | `string` | Unified player identifier (from entity resolution across data sources) |
| `match_id` | `string` | Match identifier (StatsBomb 360 match) |
| `behavioral_vector` | `array<double>` | 144-dim embedding for this player-match [128d transformer \|\| 16d Deep Sets] |

## Coverage

| Metric | Value |
|--------|-------|
| **Matches** | 323 (complete StatsBomb 360 open-data release) |
| **Player-match records** | ~4K |
| **Competitions** | La Liga, Premier League, Champions League, Euro 2020, Women's World Cup, Copa America |

Coverage is limited to players with appearances in StatsBomb 360-annotated matches. For broader player coverage (~87K player-matches, ~3,000 matches), use [Football2Vec v2 Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings).

## Use Cases

- **Context-aware similarity search**: Cosine distance on 144-dim vectors finds players with similar style *and* spatial decision-making in 360-annotated matches
- **Spatial pattern analysis**: The 16-dim Deep Sets component enables queries such as "players who behave similarly under high defensive pressure"
- **Ablation research**: Compare with Football2Vec v2 embeddings to quantify the impact of 360 freeze-frame context on player representations
- **Transfer scouting**: Identify players with equivalent behavioral profiles in competitions with 360 data coverage
- **Downstream features**: Input to GNN tactical models where spatial context and relational reasoning matter

## Limitations

- **360-match coverage only**: Players without StatsBomb 360 match appearances have no embeddings in this dataset. Use Football2Vec v2 embeddings for broader coverage.
- **Per-match granularity**: One row per player-match (no career or season aggregates in this release). Aggregate across matches client-side if needed.
- **Separate embedding space**: 144-dim vectors are not comparable to Football2Vec v2 128-dim vectors. Cannot mix in the same similarity index without re-embedding all players.
- **Small corpus effects**: 323 matches is a smaller training corpus than Football2Vec v2 (~3,000 matches). Players with few 360 appearances may have noisier embeddings.
- **Open data only**: Derived from publicly available StatsBomb 360 data. Commercial datasets with proprietary 360 annotations may yield different representations.

## Freshness

| Metric | Value |
|--------|-------|
| **Freshness SLA** | 168 hours (7 days) |
| **Inference schedule** | Daily 06:00 UTC |
| **Skip guard** | `match_id`-level &mdash; only new 360 matches trigger re-inference |

## Citation

If you use these embeddings, please cite the companion model and the Deep Sets architecture:

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
| [Football2Vec 360 Model](https://huggingface.co/luxury-lakehouse/football2vec-360) | 144-dim model that generated these embeddings |
| [360 Training Data](https://huggingface.co/datasets/luxury-lakehouse/football2vec-360-training-data) | SPADL sequences with freeze-frames used for training |
| [Football2Vec v2 Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) | 128-dim event-only embeddings with broader coverage |
| [Football2Vec v2 Model](https://huggingface.co/luxury-lakehouse/football2vec-v2) | 128-dim event-only transformer model |
| [SPADL/VAEP Action Values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Per-action offensive/defensive VAEP valuations |

## More Information

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

- **Model repo**: [`luxury-lakehouse/football2vec-360`](https://huggingface.co/luxury-lakehouse/football2vec-360)
- **License**: [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/) (StatsBomb Open Data)
