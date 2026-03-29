---
language:
  - en
license: mit
library_name: gensim
tags:
  - sports-analytics
  - soccer
  - football
  - player-embeddings
  - doc2vec
  - statsbomb
  - wyscout
  - gensim
datasets:
  - luxury-lakehouse/spadl-vaep-action-values
  - luxury-lakehouse/line-breaking-passes
  - luxury-lakehouse/football2vec-player-embeddings
  - luxury-lakehouse/pitch-control-tracking
pipeline_tag: feature-extraction
---

# Football2Vec &mdash; Player Behavioral Embeddings

32-dimensional player embedding vectors trained on **~3,000 professional soccer matches** from [StatsBomb Open Data](https://github.com/statsbomb/open-data) and [Wyscout](https://figshare.com/collections/Soccer_match_event_dataset/4415000). Built with gensim Doc2Vec &mdash; no GPU required.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Model Description

Football2Vec represents each player's per-match behavior as a fixed-length vector by:

1. **Tokenizing** match events into spatial action tokens (e.g., `pass_6_4`, `shot_11_3`) on a 12&times;8 pitch grid
2. **Training** Doc2Vec (Paragraph Vector -- Distributed Memory, PV-DM) on per-player-per-match token sequences
3. **Inferring** 32-dim embedding vectors that capture playing style

Players with similar on-pitch behavior produce similar vectors, enabling cosine-distance similarity search ("find players like Messi").

### Dual-Vector Architecture

This model provides the **behavioral** half of a dual-vector player representation:

| Vector | Dimensions | Source | Captures |
|--------|-----------|--------|----------|
| **Behavioral** (this model) | 32 | Doc2Vec on event sequences | Playing style, spatial patterns, action tendencies |
| **Statistical** | 13 | Z-score normalized per-90 stats | Goals, assists, xG, passes, VAEP, defensive metrics |

Both vectors are stored in PostgreSQL with [pgvector](https://github.com/pgvector/pgvector) HNSW indexes for sub-10ms cosine-distance similarity queries.

## Training Data

| Source | Matches | Events | License |
|--------|---------|--------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~3,000 | ~3M | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | ~1,900 | ~3M | CC-BY-NC 4.0 |

Coverage includes the Premier League, La Liga, Serie A, Bundesliga, Ligue 1, Champions League, World Cup, and more.

### Tokenization

Events are mapped to spatial action tokens using source-specific type mappings:

- **StatsBomb**: Pass, Shot, Carry, Duel, Interception, Foul, Clearance, Dribble, Goalkeeper (+ cross, corner, throw-in subtypes)
- **Wyscout**: Pass, Shot, Duel, Foul, Goalkeeper, Free Kick, Others (+ subtypes)
- **Grid**: 12 columns &times; 8 rows on a 120&times;80 pitch &rarr; 10m &times; 10m cells
- **Token format**: `{action}_{grid_x}_{grid_y}` (e.g., `pass_6_4`)

### Hyperparameters

| Parameter | Value |
|-----------|-------|
| Algorithm | PV-DM (Doc2Vec `dm=1`) |
| Vector size | 32 |
| Window | 5 |
| Min count | 2 |
| Epochs | 20 |
| Workers | 1 (deterministic) |

## How to Use

### Quick Start

```bash
pip install huggingface_hub gensim
```

```python
from huggingface_hub import snapshot_download
from gensim.models.doc2vec import Doc2Vec

# Download model
model_dir = snapshot_download("luxury-lakehouse/football2vec-statsbomb-wyscout")

# Load
model = Doc2Vec.load(f"{model_dir}/player2vec.model")

# Infer a vector from a token sequence
tokens = ["pass_6_4", "pass_7_3", "shot_11_4", "carry_8_5"]
vector = model.infer_vector(tokens, epochs=20)
print(f"Vector shape: {vector.shape}")  # (32,)
```

### Z-Score Statistical Vectors

The model also includes normalization parameters for 13-dim statistical vectors:

```python
import json

with open(f"{model_dir}/zscore_params.json") as f:
    params = json.load(f)

# params maps feature name -> {"mean": float, "std": float}
# Features: goals_per90, assists_per90, xg_per90, shots_per90,
#   passes_per90, pass_completion_pct, progressive_passes_per90,
#   tackles_per90, interceptions_per90, clearances_per90,
#   offensive_vaep_per90, defensive_vaep_per90, lb_passes_per90
```

## Intended Use

- **Player similarity search**: "Find players with a similar playing style to X"
- **Scouting**: Identify transfer targets by behavioral profile
- **Tactical analysis**: Cluster players by on-pitch behavior
- **Research**: Reproducible player embeddings for sports analytics

## Limitations

- **Open data only**: Trained on publicly available StatsBomb and Wyscout data. Commercial datasets may yield different representations.
- **Event-based**: Captures on-ball actions only. Off-ball movement, positioning, and pressing are not represented.
- **No temporal context**: Doc2Vec treats the token sequence as a bag-of-words with local context windows, not a full temporal model.
- **Cross-source alignment**: StatsBomb and Wyscout use different event taxonomies. The tokenizer normalizes them, but subtle differences in event definitions remain.

## Citation

If you use this model, please cite the Doc2Vec method and this repository:

```bibtex
@inproceedings{le2014distributed,
  title={Distributed Representations of Sentences and Documents},
  author={Le, Quoc and Mikolov, Tomas},
  booktitle={International Conference on Machine Learning},
  year={2014}
}
```

```bibtex
@software{nielsen2026football2vec,
  title={Football2Vec: Player Behavioral Embeddings from Event Sequences},
  author={Nielsen, Karsten Skytt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Model Files

```
player2vec.model               -- gensim Doc2Vec checkpoint
player2vec.model.dv.vectors.npy -- document vectors (numpy)
player2vec.model.wv.vectors.npy -- word vectors (numpy)
zscore_params.json             -- z-score normalization parameters
```

## Companion Resources

Pre-computed datasets derived from this model and the platform's analytics pipelines:

| Dataset | Description |
|---------|-------------|
| [SPADL/VAEP Action Values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Per-action offensive/defensive VAEP valuations |
| [Line-Breaking Passes](https://huggingface.co/datasets/luxury-lakehouse/line-breaking-passes) | Pass dataset with defensive line-breaking labels |
| [Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) | Pre-computed behavioral + statistical vectors (career/season/match) |
| [Pitch Control Tracking](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) | Per-player per-frame pitch control values from tracking data |

## Demo

Try the interactive [Soccer Analytics Explorer](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) &mdash; search for similar players by behavioral embedding, explore shot maps, and visualize line-breaking passes.

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## More Information

- **License**: [MIT](https://opensource.org/licenses/MIT)
