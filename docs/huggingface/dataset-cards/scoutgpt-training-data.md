---
language: [en]
license: cc-by-nc-4.0
task_categories:
  - feature-extraction
  - sentence-similarity
tags:
  - sports-analytics
  - soccer
  - football
  - player-sequences
  - transformer
  - scoutgpt
  - statsbomb
  - wyscout
size_categories:
  - 100K<n<1M
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/*.parquet"
---

# ScoutGPT Training Data &mdash; Player Action Sequences

Per-player match-level action sequences unified across [StatsBomb Open Data](https://github.com/statsbomb/open-data) and [Wyscout](https://figshare.com/collections/Soccer_match_event_dataset/4415000) open data. Each row is one player-match's ordered SPADL action sequence with contextual tokens (competition, season, score state, half, opponent), serialized as the token stream that the [ScoutGPT](https://huggingface.co/luxury-lakehouse/scoutgpt) transformer consumes during training and at inference.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/scoutgpt-training-data")
df = ds["train"].to_pandas()
print(f"{len(df):,} player-match sequences, {df['player_id'].nunique():,} unique players")
```

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## What Is This Dataset?

ScoutGPT is a transformer decoder trained to produce per-player season embeddings by modelling the *sequential* structure of on-ball actions within a match. Unlike per-action bag-of-features embeddings (Football2Vec v1), ScoutGPT sees the order of actions &mdash; so it can capture tempo, build-up patterns, and decision-making over a possession.

This dataset is the serialized per-player-match training corpus produced by [`wf-scoutgpt-export`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/workflow-cards/wf-scoutgpt-export.yaml) from the gold-layer `fct_action_values` table.

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `player_id` | `Int64` | Canonical player identifier (cross-source resolved) |
| `match_id` | `Int64` | Provider-native match identifier |
| `data_source` | `string` | Origin (`statsbomb` or `wyscout`) |
| `competition_id` | `Int64` | Competition identifier (NULL for Wyscout) |
| `season_id` | `Int64` | Season identifier (NULL for Wyscout) |
| `team_id` | `Int64` | Player's team in this match |
| `token_ids` | `list<int32>` | Tokenized action-sequence for this player in this match |
| `sequence_length` | `Int64` | Number of tokens in the sequence |

## Data Sources

| Source | Matches | License |
|--------|---------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~3,000 | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | ~1,900 | CC-BY-NC 4.0 |

Inherits the more restrictive CC-BY-NC 4.0 license via Wyscout.

## Use Cases

- **ScoutGPT training**: primary training corpus for the [ScoutGPT](https://huggingface.co/luxury-lakehouse/scoutgpt) transformer
- **Sequence-aware embedding research**: evaluate new architectures (cross-attention, Fourier position encodings, RoPE) against a common corpus
- **Downstream fine-tuning**: task-specific heads (player-type classification, next-action prediction) on top of pre-trained ScoutGPT checkpoints

## Limitations

- **Open data only**: commercial datasets cover additional leagues and seasons
- **Season-level aggregation**: per-match sequences are independent &mdash; cross-match context is not captured in a single row
- **Derived from SPADL**: downstream of the SPADL conversion; any SPADL-adapter issue (see [spadl-vaep-action-values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values)) propagates here

## Companion Resources

| Resource | Type | Description |
|----------|------|-------------|
| [ScoutGPT](https://huggingface.co/luxury-lakehouse/scoutgpt) | Model | Transformer decoder trained on this dataset |
| [ScoutGPT variants (rope)](https://huggingface.co/luxury-lakehouse/scoutgpt-variant-rope) | Model | Ablation checkpoint with RoPE position encoding |
| [SPADL/VAEP Action Values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Dataset | Upstream source — per-action valuations |

## License

CC-BY-NC 4.0 (inherited from Wyscout).
