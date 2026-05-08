---
license: cc-by-nc-4.0
language: en
library_name: pytorch
tags:
  - sports-analytics
  - soccer
  - football
  - player-embeddings
  - transformer
  - adversarial-training
  - gradient-reversal
datasets:
  - luxury-lakehouse/football2vec-training-data
  - luxury-lakehouse/football2vec-player-embeddings
metrics:
  - mlm_accuracy
pipeline_tag: feature-extraction
---

# Football2Vec v2 &mdash; Transformer Player Embeddings with Adversarial Team Debiasing

192-dimensional player embedding vectors from a 4-layer transformer encoder trained via masked language modeling (MLM) on **~87K SPADL action sequences** from ~3,000 professional soccer matches. Adversarial team debiasing via gradient reversal (Ganin et al. 2016) removes competition-specific confounds, producing style representations that generalize across leagues.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform. Replaces the [v1 Doc2Vec model](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout) (32-dim) as the `@Champion` version.

## Model Description

Football2Vec v2 learns contextual player embeddings from SPADL action sequences. Each player-match is represented as a sequence of tokenized actions with continuous spatial coordinates. The transformer encoder processes these sequences, and mean pooling over valid tokens produces a fixed-length 192-dim embedding capturing playing style.

### Architecture

| Component | Detail |
|-----------|--------|
| **Token embedding** | 23 SPADL action types &rarr; 192d lookup table |
| **Spatial encoding** | MLP(x) + MLP(y) &rarr; 192d each, summed with token embedding |
| **Positional embedding** | Learnable, max 512 tokens |
| **Encoder** | 4-layer TransformerEncoder, 4 attention heads, GELU activation, 4x FFN |
| **Pooling** | Mean pooling over valid (non-padding) tokens &rarr; 192d |
| **Adversarial head** | Gradient reversal layer (&lambda;=0.2) + competition classifier |

### Two-Stage Training

**Stage 1 &mdash; Masked Language Modeling:** 15% of action tokens are masked; the model predicts the original action type from surrounding context. This forces the encoder to learn meaningful action-context representations.

**Stage 2 &mdash; Adversarial Debiasing:** A competition classifier head is attached via a gradient reversal layer (Ganin et al. 2016). The encoder learns to produce embeddings that *cannot* predict which competition a player belongs to, removing league-style confounds (e.g., Serie A defending vs Premier League pressing) while retaining individual style signal.

### Dual-Vector Architecture

This model provides the **behavioral** half of a dual-vector player representation:

| Vector | Dimensions | Source | Captures |
|--------|-----------|--------|----------|
| **Behavioral** (this model) | 192 | Transformer on SPADL sequences | Playing style, spatial patterns, action context |
| **Statistical** | 13 | Z-score normalized per-90 stats | Goals, assists, xG, passes, VAEP, defensive metrics |

Both vectors are stored in PostgreSQL with [pgvector](https://github.com/pgvector/pgvector) HNSW indexes (192-dim, cosine ops) for sub-10ms similarity queries.

## Training Data

| Source | Matches | Events | License |
|--------|---------|--------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~3,000 | ~3M | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | ~1,900 | ~3M | CC-BY-NC 4.0 |

Training data is published as [`luxury-lakehouse/football2vec-training-data`](https://huggingface.co/datasets/luxury-lakehouse/football2vec-training-data) on HF Hub.

### Tokenization

Events are tokenized using the **23-type SPADL vocabulary** (from `fct_action_values`): `pass`, `cross`, `throw_in`, `freekick_crossed`, `freekick_short`, `corner_crossed`, `corner_short`, `take_on`, `foul`, `tackle`, `interception`, `shot`, `shot_penalty`, `shot_freekick`, `keeper_save`, `keeper_claim`, `keeper_punch`, `keeper_pick_up`, `clearance`, `bad_touch`, `non_action`, `dribble`, `goalkick`.

Continuous spatial coordinates (x, y) normalized to [0, 1] on a 105&times;68m pitch are injected via learned MLP projections summed with the token embeddings.

### Hyperparameters

| Parameter | Value |
|-----------|-------|
| Hidden dimension | 192 |
| Encoder layers | 4 |
| Attention heads | 4 |
| FFN multiplier | 4x (768) |
| Dropout | 0.1 |
| Max sequence length | 512 |
| MLM mask probability | 0.15 |
| Spatial MLP intermediate dim | 64 |
| Batch size | 256 |
| Learning rate | 1e-4 |
| Weight decay | 0.01 |
| Warmup fraction | 10% |
| Adversarial &lambda; max | 0.2 |
| Adversarial warmup epochs | 5 |

Training runs on HF Jobs A10G-large GPU (~2 hours total for both stages).

## How to Use

### Quick Start

```python
from huggingface_hub import hf_hub_download
from safetensors.torch import load_file
import json

# Download model weights (Stage 2 — adversarial debiased)
weights_path = hf_hub_download("luxury-lakehouse/football2vec-v2", "stage2/model.safetensors")
config_path = hf_hub_download("luxury-lakehouse/football2vec-v2", "stage2/config.json")

with open(config_path) as f:
    config = json.load(f)

state_dict = load_file(weights_path)
print(f"Config: {config['hidden_dim']}-dim, {config['num_layers']} layers")
print(f"Parameters: {sum(p.numel() for p in state_dict.values()):,}")
```

### Pre-Computed Embeddings (recommended)

For most use cases, load the pre-computed embeddings directly &mdash; no model inference needed:

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/football2vec-player-embeddings")
df = ds["train"].to_pandas()

vectors = np.array(df["behavioral_vector"].tolist())
print(f"{vectors.shape[0]} players, {vectors.shape[1]}-dim embeddings")  # (8950, 192)
```

## Intended Use

- **Player similarity search**: "Find players with a similar playing style to X" via cosine distance on 192-dim vectors
- **Scouting**: Identify transfer targets by behavioral profile, independent of league context
- **Tactical analysis**: Cluster players by on-pitch behavior with team-agnostic representations
- **Research**: Reproducible player embeddings for sports analytics, with adversarial debiasing removing confounding league effects
- **Downstream features**: Input to GNN tactical pattern models, sequence models (ScoutGPT), or combined with tracking-based features

## EU AI Act — Intended Use and Non-Use

This model is published for **research and reproducibility** purposes on public, open-licensed match data. It is **not intended for, not validated for, and not supplied to** any use that would fall within Annex III §4 (Employment, workers management and access to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of natural persons, decisions affecting work-related contractual relationships, promotion, termination, task allocation based on individual traits, or the monitoring and evaluation of performance and behaviour of workers for employment decisions. Player similarity search is a canonical scouting workflow, and any deployer is responsible for treating this model as decision-support at most, never as a decision system.

Any deployer who wishes to use this model for such a purpose is responsible for performing their own conformity assessment under Article 43, for drawing up the technical documentation required by Article 11 and Annex IV, for implementing the human oversight measures required by Article 14, for declaring accuracy metrics under Article 15, and for ensuring the data governance obligations of Article 10 are met. Note specifically that the training data contains no protected attributes and therefore cannot support the group-fairness audits required by Article 10(2)(g) without ingesting additional personal data. The adversarial competition debiasing (Ganin et al. 2016) described above addresses a *confounding* effect (league-style leakage) and is not a substitute for an Article 10 protected-attribute audit.

See the [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis in the source repository for the project's full risk classification, re-classification triggers, and governance posture.

## Limitations

- **Event-based only**: Captures on-ball actions. Off-ball movement, positioning, and pressing intensity are not represented.
- **Open data only**: Trained on publicly available StatsBomb and Wyscout data. Commercial datasets may yield different representations.
- **Competition debiasing, not team debiasing**: The adversarial head targets competition ID (the strongest contextual confounder). Within-league team effects are attenuated but not fully removed.
- **No temporal hierarchy**: The transformer processes a flat sequence per match. Season-level or career-level patterns emerge only through post-hoc aggregation (mean pooling across matches).
- **Cross-source alignment**: Player identity is unified via the entity resolution pipeline (`dim_players`), but subtle cross-source differences in event definitions remain.

## Freshness

| Metric | Value |
|--------|-------|
| **Training data freshness SLA** | 168 hours (7 days) |
| **Inference schedule** | Daily 06:00 UTC |
| **Skip guard** | `match_id`-level &mdash; only new matches are processed |

## Model Files

```
stage1/model.safetensors      -- Stage 1 MLM checkpoint (safetensors format)
stage2/model.safetensors      -- Stage 2 adversarial, final (safetensors format)
stage2/config.json            -- Football2VecConfig as JSON
zscore_params.json            -- z-score normalization parameters (13-dim stat vector)
```

Model weights use the **safetensors** format &mdash; a tensor-only serialization with zero pickle surface and no code execution capability. Pre-computed embeddings are delivered as Parquet (non-executable).

## Citation

```bibtex
@article{ganin2016domain,
  title={Domain-Adversarial Training of Neural Networks},
  author={Ganin, Yaroslav and Ustinova, Evgeniya and Cambau, Hana
          and Lempitsky, Victor and Laviolette, Fran{\c{c}}ois},
  journal={Journal of Machine Learning Research},
  volume={17},
  number={1},
  pages={1--35},
  year={2016}
}
```

```bibtex
@inproceedings{le2014distributed,
  title={Distributed Representations of Sentences and Documents},
  author={Le, Quoc and Mikolov, Tomas},
  booktitle={International Conference on Machine Learning},
  year={2014}
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
| [Training Data](https://huggingface.co/datasets/luxury-lakehouse/football2vec-training-data) | SPADL action sequences used for training |
| [Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) | Pre-computed 192-dim vectors (career/season/match) |
| [SPADL/VAEP Action Values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Per-action offensive/defensive VAEP valuations |
| [v1 Doc2Vec baseline](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout) | 32-dim Doc2Vec model (retained as baseline) |

## Demo

Try the interactive [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app) &mdash; search for similar players by 192-dim behavioral embedding on the Player Similarity page.

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) (inherited from Wyscout training data)
- **v1 baseline model**: [Football2Vec v1 (Doc2Vec)](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout)
- **Platform**: [Luxury Lakehouse Soccer Analytics](https://github.com/karsten-s-nielsen/luxury-lakehouse)
- **Workflow card**: `workflow-cards/wf-football2vec-v2.yaml`
