---
license: cc-by-nc-4.0
language: en
library_name: pytorch
tags:
  - soccer-analytics
  - player-embeddings
  - deep-sets
  - 360-data
  - transformer
  - adversarial-training
  - gradient-reversal
datasets:
  - luxury-lakehouse/football2vec-360-training-data
  - luxury-lakehouse/football2vec-360-embeddings
metrics:
  - mlm_accuracy
pipeline_tag: feature-extraction
---

# Football2Vec 360-Enriched &mdash; Transformer + Deep Sets Player Embeddings

208-dimensional player embedding vectors from a 4-layer transformer encoder augmented with a Deep Sets context encoder (Zaheer et al. 2017) trained on **~2M SPADL actions** with StatsBomb 360 freeze-frame data from **323 professional soccer matches**. Adversarial team debiasing via gradient reversal (Ganin et al. 2016) removes team-identity confounds, producing style representations that generalize across teams.

This model occupies a **separate embedding space** from [Football2Vec v2](https://huggingface.co/luxury-lakehouse/football2vec-v2) (192-dim, event-only). The 360-enriched vectors are not directly comparable to v2 vectors and should not be mixed in downstream similarity search without re-indexing.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Model Description

Football2Vec 360-Enriched extends the v2 transformer architecture with a Deep Sets encoder that processes the spatial positions of all visible opponents and teammates at the moment of each action. The 208-dim output captures both individual action sequences and the spatial context in which those actions occur — richer representations than event-only models for players who frequently appear in 360-annotated matches.

### Architecture

| Component | Detail |
|-----------|--------|
| **Token embedding** | 23 SPADL action types &rarr; 192d lookup table |
| **Spatial encoding** | MLP(x) + MLP(y) &rarr; 192d each, summed with token embedding |
| **Positional embedding** | Learnable, max 512 tokens |
| **Encoder** | 4-layer TransformerEncoder, 4 attention heads, GELU activation, 4x FFN |
| **Pooling** | Mean pooling over valid (non-padding) tokens &rarr; 192d |
| **Deep Sets encoder** | Per-player MLP on freeze-frame (x, y, team) &rarr; sum-pool &rarr; 16d |
| **Output** | Concatenation [192d transformer \|\| 16d Deep Sets] &rarr; 208d |
| **Adversarial head** | Gradient reversal layer (&lambda;=0.2) + team classifier |

### Two-Stage Training

**Stage 1 &mdash; Masked Language Modeling:** 15% of action tokens are masked; the model predicts the original action type from surrounding context. The Deep Sets encoder processes freeze-frame coordinates at the masked token position, providing spatial context to the transformer.

**Stage 2 &mdash; Adversarial Debiasing:** A team classifier head is attached via a gradient reversal layer (Ganin et al. 2016). The encoder learns to produce embeddings that *cannot* predict which team a player belongs to, removing team-system confounds while retaining individual style signal.

### Dual-Vector Architecture

This model provides the **behavioral** half of a dual-vector player representation:

| Vector | Dimensions | Source | Captures |
|--------|-----------|--------|----------|
| **Behavioral** (this model) | 208 | Transformer + Deep Sets on SPADL + 360 freeze-frames | Playing style, spatial context, action sequences |
| **Statistical** | 13 | Z-score normalized per-90 stats | Goals, assists, xG, passes, VAEP, defensive metrics |

Both vectors are stored in PostgreSQL with [pgvector](https://github.com/pgvector/pgvector) HNSW indexes for sub-10ms similarity queries.

## Training Data

| Source | Matches | Events | License |
|--------|---------|--------|---------|
| [StatsBomb 360 Open Data](https://github.com/statsbomb/open-data) | 323 | ~2M | CC-BY 4.0 |

The 323-match corpus is the complete StatsBomb 360 open-data release. Coverage includes La Liga, Premier League, Champions League, Euro 2020, and Women's World Cup matches with freeze-frame annotations.

Training data is published as [`luxury-lakehouse/football2vec-360-training-data`](https://huggingface.co/datasets/luxury-lakehouse/football2vec-360-training-data) on HF Hub.

### Tokenization

Events are tokenized using the **23-type SPADL vocabulary**: `pass`, `cross`, `throw_in`, `freekick_crossed`, `freekick_short`, `corner_crossed`, `corner_short`, `take_on`, `foul`, `tackle`, `interception`, `shot`, `shot_penalty`, `shot_freekick`, `keeper_save`, `keeper_claim`, `keeper_punch`, `keeper_pick_up`, `clearance`, `bad_touch`, `non_action`, `dribble`, `goalkick`.

Continuous spatial coordinates (x, y) normalized to [0, 1] on a 105&times;68m pitch are injected via learned MLP projections. Freeze-frame coordinates encode visible players as an unordered set with a team indicator bit (0 = teammate, 1 = opponent).

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
| Deep Sets MLP dims | [32, 16] |
| Output dimension | 208 |
| Batch size | 256 |
| Learning rate | 1e-4 |
| Weight decay | 0.01 |
| Warmup fraction | 10% |
| Adversarial &lambda; max | 0.2 |
| Adversarial warmup epochs | 5 |

Training runs on HF Jobs A10G-small GPU (~90 minutes total for both stages).

## How to Use

### Quick Start

```python
from huggingface_hub import hf_hub_download
from safetensors.torch import load_file
import json

# Download model weights (Stage 2 — adversarial debiased)
weights_path = hf_hub_download("luxury-lakehouse/football2vec-360", "stage2/model.safetensors")
config_path = hf_hub_download("luxury-lakehouse/football2vec-360", "stage2/config.json")

with open(config_path) as f:
    config = json.load(f)

state_dict = load_file(weights_path)
print(f"Config: {config['hidden_dim']}-dim transformer + {config['deepsets_dim']}-dim Deep Sets")
print(f"Output dimension: {config['output_dim']}")  # 208
print(f"Parameters: {sum(p.numel() for p in state_dict.values()):,}")
```

### Pre-Computed Embeddings (recommended)

For most use cases, load the pre-computed embeddings directly &mdash; no model inference needed:

```python
from datasets import load_dataset
import numpy as np

ds = load_dataset("luxury-lakehouse/football2vec-360-embeddings")
df = ds["train"].to_pandas()

vectors = np.array(df["behavioral_vector"].tolist())
print(f"{vectors.shape[0]} player-matches, {vectors.shape[1]}-dim embeddings")  # (~4K, 208)
```

> **Note:** These embeddings cover only players with StatsBomb 360 match appearances (~4K player-match records vs. ~87K for Football2Vec v2). For broader coverage, use [Football2Vec v2](https://huggingface.co/luxury-lakehouse/football2vec-v2).

## Intended Use

- **Context-aware player similarity**: Cosine distance on 208-dim vectors finds players with similar style *and* spatial decision-making
- **Spatial pattern analysis**: The 16-dim Deep Sets component captures how players behave relative to nearby opponents and teammates
- **Scouting in high-press contexts**: Embeddings encode how players handle actions under spatial pressure from surrounding defenders
- **Research**: Reproducible 360-enriched player representations with adversarial debiasing; pairs with Football2Vec v2 for ablation studies
- **Downstream features**: Input to GNN tactical models where spatial context matters

## EU AI Act — Intended Use and Non-Use

This model is published for **research and reproducibility** purposes on public, open-licensed match data. It is **not intended for, not validated for, and not supplied to** any use that would fall within Annex III §4 (Employment, workers management and access to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of natural persons, decisions affecting work-related contractual relationships, promotion, termination, task allocation based on individual traits, or the monitoring and evaluation of performance and behaviour of workers for employment decisions. Player similarity search is a canonical scouting workflow, and any deployer is responsible for treating this model as decision-support at most, never as a decision system.

Any deployer who wishes to use this model for such a purpose is responsible for performing their own conformity assessment under Article 43, for drawing up the technical documentation required by Article 11 and Annex IV, for implementing the human oversight measures required by Article 14, for declaring accuracy metrics under Article 15, and for ensuring the data governance obligations of Article 10 are met. Note specifically that the training data contains no protected attributes and therefore cannot support the group-fairness audits required by Article 10(2)(g) without ingesting additional personal data. The adversarial team debiasing (Ganin et al. 2016) described above addresses a *confounding* effect (team-system leakage) and is not a substitute for an Article 10 protected-attribute audit.

See the [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis in the source repository for the project's full risk classification, re-classification triggers, and governance posture.

## Limitations

- **360-data only**: Covers 323 StatsBomb 360 matches. Players with appearances only in non-360 matches have no embeddings from this model.
- **Smaller training corpus**: 323 matches vs. ~3,000 for Football2Vec v2. Embeddings for players with few 360 appearances may be noisier.
- **Separate embedding space**: 208-dim vectors are not comparable to Football2Vec v2 192-dim vectors. Cannot mix in the same similarity index without re-embedding all players.
- **Event-based actions + freeze-frames**: Off-ball runs and pressing without a nearby action event are not captured.
- **Team debiasing, not competition debiasing**: The adversarial head targets team ID (stronger confounder in the smaller 360 corpus). Cross-league confounds are attenuated but not fully removed.
- **Open data only**: Derived from publicly available StatsBomb 360 data. Commercial datasets with proprietary 360 annotations may yield different representations.

## Freshness

| Metric | Value |
|--------|-------|
| **Training data freshness SLA** | 168 hours (7 days) |
| **Inference schedule** | Daily 06:00 UTC |
| **Skip guard** | `match_id`-level &mdash; only new 360 matches are processed |

## Model Files

```
stage1/model.safetensors      -- Stage 1 MLM checkpoint (safetensors format)
stage2/model.safetensors      -- Stage 2 adversarial, final (safetensors format)
stage2/config.json            -- Football2Vec360Config as JSON
zscore_params.json            -- z-score normalization parameters (13-dim stat vector)
```

Model weights use the **safetensors** format &mdash; a tensor-only serialization with zero pickle surface and no code execution capability. Pre-computed embeddings are delivered as Parquet (non-executable).

## Citation

```bibtex
@inproceedings{theiner2022explainable,
  title={Explainable Expected Goal Models for Performance Analysis in Football},
  author={Theiner, Jonas and M{\"u}ller-Budack, Eric and Ewerth, Ralph},
  booktitle={Proceedings of the 4th International Workshop on Multimedia Content Analysis in Sports},
  pages={39--47},
  year={2022}
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
| [360 Training Data](https://huggingface.co/datasets/luxury-lakehouse/football2vec-360-training-data) | SPADL sequences with 360 freeze-frames used for training |
| [360 Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-360-embeddings) | Pre-computed 208-dim vectors per player-match |
| [Football2Vec v2](https://huggingface.co/luxury-lakehouse/football2vec-v2) | 192-dim event-only model (~3,000 matches, broader coverage) |
| [Football2Vec v1](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout) | 32-dim Doc2Vec baseline |
| [SPADL/VAEP Action Values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Per-action offensive/defensive VAEP valuations |

## Demo

Try the interactive [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app) &mdash; the Player Similarity page supports similarity search on 360-enriched embeddings for players with 360 match coverage.

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) (inherited from StatsBomb open data terms)
- **v2 event-only model**: [Football2Vec v2](https://huggingface.co/luxury-lakehouse/football2vec-v2)
- **v1 baseline model**: [Football2Vec v1 (Doc2Vec)](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout)
- **Platform**: [Luxury Lakehouse Soccer Analytics](https://github.com/karsten-s-nielsen/luxury-lakehouse)
- **Workflow card**: `workflow-cards/wf-football2vec-360.yaml`
