# HuggingFace Hub Integration

This guide covers how to use the pre-trained football2vec model, retrain on your own data, and set up your own HuggingFace org for publishing.

> **See also:** [Model card](huggingface/model-card.md) (source of truth for the [HF model page](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout)) and [Org card](huggingface/org-card.md) (source of truth for the [HF org page](https://huggingface.co/luxury-lakehouse)).

---

## 1. Using the Pre-Trained Model (Forks)

The published model includes a gensim Doc2Vec checkpoint and z-score normalization parameters. No GPU required.

```bash
pip install huggingface_hub gensim
```

```python
from huggingface_hub import snapshot_download

model_dir = snapshot_download("luxury-lakehouse/football2vec-statsbomb-wyscout")
# model_dir contains:
#   player2vec.model     — gensim Doc2Vec (32-dim behavioral embeddings)
#   zscore_params.json   — z-score normalization params for 13-dim stat vectors
```

Load and infer:

```python
import json
from gensim.models.doc2vec import Doc2Vec

doc2vec = Doc2Vec.load(f"{model_dir}/player2vec.model")

with open(f"{model_dir}/zscore_params.json") as f:
    zscore_params = json.load(f)
# zscore_params maps feature name -> {"mean": float, "std": float}
```

## 2. Retraining on Your Data

### Prerequisites

- Python 3.10+, `gensim`, `huggingface_hub` installed (included in project dependencies)
- `fct_player_stats` populated in your Databricks workspace

### Steps

1. **Set HF cache location** (optional, avoids cluttering home directory):
   ```bash
   export HF_HOME=/path/to/cache
   ```

2. **Run the training notebook** on Databricks:
   ```
   notebooks/train_football2vec.py
   ```
   This trains the Doc2Vec model on your StatsBomb corpus and saves artifacts to the UC Volume.

3. **Run the embedding ingestion** via the registered entry point:
   ```bash
   # On Databricks (workflow task) — catalog/schema from job parameters
   compute_embeddings --catalog soccer_analytics --schema dev_gold

   # Local development — requires Databricks Connect or exported parquet
   uv run compute_embeddings --catalog soccer_analytics --schema dev_gold
   ```

4. **Publish to HuggingFace Hub** (optional):
   The training notebook (`notebooks/train_football2vec.py`) automatically publishes to HF Hub if the Databricks secret scope `hf` / key `token` is configured. To set up:
   ```bash
   # Create the secret scope and add your HF write token
   databricks secrets create-scope hf
   databricks secrets put-secret hf token --string-value hf_xxxxx
   ```
   This uploads the trained model to `luxury-lakehouse/football2vec-statsbomb-wyscout` (or your configured org/repo). Publishing is optional — the pipeline works without it.

## 3. HuggingFace Org Setup (New Forks)

If you're forking this repo and want your own HF org:

1. **Create an account** at [huggingface.co](https://huggingface.co)
2. **Create an organization**: Settings > Organizations > New Organization
   - Pick a URL-friendly name (e.g., `my-football-analytics`)
3. **Create a write token**: Settings > Access Tokens > New Token
   - Token type: `Write`
   - Scope: your organization
4. **Update the model repo** in your fork's embedding config to point to `your-org/football2vec-statsbomb-wyscout`
5. **Store the token** as `HF_TOKEN` environment variable (never commit it)

## 4. Databricks Integration

### UC Volume Storage

Model artifacts are cached in a Unity Catalog Volume for Databricks workflow access:

```
/Volumes/soccer_analytics/dev_gold/model_weights/football2vec/
├── player2vec.model          — gensim Doc2Vec checkpoint
├── player2vec.model.dv.vectors.npy
├── player2vec.model.wv.vectors.npy
└── zscore_params.json        — z-score normalization parameters
```

The training pipeline writes here automatically. The embedding ingestion task reads from this path.

### Workflow Environment

The `compute_embeddings` Databricks workflow task requires these packages in its environment:

```
gensim>=4.3
huggingface_hub>=0.20
```

These are declared in `pyproject.toml` and included in the wheel install.
