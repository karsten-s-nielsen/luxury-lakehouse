# Hugging Face Hub Integration

> **After this guide you will have:** (1) loaded pre-trained football2vec embeddings and verified their shape, (2) retrained on your own data and confirmed the output, (3) published artifacts to Hugging Face Hub.

This guide covers how to use the pre-trained football2vec models (v1 Doc2Vec and v2 transformer), retrain on your own data, and set up your own Hugging Face org for publishing.

> **See also:** [v2 Model card](huggingface/model-cards/football2vec-v2-model-card.md) (source of truth for the [HF v2 model page](https://huggingface.co/luxury-lakehouse/football2vec-v2)), [v1 Model card](huggingface/model-card.md) (source of truth for the [HF v1 model page](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout)), and [Org card](huggingface/org-card.md) (source of truth for the [HF org page](https://huggingface.co/luxury-lakehouse)).

---

## Prerequisites

| Term | Definition |
|------|-----------|
| **UC Volume** | Databricks Unity Catalog storage volume — a managed cloud storage path for files |
| **Databricks Connect** | SDK for connecting local Python to a remote Databricks cluster |
| **SPADL** | Simplified Player Action Description Language — a unified event format ([Decroos et al. 2019](https://doi.org/10.1007/s10994-019-05849-6)) |
| **Per-90 stats** | Player statistics normalized to 90-minute match equivalents |
| **Doc2Vec / PV-DM** | Paragraph Vector Distributed Memory — a document embedding algorithm ([Le & Mikolov 2014](https://arxiv.org/abs/1405.4053)) |
| **gensim** | Python library for topic modeling and document similarity ([radimrehurek.com/gensim](https://radimrehurek.com/gensim/)) |
| **Secret scope** | Databricks-managed key vault for storing credentials (e.g., HF tokens) |
| **canonical_player_id** | Platform's deduplicated player identifier across data sources |
| **HF write token** | Hugging Face Hub API token with write permission for publishing artifacts |
| **z-score normalization** | Statistical standardization: (value - mean) / std_dev |

**Required tools:**
- Python >=3.10, <3.11 (strict — Databricks serverless constraint; 3.11+ will cause failures)
- **v1 (Doc2Vec):** `gensim>=4.3.0`, `huggingface_hub>=1.5.0` (included in project dependencies)
- **v2 (Transformer):** `torch>=2.0`, `numpy>=1.24`, `huggingface_hub>=1.5.0` (training only; pre-computed embeddings need only `datasets` + `numpy`)

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

# Quick shape check and similarity lookup
vector = doc2vec.dv[doc2vec.dv.index_to_key[0]]
similar = doc2vec.dv.most_similar(doc2vec.dv.index_to_key[0], topn=3)
print(vector.shape, similar)
```

**Verify:** Run the code above. Expected output:
- `vector.shape` returns `(32,)` — a 32-dimensional embedding vector
- `similar[:3]` returns 3 player name/similarity pairs

## 1b. Using Football2Vec v2 (Transformer, Recommended)

Football2Vec v2 is a 128-dim transformer encoder with adversarial team debiasing. It replaces v1 as the `@Champion` model. For most use cases, load pre-computed embeddings directly:

```bash
pip install datasets numpy
```

```python
from datasets import load_dataset
import numpy as np

ds = load_dataset("luxury-lakehouse/football2vec-player-embeddings")
df = ds["train"].to_pandas()

vectors = np.array(df["behavioral_vector"].tolist())
print(f"{vectors.shape[0]} players, {vectors.shape[1]}-dim embeddings")
# Expected: (8950, 128)
```

To load the model weights directly (for fine-tuning or custom inference):

```python
from huggingface_hub import hf_hub_download
from safetensors.torch import load_file
import json

weights_path = hf_hub_download("luxury-lakehouse/football2vec-v2", "stage2/model.safetensors")
config_path = hf_hub_download("luxury-lakehouse/football2vec-v2", "stage2/config.json")

with open(config_path) as f:
    config = json.load(f)

state_dict = load_file(weights_path)
print(f"Architecture: {config['hidden_dim']}-dim, {config['num_layers']} layers, {config['num_heads']} heads")
# Expected: 128-dim, 4 layers, 4 heads
```

**Verify:** `vectors.shape` returns `(8950, 128)` for career embeddings.

> **See also:** [v2 Model card](huggingface/model-cards/football2vec-v2-model-card.md) for architecture details, training hyperparameters, and adversarial debiasing methodology.

---

## 2. Retraining on Your Data

### Prerequisites

- Python >=3.10, <3.11 (strict — Databricks serverless constraint), `gensim`, `huggingface_hub` installed (included in project dependencies)
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

**Verify:** Check the embedding table was populated:
```python
# In a Databricks notebook
display(spark.table("soccer_analytics.dev_gold.fct_player_embeddings").count())
# Expected: >0 rows
```

4. **Publish to Hugging Face Hub** (optional):
   If you have configured the Databricks secret scope `hf` / key `token`, pipeline tasks that write to HF Hub (export training data, export shots, prepare 360 data) automatically authenticate. To set up:
   ```bash
   # Create the secret scope and add your HF write token (one-time setup)
   databricks secrets create-scope hf
   databricks secrets put-secret hf token --string-value hf_xxxxx
   ```
   The Terraform workflow injects `HF_TOKEN` via `{{secrets/hf/token}}` into tasks using the `hf` environment. Write tasks call `dbutils.secrets.get` at runtime; read-only tasks (importing from public repos) need no token. See `terraform/modules/workflows/main.tf` for the environment definitions.

**Verify:** Confirm the upload succeeded:
```python
from huggingface_hub import list_repo_files
files = list_repo_files("your-org/football2vec-statsbomb-wyscout")
print(files)  # Expected: includes 'model', 'z_score_params.json'
```

## 2b. Retraining Football2Vec v2 (Transformer)

V2 training runs on HF Jobs GPU (A10G-large) in two stages:

1. **Export training data** from Databricks:
   ```bash
   uv run export_embeddings_training_data --catalog soccer_analytics --schema dev_gold
   ```
   This writes SPADL action sequences to UC Volume and publishes to `luxury-lakehouse/football2vec-training-data` on HF Hub.

2. **Train Stage 1 (MLM)** on HF Jobs:
   ```bash
   hf jobs uv run scripts/train_football2vec_v2.py --stage 1 \
       --flavor a10g-large --timeout 120m \
       --secrets HF_TOKEN=$HF_TOKEN \
       --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \
       --env DATABRICKS_HOST=$DATABRICKS_HOST \
       --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN
   ```

3. **Train Stage 2 (Adversarial debiasing)** on HF Jobs:
   ```bash
   hf jobs uv run scripts/train_football2vec_v2.py --stage 2 \
       --flavor a10g-large --timeout 120m \
       --secrets HF_TOKEN=$HF_TOKEN \
       --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \
       --env DATABRICKS_HOST=$DATABRICKS_HOST \
       --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN
   ```

4. **Run embedding inference** to populate Delta tables:
   ```bash
   uv run compute_embeddings --catalog soccer_analytics --schema dev_gold
   ```

The v2 training script logs to MLflow, registers the model as `football2vec_v2`, and publishes weights + embeddings to HF Hub. Typical training time: ~2 hours on A10G-large (~$3.00).

### UC Volume Storage (v2)

```
/Volumes/soccer_analytics/dev_gold/model_weights/football2vec_v2/
├── model.safetensors    — Encoder weights (safetensors format, zero pickle)
└── config.json          — Football2VecConfig
```

---

## 3. Hugging Face Org Setup (New Forks)

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
huggingface_hub>=1.5.0
```

These are declared in `pyproject.toml` and included in the wheel install.

## Common Issues

| Problem | Cause | Fix |
|---------|-------|-----|
| `AuthenticationError` on HF Hub push | Missing or invalid HF token | Run `huggingface-cli login` with a write token, or set the Databricks secret: `databricks secrets put-secret hf token` |  <!-- pragma: allowlist secret -->
| `WAREHOUSE_NOT_RUNNING` or timeout on SQL queries | SQL warehouse auto-stopped after 10 min idle | Run `python scripts/ensure_warehouse.py` before any Databricks operation |
| `FileNotFoundError` on UC Volume path | Model weights not yet written to the Volume | Run the training notebook first (`notebooks/train_football2vec.py`), or download from HF Hub with `snapshot_download` |
| `ModuleNotFoundError: gensim` | gensim not installed in current environment | Run `uv sync` from the repo root to install all dependencies |
