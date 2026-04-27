---
license: cc-by-nc-4.0
language: en
tags:
  - sports-analytics
  - soccer
  - expected-goals
  - deep-sets
  - uncertainty-quantification
datasets:
  - luxury-lakehouse/xg-shot-data
  - luxury-lakehouse/xg-freeze-frame-data
metrics:
  - roc_auc
  - brier_score
pipeline_tag: tabular-classification
---

# xG v2 &mdash; Context-Aware Expected Goals with Freeze-Frame Set Encoding

Context-aware expected goals (xG) model that conditions on the visible player positions at the moment of each shot. Trained on **~131K shots** from [StatsBomb Open Data](https://github.com/statsbomb/open-data) and [Wyscout](https://figshare.com/collections/Soccer_match_event_dataset/4415000). Includes MC dropout uncertainty quantification &mdash; every prediction comes with a 95% confidence interval.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Model Description

Standard xG models treat each shot in isolation: distance, angle, body part, and a handful of tabular features. xG v2 adds **spatial context** by encoding the positions of all visible players from StatsBomb 360 freeze frames into a fixed-length context vector using a Deep Sets architecture (Zaheer et al. 2017).

The model answers the question: *given where the shooter is, where the defenders are, and where the goalkeeper is, what is the probability this shot results in a goal?*

Key properties:

- **Permutation-invariant**: Handles any number of visible players in any order. There is no fixed roster slot or player identity assumption.
- **Graceful degradation**: When no freeze-frame data is available, the context vector is zeroed out and the model degrades to tabular-only prediction &mdash; identical in structure to the v1 XGBoost baseline.
- **Uncertainty-aware**: MC dropout produces a mean xG estimate plus a 95% confidence interval, quantifying model confidence per shot rather than collapsing to a single scalar.
- **Serverless-compatible**: Pure NumPy inference. No PyTorch, no ONNX, no GPU. The JSON-serialized weight file is under 100 KB and loads on Databricks serverless executors.

## Architecture

The model combines a **set encoder** that processes freeze-frame player positions with a **prediction MLP** that fuses tabular shot features:

**Set Encoder (per-player, shared weights):**
1. Input: N players x 4 features (`x_norm`, `y_norm`, `is_keeper`, `is_teammate`)
2. Per-player MLP: Linear(4 &rarr; 32) &rarr; ReLU &rarr; Linear(32 &rarr; 16) &rarr; ReLU
3. Sum aggregation (permutation invariant) &rarr; **context vector (16-dim)**

**Prediction MLP:**
1. Concatenate: context vector (16-dim) + tabular features (13+ dim)
2. Linear(&rarr; 64) &rarr; ReLU &rarr; Dropout
3. Linear(&rarr; 32) &rarr; ReLU &rarr; Dropout
4. Linear(&rarr; 1) &rarr; Sigmoid &rarr; **xG score in [0, 1]**

### Set Encoder Hyperparameters

| Parameter | Value |
|-----------|-------|
| Player feature dim | 4 (`x_norm`, `y_norm`, `is_keeper`, `is_teammate`) |
| Encoder hidden dim | 32 |
| Context dim (output) | 16 |
| Aggregation | Sum (permutation invariant) |

### Prediction MLP Hyperparameters

| Parameter | Value |
|-----------|-------|
| Hidden layer 1 | 64 units, ReLU |
| Hidden layer 2 | 32 units, ReLU |
| Output | 1 unit, Sigmoid |
| Dropout rate | 0.1 |
| MC dropout samples | 50 |

## Uncertainty Quantification

xG v2 uses **MC Dropout** (Gal & Ghahramani 2016) as a practical Bayesian approximation. Dropout is active at inference time, and 50 stochastic forward passes are run per shot:

```python
for i in range(50):
    mask = Bernoulli(1 - 0.1, size=64+32)  # random dropout mask
    predictions[i] = forward_pass(shot, mask)

mean  = predictions.mean()
std   = predictions.std()
ci_95 = (clip(mean - 1.96*std, 0, 1),
         clip(mean + 1.96*std, 0, 1))
```

Each prediction returns a 4-tuple: `(mean, std, ci_lower, ci_upper)`.

**Interpretation**: A narrow CI (e.g., xG = 0.72 ± 0.03) indicates the model is confident. A wide CI (e.g., xG = 0.35 ± 0.18) signals high uncertainty &mdash; typical for partially occluded freeze frames or unusual shot geometries.

## Training Data

| Source | Shots | License |
|--------|-------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~75K | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | ~56K | CC-BY-NC 4.0 |
| **Total** | **~131K** | CC-BY-NC 4.0 (most restrictive applies) |

Freeze-frame coverage comes from StatsBomb 360 data: approximately 15.58M freeze-frame rows across 323 matches, embedded inline within the events JSON (`shot_freeze_frame` field). Wyscout shots contribute tabular features only &mdash; no freeze frames.

Coverage includes the Premier League, La Liga, Serie A, Bundesliga, Ligue 1, Champions League, World Cup, and more.

Training is performed on [Hugging Face Jobs](https://huggingface.co/docs/hub/jobs) using PyTorch. Inference uses the pure NumPy forward pass exported from the trained weights.

## Features

### Tabular Features (13)

These features are the same as the v1 XGBoost baseline:

| Feature | Type | Description |
|---------|------|-------------|
| `distance_to_goal` | Numeric | Euclidean distance from shot location to goal center (yards) |
| `shot_angle` | Numeric | Angle subtended by the goal from the shot location (radians) |
| `location_x` | Numeric | Shot x-coordinate (StatsBomb: 0&ndash;120) |
| `location_y` | Numeric | Shot y-coordinate (StatsBomb: 0&ndash;80) |
| `end_location_x` | Numeric | Intended x-coordinate of shot trajectory |
| `end_location_y` | Numeric | Intended y-coordinate of shot trajectory |
| `period` | Numeric | Match period (1&ndash;5) |
| `minute` | Numeric | Minute of the match |
| `is_first_time` | Boolean | Shot taken first-time (no control touch) |
| `shot_body_part` | Categorical | Head, Right Foot, Left Foot, No Touch |
| `shot_technique` | Categorical | Normal, Volley, Half Volley, Backheel, Overhead Kick, Diving Header, Lob |
| `shot_type` | Categorical | Open Play, Free Kick, Corner, Kick Off, Penalty |
| `play_pattern` | Categorical | From Counter, From Keeper, From Free Kick, From Corner, etc. |

### Set Encoder Input (variable-length, per visible player)

| Feature | Type | Description |
|---------|------|-------------|
| `x_norm` | Float \[0, 1\] | Player x-position normalized from StatsBomb 120m pitch |
| `y_norm` | Float \[0, 1\] | Player y-position normalized from StatsBomb 80m pitch |
| `is_keeper` | Binary | 1 if this player is the goalkeeper, 0 otherwise |
| `is_teammate` | Binary | 1 if this player is on the shooter's team, 0 for opponent |

Player identity is never used. The set encoder sees only spatial position and role.

## Performance

| Model | ROC-AUC | Brier Score | Log Loss |
|-------|---------|-------------|----------|
| v1 XGBoost + Isotonic Calibration (13 features) | 0.825 | 0.057 | 1.212 |
| v2 Set Encoder (raw, pre-calibration) | 0.901 | 0.061 | &mdash; |
| **v2 Set Encoder + Isotonic Calibration + MC Dropout** | **0.915** | **0.060** | **0.200** |

ROC-AUC improved by **+0.090** over the v1 XGBoost baseline (0.825 &rarr; 0.915) &mdash; a large gain in discrimination for xG models, where +0.02 is typically meaningful. Isotonic calibration closed the Brier score gap to 0.003 while reducing log loss sixfold (1.212 &rarr; 0.200). MC dropout 95% CI coverage: **95.1%** (properly calibrated).

**Training**: 153 seconds on HF Jobs A10G-small. MC dropout z-multiplier: 4.2, inference dropout rate: 0.30 (3&times; training dropout of 0.10).

**Evaluation protocol**: 80/20 train/test split by competition. Metrics computed on held-out test set.

## Coordinate System

All spatial features use the **StatsBomb coordinate system**:

- Pitch dimensions: 120 yards (length) &times; 80 yards (width)
- Origin: bottom-left corner of the pitch
- Attacking direction: left to right (x increases toward opponent goal)
- Goal center: approximately (120, 40)

Set encoder inputs normalize these to `[0, 1]`:

```
x_norm = location_x / 120.0
y_norm = location_y / 80.0
```

This normalization ensures that the per-player MLP receives consistent scale inputs regardless of pitch dimension conventions.

## Inference

The model is serialized as a JSON file with base64-encoded NumPy arrays &mdash; no pickle, no PyTorch dependency at inference time.

**Output mart**: predictions are persisted to `{catalog}.dev_gold.fct_xg_predictions_v2` as a dbt-built mart with `contract: enforced: true`, inheriting Kimball surrogate FKs (`match_key`, `team_key`, `player_key`, `competition_key`) via INNER JOIN to `fct_shots` on `shot_id` per ADR-013 (consumer-side ML inference output). PR 7 (2026-04-27) extended the inheritance with `team_key` + `player_key`.

```python
from huggingface_hub import hf_hub_download
import json

# Download weights
weights_path = hf_hub_download(
    repo_id="luxury-lakehouse/xg-v2-model-set-encoder",
    filename="xg_v2_weights.json",
)
with open(weights_path, "rb") as f:
    weights_bytes = f.read()

# Load weights (NumPy only)
from src.analytics.set_encoder import deserialize_set_encoder_weights
weights = deserialize_set_encoder_weights(weights_bytes)

# Encode freeze-frame player positions
import numpy as np
from src.analytics.set_encoder import encode_player_set, predict_xg_with_uncertainty

player_features = np.array([
    [0.85, 0.50, 1, 0],  # goalkeeper: x=102, y=40
    [0.80, 0.45, 0, 0],  # defender 1
    [0.78, 0.55, 0, 0],  # defender 2
    [0.82, 0.48, 0, 1],  # teammate
], dtype=np.float64)

context = encode_player_set(player_features, weights)

# Tabular features (pre-processed with build_features)
tabular = np.array([...])  # 13+ features after one-hot encoding

# Predict with uncertainty
mean_xg, std, ci_lower, ci_upper = predict_xg_with_uncertainty(
    tabular, context, weights
)
print(f"xG = {mean_xg:.3f} (95% CI: {ci_lower:.3f}-{ci_upper:.3f})")
```

For shots without freeze-frame data, pass a zero context vector:

```python
from src.analytics.set_encoder import SetEncoderConfig
config = SetEncoderConfig()
context = np.zeros(config.context_dim)  # graceful degradation to tabular-only
```

### Serialization Format

Weights are stored as a JSON envelope with base64-encoded arrays:

```json
{
  "model_type": "set_encoder_xg_v2",
  "weights": {
    "encoder_fc1_weight": {"data": "...", "shape": [32, 4], "dtype": "float64"},
    "encoder_fc1_bias":   {"data": "...", "shape": [32],   "dtype": "float64"},
    "encoder_fc2_weight": {"data": "...", "shape": [16, 32], "dtype": "float64"},
    "encoder_fc2_bias":   {"data": "...", "shape": [16],   "dtype": "float64"},
    "pred_fc1_weight":    {"data": "...", "shape": [64, ...], "dtype": "float64"},
    ...
  }
}
```

No pickle is used anywhere in the serialization or deserialization path (banned by project security policy).

## EU AI Act — Intended Use and Non-Use

This model is published for **research and reproducibility** purposes on public, open-licensed match data. It is **not intended for, not validated for, and not supplied to** any use that would fall within Annex III §4 (Employment, workers management and access to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of natural persons, decisions affecting work-related contractual relationships, promotion, termination, task allocation based on individual traits, or the monitoring and evaluation of performance and behaviour of workers for employment decisions.

Any deployer who wishes to use this model for such a purpose is responsible for performing their own conformity assessment under Article 43, for drawing up the technical documentation required by Article 11 and Annex IV, for implementing the human oversight measures required by Article 14, for declaring accuracy metrics under Article 15, and for ensuring the data governance obligations of Article 10 are met. Note specifically that the training data contains no protected attributes and therefore cannot support the group-fairness audits required by Article 10(2)(g) without ingesting additional personal data.

See the [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis in the source repository for the project's full risk classification, re-classification triggers, and governance posture.

## Limitations

- **Anonymous freeze frames**: The set encoder receives only position and role (keeper/teammate flag). Player identity, stamina, height, dominant foot, and tactical assignment are not encoded. Two players in identical positions produce identical context contributions.
- **Missing freeze-frame coverage**: Only StatsBomb 360 matches include freeze frames (~323 of ~3,000 StatsBomb matches). All Wyscout shots and non-360 StatsBomb shots fall back to the zero context vector.
- **Partial occlusion**: StatsBomb 360 freeze frames capture only *visible* players. Players behind the camera or in crowded areas may be absent. The set encoder handles this gracefully (sum over fewer players), but predictions may underestimate defensive pressure when multiple defenders are occluded.
- **Open data only**: Trained on publicly available StatsBomb and Wyscout data. Models trained on full broadcast-quality tracking data with complete visibility would likely produce narrower uncertainty intervals and higher discrimination.
- **Static snapshot**: The freeze frame captures player positions at the instant of the shot only. Prior positioning (run-up angle, off-ball movement, pressing intensity) is not encoded.
- **No player clustering or identity**: The set encoder cannot distinguish a massed low block from an isolated goalkeeper. Tactical shape is implicit in the aggregate position distribution, not explicit.

## Model Files

The model is published to three destinations, all in sync:

1. **HF Hub**: [`luxury-lakehouse/xg-v2-model-set-encoder`](https://huggingface.co/luxury-lakehouse/xg-v2-model-set-encoder)
   - `model_weights.json` — set encoder weights (JSON + base64, ~100 KB)
   - `metrics.json` — training metrics + dataset commit SHAs
2. **MLflow UC Registry**: `soccer_analytics.dev_gold.xg_model_v2@Champion`
   - Logged via `mlflow.pyfunc.log_model`; the `@Champion` alias points at
     the latest version. The raw weights are also logged as an artifact
     (`model_weights.json`) so the consumer can download them byte-for-byte.
3. **Databricks UC Volume**: `/Volumes/soccer_analytics/dev_gold/model_weights/xg_model_v2/`
   - `model_weights.json` — identical bytes to the HF Hub copy
   - `model_weights.json.sha256` — hex SHA-256 sidecar for SEC2 integrity verification

The Databricks serverless inference pipeline `ingestion.xg_model_v2` tries
MLflow `@Champion` first, then falls back to the UC Volume copy; the sidecar
lets the consumer detect tampering without trusting the MLflow registry
metadata alone.

## Citation

If you use this model, please cite the Deep Sets architecture and the MC Dropout method:

```bibtex
@inproceedings{zaheer2017deep,
  title={Deep Sets},
  author={Zaheer, Manzil and Kottur, Satwik and Ravanbakhsh, Siamak
          and P{\'o}czos, Barnab{\'a}s and Salakhutdinov, Ruslan
          and Smola, Alexander J.},
  booktitle={Advances in Neural Information Processing Systems (NeurIPS)},
  volume={30},
  year={2017}
}
```

```bibtex
@inproceedings{gal2016dropout,
  title={Dropout as a Bayesian Approximation: Representing Model Uncertainty
         in Deep Learning},
  author={Gal, Yarin and Ghahramani, Zoubin},
  booktitle={International Conference on Machine Learning (ICML)},
  pages={1050--1059},
  year={2016}
}
```

```bibtex
@software{nielsen2026xgv2,
  title={xG v2: Context-Aware Expected Goals with Freeze-Frame Set Encoding},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Companion Resources

| Dataset | Description |
|---------|-------------|
| [xG Shot Data](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data) | Tabular shot features used for training and evaluation |
| [xG Freeze Frame Data](https://huggingface.co/datasets/luxury-lakehouse/xg-freeze-frame-data) | StatsBomb 360 freeze-frame player positions (15.58M rows, 323 matches) |
| [SPADL/VAEP Action Values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Per-action offensive/defensive VAEP valuations |
| [Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) | Pre-computed behavioral + statistical vectors (career/season/match) |

## Demo

Try the interactive [Soccer Analytics Explorer](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) &mdash; visualize shot maps with v2 xG values and uncertainty bands, and compare v1 vs v2 predictions side-by-side.

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) (inherited from Wyscout training data)
- **v1 baseline model**: [xG v1 (XGBoost)](https://huggingface.co/luxury-lakehouse/xg-model-statsbomb-wyscout)
- **Platform**: [Luxury Lakehouse Soccer Analytics](https://github.com/karsten-s-nielsen/luxury-lakehouse)
