---
language:
  - en
license: cc-by-nc-4.0
library_name: xgboost
tags:
  - sports-analytics
  - soccer
  - football
  - expected-goals
  - xg
  - xgboost
  - logistic-regression
  - statsbomb
  - wyscout
  - calibration
datasets:
  - luxury-lakehouse/spadl-vaep-action-values
  - luxury-lakehouse/line-breaking-passes
  - luxury-lakehouse/football2vec-player-embeddings
  - luxury-lakehouse/pitch-control-tracking
pipeline_tag: tabular-classification
---

# xG Model &mdash; StatsBomb + Wyscout

Two expected goals (xG) models trained on **~131K professional soccer shots** from [StatsBomb Open Data](https://github.com/statsbomb/open-data) and [Wyscout](https://figshare.com/collections/Soccer_match_event_dataset/4415000):

1. **Logistic regression baseline** &mdash; distance + angle only (interpretable reference point)
2. **Calibrated XGBoost** &mdash; all 13 features with isotonic calibration (production model)

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Model Description

### Logistic Baseline

A logistic regression fitted on two geometric features (distance to goal center, shot angle) with isotonic calibration. Serves as an interpretable lower bound &mdash; any production model must beat this baseline.

### Calibrated XGBoost

A gradient-boosted tree classifier (XGBClassifier) fitted on all 13 features, wrapped in scikit-learn's `CalibratedClassifierCV` with isotonic regression. The calibration step ensures that predicted probabilities are well-calibrated (a 0.15 xG prediction means ~15% of such shots are goals).

### Serialization

Both models are serialized as **JSON envelopes** &mdash; no pickle is used (banned by project security policy):

- **XGBoost**: Booster saved via `save_raw("json")`, base64-encoded in a JSON envelope alongside isotonic calibrator thresholds
- **Logistic**: Coefficients, intercept, and classes stored as JSON arrays alongside isotonic calibrator thresholds

This makes model weights fully inspectable, version-controllable, and safe to load without arbitrary code execution.

## Training Data

| Source | Shots | License |
|--------|-------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~95K | CC-BY 4.0 (match data) |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | ~36K | CC-BY-NC 4.0 (research) |

Coverage includes the Premier League, La Liga, Serie A, Bundesliga, Ligue 1, Champions League, World Cup, and more. Both sources are unified to the StatsBomb 120&times;80 yard coordinate system at the dbt staging layer.

### Features

All 13 features used by the XGBoost model:

| Feature | Type | Description |
|---------|------|-------------|
| `distance_to_goal` | Numeric | Euclidean distance from shot location to goal center (yards) |
| `shot_angle` | Numeric | Angle subtended by the goal from the shot location (radians) |
| `location_x` | Numeric | Shot x-coordinate (0&ndash;120 yards, attacking direction) |
| `location_y` | Numeric | Shot y-coordinate (0&ndash;80 yards) |
| `end_location_x` | Numeric | Shot end x-coordinate |
| `end_location_y` | Numeric | Shot end y-coordinate |
| `period` | Numeric | Match period (1 = first half, 2 = second half, etc.) |
| `minute` | Numeric | Match minute |
| `is_first_time` | Boolean | Whether the shot was taken first-time (no prior touch) |
| `shot_body_part` | Categorical | Body part used (Right Foot, Left Foot, Head, Other) |
| `shot_technique` | Categorical | Technique (Normal, Volley, Half Volley, Lob, Overhead Kick, etc.) |
| `shot_type` | Categorical | Shot type (Open Play, Free Kick, Corner, Penalty, etc.) |
| `play_pattern` | Categorical | Build-up pattern (Regular Play, From Counter, From Corner, etc.) |

Categorical features are one-hot encoded. The logistic baseline uses only `distance_to_goal` and `shot_angle`.

### Coordinate System

All coordinates are in the StatsBomb system: **120 &times; 80 yards**, with (0, 0) at the bottom-left corner of the pitch and the attacking goal at x = 120. Wyscout coordinates (0&ndash;100% scale) are converted at the dbt staging layer.

### Hyperparameters

| Parameter | Value |
|-----------|-------|
| XGBoost `n_estimators` | 100 |
| XGBoost `max_depth` | 3 |
| XGBoost `learning_rate` | 0.1 |
| XGBoost `eval_metric` | logloss |
| Calibration method | Isotonic regression |
| Test split | 20% (stratified by `competition_id`) |
| Random state | 42 |

## Evaluation Metrics

Both models are evaluated on a held-out test set using:

| Metric | Description |
|--------|-------------|
| **Brier score** | Mean squared error of probability estimates (lower is better) |
| **Log loss** | Logarithmic loss (lower is better) |
| **ROC-AUC** | Area under the ROC curve (higher is better) |
| **Calibration error (ECE)** | Expected calibration error across 10 uniform bins (lower is better) |

### Results (held-out test set, ~26K shots)

| Model | ROC-AUC | Brier Score |
|-------|---------|-------------|
| Custom XGBoost (calibrated) | **0.979** | 0.059 |
| Custom Logistic (baseline) | 0.761 | 0.082 |

### StatsBomb xG Benchmark

The custom XGBoost model is benchmarked against StatsBomb's proprietary xG on the StatsBomb subset of the test set. **Acceptance criterion**: custom xG Brier score must be within 10% of StatsBomb xG Brier score.

## How to Use

### Quick Start

```bash
pip install huggingface_hub xgboost scikit-learn
```

```python
import json
import base64

from huggingface_hub import snapshot_download
from xgboost import XGBClassifier
import numpy as np

# Download model
model_dir = snapshot_download("luxury-lakehouse/xg-model-statsbomb-wyscout")

# Load XGBoost model from JSON envelope
with open(f"{model_dir}/xgboost_model.json") as f:
    envelope = json.load(f)

booster_raw = base64.b64decode(envelope["booster_b64"])
xgb = XGBClassifier()
xgb.load_model(bytearray(booster_raw))

# Predict xG for a shot (requires one-hot encoded feature vector)
# See training notebook for full feature engineering pipeline
```

> **Note:** The raw XGBoost booster above produces uncalibrated probabilities. For
> production use, load the model with `deserialize_xgboost_model` (shown below) which
> wraps the booster in scikit-learn's `CalibratedClassifierCV` with isotonic regression.
> Without this calibration step, predicted xG values may be systematically over- or
> under-confident.

### Full Pipeline (with calibration)

For production use with isotonic calibration, use the `deserialize_xgboost_model` and `deserialize_logistic_model` functions from the `analytics.xg_model` module:

```python
from analytics.xg_model import deserialize_xgboost_model, deserialize_logistic_model

with open(f"{model_dir}/xgboost_model.json", "rb") as f:
    xgb_model = deserialize_xgboost_model(f.read())

with open(f"{model_dir}/logistic_model.json", "rb") as f:
    logistic_model = deserialize_logistic_model(f.read())

# predict_proba returns calibrated probabilities
xg_proba = xgb_model.predict_proba(X)[:, 1]
```

## Intended Use

- **Shot valuation**: Assign expected goal probabilities to shots for match analysis
- **Player evaluation**: Aggregate xG for player performance assessment (goals vs. xG)
- **Tactical analysis**: Identify high-quality shooting opportunities by location and context
- **Research**: Reproducible xG baseline for sports analytics on open data

## EU AI Act — Intended Use and Non-Use

This model is published for **research and reproducibility** purposes on public, open-licensed match data. It is **not intended for, not validated for, and not supplied to** any use that would fall within Annex III §4 (Employment, workers management and access to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of natural persons, decisions affecting work-related contractual relationships, promotion, termination, task allocation based on individual traits, or the monitoring and evaluation of performance and behaviour of workers for employment decisions.

Any deployer who wishes to use this model for such a purpose is responsible for performing their own conformity assessment under Article 43, for drawing up the technical documentation required by Article 11 and Annex IV, for implementing the human oversight measures required by Article 14, for declaring accuracy metrics under Article 15, and for ensuring the data governance obligations of Article 10 are met. Note specifically that the training data contains no protected attributes and therefore cannot support the group-fairness audits required by Article 10(2)(g) without ingesting additional personal data.

See the [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis in the source repository for the project's full risk classification, re-classification triggers, and governance posture.

## Limitations

- **Open data only**: Trained on publicly available StatsBomb and Wyscout data. Commercial datasets with richer features (freeze-frame defenders, goalkeeper position) would yield better models.
- **No defensive context**: The model does not include freeze-frame features (number of defenders, goalkeeper position, blocking angle). These are available in StatsBomb 360 data but not universally across all matches.
- **Cross-source alignment**: StatsBomb and Wyscout use different event taxonomies and coordinate systems. The dbt staging layer normalizes them, but subtle differences in shot classification may remain.
- **Calibration on open data**: Isotonic calibration is fitted on the same data distribution. Applying to a substantially different league or era may require recalibration.

## Citation

If you use this model, please cite the XGBoost method and this repository:

```bibtex
@inproceedings{chen2016xgboost,
  title={XGBoost: A Scalable Tree Boosting System},
  author={Chen, Tianqi and Guestrin, Carlos},
  booktitle={Proceedings of the 22nd ACM SIGKDD International Conference on Knowledge Discovery and Data Mining},
  year={2016}
}
```

```bibtex
@software{nielsen2026xgmodel,
  title={Custom xG Model: Logistic Baseline + Calibrated XGBoost on StatsBomb and Wyscout Open Data},
  author={Nielsen, Karsten Skytt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Model Files

```
xgboost_model.json    -- calibrated XGBoost (JSON envelope, no pickle)
logistic_model.json   -- calibrated logistic baseline (JSON envelope, no pickle)
metrics.json          -- evaluation metrics and training configuration
```

## Training

- **Notebook**: `notebooks/train_xg_model.py` (Databricks notebook)
- **UC Volume**: `/Volumes/soccer_analytics/dev_gold/model_weights/xg_model/`
- **Source module**: `src/analytics/xg_model.py`

## Companion Resources

Pre-computed datasets derived from the platform's analytics pipelines:

| Dataset | Description |
|---------|-------------|
| [SPADL/VAEP Action Values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Per-action offensive/defensive VAEP valuations |
| [Line-Breaking Passes](https://huggingface.co/datasets/luxury-lakehouse/line-breaking-passes) | Pass dataset with defensive line-breaking labels |
| [Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) | Pre-computed behavioral + statistical vectors (career/season/match) |
| [Pitch Control Tracking](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) | Per-player per-frame pitch control values from tracking data |

## Demo

Try the interactive [Soccer Analytics Explorer](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) &mdash; explore shot maps, pitch control, player similarity, and more.

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) (inherited from Wyscout training data)
