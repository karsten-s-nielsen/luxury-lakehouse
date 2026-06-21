---
license: cc-by-nc-4.0
language:
  - en
library_name: scikit-learn
tags:
  - soccer-analytics
  - goalkeeper
  - expected-goals
  - psxg
datasets:
  - luxury-lakehouse/statsbomb-shots-on-target
metrics:
  - brier_score
  - log_loss
pipeline_tag: tabular-classification
---

# PSxG Model &mdash; Post-Shot Expected Goals for Goalkeeper Evaluation

Logistic regression model estimating the probability that an on-target shot becomes a goal, from its **projected goal-line crossing + shot geometry** (4 features). Trained on **32,698 StatsBomb on-target shots** (29.9% goal rate); out-of-sample **AUC 0.818, Brier 0.153** (GroupKFold by match). Model version `v2-ontarget`. Used to compute *goals prevented* (PSxG &minus; actual goals conceded) as the primary shot-stopping pillar of the goalkeeper evaluation framework.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Model Description

PSxG (Post-Shot Expected Goals) conditions on the observed shot destination &mdash; where the ball was headed when it crossed the goalline plane &mdash; rather than the shooter's location. This gives a fairer evaluation of the goalkeeper: a shot headed for the top corner is harder to save than one at chest height in the center, regardless of where on the pitch the shot was taken.

### Features

The model scores a 4-feature vector (`analytics.goalkeeper.PSXG_FEATURE_NAMES`), built identically for both modalities through one shared port:

| Feature | Description |
|---------|-------------|
| `goalmouth_dist_from_centre` | `min(\|y_norm − 0.5\|, 0.5)` — distance of the goal-line crossing from goal centre. The goal-vs-save signal is a symmetric arch (near-post hardest, dead-centre saveable), which a logistic on a linear y cannot represent. |
| `goalmouth_z` | Crossing height as a fraction of goal width (`z / 7.32 m`). |
| `distance_to_goal_m` | Shot origin → goal centre, **metres** (StatsBomb yards harmonised ×0.9144; tracking already metres). |
| `shot_angle` | Angle subtended at the posts, radians. |

**Goal-line crossing (the key fix — [ADR-060](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/superpowers/adrs/ADR-060-psxg-projected-goalmouth-four-feature-model.md)):** StatsBomb `end_location` is the goal-line crossing **only for goals** (`end_location_x`=120); for saves it is the *save point* (~x=118) or deflected end position, so raw `end_location_y/z` is the wrong coordinate for ~67% of shots (the prior placement-only model was near-random, OOS AUC 0.525). The crossing is recovered by projecting the trajectory `location → end_location` onto the goal plane (x=120). The **tracking** modality uses its *measured* ball crossing (TF-48 `shot_crossing_y/z`) directly — no projection. `distance_to_goal`/`shot_angle` are computed identically across modalities (replicating the dbt macros in SPADL coords), so one Champion scores both.

### Architecture

| Component | Detail |
|-----------|--------|
| **Model class** | `sklearn.linear_model.LogisticRegression` |
| **Solver** | `lbfgs` |
| **Max iterations** | 1000 |
| **Feature scaling** | `StandardScaler` (fit on training set, serialized with model) |
| **Serialization** | JSON (coefficients + intercept + scaler params &mdash; zero pickle surface) |

Logistic regression on four low-dimensional features keeps the model transparent and auditable; the surface is near-linear in log-odds space, so no tree-based or neural model is needed. Richer covariates (goalkeeper position, shot speed) are deliberately deferred (ADR-060 / spec §8/E).

### Model Output

| Field | Type | Description |
|-------|------|-------------|
| `psxg` | float64 | Probability that the shot becomes a goal (0 = certain save, 1 = certain goal) |

**Goals prevented** = sum(PSxG over shots faced) &minus; actual goals conceded. Positive = better than average, negative = worse than average.

## Training Data

| Source | On-Target Shots | License |
|--------|----------------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~15K | CC-BY 4.0 |

Training data is published as [`luxury-lakehouse/statsbomb-shots-on-target`](https://huggingface.co/datasets/luxury-lakehouse/statsbomb-shots-on-target) on HF Hub.

Only true on-target shots are included: `shot_outcome IN ('Goal', 'Saved', 'Post', 'Saved to Post')` (`Off T`, `Blocked`, `Wayward`, and `Saved Off Target` are excluded as off-target). `Post` / `Saved to Post` are kept because the tracking `shot_on_target_derived` geometry counts post/bar strikes as on-target (ball-radius tolerance), so both modalities share one definition. A prior revision filtered on `end_location_z IS NOT NULL`, which silently included ~46% off-target `Off T` shots and depressed the goal rate to 15.9%; the corrected population is **32,698 shots at a 29.9% goal rate**. On this population the placement-only 2-feature model was near-random (OOS AUC 0.525 — raw `end_location` is the save-point for non-goals); the projected-crossing 4-feature model (`v2-ontarget`, ADR-060) scores **OOS AUC 0.818, Brier 0.153** (GroupKFold by `match_key`, n=32,698 — no same-match leakage).

## How to Use

### Load and Run Inference

```python
import json
import numpy as np
from huggingface_hub import hf_hub_download

# Download model coefficients
config_path = hf_hub_download("luxury-lakehouse/psxg-model", "psxg_model.json")

with open(config_path) as f:
    model = json.load(f)

# Extract logistic regression parameters (feature order in model["feature_names"])
coef = np.array(model["coefficients"])  # shape (4,)
intercept = np.array(model["intercept"])  # shape (1,)
scaler_mean = np.array(model["scaler_mean"])
scaler_scale = np.array(model["scaler_scale"])

def predict_psxg(features: np.ndarray) -> float:
    """features = [goalmouth_dist_from_centre, goalmouth_z, distance_to_goal_m, shot_angle].
    Build them with analytics.goalkeeper.build_psxg_features_{statsbomb,tracking}
    (StatsBomb projects to the goal line; tracking uses the measured crossing)."""
    x_scaled = (np.asarray(features, dtype=float) - scaler_mean) / scaler_scale
    log_odds = x_scaled @ coef.T + intercept
    return float(1.0 / (1.0 + np.exp(-log_odds)))

# Example: near-post crossing (0.45 from centre), mid-height (0.30), 12 m out, angle 0.40 rad
psxg = predict_psxg(np.array([0.45, 0.30, 12.0, 0.40]))
print(f"PSxG: {psxg:.3f}")
```

### Load Pre-Computed Predictions

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/psxg-predictions")
df = ds["train"].to_pandas()

# Goals prevented per keeper (positive = better than average)
gp = df.groupby("player_id").agg(
    psxg_sum=("psxg", "sum"),
    goals_conceded=("is_goal", "sum"),
).assign(goals_prevented=lambda d: d["psxg_sum"] - d["goals_conceded"])
print(gp.sort_values("goals_prevented", ascending=False).head())
```

## Intended Use

- **Goalkeeper evaluation**: Primary shot-stopping metric for `fct_goalkeeper_stats`
- **Benchmarking**: Compare GK shot-stopping performance against PSxG expectations across seasons
- **Research**: Transparent four-feature PSxG baseline for goalkeeper analytics research
- **Downstream**: Input to the composite goalkeeper score (Lamberts 2025 framework)

## EU AI Act — Intended Use and Non-Use

This model is published for **research and reproducibility** purposes on public, open-licensed match data. It is **not intended for, not validated for, and not supplied to** any use that would fall within Annex III §4 (Employment, workers management and access to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of natural persons, decisions affecting work-related contractual relationships, promotion, termination, task allocation based on individual traits, or the monitoring and evaluation of performance and behaviour of workers for employment decisions. In particular, "goals prevented" (PSxG − actual goals conceded) is not a fit-for-purpose metric for goalkeeper contract decisions without full Article 14 human oversight by the deploying organisation.

Any deployer who wishes to use this model for such a purpose is responsible for performing their own conformity assessment under Article 43, for drawing up the technical documentation required by Article 11 and Annex IV, for implementing the human oversight measures required by Article 14, for declaring accuracy metrics under Article 15, and for ensuring the data governance obligations of Article 10 are met. Note specifically that the training data contains no protected attributes and therefore cannot support the group-fairness audits required by Article 10(2)(g) without ingesting additional personal data.

See the [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis in the source repository for the project's full risk classification, re-classification triggers, and governance posture.

## Limitations

- **Four features**: projected goalmouth crossing (distance-from-centre + height) + shot distance + angle. Shot speed, goalkeeper position, deflections, and defensive pressure are not captured (deferred, ADR-060 / spec §8/E).
- **Open data only**: Trained on StatsBomb open data (~15K shots). Commercial datasets with larger coverage may yield different calibration.
- **StatsBomb coordinate system**: `end_location_z` is available only in StatsBomb 360 data. Models trained on providers without z-coordinate data will require a 2D fallback.
- **No freeze-frame context**: PSxG does not model the goalkeeper's starting position or movement. See the [xG v2 model](https://huggingface.co/luxury-lakehouse/xg-v2-model-set-encoder) for freeze-frame-conditioned expected goals.

## Model Files

```
psxg_model.json      -- LogisticRegression coefficients, intercept, and scaler params (JSON)
```

Model parameters use **JSON serialization** &mdash; a text-only format with zero pickle surface and no code execution capability. Predictions are delivered as Parquet (non-executable).

## Citation

```bibtex
@article{butcher2025xgot,
  title={An Expected Goals On Target (xGOT) Model},
  author={Butcher, J. and others},
  journal={Big Data and Cognitive Computing},
  volume={9},
  number={3},
  pages={64},
  year={2025},
  publisher={MDPI},
  url={https://www.mdpi.com/2504-2289/9/3/64}
}
```

```bibtex
@software{nielsen2026psxg,
  title={PSxG Model: Post-Shot Expected Goals for Goalkeeper Evaluation},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Companion Resources

| Resource | Description |
|----------|-------------|
| [On-Target Shot Data](https://huggingface.co/datasets/luxury-lakehouse/statsbomb-shots-on-target) | Training data: ~15K StatsBomb on-target shots with goalmouth coordinates |
| [PSxG Predictions](https://huggingface.co/datasets/luxury-lakehouse/psxg-predictions) | Per-shot PSxG predictions with player and match identifiers |
| [xG Shot Data](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data) | Full shot dataset (pre-shot xG features, StatsBomb + Wyscout) |
| [xG Model v2](https://huggingface.co/luxury-lakehouse/xg-v2-model-set-encoder) | Deep Sets xG with freeze-frame context and MC Dropout uncertainty |

## Demo

Try the interactive [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app) &mdash; explore goalkeeper shot-stopping metrics on the Defensive Impact page.

> **Explore interactively:** [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/)
- **Methodology**: Butcher et al. (2025) xGOT framework
- **Platform**: [Luxury Lakehouse Soccer Analytics](https://github.com/karsten-s-nielsen/luxury-lakehouse)
- **Workflow card**: `workflow-cards/wf-goalkeeper.yaml`
