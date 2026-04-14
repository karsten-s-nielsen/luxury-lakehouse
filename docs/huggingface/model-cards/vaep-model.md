---
license: cc-by-nc-4.0
language: en
library_name: xgboost
tags:
  - sports-analytics
  - soccer
  - football
  - vaep
  - action-valuation
  - xgboost
  - statsbomb
  - wyscout
  - silly-kicks
datasets:
  - luxury-lakehouse/spadl-vaep-action-values
  - luxury-lakehouse/xg-shot-data
  - luxury-lakehouse/football2vec-player-embeddings
pipeline_tag: tabular-classification
---

# VAEP Model &mdash; StatsBomb + Wyscout

Two XGBClassifier models that estimate **P(scoring)** and **P(conceding)** within the next 10 actions, enabling per-action valuation of every on-ball event in a soccer match. Trained on **~2,388 matches** from [StatsBomb Open Data](https://github.com/statsbomb/open-data) and [Wyscout](https://figshare.com/collections/Soccer_match_event_dataset/4415000) via [Hugging Face Jobs](https://huggingface.co/docs/hub/jobs) (CPU).

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Model Description

**VAEP** (Valuing Actions by Estimating Probabilities) scores each on-ball action by its impact on the probability of scoring and conceding within the next 10 actions, as described in:

> Decroos, T., Bransen, L., Van Haaren, J., & Davis, J. (2019). **Actions Speak Louder than Goals: Valuing Player Actions in Soccer.** *Proceedings of the 25th ACM SIGKDD International Conference on Knowledge Discovery & Data Mining.*

This model implements the VAEP framework using two independent XGBClassifier models:

- **P(scores)**: Probability that the team in possession scores within the next 10 actions
- **P(concedes)**: Probability that the team in possession concedes within the next 10 actions

The net VAEP value of an action is the change in scoring probability minus the change in conceding probability before and after that action:

```
VAEP(action_i) = [P_scores(S_i) - P_scores(S_{i-1})] + [P_concedes(S_{i-1}) - P_concedes(S_i)]
```

where `S_i` is the game state after action `i`.

### Architecture

Both models are XGBClassifier instances with identical hyperparameters:

| Parameter | Value |
|-----------|-------|
| `n_estimators` | 100 |
| `max_depth` | 3 |
| `learning_rate` | 0.1 |
| `objective` | `binary:logistic` |
| `eval_metric` | `logloss` |
| `random_state` | 42 |

### Feature Extraction

Features are extracted using the [silly-kicks](https://github.com/karsten-s-nielsen/silly-kicks) library with 11 feature functions applied to game states composed of the current action and the previous `NB_PREV_ACTIONS = 3` actions:

| Feature Function | Description |
|-----------------|-------------|
| `actiontype_onehot` | One-hot encoding of the 23 SPADL action types |
| `result_onehot` | One-hot encoding of action outcomes (success, fail, etc.) |
| `bodypart_onehot` | One-hot encoding of body part (foot, head, other) |
| `time` | Period and time within the period |
| `startlocation` | Start x, y coordinates (SPADL 105&times;68m) |
| `endlocation` | End x, y coordinates |
| `startpolar` | Start location in polar coordinates (distance + angle to goal) |
| `endpolar` | End location in polar coordinates |
| `movement` | Displacement between start and end locations |
| `team` | Whether the team changed between consecutive actions |
| `time_delta` | Time elapsed between consecutive actions |

With `NB_PREV_ACTIONS = 3`, each feature function generates columns for the current action plus the 3 preceding actions, creating a rich game state representation.

### Serialization

Both models are serialized in a single **JSON envelope** &mdash; no pickle is used (banned by project security policy):

- Each model's XGBoost booster is saved via `save_raw("json")` and base64-encoded
- The envelope includes the number of input features and `nb_prev_actions` for reproducibility

```json
{
  "model_type": "vaep_xgboost_v1",
  "scores_booster_b64": "...",
  "concedes_booster_b64": "...",
  "n_features": 264,
  "nb_prev_actions": 3
}
```

This makes model weights fully inspectable, version-controllable, and safe to load without arbitrary code execution.

## Training Data

| Source | Matches | License |
|--------|---------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~3,000 | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | ~1,900 | CC-BY-NC 4.0 |
| **Total** | **~2,388** (deduplicated) | CC-BY-NC 4.0 (most restrictive applies) |

Coverage includes the Premier League, La Liga, Serie A, Bundesliga, Ligue 1, Champions League, World Cup, and more.

All event data is converted to the **SPADL** (Soccer Player Action Description Language) unified format with standardized coordinates (105&times;68 meters) and 23 canonical action types, enabling cross-source training without vendor-specific adapters.

Training is performed on [Hugging Face Jobs](https://huggingface.co/docs/hub/jobs) using the `cpu-basic` flavor.

### Training / Test Split

- **80/20 split** by game, stratified by `competition_id`
- Evaluation metrics computed on the held-out test set

## How to Use

### Quick Start

```bash
pip install huggingface_hub xgboost
```

```python
import json
import base64

from huggingface_hub import snapshot_download
from xgboost import XGBClassifier

# Download model
model_dir = snapshot_download("luxury-lakehouse/vaep-model-statsbomb-wyscout")

# Load from JSON envelope
with open(f"{model_dir}/vaep_model.json") as f:
    envelope = json.load(f)

# Deserialize P(scores) model
model_scores = XGBClassifier()
scores_raw = base64.b64decode(envelope["scores_booster_b64"])
model_scores.load_model(bytearray(scores_raw))

# Deserialize P(concedes) model
model_concedes = XGBClassifier()
concedes_raw = base64.b64decode(envelope["concedes_booster_b64"])
model_concedes.load_model(bytearray(concedes_raw))

# Predict probabilities (requires silly-kicks feature extraction)
# p_scores = model_scores.predict_proba(X)[:, 1]
# p_concedes = model_concedes.predict_proba(X)[:, 1]
```

### Full Pipeline (with silly-kicks)

```python
import silly_kicks.spadl as spadl
import silly_kicks.vaep.features as fs
import silly_kicks.vaep.labels as labels

NB_PREV_ACTIONS = 3

FEATURE_FNS = [
    fs.actiontype_onehot, fs.result_onehot, fs.bodypart_onehot,
    fs.time, fs.startlocation, fs.endlocation,
    fs.startpolar, fs.endpolar, fs.movement, fs.team, fs.time_delta,
]

# actions: pandas DataFrame in SPADL format
gamestates = fs.gamestates(actions, nb_prev_actions=NB_PREV_ACTIONS)
X = pd.concat([fn(gamestates) for fn in FEATURE_FNS], axis=1)

p_scores = model_scores.predict_proba(X)[:, 1]
p_concedes = model_concedes.predict_proba(X)[:, 1]

# Compute VAEP values (change in probabilities)
vaep_offensive = p_scores[1:] - p_scores[:-1]   # delta P(scores)
vaep_defensive = p_concedes[:-1] - p_concedes[1:]  # delta P(concedes), note sign flip
vaep_value = vaep_offensive + vaep_defensive
```

## Intended Use

- **Player valuation**: Rank players by total VAEP contribution beyond goals and assists
- **Action analysis**: Identify the most impactful passes, carries, and defensive actions in a match
- **Tactical analysis**: Evaluate team playing styles by aggregating VAEP across action types
- **Scouting**: Compare players across leagues using a unified valuation framework
- **Research**: Reproducible VAEP implementation on open data

## EU AI Act — Intended Use and Non-Use

This model is published for **research and reproducibility** purposes on public, open-licensed match data. It is **not intended for, not validated for, and not supplied to** any use that would fall within Annex III §4 (Employment, workers management and access to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of natural persons, decisions affecting work-related contractual relationships, promotion, termination, task allocation based on individual traits, or the monitoring and evaluation of performance and behaviour of workers for employment decisions.

Any deployer who wishes to use this model for such a purpose is responsible for performing their own conformity assessment under Article 43, for drawing up the technical documentation required by Article 11 and Annex IV, for implementing the human oversight measures required by Article 14, for declaring accuracy metrics under Article 15, and for ensuring the data governance obligations of Article 10 are met. Note specifically that the training data contains no protected attributes and therefore cannot support the group-fairness audits required by Article 10(2)(g) without ingesting additional personal data.

See the [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis in the source repository for the project's full risk classification, re-classification triggers, and governance posture.

## Limitations

- **Open data only**: Trained on publicly available StatsBomb and Wyscout data. Commercial datasets with richer event annotations may yield different VAEP scores.
- **No tracking data**: VAEP is event-based. Off-ball positioning, pressing intensity, and space creation are not captured. See [OBSO](https://github.com/karsten-s-nielsen/luxury-lakehouse) for tracking-based approaches.
- **Competition-agnostic**: The models are trained across all competitions jointly. League-specific models may produce more calibrated probabilities for individual leagues.
- **Cross-source alignment**: StatsBomb and Wyscout use different event taxonomies. The SPADL adapter normalizes them, but subtle differences in event definitions (e.g., duel classification) remain.
- **No calibration**: Unlike the xG models, the VAEP classifiers do not include post-hoc isotonic calibration. Predicted probabilities may not be perfectly calibrated in absolute terms. For ranking players or actions, this is less consequential; for applications requiring absolute probability values, validate with a reliability diagram.
- **10-action horizon**: VAEP considers only the next 10 actions. Longer-range effects (e.g., a switch of play that leads to a goal 20 actions later) are not captured.

## Model Files

```
vaep_model.json    -- scores + concedes XGBoost boosters (JSON envelope, no pickle)
metrics.json       -- evaluation metrics and training configuration
```

## Citation

If you use this model, please cite the original VAEP paper:

```bibtex
@inproceedings{decroos2019actions,
  title={Actions Speak Louder than Goals: Valuing Player Actions in Soccer},
  author={Decroos, Tom and Bransen, Lotte and Van Haaren, Jan and Davis, Jesse},
  booktitle={Proceedings of the 25th ACM SIGKDD International Conference on Knowledge Discovery and Data Mining},
  pages={1851--1861},
  year={2019},
  publisher={ACM}
}
```

And the silly-kicks library:

```bibtex
@article{silly-kicks,
  title={silly-kicks: A Python library for valuing soccer actions},
  author={Decroos, Tom and Van Haaren, Jan and Davis, Jesse},
  year={2020},
  url={https://github.com/karsten-s-nielsen/silly-kicks}
}
```

```bibtex
@software{nielsen2026vaep,
  title={VAEP Model: Action Valuation on StatsBomb and Wyscout Open Data},
  author={Nielsen, Karsten Skytt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Companion Resources

| Dataset | Description |
|---------|-------------|
| [SPADL/VAEP Action Values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Pre-computed per-action VAEP valuations (~9.5M actions) |
| [xG Shot Data](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data) | Tabular shot features for xG training |
| [Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) | Pre-computed behavioral + statistical vectors |

## Demo

Try the interactive [Soccer Analytics Explorer](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) &mdash; explore player impact rankings powered by VAEP valuations, and compare players across leagues.

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) (inherited from Wyscout training data)
- **Training script**: `scripts/train_vaep_model_hf.py` (PEP 723 standalone)
- **Source module**: `src/ingestion/spadl_vaep.py`
- **Platform**: [Luxury Lakehouse Soccer Analytics](https://github.com/karsten-s-nielsen/luxury-lakehouse)
