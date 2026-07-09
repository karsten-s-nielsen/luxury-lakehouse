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
  - luxury-lakehouse/xg-shot-data-v3
  - luxury-lakehouse/xg-shot-freeze-frames
metrics:
  - roc_auc
  - brier_score
pipeline_tag: tabular-classification
model-index:
  - name: xg-v3-model-set-encoder
    results:
      - task:
          type: tabular-classification
          name: Pre-Shot Expected Goals
        dataset:
          type: luxury-lakehouse/xg-shot-data-v3
          name: xG Shot Data v3 (all providers, GroupKFold-by-match_key, context-aware OOS)
        metrics:
          - type: roc_auc
            value: 0.7421
            name: ROC-AUC (all-provider, context-aware, out-of-sample)
          - type: brier_score
            value: 0.0831
            name: Brier Score (all-provider, context-aware, out-of-sample)
---

# xG v3 &mdash; Canonical-SPADL Pre-Shot Expected Goals with Freeze-Frame Set Encoding

Context-aware, **canonical-SPADL-native** expected goals (xG) model that conditions on the visible player positions at the moment of each shot. Trained on shots from all six providers in the platform &mdash; [StatsBomb Open Data](https://github.com/statsbomb/open-data), [Wyscout](https://figshare.com/collections/Soccer_match_event_dataset/4415000), IDSSE (Bundesliga), Metrica Sports, SkillCorner, and GradientSports &mdash; including the tracking cohorts' shot-instant freeze frames. Includes MC dropout uncertainty quantification &mdash; every prediction comes with a 95% confidence interval.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

> **Naming note (m4 decoupling).** The governance workflow card stays `wf-xg-v2` (evolve-in-place, no governance-inventory churn) while the underlying MLflow model artifact is **`xg_model_v3`** &mdash; the SPADL-native, all-provider retrain. The workflow-card **name** (`wf-xg-v2`) and the model-artifact **version** (`xg_model_v3`) are intentionally decoupled: "v2" identifies the governed system (the Deep Sets freeze-frame architecture); "v3" identifies the specific trained weights. This card documents the `xg_model_v3` artifact; the [`wf-xg-v2` model card](xg-v2-model-card.md) documents the governed system.

## Model Description

Standard xG models treat each shot in isolation: distance, angle, body part, and a handful of tabular features. xG v3 adds **spatial context** by encoding the positions of all visible players &mdash; from StatsBomb 360 freeze frames and from the tracking cohorts' shot-instant frames (IDSSE, Metrica, SkillCorner, GradientSports) &mdash; into a fixed-length context vector using a Deep Sets architecture (Zaheer et al. 2017). All geometry is expressed in canonical SPADL coordinates (105&times;68 m), so every provider's shot and freeze frame normalizes identically.

The model answers the question: *given where the shooter is, where the defenders are, and where the goalkeeper is, what is the probability this shot results in a goal?*

Key properties:

- **Permutation-invariant**: Handles any number of visible players in any order. There is no fixed roster slot or player-identity assumption.
- **Graceful degradation**: When no freeze-frame data is available, the context vector is zeroed out and the model degrades to tabular-only prediction. The `set_cardinality` feature (below) lets the prediction head disentangle "no players encoded" from a genuinely sparse frame.
- **Trained on the tracking cohorts**: The GS / SkillCorner full-22 freeze frames are IN the training set, held out cleanly via GroupKFold-by-`match_key`, so full-22 scoring is in-distribution rather than out-of-distribution.
- **Uncertainty-aware**: MC dropout produces a mean xG estimate plus a 95% confidence interval, quantifying model confidence per shot rather than collapsing to a single scalar.
- **Serverless-compatible**: Pure NumPy inference. No PyTorch, no ONNX, no GPU. The JSON-serialized weight file is under 100 KB and loads on Databricks serverless executors.

## What Changed vs v2

1. **Canonical SPADL 105&times;68 geometry &mdash; never StatsBomb yards.** Tabular geometry (`distance_to_goal` / `shot_angle`) is computed from the action-stream SPADL `start_x/start_y` (goal at `(105, 34)`, width 7.32 m). Freeze frames normalize `÷105, ÷68`. The weight envelope records `coordinate_system: "spadl_105x68"`.
2. **Uniform, geometry-only feature set.** v3 ships EXACTLY five tabular features &mdash; `distance_to_goal`, `shot_angle`, `location_x`, `location_y`, `set_cardinality` &mdash; and **no** StatsBomb categoricals (body part / technique / type / play pattern). This makes the tabular-only path identical across providers, so a Wyscout shot and a StatsBomb shot are scored on the same axes.
3. **Set-cardinality feature.** The Deep-Sets encoder SUMS over the player set, so a full-22 set has systematically larger context magnitude than an SB-360 partial set. `set_cardinality` (number of players encoded; 0 for zero-context) lets the prediction MLP disentangle count from summed magnitude.
4. **Single-calibration ownership.** The model emits **RAW** (uncalibrated) xG. The trainer fits per-provider AND pooled calibrators on leak-free GroupKFold out-of-fold predictions and ships them in the weight envelope under `_calibrators` as serve-time parameters; the scorer applies them. The trainer applies nothing to the served weights.
5. **Penalty constant.** `shot_penalty` shots are excluded from training (fixed ~0.76 geometry craters a geometry model). The trainer computes the empirical penalty conversion rate over the loaded corpus and ships it as `_penalty_xg`; the scorer assigns it as a constant at scoring time.

## Architecture

The model combines a **set encoder** that processes freeze-frame player positions with a **prediction MLP** that fuses tabular shot features:

**Set Encoder (per-player, shared weights):**
1. Input: N players &times; 4 features (`x_norm`, `y_norm`, `is_keeper`, `is_teammate`)
2. Per-player MLP: Linear(4 &rarr; 32) &rarr; ReLU &rarr; Linear(32 &rarr; 16) &rarr; ReLU
3. Sum aggregation (permutation invariant) &rarr; **context vector (16-dim)**

**Prediction MLP:**
1. Concatenate: context vector (16-dim) + tabular features (5-dim)
2. Linear(&rarr; 64) &rarr; ReLU &rarr; Dropout
3. Linear(&rarr; 32) &rarr; ReLU &rarr; Dropout
4. Linear(&rarr; 1) &rarr; **RAW logit** (sigmoid at inference &rarr; RAW xG in [0, 1])

### Hyperparameters

| Parameter | Value |
|-----------|-------|
| Player feature dim | 4 (`x_norm`, `y_norm`, `is_keeper`, `is_teammate`) |
| Encoder hidden dim | 32 |
| Context dim (output) | 16 |
| Aggregation | Sum (permutation invariant) |
| Prediction hidden layers | 64, 32 (ReLU) |
| Dropout rate | 0.1 |
| MC dropout samples | 50 |

## Uncertainty Quantification

xG v3 uses **MC Dropout** (Gal & Ghahramani 2016) as a practical Bayesian approximation. Dropout is active at inference time, and 50 stochastic forward passes are run per shot; the mean is the point estimate and the spread gives a 95% confidence interval (with an empirically-tuned z-multiplier stored in the envelope). A narrow CI (e.g., xG = 0.72 &plusmn; 0.03) indicates confidence; a wide CI (e.g., xG = 0.35 &plusmn; 0.18) signals high uncertainty &mdash; typical for partially occluded freeze frames or unusual shot geometries.

## Training Data

The v3 retrain is **canonical-SPADL-native and all-provider**. Shots are drawn from all six providers; the tracking cohorts additionally contribute shot-instant freeze frames. The open-play training family is `{shot, shot_freekick}`; `shot_penalty` is excluded (see *Penalty constant* above). The goal label is `action_result == 'success'`.

| Source | Freeze frames? | License |
|--------|----------------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | Yes (StatsBomb 360) | CC-BY 4.0 |
| [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | No (tabular only) | CC-BY-NC 4.0 |
| IDSSE (Bundesliga) | Yes (tracking) | Open data |
| Metrica Sports | Yes (tracking) | Open data |
| SkillCorner | Yes (tracking) | Restricted / private cohort |
| GradientSports | Yes (tracking) | Restricted / private cohort |

For the tracking cohorts the freeze frame is the strictly-pre-shot tracking snapshot, converted to canonical SPADL geometry. Wyscout shots and non-360 StatsBomb shots contribute tabular features only &mdash; no freeze frame, so they score in tabular-only mode.

> **Restricted cohorts.** SkillCorner and GradientSports are private/restricted providers. Their rows carry `access_tier = restricted` and are never published to public HuggingFace repos (per ADR-049 / ADR-064). They contribute to training and scoring via a permanent private companion repo, but not to public redistribution. Public model artifacts and datasets reflect open-licensed providers only.

Training is performed on [Hugging Face Jobs](https://huggingface.co/docs/hub/jobs) using PyTorch. Inference uses the pure NumPy forward pass exported from the trained weights.

## Features

### Tabular Features (5, geometry-only)

| Feature | Type | Description |
|---------|------|-------------|
| `distance_to_goal` | Numeric | Euclidean distance from shot location to goal center (SPADL metres, goal at `(105, 34)`) |
| `shot_angle` | Numeric | Angle subtended by the goal (7.32 m width) from the shot location (radians) |
| `location_x` | Numeric | Shot x-coordinate (SPADL: 0&ndash;105) |
| `location_y` | Numeric | Shot y-coordinate (SPADL: 0&ndash;68) |
| `set_cardinality` | Numeric | Number of players encoded in the freeze frame (0 for zero-context) |

### Set Encoder Input (variable-length, per visible player)

| Feature | Type | Description |
|---------|------|-------------|
| `x_norm` | Float \[0, 1\] | Player x-position normalized from the SPADL 105 m pitch (`x / 105`) |
| `y_norm` | Float \[0, 1\] | Player y-position normalized from the SPADL 68 m pitch (`y / 68`) |
| `is_keeper` | Binary | 1 if this player is the goalkeeper, 0 otherwise |
| `is_teammate` | Binary | 1 if this player is on the shooter's team, 0 for opponent |

Player identity is never used. The set encoder sees only spatial position and role.

## Performance

All metrics below are the live `xg_model_v3` retrain's held-out out-of-sample (OOS) evaluation, computed under GroupKFold-by-`match_key` (no same-match leakage). v3 changes the coordinate contract (SPADL-native), the provider mix (all six), the feature set (geometry-only), and the calibration scheme (per-provider + pooled OOF), so no prior split-by-competition metric describes the shipped model; the v1 baseline row is retained for reference only.

| Model | Overall ROC-AUC | Brier Score | Brier Skill |
|-------|-----------------|-------------|-------------|
| v1 XGBoost + Isotonic Calibration (13 features) | 0.825 | 0.057 | &mdash; |
| **v3 Set Encoder (SPADL-native, all-provider) + per-provider/pooled calibration + MC Dropout** | **0.7421** | **0.0831** | **0.0931** |

The all-provider context-aware Champion scores **ROC-AUC 0.7421 / Brier 0.0831 / Brier-skill 0.0931 / ECE 0.0023** (OOS). The v1 row is a different corpus (StatsBomb-only, 13 StatsBomb categoricals) and coordinate system (yards), so its higher AUC is not comparable &mdash; v3 trades single-provider categorical richness for a uniform geometry-only, all-provider, SPADL-native contract. Log loss is not reported for v3; Brier skill (skill relative to the base goal rate; higher = better) is the headline calibration-aware discrimination metric.

### Per-provider two-mode OOS

Each provider is scored two ways: **context-aware** (the full freeze frame is encoded) and **tabular-only** (the context vector is zeroed, `set_cardinality = 0`). The three providers with no freeze frames (IDSSE, Metrica, Wyscout) are **identical by construction** &mdash; their context-aware score IS the zero-context path. For the three freeze-frame providers (GradientSports, SkillCorner, StatsBomb), context-aware **beats** tabular-only, which is the empirical justification for encoding the freeze frame.

| Provider | Freeze frames? | Context-aware (AUC / Brier / Brier-skill / ECE) | Tabular-only (AUC / Brier / Brier-skill / ECE) |
|----------|:--:|-------------------------------------------------|------------------------------------------------|
| gradientsports | Yes | 0.7430 / 0.0906 / 0.0757 / 0.0312 | 0.7400 / 0.0932 / 0.0497 / 0.0301 |
| idsse | No | 0.6546 / 0.0852 / 0.0320 / 0.0566 | *identical (no freeze frames)* |
| metrica | No | 0.8240 / 0.0867 / 0.1645 / 0.0433 | *identical (no freeze frames)* |
| skillcorner | Yes | 0.6805 / 0.0951 / 0.0563 / 0.0339 | 0.6675 / 0.0945 / 0.0614 / 0.0302 |
| statsbomb | Yes | 0.7390 / 0.0831 / 0.0887 / 0.0073 | 0.7380 / 0.0832 / 0.0877 / 0.0084 |
| wyscout | No | 0.7556 / 0.0822 / 0.1043 / 0.0120 | *identical (no freeze frames)* |
| **all** | mixed | **0.7421 / 0.0831 / 0.0931 / 0.0023** | 0.7408 / 0.0832 / 0.0922 / 0.0029 |

*AUC = ROC-AUC (discrimination, higher = better); Brier = mean squared error of the probability (lower = better); Brier-skill = skill vs the base goal rate (higher = better); ECE = expected calibration error (lower = better).*

**Champion architecture**: Deep-Sets set encoder, 50 training epochs, MC-dropout confidence intervals; validation AUC &asymp; 0.744.

**Calibration** (leak-free out-of-fold): the pooled/global calibrator is **isotonic**; per-provider calibrators are {gradientsports: isotonic, idsse: platt, metrica: platt, skillcorner: platt, statsbomb: isotonic, wyscout: isotonic}.

**Evaluation protocol**: GroupKFold-by-`match_key` (no same-match leakage), scored in both modes per provider, with a per-provider discrimination gate relative to StatsBomb (see below).

## Coordinate System

All spatial features use the **canonical SPADL coordinate system**:

- Pitch dimensions: 105 m (length) &times; 68 m (width)
- Origin: bottom-left corner of the pitch
- Attacking direction: left to right (x increases toward the opponent goal)
- Goal center: **(105, 34)**; goal width: **7.32 m**

Every provider's shots and freeze frames are converted to SPADL geometry upstream (`silly-kicks` converters + the lakehouse SPADL adapters), so a single coordinate contract holds across StatsBomb, Wyscout, IDSSE, Metrica, SkillCorner, and GradientSports. Set encoder inputs normalize to `[0, 1]` via `x_norm = location_x / 105`, `y_norm = location_y / 68`.

## Per-Provider Calibration & Two-Mode Scoring

Providers differ in what freeze-frame context they can supply. Rather than assume every provider's freeze frame is equally informative, v3 scores each provider under a per-provider policy:

- **Two-mode scoring for tracking providers.** Every tracking cohort (IDSSE, Metrica, SkillCorner, GradientSports) is scored two ways &mdash; **context-aware** (the full freeze frame is encoded) and **tabular-only** (the context vector is zeroed and `set_cardinality = 0`, the trained zero-context path).
- **Per-provider discrimination gate.** For each provider, out-of-sample discrimination (ROC-AUC) in context-aware mode is compared against tabular-only mode, benchmarked **relative to StatsBomb** using the AUC CI lower bound. The shipped Champion uses the **StatsBomb-relative discrimination floor `sb_auc = 0.7390`** (the StatsBomb context-aware OOS AUC) as the certification benchmark, and `_gate` ships the per-provider AUC-CI evidence the scorer certifies against. The shipped mode for that provider is the one that clears the gate: if the tracking freeze frame does not measurably improve discrimination over tabular-only, that provider ships in tabular-only mode. This prevents a noisy or mis-oriented tracking freeze frame from *degrading* a provider's xG.
- **Per-provider + pooled calibration.** The trainer fits a calibrator per `data_source` and a pooled/global calibrator on leak-free out-of-fold predictions. On the shipped Champion the pooled calibrator is **isotonic**, and the per-provider calibrators are {gradientsports: isotonic, idsse: platt, metrica: platt, skillcorner: platt, statsbomb: isotonic, wyscout: isotonic} &mdash; isotonic is used only where it strictly wins on group-disjoint reliability, Platt otherwise. These ride in the weight envelope under `_calibrators`; the scorer applies the appropriate one. The model output itself stays RAW.
- **`ood_flag`.** When a cohort fails **either** the discrimination gate **or** the calibration fit, its predictions are stamped with an out-of-distribution flag so downstream consumers can surface the caveat or exclude the cohort.

The per-provider two-mode OOS scores, the calibration family per provider, and the `sb_auc = 0.7390` gate floor are reported in the [Performance](#performance) section above and published with the `xg_model_v3` Champion in `metrics.json` (`oos_by_provider_mode`, `calibrators`, `gate`).

## Inference

The model is serialized as a JSON file with base64-encoded NumPy arrays &mdash; no pickle, no PyTorch dependency at inference time. The envelope carries the trained `feature_names` (geometry-only, pinned order), `tabular_dim`, `coordinate_system: "spadl_105x68"`, the MC-dropout inference parameters, the per-provider + pooled `_calibrators`, and the `_penalty_xg` constant.

**Output mart**: predictions are persisted to the canonical-SPADL pre-shot xG mart `{catalog}.dev_gold.fct_shot_xg` (ADR-066) as a dbt-built mart with `contract: enforced: true`, inheriting Kimball surrogate FKs (`match_key`, `team_key`, `player_key`, `competition_key`) via INNER JOIN to `fct_shots` on `shot_id` per ADR-013. Each row also carries the per-provider scoring mode and the `ood_flag`.

No pickle is used anywhere in the serialization or deserialization path (banned by project security policy).

## EU AI Act — Intended Use and Non-Use

This model is published for **research and reproducibility** purposes on public, open-licensed match data. It is **not intended for, not validated for, and not supplied to** any use that would fall within Annex III §4 (Employment, workers management and access to self-employment) of Regulation (EU) 2024/1689 — including recruitment or selection of natural persons, decisions affecting work-related contractual relationships, promotion, termination, task allocation based on individual traits, or the monitoring and evaluation of performance and behaviour of workers for employment decisions.

Any deployer who wishes to use this model for such a purpose is responsible for performing their own conformity assessment under Article 43, for drawing up the technical documentation required by Article 11 and Annex IV, for implementing the human oversight measures required by Article 14, for declaring accuracy metrics under Article 15, and for ensuring the data governance obligations of Article 10 are met. Note specifically that the training data contains no protected attributes and therefore cannot support the group-fairness audits required by Article 10(2)(g) without ingesting additional personal data.

See the [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis in the source repository for the project's full risk classification, re-classification triggers, and governance posture.

## Limitations

- **Anonymous freeze frames**: The set encoder receives only position and role (keeper/teammate flag). Player identity, stamina, height, dominant foot, and tactical assignment are not encoded.
- **Missing freeze-frame coverage**: Wyscout shots and non-360 StatsBomb shots carry no freeze frame and fall back to the zero-context vector (tabular-only mode). Per the discrimination gate, some tracking cohorts may also ship in tabular-only mode.
- **Partial occlusion**: StatsBomb 360 freeze frames capture only *visible* players; tracking-cohort freeze frames have their own coverage and orientation caveats, which the per-provider discrimination gate and `ood_flag` are designed to catch.
- **Mixed licensing**: Trained on a mix of open-data providers (StatsBomb, Wyscout, IDSSE, Metrica) and restricted/private cohorts (SkillCorner, GradientSports). Public model artifacts and datasets reflect open-licensed providers only; restricted cohorts contribute to training/scoring but are never redistributed publicly (ADR-049 / ADR-064).
- **Static snapshot**: The freeze frame captures player positions at the instant of the shot only. Prior positioning (run-up angle, off-ball movement, pressing intensity) is not encoded.

## Model Files

The model is published to three destinations, all in sync:

1. **HF Hub**: [`luxury-lakehouse/xg-v3-model-set-encoder`](https://huggingface.co/luxury-lakehouse/xg-v3-model-set-encoder)
   - `model_weights.json` — set encoder weights + envelope (JSON + base64, ~100 KB)
   - `metrics.json` — training metrics, calibrators, penalty constant, and dataset commit SHAs
2. **MLflow UC Registry**: `soccer_analytics.dev_gold.xg_model_v3@Champion`
3. **Databricks UC Volume**: `/Volumes/soccer_analytics/dev_gold/model_weights/xg_model_v3/`
   - `model_weights.json` — identical bytes to the HF Hub copy
   - `model_weights.json.sha256` — hex SHA-256 sidecar for SEC2 integrity verification

The Databricks serverless inference pipeline tries MLflow `@Champion` first, then falls back to the UC Volume copy; the sidecar lets the consumer detect tampering without trusting the MLflow registry metadata alone.

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
@software{nielsen2026xgv3,
  title={xG v3: Canonical-SPADL Pre-Shot Expected Goals with Freeze-Frame Set Encoding},
  author={Nielsen, Karsten Skyt},
  year={2026},
  url={https://github.com/karsten-s-nielsen/luxury-lakehouse}
}
```

## Companion Resources

| Dataset | Description |
|---------|-------------|
| [xG Shot Data v3](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data-v3) | Canonical-SPADL tabular shot features used for training and evaluation |
| [xG Shot Freeze Frames](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-freeze-frames) | Per-shot SPADL freeze-frame player sets (context corpus) |
| [SPADL/VAEP Action Values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | Per-action offensive/defensive VAEP valuations |

## More Information

- **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) (inherited from Wyscout training data)
- **Governed system card**: [xG v2 (wf-xg-v2)](xg-v2-model-card.md)
- **Platform**: [Luxury Lakehouse Soccer Analytics](https://github.com/karsten-s-nielsen/luxury-lakehouse)
