# D32 — ScoutGPT Decoder: Design Spec

**Date**: 2026-04-03
**Branch**: `feature/d32-scoutgpt-decoder`
**Status**: Approved
**Scope**: D32 only (training + evaluation). D33 (integration, pgvector, Taipy) is deferred.

## Overview

Player-conditioned GPT-style causal transformer over SPADL possession episodes.
Autoregressive next-action prediction with per-action player attribution and VAEP
auxiliary head. Trained on ~9.5M actions segmented into possession episodes, with
11,918 players in the conditioning embedding table.

**Reference**: Hong, J., Lee, S., Jo, J., So, D., Bauer, S. & Ko, S. (2025).
ScoutGPT: Player-conditioned Football Language Model for Counterfactual Evaluation.
*arXiv:2512.17266*. License: GREEN (arXiv preprint, no code, ideas freely implementable).

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Player conditioning | Prepend token at position 0 | Matches Hong et al., clean counterfactual swap (replace one vector) |
| Training sequence unit | Possession episodes | Team interaction context; matches reference paper |
| Adversarial debiasing | Deferred | Avoids compounding research risk; adds cleanly later (training script change, not architecture change) |
| VAEP usage | Auxiliary regression head (weight 0.1) | Enriches embeddings without contaminating primary task |
| Input features | action_type + start_x/y + end_x/y + result + time_delta + player_id | Richer than encoder; time_delta encodes tempo, end coords encode action destination |
| Prediction targets | Next action type (primary) + VAEP (auxiliary) | Validate core approach first; spatial/temporal heads deferred |
| Model scale | 256d / 6 layers / 8 heads (~11M params) | Scale B — sufficient capacity for player identity separation across 11,918 players |
| Max sequence length | 128 | Possessions rarely exceed 50 actions |
| HF Jobs flavor | a10g-large ($1.50/hr) | Fits ~11M param model comfortably |

## Architecture

### ScoutGPTConfig

Frozen dataclass. Extends the `Football2VecConfig` pattern.

| Field | Type | Default |
|-------|------|---------|
| `vocab_size` | int | 23 |
| `hidden_dim` | int | 256 |
| `num_layers` | int | 6 |
| `num_heads` | int | 8 |
| `dropout` | float | 0.1 |
| `max_seq_len` | int | 128 |
| `num_players` | int | 11_918 |
| `spatial_mlp_dim` | int | 64 |
| `vaep_loss_weight` | float | 0.1 |

### ScoutGPTDecoder

**Special tokens**: `PAD=23`, `BOS=24`. Expanded embedding vocab = 25.

**Embedding modules** (all producing `(batch, seq_len, 256)`):
- `token_embedding`: `nn.Embedding(25, 256)` — action types + PAD + BOS
- `player_embedding`: `nn.Embedding(11_918, 256)` — player conditioning table
- `start_x_mlp`, `start_y_mlp`, `end_x_mlp`, `end_y_mlp`: 4x `SpatialMLP(256, 64)`
- `result_embedding`: `nn.Embedding(2, 256)` — binary success/fail
- `time_delta_mlp`: `SpatialMLP(256, 64)` — inter-action seconds
- `position_embedding`: `nn.Embedding(128, 256)` — learnable positional

**Embedding formula** at position _t_:
```
emb_t = tok_emb(action_type_t) + start_x_mlp(start_x_t) + start_y_mlp(start_y_t)
      + end_x_mlp(end_x_t) + end_y_mlp(end_y_t) + result_emb(result_t)
      + time_delta_mlp(time_delta_t) + player_emb(actor_t) + pos_emb(t)
```

Position 0 (conditioning token):
```
emb_0 = tok_emb(BOS=24) + player_emb(focal_player) + pos_emb(0)
```
Spatial/result/time_delta inputs are zero at position 0.

**Transformer body**: `nn.TransformerEncoder` with `is_causal=True`. Six layers,
`TransformerEncoderLayer(d_model=256, nhead=8, dim_feedforward=1024, dropout=0.1,
activation="gelu", batch_first=True)`.

Using `nn.TransformerEncoder` + causal mask is the standard GPT pattern in PyTorch
(no separate encoder-decoder cross-attention needed).

**Prediction heads**:
- `action_head`: `nn.Linear(256, 23)` — next action type logits
- `vaep_head`: `nn.Linear(256, 1)` — VAEP regression

**Key methods**:
- `_embed(action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids) -> Tensor`
- `_encode(... + attention_mask) -> Tensor` — applies causal transformer, returns `(batch, seq, 256)`
- `forward(... + attention_mask) -> Tensor` — mean-pool over valid tokens → `(batch, 256)`. For future embedding extraction and adversarial stage.
- `predict(... + attention_mask) -> tuple[Tensor, Tensor]` — returns `(action_logits, vaep_preds)` at every position. Shape: `(batch, seq, 23)` and `(batch, seq, 1)`.

**Weight initialization**: Xavier uniform on Linear/Embedding, zeros on biases,
ones/zeros on LayerNorm. Same as existing encoder.

## Training Data Export

**Entry point**: `export_scoutgpt_training_data` → `ingestion.export_scoutgpt_training_data:main`

**Source**: `fct_action_values` (~9.5M actions), ordered by `(match_id, period, time_seconds)`.

### Possession Segmentation Rules

A new episode starts when any of these occur:
1. `team_id` changes from previous action
2. `period` changes
3. Set piece restart: action type in {`goalkick`, `throw_in`, `freekick_short`,
   `freekick_crossed`, `corner_short`, `corner_crossed`}
4. Time gap > 10 seconds since previous action

**Minimum episode length**: 3 actions (shorter episodes filtered out).

### Processing Pipeline

1. Query all actions from `fct_action_values`
2. Compute `time_delta` per action (seconds since previous action; 0.0 at episode start)
3. Apply segmentation rules → assign `episode_id`
4. Filter episodes with < 3 actions
5. Build contiguous player ID mapping: `canonical_player_id` → int (0–11,917). Save as `player_id_map.json`
6. Normalize coordinates: start_x/end_x ÷ 105.0, start_y/end_y ÷ 68.0
7. Map `action_type` string → int using `_ACTION_TYPE_IDS` (0–22)
8. Map `action_result` → int: `"success"` = 1, else 0
9. Log episode length histogram to MLflow (count, mean, median, p5, p25, p75, p95, max)

### Output Schema

One row per episode. Destination: HF Hub `luxury-lakehouse/scoutgpt-training-data`
+ UC Volume staging path.

| Column | Type |
|--------|------|
| `episode_id` | STRING (match_id + period + episode_seq) |
| `match_id` | STRING |
| `competition_id` | INT |
| `season_id` | INT |
| `team_id` | INT |
| `data_source` | STRING |
| `actions` | ARRAY\<STRUCT\<action_type: INT, start_x: FLOAT, start_y: FLOAT, end_x: FLOAT, end_y: FLOAT, result: INT, vaep_value: FLOAT, time_delta: FLOAT, player_idx: INT\>\> |

`player_idx` is the contiguous integer (0–11,917) from `player_id_map.json`, not the raw source `player_id`.

**Skip guard**: Compare upstream action count against existing Parquet row count.

### Workflow

`@workflow("wf-scoutgpt-export", phase="export")` on the `run_pipeline` function.
Databricks serverless runtime. `CostEstimateHook` for observability.

## Training Script

**Files**: `scripts/train_scoutgpt_hf.py` + `scripts/train_scoutgpt_hf_helpers.py`

**Runtime**: HF Jobs `a10g-large`, timeout 180m, PEP 723 dependencies.

### ScoutGPTDataset

- Loads episodes from Parquet + `player_id_map.json` from HF Hub
- Per sample: prepend player conditioning token at position 0 (`action_id=BOS(24)`,
  spatials=0, result=0, time_delta=0, player_id=focal_player)
- Focal player = player who performs action at position 1 (first predicted action)
- Per-action player IDs as input features at every position
- Labels: action_ids shifted right by 1 (input[0:n-1] → target[1:n])
- Label at position 0 = -100 (ignored, conditioning token)
- Padding to max_seq_len=128 with PAD(23), labels=-100 at padding
- Truncation at 128 tokens

### Training Loop

| Parameter | Value |
|-----------|-------|
| Optimizer | AdamW (lr=1e-4, weight_decay=0.01) |
| Scheduler | Cosine with linear warmup (10% of total steps) |
| Gradient clipping | max_norm=1.0 |
| Batch size | 256 |
| Epochs | 30 |
| Early stopping | Patience 5, monitors val_loss |
| Split | 80/10/10, stratified by competition_id |

**Per-batch loss**:
```
loss = cross_entropy(action_logits, action_labels, ignore_index=-100)
     + 0.1 * mse(vaep_preds[valid_mask], vaep_targets[valid_mask])
```

### Checkpointing

Best model saved as `safetensors` to HF Hub `luxury-lakehouse/scoutgpt`:
- `stage1/model.safetensors`
- `stage1/config.json` (config fields + player_id_map metadata)
- `metrics.json` (all evaluation results)

### Cost Tracking

`HFJobsCostRecorder` + `@workflow("wf-scoutgpt", phase="training")`.
Estimated $1.50–3.00 per training run.

## Evaluation

All metrics computed on the held-out test set after training completes. Results
logged to MLflow and saved in `metrics.json`.

### Metric 1 — Next-Action Accuracy

- Top-1 and top-5 accuracy, overall
- Stratified by episode length bucket: short (3–7), medium (8–15), long (16+)
- Baselines: most-frequent-action-per-position (naive), bigram frequency (action_t → action_t+1)

### Metric 2 — Counterfactual Ranking Correlation

- Sample ~1,000 test episodes
- At last observed position, swap in top-100 most active players (by training data action count)
- Per swapped player: compute P(actual_next_action | player, context) from decoder softmax
- Rank 100 players by this probability
- Plausibility score: player's real-world frequency of performing that action type
- Report mean Spearman ρ across 1,000 episodes
- Target: ρ > 0.3 encouraging, ρ > 0.5 strong

### Metric 3 — Cross-Source Validation

- Accuracy gap between StatsBomb-only and Wyscout-only test splits
- Small gap = generalizes. Large gap = signals need for adversarial debiasing (stage 2)

## Merge Phases

Each phase is independently mergeable with green CI. Structured to minimize conflict
surface with concurrent `feature/cycle4-gk-embeddings-viz` branch.

| Phase | Files | Depends on | Merge point |
|-------|-------|-----------|-------------|
| **A** | `scoutgpt_decoder.py`, `test_scoutgpt_decoder.py`, `wf-scoutgpt.yaml`, `wf-scoutgpt-export.yaml`, `pyproject.toml` (1 entry point) | Nothing | All new files except pyproject.toml (1-line addition) |
| **B** | `export_scoutgpt_training_data.py` | Phase A (workflow card ID) | New file only |
| **C** | `train_scoutgpt_hf.py`, `train_scoutgpt_hf_helpers.py` | Phase A (model class), Phase B (training data) | New files only |

## New Files

| File | Purpose |
|------|---------|
| `src/analytics/scoutgpt_decoder.py` | ScoutGPTConfig + ScoutGPTDecoder model |
| `src/ingestion/export_scoutgpt_training_data.py` | Possession segmentation + Parquet export |
| `scripts/train_scoutgpt_hf.py` | HF Jobs training entry point |
| `scripts/train_scoutgpt_hf_helpers.py` | Dataset, evaluation, data loading |
| `workflow-cards/wf-scoutgpt.yaml` | Training workflow card |
| `workflow-cards/wf-scoutgpt-export.yaml` | Export workflow card |
| `src/tests/test_scoutgpt_decoder.py` | Unit tests |

## Modified Files

| File | Change |
|------|--------|
| `pyproject.toml` | 1 new entry point (`export_scoutgpt_training_data`) |

## Out of Scope (D33)

- dbt mart `fct_player_embeddings_sequence`
- pgvector HNSW index (256d)
- Synced table for sequence embeddings
- Taipy Player Similarity model selector extension
- Counterfactual substitution UI
- Embedding extraction inference script
