# ScoutGPT Decoder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a player-conditioned GPT-style causal decoder over SPADL possession episodes, with training data export and HF Jobs training pipeline.

**Architecture:** Causal transformer decoder (256d, 6 layers, 8 heads) with per-action player embeddings, spatial MLPs (start/end x/y + time delta), result embedding, and VAEP auxiliary head. Training data is ~9.5M SPADL actions segmented into possession episodes. Trained on HF Jobs a10g-large.

**Tech Stack:** PyTorch, safetensors, HF Hub, MLflow, PySpark (export only), Pydantic (workflow cards)

**Spec:** `docs/superpowers/specs/2026-04-03-scoutgpt-decoder-design.md`

---

## Phase A — Model + Tests + Workflow Cards

All new files. Zero modifications to existing code except 2 lines in pyproject.toml. Independently mergeable.

### Task 1: ScoutGPTConfig and ScoutGPTDecoder — Tests

**Files:**
- Create: `src/tests/test_scoutgpt_decoder.py`

- [ ] **Step 1: Write config tests**

```python
"""Tests for ScoutGPT decoder architecture."""

from __future__ import annotations

import pytest

torch = pytest.importorskip("torch")

from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder  # noqa: E402


def _make_batch(
    batch_size: int = 4,
    seq_len: int = 30,
    vocab_size: int = 23,
    num_players: int = 100,
) -> tuple[torch.Tensor, ...]:
    """Create synthetic inputs for decoder testing."""
    g = torch.Generator().manual_seed(42)
    action_ids = torch.randint(0, vocab_size, (batch_size, seq_len), generator=g)
    start_x = torch.rand(batch_size, seq_len, generator=g)
    start_y = torch.rand(batch_size, seq_len, generator=g)
    end_x = torch.rand(batch_size, seq_len, generator=g)
    end_y = torch.rand(batch_size, seq_len, generator=g)
    result = torch.randint(0, 2, (batch_size, seq_len), generator=g)
    time_delta = torch.rand(batch_size, seq_len, generator=g) * 10.0
    player_ids = torch.randint(0, num_players, (batch_size, seq_len), generator=g)
    attention_mask = torch.ones(batch_size, seq_len, dtype=torch.bool)
    return action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids, attention_mask


class TestScoutGPTConfig:
    def test_default_config(self) -> None:
        cfg = ScoutGPTConfig()
        assert cfg.vocab_size == 23
        assert cfg.hidden_dim == 256
        assert cfg.num_layers == 6
        assert cfg.num_heads == 8
        assert cfg.dropout == 0.1
        assert cfg.max_seq_len == 128
        assert cfg.num_players == 11_918
        assert cfg.spatial_mlp_dim == 64
        assert cfg.vaep_loss_weight == 0.1

    def test_custom_config(self) -> None:
        cfg = ScoutGPTConfig(
            vocab_size=10,
            hidden_dim=64,
            num_layers=2,
            num_heads=4,
            dropout=0.2,
            max_seq_len=64,
            num_players=500,
            spatial_mlp_dim=32,
            vaep_loss_weight=0.05,
        )
        assert cfg.vocab_size == 10
        assert cfg.hidden_dim == 64
        assert cfg.num_layers == 2
        assert cfg.num_heads == 4
        assert cfg.dropout == 0.2
        assert cfg.max_seq_len == 64
        assert cfg.num_players == 500
        assert cfg.spatial_mlp_dim == 32
        assert cfg.vaep_loss_weight == 0.05

    def test_frozen(self) -> None:
        cfg = ScoutGPTConfig()
        with pytest.raises(AttributeError):
            cfg.hidden_dim = 64  # type: ignore[misc]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_scoutgpt_decoder.py::TestScoutGPTConfig -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'analytics.scoutgpt_decoder'`

### Task 2: ScoutGPTConfig and ScoutGPTDecoder — Implementation

**Files:**
- Create: `src/analytics/scoutgpt_decoder.py`

- [ ] **Step 1: Implement ScoutGPTConfig and ScoutGPTDecoder**

```python
"""ScoutGPT: Player-conditioned causal decoder over SPADL possession episodes.

Architecture follows Hong et al. (2025), arXiv:2512.17266 — a GPT-style transformer
with player ID conditioning for counterfactual substitution. Per-action player
attribution and VAEP auxiliary regression head.

Reuses SpatialMLP from football2vec_transformer for coordinate encoding.
"""

from __future__ import annotations

from dataclasses import dataclass

import torch
import torch.nn as nn

from analytics.football2vec_transformer import SpatialMLP

# Special tokens — action vocab is 0–22 (23 types)
PAD_TOKEN_ID = 23
BOS_TOKEN_ID = 24
EXPANDED_VOCAB_SIZE = 25  # 23 actions + PAD + BOS


@dataclass(frozen=True)
class ScoutGPTConfig:
    """Configuration for the ScoutGPT decoder."""

    vocab_size: int = 23
    hidden_dim: int = 256
    num_layers: int = 6
    num_heads: int = 8
    dropout: float = 0.1
    max_seq_len: int = 128
    num_players: int = 11_918
    spatial_mlp_dim: int = 64
    vaep_loss_weight: float = 0.1


class ScoutGPTDecoder(nn.Module):
    """Player-conditioned GPT-style causal decoder.

    Input features per token: action_type, start_x, start_y, end_x, end_y,
    action_result, time_delta, player_id. Position 0 is the focal player
    conditioning token (BOS + player embedding).

    Prediction heads:
      - action_head: next action type (23-class, cross-entropy)
      - vaep_head: current action VAEP (regression, MSE, weight config.vaep_loss_weight)
    """

    def __init__(self, config: ScoutGPTConfig | None = None) -> None:
        super().__init__()
        self.config = config or ScoutGPTConfig()
        c = self.config
        hd = c.hidden_dim

        # Token and player embeddings
        self.token_embedding = nn.Embedding(EXPANDED_VOCAB_SIZE, hd)
        self.player_embedding = nn.Embedding(c.num_players, hd)

        # Spatial encoders (4 for coordinates + 1 for time delta)
        self.start_x_mlp = SpatialMLP(hd, c.spatial_mlp_dim)
        self.start_y_mlp = SpatialMLP(hd, c.spatial_mlp_dim)
        self.end_x_mlp = SpatialMLP(hd, c.spatial_mlp_dim)
        self.end_y_mlp = SpatialMLP(hd, c.spatial_mlp_dim)
        self.time_delta_mlp = SpatialMLP(hd, c.spatial_mlp_dim)

        # Result embedding (binary: 0=fail, 1=success)
        self.result_embedding = nn.Embedding(2, hd)

        # Positional embedding
        self.position_embedding = nn.Embedding(c.max_seq_len, hd)

        self.embedding_dropout = nn.Dropout(c.dropout)

        # Causal transformer (nn.TransformerEncoder + is_causal = GPT pattern)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=hd,
            nhead=c.num_heads,
            dim_feedforward=hd * 4,
            dropout=c.dropout,
            activation="gelu",
            batch_first=True,
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=c.num_layers)

        # Prediction heads
        self.action_head = nn.Linear(hd, c.vocab_size)
        self.vaep_head = nn.Linear(hd, 1)

        self.apply(self._init_weights)

    @staticmethod
    def _init_weights(module: nn.Module) -> None:
        """Xavier uniform initialization for linear layers and embeddings."""
        if isinstance(module, nn.Linear):
            nn.init.xavier_uniform_(module.weight)
            if module.bias is not None:
                nn.init.zeros_(module.bias)
        elif isinstance(module, nn.Embedding):
            nn.init.xavier_uniform_(module.weight)
        elif isinstance(module, nn.LayerNorm):
            nn.init.ones_(module.weight)
            nn.init.zeros_(module.bias)

    def _embed(
        self,
        action_ids: torch.Tensor,
        start_x: torch.Tensor,
        start_y: torch.Tensor,
        end_x: torch.Tensor,
        end_y: torch.Tensor,
        result: torch.Tensor,
        time_delta: torch.Tensor,
        player_ids: torch.Tensor,
    ) -> torch.Tensor:
        """Compute input embeddings.

        All inputs are (batch, seq_len). Returns (batch, seq_len, hidden_dim).
        """
        seq_len = action_ids.size(1)
        positions = torch.arange(seq_len, device=action_ids.device).unsqueeze(0)

        emb = (
            self.token_embedding(action_ids)
            + self.start_x_mlp(start_x)
            + self.start_y_mlp(start_y)
            + self.end_x_mlp(end_x)
            + self.end_y_mlp(end_y)
            + self.result_embedding(result)
            + self.time_delta_mlp(time_delta)
            + self.player_embedding(player_ids)
            + self.position_embedding(positions)
        )
        return self.embedding_dropout(emb)

    def _encode(
        self,
        action_ids: torch.Tensor,
        start_x: torch.Tensor,
        start_y: torch.Tensor,
        end_x: torch.Tensor,
        end_y: torch.Tensor,
        result: torch.Tensor,
        time_delta: torch.Tensor,
        player_ids: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Run causal transformer. Returns (batch, seq_len, hidden_dim)."""
        emb = self._embed(action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids)

        # Padding mask: TransformerEncoder uses True = ignore
        src_key_padding_mask: torch.Tensor | None = None
        if attention_mask is not None:
            src_key_padding_mask = ~attention_mask

        return self.transformer(emb, is_causal=True, src_key_padding_mask=src_key_padding_mask)

    def forward(
        self,
        action_ids: torch.Tensor,
        start_x: torch.Tensor,
        start_y: torch.Tensor,
        end_x: torch.Tensor,
        end_y: torch.Tensor,
        result: torch.Tensor,
        time_delta: torch.Tensor,
        player_ids: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Mean-pooled sequence representation. Returns (batch, hidden_dim).

        Used for embedding extraction and future adversarial debiasing stage.
        """
        hidden = self._encode(
            action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids, attention_mask
        )
        if attention_mask is not None:
            mask_expanded = attention_mask.unsqueeze(-1).float()
            hidden = hidden * mask_expanded
            return hidden.sum(dim=1) / mask_expanded.sum(dim=1).clamp(min=1)
        return hidden.mean(dim=1)

    def predict(
        self,
        action_ids: torch.Tensor,
        start_x: torch.Tensor,
        start_y: torch.Tensor,
        end_x: torch.Tensor,
        end_y: torch.Tensor,
        result: torch.Tensor,
        time_delta: torch.Tensor,
        player_ids: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        """Per-position predictions. Returns (action_logits, vaep_preds).

        action_logits: (batch, seq_len, vocab_size)
        vaep_preds: (batch, seq_len, 1)
        """
        hidden = self._encode(
            action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids, attention_mask
        )
        return self.action_head(hidden), self.vaep_head(hidden)
```

- [ ] **Step 2: Run config tests to verify they pass**

Run: `uv run pytest src/tests/test_scoutgpt_decoder.py::TestScoutGPTConfig -v`
Expected: 3 passed

### Task 3: ScoutGPTDecoder Shape and Behavioral Tests

**Files:**
- Modify: `src/tests/test_scoutgpt_decoder.py`

- [ ] **Step 1: Add shape and behavioral test classes**

Append to `src/tests/test_scoutgpt_decoder.py`:

```python
class TestScoutGPTDecoder:
    def test_forward_pass_shape(self) -> None:
        cfg = ScoutGPTConfig(num_players=100, hidden_dim=64, num_layers=2, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, mask = _make_batch(num_players=100)
        with torch.no_grad():
            out = model(action_ids, sx, sy, ex, ey, result, td, pids, mask)
        assert out.shape == (4, 64)

    def test_predict_shape(self) -> None:
        cfg = ScoutGPTConfig(num_players=100, hidden_dim=64, num_layers=2, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, mask = _make_batch(num_players=100)
        with torch.no_grad():
            action_logits, vaep_preds = model.predict(
                action_ids, sx, sy, ex, ey, result, td, pids, mask
            )
        assert action_logits.shape == (4, 30, 23)
        assert vaep_preds.shape == (4, 30, 1)

    def test_forward_without_attention_mask(self) -> None:
        cfg = ScoutGPTConfig(num_players=100, hidden_dim=64, num_layers=2, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, _ = _make_batch(num_players=100)
        with torch.no_grad():
            out = model(action_ids, sx, sy, ex, ey, result, td, pids)
        assert out.shape == (4, 64)

    def test_default_config_when_none(self) -> None:
        model = ScoutGPTDecoder(config=None)
        assert model.config.hidden_dim == 256
        assert model.config.num_players == 11_918

    def test_custom_config_dimensions(self) -> None:
        cfg = ScoutGPTConfig(num_players=50, hidden_dim=32, num_layers=1, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, mask = _make_batch(
            batch_size=2, seq_len=10, num_players=50
        )
        with torch.no_grad():
            out = model(action_ids, sx, sy, ex, ey, result, td, pids, mask)
        assert out.shape == (2, 32)

    def test_spatial_encoding_contributes(self) -> None:
        cfg = ScoutGPTConfig(num_players=100, hidden_dim=64, num_layers=2, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, mask = _make_batch(num_players=100)
        sx2 = torch.ones_like(sx) * 0.9
        sy2 = torch.ones_like(sy) * 0.1
        with torch.no_grad():
            out_a = model(action_ids, sx, sy, ex, ey, result, td, pids, mask)
            out_b = model(action_ids, sx2, sy2, ex, ey, result, td, pids, mask)
        assert not torch.allclose(out_a, out_b, atol=1e-6)

    def test_player_conditioning_contributes(self) -> None:
        cfg = ScoutGPTConfig(num_players=100, hidden_dim=64, num_layers=2, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, mask = _make_batch(num_players=100)
        pids2 = torch.zeros_like(pids)  # All player 0
        with torch.no_grad():
            out_a = model(action_ids, sx, sy, ex, ey, result, td, pids, mask)
            out_b = model(action_ids, sx, sy, ex, ey, result, td, pids2, mask)
        assert not torch.allclose(out_a, out_b, atol=1e-6)

    def test_attention_mask_affects_output(self) -> None:
        cfg = ScoutGPTConfig(num_players=100, hidden_dim=64, num_layers=2, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, mask = _make_batch(num_players=100)
        partial_mask = mask.clone()
        partial_mask[:, 15:] = False
        with torch.no_grad():
            out_full = model(action_ids, sx, sy, ex, ey, result, td, pids, mask)
            out_partial = model(action_ids, sx, sy, ex, ey, result, td, pids, partial_mask)
        assert not torch.allclose(out_full, out_partial, atol=1e-6)

    def test_predict_without_mask(self) -> None:
        cfg = ScoutGPTConfig(num_players=100, hidden_dim=64, num_layers=2, num_heads=4)
        model = ScoutGPTDecoder(cfg)
        model.eval()
        action_ids, sx, sy, ex, ey, result, td, pids, _ = _make_batch(num_players=100)
        with torch.no_grad():
            logits, vaep = model.predict(action_ids, sx, sy, ex, ey, result, td, pids)
        assert logits.shape == (4, 30, 23)
        assert vaep.shape == (4, 30, 1)
```

- [ ] **Step 2: Run all tests**

Run: `uv run pytest src/tests/test_scoutgpt_decoder.py -v`
Expected: 12 passed (3 config + 9 decoder)

### Task 4: Workflow Cards

**Files:**
- Create: `workflow-cards/wf-scoutgpt.yaml`
- Create: `workflow-cards/wf-scoutgpt-export.yaml`

- [ ] **Step 1: Create training workflow card**

```yaml
---
name: ScoutGPT — Player-Conditioned Decoder
id: wf-scoutgpt
version: "1.0"
status: development
type: training
domain: player-embeddings
owners:
  - karsten
tags:
  - embeddings
  - transformer
  - decoder
  - counterfactual
  - player-similarity

references:
  - citation: "Hong et al. (2025). ScoutGPT: Player-conditioned Football Language Model for Counterfactual Evaluation. arXiv:2512.17266."
    role: methodology
  - citation: "Decroos et al. (2019). Actions Speak Louder than Goals. KDD."
    role: algorithm

inputs:
  datasets:
    - id: "luxury-lakehouse/scoutgpt-training-data"
      source: huggingface
      description: "SPADL possession episodes with per-action player attribution"

outputs:
  models:
    - id: "luxury-lakehouse/scoutgpt"
      destination: huggingface

execution:
  training:
    trigger: manual
    runtime: hf-jobs
    flavor: a10g-large
    script: "scripts/train_scoutgpt_hf.py"
    timeout: "180m"

depends_on:
  - wf-scoutgpt-export

idempotency:
  strategy: full-overwrite
  key: model_version
  description: "Each training run produces a new model version. Weights are overwritten on HF Hub."

cost:
  training:
    runtime: hf-jobs
    flavor: a10g-large
    rate_usd_per_hour: 1.50
    typical_duration_minutes: 120
    typical_cost_usd: 3.00

monitoring:
  freshness_sla_hours: 336
  metrics:
    - name: "next_action_top1_accuracy"
      threshold_min: 0.20
    - name: "counterfactual_spearman_rho"
      threshold_min: 0.15

links:
  source_code:
    - "scripts/train_scoutgpt_hf.py"
    - "scripts/train_scoutgpt_hf_helpers.py"
    - "src/analytics/scoutgpt_decoder.py"
---

## Overview

ScoutGPT is a player-conditioned GPT-style causal decoder trained on SPADL possession
episodes. Architecture: 256d hidden, 6 transformer layers, 8 attention heads (~11M params).
Per-action player attribution via player embedding table (11,918 players). VAEP auxiliary
regression head enriches learned representations.

Follows Hong et al. (2025) — player ID as conditioning token enables counterfactual
substitution: swap the focal player ID to predict "what would Player X do here?"

## Architecture

- Token embedding: 23 SPADL action types + PAD + BOS → 256d
- Player embedding: 11,918 players → 256d (per-action attribution + position-0 conditioning)
- Spatial encoding: 4x SpatialMLP (start_x/y, end_x/y) + 1x SpatialMLP (time_delta) → 256d each
- Result embedding: binary success/fail → 256d
- 6-layer causal transformer, 8 heads, GELU activation
- Mean pooling over valid tokens → 256d player embedding
- Primary head: next action type (23-class cross-entropy)
- Auxiliary head: VAEP regression (MSE, weight 0.1)

## Evaluation

- Next-action accuracy (top-1, top-5) stratified by episode length
- Counterfactual ranking correlation (Spearman rho over 1K episodes, 100 player swaps)
- Cross-source validation gap (StatsBomb vs Wyscout)
```

- [ ] **Step 2: Create export workflow card**

```yaml
---
name: ScoutGPT — Training Data Export
id: wf-scoutgpt-export
version: "1.0"
status: development
type: data-movement
domain: player-embeddings
owners:
  - karsten
tags:
  - embeddings
  - training-data
  - spadl
  - huggingface
  - possession-episodes

references:
  - citation: "Decroos et al. (2019). Actions Speak Louder than Goals. KDD."
    role: methodology

inputs:
  tables:
    - id: "{catalog}.{schema}.fct_action_values"
      source: delta-table
      description: "SPADL action sequences with VAEP values"
    - id: "{catalog}.{schema}.dim_players"
      source: delta-table
      description: "Player dimension for canonical_player_id mapping"

outputs:
  datasets:
    - id: "luxury-lakehouse/scoutgpt-training-data"
      destination: huggingface
      description: "SPADL possession episodes with per-action player attribution (Parquet)"

execution:
  export:
    trigger: manual
    runtime: databricks-workflow
    entry_point: export_scoutgpt_training_data
    module: ingestion.export_scoutgpt_training_data
    distribution: driver-bound
    timeout: "900s"
    environment: analytics

depends_on:
  - wf-vaep
  - wf-entity-resolution

idempotency:
  strategy: full-overwrite
  key: episode_id
  description: "Exports all possession episodes each run; HF Hub dataset is replaced in full."

cost:
  export:
    runtime: databricks
    sku: "jobs_serverless_compute_run_dbus"
    typical_dbu: 30
    typical_cost_usd: 2.10

monitoring:
  freshness_sla_hours: 336

links:
  source_code:
    - "src/ingestion/export_scoutgpt_training_data.py"
---

## Overview

Reads SPADL actions from `fct_action_values`, segments them into possession episodes
using team-change, period-boundary, set-piece-restart, and time-gap rules. Joins
`dim_players` for canonical player ID mapping. Serializes episodes as struct arrays
in Parquet and publishes to HF Hub as `luxury-lakehouse/scoutgpt-training-data`.

Possession segmentation rules:
1. Team change → new episode
2. Period boundary → new episode
3. Set piece restart (goalkick, throw_in, freekick_short/crossed, corner_short/crossed) → new episode
4. Time gap > 10 seconds → new episode

Minimum episode length: 3 actions.

## Output Schema

One row per possession episode:

| Column | Type | Description |
|--------|------|-------------|
| `episode_id` | string | Surrogate: match_id + period + episode_seq |
| `match_id` | string | Match identifier |
| `competition_id` | int | Competition identifier |
| `season_id` | int | Season identifier |
| `team_id` | int | Possessing team |
| `data_source` | string | statsbomb or wyscout |
| `actions` | array<struct> | Per-action: action_type (0–22), start_x/y, end_x/y (0–1), result (0/1), vaep_value, time_delta, player_idx (0–11917) |
```

- [ ] **Step 3: Validate workflow cards**

Run: `uv run validate_workflow_cards --validate workflow-cards/`
Expected: `wf-scoutgpt.yaml: OK` and `wf-scoutgpt-export.yaml: OK` (plus all existing cards OK)

### Task 5: Entry Points

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add 2 new entry points**

Add after the `prepare_360_training_data` line in `[project.scripts]`:

```toml
export_scoutgpt_training_data = "ingestion.export_scoutgpt_training_data:main"
train_scoutgpt = "ingestion.export_scoutgpt_training_data:train_stub"
```

The existing pattern does NOT have entry points for HF Jobs training scripts — `train_football2vec_v2` has no entry point in pyproject.toml. Only the export pipeline needs one:

```toml
export_scoutgpt_training_data = "ingestion.export_scoutgpt_training_data:main"
```

- [ ] **Step 2: Verify pyproject.toml is valid**

Run: `uv run python -c "import tomllib; tomllib.load(open('pyproject.toml', 'rb')); print('OK')"`
Expected: `OK`

### Task 6: Phase A Lint and Test

- [ ] **Step 1: Run linter on new files**

Run: `uv run ruff check src/analytics/scoutgpt_decoder.py src/tests/test_scoutgpt_decoder.py`
Expected: No errors (fix any that appear)

- [ ] **Step 2: Run formatter check**

Run: `uv run ruff format --check src/analytics/scoutgpt_decoder.py src/tests/test_scoutgpt_decoder.py`
Expected: Files already formatted (fix any that aren't)

- [ ] **Step 3: Run pyright on new module**

Run: `uv run pyright src/analytics/scoutgpt_decoder.py`
Expected: 0 errors

- [ ] **Step 4: Run full test suite for decoder**

Run: `uv run pytest src/tests/test_scoutgpt_decoder.py -v`
Expected: 12 passed

- [ ] **Step 5: Validate all workflow cards**

Run: `uv run validate_workflow_cards --validate workflow-cards/`
Expected: All cards OK

- [ ] **Step 6: Phase A commit checkpoint**

At this point all Phase A files are complete and green. This is a potential merge point — ask user if they want to commit/merge or continue.

---

## Phase B — Training Data Export

New file only. Depends on Phase A for workflow card ID.

### Task 7: Export Pipeline — Possession Segmentation

**Files:**
- Create: `src/ingestion/export_scoutgpt_training_data.py`

- [ ] **Step 1: Implement the export pipeline**

```python
"""Export SPADL possession episodes for ScoutGPT decoder training.

Reads fct_action_values, segments into possession episodes using team-change,
period-boundary, set-piece-restart, and time-gap rules. Publishes Parquet to
HF Hub as luxury-lakehouse/scoutgpt-training-data.

Episode segmentation rules:
  1. team_id changes → new episode
  2. period changes → new episode
  3. Set piece restart (goalkick, throw_in, freekick_*, corner_*) → new episode
  4. time_delta > 10s → new episode

Minimum episode length: 3 actions.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from ingestion.utils import (
    parse_ingestion_args,
    upload_volume_to_hf_hub,
    validate_dataframe,
    write_delta_table,
)
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

_HF_DATASET_REPO = "luxury-lakehouse/scoutgpt-training-data"
_UC_VOLUME_SUBPATH = "training_data/scoutgpt"

# SPADL action type → integer mapping (canonical ordering, matches socceraction)
_ACTION_TYPE_IDS: dict[str, int] = {
    "pass": 0, "cross": 1, "throw_in": 2, "freekick_crossed": 3,
    "freekick_short": 4, "corner_crossed": 5, "corner_short": 6,
    "take_on": 7, "foul": 8, "tackle": 9, "interception": 10,
    "shot": 11, "shot_penalty": 12, "shot_freekick": 13,
    "keeper_save": 14, "keeper_claim": 15, "keeper_punch": 16,
    "keeper_pick_up": 17, "clearance": 18, "bad_touch": 19,
    "non_action": 20, "dribble": 21, "goalkick": 22,
}

_PITCH_LENGTH = 105.0
_PITCH_WIDTH = 68.0

# Set piece restart types that start a new possession episode
_SET_PIECE_TYPES = frozenset({
    "goalkick", "throw_in", "freekick_short", "freekick_crossed",
    "corner_short", "corner_crossed",
})

_TIME_GAP_THRESHOLD = 10.0  # seconds

_MIN_EPISODE_LENGTH = 3


def _build_player_id_map(spark: SparkSession, catalog: str, schema: str) -> dict[str, int]:
    """Build contiguous player_idx mapping from dim_players."""
    rows = (
        spark.table(f"{catalog}.{schema}.dim_players")
        .select("canonical_player_id")
        .distinct()
        .orderBy("canonical_player_id")
        .collect()
    )
    return {row["canonical_player_id"]: idx for idx, row in enumerate(rows)}


def _segment_possessions(df: DataFrame) -> DataFrame:
    """Add episode boundary markers and episode_id to action DataFrame.

    Input must be ordered by (match_id, period, time_seconds).
    """
    w = Window.partitionBy("match_id").orderBy("period", "time_seconds")

    # Compute boundary signals
    prev_team = F.lag("team_id").over(w)
    prev_period = F.lag("period").over(w)
    prev_time = F.lag("time_seconds").over(w)

    # Create mapping expression for set piece detection
    set_piece_types_list = list(_SET_PIECE_TYPES)
    is_set_piece = F.col("action_type").isin(set_piece_types_list)

    df = df.withColumn("_prev_team", prev_team)
    df = df.withColumn("_prev_period", prev_period)
    df = df.withColumn("_prev_time", prev_time)
    df = df.withColumn("_time_delta_raw", F.col("time_seconds") - F.col("_prev_time"))

    # Episode boundary: any of the 4 rules
    boundary = (
        F.col("_prev_team").isNull()  # first action in match
        | (F.col("team_id") != F.col("_prev_team"))  # team change
        | (F.col("period") != F.col("_prev_period"))  # period boundary
        | is_set_piece  # set piece restart
        | (F.col("_time_delta_raw") > F.lit(_TIME_GAP_THRESHOLD))  # time gap
    )

    df = df.withColumn("_is_boundary", boundary.cast("int"))

    # Cumulative sum of boundaries = episode sequence number within match
    w_match = Window.partitionBy("match_id").orderBy("period", "time_seconds")
    df = df.withColumn("_episode_seq", F.sum("_is_boundary").over(w_match))

    # Episode ID = match_id + period + episode_seq (for grouping)
    df = df.withColumn(
        "episode_id",
        F.concat_ws("_", F.col("match_id"), F.col("period").cast("string"), F.col("_episode_seq").cast("string")),
    )

    # Time delta within episode (0.0 at episode start)
    w_episode = Window.partitionBy("episode_id").orderBy("period", "time_seconds")
    prev_time_ep = F.lag("time_seconds").over(w_episode)
    df = df.withColumn(
        "time_delta",
        F.when(prev_time_ep.isNull(), F.lit(0.0)).otherwise(F.col("time_seconds") - prev_time_ep),
    )

    return df


def _export_episodes(
    spark: SparkSession,
    catalog: str,
    schema: str,
    export_logger: logging.Logger,
) -> int:
    """Main export logic. Returns episode count."""
    # Step 1: Load actions
    export_logger.info("Loading actions from fct_action_values...")
    actions_df = (
        spark.table(f"{catalog}.{schema}.fct_action_values")
        .select(
            "match_id", "player_id", "team_id", "competition_id", "season_id",
            "period", "time_seconds", "start_x", "start_y", "end_x", "end_y",
            "action_type", "action_result", "vaep_value", "data_source",
        )
        .orderBy("match_id", "period", "time_seconds")
    )

    total_actions = actions_df.count()
    export_logger.info("Total actions: %d", total_actions)

    # Step 2: Build player ID map
    export_logger.info("Building player ID map...")
    player_id_map = _build_player_id_map(spark, catalog, schema)
    export_logger.info("Player ID map: %d players", len(player_id_map))

    # Step 3: Join dim_players for canonical_player_id
    dim_players = spark.table(f"{catalog}.{schema}.dim_players").select(
        F.col("player_id").alias("_dp_player_id"),
        "canonical_player_id",
    )
    actions_df = actions_df.join(dim_players, actions_df["player_id"] == dim_players["_dp_player_id"], "left")

    # Step 4: Segment possessions
    export_logger.info("Segmenting possession episodes...")
    episodes_df = _segment_possessions(actions_df)

    # Step 5: Map action types to integers
    action_type_map_expr = F.create_map([F.lit(x) for kv in _ACTION_TYPE_IDS.items() for x in kv])
    episodes_df = episodes_df.withColumn(
        "action_type_id",
        F.coalesce(action_type_map_expr[F.col("action_type")], F.lit(_ACTION_TYPE_IDS["non_action"])),
    )

    # Step 6: Normalize coordinates and map result
    episodes_df = (
        episodes_df
        .withColumn("start_x_norm", F.col("start_x") / F.lit(_PITCH_LENGTH))
        .withColumn("start_y_norm", F.col("start_y") / F.lit(_PITCH_WIDTH))
        .withColumn("end_x_norm", F.col("end_x") / F.lit(_PITCH_LENGTH))
        .withColumn("end_y_norm", F.col("end_y") / F.lit(_PITCH_WIDTH))
        .withColumn("result_int", F.when(F.col("action_result") == "success", 1).otherwise(0))
    )

    # Step 7: Map player IDs to contiguous indices via broadcast
    player_map_broadcast = spark.sparkContext.broadcast(player_id_map)

    @F.udf(T.IntegerType())
    def _map_player_idx(canonical_pid: str | None) -> int:
        if canonical_pid is None:
            return 0
        return player_map_broadcast.value.get(canonical_pid, 0)

    episodes_df = episodes_df.withColumn("player_idx", _map_player_idx(F.col("canonical_player_id")))

    # Step 8: Build action struct and aggregate per episode
    action_struct = F.struct(
        F.col("action_type_id").alias("action_type"),
        F.col("start_x_norm").alias("start_x"),
        F.col("start_y_norm").alias("start_y"),
        F.col("end_x_norm").alias("end_x"),
        F.col("end_y_norm").alias("end_y"),
        F.col("result_int").alias("result"),
        F.col("vaep_value"),
        F.col("time_delta"),
        F.col("player_idx"),
    )

    # Sort struct for ordering within episode
    sort_struct = F.struct(
        F.col("period"), F.col("time_seconds"), action_struct.alias("action"),
    )

    grouped = (
        episodes_df
        .groupBy("episode_id", "match_id", "competition_id", "season_id", "team_id", "data_source")
        .agg(
            F.sort_array(F.collect_list(sort_struct)).alias("_sorted"),
            F.count("*").alias("_episode_len"),
        )
        .filter(F.col("_episode_len") >= _MIN_EPISODE_LENGTH)
    )

    # Extract action structs from sorted array
    grouped = grouped.withColumn(
        "actions",
        F.transform(F.col("_sorted"), lambda x: x["action"]),
    ).drop("_sorted", "_episode_len")

    # Step 9: Write to UC Volume
    output_path = f"/Volumes/{catalog}/{schema}/training_data/scoutgpt"
    export_logger.info("Writing episodes to %s...", output_path)
    episode_count = grouped.count()
    export_logger.info("Total episodes (>= %d actions): %d", _MIN_EPISODE_LENGTH, episode_count)

    grouped.write.mode("overwrite").parquet(output_path)

    # Step 10: Save player ID map alongside Parquet
    player_map_path = f"{output_path}/player_id_map.json"
    player_map_json = json.dumps(player_id_map, indent=2)
    # Write via dbutils or spark — use a single-row DataFrame trick
    spark.createDataFrame([(player_map_json,)], ["json_content"]).write.mode("overwrite").text(
        f"{output_path}/_player_id_map"
    )

    # Step 11: Log episode length statistics
    from pyspark.sql.functions import col, expr, percentile_approx

    len_stats = (
        episodes_df
        .groupBy("episode_id")
        .agg(F.count("*").alias("ep_len"))
        .filter(F.col("ep_len") >= _MIN_EPISODE_LENGTH)
        .agg(
            F.count("*").alias("count"),
            F.mean("ep_len").alias("mean"),
            F.expr("percentile(ep_len, 0.5)").alias("median"),
            F.expr("percentile(ep_len, 0.05)").alias("p5"),
            F.expr("percentile(ep_len, 0.25)").alias("p25"),
            F.expr("percentile(ep_len, 0.75)").alias("p75"),
            F.expr("percentile(ep_len, 0.95)").alias("p95"),
            F.max("ep_len").alias("max"),
        )
        .collect()[0]
    )
    export_logger.info(
        "Episode length stats — count: %d, mean: %.1f, median: %.1f, "
        "p5: %.1f, p25: %.1f, p75: %.1f, p95: %.1f, max: %d",
        len_stats["count"], len_stats["mean"], len_stats["median"],
        len_stats["p5"], len_stats["p25"], len_stats["p75"], len_stats["p95"], len_stats["max"],
    )

    # Step 12: Upload to HF Hub
    export_logger.info("Uploading to HF Hub: %s", _HF_DATASET_REPO)
    upload_volume_to_hf_hub(output_path, _HF_DATASET_REPO, export_logger)

    return episode_count


@workflow("wf-scoutgpt-export", phase="export")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    pipeline_logger: logging.Logger,
    *,
    ctx: object | None = None,
) -> int:
    """Workflow-decorated entry point for Databricks job."""
    return _export_episodes(spark, catalog, schema, pipeline_logger)


def main() -> None:
    """CLI entry point."""
    args = parse_ingestion_args()
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    run_pipeline(spark, args.catalog, args.schema, logger)
```

- [ ] **Step 2: Run linter**

Run: `uv run ruff check src/ingestion/export_scoutgpt_training_data.py`
Expected: No errors

- [ ] **Step 3: Run pyright**

Run: `uv run pyright src/ingestion/export_scoutgpt_training_data.py`
Expected: 0 errors (or acceptable PySpark-related type stubs warnings)

- [ ] **Step 4: Phase B commit checkpoint**

Phase B is complete. Ask user about commit/merge or continue.

---

## Phase C — Training Script + Helpers

New files only. Depends on Phase A (model class) and Phase B (training data).

### Task 8: Training Helpers — Dataset and Utilities

**Files:**
- Create: `scripts/train_scoutgpt_hf_helpers.py`

- [ ] **Step 1: Implement helpers**

```python
"""ScoutGPT training helpers — dataset, data loading, evaluation, scheduling.

Companion to train_scoutgpt_hf.py. Follows the established pattern from
train_football2vec_v2_helpers.py.
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any

import numpy as np
import pandas as pd
import torch
from scipy.stats import spearmanr
from torch.utils.data import Dataset

logger = logging.getLogger(__name__)

# Vocabulary constants (must match scoutgpt_decoder.py)
VOCAB_SIZE = 23
PAD_TOKEN_ID = 23
BOS_TOKEN_ID = 24
MAX_SEQ_LEN = 128

# Training constants
WEIGHT_DECAY = 0.01
WARMUP_FRACTION = 0.10
RANDOM_STATE = 42
DEFAULT_EPOCHS = 30
DEFAULT_BATCH_SIZE = 256
DEFAULT_LR = 1e-4
DEFAULT_PATIENCE = 5
VAEP_LOSS_WEIGHT = 0.1

# Evaluation constants
COUNTERFACTUAL_NUM_EPISODES = 1000
COUNTERFACTUAL_NUM_PLAYERS = 100


def load_training_data(hf_token: str, dataset_repo: str) -> tuple[pd.DataFrame, dict[str, int], str]:
    """Load episodes and player ID map from HF Hub.

    Returns (episodes_df, player_id_map, dataset_sha).
    """
    import pyarrow.parquet as pq
    from huggingface_hub import HfApi, hf_hub_download

    api = HfApi(token=hf_token)
    all_items = list(api.list_repo_tree(dataset_repo, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    if not parquet_files:
        msg = f"No parquet files found in {dataset_repo}"
        raise RuntimeError(msg)

    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local_path = hf_hub_download(dataset_repo, pf, repo_type="dataset", token=hf_token)
        table = pq.read_table(local_path)
        dfs.append(table.to_pandas())
        logger.info("  %s: %d rows", pf, len(dfs[-1]))

    data = pd.concat(dfs, ignore_index=True)
    logger.info("Total episodes: %d", len(data))

    # Load player ID map
    player_map_files = [f.path for f in all_items if hasattr(f, "size") and "player_id_map" in f.path]
    if not player_map_files:
        msg = "No player_id_map found in dataset"
        raise RuntimeError(msg)

    map_path = hf_hub_download(dataset_repo, player_map_files[0], repo_type="dataset", token=hf_token)
    with open(map_path, encoding="utf-8") as f:
        player_id_map: dict[str, int] = json.load(f)
    logger.info("Player ID map: %d players", len(player_id_map))

    dataset_info = api.repo_info(repo_id=dataset_repo, repo_type="dataset")
    return data, player_id_map, dataset_info.sha


def parse_episode_actions(
    actions_list: list[dict[str, Any]],
) -> tuple[list[int], list[float], list[float], list[float], list[float], list[int], list[float], list[float], list[int]]:
    """Parse a single episode's action struct array.

    Returns (action_types, start_xs, start_ys, end_xs, end_ys, results, vaep_values, time_deltas, player_idxs).
    """
    action_types: list[int] = []
    start_xs: list[float] = []
    start_ys: list[float] = []
    end_xs: list[float] = []
    end_ys: list[float] = []
    results: list[int] = []
    vaep_values: list[float] = []
    time_deltas: list[float] = []
    player_idxs: list[int] = []

    for a in actions_list:
        action_types.append(int(a["action_type"]))
        start_xs.append(float(a["start_x"]))
        start_ys.append(float(a["start_y"]))
        end_xs.append(float(a["end_x"]))
        end_ys.append(float(a["end_y"]))
        results.append(int(a["result"]))
        vaep_values.append(float(a.get("vaep_value", 0.0)))
        time_deltas.append(float(a.get("time_delta", 0.0)))
        player_idxs.append(int(a.get("player_idx", 0)))

    return action_types, start_xs, start_ys, end_xs, end_ys, results, vaep_values, time_deltas, player_idxs


class ScoutGPTDataset(Dataset[dict[str, torch.Tensor]]):
    """PyTorch dataset for ScoutGPT autoregressive training.

    Each sample is a possession episode with a focal player conditioning token
    prepended at position 0. Labels are action_ids shifted right by 1.
    """

    def __init__(
        self,
        action_types: list[list[int]],
        start_xs: list[list[float]],
        start_ys: list[list[float]],
        end_xs: list[list[float]],
        end_ys: list[list[float]],
        results: list[list[int]],
        vaep_values: list[list[float]],
        time_deltas: list[list[float]],
        player_idxs: list[list[int]],
        max_seq_len: int = MAX_SEQ_LEN,
        *,
        competition_ids: list[int] | None = None,
    ) -> None:
        self.action_types = action_types
        self.start_xs = start_xs
        self.start_ys = start_ys
        self.end_xs = end_xs
        self.end_ys = end_ys
        self.results = results
        self.vaep_values = vaep_values
        self.time_deltas = time_deltas
        self.player_idxs = player_idxs
        self.max_seq_len = max_seq_len
        self.competition_ids = competition_ids

    def __len__(self) -> int:
        return len(self.action_types)

    def __getitem__(self, idx: int) -> dict[str, torch.Tensor]:
        # Raw episode actions
        atypes = self.action_types[idx]
        sxs = self.start_xs[idx]
        sys_ = self.start_ys[idx]
        exs = self.end_xs[idx]
        eys = self.end_ys[idx]
        res = self.results[idx]
        vaeps = self.vaep_values[idx]
        tds = self.time_deltas[idx]
        pidxs = self.player_idxs[idx]

        # Truncate episode to max_seq_len - 1 (leave room for BOS token)
        max_actions = self.max_seq_len - 1
        ep_len = min(len(atypes), max_actions)
        total_len = ep_len + 1  # +1 for BOS conditioning token

        # Initialize padded tensors
        action_ids = torch.full((self.max_seq_len,), PAD_TOKEN_ID, dtype=torch.long)
        start_x = torch.zeros(self.max_seq_len, dtype=torch.float32)
        start_y = torch.zeros(self.max_seq_len, dtype=torch.float32)
        end_x = torch.zeros(self.max_seq_len, dtype=torch.float32)
        end_y = torch.zeros(self.max_seq_len, dtype=torch.float32)
        result = torch.zeros(self.max_seq_len, dtype=torch.long)
        time_delta = torch.zeros(self.max_seq_len, dtype=torch.float32)
        player_ids = torch.zeros(self.max_seq_len, dtype=torch.long)
        attention_mask = torch.zeros(self.max_seq_len, dtype=torch.bool)

        # Position 0: BOS conditioning token
        # Focal player = player who performs the first action (position 1)
        action_ids[0] = BOS_TOKEN_ID
        player_ids[0] = pidxs[0]  # focal player
        attention_mask[0] = True

        # Positions 1..ep_len: actual actions
        if ep_len > 0:
            action_ids[1:total_len] = torch.tensor(atypes[:ep_len], dtype=torch.long)
            start_x[1:total_len] = torch.tensor(sxs[:ep_len], dtype=torch.float32)
            start_y[1:total_len] = torch.tensor(sys_[:ep_len], dtype=torch.float32)
            end_x[1:total_len] = torch.tensor(exs[:ep_len], dtype=torch.float32)
            end_y[1:total_len] = torch.tensor(eys[:ep_len], dtype=torch.float32)
            result[1:total_len] = torch.tensor(res[:ep_len], dtype=torch.long)
            time_delta[1:total_len] = torch.tensor(tds[:ep_len], dtype=torch.float32)
            player_ids[1:total_len] = torch.tensor(pidxs[:ep_len], dtype=torch.long)
            attention_mask[1:total_len] = True

        # Labels: shifted right — predict action at t+1 given input at t
        # Position 0 (BOS) label = first action type
        # Position k label = action type at k+1
        # Last valid position and padding: -100 (ignored)
        labels = torch.full((self.max_seq_len,), -100, dtype=torch.long)
        vaep_targets = torch.zeros(self.max_seq_len, dtype=torch.float32)

        if ep_len > 0:
            # Autoregressive labeling:
            # Input:  [BOS, a0, a1, a2, ..., a_{n-1}]
            # Labels: [a0,  a1, a2, a3, ..., -100     ]
            # Label at position t = action_ids[t+1] (predict the next token)
            for t in range(total_len - 1):
                next_id = action_ids[t + 1].item()
                labels[t] = next_id if next_id != PAD_TOKEN_ID else -100

            # VAEP targets at positions 1..total_len-1 (aligned with actual actions)
            vaep_targets[1:total_len] = torch.tensor(vaeps[:ep_len], dtype=torch.float32)

        out: dict[str, torch.Tensor] = {
            "action_ids": action_ids,
            "start_x": start_x,
            "start_y": start_y,
            "end_x": end_x,
            "end_y": end_y,
            "result": result,
            "time_delta": time_delta,
            "player_ids": player_ids,
            "attention_mask": attention_mask,
            "labels": labels,
            "vaep_targets": vaep_targets,
        }
        if self.competition_ids is not None:
            out["competition_id"] = torch.tensor(self.competition_ids[idx], dtype=torch.long)
        return out


def stratified_split(
    data: pd.DataFrame,
    train_frac: float = 0.80,
    val_frac: float = 0.10,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """80/10/10 split stratified by competition_id with rare-class collapsing."""
    from sklearn.model_selection import train_test_split

    stratify_col = data["competition_id"].astype(str)
    counts = stratify_col.value_counts()
    rare_mask = stratify_col.isin(counts[counts < 3].index)
    stratify_col = stratify_col.copy()
    stratify_col.loc[rare_mask] = "_other_"

    indices = np.arange(len(data))
    test_frac = 1.0 - train_frac - val_frac
    train_val_idx, test_idx = train_test_split(
        indices, test_size=test_frac, random_state=RANDOM_STATE, stratify=stratify_col,
    )
    val_relative = val_frac / (train_frac + val_frac)
    stratify_trainval = stratify_col.iloc[train_val_idx]
    tv_counts = stratify_trainval.value_counts()
    tv_rare = stratify_trainval.isin(tv_counts[tv_counts < 2].index)
    stratify_trainval = stratify_trainval.copy()
    stratify_trainval.loc[tv_rare] = "_other_"

    train_idx, val_idx = train_test_split(
        train_val_idx, test_size=val_relative, random_state=RANDOM_STATE, stratify=stratify_trainval,
    )
    return data.iloc[train_idx], data.iloc[val_idx], data.iloc[test_idx]


def get_cosine_schedule_with_warmup(
    optimizer: torch.optim.Optimizer,
    num_warmup_steps: int,
    num_training_steps: int,
) -> torch.optim.lr_scheduler.LambdaLR:
    """Cosine annealing with linear warmup."""

    def lr_lambda(current_step: int) -> float:
        if current_step < num_warmup_steps:
            return float(current_step) / float(max(1, num_warmup_steps))
        progress = float(current_step - num_warmup_steps) / float(max(1, num_training_steps - num_warmup_steps))
        return max(0.0, 0.5 * (1.0 + math.cos(math.pi * progress)))

    return torch.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda)


def build_datasets(
    data: pd.DataFrame,
    max_seq_len: int = MAX_SEQ_LEN,
) -> tuple[list[list[int]], list[list[float]], list[list[float]], list[list[float]], list[list[float]], list[list[int]], list[list[float]], list[list[float]], list[list[int]], list[int]]:
    """Parse all episodes into per-field lists for ScoutGPTDataset."""
    all_action_types: list[list[int]] = []
    all_start_xs: list[list[float]] = []
    all_start_ys: list[list[float]] = []
    all_end_xs: list[list[float]] = []
    all_end_ys: list[list[float]] = []
    all_results: list[list[int]] = []
    all_vaeps: list[list[float]] = []
    all_time_deltas: list[list[float]] = []
    all_player_idxs: list[list[int]] = []
    all_comp_ids: list[int] = []

    for _, row in data.iterrows():
        atypes, sxs, sys_, exs, eys, res, vaeps, tds, pidxs = parse_episode_actions(row["actions"])
        all_action_types.append(atypes)
        all_start_xs.append(sxs)
        all_start_ys.append(sys_)
        all_end_xs.append(exs)
        all_end_ys.append(eys)
        all_results.append(res)
        all_vaeps.append(vaeps)
        all_time_deltas.append(tds)
        all_player_idxs.append(pidxs)
        all_comp_ids.append(int(row["competition_id"]))

    return (
        all_action_types, all_start_xs, all_start_ys, all_end_xs, all_end_ys,
        all_results, all_vaeps, all_time_deltas, all_player_idxs, all_comp_ids,
    )


def compute_baselines(
    test_ds: ScoutGPTDataset,
    data: pd.DataFrame,
) -> dict[str, float]:
    """Compute naive baselines for comparison.

    - most_frequent: always predict the most common action type
    - bigram: predict the most common next action given the current action
    """
    # Build action frequency and bigram tables from training data
    all_actions: list[int] = []
    bigram_counts: dict[tuple[int, int], int] = {}

    for _, row in data.iterrows():
        atypes, *_ = parse_episode_actions(row["actions"])
        all_actions.extend(atypes)
        for i in range(len(atypes) - 1):
            key = (atypes[i], atypes[i + 1])
            bigram_counts[key] = bigram_counts.get(key, 0) + 1

    # Most frequent action
    from collections import Counter

    action_counter = Counter(all_actions)
    most_frequent = action_counter.most_common(1)[0][0]

    # Bigram table: for each action, most likely next action
    bigram_next: dict[int, int] = {}
    for action_type in range(VOCAB_SIZE):
        candidates = {k: v for k, v in bigram_counts.items() if k[0] == action_type}
        if candidates:
            bigram_next[action_type] = max(candidates, key=lambda k: candidates[k])[1]
        else:
            bigram_next[action_type] = most_frequent

    # Evaluate baselines on test set
    mf_correct = 0
    bg_correct = 0
    total = 0

    for i in range(len(test_ds)):
        sample = test_ds[i]
        labels = sample["labels"]
        action_ids = sample["action_ids"]
        for t in range(len(labels)):
            if labels[t].item() == -100:
                continue
            total += 1
            true_label = labels[t].item()
            if true_label == most_frequent:
                mf_correct += 1
            current_action = action_ids[t].item()
            if current_action < VOCAB_SIZE and bigram_next.get(current_action) == true_label:
                bg_correct += 1

    return {
        "baseline_most_frequent_accuracy": mf_correct / max(total, 1),
        "baseline_bigram_accuracy": bg_correct / max(total, 1),
    }


def evaluate_counterfactual_ranking(
    model: torch.nn.Module,
    test_ds: ScoutGPTDataset,
    device: torch.device,
    num_episodes: int = COUNTERFACTUAL_NUM_EPISODES,
    num_players: int = COUNTERFACTUAL_NUM_PLAYERS,
    action_type_frequencies: dict[int, dict[int, float]] | None = None,
) -> dict[str, float]:
    """Counterfactual ranking correlation.

    For each test episode, swap the focal player at position 0 with top-N most
    active players. Rank by P(actual_next_action | swapped_player). Compute
    Spearman correlation with player's real-world action type frequency.

    Args:
        model: Trained ScoutGPTDecoder.
        test_ds: Test dataset.
        device: Torch device.
        num_episodes: Number of episodes to sample.
        num_players: Number of player swaps per episode.
        action_type_frequencies: Per-player action type frequency dict.
            {player_idx: {action_type: frequency}}. If None, skips plausibility
            correlation (returns rho=0).

    Returns:
        Dict with mean_spearman_rho and individual episode rhos.
    """
    model.eval()
    rng = np.random.RandomState(RANDOM_STATE)

    # Sample episode indices
    n_episodes = min(num_episodes, len(test_ds))
    episode_indices = rng.choice(len(test_ds), size=n_episodes, replace=False)

    # Top-N most active player indices (by training data frequency)
    if action_type_frequencies is None:
        return {"mean_spearman_rho": 0.0, "n_episodes_evaluated": 0}

    player_activity = {pid: sum(freqs.values()) for pid, freqs in action_type_frequencies.items()}
    top_players = sorted(player_activity, key=lambda p: player_activity[p], reverse=True)[:num_players]

    rho_values: list[float] = []

    with torch.no_grad():
        for ep_idx in episode_indices:
            sample = test_ds[int(ep_idx)]
            # Find last valid prediction position
            labels = sample["labels"]
            valid_positions = (labels != -100).nonzero(as_tuple=True)[0]
            if len(valid_positions) == 0:
                continue
            last_pos = valid_positions[-1].item()
            true_action = labels[last_pos].item()

            # For each candidate player, compute P(true_action | player)
            log_probs: list[float] = []
            plausibility_scores: list[float] = []

            for player_idx in top_players:
                # Clone sample and swap focal player at position 0
                batch = {k: v.unsqueeze(0).to(device) for k, v in sample.items() if k not in ("labels", "vaep_targets", "competition_id")}
                batch["player_ids"][0, 0] = player_idx

                action_logits, _ = model.predict(**batch)
                # Log-prob of true action at last valid position
                logits_at_pos = action_logits[0, last_pos, :]
                log_prob = torch.log_softmax(logits_at_pos, dim=-1)[true_action].item()
                log_probs.append(log_prob)

                # Plausibility: player's real-world frequency of this action type
                player_freqs = action_type_frequencies.get(player_idx, {})
                total_actions = sum(player_freqs.values())
                plausibility = player_freqs.get(true_action, 0) / max(total_actions, 1)
                plausibility_scores.append(plausibility)

            if len(log_probs) >= 2:
                rho, _ = spearmanr(log_probs, plausibility_scores)
                if not np.isnan(rho):
                    rho_values.append(float(rho))

    mean_rho = float(np.mean(rho_values)) if rho_values else 0.0
    return {
        "mean_spearman_rho": mean_rho,
        "n_episodes_evaluated": len(rho_values),
        "rho_std": float(np.std(rho_values)) if rho_values else 0.0,
    }


def build_action_type_frequencies(
    data: pd.DataFrame,
) -> dict[int, dict[int, float]]:
    """Build per-player action type frequency table from training data.

    Returns {player_idx: {action_type: count}}.
    """
    freq: dict[int, dict[int, float]] = {}
    for _, row in data.iterrows():
        atypes, *_, pidxs = parse_episode_actions(row["actions"])
        for atype, pidx in zip(atypes, pidxs):
            if pidx not in freq:
                freq[pidx] = {}
            freq[pidx][atype] = freq[pidx].get(atype, 0) + 1
    return freq
```

- [ ] **Step 2: Run linter**

Run: `uv run ruff check scripts/train_scoutgpt_hf_helpers.py`
Expected: No errors

### Task 9: Training Script — Main Entry Point

**Files:**
- Create: `scripts/train_scoutgpt_hf.py`

- [ ] **Step 1: Implement training script**

```python
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.1.0-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "torch>=2.0",
#     "safetensors>=0.4.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
#     "scikit-learn>=1.3.0",
#     "scipy>=1.11.0",
# ]
# ///
"""ScoutGPT decoder training on HF Jobs.

Player-conditioned GPT-style causal transformer over SPADL possession episodes.
Autoregressive next-action prediction with VAEP auxiliary head.

Usage:
    uv run scripts/train_scoutgpt_hf.py [--epochs N] [--batch-size N] [--lr F] [--patience N]

Reference: Hong et al. (2025), arXiv:2512.17266
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
import time
from dataclasses import asdict
from typing import Any

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from train_scoutgpt_hf_helpers import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_EPOCHS,
    DEFAULT_LR,
    DEFAULT_PATIENCE,
    MAX_SEQ_LEN,
    VAEP_LOSS_WEIGHT,
    WARMUP_FRACTION,
    WEIGHT_DECAY,
    ScoutGPTDataset,
    build_action_type_frequencies,
    build_datasets,
    compute_baselines,
    evaluate_counterfactual_ranking,
    get_cosine_schedule_with_warmup,
    load_training_data,
    stratified_split,
)

from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder
from ingestion.hf_jobs_cost import HF_RATE_A10G_LARGE, HFJobsCostRecorder
from workflows import workflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MODEL_REPO = "luxury-lakehouse/scoutgpt"
DATASET_REPO = "luxury-lakehouse/scoutgpt-training-data"


def _train_loop(
    train_ds: ScoutGPTDataset,
    val_ds: ScoutGPTDataset,
    config: ScoutGPTConfig,
    device: torch.device,
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
) -> tuple[ScoutGPTDecoder, dict[str, list[float]]]:
    """Autoregressive training loop with VAEP auxiliary head."""
    model = ScoutGPTDecoder(config).to(device)
    logger.info("Model parameters: %d", sum(p.numel() for p in model.parameters()))

    train_loader = DataLoader(
        train_ds, batch_size=batch_size, shuffle=True, num_workers=0,
        pin_memory=device.type == "cuda",
    )
    val_loader = DataLoader(
        val_ds, batch_size=batch_size, shuffle=False, num_workers=0,
        pin_memory=device.type == "cuda",
    )

    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=WEIGHT_DECAY)
    total_steps = len(train_loader) * epochs
    warmup_steps = int(total_steps * WARMUP_FRACTION)
    scheduler = get_cosine_schedule_with_warmup(optimizer, warmup_steps, total_steps)

    action_criterion = nn.CrossEntropyLoss(ignore_index=-100)
    vaep_criterion = nn.MSELoss()

    best_val_loss = float("inf")
    best_state: dict[str, torch.Tensor] = {}
    patience_ctr = 0

    history: dict[str, list[float]] = {
        "train_loss": [], "train_action_loss": [], "train_vaep_loss": [],
        "val_loss": [], "val_action_loss": [], "val_vaep_loss": [],
        "val_top1_accuracy": [], "val_top5_accuracy": [],
    }

    for epoch in range(epochs):
        t0 = time.time()

        # Training
        model.train()
        epoch_loss = 0.0
        epoch_action_loss = 0.0
        epoch_vaep_loss = 0.0
        n_batches = 0

        for batch in train_loader:
            b = {k: v.to(device) for k, v in batch.items()}
            optimizer.zero_grad()

            action_logits, vaep_preds = model.predict(
                b["action_ids"], b["start_x"], b["start_y"], b["end_x"], b["end_y"],
                b["result"], b["time_delta"], b["player_ids"], b["attention_mask"],
            )

            # Action loss: flatten (batch*seq, vocab) vs (batch*seq,)
            action_loss = action_criterion(
                action_logits.view(-1, config.vocab_size), b["labels"].view(-1),
            )

            # VAEP loss: only at valid action positions (not BOS, not PAD)
            vaep_mask = b["attention_mask"] & (b["action_ids"] != 24)  # exclude BOS
            if vaep_mask.any():
                vaep_loss = vaep_criterion(
                    vaep_preds.squeeze(-1)[vaep_mask], b["vaep_targets"][vaep_mask],
                )
            else:
                vaep_loss = torch.tensor(0.0, device=device)

            loss = action_loss + config.vaep_loss_weight * vaep_loss
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            scheduler.step()

            epoch_loss += loss.item()
            epoch_action_loss += action_loss.item()
            epoch_vaep_loss += vaep_loss.item()
            n_batches += 1

        avg_train_loss = epoch_loss / max(n_batches, 1)
        avg_train_action = epoch_action_loss / max(n_batches, 1)
        avg_train_vaep = epoch_vaep_loss / max(n_batches, 1)

        # Validation
        model.eval()
        val_loss = 0.0
        val_action_loss = 0.0
        val_vaep_loss = 0.0
        val_correct_top1 = 0
        val_correct_top5 = 0
        val_total = 0
        n_val_batches = 0

        with torch.no_grad():
            for batch in val_loader:
                b = {k: v.to(device) for k, v in batch.items()}
                action_logits, vaep_preds = model.predict(
                    b["action_ids"], b["start_x"], b["start_y"], b["end_x"], b["end_y"],
                    b["result"], b["time_delta"], b["player_ids"], b["attention_mask"],
                )

                a_loss = action_criterion(
                    action_logits.view(-1, config.vocab_size), b["labels"].view(-1),
                )

                vaep_mask = b["attention_mask"] & (b["action_ids"] != 24)
                if vaep_mask.any():
                    v_loss = vaep_criterion(
                        vaep_preds.squeeze(-1)[vaep_mask], b["vaep_targets"][vaep_mask],
                    )
                else:
                    v_loss = torch.tensor(0.0, device=device)

                val_loss += (a_loss + config.vaep_loss_weight * v_loss).item()
                val_action_loss += a_loss.item()
                val_vaep_loss += v_loss.item()
                n_val_batches += 1

                # Top-1 and top-5 accuracy
                valid_mask = b["labels"].view(-1) != -100
                if valid_mask.any():
                    preds = action_logits.view(-1, config.vocab_size)[valid_mask]
                    targets = b["labels"].view(-1)[valid_mask]
                    val_correct_top1 += (preds.argmax(dim=-1) == targets).sum().item()
                    top5 = preds.topk(min(5, config.vocab_size), dim=-1).indices
                    val_correct_top5 += (top5 == targets.unsqueeze(-1)).any(dim=-1).sum().item()
                    val_total += targets.size(0)

        avg_val_loss = val_loss / max(n_val_batches, 1)
        avg_val_action = val_action_loss / max(n_val_batches, 1)
        avg_val_vaep = val_vaep_loss / max(n_val_batches, 1)
        top1_acc = val_correct_top1 / max(val_total, 1)
        top5_acc = val_correct_top5 / max(val_total, 1)

        history["train_loss"].append(avg_train_loss)
        history["train_action_loss"].append(avg_train_action)
        history["train_vaep_loss"].append(avg_train_vaep)
        history["val_loss"].append(avg_val_loss)
        history["val_action_loss"].append(avg_val_action)
        history["val_vaep_loss"].append(avg_val_vaep)
        history["val_top1_accuracy"].append(top1_acc)
        history["val_top5_accuracy"].append(top5_acc)

        elapsed = time.time() - t0
        logger.info(
            "Epoch %d/%d [%.1fs] — train_loss: %.4f (action: %.4f, vaep: %.4f) | "
            "val_loss: %.4f (action: %.4f, vaep: %.4f) | top1: %.3f, top5: %.3f",
            epoch + 1, epochs, elapsed, avg_train_loss, avg_train_action, avg_train_vaep,
            avg_val_loss, avg_val_action, avg_val_vaep, top1_acc, top5_acc,
        )

        # Early stopping
        if avg_val_loss < best_val_loss:
            best_val_loss = avg_val_loss
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
            patience_ctr = 0
        else:
            patience_ctr += 1
            if patience_ctr >= patience:
                logger.info("Early stopping at epoch %d (patience=%d)", epoch + 1, patience)
                break

    if best_state:
        model.load_state_dict(best_state)
    return model, history


def _save_checkpoint(
    model: ScoutGPTDecoder,
    config: ScoutGPTConfig,
    hf_token: str,
    metrics: dict[str, Any] | None = None,
) -> None:
    """Save model weights and config to HF Hub."""
    from huggingface_hub import HfApi
    from safetensors.torch import save_file as _save

    api = HfApi(token=hf_token)
    api.create_repo(MODEL_REPO, exist_ok=True, repo_type="model", token=hf_token)

    with tempfile.TemporaryDirectory() as td:
        sp = os.path.join(td, "model.safetensors")
        _save(model.state_dict(), sp)

        cd = asdict(config)
        cd.update({
            "_pad_token_id": 23,
            "_bos_token_id": 24,
            "_expanded_vocab_size": 25,
        })
        cp = os.path.join(td, "config.json")
        with open(cp, "w", encoding="utf-8") as f:
            json.dump(cd, f, indent=2)

        for name, path in [("model.safetensors", sp), ("config.json", cp)]:
            api.upload_file(
                path_or_fileobj=path,
                path_in_repo=f"stage1/{name}",
                repo_id=MODEL_REPO,
                repo_type="model",
                token=hf_token,
            )

    if metrics:
        api.upload_file(
            path_or_fileobj=json.dumps(metrics, indent=2).encode("utf-8"),
            path_in_repo="metrics.json",
            repo_id=MODEL_REPO,
            repo_type="model",
            token=hf_token,
        )
    logger.info("Checkpoint saved to %s", MODEL_REPO)


def _evaluate_and_report(
    model: ScoutGPTDecoder,
    test_ds: ScoutGPTDataset,
    train_data: pd.DataFrame,
    device: torch.device,
    history: dict[str, list[float]],
) -> dict[str, Any]:
    """Run all evaluation metrics and build final metrics dict."""
    model.eval()

    # Test set accuracy
    test_loader = DataLoader(test_ds, batch_size=256, shuffle=False, num_workers=0)
    correct_top1 = 0
    correct_top5 = 0
    total = 0

    # Per-bucket accuracy
    bucket_correct: dict[str, int] = {"short": 0, "medium": 0, "long": 0}
    bucket_total: dict[str, int] = {"short": 0, "medium": 0, "long": 0}

    with torch.no_grad():
        for batch in test_loader:
            b = {k: v.to(device) for k, v in batch.items() if k not in ("labels", "vaep_targets", "competition_id")}
            labels = batch["labels"].to(device)
            action_logits, _ = model.predict(**b)

            valid_mask = labels.view(-1) != -100
            if valid_mask.any():
                preds = action_logits.view(-1, 23)[valid_mask]
                targets = labels.view(-1)[valid_mask]
                correct_top1 += (preds.argmax(dim=-1) == targets).sum().item()
                top5 = preds.topk(min(5, 23), dim=-1).indices
                correct_top5 += (top5 == targets.unsqueeze(-1)).any(dim=-1).sum().item()
                total += targets.size(0)

            # Per-episode bucket classification
            for i in range(labels.size(0)):
                ep_valid = (labels[i] != -100).sum().item()
                if ep_valid == 0:
                    continue
                if ep_valid <= 7:
                    bucket = "short"
                elif ep_valid <= 15:
                    bucket = "medium"
                else:
                    bucket = "long"
                ep_preds = action_logits[i][labels[i] != -100]
                ep_targets = labels[i][labels[i] != -100]
                bucket_correct[bucket] += (ep_preds.argmax(dim=-1) == ep_targets).sum().item()
                bucket_total[bucket] += ep_targets.size(0)

    test_top1 = correct_top1 / max(total, 1)
    test_top5 = correct_top5 / max(total, 1)

    # Baselines
    logger.info("Computing baselines...")
    baselines = compute_baselines(test_ds, train_data)

    # Counterfactual ranking
    logger.info("Computing counterfactual ranking correlation...")
    action_freqs = build_action_type_frequencies(train_data)
    cf_results = evaluate_counterfactual_ranking(model, test_ds, device, action_type_frequencies=action_freqs)

    # Cross-source accuracy — split test data by data_source
    cross_source: dict[str, float] = {}
    for source in test_data["data_source"].unique():
        source_mask = test_data["data_source"] == source
        source_data = test_data[source_mask]
        if len(source_data) < 10:
            continue
        source_fields = build_datasets(source_data)
        source_ds = ScoutGPTDataset(*source_fields[:-1], competition_ids=source_fields[-1])
        src_correct = 0
        src_total = 0
        src_loader = DataLoader(source_ds, batch_size=256, shuffle=False, num_workers=0)
        with torch.no_grad():
            for sbatch in src_loader:
                sb = {k: v.to(device) for k, v in sbatch.items() if k not in ("labels", "vaep_targets", "competition_id")}
                slabels = sbatch["labels"].to(device)
                slogits, _ = model.predict(**sb)
                svalid = slabels.view(-1) != -100
                if svalid.any():
                    spreds = slogits.view(-1, 23)[svalid]
                    stargets = slabels.view(-1)[svalid]
                    src_correct += (spreds.argmax(dim=-1) == stargets).sum().item()
                    src_total += stargets.size(0)
        cross_source[source] = src_correct / max(src_total, 1)
        logger.info("Cross-source %s: top-1 %.3f (%d predictions)", source, cross_source[source], src_total)

    metrics: dict[str, Any] = {
        "test_top1_accuracy": test_top1,
        "test_top5_accuracy": test_top5,
        "test_top1_by_bucket": {
            k: bucket_correct[k] / max(bucket_total[k], 1) for k in bucket_correct
        },
        "test_total_predictions": total,
        **baselines,
        **cf_results,
        "cross_source_accuracy": cross_source,
        "cross_source_gap": max(cross_source.values()) - min(cross_source.values()) if len(cross_source) >= 2 else 0.0,
        "best_val_loss": min(history["val_loss"]) if history["val_loss"] else 0.0,
        "best_val_top1": max(history["val_top1_accuracy"]) if history["val_top1_accuracy"] else 0.0,
        "epochs_trained": len(history["train_loss"]),
    }

    logger.info("Test top-1: %.3f, top-5: %.3f", test_top1, test_top5)
    logger.info("Baselines — most_frequent: %.3f, bigram: %.3f",
                baselines["baseline_most_frequent_accuracy"], baselines["baseline_bigram_accuracy"])
    logger.info("Counterfactual Spearman rho: %.3f (n=%d)",
                cf_results["mean_spearman_rho"], cf_results["n_episodes_evaluated"])
    for bucket in ("short", "medium", "long"):
        logger.info("  %s episodes: top-1 %.3f (%d predictions)",
                    bucket, metrics["test_top1_by_bucket"][bucket], bucket_total[bucket])

    return metrics


@workflow("wf-scoutgpt", phase="training")
def main() -> None:
    """HF Jobs entry point for ScoutGPT training."""
    parser = argparse.ArgumentParser(description="Train ScoutGPT decoder")
    parser.add_argument("--epochs", type=int, default=DEFAULT_EPOCHS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--lr", type=float, default=DEFAULT_LR)
    parser.add_argument("--patience", type=int, default=DEFAULT_PATIENCE)
    args = parser.parse_args()

    from huggingface_hub import get_token
    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN required")

    recorder = HFJobsCostRecorder(
        workflow_id="wf-scoutgpt",
        phase="training",
        rate_usd_per_hour=HF_RATE_A10G_LARGE,
        repo_id=MODEL_REPO,
        repo_type="model",
    )
    recorder.start()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Using device: %s", device)
    t0 = time.time()

    try:
        # Load data
        logger.info("Loading training data from %s...", DATASET_REPO)
        data, player_id_map, dataset_sha = load_training_data(hf_token, DATASET_REPO)
        logger.info("Dataset SHA: %s", dataset_sha)

        # Split
        train_data, val_data, test_data = stratified_split(data)
        logger.info("Split: train=%d, val=%d, test=%d", len(train_data), len(val_data), len(test_data))

        # Build datasets
        logger.info("Building PyTorch datasets...")
        train_fields = build_datasets(train_data)
        val_fields = build_datasets(val_data)
        test_fields = build_datasets(test_data)

        train_ds = ScoutGPTDataset(*train_fields[:-1], competition_ids=train_fields[-1])
        val_ds = ScoutGPTDataset(*val_fields[:-1], competition_ids=val_fields[-1])
        test_ds = ScoutGPTDataset(*test_fields[:-1], competition_ids=test_fields[-1])

        # Config
        config = ScoutGPTConfig(num_players=len(player_id_map))
        logger.info("Config: %s", config)

        # Train
        logger.info("Starting training...")
        model, history = _train_loop(
            train_ds, val_ds, config, device,
            epochs=args.epochs, batch_size=args.batch_size, lr=args.lr, patience=args.patience,
        )

        # Evaluate
        logger.info("Running evaluation...")
        metrics = _evaluate_and_report(model, test_ds, train_data, device, history)

        # Save
        logger.info("Saving checkpoint...")
        _save_checkpoint(model, config, hf_token, metrics)

        # Cost recording
        recorder.complete(metrics, row_count=len(data))

    except Exception as exc:
        recorder.fail(exc)
        raise

    logger.info("ScoutGPT training complete in %.1fs", time.time() - t0)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run linter on training script**

Run: `uv run ruff check scripts/train_scoutgpt_hf.py scripts/train_scoutgpt_hf_helpers.py`
Expected: No errors

- [ ] **Step 3: Run formatter check**

Run: `uv run ruff format --check scripts/train_scoutgpt_hf.py scripts/train_scoutgpt_hf_helpers.py`
Expected: Already formatted

### Task 10: Phase C Lint, Type Check, and Full Test Suite

- [ ] **Step 1: Run full linter on all new files**

Run: `uv run ruff check src/analytics/scoutgpt_decoder.py src/tests/test_scoutgpt_decoder.py src/ingestion/export_scoutgpt_training_data.py scripts/train_scoutgpt_hf.py scripts/train_scoutgpt_hf_helpers.py`
Expected: No errors

- [ ] **Step 2: Run pyright on model and export**

Run: `uv run pyright src/analytics/scoutgpt_decoder.py src/ingestion/export_scoutgpt_training_data.py`
Expected: 0 errors (or only PySpark stub warnings)

- [ ] **Step 3: Run full decoder test suite**

Run: `uv run pytest src/tests/test_scoutgpt_decoder.py -v`
Expected: 12 passed

- [ ] **Step 4: Validate all workflow cards**

Run: `uv run validate_workflow_cards --validate workflow-cards/`
Expected: All cards OK

- [ ] **Step 5: Run existing test suite (regression check)**

Run: `uv run pytest src/tests/ -v --timeout=60`
Expected: All existing tests still pass

- [ ] **Step 6: Phase C commit checkpoint**

All files complete and green. This is the final commit point.

---

## Summary

| Phase | Tasks | New Files | Modified Files |
|-------|-------|-----------|----------------|
| A | 1–6 | `scoutgpt_decoder.py`, `test_scoutgpt_decoder.py`, `wf-scoutgpt.yaml`, `wf-scoutgpt-export.yaml` | `pyproject.toml` (+1 entry point) |
| B | 7 | `export_scoutgpt_training_data.py` | None |
| C | 8–10 | `train_scoutgpt_hf.py`, `train_scoutgpt_hf_helpers.py` | None |

Total: 7 new files, 1 modified file (1 line added).
