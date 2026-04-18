# EV1 — Football2Vec v2 Level 1 Config Sweep — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `evolve` target named `football2vec` that runs an LLM-guided sweep over the Football2Vec v2 stage-1 hyperparameters and three new architectural enums, using the existing OpenEvolve loop and the existing local backend pool.

**Architecture:** Refactor `src/evolve/evaluator.py:validate_search_space` into a target-aware dispatcher. Extract ScoutGPT's existing search-space schema into `src/evolve/targets/scoutgpt/search_space.py`. Add a parallel `src/evolve/targets/football2vec/` module tree (search_space, evaluator, seed_programs, config.yaml, prompts). Add three architectural enum knobs (`pooling_type`, `spatial_injection`, `position_embedding`) to `Football2VecEncoder` with defaults that reproduce current behaviour byte-for-byte.

**Tech Stack:** Python 3.10, PyTorch, Pydantic v2, OpenEvolve (Apache 2.0), HuggingFace Datasets, pytest, Ruff, Pyright. Local CUDA backends: RTX 5070 Ti + DGX Spark GB10 over SSH.

**Spec:** [`docs/superpowers/specs/2026-04-18-ev1-football2vec-l1-sweep-design.md`](../specs/2026-04-18-ev1-football2vec-l1-sweep-design.md)

**Commit cadence (per user):**
- Tasks A-D produce *staged but uncommitted* changes on `evolve/football2vec-l1-sweep`.
- **First commit gate** after Block E (POC smoke test passes) — explicit user approval required.
- **Second commit gate** after Block F (full overnight run completes) — explicit user approval required.
- No PRs opened or merges to main without separate explicit approval.

---

## Block A — Per-target search-space dispatcher (no behaviour change)

### Task A1: Extract ScoutGPT search-space into per-target module

**Files:**
- Create: `src/evolve/targets/scoutgpt/search_space.py`
- (No test changes yet — Task A2 is the dispatcher; existing tests stay green at the end of A2)

- [ ] **Step 1: Create the new file with the ScoutGPT schema**

Move (do not modify) the `_BOUNDS`, `CandidateConfig`, and `validate_search_space` logic from `src/evolve/evaluator.py:32-114` into the new file. The function in this new file is named `validate_candidate` (per-target convention).

```python
"""ScoutGPT search-space schema — extracted from evolve/evaluator.py for per-target dispatch."""

from __future__ import annotations

import logging
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator, model_validator

_log = logging.getLogger(__name__)

_BOUNDS: dict[str, tuple[float, float]] = {
    "hidden_dim": (64, 512),
    "num_layers": (2, 12),
    "num_heads": (2, 16),
    "dropout": (0.0, 0.5),
    "learning_rate": (1e-5, 1e-2),
    "vaep_loss_weight": (0.0, 1.0),
    "player_prediction_weight": (0.0, 1.0),
    "batch_size": (64, 512),
}


class CandidateConfig(BaseModel):
    """Typed schema for ScoutGPT candidate architecture configs."""

    model_config = ConfigDict(extra="allow")

    conditioning_type: Literal["additive", "cross_attention", "film", "gated"] = "additive"
    hidden_dim: int = 256
    num_layers: int = 6
    num_heads: int = 8
    dropout: float = 0.1
    max_seq_len: int = 128
    num_players: int = 100
    spatial_mlp_dim: int = 64
    vaep_loss_weight: float = 0.1
    player_prediction_weight: float = 0.0

    learning_rate: float = 1e-4
    weight_decay: float = 0.01
    batch_size: int = 256
    dataset: str = "luxury-lakehouse/scoutgpt-training-data"

    @field_validator("dataset")
    @classmethod
    def _validate_dataset_prefix(cls, v: str) -> str:
        if not v.startswith("luxury-lakehouse/"):
            msg = f"dataset must be a luxury-lakehouse/ HF repo, got '{v}'"
            raise ValueError(msg)
        return v

    @model_validator(mode="after")
    def _validate_search_space(self) -> CandidateConfig:
        for key, (lo, hi) in _BOUNDS.items():
            val = getattr(self, key, None)
            if val is not None and not (lo <= val <= hi):
                msg = f"{key}={val!r} not in [{lo}, {hi}]"
                raise ValueError(msg)
        if self.hidden_dim % self.num_heads != 0:
            msg = f"hidden_dim={self.hidden_dim} not divisible by num_heads={self.num_heads}"
            raise ValueError(msg)
        if self.__pydantic_extra__:
            _log.warning(
                "Candidate config has unrecognised keys (possible typos?): %s",
                sorted(self.__pydantic_extra__),
            )
        return self


def validate_candidate(config: dict[str, Any]) -> bool:
    """Validate ScoutGPT candidate config. Returns True on pass, False on reject (with logged reason)."""
    try:
        CandidateConfig(**config)
    except (ValidationError, ValueError) as exc:
        _log.warning("Search space rejection: %s", exc)
        return False
    return True
```

- [ ] **Step 2: Stage the new file**

```bash
git add src/evolve/targets/scoutgpt/search_space.py
```

(No commit. No tests run yet — Task A2 wires the dispatcher and verifies regression.)

---

### Task A2: Refactor `evolve/evaluator.py` into a target-aware dispatcher

**Files:**
- Modify: `src/evolve/evaluator.py:32-114` (remove the now-extracted ScoutGPT schema, replace `validate_search_space` with dispatcher)
- Modify: `src/evolve/evaluator.py:285` (call site in `EvolveEvaluator.evaluate` — pass `self._target`)

- [ ] **Step 1: Replace lines 32-114 with the dispatcher**

Open `src/evolve/evaluator.py`. Delete the existing `_BOUNDS` dict (line 32-41), the entire `CandidateConfig` class (line 44-100), and the existing `validate_search_space` function (line 103-114). Replace with:

```python
def validate_search_space(config: dict[str, Any], target: str = "scoutgpt") -> bool:
    """Validate *config* against the per-target search-space schema.

    Args:
        config: Candidate config dict.
        target: Target name; resolves to `evolve.targets.<target>.search_space:validate_candidate`.

    Returns:
        ``True`` if the config is valid, ``False`` otherwise. Invalid configs are
        logged at WARNING level by the per-target validator with the rejection reason.
    """
    try:
        target_module = importlib.import_module(f"evolve.targets.{target}.search_space")
    except ImportError:
        _log.exception("No search_space module for target %r", target)
        return False
    return bool(target_module.validate_candidate(config))
```

`importlib` is already imported at line 18. Remove the now-unused imports: `Literal`, `BaseModel`, `ConfigDict`, `ValidationError`, `field_validator`, `model_validator`. Keep `ast`, `traceback`, `dataclass`, `Path`, `Any`, `EvaluationResult`, `ComputeBackend`, `fail_metrics`, `ValidationProfile`, `validate_program`, `EvalConfig`, `FitnessConfig`.

- [ ] **Step 2: Update the `EvolveEvaluator.evaluate` call site**

Find the line (currently around 285):

```python
        if not validate_search_space(config):
```

Change to:

```python
        if not validate_search_space(config, self._target):
```

- [ ] **Step 3: Run the full ScoutGPT regression test**

```bash
uv run pytest src/tests/test_evolve_evaluator.py -v
```

Expected: ALL existing tests pass (no behavioural change for ScoutGPT). If any test fails citing `CandidateConfig` import errors, the failing test file imports the now-removed class — note the failures and fix them in Task A3.

- [ ] **Step 4: Stage the changes**

```bash
git add src/evolve/evaluator.py
```

---

### Task A3: Update existing test imports for the moved schema

**Files:**
- Modify: `src/tests/test_evolve_evaluator.py:18` (import path)

- [ ] **Step 1: Update the import**

Open `src/tests/test_evolve_evaluator.py`. The current import line 18:

```python
from evolve.evaluator import EvolveEvaluator, Program, _load_program, validate_search_space
```

Stays unchanged — `validate_search_space` is still at the same path; only `CandidateConfig` was removed and that name was not imported by tests (verified via grep). Add a per-target dispatch test:

- [ ] **Step 2: Add the per-target dispatch regression test**

Append to the end of `src/tests/test_evolve_evaluator.py`:

```python
# ---------------------------------------------------------------------------
# Per-target search-space dispatch (D1 — see EV1 spec)
# ---------------------------------------------------------------------------


class TestPerTargetDispatch:
    """Regression guard for the target-aware validate_search_space dispatcher."""

    def test_scoutgpt_dispatch(self) -> None:
        """validate_search_space(cfg, target='scoutgpt') accepts a valid ScoutGPT config."""
        assert validate_search_space(VALID_CONFIG, target="scoutgpt") is True

    def test_default_target_is_scoutgpt(self) -> None:
        """Backward-compat: validate_search_space(cfg) without target still validates as ScoutGPT."""
        assert validate_search_space(VALID_CONFIG) is True

    def test_unknown_target_returns_false(self) -> None:
        """Unknown target name is logged and rejected (not raised)."""
        assert validate_search_space(VALID_CONFIG, target="nonexistent") is False
```

- [ ] **Step 3: Run the new tests**

```bash
uv run pytest src/tests/test_evolve_evaluator.py::TestPerTargetDispatch -v
```

Expected: 3 PASS.

- [ ] **Step 4: Run the full evolve test suite**

```bash
uv run pytest src/tests/test_evolve_*.py -v
```

Expected: ALL PASS (existing scoutgpt tests + 3 new dispatch tests). Investigate any failures before proceeding.

- [ ] **Step 5: Stage**

```bash
git add src/tests/test_evolve_evaluator.py
```

---

## Block B — Architectural enums on `Football2VecEncoder`

### Task B1: Add three enum fields to `Football2VecConfig` with backward-compatible defaults

**Files:**
- Modify: `src/analytics/football2vec_transformer.py:42-65` (the `Football2VecConfig` dataclass)

- [ ] **Step 1: Add the three enum fields**

Open `src/analytics/football2vec_transformer.py`. Replace the `Football2VecConfig` dataclass (lines 42-65) with:

```python
@dataclass(frozen=True)
class Football2VecConfig:
    """Immutable configuration for the Football2Vec transformer encoder.

    Attributes:
        vocab_size: SPADL 23-type action vocabulary size.
        hidden_dim: Embedding and transformer hidden dimension.
        num_layers: Number of transformer encoder layers.
        num_heads: Number of attention heads.
        dropout: Dropout rate.
        max_seq_len: Maximum sequence length for positional embedding.
        mask_prob: MLM mask probability.
        spatial_mlp_dim: Intermediate dimension for spatial coordinate MLPs.
        pooling_type: How to reduce per-token embeddings to a sequence embedding.
            "mean" (current behaviour), "attention" (learned attention pool),
            or "cls" (prepended CLS token).
        spatial_injection: How spatial coordinates are injected into the token stream.
            "additive" (current behaviour: tok + spatial_x + spatial_y + pos),
            "concat" (concatenate then project back), or "film" (per-channel scale + shift).
        position_embedding: Position encoding scheme.
            "learnable" (current behaviour: nn.Embedding),
            "sinusoidal" (fixed sinusoidal table), or "rope" (rotary, applied in attention).
    """

    vocab_size: int = 23
    hidden_dim: int = 128
    num_layers: int = 4
    num_heads: int = 4
    dropout: float = 0.1
    max_seq_len: int = 512
    mask_prob: float = 0.15
    spatial_mlp_dim: int = 64
    pooling_type: str = "mean"
    spatial_injection: str = "additive"
    position_embedding: str = "learnable"
```

The defaults exactly reproduce the previous behaviour. Existing callers that instantiate `Football2VecConfig()` see no change.

- [ ] **Step 2: Run the existing transformer tests**

```bash
uv run pytest src/tests/test_football2vec_transformer.py -v
```

Expected: ALL existing tests pass — the dataclass changes are additive only.

- [ ] **Step 3: Stage**

```bash
git add src/analytics/football2vec_transformer.py
```

---

### Task B2: Implement `pooling_type` variants

**Files:**
- Modify: `src/analytics/football2vec_transformer.py:108-266` (`Football2VecEncoder`)
- Modify: `src/tests/test_football2vec_transformer.py` (append variant tests)

- [ ] **Step 1: Write failing tests for the three pooling variants**

Append to `src/tests/test_football2vec_transformer.py`:

```python
# ---------------------------------------------------------------------------
# Architectural enum variants (EV1)
# ---------------------------------------------------------------------------

import pytest
import torch

from analytics.football2vec_transformer import Football2VecConfig, Football2VecEncoder


def _dummy_batch(batch_size: int = 2, seq_len: int = 16) -> dict[str, torch.Tensor]:
    """Build a dummy batch for forward-pass testing."""
    return {
        "action_ids": torch.randint(0, 23, (batch_size, seq_len)),
        "x_coords": torch.rand(batch_size, seq_len),
        "y_coords": torch.rand(batch_size, seq_len),
        "attention_mask": torch.ones(batch_size, seq_len, dtype=torch.bool),
    }


@pytest.mark.parametrize("pooling_type", ["mean", "attention", "cls"])
def test_football2vec_encoder_pooling_variants(pooling_type: str) -> None:
    """Encoder forward pass returns (batch, hidden_dim) for every pooling variant."""
    cfg = Football2VecConfig(hidden_dim=32, num_layers=1, num_heads=4, max_seq_len=64, pooling_type=pooling_type)
    model = Football2VecEncoder(cfg)
    model.eval()
    batch = _dummy_batch()
    with torch.no_grad():
        out = model(batch["action_ids"], batch["x_coords"], batch["y_coords"], batch["attention_mask"])
    assert out.shape == (2, 32), f"pooling_type={pooling_type!r} produced shape {out.shape}"
```

- [ ] **Step 2: Run tests, expect failure**

```bash
uv run pytest src/tests/test_football2vec_transformer.py::test_football2vec_encoder_pooling_variants -v
```

Expected: `mean` PASS (current behaviour), `attention` and `cls` FAIL (variants not implemented).

- [ ] **Step 3: Implement the pooling variants**

Open `src/analytics/football2vec_transformer.py`. Modify `Football2VecEncoder.__init__` to add the per-variant modules and `forward` to dispatch on `cfg.pooling_type`.

In `__init__` (after the existing `self.encoder = ...` block, before `self.mlm_head`):

```python
        # Pooling variants (EV1)
        if cfg.pooling_type == "attention":
            self.pool_attn = nn.Linear(cfg.hidden_dim, 1)
        elif cfg.pooling_type == "cls":
            self.cls_token = nn.Parameter(torch.zeros(1, 1, cfg.hidden_dim))
            nn.init.normal_(self.cls_token, std=0.02)
        elif cfg.pooling_type != "mean":
            msg = f"unknown pooling_type {cfg.pooling_type!r}; expected mean|attention|cls"
            raise ValueError(msg)
```

Modify `_encode` to prepend the CLS token when `cls` is used. Replace the `_encode` method body:

```python
    def _encode(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Run the transformer encoder on embedded inputs."""
        embedded = self._embed(action_ids, x_coords, y_coords)

        if self.config.pooling_type == "cls":
            batch_size = embedded.size(0)
            cls = self.cls_token.expand(batch_size, -1, -1)  # (batch, 1, hidden_dim)
            embedded = torch.cat([cls, embedded], dim=1)
            if attention_mask is not None:
                cls_mask = torch.ones(batch_size, 1, dtype=torch.bool, device=attention_mask.device)
                attention_mask = torch.cat([cls_mask, attention_mask], dim=1)

        src_key_padding_mask: torch.Tensor | None = None
        if attention_mask is not None:
            src_key_padding_mask = ~attention_mask

        return self.encoder(embedded, src_key_padding_mask=src_key_padding_mask)
```

Replace the `forward` method body:

```python
    def forward(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Compute sequence-level embedding via the configured pooling strategy."""
        encoded = self._encode(action_ids, x_coords, y_coords, attention_mask)

        # Adjust attention_mask for cls (prepended a token); for other variants, mask is unchanged.
        pooling_mask = attention_mask
        if self.config.pooling_type == "cls" and attention_mask is not None:
            batch_size = attention_mask.size(0)
            cls_mask = torch.ones(batch_size, 1, dtype=torch.bool, device=attention_mask.device)
            pooling_mask = torch.cat([cls_mask, attention_mask], dim=1)

        if self.config.pooling_type == "cls":
            return encoded[:, 0, :]

        if self.config.pooling_type == "attention":
            scores = self.pool_attn(encoded).squeeze(-1)  # (batch, seq_len)
            if pooling_mask is not None:
                scores = scores.masked_fill(~pooling_mask, float("-inf"))
            weights = torch.softmax(scores, dim=1).unsqueeze(-1)  # (batch, seq_len, 1)
            return (encoded * weights).sum(dim=1)

        # Default: mean pooling
        if pooling_mask is not None:
            mask_expanded = pooling_mask.unsqueeze(-1).float()
            summed = (encoded * mask_expanded).sum(dim=1)
            lengths = mask_expanded.sum(dim=1).clamp(min=1)
            return summed / lengths
        return encoded.mean(dim=1)
```

The `mlm_forward` method needs no changes — the MLM head consumes per-token outputs from the encoder, and for the `cls` variant, the prepended CLS token is excluded from MLM loss because `Football2VecDataset.__getitem__` does not generate labels at the prepended position. *Wait — actually MLM labels match token positions one-to-one with `action_ids`. The CLS prepending happens inside `_encode`, so `mlm_forward` returns `(batch, seq_len+1, vocab_size)` for cls but the trainer expects `(batch, seq_len, vocab_size)`.* Slice the CLS position out before the MLM head:

Replace `mlm_forward`:

```python
    def mlm_forward(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
        attention_mask: torch.Tensor | None = None,
    ) -> torch.Tensor:
        """Compute per-token MLM logits for masked language modeling."""
        encoded = self._encode(action_ids, x_coords, y_coords, attention_mask)
        # For CLS variant: drop the prepended CLS position so logits align with input action_ids.
        if self.config.pooling_type == "cls":
            encoded = encoded[:, 1:, :]
        return self.mlm_head(encoded)
```

- [ ] **Step 4: Run tests, expect pass**

```bash
uv run pytest src/tests/test_football2vec_transformer.py::test_football2vec_encoder_pooling_variants -v
```

Expected: 3 PASS (`mean`, `attention`, `cls`).

- [ ] **Step 5: Stage**

```bash
git add src/analytics/football2vec_transformer.py src/tests/test_football2vec_transformer.py
```

---

### Task B3: Implement `spatial_injection` variants

**Files:**
- Modify: `src/analytics/football2vec_transformer.py` (`Football2VecEncoder.__init__` + `_embed`)
- Modify: `src/tests/test_football2vec_transformer.py` (append variant test)

- [ ] **Step 1: Write failing test**

Append to `src/tests/test_football2vec_transformer.py`:

```python
@pytest.mark.parametrize("spatial_injection", ["additive", "concat", "film"])
def test_football2vec_encoder_spatial_variants(spatial_injection: str) -> None:
    """Encoder forward pass works for every spatial_injection variant."""
    cfg = Football2VecConfig(
        hidden_dim=32, num_layers=1, num_heads=4, max_seq_len=64,
        spatial_mlp_dim=8, spatial_injection=spatial_injection,
    )
    model = Football2VecEncoder(cfg)
    model.eval()
    batch = _dummy_batch()
    with torch.no_grad():
        out = model(batch["action_ids"], batch["x_coords"], batch["y_coords"], batch["attention_mask"])
    assert out.shape == (2, 32), f"spatial_injection={spatial_injection!r} produced shape {out.shape}"


def test_football2vec_encoder_concat_guard() -> None:
    """concat injection rejects spatial_mlp_dim > hidden_dim/2 (memory guard)."""
    cfg = Football2VecConfig(
        hidden_dim=32, num_layers=1, num_heads=4, max_seq_len=64,
        spatial_mlp_dim=20, spatial_injection="concat",
    )
    with pytest.raises(ValueError, match="spatial_mlp_dim"):
        Football2VecEncoder(cfg)
```

- [ ] **Step 2: Run tests, expect failure**

```bash
uv run pytest src/tests/test_football2vec_transformer.py::test_football2vec_encoder_spatial_variants src/tests/test_football2vec_transformer.py::test_football2vec_encoder_concat_guard -v
```

Expected: `additive` PASS; `concat` and `film` FAIL; `concat_guard` FAIL (ValueError not raised).

- [ ] **Step 3: Implement `spatial_injection` variants in `__init__` and `_embed`**

In `Football2VecEncoder.__init__`, after the existing `self.spatial_y = SpatialMLP(...)` line, add:

```python
        # Spatial injection variants (EV1)
        if cfg.spatial_injection == "concat":
            if cfg.spatial_mlp_dim > cfg.hidden_dim // 2:
                msg = (
                    f"spatial_mlp_dim={cfg.spatial_mlp_dim} too large for concat "
                    f"injection (must be <= hidden_dim/2 = {cfg.hidden_dim // 2})"
                )
                raise ValueError(msg)
            # After concat we have 3 × hidden_dim features; project back.
            self.spatial_concat_proj = nn.Linear(3 * cfg.hidden_dim, cfg.hidden_dim)
        elif cfg.spatial_injection == "film":
            # Per-channel scale + shift from spatial_x and spatial_y.
            self.film_scale = nn.Linear(2 * cfg.hidden_dim, cfg.hidden_dim)
            self.film_shift = nn.Linear(2 * cfg.hidden_dim, cfg.hidden_dim)
        elif cfg.spatial_injection != "additive":
            msg = f"unknown spatial_injection {cfg.spatial_injection!r}; expected additive|concat|film"
            raise ValueError(msg)
```

Replace `_embed`:

```python
    def _embed(
        self,
        action_ids: torch.Tensor,
        x_coords: torch.Tensor,
        y_coords: torch.Tensor,
    ) -> torch.Tensor:
        """Compute combined embedding per the configured spatial_injection strategy."""
        seq_len = action_ids.size(1)

        tok_emb = self.token_embedding(action_ids)
        x_emb = self.spatial_x(x_coords)
        y_emb = self.spatial_y(y_coords)

        # Position embedding (variant-dependent — handled in B4; default learnable here).
        pos_emb = self.position_embedding(self._pos_ids[:, :seq_len])  # type: ignore[index]

        if self.config.spatial_injection == "concat":
            stacked = torch.cat([tok_emb, x_emb, y_emb], dim=-1)
            combined = self.spatial_concat_proj(stacked) + pos_emb
        elif self.config.spatial_injection == "film":
            spatial = torch.cat([x_emb, y_emb], dim=-1)
            scale = self.film_scale(spatial)
            shift = self.film_shift(spatial)
            combined = tok_emb * (1.0 + scale) + shift + pos_emb
        else:  # additive
            combined = tok_emb + x_emb + y_emb + pos_emb

        return self.embedding_dropout(combined)
```

- [ ] **Step 4: Run tests, expect pass**

```bash
uv run pytest src/tests/test_football2vec_transformer.py::test_football2vec_encoder_spatial_variants src/tests/test_football2vec_transformer.py::test_football2vec_encoder_concat_guard -v
```

Expected: 3 PASS (additive/concat/film) + 1 PASS (concat_guard) = 4 PASS.

- [ ] **Step 5: Stage**

```bash
git add src/analytics/football2vec_transformer.py src/tests/test_football2vec_transformer.py
```

---

### Task B4: Implement `position_embedding` variants

**Files:**
- Modify: `src/analytics/football2vec_transformer.py` (`Football2VecEncoder.__init__`, `_embed`, possibly `_encode` for rope)
- Modify: `src/tests/test_football2vec_transformer.py` (append variant test)

- [ ] **Step 1: Write failing test**

Append to `src/tests/test_football2vec_transformer.py`:

```python
@pytest.mark.parametrize("position_embedding", ["learnable", "sinusoidal", "rope"])
def test_football2vec_encoder_position_variants(position_embedding: str) -> None:
    """Encoder forward pass works for every position_embedding variant."""
    cfg = Football2VecConfig(
        hidden_dim=32, num_layers=1, num_heads=4, max_seq_len=64,
        position_embedding=position_embedding,
    )
    model = Football2VecEncoder(cfg)
    model.eval()
    batch = _dummy_batch()
    with torch.no_grad():
        out = model(batch["action_ids"], batch["x_coords"], batch["y_coords"], batch["attention_mask"])
    assert out.shape == (2, 32), f"position_embedding={position_embedding!r} produced shape {out.shape}"
```

- [ ] **Step 2: Run tests, expect failure**

```bash
uv run pytest src/tests/test_football2vec_transformer.py::test_football2vec_encoder_position_variants -v
```

Expected: `learnable` PASS; `sinusoidal` and `rope` FAIL.

- [ ] **Step 3: Implement variants**

In `Football2VecEncoder.__init__`, replace the existing `self.position_embedding = nn.Embedding(cfg.max_seq_len, cfg.hidden_dim)` with:

```python
        # Position embedding variants (EV1)
        if cfg.position_embedding == "learnable":
            self.position_embedding = nn.Embedding(cfg.max_seq_len, cfg.hidden_dim)
        elif cfg.position_embedding == "sinusoidal":
            # Fixed sinusoidal table; not a parameter.
            pe = torch.zeros(cfg.max_seq_len, cfg.hidden_dim)
            position = torch.arange(0, cfg.max_seq_len, dtype=torch.float).unsqueeze(1)
            div_term = torch.exp(
                torch.arange(0, cfg.hidden_dim, 2, dtype=torch.float)
                * (-math.log(10000.0) / cfg.hidden_dim)
            )
            pe[:, 0::2] = torch.sin(position * div_term)
            pe[:, 1::2] = torch.cos(position * div_term)
            self.register_buffer("_sin_pos", pe.unsqueeze(0))  # (1, max_seq_len, hidden_dim)
        elif cfg.position_embedding == "rope":
            # RoPE applied inside attention via rotary frequencies; precompute sin/cos.
            half = cfg.hidden_dim // cfg.num_heads // 2
            inv_freq = 1.0 / (10000.0 ** (torch.arange(0, half, dtype=torch.float) / half))
            t = torch.arange(cfg.max_seq_len, dtype=torch.float)
            freqs = torch.einsum("i,j->ij", t, inv_freq)  # (max_seq_len, half)
            self.register_buffer("_rope_cos", freqs.cos())
            self.register_buffer("_rope_sin", freqs.sin())
        else:
            msg = f"unknown position_embedding {cfg.position_embedding!r}; expected learnable|sinusoidal|rope"
            raise ValueError(msg)
```

Add `import math` at the top of the file if not already imported (check imports section — currently imports are `from dataclasses import dataclass`, `from typing import cast`, `import torch`, `import torch.nn as nn`, `from torch.autograd import Function`). Add `import math` after `from typing import cast`.

Modify `_embed` to handle the three variants. Replace the position-embedding line in `_embed`:

```python
        # Position embedding (variant-dependent)
        if self.config.position_embedding == "learnable":
            pos_emb = self.position_embedding(self._pos_ids[:, :seq_len])  # type: ignore[index]
        elif self.config.position_embedding == "sinusoidal":
            pos_emb = self._sin_pos[:, :seq_len, :]  # type: ignore[index]
        else:  # rope — applied inside attention; no additive position term in _embed
            pos_emb = torch.zeros_like(tok_emb)
```

For `rope`, the rotary frequencies need to be applied inside the attention computation. **Implementation simplification:** for EV1 we approximate RoPE by adding a sinusoidal-like signal to the queries/keys via a pre-encoder modulation. To avoid invasive changes to `nn.TransformerEncoderLayer`, use a lightweight rotation: multiply the token embeddings element-wise by `cos(position)` and add a `sin(position)` shift before passing to the encoder.

For correctness without monkey-patching `nn.MultiheadAttention`, replace the `else: # rope` branch above with:

```python
        else:  # rope — apply rotary modulation to the token embedding directly
            cos_part = self._rope_cos[:seq_len, :]  # (seq_len, half)
            sin_part = self._rope_sin[:seq_len, :]  # (seq_len, half)
            # Tile to hidden_dim by repeating each frequency twice (sin/cos pair).
            cos_full = cos_part.repeat_interleave(2, dim=-1)[: , : self.config.hidden_dim].unsqueeze(0)
            sin_full = sin_part.repeat_interleave(2, dim=-1)[: , : self.config.hidden_dim].unsqueeze(0)
            pos_emb = sin_full + (cos_full - 1.0) * 0.0  # additive sin component as positional signal
```

This is an approximation, not full RoPE. Document this in a comment in the file:

```python
            # NOTE: full RoPE requires modifying attention's q/k computation.
            # For EV1 we use a rotary-flavoured additive signal as a positional cue
            # without monkey-patching nn.MultiheadAttention. If RoPE wins the sweep,
            # promote to a proper implementation in a follow-up cycle.
```

- [ ] **Step 4: Run tests, expect pass**

```bash
uv run pytest src/tests/test_football2vec_transformer.py::test_football2vec_encoder_position_variants -v
```

Expected: 3 PASS (learnable/sinusoidal/rope).

- [ ] **Step 5: Stage**

```bash
git add src/analytics/football2vec_transformer.py src/tests/test_football2vec_transformer.py
```

---

### Task B5: Backward-compatibility regression test

**Files:**
- Modify: `src/tests/test_football2vec_transformer.py` (append regression test)

- [ ] **Step 1: Write the test**

Append to `src/tests/test_football2vec_transformer.py`:

```python
def test_football2vec_encoder_backward_compat() -> None:
    """Default Football2VecConfig() produces the same module structure as before EV1."""
    cfg = Football2VecConfig()
    model = Football2VecEncoder(cfg)

    # Defaults must exactly preserve the pre-EV1 architecture.
    assert cfg.pooling_type == "mean"
    assert cfg.spatial_injection == "additive"
    assert cfg.position_embedding == "learnable"

    # Module names that existed before EV1 must still exist.
    expected_modules = {
        "token_embedding", "spatial_x", "spatial_y", "position_embedding",
        "embedding_dropout", "encoder", "mlm_head",
    }
    actual_modules = {name for name, _ in model.named_children()}
    missing = expected_modules - actual_modules
    assert not missing, f"backward-compat regression: missing modules {missing}"

    # No EV1-specific modules should be registered for the default config.
    forbidden_modules = {"pool_attn", "spatial_concat_proj", "film_scale", "film_shift"}
    extra = forbidden_modules & actual_modules
    assert not extra, f"backward-compat regression: unexpected modules {extra}"

    # Forward pass shape unchanged.
    batch = _dummy_batch()
    model.eval()
    with torch.no_grad():
        out = model(batch["action_ids"], batch["x_coords"], batch["y_coords"], batch["attention_mask"])
    assert out.shape == (2, 128)
```

- [ ] **Step 2: Run the test**

```bash
uv run pytest src/tests/test_football2vec_transformer.py::test_football2vec_encoder_backward_compat -v
```

Expected: PASS. If FAIL, check that the conditional `__init__` blocks in B2-B4 only register variant modules under the non-default branches.

- [ ] **Step 3: Run the full football2vec transformer + benchmark suite**

```bash
uv run pytest src/tests/test_football2vec_transformer.py src/tests/test_benchmarks.py -v -k "football2vec"
```

Expected: all PASS — including the existing pytest-benchmark tests which exercise the default config.

- [ ] **Step 4: Stage**

```bash
git add src/tests/test_football2vec_transformer.py
```

---

## Block C — Build the new `football2vec` evolve target

### Task C1: Create `__init__.py` files for the new target tree

**Files:**
- Create: `src/evolve/targets/football2vec/__init__.py`
- Create: `src/evolve/targets/football2vec/seed_programs/__init__.py`

- [ ] **Step 1: Create the package init**

Create `src/evolve/targets/football2vec/__init__.py`:

```python
"""Football2Vec target — Level 1 hyperparameter + architectural-enum search."""

# No VALIDATION_PROFILE — Level 2 (code evolution) is out of scope for EV1.
# EV2 may add one when stage-2 adversarial code evolution is wired up.
```

Create `src/evolve/targets/football2vec/seed_programs/__init__.py`:

```python
"""Football2Vec seed programs — initial population for the Evolve loop."""
```

- [ ] **Step 2: Stage**

```bash
git add src/evolve/targets/football2vec/__init__.py src/evolve/targets/football2vec/seed_programs/__init__.py
```

---

### Task C2: Write the search-space schema

**Files:**
- Create: `src/evolve/targets/football2vec/search_space.py`

- [ ] **Step 1: Write the schema**

```python
"""Football2Vec search-space schema for the Evolve engine (Level 1)."""

from __future__ import annotations

import logging
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator, model_validator

_log = logging.getLogger(__name__)

_BOUNDS: dict[str, tuple[float, float]] = {
    "hidden_dim": (64, 256),
    "num_layers": (2, 8),
    "num_heads": (2, 8),
    "dropout": (0.0, 0.4),
    "mask_prob": (0.10, 0.30),
    "spatial_mlp_dim": (16, 128),
    "learning_rate": (1e-5, 1e-3),
    "batch_size": (64, 512),
}


class CandidateConfig(BaseModel):
    """Typed schema for Football2Vec stage-1 candidate configs.

    Defines all known fields with defaults so that typos in key names are
    surfaced: an unknown key like ``"hiddem_dim"`` goes into
    ``__pydantic_extra__`` and triggers a logged warning.
    """

    model_config = ConfigDict(extra="allow")

    # Architecture (scalars)
    hidden_dim: int = 128
    num_layers: int = 4
    num_heads: int = 4
    dropout: float = 0.1
    mask_prob: float = 0.15
    spatial_mlp_dim: int = 64

    # Architecture (enums)
    pooling_type: Literal["mean", "attention", "cls"] = "mean"
    spatial_injection: Literal["additive", "concat", "film"] = "additive"
    position_embedding: Literal["learnable", "sinusoidal", "rope"] = "learnable"

    # Training hyperparams
    learning_rate: float = 1e-4
    batch_size: int = 256
    dataset: str = "luxury-lakehouse/football2vec-training-data"

    @field_validator("dataset")
    @classmethod
    def _validate_dataset_prefix(cls, v: str) -> str:
        if not v.startswith("luxury-lakehouse/"):
            msg = f"dataset must be a luxury-lakehouse/ HF repo, got '{v}'"
            raise ValueError(msg)
        return v

    @model_validator(mode="after")
    def _validate_search_space(self) -> CandidateConfig:
        for key, (lo, hi) in _BOUNDS.items():
            val = getattr(self, key, None)
            if val is not None and not (lo <= val <= hi):
                msg = f"{key}={val!r} not in [{lo}, {hi}]"
                raise ValueError(msg)
        if self.hidden_dim % self.num_heads != 0:
            msg = f"hidden_dim={self.hidden_dim} not divisible by num_heads={self.num_heads}"
            raise ValueError(msg)
        if self.spatial_injection == "concat" and self.spatial_mlp_dim > self.hidden_dim // 2:
            msg = (
                f"spatial_injection='concat' requires spatial_mlp_dim <= hidden_dim/2, "
                f"got {self.spatial_mlp_dim} > {self.hidden_dim // 2}"
            )
            raise ValueError(msg)
        if self.__pydantic_extra__:
            _log.warning(
                "Candidate config has unrecognised keys (possible typos?): %s",
                sorted(self.__pydantic_extra__),
            )
        return self


def validate_candidate(config: dict[str, Any]) -> bool:
    """Validate a Football2Vec candidate config. Returns True on pass, False on reject."""
    try:
        CandidateConfig(**config)
    except (ValidationError, ValueError) as exc:
        _log.warning("Search space rejection: %s", exc)
        return False
    return True
```

- [ ] **Step 2: Stage (no test yet — Task D2 covers tests)**

```bash
git add src/evolve/targets/football2vec/search_space.py
```

---

### Task C3: Write the evaluator with dataset cache and self-contained MLM loop

**Files:**
- Create: `src/evolve/targets/football2vec/evaluator.py`

- [ ] **Step 1: Write the evaluator**

```python
"""Football2Vec target evaluator — trains a candidate from config, returns fitness metrics.

Self-contained MLM training loop (no MLflow, no HF Hub publishing, no checkpoint writing).
Module-level dataset cache shared across all candidate evaluations in this process.
"""

from __future__ import annotations

import logging
import os
import threading
import time
import traceback
from dataclasses import dataclass
from typing import Any

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level dataset cache — load once, reuse across all candidates.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _CachedData:
    """Immutable container for parsed action sequences + train/val indices."""

    aids: list[list[int]]
    xs: list[list[float]]
    ys: list[list[float]]
    train_indices: list[int]
    val_indices: list[int]


_dataset_cache: dict[str, _CachedData] = {}
_cache_lock = threading.Lock()


def _load_or_cache(dataset_repo: str, hf_token: str) -> _CachedData:
    """Load and parse the dataset, caching by repo name."""
    with _cache_lock:
        if dataset_repo in _dataset_cache:
            _log.info("Using cached dataset for %s", dataset_repo)
            return _dataset_cache[dataset_repo]

        from ingestion.football2vec_v2_training import (
            load_training_data,
            parse_actions,
            stratified_split,
        )

        data, _commit = load_training_data(hf_token, dataset_repo)
        aids_all, xs_all, ys_all = parse_actions(data["actions"])
        train_df, val_df, _test_df = stratified_split(data)
        ti = train_df.index.tolist()
        vi = val_df.index.tolist()
        _log.info("Dataset split: train=%d val=%d", len(ti), len(vi))

        cached = _CachedData(
            aids=aids_all,
            xs=xs_all,
            ys=ys_all,
            train_indices=ti,
            val_indices=vi,
        )
        _dataset_cache[dataset_repo] = cached
        return cached


# ---------------------------------------------------------------------------
# Self-contained MLM train + eval loop (no checkpoints, no MLflow, no publishing)
# ---------------------------------------------------------------------------


def _train_eval_one_candidate(
    config_obj: Any,  # Football2VecConfig
    train_ds: Any,  # Football2VecDataset
    val_ds: Any,
    device: Any,  # torch.device
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
) -> dict[str, Any]:
    """Run one candidate's training and return metrics."""
    import torch
    import torch.nn as nn
    from torch.utils.data import DataLoader

    from analytics.football2vec_transformer import Football2VecEncoder
    from ingestion.football2vec_v2_training import (
        VOCAB_SIZE,
        WARMUP_FRACTION,
        WEIGHT_DECAY,
        get_cosine_schedule_with_warmup,
    )

    model = Football2VecEncoder(config_obj).to(device)
    # Expand vocab embedding to include MASK + PAD tokens (matches train_football2vec_v2.py:107-112).
    expanded = nn.Embedding(VOCAB_SIZE + 2, config_obj.hidden_dim).to(device)
    with torch.no_grad():
        expanded.weight[:VOCAB_SIZE] = model.token_embedding.weight
    model.token_embedding = expanded

    tl = DataLoader(train_ds, batch_size=batch_size, shuffle=True, num_workers=2, pin_memory=device.type == "cuda")
    vl = DataLoader(val_ds, batch_size=batch_size, shuffle=False, num_workers=2, pin_memory=device.type == "cuda")
    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=WEIGHT_DECAY)
    total_steps = max(1, len(tl) * epochs)
    scheduler = get_cosine_schedule_with_warmup(optimizer, int(total_steps * WARMUP_FRACTION), total_steps)
    criterion = nn.CrossEntropyLoss(ignore_index=-100)

    best_val_loss = float("inf")
    best_val_acc = 0.0
    patience_ctr = 0
    epochs_run = 0

    for epoch in range(epochs):
        model.train()
        for batch in tl:
            optimizer.zero_grad()
            logits = model.mlm_forward(
                batch["action_ids"].to(device),
                batch["x_coords"].to(device),
                batch["y_coords"].to(device),
                batch["attention_mask"].to(device),
            )
            loss = criterion(logits.view(-1, config_obj.vocab_size), batch["labels"].to(device).view(-1))
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            scheduler.step()

        # Validation
        model.eval()
        v_loss = 0.0
        correct = 0
        masked = 0
        nb = 0
        with torch.no_grad():
            for b in vl:
                logits = model.mlm_forward(
                    b["action_ids"].to(device),
                    b["x_coords"].to(device),
                    b["y_coords"].to(device),
                    b["attention_mask"].to(device),
                )
                labels = b["labels"].to(device)
                v_loss += criterion(logits.view(-1, config_obj.vocab_size), labels.view(-1)).item()
                nb += 1
                mask = labels != -100
                if mask.any():
                    correct += (logits.argmax(dim=-1)[mask] == labels[mask]).sum().item()
                    masked += mask.sum().item()
        v_loss /= max(nb, 1)
        v_acc = correct / max(masked, 1)
        epochs_run = epoch + 1
        _log.info("epoch %d/%d — val_loss=%.4f val_acc=%.4f", epochs_run, epochs, v_loss, v_acc)

        if v_loss < best_val_loss:
            best_val_loss = v_loss
            best_val_acc = v_acc
            patience_ctr = 0
        else:
            patience_ctr += 1
            if patience_ctr >= patience:
                _log.info("early stopping at epoch %d", epochs_run)
                break

    param_count = sum(p.numel() for p in model.parameters())
    return {
        "val_accuracy": best_val_acc,
        "val_loss": best_val_loss,
        "param_count": float(param_count),
        "epochs_trained": float(epochs_run),
    }


# ---------------------------------------------------------------------------
# Public API — train_and_evaluate (called by ComputeBackend implementations)
# ---------------------------------------------------------------------------


def train_and_evaluate(
    candidate_config: dict[str, Any],
    device: str,
    epochs: int,
    seed: int,
    program_path: str | None = None,  # noqa: ARG001 — Level 2 not used for football2vec
) -> dict[str, Any]:
    """Build model from candidate config, train, return evaluation metrics.

    Returns:
        Dict of scalar fitness metrics (all float-castable):
        ``val_accuracy``, ``val_loss``, ``param_count``, ``training_time_seconds``,
        ``epochs_trained``. On error, returns ``{"val_accuracy": 0.0, "error": 1.0,
        "_error_text": <traceback>}``.
    """
    import torch

    from analytics.football2vec_transformer import Football2VecConfig
    from ingestion.football2vec_v2_training import Football2VecDataset

    torch_device = torch.device(device)
    start = time.monotonic()
    _log.info("Football2Vec candidate starting (device=%s, epochs=%d, seed=%d)", device, epochs, seed)

    # Reproducibility (best-effort — DataLoader workers may still introduce variance).
    torch.manual_seed(seed)

    # Extract training hyperparams (not part of Football2VecConfig)
    lr: float = candidate_config.get("learning_rate", 1e-4)
    batch_size: int = candidate_config.get("batch_size", 256)

    # Build Football2VecConfig from candidate config (architecture keys only)
    config_keys = {
        "vocab_size", "hidden_dim", "num_layers", "num_heads", "dropout",
        "max_seq_len", "mask_prob", "spatial_mlp_dim",
        "pooling_type", "spatial_injection", "position_embedding",
    }
    model_kwargs = {k: v for k, v in candidate_config.items() if k in config_keys}
    config_obj = Football2VecConfig(**model_kwargs)

    # Load dataset (cached across candidates)
    hf_token = os.environ.get("HF_TOKEN", "")
    dataset_repo: str = candidate_config.get("dataset", "luxury-lakehouse/football2vec-training-data")
    cached = _load_or_cache(dataset_repo, hf_token)

    # Build per-split Football2VecDataset using mask_prob from candidate
    mask_prob: float = candidate_config.get("mask_prob", 0.15)
    train_ds = Football2VecDataset(
        [cached.aids[i] for i in cached.train_indices],
        [cached.xs[i] for i in cached.train_indices],
        [cached.ys[i] for i in cached.train_indices],
        max_seq_len=config_obj.max_seq_len,
        mask_prob=mask_prob,
        mlm=True,
    )
    val_ds = Football2VecDataset(
        [cached.aids[i] for i in cached.val_indices],
        [cached.xs[i] for i in cached.val_indices],
        [cached.ys[i] for i in cached.val_indices],
        max_seq_len=config_obj.max_seq_len,
        mask_prob=mask_prob,
        mlm=True,
    )

    try:
        metrics = _train_eval_one_candidate(
            config_obj=config_obj,
            train_ds=train_ds,
            val_ds=val_ds,
            device=torch_device,
            epochs=epochs,
            batch_size=batch_size,
            lr=lr,
            patience=max(2, epochs // 2),
        )
        metrics["training_time_seconds"] = time.monotonic() - start
        _log.info(
            "Football2Vec candidate done: val_acc=%.4f val_loss=%.4f time=%.1fs",
            metrics["val_accuracy"], metrics["val_loss"], metrics["training_time_seconds"],
        )
    except (torch.cuda.OutOfMemoryError, RuntimeError) as exc:
        _log.warning("Football2Vec candidate failed (OOM or runtime error), score 0: %s", exc)
        metrics = {
            "val_accuracy": 0.0,
            "val_loss": float("inf"),
            "param_count": 0.0,
            "epochs_trained": 0.0,
            "training_time_seconds": time.monotonic() - start,
            "error": 1.0,
            "_error_text": traceback.format_exc(),
        }

    # GPU memory hygiene
    if torch_device.type == "cuda":
        torch.cuda.empty_cache()

    return metrics


__all__ = ["train_and_evaluate"]
```

- [ ] **Step 2: Stage**

```bash
git add src/evolve/targets/football2vec/evaluator.py
```

---

### Task C4: Write the seven seed programs

**Files:**
- Create: `src/evolve/targets/football2vec/seed_programs/baseline.py`
- Create: `src/evolve/targets/football2vec/seed_programs/wider.py`
- Create: `src/evolve/targets/football2vec/seed_programs/deeper.py`
- Create: `src/evolve/targets/football2vec/seed_programs/heavier_mask.py`
- Create: `src/evolve/targets/football2vec/seed_programs/attention_pool.py`
- Create: `src/evolve/targets/football2vec/seed_programs/film_spatial.py`
- Create: `src/evolve/targets/football2vec/seed_programs/sinusoidal_pos.py`

- [ ] **Step 1: Create baseline.py**

```python
"""Seed 1: Baseline — current Football2VecConfig defaults (56.9% MLM accuracy benchmark)."""

config = {
    "hidden_dim": 128,
    "num_layers": 4,
    "num_heads": 4,
    "dropout": 0.1,
    "mask_prob": 0.15,
    "spatial_mlp_dim": 64,
    "pooling_type": "mean",
    "spatial_injection": "additive",
    "position_embedding": "learnable",
    "learning_rate": 1e-4,
    "batch_size": 256,
}
```

- [ ] **Step 2: Create wider.py**

```python
"""Seed 2: Wider — increased hidden_dim and num_heads."""

config = {
    "hidden_dim": 192,
    "num_layers": 4,
    "num_heads": 6,
    "dropout": 0.1,
    "mask_prob": 0.15,
    "spatial_mlp_dim": 64,
    "pooling_type": "mean",
    "spatial_injection": "additive",
    "position_embedding": "learnable",
    "learning_rate": 1e-4,
    "batch_size": 256,
}
```

- [ ] **Step 3: Create deeper.py**

```python
"""Seed 3: Deeper — more layers at the baseline width."""

config = {
    "hidden_dim": 128,
    "num_layers": 6,
    "num_heads": 4,
    "dropout": 0.1,
    "mask_prob": 0.15,
    "spatial_mlp_dim": 64,
    "pooling_type": "mean",
    "spatial_injection": "additive",
    "position_embedding": "learnable",
    "learning_rate": 1e-4,
    "batch_size": 256,
}
```

- [ ] **Step 4: Create heavier_mask.py**

```python
"""Seed 4: Heavier mask — higher mask_prob and learning rate."""

config = {
    "hidden_dim": 128,
    "num_layers": 4,
    "num_heads": 4,
    "dropout": 0.1,
    "mask_prob": 0.20,
    "spatial_mlp_dim": 64,
    "pooling_type": "mean",
    "spatial_injection": "additive",
    "position_embedding": "learnable",
    "learning_rate": 2e-4,
    "batch_size": 256,
}
```

- [ ] **Step 5: Create attention_pool.py**

```python
"""Seed 5: Attention pooling instead of mean."""

config = {
    "hidden_dim": 128,
    "num_layers": 4,
    "num_heads": 4,
    "dropout": 0.1,
    "mask_prob": 0.15,
    "spatial_mlp_dim": 64,
    "pooling_type": "attention",
    "spatial_injection": "additive",
    "position_embedding": "learnable",
    "learning_rate": 1e-4,
    "batch_size": 256,
}
```

- [ ] **Step 6: Create film_spatial.py**

```python
"""Seed 6: FiLM spatial injection (Perez et al. 2018)."""

config = {
    "hidden_dim": 128,
    "num_layers": 4,
    "num_heads": 4,
    "dropout": 0.1,
    "mask_prob": 0.15,
    "spatial_mlp_dim": 64,
    "pooling_type": "mean",
    "spatial_injection": "film",
    "position_embedding": "learnable",
    "learning_rate": 1e-4,
    "batch_size": 256,
}
```

- [ ] **Step 7: Create sinusoidal_pos.py**

```python
"""Seed 7: Fixed sinusoidal position embedding (no learnable position params)."""

config = {
    "hidden_dim": 128,
    "num_layers": 4,
    "num_heads": 4,
    "dropout": 0.1,
    "mask_prob": 0.15,
    "spatial_mlp_dim": 64,
    "pooling_type": "mean",
    "spatial_injection": "additive",
    "position_embedding": "sinusoidal",
    "learning_rate": 1e-4,
    "batch_size": 256,
}
```

- [ ] **Step 8: Stage**

```bash
git add src/evolve/targets/football2vec/seed_programs/
```

---

### Task C5: Write the LLM mutation prompt

**Files:**
- Create: `src/evolve/targets/football2vec/prompts/system_message.txt`

- [ ] **Step 1: Create the prompt directory and file**

Create `src/evolve/targets/football2vec/prompts/system_message.txt`:

```
You are an expert deep learning researcher improving a Football2Vec v2 stage-1 (MLM pre-training) configuration.
Your goal is to maximize the FITNESS SCORE (val_accuracy on held-out masked-action prediction) while exploring diverse solutions.
The system maintains a collection of diverse programs - both high fitness AND diversity are valuable.

# Search Space Constraints

You are modifying a `config` dict that controls the model architecture and training.
The following rules are HARD CONSTRAINTS — violations are rejected instantly without evaluation.

## Valid config keys and ranges

| Key                  | Type    | Range / Values                                   |
|----------------------|---------|--------------------------------------------------|
| hidden_dim           | int     | 64 to 256 (must be divisible by num_heads)       |
| num_layers           | int     | 2 to 8                                           |
| num_heads            | int     | 2 to 8 (must divide hidden_dim)                  |
| dropout              | float   | 0.0 to 0.4                                       |
| mask_prob            | float   | 0.10 to 0.30                                     |
| spatial_mlp_dim      | int     | 16 to 128                                        |
| pooling_type         | string  | ONLY: "mean", "attention", "cls"                 |
| spatial_injection    | string  | ONLY: "additive", "concat", "film"               |
| position_embedding   | string  | ONLY: "learnable", "sinusoidal", "rope"          |
| learning_rate        | float   | 1e-5 to 1e-3 (mutate on log scale: 1e-4 -> 3e-4) |
| batch_size           | int     | 64 to 512 (powers of 2 recommended)              |
| dataset              | string  | must start with "luxury-lakehouse/"              |

## Rules

1. Do NOT invent new config keys. Only the keys listed above are valid. Adding unknown keys (e.g. "weight_decay", "warmup_steps") will trigger a warning and be ignored.
2. Do NOT invent new enum values. Only the listed values are implemented. Values like "max" for pooling_type, "cross_attention" for spatial_injection, or "alibi" for position_embedding will be rejected.
3. hidden_dim MUST be divisible by num_heads. Safe combinations: 64/4, 128/4, 128/8, 192/6, 256/8.
4. If spatial_injection="concat", spatial_mlp_dim MUST be <= hidden_dim/2 (memory guard). For hidden_dim=128 that means spatial_mlp_dim <= 64.
5. Keep changes targeted. Modify 1-3 config values at a time to isolate which changes improve fitness.
6. learning_rate is best mutated on log scale (e.g. 1e-4 -> 3e-4 -> 1e-3, not 1e-4 -> 1.1e-4).

## Tips

- The baseline config (hidden_dim=128, num_layers=4, num_heads=4, all enum defaults) reaches 56.9% val_accuracy at 15 epochs on full data. Your goal is to beat that within the 5-epoch evolve budget.
- The architectural enums (pooling_type, spatial_injection, position_embedding) introduce categorical structure — mutating one enum at a time gives clearer signal than changing multiple.
- Larger hidden_dim and more layers cost wall-clock per candidate but may unlock higher accuracy. The full budget is fixed; expensive configs reduce how many candidates the search can evaluate.
```

- [ ] **Step 2: Stage**

```bash
git add src/evolve/targets/football2vec/prompts/system_message.txt
```

---

### Task C6: Write the EvolveConfig YAML

**Files:**
- Create: `src/evolve/targets/football2vec/config.yaml`

- [ ] **Step 1: Create the config**

```yaml
target: football2vec
description: "Evolve Football2Vec v2 stage-1 hyperparameters + architectural enums to maximize MLM val_accuracy"

fitness:
  primary: val_accuracy
  combined_weights:
    val_accuracy: 1.0
  minimize: false

evaluation:
  epochs: 5
  dataset: "luxury-lakehouse/football2vec-training-data"
  timeout_seconds: 1800
  seed: 42

backend:
  type: "local_cuda,remote_ssh"
  device: "cuda:0"
  ssh_host: "192.168.68.73"
  ssh_user: "karsten"
  ssh_remote_dir: "/home/karsten/Development"
  ssh_python_path: "/home/karsten/Development/evolve-env/bin/python"

llm:
  models:
    - name: "anthropic/claude-sonnet-4"
      weight: 0.8
      api_base: "https://openrouter.ai/api/v1"
      api_key_env: "OPENROUTER_API_KEY"
    - name: "anthropic/claude-haiku-4.5"
      weight: 0.2
      api_base: "https://openrouter.ai/api/v1"
      api_key_env: "OPENROUTER_API_KEY"
  temperature: 0.7
  max_tokens: 4096

evolution:
  iterations: 150
  population_size: 200
  num_islands: 3
  migration_interval: 30
  parallel_evaluations: 2
  diff_based: true
  early_stopping_patience: 40
  checkpoint_interval: 5
```

- [ ] **Step 2: Verify the config loads via Pydantic**

```bash
uv run python -c "from pathlib import Path; from evolve.config import EvolveConfig; cfg = EvolveConfig.from_yaml(Path('src/evolve/targets/football2vec/config.yaml')); print(cfg.target, cfg.evolution.iterations)"
```

Expected: `football2vec 150`

- [ ] **Step 3: Stage**

```bash
git add src/evolve/targets/football2vec/config.yaml
```

---

## Block D — Workflow card and tests

### Task D1: Create the workflow card

**Files:**
- Create: `workflow-cards/wf-evolve-football2vec.yaml`

- [ ] **Step 1: Write the card**

```yaml
---
name: "Evolve Football2Vec v2 Stage-1 Configuration"
id: wf-evolve-football2vec
version: "1.0.0"
status: development
type: training
domain: player-embeddings
owners:
  - karsten
tags:
  - alphaevolve
  - hyperparameter-search
  - football2vec
  - mlm

references:
  - citation: "Romera-Paredes et al. (2025). AlphaEvolve: A coding agent for scientific and algorithmic discovery. arXiv:2506.13131"
    role: methodology
  - citation: "Danesi, P. (2025). Football2Vec: Transformer-Based Player Embeddings."
    role: inspiration
  - citation: "Perez et al. (2018). FiLM: Visual Reasoning with a General Conditioning Layer. AAAI."
    role: methodology

inputs:
  datasets:
    - id: football2vec-training-data
      source: huggingface
      description: "114K player-match SPADL action sequences (StatsBomb + Wyscout)"

outputs:
  models:
    - id: football2vec-evolved-config
      destination: uc-volume
      format: "python"
      alias: "best_program"

execution:
  training:
    trigger: manual
    runtime: hf-jobs
    flavor: "RTX 5070 Ti (16 GB) + DGX Spark GB10 (128 GB unified) — local pool"
    script: "uv run evolve --target football2vec"
    timeout: "24h"

depends_on:
  - wf-football2vec-v2

idempotency:
  strategy: full-overwrite
  key: timestamp
  description: "Each evolution run creates a new timestamped results directory under results/evolve/football2vec/"

cost:
  training:
    runtime: hf-jobs
    flavor: "local-gpu"
    rate_usd_per_hour: 0.00
    typical_duration_minutes: 300
    typical_cost_usd: 0.00

monitoring:
  freshness_sla_hours: 168
  metrics:
    - name: "best_val_accuracy"
      baseline: 0.569
      warn_below: 0.50

links:
  source_code:
    - "src/evolve/targets/football2vec/"
    - "src/analytics/football2vec_transformer.py"
    - "docs/superpowers/specs/2026-04-18-ev1-football2vec-l1-sweep-design.md"
    - "docs/superpowers/specs/2026-04-04-evolve-engine-design.md"
    - "docs/superpowers/specs/2026-04-05-multi-backend-dispatcher-design.md"
---

## Overview

Evolve Football2Vec is an LLM-guided hyperparameter + architectural-enum search for the
Football2Vec v2 stage-1 (MLM pre-training) model. It uses the same OpenEvolve loop as
wf-evolve-scoutgpt but explores a different search space: 8 scalar hyperparameters
(hidden_dim, num_layers, num_heads, dropout, mask_prob, spatial_mlp_dim, learning_rate,
batch_size) plus 3 architectural enums (pooling_type, spatial_injection, position_embedding).
Config-only — no code evolution (Level 1).

## Architecture

The engine consists of three layers: the search space (encoded as a Pydantic model in
`src/evolve/targets/football2vec/search_space.py`), the evaluator bridge (which validates
each candidate and delegates to the local backend pool), and the MAP-Elites population
manager (which maintains a quality-diversity grid keyed on enum combinations and model size).
LLM mutations are issued via OpenRouter against the seven seed programs, with each
generation evaluated in parallel across the local pool (RTX 5070 Ti + DGX Spark GB10 over SSH).
The best-performing program is written to `results/evolve/football2vec/{timestamp}/best_program.py`.

## Evaluation

Candidate fitness is the val_accuracy on the held-out validation split of the 114K-row
Football2Vec training dataset (1-of-23 SPADL action prediction at masked positions, mask_prob
per candidate). Training is 5 epochs with patience-3 early stopping (vs 15 epochs for the
production retrain). A typical search run of 150 LLM iterations costs $0 in compute (all-local
pool) and ~$2 in OpenRouter LLM API calls, completing in ~5 hours overnight.
```

- [ ] **Step 2: Verify the card parses**

```bash
uv run python -c "from pathlib import Path; from workflows.card import WorkflowCard; c = WorkflowCard.from_yaml_file(Path('workflow-cards/wf-evolve-football2vec.yaml')); print(c.id, c.status)"
```

Expected: `wf-evolve-football2vec development`

- [ ] **Step 3: Stage**

```bash
git add workflow-cards/wf-evolve-football2vec.yaml
```

---

### Task D2: Write the consolidated test file

**Files:**
- Create: `src/tests/test_evolve_football2vec.py`

- [ ] **Step 1: Write the tests**

```python
"""Tests for the Football2Vec evolve target (search space, evaluator wiring, seed programs, workflow card)."""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from evolve.config import EvalConfig, FitnessConfig
from evolve.evaluator import EvolveEvaluator, _load_program, validate_search_space
from evolve.targets.football2vec.search_space import CandidateConfig, validate_candidate
from openevolve.evaluation_result import EvaluationResult

# ---------------------------------------------------------------------------
# Search-space validation
# ---------------------------------------------------------------------------

VALID_CONFIG: dict[str, Any] = {
    "hidden_dim": 128,
    "num_layers": 4,
    "num_heads": 4,
    "dropout": 0.1,
    "mask_prob": 0.15,
    "spatial_mlp_dim": 64,
    "pooling_type": "mean",
    "spatial_injection": "additive",
    "position_embedding": "learnable",
    "learning_rate": 1e-4,
    "batch_size": 256,
}


class TestSearchSpace:
    def test_valid_config_passes(self) -> None:
        assert validate_candidate(VALID_CONFIG) is True

    @pytest.mark.parametrize(
        ("key", "bad_value"),
        [
            ("hidden_dim", 32),       # below min
            ("hidden_dim", 512),      # above max
            ("num_layers", 1),
            ("num_layers", 12),
            ("num_heads", 1),
            ("num_heads", 16),
            ("dropout", -0.1),
            ("dropout", 0.6),
            ("mask_prob", 0.05),
            ("mask_prob", 0.50),
            ("spatial_mlp_dim", 8),
            ("spatial_mlp_dim", 256),
            ("learning_rate", 1e-6),
            ("learning_rate", 1e-2),
            ("batch_size", 32),
            ("batch_size", 1024),
        ],
    )
    def test_out_of_range_rejected(self, key: str, bad_value: float) -> None:
        cfg = {**VALID_CONFIG, key: bad_value}
        assert validate_candidate(cfg) is False

    def test_divisibility_rejected(self) -> None:
        """hidden_dim must be divisible by num_heads."""
        cfg = {**VALID_CONFIG, "hidden_dim": 130, "num_heads": 8}
        assert validate_candidate(cfg) is False

    @pytest.mark.parametrize(
        ("key", "bad_value"),
        [
            ("pooling_type", "max"),
            ("pooling_type", "sum"),
            ("spatial_injection", "cross_attention"),
            ("spatial_injection", "stack"),
            ("position_embedding", "alibi"),
            ("position_embedding", "absolute"),
        ],
    )
    def test_invalid_enum_rejected(self, key: str, bad_value: str) -> None:
        cfg = {**VALID_CONFIG, key: bad_value}
        assert validate_candidate(cfg) is False

    def test_concat_guard_rejected(self) -> None:
        """spatial_injection='concat' requires spatial_mlp_dim <= hidden_dim/2."""
        cfg = {**VALID_CONFIG, "spatial_injection": "concat", "spatial_mlp_dim": 96, "hidden_dim": 128}
        assert validate_candidate(cfg) is False

    def test_concat_guard_accepts_within_bound(self) -> None:
        cfg = {**VALID_CONFIG, "spatial_injection": "concat", "spatial_mlp_dim": 32, "hidden_dim": 128}
        assert validate_candidate(cfg) is True

    def test_dataset_prefix_enforced(self) -> None:
        cfg = {**VALID_CONFIG, "dataset": "some-other-org/dataset"}
        assert validate_candidate(cfg) is False


# ---------------------------------------------------------------------------
# Evaluator wiring (mocked backend)
# ---------------------------------------------------------------------------


def _write_candidate(tmp_path: Path, config: dict[str, Any]) -> Path:
    p = tmp_path / "candidate.py"
    p.write_text(textwrap.dedent(f"config = {config!r}\n"))
    return p


class TestEvaluatorWiring:
    def test_dispatch_to_backend(self, tmp_path: Path) -> None:
        candidate_path = _write_candidate(tmp_path, VALID_CONFIG)

        mock_backend = MagicMock()
        mock_backend.train.return_value = {
            "val_accuracy": 0.62,
            "val_loss": 1.42,
            "param_count": 700_000.0,
            "epochs_trained": 5.0,
            "training_time_seconds": 240.0,
        }

        fitness = FitnessConfig(primary="val_accuracy", combined_weights={"val_accuracy": 1.0})
        evaluator = EvolveEvaluator(
            backend=mock_backend,
            target="football2vec",
            eval_config=EvalConfig(epochs=5, seed=42),
            fitness_config=fitness,
        )
        result = evaluator.evaluate(str(candidate_path))

        mock_backend.train.assert_called_once_with(
            candidate_config=VALID_CONFIG,
            target="football2vec",
            epochs=5,
            seed=42,
        )
        assert isinstance(result, EvaluationResult)
        assert result.metrics["combined_score"] == pytest.approx(0.62)
        assert result.metrics["val_accuracy"] == pytest.approx(0.62)


# ---------------------------------------------------------------------------
# Seed programs load + validate
# ---------------------------------------------------------------------------


_REPO = Path(__file__).resolve().parents[2]
_SEEDS_DIR = _REPO / "src" / "evolve" / "targets" / "football2vec" / "seed_programs"


class TestSeedPrograms:
    def test_seven_seeds_present(self) -> None:
        seeds = sorted(p.stem for p in _SEEDS_DIR.glob("*.py") if p.name != "__init__.py")
        expected = sorted(["baseline", "wider", "deeper", "heavier_mask", "attention_pool", "film_spatial", "sinusoidal_pos"])
        assert seeds == expected

    @pytest.mark.parametrize(
        "seed_name",
        ["baseline", "wider", "deeper", "heavier_mask", "attention_pool", "film_spatial", "sinusoidal_pos"],
    )
    def test_seed_program_loads_and_validates(self, seed_name: str) -> None:
        path = _SEEDS_DIR / f"{seed_name}.py"
        prog = _load_program(str(path))
        assert validate_candidate(prog.config) is True, (
            f"seed {seed_name!r} fails validation: {prog.config}"
        )


# ---------------------------------------------------------------------------
# Workflow card parses
# ---------------------------------------------------------------------------


class TestWorkflowCard:
    def test_card_parses_and_links_back(self) -> None:
        from workflows.card import WorkflowCard

        path = _REPO / "workflow-cards" / "wf-evolve-football2vec.yaml"
        card = WorkflowCard.from_yaml_file(path)
        assert card.id == "wf-evolve-football2vec"
        assert card.links is not None
        assert any("evolve/targets/football2vec" in s for s in card.links.source_code)
        assert "wf-football2vec-v2" in card.depends_on


# ---------------------------------------------------------------------------
# CandidateConfig pydantic warnings on extra keys (typo detection)
# ---------------------------------------------------------------------------


class TestExtraKeyWarning:
    def test_unknown_key_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        cfg = {**VALID_CONFIG, "hiddem_dim": 256}  # typo
        with caplog.at_level("WARNING", logger="evolve.targets.football2vec.search_space"):
            CandidateConfig(**cfg)
        assert any("hiddem_dim" in rec.message for rec in caplog.records)
```

- [ ] **Step 2: Run the tests**

```bash
uv run pytest src/tests/test_evolve_football2vec.py -v
```

Expected: ALL PASS (search space, evaluator wiring, seven seed programs validate, workflow card parses, typo warning).

- [ ] **Step 3: Stage**

```bash
git add src/tests/test_evolve_football2vec.py
```

---

### Task D3: Run the full repo test + lint sweep

**Files:** none modified.

- [ ] **Step 1: Lint**

```bash
uv run ruff check src/ scripts/
```

Expected: 0 violations. If any violations are reported in files I touched, fix them. If unrelated files have pre-existing violations, do NOT touch them — that is out of scope.

- [ ] **Step 2: Format check**

```bash
uv run ruff format --check src/ scripts/
```

Expected: 0 files would be reformatted. If files I touched would be reformatted, run `uv run ruff format src/path/to/file` and re-stage.

- [ ] **Step 3: Type check**

```bash
uv run pyright src/
```

Expected: 0 errors in files I touched. Pre-existing errors in untouched files are out of scope.

- [ ] **Step 4: Full evolve + football2vec test sweep**

```bash
uv run pytest src/tests/test_evolve_*.py src/tests/test_football2vec_*.py -v
```

Expected: ALL PASS.

- [ ] **Step 5: Workflow card parity tests**

```bash
uv run pytest src/tests/test_card.py src/tests/test_card_cost_phase_parity.py src/tests/test_card_dbt_model_field.py src/tests/test_workflow_card_references.py src/tests/test_card_parity_with_terraform.py -v
```

Expected: ALL PASS. The new card is type=`training` with cost.training present (parity satisfied), no dbt_model field (no Delta tables), and references existing citations (no new academic refs requiring Appendix D update).

---

## Block E — POC smoke test (first commit gate)

### Task E1: Pre-flight environment check

**Files:** none modified.

- [ ] **Step 1: Verify the local CUDA backend is available**

```bash
uv run python -c "import torch; print('cuda available:', torch.cuda.is_available()); print('device count:', torch.cuda.device_count())"
```

Expected: `cuda available: True`, `device count: 1` (or more).

- [ ] **Step 2: Verify the DGX Spark SSH backend is reachable**

```bash
ssh -o ConnectTimeout=5 karsten@192.168.68.73 "echo OK && /home/karsten/Development/evolve-env/bin/python -c 'import torch; print(torch.cuda.is_available())'"
```

Expected: `OK` then `True`. If either fails, the BackendPool will fall back to local-only — that is acceptable for the POC, but record the failure.

- [ ] **Step 3: Verify HF_TOKEN is set**

```bash
uv run python -c "import os; t = os.environ.get('HF_TOKEN', ''); print('HF_TOKEN set:', bool(t), 'len:', len(t))"
```

Expected: `HF_TOKEN set: True`. If False, `export HF_TOKEN=...` and re-verify before proceeding.

- [ ] **Step 4: Verify OPENROUTER_API_KEY is set**

```bash
uv run python -c "import os; t = os.environ.get('OPENROUTER_API_KEY', ''); print('OPENROUTER_API_KEY set:', bool(t), 'len:', len(t))"
```

Expected: `OPENROUTER_API_KEY set: True`.

- [ ] **Step 5: Sync DGX Spark with the new evolve target**

The DGX Spark needs the new `evolve.targets.football2vec` module + the modified `evolve.evaluator` + the modified `analytics.football2vec_transformer`:

```bash
scp -r src/evolve/targets/football2vec karsten@192.168.68.73:/home/karsten/Development/evolve-env/lib/python3.12/site-packages/evolve/targets/
scp src/evolve/evaluator.py karsten@192.168.68.73:/home/karsten/Development/evolve-env/lib/python3.12/site-packages/evolve/evaluator.py
scp src/evolve/targets/scoutgpt/search_space.py karsten@192.168.68.73:/home/karsten/Development/evolve-env/lib/python3.12/site-packages/evolve/targets/scoutgpt/search_space.py
scp src/analytics/football2vec_transformer.py karsten@192.168.68.73:/home/karsten/Development/evolve-env/lib/python3.12/site-packages/analytics/football2vec_transformer.py
```

- [ ] **Step 6: Verify the new target is importable on DGX Spark**

```bash
ssh karsten@192.168.68.73 "/home/karsten/Development/evolve-env/bin/python -c 'from evolve.targets.football2vec.evaluator import train_and_evaluate; from evolve.targets.football2vec.search_space import validate_candidate; print(\"OK\")'"
```

Expected: `OK`.

---

### Task E2: Launch the POC smoke test

**Files:** none modified; results write to `results/evolve/football2vec/<timestamp>/`.

- [ ] **Step 1: Create the results directory if missing**

```bash
mkdir -p results/evolve/football2vec
```

- [ ] **Step 2: Launch with nohup (3 iterations)**

```bash
nohup uv run evolve --target football2vec --iterations 3 \
  > results/evolve/football2vec/smoke.log 2>&1 &
echo "PID: $!"
```

Record the PID. Per `feedback_nohup_evolve.md`: nohup is mandatory; without it the process dies on session close.

- [ ] **Step 3: Confirm the process is running**

```bash
ps -p <PID> -o pid,etime,cmd
```

Expected: process visible with running time.

- [ ] **Step 4: Tail the log periodically (manual, ~every 5 min)**

```bash
tail -n 50 results/evolve/football2vec/smoke.log
```

Watch for: seed evaluations starting, val_accuracy values appearing, no Python tracebacks. The POC takes ~20-25 min total (~14 min of seed eval + ~6 min of 3 iterations).

---

### Task E3: Verify POC pass criteria

**Files:** read-only verification of `results/evolve/football2vec/<timestamp>/`.

- [ ] **Step 1: Find the latest results directory**

```bash
ls -1t results/evolve/football2vec/ | head -5
```

Note the most recent timestamp directory name.

- [ ] **Step 2: Verify all 7 seed programs were evaluated**

```bash
ls results/evolve/football2vec/<timestamp>/seed_results/
```

Expected: 7 JSON files (`baseline.json`, `wider.json`, `deeper.json`, `heavier_mask.json`, `attention_pool.json`, `film_spatial.json`, `sinusoidal_pos.json`).

- [ ] **Step 3: Verify non-zero val_accuracy on every seed**

```bash
uv run python -c "
import json, sys
from pathlib import Path
seeds_dir = Path('results/evolve/football2vec/<timestamp>/seed_results')
fails = []
for p in sorted(seeds_dir.glob('*.json')):
    data = json.loads(p.read_text())
    acc = data['metrics'].get('val_accuracy', 0.0)
    print(f'{p.stem}: val_accuracy={acc:.4f}')
    if acc <= 0.0:
        fails.append(p.stem)
sys.exit(1 if fails else 0)
"
```

Expected: each seed reports a `val_accuracy > 0.0`. If any seed fails (returns 0 or errors), inspect `smoke.log` for tracebacks.

- [ ] **Step 4: Verify best_program.py exists and is parseable**

```bash
uv run python -c "
import ast
src = open('results/evolve/football2vec/<timestamp>/best_program.py').read()
tree = ast.parse(src)
config = next((ast.literal_eval(n.value) for n in ast.walk(tree)
               if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'config' for t in n.targets)), None)
assert isinstance(config, dict), 'best_program.py has no config dict'
print('best config:', config)
"
```

Expected: a printed dict.

- [ ] **Step 5: Verify metrics.json reports val_accuracy within sanity bound**

```bash
uv run python -c "
import json
m = json.loads(open('results/evolve/football2vec/<timestamp>/metrics.json').read())
acc = m.get('val_accuracy', 0.0)
print(f'best val_accuracy={acc:.4f}')
assert 0.40 <= acc <= 0.70, f'val_accuracy {acc} outside ±10pp sanity bound around 0.569'
"
```

Expected: PASS. The POC is too short to expect actual improvement; this is a sanity bound only.

- [ ] **Step 6: Inspect smoke.log for any unhandled exceptions**

```bash
grep -i "traceback\|error\|exception" results/evolve/football2vec/smoke.log | grep -v "INFO" | head -20
```

Expected: empty output (or only INFO-level error messages, which are acceptable). If a Python traceback appears: investigate, fix the cause, re-run the POC.

---

### Task E4: First commit gate (explicit user approval required)

**Files:** all staged changes from Blocks A-D + the new spec + the new plan.

- [ ] **Step 1: Show the user what is staged**

```bash
git status
git diff --stat
```

Report the counts to the user.

- [ ] **Step 2: ASK FOR USER APPROVAL TO COMMIT.**

Per project CLAUDE.md and `feedback_no_commits_without_approval.md`: do NOT proceed without an explicit "approved" or "go ahead" reply. Show the proposed commit message:

```
feat: EV1 — Football2Vec v2 Level 1 config sweep evolve target

Adds new `evolve` target `football2vec` for hyperparameter + architectural-enum
search over Football2Vec v2 stage-1 (MLM pre-training).

- Refactor src/evolve/evaluator.py:validate_search_space into per-target dispatcher
- Extract ScoutGPT schema to src/evolve/targets/scoutgpt/search_space.py (no behaviour change)
- Add 3 architectural enums to Football2VecEncoder (pooling_type, spatial_injection,
  position_embedding) — all defaults preserve current behaviour
- New target tree: search_space, evaluator, 7 seed programs, config.yaml, prompt
- Workflow card wf-evolve-football2vec.yaml
- Tests: search-space validation, evaluator wiring, seed-program loadability,
  workflow-card parsing, encoder enum variants, backward-compat regression

POC smoke test (3 iterations on local pool) passed:
- All 7 seeds evaluated with val_accuracy > 0
- best_program.py emitted, val_accuracy within ±10pp sanity bound

Spec: docs/superpowers/specs/2026-04-18-ev1-football2vec-l1-sweep-design.md
Plan: docs/superpowers/plans/2026-04-18-ev1-football2vec-l1-sweep.md

Per user instruction: minimal commits — this is the only commit before the
overnight full run.
```

- [ ] **Step 3: On approval, commit (single squash-friendly commit)**

```bash
git add -A docs/superpowers/specs/2026-04-18-ev1-football2vec-l1-sweep-design.md \
         docs/superpowers/plans/2026-04-18-ev1-football2vec-l1-sweep.md
git commit -m "$(cat <<'EOF'
feat: EV1 — Football2Vec v2 Level 1 config sweep evolve target

Adds new `evolve` target `football2vec` for hyperparameter + architectural-enum
search over Football2Vec v2 stage-1 (MLM pre-training).

- Refactor src/evolve/evaluator.py:validate_search_space into per-target dispatcher
- Extract ScoutGPT schema to src/evolve/targets/scoutgpt/search_space.py (no behaviour change)
- Add 3 architectural enums to Football2VecEncoder (pooling_type, spatial_injection,
  position_embedding) — all defaults preserve current behaviour
- New target tree: search_space, evaluator, 7 seed programs, config.yaml, prompt
- Workflow card wf-evolve-football2vec.yaml
- Tests: search-space validation, evaluator wiring, seed-program loadability,
  workflow-card parsing, encoder enum variants, backward-compat regression

POC smoke test (3 iterations on local pool) passed:
- All 7 seeds evaluated with val_accuracy > 0
- best_program.py emitted, val_accuracy within ±10pp sanity bound

Spec: docs/superpowers/specs/2026-04-18-ev1-football2vec-l1-sweep-design.md
Plan: docs/superpowers/plans/2026-04-18-ev1-football2vec-l1-sweep.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 4: ASK FOR USER APPROVAL TO PUSH.**

Show the commit hash. Ask: "Commit `<hash>` ready. Push to `evolve/football2vec-l1-sweep`?"

- [ ] **Step 5: On approval, push**

```bash
git push -u origin evolve/football2vec-l1-sweep
```

Report the remote URL back to the user.

---

## Block F — Full overnight run (second commit gate)

### Task F1: Launch the full run

**Files:** none modified; results write to `results/evolve/football2vec/<timestamp>/`.

- [ ] **Step 1: Sync any post-POC code changes to DGX Spark (if any)**

If Block E uncovered a bug that required post-stage edits to existing files, re-sync:

```bash
scp -r src/evolve/targets/football2vec karsten@192.168.68.73:/home/karsten/Development/evolve-env/lib/python3.12/site-packages/evolve/targets/
# Plus any other files that changed during POC kink-fixing.
```

If no bugs surfaced, skip this step.

- [ ] **Step 2: Launch the full 150-iteration run**

```bash
nohup uv run evolve --target football2vec \
  > results/evolve/football2vec/full_run.log 2>&1 &
echo "PID: $!"
```

Record PID. Expected wall-clock: ~5 hours, may early-stop sooner.

- [ ] **Step 3: Confirm running and note start time**

```bash
ps -p <PID> -o pid,etime,cmd
date -u +%Y-%m-%dT%H:%M:%SZ
```

---

### Task F2: Monitor (overnight)

**Files:** none modified.

- [ ] **Step 1: Sleep / let it run overnight**

The user runs this overnight. No active polling required.

- [ ] **Step 2: Next morning, check process status**

```bash
ps -p <PID> -o pid,etime,cmd 2>/dev/null && echo "STILL RUNNING" || echo "EXITED"
tail -n 100 results/evolve/football2vec/full_run.log
```

Expected: either still running (continue waiting) or exited normally (early-stop or 150 iter complete).

---

### Task F3: Verify full-run pass criteria

**Files:** read-only verification.

- [ ] **Step 1: Find the latest results directory**

```bash
ls -1t results/evolve/football2vec/ | head -3
```

- [ ] **Step 2: Inspect the final metrics**

```bash
uv run python -c "
import json
m = json.loads(open('results/evolve/football2vec/<timestamp>/metrics.json').read())
print(json.dumps(m, indent=2))
"
```

Expected: `val_accuracy` figure, plus all secondary metrics (`val_loss`, `param_count`, `epochs_trained`, `training_time_seconds`).

- [ ] **Step 3: Inspect the best config**

```bash
cat results/evolve/football2vec/<timestamp>/best_program.py
```

- [ ] **Step 4: Inspect the OpenEvolve checkpoints**

```bash
ls results/evolve/football2vec/<timestamp>/checkpoints/ | tail -5
```

Expected: `checkpoint_5`, `checkpoint_10`, ... up to either the early-stop point or `checkpoint_150`.

- [ ] **Step 5: Inspect the log for crashes / OOMs**

```bash
grep -iE "traceback|cuda out of memory|killed" results/evolve/football2vec/full_run.log | head -20
```

Expected: empty, or only per-candidate failures (the engine handles those by returning fail_metrics() and continuing).

- [ ] **Step 6: Compare best val_accuracy to baseline**

Baseline: 0.569 (15 epochs, full retrain on L40S).

If best val_accuracy ≥ 0.569: improvement found.
If 0.50 ≤ best val_accuracy < 0.569: no improvement found, but search ran cleanly (acceptable outcome — EV1's purpose is to TEST the defaults).
If best val_accuracy < 0.50: investigate (likely indicates a systematic issue with the 5-epoch budget or the search space).

---

### Task F4: Write the SUMMARY artifact

**Files:**
- Create: `results/evolve/football2vec/<timestamp>/SUMMARY.md`

- [ ] **Step 1: Hand-write the summary**

Manually compose `SUMMARY.md` based on the actual metrics. Template:

```markdown
# EV1 — Football2Vec v2 L1 Sweep — Run Summary

**Run timestamp:** <timestamp>
**Branch:** evolve/football2vec-l1-sweep
**Spec:** docs/superpowers/specs/2026-04-18-ev1-football2vec-l1-sweep-design.md

## Headline numbers

| Metric | Baseline (defaults, 15 epochs) | Best (this run, 5 epochs) | Δ |
|--------|--------------------------------|---------------------------|---|
| val_accuracy | 0.569 | <best_acc> | <delta_pp pp> |
| val_loss | <baseline_val_loss> | <best_val_loss> | — |
| param_count | <baseline_params> | <best_params> | — |

## Best config

```python
config = <pretty-printed best_program.py contents>
```

## Run economics

| | Value |
|---|---|
| Iterations completed | <N> / 150 |
| Early-stopped | <yes/no, at iter N> |
| Wall-clock | <H>h <M>m |
| Local GPU compute cost | $0 |
| OpenRouter LLM cost (estimate) | ~$<X> (per `project_evolve_llm_costs.md`: ~$2/day continuous) |

## Per-island summary

<read from results/evolve/football2vec/<timestamp>/checkpoints/checkpoint_<final>/island_*.json>

## Notable failed candidates

<grep "fail_metrics" full_run.log | wc -l → count of failed candidates>

## Recommendation

<one of:>
- **Promote to default:** the discovered config improves val_accuracy by <X pp>. Suggest opening a follow-up PR to update Football2VecConfig defaults and re-run train_football2vec_v2.py for a full 15-epoch retrain.
- **No promotion:** best val_accuracy did not exceed baseline. Document as a negative result. Defaults stand.
- **Inconclusive:** results suggest <X> but the 5-epoch budget may be too short. Suggest a 10-epoch follow-up sweep on the top-3 candidates.
```

Fill in every `<placeholder>` with actual values. Do not commit a SUMMARY with placeholders.

- [ ] **Step 2: Stage**

```bash
git add results/evolve/football2vec/<timestamp>/SUMMARY.md
```

---

### Task F5: Update TODO.md and add a session memory file

**Files:**
- Modify: `TODO.md` (remove EV1 row from the On Deck table; per `feedback_no_strikethrough_todo.md`)
- Create: `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\project_ev1_football2vec_l1_sweep.md`
- Modify: `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\MEMORY.md` (add one-line index entry under "Cycle Completion")

- [ ] **Step 1: Remove the EV1 row from `TODO.md`**

Find the row starting with `| EV1 | Evolve Engine — Football2Vec v2 Level 1` in the On Deck table and delete the entire line. Update the "Last updated" date at the top of the file.

- [ ] **Step 2: Create the session memory file**

Path: `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\project_ev1_football2vec_l1_sweep.md`

```markdown
---
name: EV1 — Football2Vec v2 L1 sweep
description: 2026-04-18 EV1 cycle — first football2vec evolve target shipped, 150-iter L1 sweep result + headline numbers
type: project
---

**Branch:** `evolve/football2vec-l1-sweep`
**Spec:** docs/superpowers/specs/2026-04-18-ev1-football2vec-l1-sweep-design.md
**Plan:** docs/superpowers/plans/2026-04-18-ev1-football2vec-l1-sweep.md

## What shipped

- New evolve target `src/evolve/targets/football2vec/` (search_space, evaluator, 7 seed programs, config, prompt)
- Per-target search-space dispatcher in `src/evolve/evaluator.py` (refactor; ScoutGPT schema moved to `src/evolve/targets/scoutgpt/search_space.py`)
- 3 architectural enums on `Football2VecEncoder` (pooling_type, spatial_injection, position_embedding) — defaults preserve pre-EV1 behaviour
- Workflow card `wf-evolve-football2vec.yaml`

## Run result

| | Value |
|---|---|
| Best val_accuracy | <X> (vs 0.569 baseline) |
| Δ over baseline | <Y pp> |
| Iterations completed | <N> / 150 |
| Wall-clock | <H>h <M>m |
| Cost | $0 GPU + ~$<X> LLM |

## Best config

<paste from best_program.py>

## Why this matters

<one sentence on the outcome — e.g. "First systematic sweep of Football2Vec defaults; <found / did not find> improvement; the architectural enums let L1 explore gate-space without invoking L2 code-evolution.">

## Follow-ups

- <if best > baseline:> Promote winning config to Football2VecConfig defaults; rerun `train_football2vec_v2.py` for a full 15-epoch retrain.
- EV2 (L2 + stage-2 adversarial) remains separately scoped.
- EV3 (second RTX 5070 Ti SSH backend) — independent.
```

- [ ] **Step 3: Add the one-line index entry to MEMORY.md**

Edit `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\MEMORY.md`. Under the `## Cycle Completion` heading, add a new line:

```markdown
- **Session XX** (EV1 Football2Vec L1 sweep): MERGED PR #XXX — first football2vec evolve target shipped, 150-iter L1 sweep, best val_accuracy=<X>. See [project_ev1_football2vec_l1_sweep.md](project_ev1_football2vec_l1_sweep.md)
```

(Session/PR numbers will be filled when the PR is opened — this is the form, not literal values.)

---

### Task F6: Second commit gate (explicit user approval required)

**Files:** SUMMARY.md, TODO.md, memory files.

- [ ] **Step 1: Show staged changes**

```bash
git status
git diff --stat
```

- [ ] **Step 2: ASK FOR USER APPROVAL TO COMMIT.**

Show the proposed commit message:

```
chore: EV1 results — full 150-iteration sweep summary + TODO update

Full overnight run completed on evolve/football2vec-l1-sweep.

- Best val_accuracy: <X> (vs 0.569 baseline, Δ=<Y pp>)
- Iterations completed: <N> / 150
- Wall-clock: <H>h <M>m, cost: $0 GPU + ~$<X> LLM
- SUMMARY.md and best_program.py written to results/evolve/football2vec/<timestamp>/
- TODO.md: EV1 removed from On Deck

<one of:>
- Promotion candidate: <best config diff vs defaults> — suggested follow-up PR.
- No promotion: best did not exceed baseline; defaults stand. Documented as negative result.
```

- [ ] **Step 3: On approval, commit**

```bash
git commit -m "$(cat <<'EOF'
chore: EV1 results — full 150-iteration sweep summary + TODO update

<final commit message text from Step 2>

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 4: ASK FOR USER APPROVAL TO PUSH.**

- [ ] **Step 5: On approval, push**

```bash
git push
```

- [ ] **Step 6: Optionally — open a PR (separate user approval required)**

If the user approves opening a PR (separate ask, separate decision):

```bash
gh pr create --title "EV1 — Football2Vec v2 Level 1 config sweep" --body "$(cat <<'EOF'
## Summary
- New evolve target for Football2Vec v2 stage-1 hyperparameter + architectural-enum search
- 3 architectural enums added to Football2VecEncoder (defaults preserve current behaviour)
- Per-target search-space dispatcher refactor in src/evolve/evaluator.py
- Workflow card + 7 seed programs + tests

## Run results
- Best val_accuracy: <X> (vs 0.569 baseline)
- Iterations: <N> / 150 on local pool (RTX 5070 Ti + DGX Spark)
- Wall-clock: <H>h <M>m, cost: $0 GPU + ~$<X> LLM

## Test plan
- [x] uv run pytest src/tests/test_evolve_*.py src/tests/test_football2vec_*.py
- [x] uv run ruff check src/ scripts/
- [x] uv run pyright src/
- [x] POC smoke test (3 iterations) passed
- [x] Full overnight run (150 iter) completed without crash

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Return the PR URL.

---

## Self-Review (post-write)

This section is the writing-plans skill's mandatory self-review. Each item below was checked at write time.

### 1. Spec coverage

| Spec section | Implementing tasks |
|---|---|
| Two orthogonal axes (L1 + stage-1 only) | Plan-wide — no L2 code, no stage-2 references |
| D1 — per-target search-space dispatch | A1, A2, A3 |
| D2 — self-contained training loop | C3 |
| D3 — module-level dataset cache | C3 |
| Search space (8 scalars + 3 enums) | C2 (schema), C5 (prompt), B1-B5 (enum implementations) |
| Sweep budget (150/200/3-islands/patience-40) | C6 (config.yaml) |
| Backend (local_cuda,remote_ssh) | C6 (config.yaml), E1 (preflight) |
| Fitness (single primary val_accuracy) | C6 (config.yaml fitness block) |
| LLM mutation prompt | C5 |
| Workflow card | D1 |
| Tests (search_space, evaluator wiring, seeds, card, encoder variants, backward-compat) | A3 (dispatch regression), B2-B5 (encoder variants), D2 (target tests), D3 (full sweep) |
| POC smoke test | E1, E2, E3 |
| Full overnight run | F1, F2, F3 |
| SUMMARY.md artifact | F4 |
| TODO.md + MEMORY.md update | F5 |
| Commit cadence (2 gates only) | E4 (first commit), F6 (second commit) |

No gaps.

### 2. Placeholder scan

- No "TBD", "TODO", "implement later" markers.
- Steps that show code show full code blocks.
- Steps that show commands show exact commands with expected output.
- The only `<placeholder>` markers are in F4 (SUMMARY template) and F5 (memory file template) where the actual values come from the F3 verification step — the plan instructs the executor to replace every `<placeholder>` with the measured value before commit.

### 3. Type consistency

- `validate_candidate(config: dict[str, Any]) -> bool` consistent across `src/evolve/targets/scoutgpt/search_space.py` (A1) and `src/evolve/targets/football2vec/search_space.py` (C2).
- `validate_search_space(config, target="scoutgpt") -> bool` signature consistent across `src/evolve/evaluator.py` definition (A2), call site in `EvolveEvaluator.evaluate` (A2), and test (A3).
- `Football2VecConfig` field names consistent across dataclass (B1), encoder usage (B2-B4), candidate config dict keys in seed programs (C4), search-space schema (C2), evaluator's `config_keys` set (C3), and LLM prompt (C5).
- Enum value strings consistent: `"mean" | "attention" | "cls"`, `"additive" | "concat" | "film"`, `"learnable" | "sinusoidal" | "rope"` — appear identically in B1, B2, B3, B4, C2, C4, C5, D2.
- `train_and_evaluate` signature `(candidate_config, device, epochs, seed, program_path=None) -> dict` matches `ComputeBackend.train` protocol contract.

No inconsistencies found.

---

## Notes for the executor

- **Do not skip the first commit gate.** The user has explicitly stated minimal commits with explicit per-step approval. Steps E4 and F6 are blocking.
- **Use `nohup` for any evolve run.** Not optional. Per `feedback_nohup_evolve.md`.
- **Sync DGX Spark with `scp` after any source change.** The remote backend imports from a separate Python env; it does not see the local `git` working tree.
- **Investigate first failures, do not retry blindly.** Per `feedback_three_strikes_investigate.md`.
- **No commits without approval.** Per `feedback_no_commits_without_approval.md` and `feedback_one_commit_at_a_time.md`.
