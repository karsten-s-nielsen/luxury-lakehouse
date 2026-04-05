# Evolve Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an AlphaEvolve-style evolutionary architecture search engine that optimizes ScoutGPT's counterfactual Spearman rho using OpenEvolve + local GPU training.

**Architecture:** Pluggable compute backends (local CUDA now, Docker/HF Jobs/SSH future) dispatch model training. OpenEvolve handles population management and LLM-guided mutation. Target-specific evaluators bridge the two. Four conditioning mechanisms (additive, cross-attention, FiLM, gated) form the initial population.

**Tech Stack:** OpenEvolve (Apache 2.0), PyTorch, Pydantic, Claude API via OpenAI-compatible endpoint.

**Spec:** `docs/superpowers/specs/2026-04-04-evolve-engine-design.md`

---

## File Map

### New files

| File | Responsibility |
|------|---------------|
| `src/evolve/__init__.py` | Package init, re-export key types |
| `src/evolve/config.py` | Pydantic config models (EvolveConfig, BackendConfig, FitnessConfig, etc.) |
| `src/evolve/runner.py` | CLI entry point — load config, wire OpenEvolve, run evolution |
| `src/evolve/evaluator.py` | Bridge OpenEvolve's evaluate() to our ComputeBackend protocol |
| `src/evolve/backends/__init__.py` | Backend registry — resolve backend type string to class |
| `src/evolve/backends/base.py` | ComputeBackend Protocol definition |
| `src/evolve/backends/local_cuda.py` | Local GPU training backend |
| `src/evolve/backends/docker.py` | Docker backend stub |
| `src/evolve/backends/hf_jobs.py` | HF Jobs backend stub |
| `src/evolve/backends/remote_ssh.py` | SSH backend stub |
| `src/evolve/targets/__init__.py` | Target registry — resolve target name to module |
| `src/evolve/targets/scoutgpt/__init__.py` | ScoutGPT target package |
| `src/evolve/targets/scoutgpt/evaluator.py` | Build ScoutGPT model from config, train, score |
| `src/evolve/targets/scoutgpt/config.yaml` | Default evolution config for ScoutGPT |
| `src/evolve/targets/scoutgpt/seed_programs/__init__.py` | Seed programs package |
| `src/evolve/targets/scoutgpt/seed_programs/additive.py` | Seed 1: current additive conditioning |
| `src/evolve/targets/scoutgpt/seed_programs/cross_attention.py` | Seed 2: cross-attention conditioning |
| `src/evolve/targets/scoutgpt/seed_programs/film.py` | Seed 3: FiLM conditioning |
| `src/evolve/targets/scoutgpt/seed_programs/gated.py` | Seed 4: gated conditioning + player prediction loss |
| `src/tests/test_evolve_config.py` | Config validation tests |
| `src/tests/test_evolve_evaluator.py` | Evaluator bridge tests with mock backend |
| `src/tests/test_scoutgpt_conditioning.py` | Tests for new conditioning mechanisms in decoder |
| `workflow-cards/wf-evolve-scoutgpt.yaml` | Workflow card |

### Modified files

| File | Change |
|------|--------|
| `src/analytics/scoutgpt_decoder.py` | Add `conditioning_type` to config, implement cross_attention/film/gated in `_embed()` |
| `pyproject.toml` | Add `evolve` optional-dep group, entry point, wheel package, isort config |
| `.gitignore` | Add `results/evolve/` |

---

## Task 1: Project scaffolding and config models

**Files:**
- Create: `src/evolve/__init__.py`
- Create: `src/evolve/config.py`
- Create: `src/tests/test_evolve_config.py`
- Modify: `pyproject.toml`
- Modify: `.gitignore`

- [ ] **Step 1: Write config validation tests**

```python
# src/tests/test_evolve_config.py
"""Tests for evolve engine configuration."""

from __future__ import annotations

import pytest

from evolve.config import (
    BackendConfig,
    EvalConfig,
    EvolveConfig,
    EvolutionConfig,
    FitnessConfig,
    LLMModelConfig,
    LLMConfig,
)


class TestFitnessConfig:
    def test_valid_config(self) -> None:
        cfg = FitnessConfig(
            primary="spearman_rho",
            secondary="top1_accuracy",
            combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
            minimize=False,
        )
        assert cfg.primary == "spearman_rho"

    def test_weights_must_sum_to_one(self) -> None:
        with pytest.raises(ValueError, match="must sum to 1.0"):
            FitnessConfig(
                primary="spearman_rho",
                combined_weights={"spearman_rho": 0.5, "top1_accuracy": 0.3},
            )

    def test_primary_must_be_in_weights(self) -> None:
        with pytest.raises(ValueError, match="primary.*must be in combined_weights"):
            FitnessConfig(
                primary="missing_key",
                combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
            )


class TestBackendConfig:
    def test_local_cuda_default(self) -> None:
        cfg = BackendConfig(type="local_cuda")
        assert cfg.device == "cuda:0"

    def test_unknown_type_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unknown backend type"):
            BackendConfig(type="quantum_computer")


class TestEvolveConfig:
    def test_from_yaml(self, tmp_path: object) -> None:
        import yaml
        from pathlib import Path

        p = Path(str(tmp_path)) / "config.yaml"
        p.write_text(yaml.dump({
            "target": "scoutgpt",
            "description": "test run",
            "fitness": {
                "primary": "spearman_rho",
                "combined_weights": {"spearman_rho": 0.7, "top1_accuracy": 0.3},
            },
            "evaluation": {"epochs": 5, "dataset": "test/dataset"},
            "backend": {"type": "local_cuda"},
            "llm": {
                "models": [{"name": "test-model", "weight": 1.0, "api_base": "http://localhost", "api_key_env": "TEST_KEY"}],
            },
            "evolution": {"iterations": 10},
        }))
        cfg = EvolveConfig.from_yaml(p)
        assert cfg.target == "scoutgpt"
        assert cfg.fitness.primary == "spearman_rho"
        assert cfg.evolution.iterations == 10


class TestLLMConfig:
    def test_model_weights_must_sum_to_one(self) -> None:
        with pytest.raises(ValueError, match="must sum to 1.0"):
            LLMConfig(
                models=[
                    LLMModelConfig(name="a", weight=0.3, api_base="http://x", api_key_env="K"),
                    LLMModelConfig(name="b", weight=0.3, api_base="http://x", api_key_env="K"),
                ],
            )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run pytest src/tests/test_evolve_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'evolve'`

- [ ] **Step 3: Create package init**

```python
# src/evolve/__init__.py
"""Evolve Engine — LLM-guided evolutionary architecture search."""

from __future__ import annotations

from evolve.config import EvolveConfig

__all__ = ["EvolveConfig"]
```

- [ ] **Step 4: Implement config models**

```python
# src/evolve/config.py
"""Pydantic configuration models for the evolve engine."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, field_validator, model_validator


class FitnessConfig(BaseModel):
    """Defines what to optimize."""

    primary: str
    secondary: str | None = None
    combined_weights: dict[str, float]
    minimize: bool = False

    @field_validator("combined_weights")
    @classmethod
    def _weights_sum_to_one(cls, v: dict[str, float]) -> dict[str, float]:
        total = sum(v.values())
        if not math.isclose(total, 1.0, abs_tol=1e-6):
            msg = f"combined_weights must sum to 1.0, got {total}"
            raise ValueError(msg)
        return v

    @model_validator(mode="after")
    def _primary_in_weights(self) -> FitnessConfig:
        if self.primary not in self.combined_weights:
            msg = f"primary '{self.primary}' must be in combined_weights keys: {list(self.combined_weights)}"
            raise ValueError(msg)
        return self


_VALID_BACKEND_TYPES = frozenset({"local_cuda", "docker", "hf_jobs", "remote_ssh"})


class BackendConfig(BaseModel):
    """Compute backend configuration."""

    type: str
    device: str = "cuda:0"
    docker_image: str | None = None
    hf_flavor: str | None = None
    ssh_host: str | None = None

    @field_validator("type")
    @classmethod
    def _valid_type(cls, v: str) -> str:
        if v not in _VALID_BACKEND_TYPES:
            msg = f"Unknown backend type '{v}'. Must be one of: {sorted(_VALID_BACKEND_TYPES)}"
            raise ValueError(msg)
        return v


class EvalConfig(BaseModel):
    """Per-candidate evaluation budget."""

    epochs: int = 5
    dataset: str = "luxury-lakehouse/scoutgpt-training-data"
    timeout_seconds: int = 900
    seed: int = 42


class LLMModelConfig(BaseModel):
    """Single LLM model in the ensemble."""

    name: str
    weight: float
    api_base: str
    api_key_env: str


class LLMConfig(BaseModel):
    """LLM ensemble configuration."""

    models: list[LLMModelConfig]
    temperature: float = 0.7
    max_tokens: int = 4096

    @field_validator("models")
    @classmethod
    def _weights_sum_to_one(cls, v: list[LLMModelConfig]) -> list[LLMModelConfig]:
        total = sum(m.weight for m in v)
        if not math.isclose(total, 1.0, abs_tol=1e-6):
            msg = f"LLM model weights must sum to 1.0, got {total}"
            raise ValueError(msg)
        return v


class EvolutionConfig(BaseModel):
    """OpenEvolve evolution parameters."""

    iterations: int = 150
    population_size: int = 200
    num_islands: int = 3
    migration_interval: int = 30
    parallel_evaluations: int = 1
    diff_based: bool = True
    early_stopping_patience: int = 40


class EvolveConfig(BaseModel):
    """Top-level evolve engine configuration."""

    target: str
    description: str = ""
    fitness: FitnessConfig
    evaluation: EvalConfig = EvalConfig()
    backend: BackendConfig = BackendConfig(type="local_cuda")
    llm: LLMConfig
    evolution: EvolutionConfig = EvolutionConfig()

    @classmethod
    def from_yaml(cls, path: Path) -> EvolveConfig:
        """Load config from a YAML file."""
        with open(path) as f:
            data: dict[str, Any] = yaml.safe_load(f)
        return cls(**data)
```

- [ ] **Step 5: Update pyproject.toml**

Add to `[project.optional-dependencies]` after the `training` line:
```toml
evolve = ["openevolve>=0.3.0"]
```

Add to `[project.scripts]`:
```toml
evolve = "evolve.runner:main"
```

Add `"src/evolve"` to `[tool.hatch.build.targets.wheel]` packages:
```toml
packages = ["src/ingestion", "src/analytics", "src/shared", "src/workflows", "src/evolve"]
```

Add `"evolve"` to `[tool.ruff.lint.isort]` known-first-party:
```toml
known-first-party = ["ingestion", "analytics", "shared", "workflows", "evolve"]
```

- [ ] **Step 6: Update .gitignore**

Add after the `temp/` line:
```gitignore
results/evolve/
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run pytest src/tests/test_evolve_config.py -v`
Expected: All 6 tests PASS

- [ ] **Step 8: Lint and type check**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run ruff check src/evolve/ src/tests/test_evolve_config.py && uv run ruff format --check src/evolve/ src/tests/test_evolve_config.py && uv run pyright src/evolve/`
Expected: All clean

- [ ] **Step 9: Commit**

```bash
git add src/evolve/__init__.py src/evolve/config.py src/tests/test_evolve_config.py pyproject.toml .gitignore
git commit -m "feat(evolve): scaffolding and Pydantic config models"
```

---

## Task 2: Compute backend protocol and local CUDA backend

**Files:**
- Create: `src/evolve/backends/__init__.py`
- Create: `src/evolve/backends/base.py`
- Create: `src/evolve/backends/local_cuda.py`
- Create: `src/evolve/backends/docker.py`
- Create: `src/evolve/backends/hf_jobs.py`
- Create: `src/evolve/backends/remote_ssh.py`

- [ ] **Step 1: Create backend protocol**

```python
# src/evolve/backends/base.py
"""Compute backend protocol for the evolve engine."""

from __future__ import annotations

from typing import Any, Protocol


class ComputeBackend(Protocol):
    """Protocol for compute backends that train model candidates."""

    def train(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
    ) -> dict[str, float]:
        """Train a candidate and return evaluation metrics.

        Returns dict with at minimum the fitness metric keys
        (e.g., {"spearman_rho": 0.23, "top1_accuracy": 0.79, "param_count": 8100000}).
        Raises TimeoutError if training exceeds budget.
        """
        ...

    def available(self) -> bool:
        """Check if this backend is usable (GPU present, Docker running, etc.)."""
        ...
```

- [ ] **Step 2: Create local CUDA backend**

```python
# src/evolve/backends/local_cuda.py
"""Local CUDA compute backend — trains models on the local GPU."""

from __future__ import annotations

import importlib
import logging
from typing import Any

logger = logging.getLogger(__name__)


class LocalCudaBackend:
    """Trains model candidates directly on the local GPU via PyTorch."""

    def __init__(self, device: str = "cuda:0") -> None:
        self.device = device

    def train(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
    ) -> dict[str, float]:
        """Import the target evaluator and run training."""
        module = importlib.import_module(f"evolve.targets.{target}.evaluator")
        train_and_evaluate = module.train_and_evaluate
        logger.info("Training candidate on %s for %d epochs (target=%s)", self.device, epochs, target)
        return train_and_evaluate(
            candidate_config=candidate_config,
            device=self.device,
            epochs=epochs,
            seed=seed,
        )

    def available(self) -> bool:
        """Check if CUDA is available."""
        try:
            import torch

            return torch.cuda.is_available()
        except ImportError:
            return False
```

- [ ] **Step 3: Create backend stubs**

```python
# src/evolve/backends/docker.py
"""Docker compute backend (stub — not yet implemented)."""

from __future__ import annotations

from typing import Any


class DockerBackend:
    """Trains model candidates inside Docker containers."""

    def __init__(self, image: str = "") -> None:
        self.image = image

    def train(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
    ) -> dict[str, float]:
        raise NotImplementedError("Docker backend is not yet implemented")

    def available(self) -> bool:
        return False
```

```python
# src/evolve/backends/hf_jobs.py
"""HF Jobs compute backend (stub — not yet implemented)."""

from __future__ import annotations

from typing import Any


class HFJobsBackend:
    """Submits training to HuggingFace Jobs infrastructure."""

    def __init__(self, flavor: str = "a10g-large") -> None:
        self.flavor = flavor

    def train(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
    ) -> dict[str, float]:
        raise NotImplementedError("HF Jobs backend is not yet implemented")

    def available(self) -> bool:
        return False
```

```python
# src/evolve/backends/remote_ssh.py
"""Remote SSH compute backend (stub — not yet implemented)."""

from __future__ import annotations

from typing import Any


class RemoteSSHBackend:
    """Trains model candidates on a remote machine via SSH."""

    def __init__(self, host: str = "") -> None:
        self.host = host

    def train(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
    ) -> dict[str, float]:
        raise NotImplementedError("Remote SSH backend is not yet implemented")

    def available(self) -> bool:
        return False
```

- [ ] **Step 4: Create backend registry**

```python
# src/evolve/backends/__init__.py
"""Compute backend registry."""

from __future__ import annotations

from evolve.backends.base import ComputeBackend
from evolve.config import BackendConfig


def create_backend(config: BackendConfig) -> ComputeBackend:
    """Create a compute backend from config."""
    if config.type == "local_cuda":
        from evolve.backends.local_cuda import LocalCudaBackend

        return LocalCudaBackend(device=config.device)
    if config.type == "docker":
        from evolve.backends.docker import DockerBackend

        return DockerBackend(image=config.docker_image or "")
    if config.type == "hf_jobs":
        from evolve.backends.hf_jobs import HFJobsBackend

        return HFJobsBackend(flavor=config.hf_flavor or "a10g-large")
    if config.type == "remote_ssh":
        from evolve.backends.remote_ssh import RemoteSSHBackend

        return RemoteSSHBackend(host=config.ssh_host or "")
    msg = f"Unknown backend type: {config.type}"
    raise ValueError(msg)


__all__ = ["ComputeBackend", "create_backend"]
```

- [ ] **Step 5: Lint and type check**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run ruff check src/evolve/backends/ && uv run ruff format --check src/evolve/backends/ && uv run pyright src/evolve/backends/`
Expected: All clean

- [ ] **Step 6: Commit**

```bash
git add src/evolve/backends/
git commit -m "feat(evolve): compute backend protocol and local CUDA backend"
```

---

## Task 3: Conditioning mechanisms in ScoutGPT decoder

**Files:**
- Modify: `src/analytics/scoutgpt_decoder.py`
- Create: `src/tests/test_scoutgpt_conditioning.py`

- [ ] **Step 1: Write tests for new conditioning types**

```python
# src/tests/test_scoutgpt_conditioning.py
"""Tests for ScoutGPT conditioning mechanisms (additive, cross_attention, film, gated)."""

from __future__ import annotations

from typing import Any

import pytest

try:
    import torch

    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False

pytestmark = pytest.mark.skipif(not HAS_TORCH, reason="torch not installed")


def _make_config(**overrides: Any) -> Any:
    from analytics.scoutgpt_decoder import ScoutGPTConfig

    defaults = {
        "hidden_dim": 32,
        "num_layers": 1,
        "num_heads": 2,
        "num_players": 100,
        "max_seq_len": 16,
        "spatial_mlp_dim": 16,
    }
    defaults.update(overrides)
    return ScoutGPTConfig(**defaults)


def _make_batch(batch_size: int = 2, seq_len: int = 8) -> dict[str, Any]:
    g = torch.Generator().manual_seed(42)
    return {
        "action_ids": torch.randint(0, 23, (batch_size, seq_len), generator=g),
        "start_x": torch.rand(batch_size, seq_len, generator=g),
        "start_y": torch.rand(batch_size, seq_len, generator=g),
        "end_x": torch.rand(batch_size, seq_len, generator=g),
        "end_y": torch.rand(batch_size, seq_len, generator=g),
        "result": torch.randint(0, 2, (batch_size, seq_len), generator=g),
        "time_delta": torch.rand(batch_size, seq_len, generator=g),
        "player_ids": torch.randint(0, 100, (batch_size, seq_len), generator=g),
        "attention_mask": torch.ones(batch_size, seq_len, dtype=torch.bool),
    }


@pytest.mark.parametrize("conditioning_type", ["additive", "cross_attention", "film", "gated"])
def test_predict_shape(conditioning_type: str) -> None:
    """All conditioning types produce correct output shapes from predict()."""
    from analytics.scoutgpt_decoder import ScoutGPTDecoder

    config = _make_config(conditioning_type=conditioning_type)
    model = ScoutGPTDecoder(config)
    model.eval()
    batch = _make_batch()
    with torch.no_grad():
        logits, vaep = model.predict(**batch)
    assert logits.shape == (2, 8, 23)
    assert vaep.shape == (2, 8, 1)


@pytest.mark.parametrize("conditioning_type", ["additive", "cross_attention", "film", "gated"])
def test_forward_shape(conditioning_type: str) -> None:
    """All conditioning types produce correct output shape from forward()."""
    from analytics.scoutgpt_decoder import ScoutGPTDecoder

    config = _make_config(conditioning_type=conditioning_type)
    model = ScoutGPTDecoder(config)
    model.eval()
    batch = _make_batch()
    with torch.no_grad():
        emb = model(**batch)
    assert emb.shape == (2, 32)  # (batch, hidden_dim)


@pytest.mark.parametrize("conditioning_type", ["cross_attention", "film", "gated"])
def test_player_swap_changes_output(conditioning_type: str) -> None:
    """Non-additive conditioning types produce different logits when player_ids change."""
    from analytics.scoutgpt_decoder import ScoutGPTDecoder

    config = _make_config(conditioning_type=conditioning_type)
    model = ScoutGPTDecoder(config)
    model.eval()
    batch = _make_batch()

    with torch.no_grad():
        logits_a, _ = model.predict(**batch)
        # Swap focal player at position 0
        batch["player_ids"] = batch["player_ids"].clone()
        batch["player_ids"][:, 0] = 99
        logits_b, _ = model.predict(**batch)

    # Logits should differ (not guaranteed for additive due to summation,
    # but multiplicative/gated mechanisms should be more sensitive)
    assert not torch.allclose(logits_a, logits_b, atol=1e-5)


def test_additive_is_default() -> None:
    """Default ScoutGPTConfig uses additive conditioning."""
    from analytics.scoutgpt_decoder import ScoutGPTConfig

    config = ScoutGPTConfig()
    assert config.conditioning_type == "additive"


def test_unknown_conditioning_type_raises() -> None:
    """Unknown conditioning type raises ValueError."""
    from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder

    config = ScoutGPTConfig(conditioning_type="unknown_type")
    with pytest.raises(ValueError, match="Unknown conditioning_type"):
        ScoutGPTDecoder(config)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run pytest src/tests/test_scoutgpt_conditioning.py -v`
Expected: FAIL — `ScoutGPTConfig` has no `conditioning_type` field

- [ ] **Step 3: Add conditioning_type to ScoutGPTConfig**

In `src/analytics/scoutgpt_decoder.py`, add a field to the frozen dataclass at line 37 (before the closing of the class):

```python
    conditioning_type: str = "additive"
```

- [ ] **Step 4: Implement conditioning mechanisms in ScoutGPTDecoder.__init__**

In `src/analytics/scoutgpt_decoder.py`, in `__init__()` (around line 60), after the existing `self.player_embedding` line, add the conditioning-specific layers. The `__init__` method needs to branch based on `config.conditioning_type`:

After the existing `self.player_embedding = nn.Embedding(config.num_players, config.hidden_dim)` line, add:

```python
        self._conditioning_type = config.conditioning_type

        if config.conditioning_type == "cross_attention":
            self.player_cross_attn = nn.MultiheadAttention(
                embed_dim=config.hidden_dim,
                num_heads=config.num_heads,
                dropout=config.dropout,
                batch_first=True,
            )
            self.player_cross_norm = nn.LayerNorm(config.hidden_dim)
        elif config.conditioning_type == "film":
            self.film_scale = nn.Sequential(
                nn.Linear(config.hidden_dim, config.hidden_dim),
                nn.Sigmoid(),
            )
            self.film_shift = nn.Linear(config.hidden_dim, config.hidden_dim)
        elif config.conditioning_type == "gated":
            self.player_gate = nn.Sequential(
                nn.Linear(config.hidden_dim, config.hidden_dim),
                nn.Sigmoid(),
            )
        elif config.conditioning_type != "additive":
            msg = f"Unknown conditioning_type: {config.conditioning_type!r}"
            raise ValueError(msg)
```

- [ ] **Step 5: Implement conditioning in _embed()**

Replace the current `_embed()` method body. The signature stays the same. The new implementation branches on `self._conditioning_type`:

```python
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
        """Compute input embeddings with configurable player conditioning.

        All inputs are (batch, seq_len). Returns (batch, seq_len, hidden_dim).
        """
        seq_len = action_ids.size(1)
        player_emb = self.player_embedding(player_ids)

        # Action embedding: everything except player
        action_emb = (
            self.token_embedding(action_ids)
            + self.start_x_mlp(start_x)
            + self.start_y_mlp(start_y)
            + self.end_x_mlp(end_x)
            + self.end_y_mlp(end_y)
            + self.result_embedding(result)
            + self.time_delta_mlp(time_delta)
            + self.position_embedding(self._pos_ids[:, :seq_len])
        )

        if self._conditioning_type == "additive":
            emb = action_emb + player_emb
        elif self._conditioning_type == "cross_attention":
            # Player embedding as K/V, action sequence as Q
            attn_out, _ = self.player_cross_attn(
                query=action_emb,
                key=player_emb,
                value=player_emb,
            )
            emb = self.player_cross_norm(action_emb + attn_out)
        elif self._conditioning_type == "film":
            # Feature-wise Linear Modulation: scale and shift action embedding
            scale = self.film_scale(player_emb)
            shift = self.film_shift(player_emb)
            emb = scale * action_emb + shift
        elif self._conditioning_type == "gated":
            # Learned gate: player selectively amplifies/suppresses action features
            gate = self.player_gate(player_emb)
            emb = gate * action_emb + player_emb
        else:
            msg = f"Unknown conditioning_type: {self._conditioning_type!r}"
            raise ValueError(msg)

        return self.embedding_dropout(emb)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run pytest src/tests/test_scoutgpt_conditioning.py -v`
Expected: All 11 tests PASS (4 predict_shape + 4 forward_shape + 3 player_swap)

- [ ] **Step 7: Run existing ScoutGPT tests to verify no regression**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run pytest src/tests/test_scoutgpt_decoder.py src/tests/test_benchmarks.py -v -k scoutgpt`
Expected: All existing tests PASS (default config uses `conditioning_type="additive"`, identical behavior)

- [ ] **Step 8: Lint and type check**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run ruff check src/analytics/scoutgpt_decoder.py src/tests/test_scoutgpt_conditioning.py && uv run pyright src/analytics/scoutgpt_decoder.py`
Expected: All clean

- [ ] **Step 9: Commit**

```bash
git add src/analytics/scoutgpt_decoder.py src/tests/test_scoutgpt_conditioning.py
git commit -m "feat(scoutgpt): add cross_attention, film, gated conditioning mechanisms"
```

---

## Task 4: ScoutGPT target evaluator

**Files:**
- Create: `src/evolve/targets/__init__.py`
- Create: `src/evolve/targets/scoutgpt/__init__.py`
- Create: `src/evolve/targets/scoutgpt/evaluator.py`
- Create: `src/tests/test_evolve_evaluator.py`

- [ ] **Step 1: Write evaluator tests with mock backend**

```python
# src/tests/test_evolve_evaluator.py
"""Tests for evolve evaluator bridge and ScoutGPT target evaluator."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from evolve.config import BackendConfig, EvalConfig, FitnessConfig
from evolve.evaluator import EvolveEvaluator

# --- Search space validation tests ---

VALID_CONFIG: dict[str, Any] = {
    "hidden_dim": 256,
    "num_layers": 6,
    "num_heads": 8,
    "conditioning_type": "additive",
    "dropout": 0.1,
    "learning_rate": 1e-4,
    "vaep_loss_weight": 0.1,
    "batch_size": 256,
}


class TestSearchSpaceValidation:
    def test_valid_config_passes(self) -> None:
        from evolve.evaluator import validate_search_space

        assert validate_search_space(VALID_CONFIG) is True

    def test_hidden_dim_too_large(self) -> None:
        from evolve.evaluator import validate_search_space

        bad = {**VALID_CONFIG, "hidden_dim": 1024}
        assert validate_search_space(bad) is False

    def test_hidden_dim_too_small(self) -> None:
        from evolve.evaluator import validate_search_space

        bad = {**VALID_CONFIG, "hidden_dim": 32}
        assert validate_search_space(bad) is False

    def test_num_heads_must_divide_hidden_dim(self) -> None:
        from evolve.evaluator import validate_search_space

        bad = {**VALID_CONFIG, "hidden_dim": 300, "num_heads": 8}
        assert validate_search_space(bad) is False

    def test_dropout_out_of_range(self) -> None:
        from evolve.evaluator import validate_search_space

        bad = {**VALID_CONFIG, "dropout": 0.8}
        assert validate_search_space(bad) is False

    def test_unknown_conditioning_type(self) -> None:
        from evolve.evaluator import validate_search_space

        bad = {**VALID_CONFIG, "conditioning_type": "transformer_xl"}
        assert validate_search_space(bad) is False


class TestEvolveEvaluator:
    def test_evaluate_calls_backend(self, tmp_path: Any) -> None:
        """Evaluator extracts config from program file and calls backend."""
        program = tmp_path / "candidate.py"
        program.write_text(
            'config = {"hidden_dim": 256, "num_layers": 4, "num_heads": 8, '
            '"conditioning_type": "additive", "dropout": 0.1, "learning_rate": 1e-4, '
            '"vaep_loss_weight": 0.1, "batch_size": 256}\n'
        )

        mock_backend = MagicMock()
        mock_backend.train.return_value = {"spearman_rho": 0.25, "top1_accuracy": 0.80}

        fitness = FitnessConfig(
            primary="spearman_rho",
            combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
        )
        evaluator = EvolveEvaluator(
            backend=mock_backend,
            target="scoutgpt",
            eval_config=EvalConfig(epochs=5),
            fitness_config=fitness,
        )
        result = evaluator.evaluate(str(program))
        assert result["combined_score"] == pytest.approx(0.7 * 0.25 + 0.3 * 0.80)
        mock_backend.train.assert_called_once()

    def test_evaluate_rejects_invalid_config(self, tmp_path: Any) -> None:
        """Invalid config returns zero score."""
        program = tmp_path / "bad.py"
        program.write_text('config = {"hidden_dim": 2000, "num_layers": 4, "num_heads": 8}\n')

        mock_backend = MagicMock()
        fitness = FitnessConfig(
            primary="spearman_rho",
            combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
        )
        evaluator = EvolveEvaluator(
            backend=mock_backend,
            target="scoutgpt",
            eval_config=EvalConfig(epochs=5),
            fitness_config=fitness,
        )
        result = evaluator.evaluate(str(program))
        assert result["combined_score"] == 0.0
        mock_backend.train.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run pytest src/tests/test_evolve_evaluator.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'evolve.evaluator'`

- [ ] **Step 3: Create target registry**

```python
# src/evolve/targets/__init__.py
"""Target registry for the evolve engine."""

from __future__ import annotations

# Targets are discovered by importlib in the evaluator bridge.
# This file exists to make targets/ a proper package.
```

```python
# src/evolve/targets/scoutgpt/__init__.py
"""ScoutGPT evolution target."""

from __future__ import annotations
```

- [ ] **Step 4: Implement evaluator bridge**

```python
# src/evolve/evaluator.py
"""Evaluator bridge — connects OpenEvolve's interface to our ComputeBackend."""

from __future__ import annotations

import importlib.util
import logging
import types
from typing import Any

from evolve.backends.base import ComputeBackend
from evolve.config import EvalConfig, FitnessConfig

logger = logging.getLogger(__name__)

# --- Search space bounds ---

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

_MAX_PARAM_COUNT = 20_000_000

_VALID_CONDITIONING_TYPES = frozenset({"additive", "cross_attention", "film", "gated"})


def validate_search_space(config: dict[str, Any]) -> bool:
    """Validate a candidate config against search space bounds."""
    for key, (lo, hi) in _BOUNDS.items():
        val = config.get(key)
        if val is not None and not (lo <= float(val) <= hi):
            logger.warning("Rejected: %s=%s outside [%s, %s]", key, val, lo, hi)
            return False

    # num_heads must divide hidden_dim
    hidden_dim = config.get("hidden_dim", 256)
    num_heads = config.get("num_heads", 8)
    if hidden_dim % num_heads != 0:
        logger.warning("Rejected: hidden_dim=%d not divisible by num_heads=%d", hidden_dim, num_heads)
        return False

    # Conditioning type must be known
    ctype = config.get("conditioning_type", "additive")
    if ctype not in _VALID_CONDITIONING_TYPES:
        logger.warning("Rejected: unknown conditioning_type=%s", ctype)
        return False

    return True


def _load_config_from_program(program_path: str) -> dict[str, Any]:
    """Dynamically import a candidate program and extract its config dict."""
    spec = importlib.util.spec_from_file_location("_candidate", program_path)
    if spec is None or spec.loader is None:
        msg = f"Cannot load program: {program_path}"
        raise ImportError(msg)
    module = types.ModuleType("_candidate")
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    config = getattr(module, "config", None)
    if not isinstance(config, dict):
        msg = f"Program {program_path} must define a 'config' dict"
        raise ValueError(msg)
    return config


class EvolveEvaluator:
    """Bridges OpenEvolve's evaluate(program_path) to our ComputeBackend."""

    def __init__(
        self,
        backend: ComputeBackend,
        target: str,
        eval_config: EvalConfig,
        fitness_config: FitnessConfig,
    ) -> None:
        self.backend = backend
        self.target = target
        self.eval_config = eval_config
        self.fitness_config = fitness_config

    def evaluate(self, program_path: str) -> dict[str, float]:
        """Load candidate config, validate, train, and return scored metrics."""
        try:
            candidate_config = _load_config_from_program(program_path)
        except (ImportError, ValueError, SyntaxError):
            logger.exception("Failed to load program: %s", program_path)
            return {"combined_score": 0.0, "error": 1.0}

        if not validate_search_space(candidate_config):
            return {"combined_score": 0.0, "rejected": 1.0}

        try:
            metrics = self.backend.train(
                candidate_config=candidate_config,
                target=self.target,
                epochs=self.eval_config.epochs,
                seed=self.eval_config.seed,
            )
        except Exception:
            logger.exception("Training failed for candidate: %s", program_path)
            return {"combined_score": 0.0, "error": 1.0}

        # Compute combined fitness score
        combined = sum(
            self.fitness_config.combined_weights.get(k, 0.0) * metrics.get(k, 0.0)
            for k in self.fitness_config.combined_weights
        )
        metrics["combined_score"] = combined
        logger.info("Candidate scored: combined=%.4f, metrics=%s", combined, metrics)
        return metrics
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run pytest src/tests/test_evolve_evaluator.py -v`
Expected: All 8 tests PASS

- [ ] **Step 6: Lint and type check**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run ruff check src/evolve/evaluator.py src/evolve/targets/ src/tests/test_evolve_evaluator.py && uv run pyright src/evolve/evaluator.py src/evolve/targets/`
Expected: All clean

- [ ] **Step 7: Commit**

```bash
git add src/evolve/evaluator.py src/evolve/targets/ src/tests/test_evolve_evaluator.py
git commit -m "feat(evolve): evaluator bridge with search space validation"
```

---

## Task 5: ScoutGPT target evaluator (training integration)

**Files:**
- Create: `src/evolve/targets/scoutgpt/evaluator.py`

This is the domain-specific glue that builds a ScoutGPT model from a candidate config, trains it, and returns fitness metrics. It reuses `train_loop()` and `evaluate_counterfactual_ranking()` from `scoutgpt_training.py`.

- [ ] **Step 1: Implement the ScoutGPT target evaluator**

```python
# src/evolve/targets/scoutgpt/evaluator.py
"""ScoutGPT target evaluator — trains a model from candidate config and returns fitness metrics."""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

# Reduced evaluation budget for evolution (vs. full training)
_EVOLVE_COUNTERFACTUAL_EPISODES = 200
_EVOLVE_COUNTERFACTUAL_PLAYERS = 50


def train_and_evaluate(
    candidate_config: dict[str, Any],
    device: str,
    epochs: int,
    seed: int,
) -> dict[str, float]:
    """Build model from candidate config, train, return evaluation metrics.

    This function is called by the LocalCudaBackend (and future backends).
    It imports torch lazily so the evolve package can be imported without torch.
    """
    import torch

    from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder
    from analytics.scoutgpt_training import (
        ScoutGPTDataset,
        evaluate_counterfactual_ranking,
        load_and_split_data,
        train_loop,
    )

    torch_device = torch.device(device)
    start_time = time.monotonic()

    # --- Build config from candidate dict ---
    # Extract training hyperparams (not part of ScoutGPTConfig)
    lr = candidate_config.get("learning_rate", 1e-4)
    batch_size = candidate_config.get("batch_size", 256)
    player_prediction_weight = candidate_config.get("player_prediction_weight", 0.0)

    # Build ScoutGPTConfig from remaining keys
    config_keys = {
        "hidden_dim", "num_layers", "num_heads", "dropout", "max_seq_len",
        "num_players", "spatial_mlp_dim", "vaep_loss_weight", "conditioning_type",
    }
    model_kwargs = {k: v for k, v in candidate_config.items() if k in config_keys}
    config = ScoutGPTConfig(**model_kwargs)

    # --- Load dataset (cached after first call within process) ---
    train_df, val_df, test_df = load_and_split_data(seed=seed)
    train_ds = ScoutGPTDataset(train_df, max_seq_len=config.max_seq_len)
    val_ds = ScoutGPTDataset(val_df, max_seq_len=config.max_seq_len)
    test_ds = ScoutGPTDataset(test_df, max_seq_len=config.max_seq_len)

    # --- Train ---
    model, history = train_loop(
        train_ds=train_ds,
        val_ds=val_ds,
        config=config,
        device=torch_device,
        epochs=epochs,
        batch_size=batch_size,
        lr=lr,
        patience=max(3, epochs // 2),  # Adaptive patience for short runs
    )

    # --- Evaluate ---
    model.eval()

    # Top-1 accuracy from the last validation epoch
    top1 = history["val_top1_accuracy"][-1] if history["val_top1_accuracy"] else 0.0

    # Counterfactual Spearman rho (reduced budget for speed)
    cf_results = evaluate_counterfactual_ranking(
        model=model,
        test_ds=test_ds,
        device=torch_device,
        num_episodes=_EVOLVE_COUNTERFACTUAL_EPISODES,
        num_players=_EVOLVE_COUNTERFACTUAL_PLAYERS,
    )

    elapsed = time.monotonic() - start_time
    param_count = sum(p.numel() for p in model.parameters())

    metrics = {
        "spearman_rho": cf_results["mean_spearman_rho"],
        "rho_std": cf_results["rho_std"],
        "top1_accuracy": top1,
        "val_loss": history["val_loss"][-1] if history["val_loss"] else float("inf"),
        "param_count": float(param_count),
        "training_time_seconds": elapsed,
        "epochs_trained": float(len(history["val_loss"])),
    }

    logger.info(
        "ScoutGPT candidate: rho=%.4f, top1=%.4f, params=%d, time=%.1fs",
        metrics["spearman_rho"],
        metrics["top1_accuracy"],
        param_count,
        elapsed,
    )

    # Clean up GPU memory for next candidate
    del model, train_ds, val_ds, test_ds
    torch.cuda.empty_cache()

    return metrics
```

- [ ] **Step 2: Verify the import `load_and_split_data` exists in scoutgpt_training.py**

Check that `scoutgpt_training.py` exports `load_and_split_data`. If this function does not exist with this exact name, find the actual function name that loads the dataset from HF Hub and splits it into train/val/test DataFrames, and update the import. The key thing is the target evaluator must be able to load the dataset without knowing the HF Hub path (the training module handles that).

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run python -c "from analytics.scoutgpt_training import load_and_split_data; print('OK')"`

If this fails, check the actual function name with: `grep -n "def.*split\|def.*load.*data" src/analytics/scoutgpt_training.py` and update the import accordingly.

- [ ] **Step 3: Lint and type check**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run ruff check src/evolve/targets/scoutgpt/evaluator.py && uv run pyright src/evolve/targets/scoutgpt/evaluator.py`
Expected: Clean (pyright may warn about torch imports — acceptable as warnings)

- [ ] **Step 4: Commit**

```bash
git add src/evolve/targets/scoutgpt/evaluator.py
git commit -m "feat(evolve): ScoutGPT target evaluator with counterfactual scoring"
```

---

## Task 6: Seed programs

**Files:**
- Create: `src/evolve/targets/scoutgpt/seed_programs/__init__.py`
- Create: `src/evolve/targets/scoutgpt/seed_programs/additive.py`
- Create: `src/evolve/targets/scoutgpt/seed_programs/cross_attention.py`
- Create: `src/evolve/targets/scoutgpt/seed_programs/film.py`
- Create: `src/evolve/targets/scoutgpt/seed_programs/gated.py`

- [ ] **Step 1: Create seed programs package**

```python
# src/evolve/targets/scoutgpt/seed_programs/__init__.py
"""Seed programs for ScoutGPT evolution."""

from __future__ import annotations
```

- [ ] **Step 2: Create additive seed (baseline)**

```python
# src/evolve/targets/scoutgpt/seed_programs/additive.py
"""Seed 1: Additive player conditioning (current baseline).

Sum player embedding with all other embeddings. This is the existing
ScoutGPT architecture that achieves 81.5% top-1 but only 0.094 rho.
"""

config = {
    "conditioning_type": "additive",
    "hidden_dim": 256,
    "num_layers": 6,
    "num_heads": 8,
    "dropout": 0.1,
    "max_seq_len": 128,
    "spatial_mlp_dim": 64,
    "vaep_loss_weight": 0.1,
    "learning_rate": 1e-4,
    "weight_decay": 0.01,
    "batch_size": 256,
}
```

- [ ] **Step 3: Create cross-attention seed**

```python
# src/evolve/targets/scoutgpt/seed_programs/cross_attention.py
"""Seed 2: Cross-attention player conditioning.

Player embedding as K/V in a dedicated cross-attention layer.
Hypothesis: separating player signal from action signal prevents dilution.
Fewer transformer layers to compensate for added cross-attention compute.
"""

config = {
    "conditioning_type": "cross_attention",
    "hidden_dim": 256,
    "num_layers": 4,
    "num_heads": 8,
    "dropout": 0.1,
    "max_seq_len": 128,
    "spatial_mlp_dim": 64,
    "vaep_loss_weight": 0.1,
    "learning_rate": 3e-4,
    "weight_decay": 0.01,
    "batch_size": 256,
}
```

- [ ] **Step 4: Create FiLM seed**

```python
# src/evolve/targets/scoutgpt/seed_programs/film.py
"""Seed 3: Feature-wise Linear Modulation (FiLM) player conditioning.

Player embedding predicts per-channel scale + shift applied to the
action embedding (Perez et al. 2018). Multiplicative interaction gives
the player stronger control over the representation.
"""

config = {
    "conditioning_type": "film",
    "hidden_dim": 256,
    "num_layers": 6,
    "num_heads": 8,
    "dropout": 0.1,
    "max_seq_len": 128,
    "spatial_mlp_dim": 64,
    "vaep_loss_weight": 0.1,
    "learning_rate": 1e-4,
    "weight_decay": 0.01,
    "batch_size": 256,
}
```

- [ ] **Step 5: Create gated seed (with auxiliary player prediction loss)**

```python
# src/evolve/targets/scoutgpt/seed_programs/gated.py
"""Seed 4: Gated player conditioning with auxiliary player prediction loss.

Learned gate: sigmoid(W * player_emb) . action_emb — player selectively
amplifies/suppresses action features. Includes an auxiliary loss that
predicts the player from the hidden state, forcing representations to
retain player identity throughout the network.
"""

config = {
    "conditioning_type": "gated",
    "hidden_dim": 256,
    "num_layers": 6,
    "num_heads": 8,
    "dropout": 0.1,
    "max_seq_len": 128,
    "spatial_mlp_dim": 64,
    "vaep_loss_weight": 0.1,
    "player_prediction_weight": 0.05,
    "learning_rate": 1e-4,
    "weight_decay": 0.01,
    "batch_size": 256,
}
```

- [ ] **Step 6: Lint**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run ruff check src/evolve/targets/scoutgpt/seed_programs/ && uv run ruff format --check src/evolve/targets/scoutgpt/seed_programs/`
Expected: Clean

- [ ] **Step 7: Commit**

```bash
git add src/evolve/targets/scoutgpt/seed_programs/
git commit -m "feat(evolve): four ScoutGPT seed programs (additive, cross_attn, film, gated)"
```

---

## Task 7: Runner (CLI entry point + OpenEvolve wiring)

**Files:**
- Create: `src/evolve/runner.py`
- Create: `src/evolve/targets/scoutgpt/config.yaml`

- [ ] **Step 1: Create the default ScoutGPT evolution config**

```yaml
# src/evolve/targets/scoutgpt/config.yaml
target: scoutgpt
description: "Evolve ScoutGPT conditioning mechanism to maximize counterfactual Spearman rho"

fitness:
  primary: spearman_rho
  secondary: top1_accuracy
  combined_weights:
    spearman_rho: 0.7
    top1_accuracy: 0.3
  minimize: false

evaluation:
  epochs: 5
  dataset: "luxury-lakehouse/scoutgpt-training-data"
  timeout_seconds: 900
  seed: 42

backend:
  type: local_cuda
  device: "cuda:0"

llm:
  models:
    - name: "claude-sonnet-4-20250514"
      weight: 0.8
      api_base: "https://api.anthropic.com/v1/"
      api_key_env: "ANTHROPIC_API_KEY"
    - name: "claude-haiku-4-5-20251001"
      weight: 0.2
      api_base: "https://api.anthropic.com/v1/"
      api_key_env: "ANTHROPIC_API_KEY"
  temperature: 0.7
  max_tokens: 4096

evolution:
  iterations: 150
  population_size: 200
  num_islands: 3
  migration_interval: 30
  parallel_evaluations: 1
  diff_based: true
  early_stopping_patience: 40
```

- [ ] **Step 2: Implement the CLI runner**

```python
# src/evolve/runner.py
"""CLI entry point for the evolve engine."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from evolve.backends import create_backend
from evolve.config import EvolveConfig
from evolve.evaluator import EvolveEvaluator

logger = logging.getLogger(__name__)

_RESULTS_DIR = Path("results/evolve")


def _find_default_config(target: str) -> Path:
    """Find the default config.yaml for a target."""
    # Check relative to this file's package
    pkg_dir = Path(__file__).parent / "targets" / target
    config_path = pkg_dir / "config.yaml"
    if config_path.exists():
        return config_path
    msg = f"No default config found for target '{target}' at {config_path}"
    raise FileNotFoundError(msg)


def _find_seed_programs(target: str) -> list[Path]:
    """Find seed program files for a target."""
    pkg_dir = Path(__file__).parent / "targets" / target / "seed_programs"
    if not pkg_dir.exists():
        return []
    return sorted(p for p in pkg_dir.glob("*.py") if p.name != "__init__.py")


def _setup_results_dir(target: str) -> Path:
    """Create timestamped results directory."""
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    results = _RESULTS_DIR / target / ts
    results.mkdir(parents=True, exist_ok=True)
    return results


def main(argv: list[str] | None = None) -> None:
    """Run evolutionary architecture search."""
    parser = argparse.ArgumentParser(description="Evolve Engine — LLM-guided architecture search")
    parser.add_argument("--target", required=True, help="Target to evolve (e.g., scoutgpt)")
    parser.add_argument("--config", type=Path, default=None, help="Path to config YAML (default: target's config.yaml)")
    parser.add_argument("--backend", default=None, help="Override backend type")
    parser.add_argument("--device", default=None, help="Override device (e.g., cuda:0)")
    parser.add_argument("--iterations", type=int, default=None, help="Override iteration count")
    parser.add_argument("--resume", type=Path, default=None, help="Resume from checkpoint directory")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    # Load config
    config_path = args.config or _find_default_config(args.target)
    config = EvolveConfig.from_yaml(config_path)

    # Apply CLI overrides
    if args.backend:
        config.backend.type = args.backend
    if args.device:
        config.backend.device = args.device
    if args.iterations:
        config.evolution.iterations = args.iterations

    # Create backend and verify availability
    backend = create_backend(config.backend)
    if not backend.available():
        logger.error("Backend '%s' is not available on this machine", config.backend.type)
        sys.exit(1)

    # Setup results directory
    results_dir = _setup_results_dir(args.target)
    logger.info("Results will be saved to: %s", results_dir)

    # Save config snapshot
    import yaml

    (results_dir / "config.yaml").write_text(yaml.dump(config.model_dump(), default_flow_style=False))

    # Create evaluator bridge
    evaluator = EvolveEvaluator(
        backend=backend,
        target=args.target,
        eval_config=config.evaluation,
        fitness_config=config.fitness,
    )

    # Find seed programs
    seeds = _find_seed_programs(args.target)
    if not seeds:
        logger.error("No seed programs found for target '%s'", args.target)
        sys.exit(1)
    logger.info("Found %d seed programs: %s", len(seeds), [s.name for s in seeds])

    # --- Run seed baselines first ---
    seed_results: dict[str, dict[str, float]] = {}
    for seed_path in seeds:
        logger.info("Evaluating seed: %s", seed_path.name)
        result = evaluator.evaluate(str(seed_path))
        seed_results[seed_path.name] = result
        logger.info("Seed %s: combined=%.4f", seed_path.name, result.get("combined_score", 0.0))

    (results_dir / "seed_results").mkdir(exist_ok=True)
    for name, result in seed_results.items():
        (results_dir / "seed_results" / f"{name}.json").write_text(json.dumps(result, indent=2))

    # --- Launch OpenEvolve ---
    try:
        from openevolve import run_evolution  # type: ignore[import-untyped]
    except ImportError:
        logger.error(
            "OpenEvolve is not installed. Install with: uv sync --extra evolve"
        )
        sys.exit(1)

    # Pick the best seed as the initial program
    best_seed = max(seeds, key=lambda s: seed_results.get(s.name, {}).get("combined_score", 0.0))
    logger.info("Best seed: %s (combined=%.4f)", best_seed.name, seed_results[best_seed.name]["combined_score"])

    # Build OpenEvolve config dict
    oe_config = {
        "llm": {
            "models": [
                {
                    "name": m.name,
                    "weight": m.weight,
                    "api_base": m.api_base,
                }
                for m in config.llm.models
            ],
            "temperature": config.llm.temperature,
            "max_tokens": config.llm.max_tokens,
        },
        "evolution": {
            "max_iterations": config.evolution.iterations,
            "population_size": config.evolution.population_size,
            "num_islands": config.evolution.num_islands,
            "migration_interval": config.evolution.migration_interval,
            "parallel_evaluations": config.evolution.parallel_evaluations,
            "diff_based_evolution": config.evolution.diff_based,
        },
        "evaluator": {
            "timeout": config.evaluation.timeout_seconds,
        },
    }

    # Set API keys from env vars
    import os

    for m in config.llm.models:
        key = os.environ.get(m.api_key_env, "")
        if not key:
            logger.warning("Environment variable %s not set for model %s", m.api_key_env, m.name)
        os.environ.setdefault("OPENAI_API_KEY", key)

    logger.info("Starting evolution: %d iterations, %d islands", config.evolution.iterations, config.evolution.num_islands)

    result = run_evolution(
        initial_program=str(best_seed),
        evaluator=evaluator.evaluate,
        config=oe_config,
        iterations=config.evolution.iterations,
    )

    # Save best result
    if hasattr(result, "best_code"):
        (results_dir / "best_program.py").write_text(result.best_code)
    if hasattr(result, "best_score"):
        best_metrics = {"combined_score": result.best_score}
        (results_dir / "metrics.json").write_text(json.dumps(best_metrics, indent=2))

    logger.info("Evolution complete. Best score: %s", getattr(result, "best_score", "unknown"))
    logger.info("Results saved to: %s", results_dir)
```

- [ ] **Step 3: Verify the runner can at least parse args**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run python -c "from evolve.runner import main; main(['--target', 'scoutgpt', '--help'])" 2>&1 || true`
Expected: Prints help text (may exit with code 0)

- [ ] **Step 4: Lint and type check**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run ruff check src/evolve/runner.py && uv run ruff format --check src/evolve/runner.py && uv run pyright src/evolve/runner.py`
Expected: Clean (openevolve import may be a pyright warning — acceptable)

- [ ] **Step 5: Commit**

```bash
git add src/evolve/runner.py src/evolve/targets/scoutgpt/config.yaml
git commit -m "feat(evolve): CLI runner with OpenEvolve integration and seed evaluation"
```

---

## Task 8: Workflow card and final integration

**Files:**
- Create: `workflow-cards/wf-evolve-scoutgpt.yaml`
- Modify: `src/evolve/__init__.py` (update exports)

- [ ] **Step 1: Create workflow card**

```yaml
# workflow-cards/wf-evolve-scoutgpt.yaml
name: "Evolve ScoutGPT Architecture"
id: wf-evolve-scoutgpt
version: "1.0.0"
status: development
type: optimization
domain: player-valuation
owners:
  - name: "Karsten Nielsen"
tags:
  - alphaevolve
  - architecture-search
  - scoutgpt
  - counterfactual

references:
  - citation: "Romera-Paredes et al. (2025). AlphaEvolve: A coding agent for scientific and algorithmic discovery. arXiv:2506.13131"
    role: "Evolutionary search methodology — MAP-Elites + LLM-guided mutation"
  - citation: "Hong et al. (2025). ScoutGPT: Player-Conditioned Counterfactual Prediction. arXiv:2512.17266"
    role: "Base model architecture being evolved"
  - citation: "Perez et al. (2018). FiLM: Visual Reasoning with a General Conditioning Layer. AAAI."
    role: "Feature-wise Linear Modulation conditioning mechanism"

inputs:
  datasets:
    - id: scoutgpt-training-data
      source: "luxury-lakehouse/scoutgpt-training-data"
      description: "894K SPADL possession episodes with player IDs and VAEP values"

outputs:
  models:
    - id: scoutgpt-evolved
      destination: "results/evolve/scoutgpt/{timestamp}/best_program.py"
      description: "Best architecture config discovered by evolution (local, not published)"

execution:
  training:
    trigger: manual
    runtime: local
    flavor: "RTX 5070 Ti (16 GB) or DGX Spark (128 GB)"
    script: "uv run evolve --target scoutgpt"
    timeout: "24h"

depends_on:
  - wf-scoutgpt-export

idempotency:
  strategy: full-overwrite
  key: timestamp
  description: "Each evolution run creates a new timestamped results directory"

cost:
  training:
    runtime: local
    flavor: "RTX 5070 Ti"
    rate_usd_per_hour: 0.00
    typical_duration_minutes: 750
    typical_cost_usd: 0.00
  llm_api:
    rate_usd_per_iteration: 0.08
    typical_iterations: 150
    typical_cost_usd: 12.00

monitoring:
  freshness_sla_hours: 168
  metrics:
    - name: "best_spearman_rho"
      threshold_min: 0.15
      baseline: 0.094
    - name: "best_top1_accuracy"
      threshold_min: 0.75
      baseline: 0.815

links:
  source_code:
    - "src/evolve/"
    - "src/analytics/scoutgpt_decoder.py"
    - "docs/superpowers/specs/2026-04-04-evolve-engine-design.md"

---

## Overview

LLM-guided evolutionary architecture search for ScoutGPT's player conditioning mechanism. Uses OpenEvolve (Apache 2.0 implementation of DeepMind's AlphaEvolve) with Claude as the mutation LLM. Trains candidates on local GPU at zero compute cost.

The primary goal is improving counterfactual Spearman rho (currently 0.094) by evolving the conditioning mechanism that controls how player identity influences action predictions.

## Architecture

Four conditioning mechanisms form the initial population: additive (baseline), cross-attention, FiLM, and gated. OpenEvolve's MAP-Elites algorithm maintains diversity across islands while the LLM generates semantically meaningful mutations to model configs.

Pluggable compute backends allow training on local CUDA (RTX 5070 Ti), Docker containers, HF Jobs, or remote SSH — configured via YAML.

## Evaluation

Each candidate is trained for 5 epochs and scored by `0.7 * spearman_rho + 0.3 * top1_accuracy`. Counterfactual evaluation uses 200 episodes x 50 players (reduced from full 1000x100 for speed). The winning candidate gets a full 30-epoch training run with complete evaluation.
```

- [ ] **Step 2: Update __init__.py exports**

```python
# src/evolve/__init__.py
"""Evolve Engine — LLM-guided evolutionary architecture search."""

from __future__ import annotations

from evolve.config import EvolveConfig
from evolve.evaluator import EvolveEvaluator

__all__ = ["EvolveConfig", "EvolveEvaluator"]
```

- [ ] **Step 3: Run full lint and type check across all evolve code**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run ruff check src/evolve/ && uv run ruff format --check src/evolve/ && uv run pyright src/evolve/`
Expected: All clean

- [ ] **Step 4: Run all evolve tests**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run pytest src/tests/test_evolve_config.py src/tests/test_evolve_evaluator.py src/tests/test_scoutgpt_conditioning.py -v`
Expected: All tests PASS

- [ ] **Step 5: Run full test suite to verify no regressions**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run pytest src/tests/ -v`
Expected: All tests PASS (existing + new)

- [ ] **Step 6: Validate workflow card**

Run: `cd D:/Development/karstenskyt__luxury-lakehouse-d32 && uv run validate_workflow_cards`
Expected: All cards valid including the new `wf-evolve-scoutgpt`

- [ ] **Step 7: Commit**

```bash
git add workflow-cards/wf-evolve-scoutgpt.yaml src/evolve/__init__.py
git commit -m "feat(evolve): workflow card and final integration"
```

---

## Post-Implementation: First Evolution Run

After all tasks are committed and tests pass, the first evolution run can be launched:

```bash
# Install evolve dependencies
uv sync --extra evolve

# Set API key
export ANTHROPIC_API_KEY="sk-ant-..."

# Quick test run (10 iterations, verify the loop works)
uv run evolve --target scoutgpt --iterations 10

# Full overnight run
uv run evolve --target scoutgpt --iterations 150
```

Monitor progress in `results/evolve/scoutgpt/{timestamp}/evolution_log.jsonl`.
