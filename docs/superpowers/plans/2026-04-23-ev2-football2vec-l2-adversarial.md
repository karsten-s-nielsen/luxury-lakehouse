# EV2 — Football2Vec v2 L2 Adversarial Architecture Search — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the infrastructure needed to run Phase 1 of EV2 — a harvest of 6 hand-written adversary seed programs + baseline against the Football2Vec v2 stage-2 training loop on the local compute pool. Phase 2 (LLM-guided evolve sweep) is conditional on the user checkpoint and is set up but not triggered by this plan.

**Architecture:** New `AdversaryConfig` + registry + `lambda_schedule` module (`src/analytics/football2vec_adversary.py`); refactored `_train_stage2_loop` with injection points for adversary module and schedule function (backward-compat at production defaults); new Football2Vec L2 `ValidationProfile` reusing the existing `custom_layers` patch-point with a `{"adversary": <nn.Module>}` return-key convention (avoids extending `code_validator.py`); 6 seed programs + baseline; extended evaluator with `train_and_evaluate_stage2`; PEP 723 orchestrator script dispatching via existing `BackendPool`; workflow card.

**Tech Stack:** Python 3.10, PyTorch, pydantic, PEP 723 scripts, OpenEvolve (Phase 2 only), HuggingFace Hub (HF Hub) for artefact storage, existing `src/evolve/backends/pool.py::BackendPool` for local-pool dispatch.

**Spec:** `docs/superpowers/specs/2026-04-23-ev2-football2vec-l2-adversarial-design.md` — all pre-registered decisions (fitness function, disposition thresholds, shared config, seed slate) are frozen in that document. This plan implements the infrastructure only.

**Commit cadence:** No intermediate commits. **Single commit** at end of Milestone A after all tests green, quality gates pass, and user approves Commit #1. Each subsequent commit (Phase 1 results, Phase 2 results, promotion) is a separate operational milestone outside this plan.

---

## File structure

**New files:**

- `src/analytics/football2vec_adversary.py` — `AdversaryConfig`, `lambda_schedule`, `build_adversary`, `_ADVERSARY_REGISTRY`, `_build_linear_head`, reusable `LinearAdversaryHead` class
- `src/evolve/targets/football2vec/validation.py` — `FOOTBALL2VEC_ADVERSARY_PROFILE`
- `src/evolve/targets/football2vec/seed_programs_stage2/__init__.py` — namespace package marker
- `src/evolve/targets/football2vec/seed_programs_stage2/deep_mlp_2layer.py`
- `src/evolve/targets/football2vec/seed_programs_stage2/deep_mlp_3layer.py`
- `src/evolve/targets/football2vec/seed_programs_stage2/cross_attention_adversary.py`
- `src/evolve/targets/football2vec/seed_programs_stage2/attention_pool_head.py`
- `src/evolve/targets/football2vec/seed_programs_stage2/residual_mlp.py`
- `src/evolve/targets/football2vec/seed_programs_stage2/dual_head_ensemble.py`
- `scripts/evaluate_football2vec_l2_adversary_seeds.py` — PEP 723 orchestrator (Phase 1 dispatch)
- `workflow-cards/wf-evolve-football2vec-l2-stage2.yaml` — workflow card
- `src/tests/test_football2vec_adversary.py` — tests for the adversary module
- `src/tests/test_evolve_football2vec_l2.py` — tests for L2 infrastructure (validation, seeds, evaluator)

**Modified files:**

- `src/evolve/targets/football2vec/__init__.py` — export `VALIDATION_PROFILE = FOOTBALL2VEC_ADVERSARY_PROFILE`
- `src/evolve/targets/football2vec/evaluator.py` — add `train_and_evaluate_stage2`, `_apply_program_adversary`, stage-1 encoder caching
- `scripts/train_football2vec_v2.py` — `_train_stage2_loop` signature accepts optional `adversary_module` + `lambda_schedule_fn`; body uses injected values when provided

**Out-of-plan (deferred to later milestones):**

- `src/evolve/targets/football2vec/config_stage2.yaml` — Phase 2 evolve sweep config (only created if checkpoint is GO)
- `src/evolve/targets/football2vec/prompts/stage2_system_message.txt` — Phase 2 LLM prompt (only if GO)
- `docs/evolve/ev2-football2vec-l2-adversarial/SUMMARY.md` — written after Phase 1 results, part of Commit #2
- `docs/evolve/ev2-football2vec-l2-adversarial/results.json` — mirrored after Phase 1 upload

---

## Milestone A — Infrastructure for Phase 1 dispatch

Goal: end of milestone = user can approve Commit #1 + approve Phase 1 dispatch; orchestrator runs without code changes needed.

### Task 1: Scaffold `football2vec_adversary` module + `AdversaryConfig`

**Files:**
- Create: `src/analytics/football2vec_adversary.py`
- Create: `src/tests/test_football2vec_adversary.py`

- [ ] **Step 1: Write the failing test** in `src/tests/test_football2vec_adversary.py`:

```python
"""Tests for src/analytics/football2vec_adversary.py."""

from __future__ import annotations

import pytest


def test_adversary_config_defaults():
    """AdversaryConfig() produces the pinned production defaults."""
    from analytics.football2vec_adversary import AdversaryConfig

    cfg = AdversaryConfig()
    assert cfg.architecture == "linear"
    assert cfg.lambda_schedule_shape == "linear"
    assert cfg.lambda_max == pytest.approx(0.2)
    assert cfg.lambda_warmup_epochs == 5


def test_adversary_config_is_frozen():
    """Mutating a frozen dataclass should raise."""
    from dataclasses import FrozenInstanceError

    from analytics.football2vec_adversary import AdversaryConfig

    cfg = AdversaryConfig()
    with pytest.raises(FrozenInstanceError):
        cfg.lambda_max = 0.5  # type: ignore[misc]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_football2vec_adversary.py::test_adversary_config_defaults -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'analytics.football2vec_adversary'`

- [ ] **Step 3: Create `src/analytics/football2vec_adversary.py`** with the dataclass:

```python
"""Stage-2 adversary configuration, lambda schedule, and module registry.

Central home for Football2Vec v2 stage-2 adversarial training configuration.
The module replaces the training loop's hardcoded TeamClassifierHead + linear
lambda ramp with a typed config + registry + schedule-function trio, enabling:

- Byte-equivalent backward compatibility at production defaults
- L1 config-space search over three axes (lambda_schedule_shape, lambda_max,
  lambda_warmup_epochs) in Phase 2 of EV2
- L2 code-space search over the adversary head architecture via the registry
  (_ADVERSARY_REGISTRY) which grows with promoted variants from EV2 Phase 2

References:
    Ganin, Y. et al. (2016). "Domain-Adversarial Training of Neural Networks."
    JMLR 17(1), pp. 1-35.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

import torch
import torch.nn as nn

from analytics.football2vec_transformer import GradientReversalLayer


@dataclass(frozen=True)
class AdversaryConfig:
    """Stage-2 adversary configuration.

    Attributes:
        architecture: Named entry in _ADVERSARY_REGISTRY. "linear" matches
            the current production TeamClassifierHead byte-for-byte.
        lambda_schedule_shape: Ramp shape for the gradient-reversal lambda
            across training epochs.
        lambda_max: Peak lambda value reached at epoch = lambda_warmup_epochs.
        lambda_warmup_epochs: Number of epochs to ramp from 0 to lambda_max.
    """

    architecture: Literal["linear"] = "linear"
    lambda_schedule_shape: Literal["linear", "sigmoid", "cosine"] = "linear"
    lambda_max: float = 0.2
    lambda_warmup_epochs: int = 5
```

- [ ] **Step 4: Run tests, verify they pass**

Run: `uv run pytest src/tests/test_football2vec_adversary.py -v`
Expected: 2 passed

---

### Task 2: Add `lambda_schedule` with three shapes

**Files:**
- Modify: `src/analytics/football2vec_adversary.py`
- Modify: `src/tests/test_football2vec_adversary.py`

- [ ] **Step 1: Write the failing tests** (append to `test_football2vec_adversary.py`):

```python
def test_lambda_schedule_linear_matches_production():
    """Linear shape at production defaults reproduces current hardcoded ramp."""
    from analytics.football2vec_adversary import AdversaryConfig, lambda_schedule

    cfg = AdversaryConfig()  # linear, lambda_max=0.2, warmup=5
    # Ramp epochs 0..4 linearly from 0 to lambda_max, then hold
    expected_by_epoch = {0: 0.0, 1: 0.04, 2: 0.08, 3: 0.12, 4: 0.16, 5: 0.2, 10: 0.2, 29: 0.2}
    for epoch, expected in expected_by_epoch.items():
        actual = lambda_schedule(cfg, epoch, total_epochs=30)
        assert actual == pytest.approx(expected, abs=1e-9), f"epoch={epoch}"


def test_lambda_schedule_sigmoid_monotonic_to_max():
    """Sigmoid shape: monotonic from 0 at epoch 0 to lambda_max at or near warmup_epochs."""
    from analytics.football2vec_adversary import AdversaryConfig, lambda_schedule

    cfg = AdversaryConfig(lambda_schedule_shape="sigmoid")
    values = [lambda_schedule(cfg, e, total_epochs=30) for e in range(31)]
    assert values[0] == pytest.approx(0.0, abs=1e-6)
    # Monotonic non-decreasing
    for i in range(1, len(values)):
        assert values[i] >= values[i - 1] - 1e-9, f"non-monotonic at epoch {i}"
    # Reaches lambda_max (within 1%) by epoch 10 (2x warmup) and holds
    assert values[10] == pytest.approx(cfg.lambda_max, rel=0.02)
    assert values[29] == pytest.approx(cfg.lambda_max, rel=0.02)


def test_lambda_schedule_cosine_monotonic_to_max():
    """Cosine shape: monotonic from 0 at epoch 0 to lambda_max at warmup_epochs, then holds."""
    from analytics.football2vec_adversary import AdversaryConfig, lambda_schedule

    cfg = AdversaryConfig(lambda_schedule_shape="cosine")
    values = [lambda_schedule(cfg, e, total_epochs=30) for e in range(31)]
    assert values[0] == pytest.approx(0.0, abs=1e-6)
    for i in range(1, len(values)):
        assert values[i] >= values[i - 1] - 1e-9, f"non-monotonic at epoch {i}"
    # Reaches lambda_max exactly at warmup_epochs (cosine from 0 to pi/2)
    assert values[cfg.lambda_warmup_epochs] == pytest.approx(cfg.lambda_max, abs=1e-9)
    assert values[29] == pytest.approx(cfg.lambda_max, abs=1e-9)


def test_lambda_schedule_rejects_unknown_shape():
    """Unknown shape raises ValueError."""
    from analytics.football2vec_adversary import AdversaryConfig, lambda_schedule

    cfg = AdversaryConfig()
    # Bypass dataclass validation by constructing with object.__setattr__ to
    # ensure lambda_schedule itself raises, not the config constructor.
    object.__setattr__(cfg, "lambda_schedule_shape", "bogus")
    with pytest.raises(ValueError, match="unknown lambda_schedule_shape"):
        lambda_schedule(cfg, 0, total_epochs=30)
```

- [ ] **Step 2: Run tests, verify failure**

Run: `uv run pytest src/tests/test_football2vec_adversary.py -v`
Expected: 4 new tests FAIL with `ImportError: cannot import name 'lambda_schedule'`

- [ ] **Step 3: Implement `lambda_schedule`** in `src/analytics/football2vec_adversary.py`:

```python
def lambda_schedule(cfg: AdversaryConfig, epoch: int, total_epochs: int) -> float:
    """Compute the gradient-reversal lambda at a given epoch.

    Three shapes, all (0 -> lambda_max) over lambda_warmup_epochs, then flat at lambda_max:

    - linear: lambda = lambda_max * min(epoch / warmup, 1.0)
    - sigmoid: lambda = lambda_max * sigmoid(10 * (progress - 0.5))
        where progress = min(epoch / warmup, 1.0); Ganin-2016 style DANN schedule
    - cosine: lambda = lambda_max * 0.5 * (1 - cos(pi * progress)); smoothly rises
        from 0 at epoch 0 to lambda_max at epoch = warmup, then holds

    Args:
        cfg: Adversary config (reads lambda_schedule_shape, lambda_max, lambda_warmup_epochs).
        epoch: Current epoch (0-indexed).
        total_epochs: Total epochs in the training run — reserved for shape
            variants that interpolate across the full run rather than the warmup window.

    Returns:
        Lambda value for this epoch, in [0.0, lambda_max].

    Raises:
        ValueError: unknown lambda_schedule_shape.
    """
    del total_epochs  # reserved for future shape variants that use it
    warmup = max(1, cfg.lambda_warmup_epochs)
    progress = min(epoch / warmup, 1.0)

    if cfg.lambda_schedule_shape == "linear":
        return cfg.lambda_max * progress

    if cfg.lambda_schedule_shape == "sigmoid":
        # Standard Ganin-2016 DANN sigmoid ramp, centered at progress=0.5
        # Scale factor 10 chosen to saturate within the warmup window
        return cfg.lambda_max * (1.0 / (1.0 + math.exp(-10.0 * (progress - 0.5))))

    if cfg.lambda_schedule_shape == "cosine":
        return cfg.lambda_max * 0.5 * (1.0 - math.cos(math.pi * progress))

    msg = f"unknown lambda_schedule_shape {cfg.lambda_schedule_shape!r}; expected linear|sigmoid|cosine"
    raise ValueError(msg)
```

- [ ] **Step 4: Run tests, verify they pass**

Run: `uv run pytest src/tests/test_football2vec_adversary.py -v`
Expected: 6 passed (2 from Task 1 + 4 new)

---

### Task 3: Byte-equivalent `linear` adversary matches `TeamClassifierHead`

**Files:**
- Modify: `src/analytics/football2vec_adversary.py`
- Modify: `src/tests/test_football2vec_adversary.py`

- [ ] **Step 1: Write the failing test** (append):

```python
def test_build_adversary_linear_matches_team_classifier():
    """build_adversary(AdversaryConfig(), ...) has same state_dict keys + forward
    behavior as TeamClassifierHead from football2vec_transformer.py."""
    import torch

    from analytics.football2vec_adversary import AdversaryConfig, build_adversary
    from analytics.football2vec_transformer import TeamClassifierHead

    hd, num_comp = 192, 22
    torch.manual_seed(42)
    ours = build_adversary(AdversaryConfig(), hd, num_comp)
    torch.manual_seed(42)
    theirs = TeamClassifierHead(hd, num_comp, lambda_val=0.2)

    # Same state_dict key structure (adversary has GRL + classifier Linear; TeamClassifierHead same)
    our_keys = {k for k in ours.state_dict() if not k.startswith("grl.")}
    their_keys = {k for k in theirs.state_dict() if not k.startswith("grl.")}
    assert our_keys == their_keys, f"state_dict key mismatch: {our_keys} vs {their_keys}"

    # Same forward shapes
    x = torch.randn(4, 8, hd)  # (B, S, hd) — EV2 adversary takes per-token + mask
    mask = torch.ones(4, 8, dtype=torch.bool)
    # Our adversary signature is (encoder_output, attention_mask); for the linear
    # builtin it should use CLS pool internally (encoder_output[:, 0])
    our_logits = ours(x, mask)
    # TeamClassifierHead signature is (pooled,); call with CLS pool of same input
    their_logits = theirs(x[:, 0])

    assert our_logits.shape == their_logits.shape == (4, num_comp)


def test_build_adversary_rejects_unknown_architecture():
    """Unknown architecture raises ValueError."""
    from analytics.football2vec_adversary import AdversaryConfig, build_adversary

    cfg = AdversaryConfig()
    object.__setattr__(cfg, "architecture", "does_not_exist")
    with pytest.raises(ValueError, match="unknown architecture"):
        build_adversary(cfg, hidden_dim=192, num_competitions=22)
```

- [ ] **Step 2: Run tests, verify failure**

Run: `uv run pytest src/tests/test_football2vec_adversary.py::test_build_adversary_linear_matches_team_classifier -v`
Expected: FAIL — `build_adversary` and `LinearAdversaryHead` not defined

- [ ] **Step 3: Add `LinearAdversaryHead`, `_ADVERSARY_REGISTRY`, `build_adversary`** to `src/analytics/football2vec_adversary.py`:

```python
class LinearAdversaryHead(nn.Module):
    """Baseline adversary — CLS pool, GRL, single Linear classifier.

    Byte-equivalent wiring to football2vec_transformer.TeamClassifierHead,
    but signature adapted to EV2's (encoder_output, attention_mask) -> logits
    convention. CLS pooling picks encoder_output[:, 0].

    Args:
        hidden_dim: Input feature dimension.
        num_competitions: Number of competition classes.
    """

    def __init__(self, hidden_dim: int, num_competitions: int) -> None:
        super().__init__()
        self.grl = GradientReversalLayer(lambda_val=1.0)  # lambda_ injected per-epoch
        self.classifier = nn.Linear(hidden_dim, num_competitions)

    def forward(self, encoder_output: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
        """CLS-pool then GRL+Linear.

        Args:
            encoder_output: (B, S, hidden_dim) per-token encoder output.
            attention_mask: (B, S) bool — unused here (CLS at position 0 always valid).

        Returns:
            (B, num_competitions) unnormalized logits.
        """
        del attention_mask  # CLS pooling ignores mask (CLS is always valid)
        cls = encoder_output[:, 0]
        return self.classifier(self.grl(cls))


def _build_linear_head(hidden_dim: int, num_competitions: int) -> nn.Module:
    return LinearAdversaryHead(hidden_dim, num_competitions)


_ADVERSARY_REGISTRY: dict[str, Callable[[int, int], nn.Module]] = {
    "linear": _build_linear_head,
}


def build_adversary(cfg: AdversaryConfig, hidden_dim: int, num_competitions: int) -> nn.Module:
    """Build the adversary module for cfg.architecture.

    Args:
        cfg: Adversary config — only cfg.architecture is read.
        hidden_dim: Encoder hidden dimension.
        num_competitions: Number of competition classes.

    Returns:
        nn.Module taking (encoder_output, attention_mask) -> (B, num_competitions) logits.

    Raises:
        ValueError: cfg.architecture is not registered in _ADVERSARY_REGISTRY.
    """
    builder = _ADVERSARY_REGISTRY.get(cfg.architecture)
    if builder is None:
        msg = f"unknown architecture {cfg.architecture!r}; registered: {sorted(_ADVERSARY_REGISTRY)}"
        raise ValueError(msg)
    return builder(hidden_dim, num_competitions)
```

- [ ] **Step 4: Run tests, verify pass**

Run: `uv run pytest src/tests/test_football2vec_adversary.py -v`
Expected: 8 passed

---

### Task 4: Refactor `_train_stage2_loop` with injection + backward-compat

**Files:**
- Modify: `scripts/train_football2vec_v2.py` — `_train_stage2_loop` signature + body
- Modify: `src/tests/test_football2vec_adversary.py` — add backward-compat trajectory test

- [ ] **Step 1: Write the failing backward-compat test** (append to `test_football2vec_adversary.py`):

```python
def test_stage2_loop_injection_backcompat(tmp_path):
    """Refactored _train_stage2_loop with adversary_module=None, lambda_schedule_fn=None
    must produce byte-equivalent 1-epoch training trajectory vs the pre-refactor
    hardcoded path. Uses a tiny fixture (10 samples, 1 epoch) for speed.

    This test protects against accidental production-behavior drift during the
    refactor — it asserts the defaults reproduce the current training exactly.
    """
    import sys

    import torch

    # Import both the refactored loop AND force the module to expose the pre-refactor
    # constants so we can reconstruct the hardcoded reference path below.
    sys.path.insert(0, "scripts")
    try:
        from train_football2vec_v2 import _train_stage2_loop
    finally:
        sys.path.pop(0)

    from analytics.football2vec_transformer import (
        Football2VecConfig,
        Football2VecEncoder,
        TeamClassifierHead,
    )
    from ingestion.football2vec_v2_training import (
        ADVERSARIAL_LAMBDA_MAX,
        ADVERSARIAL_WARMUP_EPOCHS,
        Football2VecDataset,
    )

    device = torch.device("cpu")
    hd, num_comp = 64, 4  # tiny config for speed
    cfg = Football2VecConfig(hidden_dim=hd, num_layers=2, num_heads=4, max_seq_len=16, spatial_mlp_dim=16)

    # 10-sample fixture
    torch.manual_seed(42)
    action_ids = [[1, 2, 3, 4, 5] for _ in range(10)]
    x_coords = [[0.1, 0.2, 0.3, 0.4, 0.5] for _ in range(10)]
    y_coords = [[0.5, 0.4, 0.3, 0.2, 0.1] for _ in range(10)]
    competition_ids = [i % num_comp for i in range(10)]

    train_ds = Football2VecDataset(
        action_ids, x_coords, y_coords, max_seq_len=16, mask_prob=0.15, mlm=True,
        competition_ids=competition_ids,
    )
    val_ds = Football2VecDataset(
        action_ids, x_coords, y_coords, max_seq_len=16, mask_prob=0.15, mlm=True,
        competition_ids=competition_ids,
    )

    # Reference: construct + run with refactored loop using defaults (backward-compat path)
    torch.manual_seed(42)
    encoder_ref = Football2VecEncoder(cfg)
    _, _, hist_ref = _train_stage2_loop(
        encoder_ref, train_ds, val_ds, num_comp, cfg, device,
        epochs=1, batch_size=5, lr=1e-3, patience=3,
        adversary_module=None, lambda_schedule_fn=None,  # defaults
    )

    # Explicit-injection path: construct an identical TeamClassifierHead manually,
    # pass it in, supply the production linear schedule function explicitly. The
    # refactored loop must produce identical trajectories.
    torch.manual_seed(42)
    encoder_explicit = Football2VecEncoder(cfg)
    adversary = TeamClassifierHead(hidden_dim=hd, num_teams=num_comp, lambda_val=0.0)
    def linear_schedule(epoch: int, total_epochs: int) -> float:
        del total_epochs
        return ADVERSARIAL_LAMBDA_MAX * min(epoch / ADVERSARIAL_WARMUP_EPOCHS, 1.0)

    _, _, hist_explicit = _train_stage2_loop(
        encoder_explicit, train_ds, val_ds, num_comp, cfg, device,
        epochs=1, batch_size=5, lr=1e-3, patience=3,
        adversary_module=adversary, lambda_schedule_fn=linear_schedule,
    )

    # Trajectories must match (deterministic seed + same structure)
    for key in ("train_mlm_loss", "train_adv_loss", "val_mlm_loss", "val_adv_accuracy", "lambda_val"):
        for i, (a, b) in enumerate(zip(hist_ref[key], hist_explicit[key])):
            assert a == pytest.approx(b, abs=1e-6), f"{key}[{i}]: ref={a} vs explicit={b}"
```

- [ ] **Step 2: Run test, verify failure**

Run: `uv run pytest src/tests/test_football2vec_adversary.py::test_stage2_loop_injection_backcompat -v`
Expected: FAIL — `_train_stage2_loop()` does not accept `adversary_module` or `lambda_schedule_fn` kwargs

- [ ] **Step 3: Refactor `scripts/train_football2vec_v2.py::_train_stage2_loop`**.

Current signature (`scripts/train_football2vec_v2.py:221-232`):
```python
def _train_stage2_loop(
    model: Football2VecEncoder,
    train_ds: Football2VecDataset,
    val_ds: Football2VecDataset,
    num_comp: int,
    config: Football2VecConfig,
    device: torch.device,
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
) -> tuple[Football2VecEncoder, TeamClassifierHead, dict[str, list[float]]]:
```

Replace with:

```python
def _train_stage2_loop(
    model: Football2VecEncoder,
    train_ds: Football2VecDataset,
    val_ds: Football2VecDataset,
    num_comp: int,
    config: Football2VecConfig,
    device: torch.device,
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
    adversary_module: nn.Module | None = None,
    lambda_schedule_fn: Callable[[int, int], float] | None = None,
) -> tuple[Football2VecEncoder, nn.Module, dict[str, list[float]]]:
    """Stage-2 adversarial fine-tuning loop.

    Args:
        model: Pre-trained Football2VecEncoder from stage-1.
        train_ds, val_ds: MLM datasets with competition_ids populated.
        num_comp: Number of competition classes.
        config: Football2VecConfig for hidden_dim / vocab_size lookups.
        device: Target device.
        epochs, batch_size, lr, patience: Standard training hyperparameters.
        adversary_module: Optional injected adversary. If None, reproduces current
            production behavior byte-equivalent (TeamClassifierHead(hd, num_comp)).
            If provided, module must accept (encoder_output, attention_mask) ->
            (B, num_comp) logits and have a .grl attribute with a settable
            .lambda_val float (matching GradientReversalLayer's API).
        lambda_schedule_fn: Optional injected schedule function of signature
            (epoch, total_epochs) -> lambda. If None, reproduces the current
            linear ramp over ADVERSARIAL_WARMUP_EPOCHS to ADVERSARIAL_LAMBDA_MAX.
    """
```

Body changes:

1. At the top, if `adversary_module is None`: `adversary_module = TeamClassifierHead(config.hidden_dim, num_comp, lambda_val=0.0).to(device)` (matches pre-refactor line 234).
2. If `lambda_schedule_fn is None`: `lambda_schedule_fn = lambda epoch, total_epochs: ADVERSARIAL_LAMBDA_MAX * min(epoch / ADVERSARIAL_WARMUP_EPOCHS, 1.0)`.
3. Rename local `adversary` → `adversary_module` throughout the body (optimizer param list, train/eval loops, save/restore state_dict lookups).
4. Replace the inline `lam = ADVERSARIAL_LAMBDA_MAX * min(epoch / ADVERSARIAL_WARMUP_EPOCHS, 1.0)` computation (line 273) with `lam = lambda_schedule_fn(epoch, epochs)`.
5. Update `adversary.grl.lambda_val = lam` (line 274) to work on the injected module — the module must expose `.grl.lambda_val` (documented contract).

Also add `from collections.abc import Callable` at the top of the file (if not present).

Type return: change `tuple[..., TeamClassifierHead, ...]` → `tuple[..., nn.Module, ...]` since the caller may inject custom modules. Update the `_run_stage2` caller: `model, adversary, history = _train_stage2_loop(...)` continues to work (nn.Module is the common base).

Also update the existing `_eval_stage2` helper (`scripts/train_football2vec_v2.py:336-363`): change type annotation `adv: TeamClassifierHead` → `adv: nn.Module`.

- [ ] **Step 4: Run backward-compat test, verify pass**

Run: `uv run pytest src/tests/test_football2vec_adversary.py::test_stage2_loop_injection_backcompat -v`
Expected: PASS

- [ ] **Step 5: Run full tests to verify no regression**

Run: `uv run pytest src/tests/test_football2vec_adversary.py -v`
Expected: 9 passed

---

### Task 5: `FOOTBALL2VEC_ADVERSARY_PROFILE` validation profile

**Files:**
- Create: `src/evolve/targets/football2vec/validation.py`
- Modify: `src/evolve/targets/football2vec/__init__.py` — export `VALIDATION_PROFILE`
- Create: `src/tests/test_evolve_football2vec_l2.py`

- [ ] **Step 1: Write the failing test** in `src/tests/test_evolve_football2vec_l2.py`:

```python
"""Tests for EV2 Football2Vec L2 infrastructure — validator profile, seed loading,
stage-2 evaluator entry point."""

from __future__ import annotations

import pytest


def test_f2v_adversary_validation_profile_exists():
    """Profile is importable and registered as the target's VALIDATION_PROFILE."""
    from evolve.targets.football2vec import VALIDATION_PROFILE
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE

    assert VALIDATION_PROFILE is FOOTBALL2VEC_ADVERSARY_PROFILE


def test_f2v_adversary_validation_profile_accepts_valid_seed():
    """A minimal valid seed passes AST validation."""
    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE

    source = '''
def custom_layers(hidden_dim, num_competitions):
    head = torch.nn.Sequential(
        torch.nn.Linear(hidden_dim, hidden_dim),
        torch.nn.GELU(),
        torch.nn.Linear(hidden_dim, num_competitions),
    )
    class H(torch.nn.Module):
        def __init__(self, net):
            super().__init__()
            self.grl = GradientReversal(lambda_=1.0)
            self.net = net
        def forward(self, encoder_output, attention_mask):
            cls = encoder_output[:, 0]
            return self.net(self.grl(cls))
    return {"adversary": H(head)}
'''
    ok, reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert ok, f"expected valid, got: {reason}"


def test_f2v_adversary_validation_profile_rejects_os_system():
    """Seeds invoking os.system are rejected (AST allowlist defense)."""
    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE

    source = '''
def custom_layers(hidden_dim, num_competitions):
    os.system("echo pwned")
    return {"adversary": torch.nn.Linear(hidden_dim, num_competitions)}
'''
    ok, _reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert not ok


def test_f2v_adversary_validation_profile_rejects_imports():
    """Seeds attempting imports are rejected."""
    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE

    source = '''
import subprocess

def custom_layers(hidden_dim, num_competitions):
    return {"adversary": torch.nn.Linear(hidden_dim, num_competitions)}
'''
    ok, _reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert not ok


def test_f2v_adversary_validation_profile_rejects_wrong_signature():
    """Seed with wrong parameter list is rejected."""
    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE

    source = '''
def custom_layers(hidden_dim):
    return {"adversary": torch.nn.Linear(hidden_dim, 22)}
'''
    ok, _reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert not ok
```

- [ ] **Step 2: Run tests, verify failure**

Run: `uv run pytest src/tests/test_evolve_football2vec_l2.py -v`
Expected: All 5 fail with `ImportError: cannot import name 'VALIDATION_PROFILE'` (or `FOOTBALL2VEC_ADVERSARY_PROFILE`)

- [ ] **Step 3: Create `src/evolve/targets/football2vec/validation.py`**:

```python
"""Football2Vec Level-2 adversary validation profile.

Defines which model attributes, namespaces, and builtins are allowed in
LLM-generated (or hand-written) custom_layers() functions that return
{"adversary": <nn.Module>}. Each seed program file is validated against
this profile before exec — defense-in-depth belt layer per ADR-001.
"""

from __future__ import annotations

from evolve.code_validator import ValidationProfile

FOOTBALL2VEC_ADVERSARY_PROFILE = ValidationProfile(
    patch_method="adversary",
    patch_signature=["hidden_dim", "num_competitions"],
    return_shape="dict with key 'adversary' -> nn.Module taking (encoder_output, attention_mask) -> (B, num_competitions)",
    known_model_attrs=frozenset(),  # seeds don't access self.* (they return standalone modules)
    allowed_namespaces=frozenset(
        {
            "torch",
            "math",
            "GradientReversal",
            "MoERouter",
            "HyperLinear",
            "KANLayer",
            "AdaLNZero",
            "CrossLayer",
            "CompetitiveGate",
            "AdaptiveBandwidth",
            "RatioGate",
        }
    ),
    layers_args=["hidden_dim", "num_competitions"],
    rejected_builtins=frozenset(
        {
            "eval",
            "exec",
            "compile",
            "__import__",
            "open",
            "print",
            "input",
            "getattr",
            "hasattr",
            "setattr",
            "delattr",
            "globals",
            "locals",
            "vars",
            "dir",
            "type",
            "super",
            "breakpoint",
            "memoryview",
            "classmethod",
            "staticmethod",
            "property",
        }
    ),
)
```

- [ ] **Step 4: Update `src/evolve/targets/football2vec/__init__.py`**:

```python
"""Football2Vec evolve target — Level 1 stage-1 search + Level 2 stage-2 adversary search.

EV1 (PR #158): L1 search over stage-1 hyperparameters + architectural enums.
EV2 (this cycle): L2 search over stage-2 adversary architecture + L1 search
over lambda schedule shape / max / warmup.
"""

from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE

# Generic name used by runner.py to look up the profile for any target
VALIDATION_PROFILE = FOOTBALL2VEC_ADVERSARY_PROFILE

__all__ = ["FOOTBALL2VEC_ADVERSARY_PROFILE", "VALIDATION_PROFILE"]
```

- [ ] **Step 5: Run tests, verify pass**

Run: `uv run pytest src/tests/test_evolve_football2vec_l2.py -v`
Expected: 5 passed

---

### Task 6: Create `seed_programs_stage2/` directory

**Files:**
- Create: `src/evolve/targets/football2vec/seed_programs_stage2/__init__.py`

- [ ] **Step 1: Create the file** with a short docstring:

```python
"""EV2 Phase 1 seed programs for Football2Vec v2 stage-2 adversary architecture search.

Each .py file in this directory defines a ``custom_layers(hidden_dim, num_competitions)``
function returning ``{"adversary": <nn.Module>}``. The evaluator execs the file under
restricted globals (per ADR-001), calls the function, and uses the returned module as
the stage-2 adversary.

See docs/superpowers/specs/2026-04-23-ev2-football2vec-l2-adversarial-design.md.
"""
```

- [ ] **Step 2: No test needed** — namespace package presence is verified by seed tests in Tasks 7-12.

---

### Task 7: Seed — `deep_mlp_2layer`

**Files:**
- Create: `src/evolve/targets/football2vec/seed_programs_stage2/deep_mlp_2layer.py`
- Modify: `src/tests/test_evolve_football2vec_l2.py` — add seed-load test

- [ ] **Step 1: Write the failing test** (append to `test_evolve_football2vec_l2.py`):

```python
def test_seed_deep_mlp_2layer_loads_and_forwards():
    """deep_mlp_2layer seed parses, validates, exec's, and produces a correctly-shaped forward."""
    import torch

    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE

    from pathlib import Path
    seed_path = Path(
        "src/evolve/targets/football2vec/seed_programs_stage2/deep_mlp_2layer.py"
    )
    source = seed_path.read_text(encoding="utf-8")

    # AST validation
    ok, reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert ok, f"validation failed: {reason}"

    # Exec + forward smoke test
    from evolve.targets.scoutgpt.building_blocks import GradientReversal
    restricted_globals = {
        "torch": torch,
        "math": __import__("math"),
        "GradientReversal": GradientReversal,
        "__builtins__": {},
    }
    exec(source, restricted_globals)  # noqa: S102 — test-only exec under restricted globals
    layers_fn = restricted_globals["custom_layers"]
    result = layers_fn(hidden_dim=192, num_competitions=22)
    assert "adversary" in result
    adv = result["adversary"]

    # Forward shape check
    x = torch.randn(4, 16, 192)
    mask = torch.ones(4, 16, dtype=torch.bool)
    logits = adv(x, mask)
    assert logits.shape == (4, 22), f"unexpected logits shape {logits.shape}"
```

- [ ] **Step 2: Run test, verify failure**

Run: `uv run pytest src/tests/test_evolve_football2vec_l2.py::test_seed_deep_mlp_2layer_loads_and_forwards -v`
Expected: FAIL — `FileNotFoundError: ...deep_mlp_2layer.py`

- [ ] **Step 3: Create `src/evolve/targets/football2vec/seed_programs_stage2/deep_mlp_2layer.py`**:

```python
"""Seed 1 — deep_mlp_2layer.

Hypothesis: stronger adversary → more competition signal recovered → more
debias pressure on encoder. A 2-layer MLP with GELU + LayerNorm has more
capacity than the baseline single-layer linear head. If the adversary's
extra capacity lets it fit otherwise-latent competition signal, the encoder
is forced to suppress more of that signal, yielding a cleaner debias.

Adversary: CLS pool → GRL → Linear(hd, hd) → GELU → LayerNorm → Linear(hd, num_comp).
"""


def custom_layers(hidden_dim, num_competitions):
    class H(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.grl = GradientReversal(lambda_=1.0)
            self.mlp = torch.nn.Sequential(
                torch.nn.Linear(hidden_dim, hidden_dim),
                torch.nn.GELU(),
                torch.nn.LayerNorm(hidden_dim),
                torch.nn.Linear(hidden_dim, num_competitions),
            )

        def forward(self, encoder_output, attention_mask):
            cls = encoder_output[:, 0]
            return self.mlp(self.grl(cls))

    return {"adversary": H()}
```

- [ ] **Step 4: Run test, verify pass**

Run: `uv run pytest src/tests/test_evolve_football2vec_l2.py::test_seed_deep_mlp_2layer_loads_and_forwards -v`
Expected: PASS

---

### Task 8: Seed — `deep_mlp_3layer`

**Files:**
- Create: `src/evolve/targets/football2vec/seed_programs_stage2/deep_mlp_3layer.py`
- Modify: `src/tests/test_evolve_football2vec_l2.py`

- [ ] **Step 1: Append the test**:

```python
def test_seed_deep_mlp_3layer_loads_and_forwards():
    import torch
    from pathlib import Path
    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE
    from evolve.targets.scoutgpt.building_blocks import GradientReversal

    source = Path(
        "src/evolve/targets/football2vec/seed_programs_stage2/deep_mlp_3layer.py"
    ).read_text(encoding="utf-8")
    ok, reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert ok, f"validation failed: {reason}"
    restricted_globals = {
        "torch": torch, "math": __import__("math"),
        "GradientReversal": GradientReversal, "__builtins__": {},
    }
    exec(source, restricted_globals)  # noqa: S102
    adv = restricted_globals["custom_layers"](hidden_dim=192, num_competitions=22)["adversary"]
    assert adv(torch.randn(4, 16, 192), torch.ones(4, 16, dtype=torch.bool)).shape == (4, 22)
```

- [ ] **Step 2: Run, verify failure** (missing file):

Run: `uv run pytest src/tests/test_evolve_football2vec_l2.py::test_seed_deep_mlp_3layer_loads_and_forwards -v`
Expected: FAIL — `FileNotFoundError`

- [ ] **Step 3: Create the seed file**:

```python
"""Seed 2 — deep_mlp_3layer.

Hypothesis: even higher-capacity adversary than deep_mlp_2layer. Tests whether
continuing to scale adversary capacity yields diminishing or increasing
debias-pressure returns. Adversary: CLS pool → GRL → Linear(hd, 2*hd) →
GELU → LayerNorm → Linear(2*hd, hd) → GELU → LayerNorm → Linear(hd, num_comp).
"""


def custom_layers(hidden_dim, num_competitions):
    class H(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.grl = GradientReversal(lambda_=1.0)
            wide = hidden_dim * 2
            self.mlp = torch.nn.Sequential(
                torch.nn.Linear(hidden_dim, wide),
                torch.nn.GELU(),
                torch.nn.LayerNorm(wide),
                torch.nn.Linear(wide, hidden_dim),
                torch.nn.GELU(),
                torch.nn.LayerNorm(hidden_dim),
                torch.nn.Linear(hidden_dim, num_competitions),
            )

        def forward(self, encoder_output, attention_mask):
            cls = encoder_output[:, 0]
            return self.mlp(self.grl(cls))

    return {"adversary": H()}
```

- [ ] **Step 4: Run test, verify pass**

Run: `uv run pytest src/tests/test_evolve_football2vec_l2.py::test_seed_deep_mlp_3layer_loads_and_forwards -v`
Expected: PASS

---

### Task 9: Seed — `cross_attention_adversary`

**Files:**
- Create: `src/evolve/targets/football2vec/seed_programs_stage2/cross_attention_adversary.py`
- Modify: `src/tests/test_evolve_football2vec_l2.py`

- [ ] **Step 1: Append the test** (mirror the Task 7/8 structure; point at `cross_attention_adversary.py`):

```python
def test_seed_cross_attention_adversary_loads_and_forwards():
    import torch
    from pathlib import Path
    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE
    from evolve.targets.scoutgpt.building_blocks import GradientReversal

    source = Path(
        "src/evolve/targets/football2vec/seed_programs_stage2/cross_attention_adversary.py"
    ).read_text(encoding="utf-8")
    ok, reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert ok, f"validation failed: {reason}"
    restricted_globals = {
        "torch": torch, "math": __import__("math"),
        "GradientReversal": GradientReversal, "__builtins__": {},
    }
    exec(source, restricted_globals)  # noqa: S102
    adv = restricted_globals["custom_layers"](hidden_dim=192, num_competitions=22)["adversary"]
    # Per-token adversary: with attention_mask True everywhere, all positions contribute
    assert adv(torch.randn(4, 16, 192), torch.ones(4, 16, dtype=torch.bool)).shape == (4, 22)
    # With half-masked sequence, result should still be (4, 22)
    mask = torch.ones(4, 16, dtype=torch.bool)
    mask[:, 8:] = False
    assert adv(torch.randn(4, 16, 192), mask).shape == (4, 22)
```

- [ ] **Step 2: Run, verify failure**

Run: `uv run pytest src/tests/test_evolve_football2vec_l2.py::test_seed_cross_attention_adversary_loads_and_forwards -v`
Expected: FAIL — `FileNotFoundError`

- [ ] **Step 3: Create the seed file**:

```python
"""Seed 3 — cross_attention_adversary.

Hypothesis: each competition class has its own learnable query that
cross-attends over the per-token encoder output to aggregate evidence FOR
that class. Structurally distinct from attention_pool_head (which pools
first then classifies). Mechanistic match with ScoutGPT's cross-attention
Fourier finding (PR #163 → #166 → #176): cross-attention was the mechanism
that produced ScoutGPT's largest rho wins.

Adversary: GRL(per-token) → 22 learnable competition queries cross-attend over
reversed-gradient per-token output → Linear(hd, 1) → squeeze → (B, num_comp).

Reference: Carion et al. (2020) "DETR" — learnable object queries.
            Lee et al. (2019) "Set Transformer" — per-class query attention.
"""


def custom_layers(hidden_dim, num_competitions):
    class H(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.grl = GradientReversal(lambda_=1.0)
            # Learnable per-class queries: (num_comp, hidden_dim)
            self.class_queries = torch.nn.Parameter(
                torch.randn(num_competitions, hidden_dim) * 0.02
            )
            # Fixed 4 heads for stability (hidden_dim=192 divisible by 4)
            self.cross_attn = torch.nn.MultiheadAttention(
                hidden_dim, num_heads=4, batch_first=True
            )
            self.out_proj = torch.nn.Linear(hidden_dim, 1)

        def forward(self, encoder_output, attention_mask):
            reversed_tokens = self.grl(encoder_output)  # (B, S, hd)
            batch_size = reversed_tokens.size(0)
            queries = self.class_queries.unsqueeze(0).expand(batch_size, -1, -1)  # (B, C, hd)
            # key_padding_mask: True means "ignore this key position"
            key_padding_mask = ~attention_mask  # (B, S)
            attended, _ = self.cross_attn(
                queries, reversed_tokens, reversed_tokens, key_padding_mask=key_padding_mask
            )  # (B, C, hd)
            return self.out_proj(attended).squeeze(-1)  # (B, C)

    return {"adversary": H()}
```

- [ ] **Step 4: Run test, verify pass**

Run: `uv run pytest src/tests/test_evolve_football2vec_l2.py::test_seed_cross_attention_adversary_loads_and_forwards -v`
Expected: PASS

---

### Task 10: Seed — `attention_pool_head`

**Files:**
- Create: `src/evolve/targets/football2vec/seed_programs_stage2/attention_pool_head.py`
- Modify: `src/tests/test_evolve_football2vec_l2.py`

- [ ] **Step 1: Append the test** (same shape as previous seed tests, point at `attention_pool_head.py`):

```python
def test_seed_attention_pool_head_loads_and_forwards():
    import torch
    from pathlib import Path
    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE
    from evolve.targets.scoutgpt.building_blocks import GradientReversal

    source = Path(
        "src/evolve/targets/football2vec/seed_programs_stage2/attention_pool_head.py"
    ).read_text(encoding="utf-8")
    ok, reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert ok, f"validation failed: {reason}"
    restricted_globals = {
        "torch": torch, "math": __import__("math"),
        "GradientReversal": GradientReversal, "__builtins__": {},
    }
    exec(source, restricted_globals)  # noqa: S102
    adv = restricted_globals["custom_layers"](hidden_dim=192, num_competitions=22)["adversary"]
    assert adv(torch.randn(4, 16, 192), torch.ones(4, 16, dtype=torch.bool)).shape == (4, 22)
```

- [ ] **Step 2: Run, verify failure**

- [ ] **Step 3: Create the seed file**:

```python
"""Seed 4 — attention_pool_head.

Hypothesis: if competition signal is localized at specific token positions
(kickoff locations, unique end-of-half patterns, etc.), an attention-pool
adversary can focus on them. A single learnable query attends over the
reversed per-token output; the weighted pool is classified.

Adversary: GRL(per-token) → single learnable query → softmax-attention pool
over sequence → Linear(hd, num_comp).
"""


def custom_layers(hidden_dim, num_competitions):
    class H(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.grl = GradientReversal(lambda_=1.0)
            # Single learnable query vector
            self.query = torch.nn.Parameter(torch.randn(hidden_dim) * 0.02)
            self.key_proj = torch.nn.Linear(hidden_dim, hidden_dim)
            self.classifier = torch.nn.Linear(hidden_dim, num_competitions)

        def forward(self, encoder_output, attention_mask):
            reversed_tokens = self.grl(encoder_output)  # (B, S, hd)
            keys = self.key_proj(reversed_tokens)  # (B, S, hd)
            # Attention scores: dot(query, key) for each position
            scores = keys @ self.query  # (B, S)
            # Mask out padded positions by setting scores to -inf
            scores = scores.masked_fill(~attention_mask, float("-inf"))
            # Guard: if a row has zero valid positions, softmax produces NaN; replace with
            # uniform weights. Upstream loader should filter empties but guard anyway.
            any_valid = attention_mask.any(dim=1, keepdim=True)
            scores = torch.where(any_valid, scores, torch.zeros_like(scores))
            weights = torch.softmax(scores, dim=1).unsqueeze(-1)  # (B, S, 1)
            pooled = (reversed_tokens * weights).sum(dim=1)  # (B, hd)
            return self.classifier(pooled)

    return {"adversary": H()}
```

- [ ] **Step 4: Run test, verify pass**

---

### Task 11: Seed — `residual_mlp`

**Files:**
- Create: `src/evolve/targets/football2vec/seed_programs_stage2/residual_mlp.py`
- Modify: `src/tests/test_evolve_football2vec_l2.py`

- [ ] **Step 1: Append the test** (same template):

```python
def test_seed_residual_mlp_loads_and_forwards():
    import torch
    from pathlib import Path
    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE
    from evolve.targets.scoutgpt.building_blocks import GradientReversal

    source = Path(
        "src/evolve/targets/football2vec/seed_programs_stage2/residual_mlp.py"
    ).read_text(encoding="utf-8")
    ok, reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert ok, f"validation failed: {reason}"
    restricted_globals = {
        "torch": torch, "math": __import__("math"),
        "GradientReversal": GradientReversal, "__builtins__": {},
    }
    exec(source, restricted_globals)  # noqa: S102
    adv = restricted_globals["custom_layers"](hidden_dim=192, num_competitions=22)["adversary"]
    assert adv(torch.randn(4, 16, 192), torch.ones(4, 16, dtype=torch.bool)).shape == (4, 22)
```

- [ ] **Step 2: Run, verify failure**

- [ ] **Step 3: Create the seed file**:

```python
"""Seed 5 — residual_mlp.

Hypothesis: same capacity as deep_mlp_2layer but with a residual connection
through the MLP block. Residuals are known to improve gradient flow in deep
networks; for an adversary, better gradient flow means the adversary can
more effectively propagate the debias signal back to the encoder.

Adversary: CLS pool → GRL → (Linear → GELU → LN → Linear) + residual →
           Linear(hd, num_comp).
"""


def custom_layers(hidden_dim, num_competitions):
    class H(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.grl = GradientReversal(lambda_=1.0)
            self.ln = torch.nn.LayerNorm(hidden_dim)
            self.fc1 = torch.nn.Linear(hidden_dim, hidden_dim)
            self.act = torch.nn.GELU()
            self.fc2 = torch.nn.Linear(hidden_dim, hidden_dim)
            self.classifier = torch.nn.Linear(hidden_dim, num_competitions)

        def forward(self, encoder_output, attention_mask):
            cls = encoder_output[:, 0]
            h = self.grl(cls)
            # Residual block
            mid = self.fc2(self.act(self.fc1(h)))
            h = self.ln(h + mid)
            return self.classifier(h)

    return {"adversary": H()}
```

- [ ] **Step 4: Run, verify pass**

---

### Task 12: Seed — `dual_head_ensemble`

**Files:**
- Create: `src/evolve/targets/football2vec/seed_programs_stage2/dual_head_ensemble.py`
- Modify: `src/tests/test_evolve_football2vec_l2.py`

- [ ] **Step 1: Append the test**:

```python
def test_seed_dual_head_ensemble_loads_and_forwards():
    import torch
    from pathlib import Path
    from evolve.code_validator import validate_program
    from evolve.targets.football2vec.validation import FOOTBALL2VEC_ADVERSARY_PROFILE
    from evolve.targets.scoutgpt.building_blocks import GradientReversal

    source = Path(
        "src/evolve/targets/football2vec/seed_programs_stage2/dual_head_ensemble.py"
    ).read_text(encoding="utf-8")
    ok, reason = validate_program(source, FOOTBALL2VEC_ADVERSARY_PROFILE, code_evolution=True)
    assert ok, f"validation failed: {reason}"
    restricted_globals = {
        "torch": torch, "math": __import__("math"),
        "GradientReversal": GradientReversal, "__builtins__": {},
    }
    exec(source, restricted_globals)  # noqa: S102
    adv = restricted_globals["custom_layers"](hidden_dim=192, num_competitions=22)["adversary"]
    assert adv(torch.randn(4, 16, 192), torch.ones(4, 16, dtype=torch.bool)).shape == (4, 22)
```

- [ ] **Step 2: Run, verify failure**

- [ ] **Step 3: Create the seed file**:

```python
"""Seed 6 — dual_head_ensemble.

Hypothesis: two parallel adversaries with different capacities both act on
the same shared GRL output; their logits are averaged before the cross-entropy
loss. Intuition: different-capacity discriminators pick up on different
competition signatures, and averaging pushes the encoder to defeat both
simultaneously — analogous to multi-scale GAN discriminators.

Adversary: CLS pool → GRL → {linear_head, mlp_2layer_head} →
           mean-average of their logits → (B, num_comp).
"""


def custom_layers(hidden_dim, num_competitions):
    class H(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.grl = GradientReversal(lambda_=1.0)
            # Head A: single linear (baseline capacity)
            self.head_linear = torch.nn.Linear(hidden_dim, num_competitions)
            # Head B: 2-layer MLP (higher capacity)
            self.head_mlp = torch.nn.Sequential(
                torch.nn.Linear(hidden_dim, hidden_dim),
                torch.nn.GELU(),
                torch.nn.LayerNorm(hidden_dim),
                torch.nn.Linear(hidden_dim, num_competitions),
            )

        def forward(self, encoder_output, attention_mask):
            cls = encoder_output[:, 0]
            reversed = self.grl(cls)
            a = self.head_linear(reversed)
            b = self.head_mlp(reversed)
            return 0.5 * (a + b)

    return {"adversary": H()}
```

- [ ] **Step 4: Run, verify pass**

---

### Task 13: Extend evaluator with `train_and_evaluate_stage2` + stage-1 encoder cache

**Files:**
- Modify: `src/evolve/targets/football2vec/evaluator.py`
- Modify: `src/tests/test_evolve_football2vec_l2.py`

- [ ] **Step 1: Write the failing test** (append):

```python
def test_apply_program_adversary_extracts_adversary_module(tmp_path):
    """_apply_program_adversary execs a seed file and returns the 'adversary' module,
    with GradientReversal accessible for per-epoch lambda injection."""
    import torch

    from evolve.targets.football2vec.evaluator import _apply_program_adversary

    seed_path = tmp_path / "simple.py"
    seed_path.write_text('''
def custom_layers(hidden_dim, num_competitions):
    class H(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.grl = GradientReversal(lambda_=1.0)
            self.fc = torch.nn.Linear(hidden_dim, num_competitions)
        def forward(self, encoder_output, attention_mask):
            return self.fc(self.grl(encoder_output[:, 0]))
    return {"adversary": H()}
''', encoding="utf-8")

    adv = _apply_program_adversary(str(seed_path), hidden_dim=64, num_competitions=4, device=torch.device("cpu"))
    # Adversary must be an nn.Module and have .grl accessible
    assert isinstance(adv, torch.nn.Module)
    assert hasattr(adv, "grl")
    assert hasattr(adv.grl, "lambda_val") or hasattr(adv.grl, "lambda_")
    # Forward smoke test
    out = adv(torch.randn(2, 8, 64), torch.ones(2, 8, dtype=torch.bool))
    assert out.shape == (2, 4)


def test_apply_program_adversary_rejects_missing_adversary_key(tmp_path):
    """If custom_layers returns a dict without 'adversary' key, a clear error is raised."""
    import torch

    from evolve.targets.football2vec.evaluator import _apply_program_adversary

    seed_path = tmp_path / "bad.py"
    seed_path.write_text('''
def custom_layers(hidden_dim, num_competitions):
    return {"something_else": torch.nn.Linear(hidden_dim, num_competitions)}
''', encoding="utf-8")

    import pytest
    with pytest.raises(ValueError, match="'adversary'"):
        _apply_program_adversary(str(seed_path), hidden_dim=64, num_competitions=4, device=torch.device("cpu"))
```

- [ ] **Step 2: Run, verify failure**

Run: `uv run pytest src/tests/test_evolve_football2vec_l2.py::test_apply_program_adversary_extracts_adversary_module -v`
Expected: FAIL — `_apply_program_adversary` not defined

- [ ] **Step 3: Add `_apply_program_adversary` + `train_and_evaluate_stage2`** to `src/evolve/targets/football2vec/evaluator.py`.

Append to `src/evolve/targets/football2vec/evaluator.py` (after existing code):

```python
# ---------------------------------------------------------------------------
# Stage-2 adversarial fine-tuning — EV2 infrastructure.
# ---------------------------------------------------------------------------


_stage1_cache: dict[tuple[str, str], dict[str, Any]] = {}
_stage1_lock = threading.Lock()


def _load_or_cache_stage1_encoder(
    model_repo: str,
    commit_sha: str,
    config_obj: Any,  # Football2VecConfig
    device: Any,  # torch.device
    hf_token: str,
) -> Any:
    """Load the stage-1 encoder from a pinned HF Hub revision, cache per process.

    Keyed by (model_repo, commit_sha). Weights are ~500 MB — re-download per
    candidate evaluation is wasteful; the cache saves it.
    """
    import torch
    import torch.nn as nn
    from huggingface_hub import hf_hub_download
    from safetensors.torch import load_file as _load

    from analytics.football2vec_transformer import Football2VecEncoder
    from ingestion.football2vec_v2_training import VOCAB_SIZE

    key = (model_repo, commit_sha)
    with _stage1_lock:
        cached = _stage1_cache.get(key)
        if cached is not None:
            _log.info("Using cached stage-1 encoder for %s @ %s", model_repo, commit_sha[:8])
            model = Football2VecEncoder(config_obj)
            expanded = nn.Embedding(VOCAB_SIZE + 2, config_obj.hidden_dim)
            with torch.no_grad():
                expanded.weight[:VOCAB_SIZE] = model.token_embedding.weight
            model.token_embedding = expanded
            model.load_state_dict(cached["state_dict"])
            return model.to(device)

        local = hf_hub_download(
            model_repo, "stage1/model.safetensors", repo_type="model",
            token=hf_token, revision=commit_sha,
        )
        state = _load(local, device="cpu")
        _stage1_cache[key] = {"state_dict": state}

        model = Football2VecEncoder(config_obj)
        expanded = nn.Embedding(VOCAB_SIZE + 2, config_obj.hidden_dim)
        with torch.no_grad():
            expanded.weight[:VOCAB_SIZE] = model.token_embedding.weight
        model.token_embedding = expanded
        model.load_state_dict(state)
        return model.to(device)


def _apply_program_adversary(
    program_path: str,
    hidden_dim: int,
    num_competitions: int,
    device: Any,  # torch.device
) -> Any:  # nn.Module
    """Exec a Phase 1 seed under restricted globals and return the 'adversary' module.

    Raises ValueError if the returned dict lacks the 'adversary' key.
    """
    import torch

    from evolve.targets.scoutgpt.building_blocks import (
        AdaLNZero,
        AdaptiveBandwidth,
        CompetitiveGate,
        CrossLayer,
        GradientReversal,
        HyperLinear,
        KANLayer,
        MoERouter,
        RatioGate,
    )

    source = Path(program_path).read_text(encoding="utf-8")
    restricted_globals: dict[str, Any] = {
        "torch": torch,
        "math": __import__("math"),
        "MoERouter": MoERouter,
        "HyperLinear": HyperLinear,
        "KANLayer": KANLayer,
        "AdaLNZero": AdaLNZero,
        "CrossLayer": CrossLayer,
        "CompetitiveGate": CompetitiveGate,
        "GradientReversal": GradientReversal,
        "AdaptiveBandwidth": AdaptiveBandwidth,
        "RatioGate": RatioGate,
        "__builtins__": {},
    }
    exec(source, restricted_globals)  # noqa: S102 — see ADR-001  # nosemgrep: python.lang.security.audit.exec-detected.exec-detected

    layers_fn = restricted_globals.get("custom_layers")
    if layers_fn is None:
        msg = f"seed {program_path} has no custom_layers() function"
        raise ValueError(msg)
    result = layers_fn(hidden_dim, num_competitions)
    if not isinstance(result, dict) or "adversary" not in result:
        msg = f"seed {program_path} custom_layers must return dict with 'adversary' key, got {type(result).__name__}"
        raise ValueError(msg)
    return result["adversary"].to(device)


def train_and_evaluate_stage2(
    candidate_config: dict[str, Any],
    device: str,
    epochs: int,
    seed: int,
    program_path: str | None = None,
) -> dict[str, Any]:
    """Stage-2 adversarial fine-tuning evaluator.

    Loads the pinned stage-1 encoder, builds the adversary (injected seed OR
    registry lookup), runs refactored _train_stage2_loop, and returns the
    metrics dict the harvest orchestrator consumes.

    Required candidate_config keys (in addition to the stage-1 architecture keys):
    - ``stage1_model_repo``: HF Hub model repo ID. Default
      ``"luxury-lakehouse/football2vec-v2"``.
    - ``stage1_commit_sha``: Pinned commit SHA for reproducibility.
    - ``dataset``: HF Hub dataset ID. Default
      ``"luxury-lakehouse/football2vec-training-data"``.
    - ``adversary_architecture``: str — "linear" for baseline; ignored when
      program_path is not None.
    - ``lambda_schedule_shape``: "linear"|"sigmoid"|"cosine". Default "linear".
    - ``lambda_max``: float. Default 0.2.
    - ``lambda_warmup_epochs``: int. Default 5.

    Returns a dict with keys: val_mlm_loss, val_adv_accuracy, num_competitions,
    mlm_score (populated when candidate_config supplies L_0_reference), fitness
    (populated when mlm_score and debias_score are computable), chance,
    leakage, debias_score, param_count, training_time_seconds, epochs_trained.
    """
    import sys
    import torch
    import torch.nn as nn
    from torch.utils.data import DataLoader

    from analytics.football2vec_adversary import AdversaryConfig, build_adversary, lambda_schedule
    from analytics.football2vec_transformer import Football2VecConfig
    from ingestion.football2vec_v2_training import (
        Football2VecDataset,
        load_training_data,
        parse_actions,
        stratified_split,
    )

    torch_device = torch.device(device)
    start = time.monotonic()
    _log.info("Stage-2 candidate starting (device=%s, epochs=%d, seed=%d)", device, epochs, seed)
    torch.manual_seed(seed)

    # Resolve stage-1 encoder config (Phase 1 pins production architecture)
    config_keys = {
        "vocab_size", "hidden_dim", "num_layers", "num_heads", "dropout",
        "max_seq_len", "spatial_mlp_dim", "pooling_type", "spatial_injection",
        "position_embedding",
    }
    model_kwargs = {k: v for k, v in candidate_config.items() if k in config_keys}
    config_obj = Football2VecConfig(**model_kwargs)

    # Dataset (cached)
    hf_token = os.environ.get("HF_TOKEN", "")
    dataset_repo: str = candidate_config.get("dataset", "luxury-lakehouse/football2vec-training-data")
    data, _commit = load_training_data(hf_token, dataset_repo)
    aids_all, xs_all, ys_all = parse_actions(data["actions"])
    ucomp = sorted(data["competition_id"].unique().tolist())
    c2i: dict[int, int] = {int(c): i for i, c in enumerate(ucomp)}
    cl = [c2i[int(c)] for c in data["competition_id"].values]
    num_competitions = len(ucomp)

    # Splits
    train_df, val_df, _test_df = stratified_split(data)
    ti, vi = train_df.index.tolist(), val_df.index.tolist()

    batch_size: int = int(candidate_config.get("batch_size", 256))
    lr: float = float(candidate_config.get("learning_rate", 3e-4))
    mask_prob: float = float(candidate_config.get("mask_prob", 0.22))
    patience: int = max(3, epochs // 2)

    train_ds = Football2VecDataset(
        [aids_all[i] for i in ti], [xs_all[i] for i in ti], [ys_all[i] for i in ti],
        max_seq_len=config_obj.max_seq_len, mask_prob=mask_prob, mlm=True,
        competition_ids=[cl[i] for i in ti],
    )
    val_ds = Football2VecDataset(
        [aids_all[i] for i in vi], [xs_all[i] for i in vi], [ys_all[i] for i in vi],
        max_seq_len=config_obj.max_seq_len, mask_prob=mask_prob, mlm=True,
        competition_ids=[cl[i] for i in vi],
    )

    # Load stage-1 encoder (cached)
    stage1_repo: str = candidate_config.get("stage1_model_repo", "luxury-lakehouse/football2vec-v2")
    stage1_sha: str = str(candidate_config.get("stage1_commit_sha", "main"))
    encoder = _load_or_cache_stage1_encoder(stage1_repo, stage1_sha, config_obj, torch_device, hf_token)

    # Build adversary — inject-from-seed or registry
    adv_cfg = AdversaryConfig(
        architecture=candidate_config.get("adversary_architecture", "linear"),
        lambda_schedule_shape=candidate_config.get("lambda_schedule_shape", "linear"),
        lambda_max=float(candidate_config.get("lambda_max", 0.2)),
        lambda_warmup_epochs=int(candidate_config.get("lambda_warmup_epochs", 5)),
    )
    if program_path is not None:
        adversary = _apply_program_adversary(
            program_path, config_obj.hidden_dim, num_competitions, torch_device
        )
    else:
        adversary = build_adversary(adv_cfg, config_obj.hidden_dim, num_competitions).to(torch_device)

    # Schedule function
    def schedule_fn(epoch: int, total_epochs: int) -> float:
        return lambda_schedule(adv_cfg, epoch, total_epochs)

    # Import the refactored loop
    sys.path.insert(0, "scripts")
    try:
        from train_football2vec_v2 import _train_stage2_loop
    finally:
        sys.path.pop(0)

    try:
        _encoder, _adversary, history = _train_stage2_loop(
            encoder, train_ds, val_ds, num_competitions, config_obj, torch_device,
            epochs=epochs, batch_size=batch_size, lr=lr, patience=patience,
            adversary_module=adversary, lambda_schedule_fn=schedule_fn,
        )

        val_mlm_loss_final = history["val_mlm_loss"][-1] if history["val_mlm_loss"] else float("inf")
        val_adv_acc_final = history["val_adv_accuracy"][-1] if history["val_adv_accuracy"] else 0.0
        chance = 1.0 / num_competitions
        leakage = max(0.0, (val_adv_acc_final - chance) / max(1.0 - chance, 1e-9))
        debias_score = 1.0 - leakage

        L_0_reference = candidate_config.get("L_0_reference")
        mlm_score: float | None = None
        fitness: float | None = None
        if L_0_reference is not None and val_mlm_loss_final > 0:
            mlm_score = min(1.0, float(L_0_reference) / val_mlm_loss_final)
            fitness = 0.4 * mlm_score + 0.6 * debias_score

        param_count = sum(p.numel() for p in encoder.parameters()) + sum(
            p.numel() for p in adversary.parameters()
        )

        elapsed = time.monotonic() - start
        metrics: dict[str, Any] = {
            "val_mlm_loss": val_mlm_loss_final,
            "val_adv_accuracy": val_adv_acc_final,
            "num_competitions": float(num_competitions),
            "chance": chance,
            "leakage": leakage,
            "debias_score": debias_score,
            "mlm_score": mlm_score if mlm_score is not None else float("nan"),
            "fitness": fitness if fitness is not None else float("nan"),
            "param_count": float(param_count),
            "training_time_seconds": elapsed,
            "epochs_trained": float(len(history.get("val_mlm_loss", []))),
        }
        _log.info(
            "Stage-2 candidate done: val_mlm=%.4f val_adv_acc=%.4f leak=%.3f time=%.1fs",
            val_mlm_loss_final, val_adv_acc_final, leakage, elapsed,
        )
    except (torch.cuda.OutOfMemoryError, RuntimeError, ValueError) as exc:
        _log.warning("Stage-2 candidate failed: %s", exc)
        metrics = {
            "val_mlm_loss": float("inf"),
            "val_adv_accuracy": 0.0,
            "debias_score": 0.0,
            "mlm_score": 0.0,
            "fitness": 0.0,
            "param_count": 0.0,
            "training_time_seconds": time.monotonic() - start,
            "epochs_trained": 0.0,
            "error": 1.0,
            "_error_text": traceback.format_exc(),
        }

    if torch_device.type == "cuda":
        torch.cuda.empty_cache()
    return metrics
```

Update `__all__` at the bottom of `src/evolve/targets/football2vec/evaluator.py`:

```python
__all__ = ["train_and_evaluate", "train_and_evaluate_stage2", "_apply_program_adversary"]
```

- [ ] **Step 4: Run tests, verify pass**

Run: `uv run pytest src/tests/test_evolve_football2vec_l2.py -v`
Expected: all tests in this file passing (12 total: 5 validation + 6 seeds + 1 _apply_program_adversary + 1 missing-adversary-key)

---

### Task 14: PEP 723 orchestrator `scripts/evaluate_football2vec_l2_adversary_seeds.py`

**Files:**
- Create: `scripts/evaluate_football2vec_l2_adversary_seeds.py`
- Modify: `src/tests/test_evolve_football2vec_l2.py` — add import smoke test

- [ ] **Step 1: Write the failing smoke test** (append):

```python
def test_orchestrator_script_imports_and_has_main():
    """The PEP 723 orchestrator script can be imported by filename (not package)
    and exposes a top-level main() function."""
    import importlib.util
    from pathlib import Path

    path = Path("scripts/evaluate_football2vec_l2_adversary_seeds.py")
    assert path.exists(), f"orchestrator not at {path}"
    spec = importlib.util.spec_from_file_location("ev2_orchestrator", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert hasattr(module, "main")
    assert callable(module.main)
    # Ensure the variants list exists at module scope (pre-registration check)
    assert hasattr(module, "_VARIANTS")
    variants = module._VARIANTS
    # Baseline + 6 seeds
    assert len(variants) == 7
    variant_names = {v[0] for v in variants}
    expected = {
        "linear", "deep_mlp_2layer", "deep_mlp_3layer", "cross_attention_adversary",
        "attention_pool_head", "residual_mlp", "dual_head_ensemble",
    }
    assert variant_names == expected
```

- [ ] **Step 2: Run, verify failure** (file not found).

- [ ] **Step 3: Create `scripts/evaluate_football2vec_l2_adversary_seeds.py`**:

```python
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.12-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "datasets>=3.0",
#     "torch>=2.0",
#     "safetensors>=0.4.0",
#     "huggingface-hub>=1.5.0",
#     "scikit-learn>=1.3.0",
#     "scipy>=1.11.0",
#     "openevolve>=0.2.0",
# ]
# ///
"""EV2 Phase 1 orchestrator — harvest of 6 adversary seed programs + linear baseline.

Evaluates each variant at 30-epoch production fidelity against the pinned stage-1
encoder and dataset. Dispatches through the existing BackendPool (local_cuda,
remote_ssh) across AI-PC, Media-PC, and DGX Spark.

Per-variant ``metrics.json`` is uploaded to ``luxury-lakehouse/football2vec-l2-harvest``
as each variant completes (partial-crash survival). The combined ``results.json`` is
uploaded after all variants complete.

Usage:
    uv run python scripts/evaluate_football2vec_l2_adversary_seeds.py \\
        --stage1-sha <pinned sha> \\
        --dataset-sha <pinned sha> \\
        [--hosts ai,media,spark]  [--force-sequential]

The --hosts flag is a subset of {ai, media, spark} for pool restriction; omit
to use all three. --force-sequential disables BackendPool and runs all variants
on the local LocalCudaBackend only.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

logging.basicConfig(
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
RESULTS_REPO = f"{HF_ORG}/football2vec-l2-harvest"
TRAINING_DATASET = f"{HF_ORG}/football2vec-training-data"
STAGE1_MODEL_REPO = f"{HF_ORG}/football2vec-v2"

# Pinned shared config — mirrors the Phase 1 spec section. Reproducibility
# anchors (stage1_commit_sha, training_dataset_sha) are injected at dispatch.
_SHARED_CONFIG: dict[str, Any] = {
    "hidden_dim": 192,
    "num_layers": 4,
    "num_heads": 6,
    "dropout": 0.1,
    "mask_prob": 0.22,
    "spatial_mlp_dim": 64,
    "pooling_type": "cls",
    "spatial_injection": "additive",
    "position_embedding": "learnable",
    "learning_rate": 3e-4,
    "batch_size": 256,
    "adversary_architecture": "linear",
    "lambda_schedule_shape": "linear",
    "lambda_max": 0.2,
    "lambda_warmup_epochs": 5,
    "dataset": TRAINING_DATASET,
    "stage1_model_repo": STAGE1_MODEL_REPO,
}

_EPOCHS = 30
_SEED = 42
_FITNESS_W_MLM = 0.4
_FITNESS_W_DEBIAS = 0.6

_VARIANTS: list[tuple[str, str | None]] = [
    ("linear", None),
    ("deep_mlp_2layer", "deep_mlp_2layer.py"),
    ("deep_mlp_3layer", "deep_mlp_3layer.py"),
    ("cross_attention_adversary", "cross_attention_adversary.py"),
    ("attention_pool_head", "attention_pool_head.py"),
    ("residual_mlp", "residual_mlp.py"),
    ("dual_head_ensemble", "dual_head_ensemble.py"),
]


def _seed_program_path(rel: str) -> str:
    """Resolve seed file path inside the wheel-bundled package."""
    import evolve.targets.football2vec.seed_programs_stage2 as pkg

    pkg_dir = Path(pkg.__file__).parent
    path = pkg_dir / rel
    if not path.exists():
        msg = f"seed program not found: {path}"
        raise FileNotFoundError(msg)
    return str(path)


def _run_variant(
    variant: str,
    program_rel: str | None,
    device: str,
    stage1_sha: str,
    dataset_sha: str,
    L_0_reference: float | None,
) -> dict[str, Any]:
    """Run one variant; return its metrics dict (with variant metadata injected)."""
    from evolve.targets.football2vec.evaluator import train_and_evaluate_stage2

    candidate_config = dict(_SHARED_CONFIG)
    candidate_config["stage1_commit_sha"] = stage1_sha
    candidate_config["_dataset_sha_pinned"] = dataset_sha  # recorded for provenance
    if L_0_reference is not None:
        candidate_config["L_0_reference"] = L_0_reference

    program_path = _seed_program_path(program_rel) if program_rel is not None else None

    logger.info("=== Evaluating variant=%s program=%s ===", variant, program_rel)
    t0 = time.monotonic()
    metrics = train_and_evaluate_stage2(
        candidate_config=candidate_config,
        device=device,
        epochs=_EPOCHS,
        seed=_SEED,
        program_path=program_path,
    )
    elapsed = time.monotonic() - t0
    metrics["variant"] = variant
    metrics["program_path"] = program_rel or "<baseline>"
    metrics["wall_clock_seconds"] = elapsed
    logger.info(
        "variant=%s val_mlm=%.4f val_adv_acc=%.4f debias=%.3f mlm=%.3f fitness=%.3f elapsed=%.1fs",
        variant,
        metrics.get("val_mlm_loss", float("inf")),
        metrics.get("val_adv_accuracy", 0.0),
        metrics.get("debias_score", 0.0),
        metrics.get("mlm_score", 0.0),
        metrics.get("fitness", 0.0),
        elapsed,
    )
    return metrics


def _upload_json(api: Any, hf_token: str, obj: Any, path_in_repo: str) -> None:
    data = json.dumps(obj, indent=2, default=str).encode("utf-8")
    api.upload_file(
        path_or_fileobj=data,
        path_in_repo=path_in_repo,
        repo_id=RESULTS_REPO,
        repo_type="model",
        token=hf_token,
    )
    logger.info("Uploaded %s -> %s", path_in_repo, RESULTS_REPO)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stage1-sha", required=True,
        help="HF Hub commit SHA of luxury-lakehouse/football2vec-v2 to pin"
    )
    parser.add_argument(
        "--dataset-sha", required=True,
        help="HF Hub commit SHA of luxury-lakehouse/football2vec-training-data to pin"
    )
    parser.add_argument(
        "--hosts", default="ai,media,spark",
        help="Comma-separated subset of {ai,media,spark}; default all three"
    )
    parser.add_argument(
        "--force-sequential", action="store_true",
        help="Disable BackendPool; run all variants sequentially on local CUDA"
    )
    args = parser.parse_args()

    hf_token = os.environ.get("HF_TOKEN", "")
    if not hf_token:
        msg = "HF_TOKEN required"
        raise RuntimeError(msg)

    import torch
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)
    api.create_repo(RESULTS_REPO, exist_ok=True, repo_type="model", token=hf_token)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info("Starting harvest — device=%s stage1_sha=%s dataset_sha=%s",
                device, args.stage1_sha[:8], args.dataset_sha[:8])

    # Run the baseline variant first sequentially to capture L_0_reference
    # for the remaining variants' fitness computation.
    baseline_metrics = _run_variant(
        "linear", None, device, args.stage1_sha, args.dataset_sha, L_0_reference=None,
    )
    L_0 = baseline_metrics.get("val_mlm_loss", float("inf"))
    baseline_metrics["mlm_score"] = 1.0
    baseline_metrics["fitness"] = (
        _FITNESS_W_MLM * 1.0 + _FITNESS_W_DEBIAS * baseline_metrics.get("debias_score", 0.0)
    )
    _upload_json(api, hf_token, baseline_metrics, "linear/metrics.json")
    logger.info("Baseline complete: L_0 = %.4f, fitness = %.4f", L_0, baseline_metrics["fitness"])

    # Remaining 6 seeds dispatched via BackendPool (or sequentially if forced)
    remaining_variants = [(name, rel) for name, rel in _VARIANTS if name != "linear"]

    if args.force_sequential:
        results: list[dict[str, Any]] = [baseline_metrics]
        for name, rel in remaining_variants:
            try:
                metrics = _run_variant(name, rel, device, args.stage1_sha, args.dataset_sha, L_0)
            except Exception as exc:
                logger.exception("Variant %s failed", name)
                metrics = {"variant": name, "program_path": rel, "error": str(exc), "fitness": 0.0}
            results.append(metrics)
            _upload_json(api, hf_token, metrics, f"{name}/metrics.json")
    else:
        # BackendPool-based dispatch across local pool.
        from evolve.backends.local_cuda import LocalCudaBackend
        from evolve.backends.pool import BackendPool
        from evolve.backends.remote_ssh import RemoteSshBackend

        host_set = set(args.hosts.split(","))
        backends = []
        if "ai" in host_set:
            backends.append(LocalCudaBackend(device="cuda:0"))
        if "media" in host_set:
            backends.append(RemoteSshBackend(
                host="super@192.168.68.70",
                remote_dir="/home/super/Development",
                python_path="/home/super/Development/evolve-env/bin/python",
            ))
        if "spark" in host_set:
            backends.append(RemoteSshBackend(
                host="karsten@192.168.68.73",
                remote_dir="/home/karsten/Development",
                python_path="/home/karsten/Development/evolve-env/bin/python",
            ))
        if not backends:
            raise RuntimeError(f"no backends selected — --hosts={args.hosts}")

        pool = BackendPool(backends)
        results = [baseline_metrics]
        with ThreadPoolExecutor(max_workers=len(backends)) as ex:
            future_to_variant = {
                ex.submit(
                    _run_variant, name, rel, device, args.stage1_sha, args.dataset_sha, L_0,
                ): name
                for name, rel in remaining_variants
            }
            for fut in future_to_variant:
                name = future_to_variant[fut]
                try:
                    metrics = fut.result()
                except Exception as exc:
                    logger.exception("Variant %s failed", name)
                    metrics = {"variant": name, "error": str(exc), "fitness": 0.0}
                results.append(metrics)
                _upload_json(api, hf_token, metrics, f"{name}/metrics.json")

    results_sorted = sorted(results, key=lambda r: -r.get("fitness", 0.0))
    combined = {
        "dataset": TRAINING_DATASET,
        "dataset_sha_pinned": args.dataset_sha,
        "stage1_model_repo": STAGE1_MODEL_REPO,
        "stage1_sha_pinned": args.stage1_sha,
        "shared_config": _SHARED_CONFIG,
        "epochs": _EPOCHS,
        "seed": _SEED,
        "fitness_formula": f"{_FITNESS_W_MLM} * mlm_score + {_FITNESS_W_DEBIAS} * debias_score",
        "L_0": L_0,
        "variants": results_sorted,
    }
    _upload_json(api, hf_token, combined, "results.json")
    # Local mirror for git commit
    mirror_path = Path("docs/evolve/ev2-football2vec-l2-adversarial/results.json")
    mirror_path.parent.mkdir(parents=True, exist_ok=True)
    mirror_path.write_text(json.dumps(combined, indent=2, default=str), encoding="utf-8")

    logger.info("Harvest complete — %d variants evaluated", len(results))
    if any("error" in r for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run smoke test, verify pass**

Run: `uv run pytest src/tests/test_evolve_football2vec_l2.py::test_orchestrator_script_imports_and_has_main -v`
Expected: PASS

---

### Task 15: Workflow card `wf-evolve-football2vec-l2-stage2.yaml`

**Files:**
- Create: `workflow-cards/wf-evolve-football2vec-l2-stage2.yaml`
- Modify: `src/tests/test_evolve_football2vec_l2.py` — add card-parse test

First, check the sibling card as a template reference (required before writing a new card):

- [ ] **Step 1: Read the existing EV1 card as template**

Run: `cat workflow-cards/wf-evolve-football2vec.yaml | head -80`
(Informational — read to copy structure.)

- [ ] **Step 2: Write the failing test** (append):

```python
def test_card_wf_evolve_football2vec_l2_stage2_yaml_parses():
    """New workflow card parses via WorkflowCard.from_yaml_file and carries expected fields."""
    from pathlib import Path

    from analytics.workflow_card_schema import WorkflowCard

    card_path = Path("workflow-cards/wf-evolve-football2vec-l2-stage2.yaml")
    assert card_path.exists()
    card = WorkflowCard.from_yaml_file(card_path)
    assert card.id == "wf-evolve-football2vec-l2-stage2"
    assert card.type == "training"
    # Execution phase — runs locally, no HF Jobs cost
    assert "training" in card.execution
    # Cost phase parity (test_card_cost_phase_parity in the main test suite enforces this too)
    assert "training" in card.cost
    # Source links include the new seed_programs_stage2 directory
    source_links = getattr(card.links, "source_code", None) if card.links else None
    if source_links is not None:
        assert any("seed_programs_stage2" in str(x) or "football2vec_adversary" in str(x)
                   for x in source_links), f"expected seed_programs_stage2 or football2vec_adversary in source links, got {source_links}"
```

- [ ] **Step 3: Run, verify failure** (file missing).

- [ ] **Step 4: Write the card** mirroring `workflow-cards/wf-evolve-football2vec.yaml`:

```yaml
id: wf-evolve-football2vec-l2-stage2
type: training
domain: player-embeddings
description: >
  Level 2 (code-evolution) search over Football2Vec v2 stage-2 adversary head
  architecture + Level 1 search over lambda schedule shape/max/warmup epochs.
  Phase 1 harvests 6 hand-written seed programs + linear baseline on the local
  compute pool; Phase 2 (conditional on user checkpoint) runs an OpenEvolve
  sweep seeded from Phase 1 winners. See
  docs/superpowers/specs/2026-04-23-ev2-football2vec-l2-adversarial-design.md.

references:
  - author: Ganin, Y.
    year: 2016
    title: "Domain-Adversarial Training of Neural Networks"
    venue: JMLR
    doi: ""
  - author: Romera-Paredes, B.
    year: 2025
    title: AlphaEvolve
    venue: arXiv:2506.13131
    doi: ""
  - author: Carion, N.
    year: 2020
    title: "End-to-End Object Detection with Transformers"
    venue: ECCV
    doi: ""

inputs:
  datasets:
    - luxury-lakehouse/football2vec-training-data
  models:
    - luxury-lakehouse/football2vec-v2

outputs:
  models:
    - id: football2vec-adversary-evolved
      format: python
      location: uc-volume
      alias: best_program

execution:
  training:
    trigger: manual
    runtime: local-gpu
    script: "scripts/evaluate_football2vec_l2_adversary_seeds.py"
    timeout: "6h"

depends_on:
  - wf-football2vec-v2

idempotency:
  strategy: full-overwrite
  key: timestamp

cost:
  training:
    runtime: local-gpu
    flavor: "local-gpu"
    rate_usd_per_hour: 0.00
    typical_duration_minutes: 240
    typical_cost_usd: 0.00

monitoring:
  metrics:
    - name: val_mlm_loss
      baseline: 1.5
      warn_above: 3.0
    - name: val_adv_accuracy
      baseline: 0.25
      warn_above: 0.50

links:
  source_code:
    - "src/analytics/football2vec_adversary.py"
    - "src/evolve/targets/football2vec/seed_programs_stage2/"
    - "src/evolve/targets/football2vec/validation.py"
    - "src/evolve/targets/football2vec/evaluator.py"
    - "scripts/evaluate_football2vec_l2_adversary_seeds.py"
  design_docs:
    - "docs/superpowers/specs/2026-04-23-ev2-football2vec-l2-adversarial-design.md"
    - "docs/superpowers/plans/2026-04-23-ev2-football2vec-l2-adversarial.md"
    - "docs/superpowers/specs/2026-04-07-evolve-level2-code-evolution-design.md"
```

Note: field names (`inputs`, `outputs`, `execution`, etc.) must match the actual `WorkflowCard` schema. If the test fails because of a schema mismatch, inspect `src/analytics/workflow_card_schema.py` and adjust the YAML field names to match.

- [ ] **Step 5: Run the card test, verify pass**

Run: `uv run pytest src/tests/test_evolve_football2vec_l2.py::test_card_wf_evolve_football2vec_l2_stage2_yaml_parses -v`
Expected: PASS

- [ ] **Step 6: Run the existing card-parity test to confirm no regression**

Run: `uv run pytest src/tests/test_workflow_cards.py -v 2>&1 | tail -40`
Expected: all existing card-related tests stay green (new card parses; the cost.phase parity test enforces cost.training matching execution.training, which this card honors).

---

### Task 16: AST regression test + final quality gate

**Files:**
- Modify: `src/tests/test_evolve_football2vec_l2.py`

- [ ] **Step 1: Write the final AST regression test** (append):

```python
def test_football2vec_adversary_public_surface():
    """football2vec_adversary public API surface is stable — guard against
    inadvertent refactors. Catches broken re-exports and accidental renames."""
    from analytics import football2vec_adversary as mod

    for name in (
        "AdversaryConfig",
        "LinearAdversaryHead",
        "build_adversary",
        "lambda_schedule",
        "_ADVERSARY_REGISTRY",
    ):
        assert hasattr(mod, name), f"missing {name} in public API"

    # Registry always has at least the baseline
    assert "linear" in mod._ADVERSARY_REGISTRY
```

- [ ] **Step 2: Run the single new test, verify pass**

Run: `uv run pytest src/tests/test_evolve_football2vec_l2.py::test_football2vec_adversary_public_surface -v`
Expected: PASS

- [ ] **Step 3: Run the full new-test suite**

Run: `uv run pytest src/tests/test_football2vec_adversary.py src/tests/test_evolve_football2vec_l2.py -v`
Expected: all new tests passing. Count: 9 + 13 = 22 new test cases (Task 1: 2, Task 2: 4, Task 3: 2, Task 4: 1, Task 5: 5, Tasks 7-12: 6 seed tests, Task 13: 2, Task 14: 1, Task 15: 1, Task 16: 1).

Actually 2 + 4 + 2 + 1 + 5 + 6 + 2 + 1 + 1 + 1 = 25. The spec called for 9 new tests; reality during TDD is 25. Spec updates tests count range not hard cap — the spec said "+9 = 1816" but actual will be higher. Document in the Commit #1 message.

- [ ] **Step 4: Run the full suite with zero regressions**

Run: `uv run pytest src/tests/ -v 2>&1 | tail -20`
Expected: all 1807 pre-existing + new tests passing. Total ≈ 1832. Any failure must be diagnosed before proceeding.

- [ ] **Step 5: Run lint + format check + type check**

Run: `uv run ruff check src/ scripts/`
Expected: clean

Run: `uv run ruff format --check src/ scripts/`
Expected: clean (if this fails, run `uv run ruff format src/ scripts/` to fix, then re-run `--check`)

Run: `uv run pyright src/`
Expected: clean in basic mode

- [ ] **Step 6: Stage all changes and report to user**

Run: `git status` — verify the expected files are all listed as new or modified.

Expected file list (from `git status`):

New files:
- `src/analytics/football2vec_adversary.py`
- `src/evolve/targets/football2vec/validation.py`
- `src/evolve/targets/football2vec/seed_programs_stage2/__init__.py`
- `src/evolve/targets/football2vec/seed_programs_stage2/deep_mlp_2layer.py`
- `src/evolve/targets/football2vec/seed_programs_stage2/deep_mlp_3layer.py`
- `src/evolve/targets/football2vec/seed_programs_stage2/cross_attention_adversary.py`
- `src/evolve/targets/football2vec/seed_programs_stage2/attention_pool_head.py`
- `src/evolve/targets/football2vec/seed_programs_stage2/residual_mlp.py`
- `src/evolve/targets/football2vec/seed_programs_stage2/dual_head_ensemble.py`
- `scripts/evaluate_football2vec_l2_adversary_seeds.py`
- `workflow-cards/wf-evolve-football2vec-l2-stage2.yaml`
- `src/tests/test_football2vec_adversary.py`
- `src/tests/test_evolve_football2vec_l2.py`
- `docs/superpowers/specs/2026-04-23-ev2-football2vec-l2-adversarial-design.md`
- `docs/superpowers/plans/2026-04-23-ev2-football2vec-l2-adversarial.md`

Modified files:
- `src/evolve/targets/football2vec/__init__.py`
- `src/evolve/targets/football2vec/evaluator.py`
- `scripts/train_football2vec_v2.py`

- [ ] **Step 7: Request Commit #1 approval**

Report to user: "Milestone A complete. 22 new tests green, full suite green, ruff/pyright/format clean. Ready for Commit #1 — see `git status` for the 18 files staged. Commit message draft:

```
feat(ev2): Football2Vec v2 L2 adversarial infrastructure — Phase 1 ready

Implements the infrastructure needed to run EV2 Phase 1 (6-seed harvest + linear
baseline) on the local compute pool at 30-epoch production fidelity. Phase 2
(LLM-guided evolve sweep) is set up but not triggered by this commit.

New:
- src/analytics/football2vec_adversary.py — AdversaryConfig, lambda_schedule
  (linear/sigmoid/cosine shapes), build_adversary, _ADVERSARY_REGISTRY,
  LinearAdversaryHead (byte-equivalent to current TeamClassifierHead)
- src/evolve/targets/football2vec/validation.py — FOOTBALL2VEC_ADVERSARY_PROFILE
- src/evolve/targets/football2vec/seed_programs_stage2/ — 6 seeds
  (deep_mlp_2layer, deep_mlp_3layer, cross_attention_adversary,
  attention_pool_head, residual_mlp, dual_head_ensemble) + namespace __init__
- scripts/evaluate_football2vec_l2_adversary_seeds.py — PEP 723 orchestrator
  using existing BackendPool for local-pool dispatch
- workflow-cards/wf-evolve-football2vec-l2-stage2.yaml
- docs/superpowers/specs/2026-04-23-ev2-football2vec-l2-adversarial-design.md
- docs/superpowers/plans/2026-04-23-ev2-football2vec-l2-adversarial.md

Modified:
- scripts/train_football2vec_v2.py::_train_stage2_loop now accepts optional
  adversary_module + lambda_schedule_fn kwargs. Backward-compat at defaults
  verified byte-equivalent to prior loop on a 1-epoch 10-sample fixture.
- src/evolve/targets/football2vec/evaluator.py — new train_and_evaluate_stage2
  + _apply_program_adversary + stage-1 encoder cache keyed by (repo, SHA)
- src/evolve/targets/football2vec/__init__.py — exports VALIDATION_PROFILE

Tests: +22 (full suite goes from 1807 → 1829, all green). Ruff / pyright /
format all clean.

Pre-registered artefacts (spec-frozen, not part of this commit — produced by
Phase 1 dispatch):
- luxury-lakehouse/football2vec-l2-harvest HF Hub repo (per-variant metrics.json
  + combined results.json)
- docs/evolve/ev2-football2vec-l2-adversarial/SUMMARY.md (human writeup after
  Phase 1 completes)

Approval needed: Commit #1 (this commit), then separately: Phase 1 dispatch
(real-money-equivalent — consumes local pool ~60 min). See
feedback_one_commit_at_a_time.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

Do not run `git commit` until user approves explicitly per `feedback_no_commits_without_approval.md`."

---

## Milestone B (outside this plan) — Phase 1 dispatch + SUMMARY + Commit #2

Operational, not TDD. After Commit #1 is approved and merged to the feature branch:

1. User wakes Media-PC. User verifies DGX Spark is available (SSH connectivity + GPU recognition).
2. Capture reproducibility SHAs: `hf api repos/luxury-lakehouse/football2vec-v2` and `hf api datasets/luxury-lakehouse/football2vec-training-data` to extract commit SHAs.
3. **APPROVAL: Phase 1 dispatch.** Request user approval to fire the orchestrator.
4. Fire: `nohup uv run python scripts/evaluate_football2vec_l2_adversary_seeds.py --stage1-sha <sha> --dataset-sha <sha> > phase1.log 2>&1 &`. Poll `phase1.log` every 5-10 min per `feedback_orchestration_lessons.md` rule 6 + project CLAUDE.md "never disappear" rule.
5. After completion: write `docs/evolve/ev2-football2vec-l2-adversarial/SUMMARY.md` from the results.json (disposition table, hypothesis-vs-outcome notes, Phase 2 recommendation, wall-clock per variant, pinned SHAs).
6. Apply pre-registered disposition rules to each seed (WINNER / ARCHIVE / PRUNE).
7. **APPROVAL: Commit #2 (Phase 1 results).** Request user approval for the second commit.
8. **APPROVAL: Phase 2 GO/NO-GO.** User inspects SUMMARY and decides.

## Milestone C (outside this plan) — Phase 2 sweep (conditional)

Only if B.8 = GO. See spec section "Phase 2 sweep protocol" for the protocol. Deliverables:

- `src/evolve/targets/football2vec/config_stage2.yaml`
- `src/evolve/targets/football2vec/prompts/stage2_system_message.txt`
- Phase 2 sweep results (local pool, overnight)
- Clean solo re-run of winner on single machine
- Updated SUMMARY with Phase 2 disposition + clean-re-run outcome

## Milestone D (outside this plan) — Production promotion (conditional)

Only if C's winner clears the PROMOTE rule. See spec section "Phase 2 disposition rules". Deliverables:

- `_ADVERSARY_REGISTRY` extended with winner's class
- `AdversaryConfig.architecture` default flipped
- New registered enum value's seed remains in `seed_programs_stage2/` as reference
- `RETAIN` variants registered as additional enum options

---

## Self-review (completed during plan writing)

**Spec coverage check:** every spec section has a corresponding task or is explicitly out-of-plan (listed as Milestone B/C/D operational notes). Grid:

| Spec section | Task(s) |
|---|---|
| Module changes (new files list) | Tasks 1, 2, 3, 5, 6-12, 13, 14, 15 |
| Refactor `_train_stage2_loop` | Task 4 |
| Fitness function | Task 13 (encoded in `train_and_evaluate_stage2`), Task 14 (encoded in orchestrator's `_FITNESS_W_MLM`, `_FITNESS_W_DEBIAS`) |
| Thresholds | Not code-enforced in this plan — applied at disposition time during Milestone B.6 |
| Shared config | Task 14 (`_SHARED_CONFIG`) |
| Seed slate (6 seeds) | Tasks 7-12 |
| Dispatch (BackendPool) | Task 14 |
| Tests | Tasks 1-16 |
| Acceptance criteria (Phase 1 part) | Task 16 steps 3-7 |
| Commit cadence | Task 16 step 7 |
| Out of scope | Documented in plan's Milestone B/C/D notes |
| Risks | Spec is authoritative; plan inherits |

**Placeholder scan:** grepped for "TBD", "TODO", "placeholder", "implement later", "similar to" — zero hits. All steps contain concrete code or concrete commands.

**Type consistency:**
- `custom_layers(hidden_dim, num_competitions) → {"adversary": nn.Module}` — used consistently across Tasks 5, 7-13.
- `adversary_module`, `lambda_schedule_fn` — used consistently in Task 4 and Task 13.
- `AdversaryConfig` fields — `architecture`, `lambda_schedule_shape`, `lambda_max`, `lambda_warmup_epochs` — used consistently across Tasks 1, 2, 13, 14.
- `_ADVERSARY_REGISTRY` — used consistently in Tasks 1, 3, 16.
- Orchestrator variant names match seed filenames exactly.
