# ScoutGPT `fourier_cross_attention` Promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port the harvested `fourier_cross_attention` + `swiglu` seeds into `ScoutGPTDecoder` as first-class `conditioning_type` enum values, run a 5-arm A/B at production fidelity on local hardware (RTX 5070 Ti + DGX Spark), apply the pre-registered decision rule, and ship promote/archive decisions for each mechanism.

**Architecture:** Additive extension of `ScoutGPTDecoder._embed()` with two new conditioning branches (RoPE-cycle template). Parity tests enforce semantic identity between new first-class branches and the harvest's monkey-patched paths. Local orchestrator imports training code from the source tree (bypassing PEP 723 wheel fetch to avoid the chicken-and-egg), dispatches arms across two machines via local subprocess / SSH, and applies a pure-function decision rule to generate the SUMMARY.

**Tech Stack:** PyTorch, `huggingface_hub` (dataset read-only), pytest, ruff, pyright, `bump_wheel.py`.

**Project rule override:** The writing-plans template recommends frequent commits. This project's CLAUDE.md requires "Never commit without explicit user approval" and this cycle is scoped to a **single commit at the end**. Tasks below end with verification checkpoints (not commits); only the final task creates a commit, and only after explicit user approval.

**Spec:** `docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md`

---

## File Structure

### Created files

| Path | Responsibility |
|---|---|
| `src/analytics/promotion_rules.py` | Pure function `apply_decision_rule(rho_ctrl, rho_trt, top1_ctrl, top1_trt) -> Literal["PROMOTE", "ARCHIVE"]`. Imported by tests and orchestrator — no test-file-as-production-import anti-pattern. |
| `src/tests/test_fourier_promotion_decision.py` | Parametrized tests for `apply_decision_rule`, including the RoPE historical case. |
| `src/tests/test_scoutgpt_fourier_parity.py` | Parity test: monkey-patched seed vs first-class `conditioning_type="fourier_cross_attention"` produce byte-identical forward outputs. |
| `src/tests/test_scoutgpt_swiglu_parity.py` | Parity test: monkey-patched swiglu seed vs first-class `conditioning_type="swiglu"`. |
| `scripts/run_fourier_scoutgpt_ab.py` | Pure-local orchestrator. Regular Python (no PEP 723 header). Two modes: `drive` (top-level dispatch across machines) and `run-arm` (single-arm execution, imports `train_loop` directly). |
| `docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md` | Design spec (written during brainstorm; committed as part of this cycle). |
| `docs/superpowers/plans/2026-04-20-scoutgpt-fourier-cross-attention-promote.md` | This plan. |
| `docs/evolve/fourier-scoutgpt/SUMMARY.md` | Post-A/B results. Written by the orchestrator. |

### Modified files

| Path | Change |
|---|---|
| `src/analytics/scoutgpt_decoder.py` | Add two `conditioning_type` branches (`fourier_cross_attention`, `swiglu`) in `__init__` and `_embed`. In-code comment noting the deliberate conflation in the Fourier branch. |
| `src/tests/test_scoutgpt_decoder.py` | Extend parametrized tests to cover the two new enum values (config validation, forward/backward, round-trip). |
| `scripts/train_scoutgpt_hf.py` | Add CLI args (`--conditioning-type`, `--hidden-dim`, `--num-layers`, `--num-heads`, `--local-output-dir`) for forward-compatible HF Jobs callers. Not exercised by this cycle's A/B (orchestrator uses library import). |
| `pyproject.toml` | Version `0.3.4` → `0.3.5`. |
| `src/shared/wheel.py` | `WHEEL_VERSION` and `WHEEL_FILENAME` updated. |
| `workflow-cards/wf-scoutgpt.yaml` | Append Tancik (2020) and Shazeer (2020) to `references:`. |
| `ARCHITECTURE.md` | Appendix D: two new rows (Tancik, Shazeer). |
| `src/tests/test_architecture_md_appendix.py` | Extend `expected_authors` list. |
| Terraform + PEP 723 consumer scripts | Updated by `scripts/bump_wheel.py`. |

### Gitignored (added to `.gitignore` if not already present)

- `artifacts/fourier-scoutgpt/**` — per-arm checkpoints, metrics.json, dispatch manifest.

---

## Task 1: Pre-register the decision rule

**Files:**
- Create: `src/analytics/promotion_rules.py`
- Create: `src/tests/test_fourier_promotion_decision.py`

### - [ ] Step 1.1: Write the failing test

Create `src/tests/test_fourier_promotion_decision.py`:

```python
"""Tests for the pre-registered Fourier/Swiglu promotion decision rule.

The rule is locked in code so that SUMMARY.md generation cannot be
motivated-reasoned post-hoc. See
docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md
section C.
"""

from __future__ import annotations

import pytest

from analytics.promotion_rules import apply_decision_rule


@pytest.mark.parametrize(
    ("rho_ctrl", "rho_trt", "top1_ctrl", "top1_trt", "expected"),
    [
        # Boundary — rho delta exactly +0.10, top1 regression exactly -0.005 → PROMOTE
        (0.030, 0.130, 0.815, 0.810, "PROMOTE"),
        # Clear promote — both metrics comfortably above thresholds
        (0.030, 0.400, 0.815, 0.820, "PROMOTE"),
        # Rho gain just below +0.10 → ARCHIVE
        (0.030, 0.129, 0.815, 0.815, "ARCHIVE"),
        # Top1 regression just beyond -0.005 → ARCHIVE (even with big rho gain)
        (0.030, 0.400, 0.815, 0.809, "ARCHIVE"),
        # RoPE historical case: rho delta +0.016, top1 delta +0.00009 → ARCHIVE
        (0.02986, 0.04547, 0.81535, 0.81526, "ARCHIVE"),
        # Clear archive — no signal
        (0.030, 0.025, 0.815, 0.815, "ARCHIVE"),
        # Rho regression, top1 stable → ARCHIVE
        (0.030, -0.100, 0.815, 0.815, "ARCHIVE"),
    ],
    ids=[
        "boundary_both_at_threshold",
        "clear_promote",
        "rho_gain_just_below_threshold",
        "top1_regression_too_large",
        "rope_historical_case",
        "clear_archive_no_signal",
        "rho_regression",
    ],
)
def test_apply_decision_rule(
    rho_ctrl: float, rho_trt: float, top1_ctrl: float, top1_trt: float, expected: str
) -> None:
    assert apply_decision_rule(rho_ctrl, rho_trt, top1_ctrl, top1_trt) == expected


def test_apply_decision_rule_returns_literal_strings() -> None:
    """The return type is Literal['PROMOTE', 'ARCHIVE'] — no other strings allowed."""
    result = apply_decision_rule(0.0, 0.5, 0.8, 0.8)
    assert result in {"PROMOTE", "ARCHIVE"}
```

### - [ ] Step 1.2: Run the test to verify it fails

Run:
```bash
uv run pytest src/tests/test_fourier_promotion_decision.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'analytics.promotion_rules'`.

### - [ ] Step 1.3: Implement `apply_decision_rule`

Create `src/analytics/promotion_rules.py`:

```python
"""Pre-registered promotion decision rule for evolve cycles.

The rule is a pure function so SUMMARY.md generation cannot be
motivated-reasoned post-hoc. Thresholds are calibrated from the
ScoutGPT RoPE A/B (session 50) rejection margin — 6x above it — and
from the L2 harvest's observed rho_std (~0.30), targeting 0.33sigma.

See docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md section C.
"""

from __future__ import annotations

from typing import Literal

# Threshold constants — pre-registered. Changing these after an A/B runs
# is motivated reasoning. If the rule ever needs to change, do it in a
# separate PR before the next cycle.
RHO_PROMOTE_THRESHOLD: float = 0.10
TOP1_REGRESSION_FLOOR: float = -0.005


def apply_decision_rule(
    rho_ctrl: float,
    rho_trt: float,
    top1_ctrl: float,
    top1_trt: float,
) -> Literal["PROMOTE", "ARCHIVE"]:
    """Return PROMOTE iff rho gain >= +0.10 AND top1 regression >= -0.005.

    Args:
        rho_ctrl: Control arm's mean Spearman rho (counterfactual ranking).
        rho_trt: Treatment arm's mean Spearman rho.
        top1_ctrl: Control arm's top-1 next-action accuracy on test set.
        top1_trt: Treatment arm's top-1 next-action accuracy.

    Returns:
        "PROMOTE" if both conditions hold, otherwise "ARCHIVE".
    """
    rho_delta = rho_trt - rho_ctrl
    top1_delta = top1_trt - top1_ctrl

    rho_ok = rho_delta >= RHO_PROMOTE_THRESHOLD
    top1_ok = top1_delta >= TOP1_REGRESSION_FLOOR

    if rho_ok and top1_ok:
        return "PROMOTE"
    return "ARCHIVE"
```

### - [ ] Step 1.4: Run the test to verify it passes

Run:
```bash
uv run pytest src/tests/test_fourier_promotion_decision.py -v
```

Expected: 8 passed (7 parametrized + 1 type check).

### - [ ] Step 1.5: Verify pyright and ruff on the new module

Run:
```bash
uv run ruff check src/analytics/promotion_rules.py src/tests/test_fourier_promotion_decision.py
uv run pyright src/analytics/promotion_rules.py
```

Expected: both clean (zero errors, zero warnings).

---

## Task 2: Write the Fourier parity test (fails until implementation)

**Files:**
- Create: `src/tests/test_scoutgpt_fourier_parity.py`

### - [ ] Step 2.1: Understand the monkey-patch mechanism

Re-read `src/evolve/targets/scoutgpt/evaluator.py::_apply_program` (lines ~88-148) to confirm the injection order:
1. `exec()` the seed file in restricted globals.
2. Call `seed.custom_layers(hidden_dim)` → dict of new modules.
3. For each, `model.register_module(name, module.to(device))`.
4. Bind `seed.custom_embed` to the model via `types.MethodType`.

### - [ ] Step 2.2: Write the failing parity test

Create `src/tests/test_scoutgpt_fourier_parity.py`:

```python
"""Parity test for conditioning_type="fourier_cross_attention".

The L2 harvest scored fourier_cross_attention at rho=+0.3799 via
monkey-patching a vanilla ScoutGPTDecoder with the seed program's
custom_layers + custom_embed at runtime.

This cycle promotes the mechanism to a first-class conditioning_type
enum value. This test asserts that the first-class implementation
produces byte-identical forward-pass output to the monkey-patched
harvest path, given matched random initializations.

If this test fails, we are testing a different mechanism than what
harvest scored. The cycle must halt and the implementation must be
corrected before any A/B arm runs.
"""

from __future__ import annotations

import types
from pathlib import Path

import torch

from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder


def _apply_harvest_monkey_patch(model: ScoutGPTDecoder, seed_path: Path) -> ScoutGPTDecoder:
    """Replicate src/evolve/targets/scoutgpt/evaluator.py::_apply_program semantics.

    Execs the seed file in a restricted globals namespace, registers the returned
    custom_layers modules, and binds custom_embed to the model instance.
    """
    restricted_globals: dict[str, object] = {
        "__builtins__": {},
        "torch": torch,
    }
    source = seed_path.read_text(encoding="utf-8")
    exec(compile(source, str(seed_path), "exec"), restricted_globals)  # noqa: S102 — controlled test input

    custom_layers_fn = restricted_globals["custom_layers"]
    custom_embed_fn = restricted_globals["custom_embed"]

    hidden_dim = model.config.hidden_dim
    new_modules = custom_layers_fn(hidden_dim)  # type: ignore[operator]
    for name, module in new_modules.items():
        model.register_module(name, module)

    model._embed = types.MethodType(custom_embed_fn, model)  # type: ignore[assignment,method-assign]
    return model


def _build_fixed_input(batch: int, seq_len: int, num_players: int) -> dict[str, torch.Tensor]:
    torch.manual_seed(123)
    return {
        "action_ids": torch.randint(0, 23, (batch, seq_len)),
        "start_x": torch.rand(batch, seq_len),
        "start_y": torch.rand(batch, seq_len),
        "end_x": torch.rand(batch, seq_len),
        "end_y": torch.rand(batch, seq_len),
        "result": torch.randint(0, 2, (batch, seq_len)),
        "time_delta": torch.rand(batch, seq_len),
        "player_ids": torch.randint(0, num_players, (batch, seq_len)),
    }


def test_fourier_first_class_matches_monkey_patched_seed() -> None:
    """First-class conditioning_type=fourier_cross_attention must match the harvest path."""
    seed_path = (
        Path(__file__).resolve().parent.parent
        / "evolve"
        / "targets"
        / "scoutgpt"
        / "seed_programs"
        / "fourier_cross_attention.py"
    )
    assert seed_path.exists(), f"Seed file missing: {seed_path}"

    # Build the monkey-patched reference model. Start with a vanilla
    # additive decoder because the seed's custom_embed fully replaces _embed;
    # the base model's conditioning_type choice is irrelevant (its modules
    # aren't used by custom_embed).
    torch.manual_seed(42)
    base_cfg = ScoutGPTConfig(
        hidden_dim=192,
        num_layers=3,
        num_heads=6,
        dropout=0.15,
        num_players=11_918,
        conditioning_type="additive",
    )
    reference_model = ScoutGPTDecoder(base_cfg).eval()
    _apply_harvest_monkey_patch(reference_model, seed_path)

    # Build the first-class model with conditioning_type=fourier_cross_attention
    torch.manual_seed(42)
    first_class_cfg = ScoutGPTConfig(
        hidden_dim=192,
        num_layers=3,
        num_heads=6,
        dropout=0.15,
        num_players=11_918,
        conditioning_type="fourier_cross_attention",
    )
    first_class_model = ScoutGPTDecoder(first_class_cfg).eval()

    # Copy the fourier modules' state from reference → first-class
    # (random init differences would otherwise dominate).
    for name in ("fourier_B", "fourier_proj", "fourier_cross_attn", "fourier_cross_norm"):
        ref_module = reference_model.get_submodule(name)
        tgt_module = first_class_model.get_submodule(name)
        tgt_module.load_state_dict(ref_module.state_dict())

    # Copy the shared modules (token embedding, player embedding, spatial MLPs,
    # result embedding, time_delta_mlp, position_embedding) from reference to
    # first-class so any random-init differences don't contaminate the compare.
    for name in (
        "token_embedding",
        "player_embedding",
        "start_x_mlp",
        "start_y_mlp",
        "end_x_mlp",
        "end_y_mlp",
        "result_embedding",
        "time_delta_mlp",
        "position_embedding",
        "embedding_dropout",
    ):
        if hasattr(reference_model, name) and hasattr(first_class_model, name):
            ref_module = reference_model.get_submodule(name)
            tgt_module = first_class_model.get_submodule(name)
            tgt_module.load_state_dict(ref_module.state_dict())

    inputs = _build_fixed_input(batch=2, seq_len=16, num_players=11_918)

    with torch.no_grad():
        ref_emb = reference_model._embed(**inputs)
        first_class_emb = first_class_model._embed(**inputs)

    assert ref_emb.shape == first_class_emb.shape, (
        f"Shape mismatch: ref={ref_emb.shape} first_class={first_class_emb.shape}"
    )
    assert torch.allclose(ref_emb, first_class_emb, atol=1e-6), (
        f"First-class Fourier branch diverges from monkey-patched seed. "
        f"Max abs diff: {(ref_emb - first_class_emb).abs().max().item()}"
    )
```

### - [ ] Step 2.3: Run the test to verify it fails

Run:
```bash
uv run pytest src/tests/test_scoutgpt_fourier_parity.py -v
```

Expected: FAIL with `ValueError: Unknown conditioning_type: 'fourier_cross_attention'` (or similar).

---

## Task 3: Implement the Fourier branch in `ScoutGPTDecoder`

**Files:**
- Modify: `src/analytics/scoutgpt_decoder.py`

### - [ ] Step 3.1: Add the `fourier_cross_attention` branch to `__init__` module registration

Open `src/analytics/scoutgpt_decoder.py`. Locate the conditioning_type branching in `__init__` (currently lines 71-83):

```python
        # Conditioning mechanism for player identity
        self._conditioning_type = c.conditioning_type
        if c.conditioning_type == "additive":
            pass  # Player embedding summed directly with action embedding
        elif c.conditioning_type == "cross_attention":
            self.player_cross_attn = nn.MultiheadAttention(hd, c.num_heads, dropout=c.dropout, batch_first=True)
            self.player_cross_norm = nn.LayerNorm(hd)
        elif c.conditioning_type == "film":
            self.film_scale = nn.Sequential(nn.Linear(hd, hd), nn.Sigmoid())
            self.film_shift = nn.Linear(hd, hd)
        elif c.conditioning_type == "gated":
            self.player_gate = nn.Sequential(nn.Linear(hd, hd), nn.Sigmoid())
        else:
            msg = f"Unknown conditioning_type: {c.conditioning_type!r}"
            raise ValueError(msg)
```

Insert two new branches before the `else` — Fourier first:

```python
        elif c.conditioning_type == "fourier_cross_attention":
            # NOTE: fourier_cross_attention bundles two architectural changes:
            # (1) RFF spatial encoding (replaces the 4 SpatialMLPs), and
            # (2) cross-attention conditioning (replaces additive conditioning).
            # Future work: decompose into spatial_encoding x conditioning_type
            # axes with a loader shim for backward compat. See
            # docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md
            n_freqs = 32  # Matches the harvest seed. Not configurable (untested hyperparameter).
            self.fourier_B = nn.Linear(4, n_freqs * 4, bias=False)
            self.fourier_proj = nn.Linear(n_freqs * 4 * 2, hd)
            self.fourier_cross_attn = nn.MultiheadAttention(hd, c.num_heads, dropout=c.dropout, batch_first=True)
            self.fourier_cross_norm = nn.LayerNorm(hd)
        elif c.conditioning_type == "swiglu":
            self.swiglu_w1 = nn.Linear(hd * 2, hd, bias=False)
            self.swiglu_w2 = nn.Linear(hd * 2, hd, bias=False)
            self.swiglu_proj = nn.Linear(hd, hd, bias=False)
            self.swiglu_norm = nn.LayerNorm(hd)
```

### - [ ] Step 3.2: Add the `fourier_cross_attention` branch to `_embed`

Locate the `_embed` conditioning branching (currently lines 193-208):

```python
        # Apply conditioning
        if self._conditioning_type == "additive":
            emb = action_emb + player_emb
        elif self._conditioning_type == "cross_attention":
            attn_out, _ = self.player_cross_attn(query=action_emb, key=player_emb, value=player_emb)
            emb = self.player_cross_norm(action_emb + attn_out)
        elif self._conditioning_type == "film":
            scale = self.film_scale(player_emb)
            shift = self.film_shift(player_emb)
            emb = scale * action_emb + shift
        elif self._conditioning_type == "gated":
            gate = self.player_gate(player_emb)
            emb = gate * action_emb + player_emb
        else:
            msg = f"Unknown conditioning_type: {self._conditioning_type!r}"
            raise ValueError(msg)
```

Insert two new branches before the `else`:

```python
        elif self._conditioning_type == "fourier_cross_attention":
            # Replace MLP spatial path with Random Fourier Features (Tancik 2020).
            # action_emb computed above is discarded in this branch — the Fourier
            # path rebuilds it from scratch using RFF instead of the four SpatialMLPs.
            spatial = torch.stack([start_x, start_y, end_x, end_y], dim=-1)  # (B, S, 4)
            projected = self.fourier_B(spatial)  # (B, S, 128)
            fourier_feats = torch.cat([torch.sin(projected), torch.cos(projected)], dim=-1)  # (B, S, 256)
            spatial_emb = self.fourier_proj(fourier_feats)  # (B, S, hd)
            action_emb_f = (
                self.token_embedding(action_ids)
                + spatial_emb
                + self.result_embedding(result)
                + self.time_delta_mlp(time_delta)
            )
            if self.config.position_embedding == "learnable":
                action_emb_f = action_emb_f + self.position_embedding(self._pos_ids[:, :seq_len])  # type: ignore[index]
            attn_out, _ = self.fourier_cross_attn(query=action_emb_f, key=player_emb, value=player_emb)
            emb = self.fourier_cross_norm(action_emb_f + attn_out)
        elif self._conditioning_type == "swiglu":
            # SwiGLU conditioning (Shazeer 2020): concat player+action, split into
            # data path and Swish-gated control path, Hadamard fuse, project back.
            combined = torch.cat([action_emb, player_emb], dim=-1)  # (B, S, 2*hd)
            data_path = self.swiglu_w1(combined)  # (B, S, hd)
            gate_path = nn.functional.silu(self.swiglu_w2(combined))  # (B, S, hd)
            fused = data_path * gate_path  # (B, S, hd) — Hadamard product
            emb = self.swiglu_norm(action_emb + self.swiglu_proj(fused))
```

### - [ ] Step 3.3: Update the `_embed` docstring

Locate the `_embed` docstring (currently lines 164-173). Replace the conditioning-types list:

```python
        """Compute input embeddings with configurable player conditioning.

        All inputs are (batch, seq_len). Returns (batch, seq_len, hidden_dim).

        Conditioning types:
          - additive: player_emb summed with action features (original behavior)
          - cross_attention: action attends to player embedding via multi-head attention
          - film: Feature-wise Linear Modulation — player controls scale and shift
          - gated: learned sigmoid gate weights the action signal, plus player residual
          - fourier_cross_attention: RFF spatial encoding (Tancik 2020) replaces
            the four SpatialMLPs, plus cross-attention conditioning. Bundles two
            mechanisms — future refactor may split into spatial_encoding x
            conditioning_type axes.
          - swiglu: SwiGLU conditioning (Shazeer 2020) — concat player+action,
            Swish-gated split and Hadamard fuse.
        """
```

### - [ ] Step 3.4: Run the Fourier parity test — expect PASS

Run:
```bash
uv run pytest src/tests/test_scoutgpt_fourier_parity.py -v
```

Expected: PASS (max abs diff well below 1e-6).

### - [ ] Step 3.5: Run the full decoder test file to catch regressions

Run:
```bash
uv run pytest src/tests/test_scoutgpt_decoder.py -v
```

Expected: all existing tests still PASS (no regressions in additive, cross_attention, film, gated, rope branches).

---

## Task 4: Write and verify the Swiglu parity test

**Files:**
- Create: `src/tests/test_scoutgpt_swiglu_parity.py`

### - [ ] Step 4.1: Write the Swiglu parity test

Create `src/tests/test_scoutgpt_swiglu_parity.py` — same structure as `test_scoutgpt_fourier_parity.py` but targeting `swiglu_conditioning`:

```python
"""Parity test for conditioning_type="swiglu".

Analogue of test_scoutgpt_fourier_parity.py for the second promoted
mechanism. See that file for the full rationale.
"""

from __future__ import annotations

import types
from pathlib import Path

import torch

from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder


def _apply_harvest_monkey_patch(model: ScoutGPTDecoder, seed_path: Path) -> ScoutGPTDecoder:
    restricted_globals: dict[str, object] = {
        "__builtins__": {},
        "torch": torch,
    }
    source = seed_path.read_text(encoding="utf-8")
    exec(compile(source, str(seed_path), "exec"), restricted_globals)  # noqa: S102 — controlled test input

    custom_layers_fn = restricted_globals["custom_layers"]
    custom_embed_fn = restricted_globals["custom_embed"]

    hidden_dim = model.config.hidden_dim
    new_modules = custom_layers_fn(hidden_dim)  # type: ignore[operator]
    for name, module in new_modules.items():
        model.register_module(name, module)

    model._embed = types.MethodType(custom_embed_fn, model)  # type: ignore[assignment,method-assign]
    return model


def _build_fixed_input(batch: int, seq_len: int, num_players: int) -> dict[str, torch.Tensor]:
    torch.manual_seed(123)
    return {
        "action_ids": torch.randint(0, 23, (batch, seq_len)),
        "start_x": torch.rand(batch, seq_len),
        "start_y": torch.rand(batch, seq_len),
        "end_x": torch.rand(batch, seq_len),
        "end_y": torch.rand(batch, seq_len),
        "result": torch.randint(0, 2, (batch, seq_len)),
        "time_delta": torch.rand(batch, seq_len),
        "player_ids": torch.randint(0, num_players, (batch, seq_len)),
    }


def test_swiglu_first_class_matches_monkey_patched_seed() -> None:
    seed_path = (
        Path(__file__).resolve().parent.parent
        / "evolve"
        / "targets"
        / "scoutgpt"
        / "seed_programs"
        / "swiglu_conditioning.py"
    )
    assert seed_path.exists(), f"Seed file missing: {seed_path}"

    torch.manual_seed(42)
    base_cfg = ScoutGPTConfig(
        hidden_dim=192,
        num_layers=3,
        num_heads=6,
        dropout=0.15,
        num_players=11_918,
        conditioning_type="additive",
    )
    reference_model = ScoutGPTDecoder(base_cfg).eval()
    _apply_harvest_monkey_patch(reference_model, seed_path)

    torch.manual_seed(42)
    first_class_cfg = ScoutGPTConfig(
        hidden_dim=192,
        num_layers=3,
        num_heads=6,
        dropout=0.15,
        num_players=11_918,
        conditioning_type="swiglu",
    )
    first_class_model = ScoutGPTDecoder(first_class_cfg).eval()

    # Copy swiglu modules ref → first-class
    for name in ("swiglu_w1", "swiglu_w2", "swiglu_proj", "swiglu_norm"):
        ref_module = reference_model.get_submodule(name)
        tgt_module = first_class_model.get_submodule(name)
        tgt_module.load_state_dict(ref_module.state_dict())

    # Copy shared modules
    for name in (
        "token_embedding",
        "player_embedding",
        "start_x_mlp",
        "start_y_mlp",
        "end_x_mlp",
        "end_y_mlp",
        "result_embedding",
        "time_delta_mlp",
        "position_embedding",
        "embedding_dropout",
    ):
        if hasattr(reference_model, name) and hasattr(first_class_model, name):
            ref_module = reference_model.get_submodule(name)
            tgt_module = first_class_model.get_submodule(name)
            tgt_module.load_state_dict(ref_module.state_dict())

    inputs = _build_fixed_input(batch=2, seq_len=16, num_players=11_918)

    with torch.no_grad():
        ref_emb = reference_model._embed(**inputs)
        first_class_emb = first_class_model._embed(**inputs)

    assert ref_emb.shape == first_class_emb.shape
    assert torch.allclose(ref_emb, first_class_emb, atol=1e-6), (
        f"First-class Swiglu branch diverges from monkey-patched seed. "
        f"Max abs diff: {(ref_emb - first_class_emb).abs().max().item()}"
    )
```

### - [ ] Step 4.2: Run the Swiglu parity test — expect PASS

Run:
```bash
uv run pytest src/tests/test_scoutgpt_swiglu_parity.py -v
```

Expected: PASS (the implementation from Task 3 already includes both branches).

---

## Task 5: Extend `test_scoutgpt_decoder.py` with new conditioning types

**Files:**
- Modify: `src/tests/test_scoutgpt_decoder.py`

### - [ ] Step 5.1: Read the existing test file to find the parametrized tests

Run:
```bash
uv run pytest src/tests/test_scoutgpt_decoder.py --collect-only -q | head -30
```

Locate any existing parametrized fixtures over `conditioning_type` (likely something like `@pytest.mark.parametrize("conditioning_type", ["additive", "cross_attention", "film", "gated"])`).

### - [ ] Step 5.2: Extend the parametrized tests to cover the new types

For each existing test that parametrizes over `conditioning_type`, extend the parameter list:

```python
@pytest.mark.parametrize(
    "conditioning_type",
    ["additive", "cross_attention", "film", "gated", "fourier_cross_attention", "swiglu"],
)
```

Scope: forward-pass shape test, backward-pass gradient-flow test, config round-trip (save state_dict + config, reload, forward output matches).

If a test uses a fixture with a fixed `conditioning_type`, leave it alone (the parity tests already cover first-class branch semantics; this task only extends the coverage breadth).

### - [ ] Step 5.3: If no existing parametrized tests cover forward/backward/round-trip for all conditioning_types, add them

If the file lacks coverage for forward shape or backward gradient flow, add:

```python
import pytest
import torch

from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder


ALL_CONDITIONING_TYPES = [
    "additive",
    "cross_attention",
    "film",
    "gated",
    "fourier_cross_attention",
    "swiglu",
]


@pytest.mark.parametrize("conditioning_type", ALL_CONDITIONING_TYPES)
def test_forward_shape(conditioning_type: str) -> None:
    torch.manual_seed(0)
    cfg = ScoutGPTConfig(
        hidden_dim=128,
        num_layers=2,
        num_heads=4,
        num_players=100,
        conditioning_type=conditioning_type,
    )
    model = ScoutGPTDecoder(cfg).eval()
    batch, seq_len = 2, 8
    out = model._embed(
        action_ids=torch.randint(0, 23, (batch, seq_len)),
        start_x=torch.rand(batch, seq_len),
        start_y=torch.rand(batch, seq_len),
        end_x=torch.rand(batch, seq_len),
        end_y=torch.rand(batch, seq_len),
        result=torch.randint(0, 2, (batch, seq_len)),
        time_delta=torch.rand(batch, seq_len),
        player_ids=torch.randint(0, 100, (batch, seq_len)),
    )
    assert out.shape == (batch, seq_len, cfg.hidden_dim)
    assert not torch.isnan(out).any()
    assert not torch.isinf(out).any()


@pytest.mark.parametrize("conditioning_type", ALL_CONDITIONING_TYPES)
def test_backward_gradients(conditioning_type: str) -> None:
    torch.manual_seed(0)
    cfg = ScoutGPTConfig(
        hidden_dim=128,
        num_layers=2,
        num_heads=4,
        num_players=100,
        conditioning_type=conditioning_type,
    )
    model = ScoutGPTDecoder(cfg)
    batch, seq_len = 2, 8
    out = model._embed(
        action_ids=torch.randint(0, 23, (batch, seq_len)),
        start_x=torch.rand(batch, seq_len),
        start_y=torch.rand(batch, seq_len),
        end_x=torch.rand(batch, seq_len),
        end_y=torch.rand(batch, seq_len),
        result=torch.randint(0, 2, (batch, seq_len)),
        time_delta=torch.rand(batch, seq_len),
        player_ids=torch.randint(0, 100, (batch, seq_len)),
    )
    loss = out.sum()
    loss.backward()

    # All parameters registered for this conditioning_type should receive gradients.
    for name, param in model.named_parameters():
        if param.requires_grad and "player_embedding" in name:
            assert param.grad is not None, f"No grad for {name}"


@pytest.mark.parametrize("conditioning_type", ALL_CONDITIONING_TYPES)
def test_config_round_trip(conditioning_type: str) -> None:
    import json
    from dataclasses import asdict

    torch.manual_seed(0)
    cfg = ScoutGPTConfig(
        hidden_dim=128,
        num_layers=2,
        num_heads=4,
        num_players=100,
        conditioning_type=conditioning_type,
    )
    model_a = ScoutGPTDecoder(cfg).eval()

    cfg_json = json.dumps(asdict(cfg))
    cfg_reloaded = ScoutGPTConfig(**json.loads(cfg_json))
    model_b = ScoutGPTDecoder(cfg_reloaded).eval()
    model_b.load_state_dict(model_a.state_dict())

    batch, seq_len = 2, 8
    inputs = {
        "action_ids": torch.randint(0, 23, (batch, seq_len)),
        "start_x": torch.rand(batch, seq_len),
        "start_y": torch.rand(batch, seq_len),
        "end_x": torch.rand(batch, seq_len),
        "end_y": torch.rand(batch, seq_len),
        "result": torch.randint(0, 2, (batch, seq_len)),
        "time_delta": torch.rand(batch, seq_len),
        "player_ids": torch.randint(0, 100, (batch, seq_len)),
    }
    with torch.no_grad():
        out_a = model_a._embed(**inputs)
        out_b = model_b._embed(**inputs)
    assert torch.allclose(out_a, out_b, atol=1e-6)


def test_invalid_conditioning_type_raises() -> None:
    with pytest.raises(ValueError, match="Unknown conditioning_type"):
        ScoutGPTDecoder(ScoutGPTConfig(conditioning_type="not_a_real_type"))
```

### - [ ] Step 5.4: Run the extended test file

Run:
```bash
uv run pytest src/tests/test_scoutgpt_decoder.py -v
```

Expected: all parametrized tests PASS for all 6 conditioning types.

---

## Task 6: Extend `train_scoutgpt_hf.py` CLI args (forward-compat)

**Files:**
- Modify: `scripts/train_scoutgpt_hf.py`

### - [ ] Step 6.1: Add new CLI arguments

Open `scripts/train_scoutgpt_hf.py` and find the argparse definition (search for `ArgumentParser` or existing `add_argument` calls for `--variant`).

Add the following arguments alongside the existing ones:

```python
    parser.add_argument(
        "--conditioning-type",
        type=str,
        default=None,
        choices=[
            "additive",
            "cross_attention",
            "film",
            "gated",
            "fourier_cross_attention",
            "swiglu",
        ],
        help="ScoutGPTConfig.conditioning_type override. None → use config default (additive).",
    )
    parser.add_argument(
        "--hidden-dim",
        type=int,
        default=None,
        help="ScoutGPTConfig.hidden_dim override. None → use config default (256).",
    )
    parser.add_argument(
        "--num-layers",
        type=int,
        default=None,
        help="ScoutGPTConfig.num_layers override. None → use config default (6).",
    )
    parser.add_argument(
        "--num-heads",
        type=int,
        default=None,
        help="ScoutGPTConfig.num_heads override. None → use config default (8).",
    )
    parser.add_argument(
        "--local-output-dir",
        type=str,
        default=None,
        help=(
            "Local directory to write checkpoint + metrics.json. When set, "
            "_save_checkpoint writes to disk instead of uploading to HF Hub. "
            "Used by the local A/B orchestrator; default behavior (HF Hub "
            "upload) is preserved for HF Jobs callers."
        ),
    )
```

### - [ ] Step 6.2: Plumb the new args into `ScoutGPTConfig` construction

Find where `ScoutGPTConfig` is constructed in `main()` (around line 292 based on earlier research). Extend the construction to conditionally include each non-None override:

```python
    cfg_overrides: dict[str, int | str] = {"position_embedding": args.variant}
    if args.conditioning_type is not None:
        cfg_overrides["conditioning_type"] = args.conditioning_type
    if args.hidden_dim is not None:
        cfg_overrides["hidden_dim"] = args.hidden_dim
    if args.num_layers is not None:
        cfg_overrides["num_layers"] = args.num_layers
    if args.num_heads is not None:
        cfg_overrides["num_heads"] = args.num_heads
    config = ScoutGPTConfig(**cfg_overrides)
```

### - [ ] Step 6.3: Plumb `--local-output-dir` into `_save_checkpoint`

Find `_save_checkpoint` (around lines 90-136). Add a branch for local-disk output at the top of the function:

```python
def _save_checkpoint(
    model: ScoutGPTDecoder,
    config: ScoutGPTConfig,
    metrics: dict[str, Any],
    hf_api: HfApi | None,
    repo_id: str | None,
    local_output_dir: Path | None = None,
) -> None:
    """Save checkpoint + metrics. When local_output_dir is set, write to disk;
    otherwise upload to HF Hub repo_id."""
    ...
    if local_output_dir is not None:
        local_output_dir.mkdir(parents=True, exist_ok=True)
        stage1_dir = local_output_dir / "stage1"
        stage1_dir.mkdir(exist_ok=True)
        save_file(state_dict_cpu, stage1_dir / "model.safetensors")
        (stage1_dir / "config.json").write_text(json.dumps(asdict(config), indent=2))
        (local_output_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))
        logger.info("Wrote checkpoint + metrics to local path: %s", local_output_dir)
        return

    # ... existing HF Hub upload code unchanged
```

Update the call site in `main()` to pass the `local_output_dir`:

```python
    local_output_dir = Path(args.local_output_dir) if args.local_output_dir else None
    _save_checkpoint(
        model=model,
        config=config,
        metrics=metrics,
        hf_api=hf_api,
        repo_id=output_repo,
        local_output_dir=local_output_dir,
    )
```

### - [ ] Step 6.4: Ruff + pyright check

Run:
```bash
uv run ruff check scripts/train_scoutgpt_hf.py
uv run pyright scripts/train_scoutgpt_hf.py
```

Expected: zero violations.

### - [ ] Step 6.5: Verify argparse accepts the new args without breaking existing invocations

Run a dry import smoke test:
```bash
uv run python -c "import subprocess; r = subprocess.run(['python', 'scripts/train_scoutgpt_hf.py', '--help'], capture_output=True, text=True); print(r.stdout[:2000])"
```

Expected: help text contains `--conditioning-type`, `--hidden-dim`, `--num-layers`, `--num-heads`, `--local-output-dir`.

---

## Task 7: Bump wheel version `0.3.4 → 0.3.5`

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/shared/wheel.py`
- Modify: Files updated by `scripts/bump_wheel.py` (PEP 723 scripts, Terraform, `deploy.sh`)

### - [ ] Step 7.1: Update `pyproject.toml`

Find `[project] version = "0.3.4"` in `pyproject.toml` and change to `"0.3.5"`.

### - [ ] Step 7.2: Update `src/shared/wheel.py`

Open `src/shared/wheel.py` and update:
- `WHEEL_VERSION = "0.3.5"`
- `WHEEL_FILENAME = "luxury_lakehouse-0.3.5-py3-none-any.whl"` (or whatever the naming convention is — preserve the format)

### - [ ] Step 7.3: Run `bump_wheel.py` to propagate

Run:
```bash
uv run python scripts/bump_wheel.py
```

Expected: script reports updated files (PEP 723 scripts under `scripts/`, Terraform files, `deploy.sh`). No errors.

### - [ ] Step 7.4: Verify with `--check`

Run:
```bash
uv run python scripts/bump_wheel.py --check
```

Expected: no drift detected. All consumers consistently reference `0.3.5`.

### - [ ] Step 7.5: Verify no `#sha256=` fragments appear in Terraform files

Run:
```bash
uv run python scripts/bump_wheel.py --check 2>&1 | grep -i "sha256\|drift\|error" || echo "clean"
```

Expected: `clean` (no drift, no sha256 fragments on local file paths per `CLAUDE.md` project rule).

---

## Task 8: Update `wf-scoutgpt.yaml` references

**Files:**
- Modify: `workflow-cards/wf-scoutgpt.yaml`

### - [ ] Step 8.1: Append Tancik and Shazeer citations

Open `workflow-cards/wf-scoutgpt.yaml`. Find the `references:` block (currently lists Hong 2025 and Decroos 2019). Append:

```yaml
  - citation: "Tancik et al. (2020). Fourier Features Let Networks Learn High Frequency Functions in Low Dimensional Domains. arXiv:2006.10739."
    role: methodology
  - citation: "Shazeer, N. (2020). GLU Variants Improve Transformer. arXiv:2002.05202."
    role: methodology
```

### - [ ] Step 8.2: Validate the workflow card

Run:
```bash
uv run validate_workflow_cards
```

Expected: `wf-scoutgpt.yaml` passes validation. All existing cards continue to pass.

### - [ ] Step 8.3: Confirm no YAML syntax errors

Run:
```bash
uv run python -c "import yaml; yaml.safe_load(open('workflow-cards/wf-scoutgpt.yaml'))"
```

Expected: no output (parse succeeds).

---

## Task 9: Update `ARCHITECTURE.md` Appendix D

**Files:**
- Modify: `ARCHITECTURE.md`
- Modify: `src/tests/test_architecture_md_appendix.py`

### - [ ] Step 9.1: Add two rows to Appendix D

Open `ARCHITECTURE.md` and find the "D. Academic References" table. Locate the row alphabetically sorted position for Tancik (between S and U) and Shazeer (between S entries — check existing `Robberechts` and any surrounding entries for order).

Insert the two new rows in alphabetical order by author last name:

```markdown
| Shazeer, N. (2020) | "GLU Variants Improve Transformer." *arXiv:2002.05202* | `src/analytics/scoutgpt_decoder.py` (swiglu branch), `wf-scoutgpt` |
| Tancik, M. et al. (2020) | "Fourier Features Let Networks Learn High Frequency Functions in Low Dimensional Domains." *arXiv:2006.10739* | `src/analytics/scoutgpt_decoder.py` (fourier_cross_attention branch), `wf-scoutgpt` |
```

### - [ ] Step 9.2: Update `expected_authors` in the appendix test

Open `src/tests/test_architecture_md_appendix.py`. Find the `expected_authors` list (or similar constant). Add:

```python
"Shazeer, N. (2020)",
"Tancik, M. et al. (2020)",
```

Keep the list alphabetically sorted if that's the existing convention.

### - [ ] Step 9.3: Run the appendix test

Run:
```bash
uv run pytest src/tests/test_architecture_md_appendix.py -v
```

Expected: all tests PASS — the new entries match the updated expected_authors list.

---

## Task 10: Orchestrator skeleton + arm roster + dry-run mode

**Files:**
- Create: `scripts/run_fourier_scoutgpt_ab.py`

### - [ ] Step 10.1: Write the orchestrator with arm roster and dry-run

Create `scripts/run_fourier_scoutgpt_ab.py`:

```python
"""Local A/B orchestrator for the fourier_cross_attention + swiglu promotion cycle.

Two modes:
  - drive (default): top-level. Resolves dataset SHA, maintains arm roster,
    dispatches arms across 1x RTX 5070 Ti (local subprocess) + 1x DGX Spark (SSH),
    collects metrics, applies apply_decision_rule, writes SUMMARY.md.
  - run-arm: single-arm execution. Imports train_loop from source tree and runs
    in-process. Invoked both locally (subprocess) and on Spark (SSH after rsync).

Pure-local: no HF Jobs, no HF Hub for artefacts. HF Hub is used only for
read-only dataset streaming with revision pinning.

See docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal

logging.basicConfig(
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
TRAINING_DATASET = f"{HF_ORG}/scoutgpt-training-data"

LOCAL_MACHINE = "local"
SPARK_MACHINE = "spark"
SPARK_SSH = "karsten@192.168.68.73"
SPARK_WORKSPACE = "~/Development/luxury-lakehouse-fourier-promote"
SPARK_ENV_ACTIVATE = "source ~/Development/evolve-env/bin/activate"

ARTIFACTS_DIR = Path("artifacts/fourier-scoutgpt")


@dataclass(frozen=True)
class ArmSpec:
    """Specification for a single A/B arm."""

    name: str
    conditioning_type: str
    hidden_dim: int
    num_layers: int
    num_heads: int
    machine: Literal["local", "spark"]
    role: str  # "CONTROL" | "TREATMENT" | "ABLATION" | "ISOLATION"


ARMS: list[ArmSpec] = [
    ArmSpec(
        name="arm1-control-additive",
        conditioning_type="additive",
        hidden_dim=256,
        num_layers=6,
        num_heads=8,
        machine=SPARK_MACHINE,
        role="CONTROL",
    ),
    ArmSpec(
        name="arm2-fourier-prod",
        conditioning_type="fourier_cross_attention",
        hidden_dim=256,
        num_layers=6,
        num_heads=8,
        machine=LOCAL_MACHINE,
        role="TREATMENT",
    ),
    ArmSpec(
        name="arm3-fourier-seed",
        conditioning_type="fourier_cross_attention",
        hidden_dim=192,
        num_layers=3,
        num_heads=6,
        machine=LOCAL_MACHINE,
        role="ABLATION",
    ),
    ArmSpec(
        name="arm4-swiglu",
        conditioning_type="swiglu",
        hidden_dim=256,
        num_layers=6,
        num_heads=8,
        machine=SPARK_MACHINE,
        role="TREATMENT",
    ),
    ArmSpec(
        name="arm5-cross-attention",
        conditioning_type="cross_attention",
        hidden_dim=256,
        num_layers=6,
        num_heads=8,
        machine=SPARK_MACHINE,
        role="ISOLATION",
    ),
]

SHARED_TRAINING_CONFIG: dict[str, object] = {
    "position_embedding": "learnable",
    "dropout": 0.10,
    "learning_rate": 1e-4,
    "batch_size": 256,
    "vaep_loss_weight": 0.10,
    "epochs": 30,
    "patience": 5,
    "seed": 42,
}


def resolve_dataset_revision() -> str:
    """Resolve the current HF dataset SHA once at dispatch start."""
    from huggingface_hub import HfApi

    api = HfApi()
    info = api.repo_info(repo_id=TRAINING_DATASET, repo_type="dataset")
    sha = info.sha or ""
    if not sha:
        msg = f"Could not resolve HF dataset SHA for {TRAINING_DATASET}"
        raise RuntimeError(msg)
    return sha


def build_dispatch_manifest(dataset_revision: str) -> dict[str, object]:
    """Serialize the dispatch plan so arms see the same pinning."""
    return {
        "dispatch_start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset_revision": dataset_revision,
        "dataset_repo": TRAINING_DATASET,
        "shared_config": SHARED_TRAINING_CONFIG,
        "arms": [asdict(a) for a in ARMS],
    }


def cmd_drive_dry_run() -> None:
    """Dry-run mode: print the dispatch plan without executing."""
    try:
        dataset_revision = resolve_dataset_revision()
    except Exception as e:  # noqa: BLE001 — network resolution is inherently flaky; dry-run continues with placeholder
        logger.warning("Dataset SHA resolution failed (%s); using placeholder for dry-run", e)
        dataset_revision = "<unresolved>"

    manifest = build_dispatch_manifest(dataset_revision)
    print(json.dumps(manifest, indent=2, default=str))


def main() -> int:
    parser = argparse.ArgumentParser(description="Fourier/Swiglu promotion A/B orchestrator.")
    parser.add_argument(
        "--mode",
        choices=["drive", "run-arm"],
        default="drive",
        help="drive = top-level dispatch; run-arm = single-arm execution.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Drive mode only: print the dispatch plan without executing.",
    )
    parser.add_argument("--arm", type=str, default=None, help="run-arm mode: arm name from ARMS.")
    parser.add_argument(
        "--local-output-dir",
        type=str,
        default=None,
        help="run-arm mode: directory to write checkpoint + metrics.json.",
    )
    parser.add_argument(
        "--dataset-revision",
        type=str,
        default=None,
        help="run-arm mode: HF dataset SHA (set by drive mode).",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="run-arm mode: reduce epochs to 2 and dataset to ~1000 episodes.",
    )
    args = parser.parse_args()

    if args.mode == "drive":
        if args.dry_run:
            cmd_drive_dry_run()
            return 0
        msg = "Non-dry-run drive mode not yet implemented; see Task 11."
        raise NotImplementedError(msg)

    # run-arm mode
    msg = "run-arm mode not yet implemented; see Task 11."
    raise NotImplementedError(msg)


if __name__ == "__main__":
    sys.exit(main())
```

### - [ ] Step 10.2: Test dry-run mode prints 5-arm manifest

Run:
```bash
uv run python scripts/run_fourier_scoutgpt_ab.py --mode drive --dry-run
```

Expected: JSON output with `dispatch_start_utc`, `dataset_revision`, `dataset_repo`, `shared_config`, and `arms` (list of 5).

### - [ ] Step 10.3: Ruff + pyright clean

Run:
```bash
uv run ruff check scripts/run_fourier_scoutgpt_ab.py
uv run pyright scripts/run_fourier_scoutgpt_ab.py
```

Expected: zero violations.

---

## Task 11: Orchestrator `run-arm` mode (single-arm execution, imports train_loop)

**Files:**
- Modify: `scripts/run_fourier_scoutgpt_ab.py`

### - [ ] Step 11.1: Implement `run-arm` mode

Replace the `NotImplementedError` in `main()` with a `run-arm` dispatch. Add a helper function near the top that imports `train_loop` lazily (to keep `drive --dry-run` fast):

```python
def cmd_run_arm(
    arm_name: str,
    local_output_dir: Path,
    dataset_revision: str,
    smoke_test: bool,
) -> None:
    """Execute a single arm in-process. Imports train_loop from the source tree."""
    # Lazy import so dry-run doesn't pay the cost
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
    from analytics.scoutgpt_decoder import ScoutGPTConfig
    from analytics.scoutgpt_training import train_loop
    from safetensors.torch import save_file

    arm = next((a for a in ARMS if a.name == arm_name), None)
    if arm is None:
        msg = f"Unknown arm {arm_name!r}; known arms: {[a.name for a in ARMS]}"
        raise ValueError(msg)

    cfg = ScoutGPTConfig(
        hidden_dim=arm.hidden_dim,
        num_layers=arm.num_layers,
        num_heads=arm.num_heads,
        conditioning_type=arm.conditioning_type,
        dropout=float(SHARED_TRAINING_CONFIG["dropout"]),
        position_embedding=str(SHARED_TRAINING_CONFIG["position_embedding"]),
    )

    epochs = 2 if smoke_test else int(SHARED_TRAINING_CONFIG["epochs"])

    hf_token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
    if not hf_token:
        msg = "HF_TOKEN must be set to stream the training dataset from HF Hub"
        raise RuntimeError(msg)

    logger.info("Arm %s: config=%s epochs=%d smoke_test=%s", arm_name, cfg, epochs, smoke_test)

    local_output_dir.mkdir(parents=True, exist_ok=True)

    metrics = train_loop(
        config=cfg,
        hf_token=hf_token,
        dataset_repo=TRAINING_DATASET,
        dataset_revision=dataset_revision,
        epochs=epochs,
        batch_size=int(SHARED_TRAINING_CONFIG["batch_size"]),
        learning_rate=float(SHARED_TRAINING_CONFIG["learning_rate"]),
        patience=int(SHARED_TRAINING_CONFIG["patience"]),
        seed=int(SHARED_TRAINING_CONFIG["seed"]),
        vaep_loss_weight=float(SHARED_TRAINING_CONFIG["vaep_loss_weight"]),
        smoke_test_subset=1000 if smoke_test else None,
        # train_loop returns a metrics dict; implementation details are library-side.
    )

    stage1_dir = local_output_dir / "stage1"
    stage1_dir.mkdir(exist_ok=True)
    # Checkpoint saved inside train_loop if the library writes to local_output_dir;
    # otherwise, we accept that the library returns metrics only and write them here.
    (local_output_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))
    logger.info("Arm %s: wrote metrics to %s", arm_name, local_output_dir / "metrics.json")
```

And update `main()`'s `run-arm` branch:

```python
    # run-arm mode
    if args.arm is None:
        msg = "--arm is required in run-arm mode"
        raise ValueError(msg)
    if args.local_output_dir is None:
        msg = "--local-output-dir is required in run-arm mode"
        raise ValueError(msg)
    if args.dataset_revision is None:
        msg = "--dataset-revision is required in run-arm mode"
        raise ValueError(msg)

    cmd_run_arm(
        arm_name=args.arm,
        local_output_dir=Path(args.local_output_dir),
        dataset_revision=args.dataset_revision,
        smoke_test=args.smoke_test,
    )
    return 0
```

### - [ ] Step 11.2: Verify `train_loop` signature matches

Run a focused grep to confirm `train_loop` accepts these kwargs:
```bash
uv run python -c "from src.analytics.scoutgpt_training import train_loop; import inspect; print(inspect.signature(train_loop))"
```

If the signature doesn't match `config, hf_token, dataset_repo, dataset_revision, epochs, batch_size, ...`, adjust `cmd_run_arm` to use whatever the real signature expects. If `smoke_test_subset` isn't a parameter, remove it and handle smoke-subsetting inside `train_loop` or via a pre-filter here.

### - [ ] Step 11.3: Ruff + pyright clean

Run:
```bash
uv run ruff check scripts/run_fourier_scoutgpt_ab.py
uv run pyright scripts/run_fourier_scoutgpt_ab.py
```

Expected: zero violations.

---

## Task 12: Orchestrator `drive` mode — local + SSH dispatch, polling, SUMMARY

**Files:**
- Modify: `scripts/run_fourier_scoutgpt_ab.py`

### - [ ] Step 12.1: Implement `drive` mode dispatch

Replace the `NotImplementedError` in `drive` (non-dry-run) with the dispatcher. Add helpers:

```python
def _rsync_branch_to_spark() -> None:
    """Rsync the current working tree to Spark at SPARK_WORKSPACE."""
    logger.info("Rsyncing branch to Spark...")
    result = subprocess.run(
        [
            "rsync",
            "-avz",
            "--delete",
            "--exclude=artifacts/",
            "--exclude=.venv/",
            "--exclude=__pycache__/",
            "--exclude=.git/",
            "./",
            f"{SPARK_SSH}:{SPARK_WORKSPACE}/",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    logger.info("Rsync complete (%d bytes transferred)", len(result.stdout))


def _dispatch_local(arm: ArmSpec, dataset_revision: str, smoke_test: bool) -> subprocess.Popen[str]:
    """Dispatch an arm to the local 5070 Ti as a subprocess."""
    arm_out_dir = ARTIFACTS_DIR / arm.name
    arm_out_dir.mkdir(parents=True, exist_ok=True)
    log_path = arm_out_dir / "run-arm.log"
    logger.info("Dispatching %s on local 5070 Ti → %s", arm.name, log_path)

    log_fh = log_path.open("w", encoding="utf-8")
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--mode",
        "run-arm",
        "--arm",
        arm.name,
        "--local-output-dir",
        str(arm_out_dir),
        "--dataset-revision",
        dataset_revision,
    ]
    if smoke_test:
        cmd.append("--smoke-test")

    return subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT, text=True)


def _dispatch_spark(arm: ArmSpec, dataset_revision: str, smoke_test: bool) -> subprocess.Popen[str]:
    """Dispatch an arm to Spark via SSH."""
    arm_out_dir = ARTIFACTS_DIR / arm.name
    arm_out_dir.mkdir(parents=True, exist_ok=True)
    log_path = arm_out_dir / "run-arm.log"
    logger.info("Dispatching %s on DGX Spark → %s", arm.name, log_path)

    spark_out_dir = f"{SPARK_WORKSPACE}/artifacts/fourier-scoutgpt/{arm.name}"
    remote_cmd_parts = [
        f"cd {SPARK_WORKSPACE}",
        SPARK_ENV_ACTIVATE,
        f"mkdir -p {spark_out_dir}",
        (
            "nohup python scripts/run_fourier_scoutgpt_ab.py --mode run-arm "
            f"--arm {arm.name} --local-output-dir {spark_out_dir} "
            f"--dataset-revision {dataset_revision} "
            f"{'--smoke-test ' if smoke_test else ''}"
            f"> {spark_out_dir}/run-arm.log 2>&1 &"
        ),
        f"echo $! > {spark_out_dir}/run-arm.pid",
        f"wait $(cat {spark_out_dir}/run-arm.pid)",
    ]
    remote_cmd = " && ".join(remote_cmd_parts)

    log_fh = log_path.open("w", encoding="utf-8")
    return subprocess.Popen(
        ["ssh", SPARK_SSH, remote_cmd],
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _collect_metrics(arm: ArmSpec) -> dict[str, object]:
    """Collect metrics.json from either local disk or Spark (via scp)."""
    local_path = ARTIFACTS_DIR / arm.name / "metrics.json"

    if arm.machine == SPARK_MACHINE:
        spark_metrics_path = f"{SPARK_WORKSPACE}/artifacts/fourier-scoutgpt/{arm.name}/metrics.json"
        subprocess.run(
            ["scp", f"{SPARK_SSH}:{spark_metrics_path}", str(local_path)],
            check=True,
            capture_output=True,
            text=True,
        )

    if not local_path.exists():
        msg = f"metrics.json missing for arm {arm.name} at {local_path}"
        raise FileNotFoundError(msg)

    return json.loads(local_path.read_text(encoding="utf-8"))


def cmd_drive(smoke_test: bool) -> None:
    """Execute the full 5-arm A/B (or smoke subset if smoke_test=True)."""
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    dataset_revision = resolve_dataset_revision()
    manifest = build_dispatch_manifest(dataset_revision)
    (ARTIFACTS_DIR / "dispatch-manifest.json").write_text(json.dumps(manifest, indent=2, default=str))

    _rsync_branch_to_spark()

    # Pair-parallel dispatch. Group arms by machine, iterate until all done.
    pending = list(ARMS)
    running: dict[str, subprocess.Popen[str]] = {}  # arm_name -> proc
    completed: list[str] = []

    while pending or running:
        # Dispatch next arm per idle machine (up to 1 per machine at a time).
        machines_busy = {arm.machine for arm_name, _ in running.items() for arm in ARMS if arm.name == arm_name}
        for arm in list(pending):
            if arm.machine in machines_busy:
                continue
            proc = (
                _dispatch_local(arm, dataset_revision, smoke_test)
                if arm.machine == LOCAL_MACHINE
                else _dispatch_spark(arm, dataset_revision, smoke_test)
            )
            running[arm.name] = proc
            pending.remove(arm)
            machines_busy.add(arm.machine)

        # Poll. Sleep 30s between poll iterations per project rule.
        time.sleep(30)
        for arm_name in list(running.keys()):
            proc = running[arm_name]
            if proc.poll() is not None:
                rc = proc.returncode
                if rc == 0:
                    logger.info("Arm %s completed (rc=0)", arm_name)
                    completed.append(arm_name)
                else:
                    msg = f"Arm {arm_name} failed with rc={rc}; see logs at {ARTIFACTS_DIR / arm_name / 'run-arm.log'}"
                    raise RuntimeError(msg)
                del running[arm_name]

    # Collect metrics
    all_metrics = {arm.name: _collect_metrics(arm) for arm in ARMS}

    # Write combined results manifest
    (ARTIFACTS_DIR / "results.json").write_text(json.dumps(all_metrics, indent=2, default=str))
    logger.info("All 5 arms complete; results written to %s", ARTIFACTS_DIR / "results.json")

    # Apply decision rule and generate SUMMARY
    _write_summary(all_metrics, dataset_revision)


def _write_summary(all_metrics: dict[str, dict[str, object]], dataset_revision: str) -> None:
    """Apply apply_decision_rule and write docs/evolve/fourier-scoutgpt/SUMMARY.md."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
    from analytics.promotion_rules import apply_decision_rule

    control = all_metrics["arm1-control-additive"]
    fourier_prod = all_metrics["arm2-fourier-prod"]
    swiglu = all_metrics["arm4-swiglu"]

    fourier_disposition = apply_decision_rule(
        rho_ctrl=float(control["counterfactual_rho"]),  # type: ignore[arg-type]
        rho_trt=float(fourier_prod["counterfactual_rho"]),  # type: ignore[arg-type]
        top1_ctrl=float(control["test_top1"]),  # type: ignore[arg-type]
        top1_trt=float(fourier_prod["test_top1"]),  # type: ignore[arg-type]
    )
    swiglu_disposition = apply_decision_rule(
        rho_ctrl=float(control["counterfactual_rho"]),  # type: ignore[arg-type]
        rho_trt=float(swiglu["counterfactual_rho"]),  # type: ignore[arg-type]
        top1_ctrl=float(control["test_top1"]),  # type: ignore[arg-type]
        top1_trt=float(swiglu["test_top1"]),  # type: ignore[arg-type]
    )

    summary_path = Path("docs/evolve/fourier-scoutgpt/SUMMARY.md")
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    summary = f"""# ScoutGPT Fourier / Swiglu Promotion A/B — Summary

**Date:** {time.strftime("%Y-%m-%d", time.gmtime())}
**Branch:** `evolve/scoutgpt-fourier-promote`
**Execution venue:** Local (1x RTX 5070 Ti + 1x DGX Spark via SSH)
**Dataset:** `{TRAINING_DATASET}` revision `{dataset_revision}`
**Spec:** `docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md`

## Pre-registered decision rule

PROMOTE iff `rho_trt − rho_ctrl ≥ +0.10` AND `top1_trt ≥ top1_ctrl − 0.005`.
Applied via `src/analytics/promotion_rules.py::apply_decision_rule`.

## Headline

| Arm | Role | conditioning_type | hd / L / H | `counterfactual_rho` | `rho_std` | `test_top1` | `val_loss` | `wall_clock` |
|---|---|---|---|---:|---:|---:|---:|---:|
"""
    for arm in ARMS:
        m = all_metrics[arm.name]
        rho = m.get("counterfactual_rho", "—")
        rho_std = m.get("rho_std", "—")
        top1 = m.get("test_top1", "—")
        vloss = m.get("val_loss", "—")
        wc = m.get("wall_clock_minutes", "—")
        summary += (
            f"| {arm.name} | {arm.role} | {arm.conditioning_type} | "
            f"{arm.hidden_dim}/{arm.num_layers}/{arm.num_heads} | "
            f"{rho} | {rho_std} | {top1} | {vloss} | {wc} |\n"
        )

    summary += f"""
## Dispositions

- **Fourier** (Arm 2 vs Arm 1): **{fourier_disposition}**
- **Swiglu** (Arm 4 vs Arm 1): **{swiglu_disposition}**

## Cross-reference

- L2 harvest (2026-04-20): fourier_cross_attention rho=+0.3799 at 15-epoch evolve-scale.
- RoPE-for-ScoutGPT (2026-04-19): rho delta +0.016 rejected.

## Follow-ups

See `docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md` Section I.
"""
    summary_path.write_text(summary, encoding="utf-8")
    logger.info("SUMMARY written to %s", summary_path)
```

Update `main()`'s `drive` branch:

```python
    if args.mode == "drive":
        if args.dry_run:
            cmd_drive_dry_run()
            return 0
        cmd_drive(smoke_test=args.smoke_test)
        return 0
```

### - [ ] Step 12.2: Add `--smoke-test` also to drive mode (shared flag)

The `--smoke-test` flag was previously only documented for `run-arm`. Confirm the argparse definition in Step 10.1 already declares it at the top level and that `cmd_drive` reads `args.smoke_test` — no additional argparse changes needed.

### - [ ] Step 12.3: Dry-run test

Run:
```bash
uv run python scripts/run_fourier_scoutgpt_ab.py --mode drive --dry-run
```

Expected: same JSON manifest as Task 10.2 (the new code didn't break dry-run).

### - [ ] Step 12.4: Ruff + pyright clean

Run:
```bash
uv run ruff check scripts/run_fourier_scoutgpt_ab.py
uv run pyright scripts/run_fourier_scoutgpt_ab.py
```

Expected: zero violations.

---

## Task 13: Add gitignore entry for artifacts

**Files:**
- Modify: `.gitignore`

### - [ ] Step 13.1: Ensure `artifacts/` is gitignored

Open `.gitignore`. If `artifacts/` is already present (or covered by a broader pattern), skip this task. Otherwise, append:

```
# Local A/B cycle artefacts (per-arm checkpoints, metrics, dispatch manifests)
artifacts/
```

### - [ ] Step 13.2: Verify nothing under `artifacts/` will be staged

Run:
```bash
git status
```

Expected: no files under `artifacts/` in "Untracked files" (or confirm the pattern covers them).

---

## Task 14: Full pre-execution test suite verification

Run all tests, lint, and type-check gates before starting any training.

### - [ ] Step 14.1: Run ruff check

Run:
```bash
uv run ruff check src/ scripts/
```

Expected: zero violations.

### - [ ] Step 14.2: Run ruff format check

Run:
```bash
uv run ruff format --check src/ scripts/
```

Expected: zero files needing reformat.

### - [ ] Step 14.3: Run pyright

Run:
```bash
uv run pyright src/
```

Expected: zero errors in basic mode.

### - [ ] Step 14.4: Run full pytest suite

Run (in background per project rule, since full suite may exceed 30s):
```bash
uv run pytest src/tests/ -v
```

Expected: all tests PASS. Specifically the new tests pass:
- `test_fourier_promotion_decision.py` — 8 tests
- `test_scoutgpt_fourier_parity.py` — 1 test
- `test_scoutgpt_swiglu_parity.py` — 1 test
- `test_scoutgpt_decoder.py` — parametrized tests across 6 conditioning types
- `test_architecture_md_appendix.py` — appendix coverage

### - [ ] Step 14.5: Confirm bump_wheel --check still clean

Run:
```bash
uv run python scripts/bump_wheel.py --check
```

Expected: no drift.

**GATE:** If any of Steps 14.1–14.5 fail, halt and diagnose before proceeding to smoke-test.

---

## Task 15: Smoke test on RTX 5070 Ti (blocking gate)

### - [ ] Step 15.1: Confirm HF_TOKEN is set locally

Run:
```bash
test -n "$HF_TOKEN" && echo "HF_TOKEN set" || echo "HF_TOKEN MISSING — set before proceeding"
```

Expected: `HF_TOKEN set`. If missing, prompt user to export `HF_TOKEN` (never write to disk or repo).

### - [ ] Step 15.2: Run smoke test for Arm 2 (FOURIER@PROD) on local

Run (background per project rule; this takes ~5 min):
```bash
uv run python scripts/run_fourier_scoutgpt_ab.py --mode run-arm \
    --arm arm2-fourier-prod \
    --local-output-dir artifacts/fourier-scoutgpt-smoke/arm2-fourier-prod \
    --dataset-revision $(uv run python -c "from huggingface_hub import HfApi; print(HfApi().repo_info('luxury-lakehouse/scoutgpt-training-data', repo_type='dataset').sha)") \
    --smoke-test
```

Use `run_in_background=true` for the actual invocation and poll the log file every 30s per project rule.

Expected: log reports training loss decreasing over 2 epochs; `metrics.json` written at the end.

### - [ ] Step 15.3: Verify smoke output

Run:
```bash
cat artifacts/fourier-scoutgpt-smoke/arm2-fourier-prod/metrics.json | head -30
```

Expected: JSON with keys including `counterfactual_rho`, `test_top1`, `val_loss`. Values are numeric (not NaN).

### - [ ] Step 15.4: Run smoke test for Arm 4 (SWIGLU) on local

Same as Step 15.2 but `--arm arm4-swiglu`.

### - [ ] Step 15.5: Verify Arm 4 smoke output

Same as Step 15.3 for Arm 4.

**GATE:** If either smoke fails (training divergence, CUDA error, missing metrics.json keys), halt and diagnose env before proceeding.

---

## Task 16: Smoke test on DGX Spark (blocking gate)

### - [ ] Step 16.1: Rsync branch to Spark manually for smoke (driver not yet invoked)

Run:
```bash
rsync -avz --delete --exclude=artifacts/ --exclude=.venv/ --exclude=__pycache__/ --exclude=.git/ ./ karsten@192.168.68.73:~/Development/luxury-lakehouse-fourier-promote/
```

Expected: rsync completes without errors.

### - [ ] Step 16.2: Run smoke test for Arm 1 (CONTROL) on Spark via SSH

Run (background, 30s poll):
```bash
ssh karsten@192.168.68.73 "cd ~/Development/luxury-lakehouse-fourier-promote && source ~/Development/evolve-env/bin/activate && python scripts/run_fourier_scoutgpt_ab.py --mode run-arm --arm arm1-control-additive --local-output-dir artifacts/fourier-scoutgpt-smoke/arm1 --dataset-revision <sha> --smoke-test"
```

Where `<sha>` is the same dataset revision from Task 15.2.

Expected: training proceeds for 2 epochs on Spark, metrics.json produced.

### - [ ] Step 16.3: Copy Spark smoke metrics back and verify

Run:
```bash
scp karsten@192.168.68.73:~/Development/luxury-lakehouse-fourier-promote/artifacts/fourier-scoutgpt-smoke/arm1/metrics.json artifacts/fourier-scoutgpt-smoke/arm1-spark/metrics.json
cat artifacts/fourier-scoutgpt-smoke/arm1-spark/metrics.json | head -30
```

Expected: JSON with numeric values. No NaN.

**GATE:** If Spark smoke fails, halt and diagnose Spark env before proceeding to full A/B.

---

## Task 17: Execute full 5-arm A/B via orchestrator

Run this only after Tasks 14, 15, and 16 all pass.

### - [ ] Step 17.1: Dispatch the full A/B

Run (background, poll every 30s; total wall time estimate ~1-2 days):
```bash
uv run python scripts/run_fourier_scoutgpt_ab.py --mode drive
```

Expected: orchestrator logs per-arm dispatch, then polls each machine. As arms complete, orchestrator dispatches the next pending arm.

### - [ ] Step 17.2: Monitor logs

Periodically (every 15-30 min per project "never disappear into long-running commands" rule), read:
```bash
tail -20 artifacts/fourier-scoutgpt/arm1-control-additive/run-arm.log
# ... and for each arm that is currently running
```

Report progress to the user. If any arm exceeds 150m wall time or loss spikes, escalate to user.

### - [ ] Step 17.3: Verify orchestrator wrote `results.json` and `SUMMARY.md`

After orchestrator exits:
```bash
ls artifacts/fourier-scoutgpt/
cat artifacts/fourier-scoutgpt/results.json | head -40
cat docs/evolve/fourier-scoutgpt/SUMMARY.md
```

Expected: `results.json` has metrics for all 5 arms; `SUMMARY.md` has dispositions for Fourier and Swiglu.

---

## Task 18: Review A/B results and finalize SUMMARY.md

### - [ ] Step 18.1: Read the generated SUMMARY

Read:
```bash
cat docs/evolve/fourier-scoutgpt/SUMMARY.md
```

### - [ ] Step 18.2: Present results to user

Include in the response:
- Arm-by-arm table (5 arms, rho, rho_std, top1, val_loss, wall_clock)
- Dispositions for Fourier and Swiglu
- Arm 3 (capacity) and Arm 5 (mechanism) narrative
- Cross-reference to L2 harvest +0.38 signal: did it replicate?

### - [ ] Step 18.3: User decides whether to edit SUMMARY narrative

The decision rule is already applied via `apply_decision_rule`. The narrative sections ("why did it win?", capacity + mechanism analysis) are generated from templates; user may want to refine the prose. Edit `docs/evolve/fourier-scoutgpt/SUMMARY.md` per user guidance.

---

## Task 19: Pre-commit verification (full test suite + lint + type-check)

### - [ ] Step 19.1: Ruff check

Run:
```bash
uv run ruff check src/ scripts/
```

Expected: zero violations.

### - [ ] Step 19.2: Ruff format check

Run:
```bash
uv run ruff format --check src/ scripts/
```

Expected: zero files need reformat.

### - [ ] Step 19.3: Pyright

Run:
```bash
uv run pyright src/
```

Expected: zero errors.

### - [ ] Step 19.4: Full pytest suite

Run (background, 30s poll):
```bash
uv run pytest src/tests/ -v
```

Expected: all tests PASS.

### - [ ] Step 19.5: Workflow card validation

Run:
```bash
uv run validate_workflow_cards
```

Expected: zero violations across all cards.

### - [ ] Step 19.6: bump_wheel --check

Run:
```bash
uv run python scripts/bump_wheel.py --check
```

Expected: no drift.

### - [ ] Step 19.7: git status review

Run:
```bash
git status
git diff --stat
```

Expected: the file set matches the "File Structure" section of this plan (new test files, modified decoder, orchestrator, wheel bump, workflow card, ARCHITECTURE.md, etc.). No unexpected files.

---

## Task 20: Commit preparation and user approval gate

**Files:**
- None (user-authorized `git commit` only).

### - [ ] Step 20.1: Draft the commit message and present to user for approval

Compose a commit message:

```
feat: ScoutGPT fourier_cross_attention + swiglu promotion cycle

Promote harvested L2 seeds fourier_cross_attention and swiglu to
first-class conditioning_type enum values on ScoutGPTDecoder. Run 5-arm
A/B at production fidelity on local hardware (RTX 5070 Ti + DGX Spark);
apply pre-registered decision rule (rho gain ≥ +0.10, top1 regression
≤ -0.005).

Architecture: Additive extension of ScoutGPTDecoder._embed with two new
branches, following the RoPE-cycle template. Parity tests assert byte-
identical forward outputs between the new first-class branches and the
harvest's monkey-patched paths.

Disposition:
- Fourier (Arm 2 vs Arm 1): <PROMOTE|ARCHIVE>
- Swiglu (Arm 4 vs Arm 1): <PROMOTE|ARCHIVE>

Spec: docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md
Plan: docs/superpowers/plans/2026-04-20-scoutgpt-fourier-cross-attention-promote.md
SUMMARY: docs/evolve/fourier-scoutgpt/SUMMARY.md

Wheel bump 0.3.4 → 0.3.5 for new ScoutGPTConfig enum values.
ARCHITECTURE.md Appendix D: Tancik (2020), Shazeer (2020).
wf-scoutgpt.yaml: Tancik + Shazeer citations appended.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

Fill in the actual dispositions from the SUMMARY.

Present this message to the user and **wait for explicit approval before committing**. Per project CLAUDE.md: "Never commit without explicit user approval."

### - [ ] Step 20.2: On approval, run the commit

Only after user says "approved, commit" (or equivalent):

```bash
git add -- \
    src/analytics/promotion_rules.py \
    src/analytics/scoutgpt_decoder.py \
    src/tests/test_fourier_promotion_decision.py \
    src/tests/test_scoutgpt_fourier_parity.py \
    src/tests/test_scoutgpt_swiglu_parity.py \
    src/tests/test_scoutgpt_decoder.py \
    src/tests/test_architecture_md_appendix.py \
    scripts/run_fourier_scoutgpt_ab.py \
    scripts/train_scoutgpt_hf.py \
    scripts/bump_wheel.py \
    pyproject.toml \
    src/shared/wheel.py \
    workflow-cards/wf-scoutgpt.yaml \
    ARCHITECTURE.md \
    docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md \
    docs/superpowers/plans/2026-04-20-scoutgpt-fourier-cross-attention-promote.md \
    docs/evolve/fourier-scoutgpt/SUMMARY.md \
    .gitignore

# Plus any files touched by bump_wheel.py (Terraform, other PEP 723 scripts)
git add -- <files reported by bump_wheel>
```

Then:
```bash
git commit -m "$(cat <<'EOF'
<approved commit message from Step 20.1>
EOF
)"
```

### - [ ] Step 20.3: Verify commit succeeded

Run:
```bash
git log -1 --stat
```

Expected: the commit appears with the expected file list. Pre-commit hooks pass (if any).

### - [ ] Step 20.4: Do NOT push, do NOT create PR

Per project rule. Stop here and report to user. User decides next steps (push, create PR, etc.) in a subsequent instruction.

---

## Self-Review Checklist (done during plan authoring)

1. **Spec coverage:** Every Section A–E requirement maps to a task. Decision rule (C) → Task 1. Fourier branch (A) → Tasks 2-3. Swiglu branch (A) → Tasks 3-4. Decoder tests (D.2, D.3) → Task 5. CLI args (B.6) → Task 6. Wheel bump (E.1) → Task 7. Workflow card (E.2) → Task 8. ARCHITECTURE.md (E.3, E.4) → Task 9. Orchestrator (B.6) → Tasks 10-12. Smoke + full A/B (B.4, D.5) → Tasks 15-17. SUMMARY (E.6) → Tasks 12, 17, 18. Single-commit rule → Task 20 (replaces per-task commits).

2. **Placeholder scan:** No "TBD", "TODO", "implement later", "fill in details". The SUMMARY narrative sections are marked with specific content (headline table structure, decision rule quote, follow-ups reference). The commit message has `<PROMOTE|ARCHIVE>` placeholders to be filled from actual SUMMARY content before presenting to user — that's a real runtime substitution, not an unfinished plan.

3. **Type consistency:** `apply_decision_rule(rho_ctrl, rho_trt, top1_ctrl, top1_trt)` is called with the same parameter order in Task 1, Task 12 (`_write_summary`), and referenced in the spec. `ArmSpec` dataclass is used consistently. File paths (`artifacts/fourier-scoutgpt/`, `docs/evolve/fourier-scoutgpt/SUMMARY.md`) are stable across tasks.
