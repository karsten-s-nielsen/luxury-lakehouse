# RoPE for ScoutGPTDecoder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `position_embedding: str = "learnable" | "rope"` config option to `ScoutGPTDecoder`, run an HF Jobs L40S A/B comparison against the current learnable baseline, and publish a SUMMARY.md with empirical evidence that informs a future promotion decision.

**Architecture:** Mirror the PR #151 Football2Vec pattern — one config field, three conditional branches in `ScoutGPTDecoder` (`__init__` / `_embed` / `_encode`), with `RotaryTransformerEncoder` replacing the stdlib `nn.TransformerEncoder` stack when `position_embedding == "rope"`. Training script grows two CLI args to route outputs to sibling HF repos; canonical `luxury-lakehouse/scoutgpt` is untouched.

**Tech Stack:** PyTorch 2.x, HuggingFace Hub, HF Jobs L40S, `pytest`, `ruff`, `pyright`.

**Commit policy (overrides skill default):** Per user CLAUDE.md `## Git Workflow` rule and feedback_no_staging_commits, this is a **single-commit cycle**. No per-task commits. Each task ends with local checks only; the one authorized commit happens in Task 15 after explicit user approval with a diff summary.

**Reference spec:** `docs/superpowers/specs/2026-04-19-rope-scoutgpt-decoder-design.md`

---

## File map

| File | Action | Role |
|---|---|---|
| `src/analytics/scoutgpt_decoder.py` | Modify | Add config field, three conditional branches |
| `src/analytics/scoutgpt_training.py` | Modify | Add `revision` param to `load_training_data` for SHA pinning |
| `scripts/train_scoutgpt_hf.py` | Modify | `--variant` + `--output-repo-suffix` CLI, dataset SHA pin env-var read |
| `scripts/run_rope_scoutgpt_ab.py` | Create | A/B orchestration shim |
| `src/tests/test_scoutgpt_rope.py` | Create | 8 unit tests |
| `workflow-cards/wf-scoutgpt.yaml` | Modify | Add sibling-repo entries to `outputs.models` |
| `docs/evolve/rope-scoutgpt/SUMMARY.md` | Create (Phase C) | A/B results |

---

## Phase A — Core architecture change

### Task 1: Create test file skeleton with baseline-pin tests

**Files:**
- Create: `src/tests/test_scoutgpt_rope.py`

**Rationale:** The two baseline tests pass on unmodified `scoutgpt_decoder.py` — they pin current behavior so any subsequent change that breaks the default-config path is caught immediately. Run them first to capture the exact state_dict key set.

- [ ] **Step 1: Capture the current state-dict key set from the running code**

Run:
```bash
uv run python -c "
from analytics.scoutgpt_decoder import ScoutGPTDecoder
m = ScoutGPTDecoder()
keys = sorted(m.state_dict().keys())
for k in keys:
    print(repr(k) + ',')
"
```

Save the output; each line is one key entry to paste into `EXPECTED_KEYS_LEARNABLE_DEFAULT` in Step 2. The list covers token / player / result embeddings, 4 spatial MLPs + 1 time_delta MLP, `position_embedding`, 6 transformer layers (each with `self_attn.in_proj_{weight,bias}` / `self_attn.out_proj.{weight,bias}` / `linear1` / `linear2` / `norm1` / `norm2`), action_head, vaep_head, and the `_causal_mask` / `_pos_ids` buffer keys.

- [ ] **Step 2: Write the test file**

```python
"""Unit tests for ScoutGPTDecoder RoPE config option."""

from __future__ import annotations

import pytest
import torch

from analytics.rotary_attention import RotaryTransformerEncoder
from analytics.scoutgpt_decoder import (
    BOS_TOKEN_ID,
    EXPANDED_VOCAB_SIZE,
    PAD_TOKEN_ID,
    ScoutGPTConfig,
    ScoutGPTDecoder,
)

# Captured from ScoutGPTDecoder() state_dict on 2026-04-19 (pre-RoPE).
# Regression guard: any parameter rename by accident breaks this set.
EXPECTED_KEYS_LEARNABLE_DEFAULT: frozenset[str] = frozenset({
    # <<< PASTE keys from Step 1 here, one per line, stripping trailing commas >>>
})


def _small_config(position_embedding: str = "learnable") -> ScoutGPTConfig:
    """Tiny config — fast construction, runs on CPU."""
    return ScoutGPTConfig(
        vocab_size=23,
        hidden_dim=32,
        num_layers=2,
        num_heads=4,
        dropout=0.0,
        max_seq_len=16,
        num_players=50,
        spatial_mlp_dim=8,
        position_embedding=position_embedding,
    )


def _dummy_batch(batch: int = 2, seq_len: int = 8) -> dict[str, torch.Tensor]:
    """Build a valid dummy batch at the shape ScoutGPTDecoder expects."""
    return {
        "action_ids": torch.randint(0, 23, (batch, seq_len)),
        "start_x": torch.rand(batch, seq_len),
        "start_y": torch.rand(batch, seq_len),
        "end_x": torch.rand(batch, seq_len),
        "end_y": torch.rand(batch, seq_len),
        "result": torch.randint(0, 2, (batch, seq_len)),
        "time_delta": torch.rand(batch, seq_len),
        "player_ids": torch.randint(0, 50, (batch, seq_len)),
        "attention_mask": torch.ones(batch, seq_len, dtype=torch.bool),
    }


def test_learnable_default_unchanged() -> None:
    """Default config still builds the stdlib transformer stack + learned pos emb."""
    import torch.nn as nn

    m = ScoutGPTDecoder()
    assert isinstance(m.transformer, nn.TransformerEncoder)
    assert hasattr(m, "position_embedding")
    assert isinstance(m.position_embedding, nn.Embedding)
    # _causal_mask and _pos_ids live as buffers on the module.
    assert "_causal_mask" in dict(m.named_buffers())
    assert "_pos_ids" in dict(m.named_buffers())


def test_state_dict_keys_stable_for_learnable() -> None:
    """Regression guard: accidental parameter renames break this test."""
    m = ScoutGPTDecoder()
    observed = frozenset(m.state_dict().keys())
    missing = EXPECTED_KEYS_LEARNABLE_DEFAULT - observed
    extra = observed - EXPECTED_KEYS_LEARNABLE_DEFAULT
    assert not missing, f"missing expected keys: {sorted(missing)}"
    assert not extra, f"unexpected new keys: {sorted(extra)}"
```

- [ ] **Step 3: Run the baseline-pin tests**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_rope.py::test_learnable_default_unchanged src/tests/test_scoutgpt_rope.py::test_state_dict_keys_stable_for_learnable -v
```

Expected: 2 passed. If `test_state_dict_keys_stable_for_learnable` fails with missing/extra keys, the `EXPECTED_KEYS_LEARNABLE_DEFAULT` paste from Step 1 was incomplete or malformed — re-capture and re-paste.

**Do not commit.** Leave changes on the branch.

---

### Task 2: Add `position_embedding` config field + input validator

**Files:**
- Modify: `src/analytics/scoutgpt_decoder.py` (add field to `ScoutGPTConfig`, add validator in `ScoutGPTDecoder.__init__`)
- Modify: `src/tests/test_scoutgpt_rope.py` (add validator test)

- [ ] **Step 1: Write the failing test**

Append to `src/tests/test_scoutgpt_rope.py`:

```python
def test_unknown_position_embedding_raises() -> None:
    """position_embedding must be 'learnable' or 'rope'; anything else raises."""
    for bad in ("sinusoidal", "garbage", ""):
        with pytest.raises(ValueError, match="position_embedding"):
            ScoutGPTDecoder(_small_config(position_embedding=bad))
```

- [ ] **Step 2: Run the test, verify it fails**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_rope.py::test_unknown_position_embedding_raises -v
```

Expected: FAIL — either because `ScoutGPTConfig` has no `position_embedding` field (TypeError from dataclass), or because no validator raises.

- [ ] **Step 3: Add the config field**

Edit `src/analytics/scoutgpt_decoder.py`. In `ScoutGPTConfig` (line 26), add one line at the end of the dataclass body:

```python
    position_embedding: str = "learnable"
```

The dataclass is `frozen=True` — no additional changes are needed there.

- [ ] **Step 4: Add the validator in `ScoutGPTDecoder.__init__`**

Edit `src/analytics/scoutgpt_decoder.py`. Immediately after `self.config = config or ScoutGPTConfig()` / `c = self.config` / `hd = c.hidden_dim` (around line 56–57), insert:

```python
        if c.position_embedding not in ("learnable", "rope"):
            msg = f"unknown position_embedding {c.position_embedding!r}; expected learnable|rope"
            raise ValueError(msg)
```

- [ ] **Step 5: Verify test passes**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_rope.py -v
```

Expected: all three tests pass (baseline-pin × 2 + new validator).

**Do not commit.**

---

### Task 3: Implement `__init__` RoPE branch

**Files:**
- Modify: `src/analytics/scoutgpt_decoder.py` (conditional allocation of `position_embedding`, `_causal_mask`, `_pos_ids`; swap transformer stack)
- Modify: `src/tests/test_scoutgpt_rope.py` (add rope-constructs test)

- [ ] **Step 1: Write the failing test**

Append to `src/tests/test_scoutgpt_rope.py`:

```python
def test_rope_config_constructs() -> None:
    """RoPE variant builds RotaryTransformerEncoder and skips learned pos + causal mask."""
    m = ScoutGPTDecoder(_small_config(position_embedding="rope"))
    assert isinstance(m.transformer, RotaryTransformerEncoder)
    assert not hasattr(m, "position_embedding")
    buffers = dict(m.named_buffers())
    assert "_causal_mask" not in buffers
    assert "_pos_ids" not in buffers
```

- [ ] **Step 2: Run test, verify it fails**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_rope.py::test_rope_config_constructs -v
```

Expected: FAIL — `RotaryTransformerEncoder` is not imported or wired up yet.

- [ ] **Step 3: Add the import**

Edit `src/analytics/scoutgpt_decoder.py`. The current import block has `from analytics.football2vec_transformer import SpatialMLP` (line 17). Add immediately below:

```python
from analytics.rotary_attention import RotaryTransformerEncoder
```

- [ ] **Step 4: Gate the learned `position_embedding` allocation**

Edit `src/analytics/scoutgpt_decoder.py`. Replace line 90:

```python
        # Positional embedding
        self.position_embedding = nn.Embedding(c.max_seq_len, hd)
```

with:

```python
        # Positional embedding (variant-dependent; skipped for rope — rotation applied in attention)
        if c.position_embedding == "learnable":
            self.position_embedding = nn.Embedding(c.max_seq_len, hd)
```

- [ ] **Step 5: Swap the transformer stack conditionally**

Edit `src/analytics/scoutgpt_decoder.py`. Replace the block at lines 94–103:

```python
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
```

with:

```python
        # Causal transformer. For rope, RotaryTransformerEncoder rotates Q/K inside
        # scaled dot-product attention and takes is_causal=True at forward time;
        # for learnable, the stdlib encoder stack + explicit triu causal mask.
        self.transformer: nn.Module
        if c.position_embedding == "rope":
            self.transformer = RotaryTransformerEncoder(
                d_model=hd,
                nhead=c.num_heads,
                dim_feedforward=hd * 4,
                dropout=c.dropout,
                activation="gelu",
                num_layers=c.num_layers,
                max_seq_len=c.max_seq_len,
            )
        else:
            encoder_layer = nn.TransformerEncoderLayer(
                d_model=hd,
                nhead=c.num_heads,
                dim_feedforward=hd * 4,
                dropout=c.dropout,
                activation="gelu",
                batch_first=True,
            )
            self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=c.num_layers)
```

- [ ] **Step 6: Gate `_causal_mask` and `_pos_ids` buffer registration**

Edit `src/analytics/scoutgpt_decoder.py`. Replace the block at lines 109–113:

```python
        # Pre-computed buffers (avoids per-forward-pass GPU allocation)
        self.register_buffer(
            "_causal_mask", torch.triu(torch.ones(c.max_seq_len, c.max_seq_len, dtype=torch.bool), diagonal=1)
        )
        self.register_buffer("_pos_ids", torch.arange(c.max_seq_len).unsqueeze(0))
```

with:

```python
        # Pre-computed buffers (learnable only — rope uses is_causal=True in SDPA
        # and does not index a learned position table, so neither buffer applies).
        if c.position_embedding == "learnable":
            self.register_buffer(
                "_causal_mask", torch.triu(torch.ones(c.max_seq_len, c.max_seq_len, dtype=torch.bool), diagonal=1)
            )
            self.register_buffer("_pos_ids", torch.arange(c.max_seq_len).unsqueeze(0))
```

- [ ] **Step 7: Run all tests so far**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_rope.py -v
```

Expected: 4 passed (2 baseline + validator + rope_constructs).

Baseline tests (`test_learnable_default_unchanged`, `test_state_dict_keys_stable_for_learnable`) must still pass — the default config path is still the full learnable stack.

**Do not commit.**

---

### Task 4: Implement `_embed` RoPE branch

**Files:**
- Modify: `src/analytics/scoutgpt_decoder.py` (`_embed` method)
- Modify: `src/tests/test_scoutgpt_rope.py` (add forward/predict shape test that will fail at `_encode` time in task 5 — for now we just prove `_embed` runs without IndexError)

- [ ] **Step 1: Modify `_embed` to skip additive position signal for rope**

Edit `src/analytics/scoutgpt_decoder.py`. In `_embed` at lines 157–166, the current code sums `self.position_embedding(self._pos_ids[:, :seq_len])` unconditionally into `action_emb`. Split the sum so position is added only for learnable:

Replace:

```python
        # Action embedding: all components EXCEPT player
        action_emb = (
            self.token_embedding(action_ids)
            + self.start_x_mlp(start_x)
            + self.start_y_mlp(start_y)
            + self.end_x_mlp(end_x)
            + self.end_y_mlp(end_y)
            + self.result_embedding(result)
            + self.time_delta_mlp(time_delta)
            + self.position_embedding(self._pos_ids[:, :seq_len])  # type: ignore[index]
        )
```

with:

```python
        # Action embedding: all components EXCEPT player. For rope, position is
        # applied inside attention (rotation on Q/K), not as an additive term here.
        action_emb = (
            self.token_embedding(action_ids)
            + self.start_x_mlp(start_x)
            + self.start_y_mlp(start_y)
            + self.end_x_mlp(end_x)
            + self.end_y_mlp(end_y)
            + self.result_embedding(result)
            + self.time_delta_mlp(time_delta)
        )
        if self.config.position_embedding == "learnable":
            action_emb = action_emb + self.position_embedding(self._pos_ids[:, :seq_len])  # type: ignore[index]
```

- [ ] **Step 2: Run tests — baseline and rope_constructs should still pass**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_rope.py -v
```

Expected: 4 passed.

`_embed` now runs for both variants without IndexError. `_encode` still only handles the learnable path, so forward/predict on rope will fail until Task 5.

**Do not commit.**

---

### Task 5: Implement `_encode` RoPE branch + full rope-correctness tests

This is the largest task — implements the `_encode` branch and adds four high-signal correctness tests (shape, causal property, padding mask propagation, backward).

**Files:**
- Modify: `src/analytics/scoutgpt_decoder.py` (`_encode` method)
- Modify: `src/tests/test_scoutgpt_rope.py` (add 4 tests)

- [ ] **Step 1: Write the failing tests**

Append to `src/tests/test_scoutgpt_rope.py`:

```python
def test_rope_forward_shape() -> None:
    """forward(...) returns (batch, hidden_dim); predict(...) returns expected shapes."""
    m = ScoutGPTDecoder(_small_config(position_embedding="rope"))
    m.eval()
    b = _dummy_batch(batch=2, seq_len=8)
    with torch.no_grad():
        pooled = m(**b)
        action_logits, vaep_preds = m.predict(**b)
    assert pooled.shape == (2, 32)
    assert action_logits.shape == (2, 8, 23)
    assert vaep_preds.shape == (2, 8, 1)


def test_rope_causal_property_preserved() -> None:
    """Perturbing token at position t must not change outputs at positions < t.

    The single most important correctness guard for swapping the causal mechanism
    from an explicit triu mask to is_causal=True in SDPA.
    """
    torch.manual_seed(0)
    m = ScoutGPTDecoder(_small_config(position_embedding="rope"))
    m.eval()
    b = _dummy_batch(batch=1, seq_len=8)
    perturb_pos = 5

    with torch.no_grad():
        orig_logits, _ = m.predict(**b)
        b_perturbed = dict(b)
        b_perturbed["action_ids"] = b["action_ids"].clone()
        # Flip the token at perturb_pos to a different valid action id.
        current = b["action_ids"][0, perturb_pos].item()
        b_perturbed["action_ids"][0, perturb_pos] = (current + 1) % 23
        perturbed_logits, _ = m.predict(**b_perturbed)

    # Positions 0..perturb_pos-1 must be bit-identical.
    for pos in range(perturb_pos):
        assert torch.equal(orig_logits[0, pos], perturbed_logits[0, pos]), (
            f"causal leak at position {pos} (< perturbed position {perturb_pos})"
        )
    # Sanity: the perturbed position itself must differ.
    assert not torch.equal(orig_logits[0, perturb_pos], perturbed_logits[0, perturb_pos])


def test_rope_padding_mask_preserved() -> None:
    """Scrambling padded positions must not change outputs at valid positions."""
    torch.manual_seed(0)
    m = ScoutGPTDecoder(_small_config(position_embedding="rope"))
    m.eval()
    batch, seq_len = 1, 8
    n_valid = 5  # positions 0..4 valid, 5..7 padded
    b = _dummy_batch(batch=batch, seq_len=seq_len)
    mask = torch.zeros(batch, seq_len, dtype=torch.bool)
    mask[:, :n_valid] = True
    b["attention_mask"] = mask

    with torch.no_grad():
        orig_logits, _ = m.predict(**b)

        # Scramble the padded positions — everything we can scramble.
        b_scrambled = {k: v.clone() if isinstance(v, torch.Tensor) else v for k, v in b.items()}
        for k in ("action_ids", "result", "player_ids"):
            b_scrambled[k][:, n_valid:] = torch.randint(0, 22, (batch, seq_len - n_valid))
        for k in ("start_x", "start_y", "end_x", "end_y", "time_delta"):
            b_scrambled[k][:, n_valid:] = torch.rand(batch, seq_len - n_valid)
        scrambled_logits, _ = m.predict(**b_scrambled)

    # Outputs at valid positions (0..n_valid-1) must be bit-identical.
    for pos in range(n_valid):
        assert torch.equal(orig_logits[0, pos], scrambled_logits[0, pos]), (
            f"padding leak: position {pos} changed when positions >= {n_valid} were scrambled"
        )


def test_rope_backward_produces_finite_gradients() -> None:
    """loss.backward() runs and produces finite gradients on all trainable params."""
    torch.manual_seed(0)
    m = ScoutGPTDecoder(_small_config(position_embedding="rope"))
    m.train()
    b = _dummy_batch(batch=2, seq_len=8)
    action_logits, vaep_preds = m.predict(**b)
    labels = torch.randint(0, 23, (2, 8))
    loss = torch.nn.functional.cross_entropy(action_logits.reshape(-1, 23), labels.reshape(-1))
    loss = loss + vaep_preds.pow(2).mean() * 0.1
    loss.backward()
    for name, p in m.named_parameters():
        if p.requires_grad:
            assert p.grad is not None, f"{name}: no grad"
            assert torch.isfinite(p.grad).all(), f"{name}: non-finite grad"
```

- [ ] **Step 2: Run tests, verify they fail**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_rope.py -v
```

Expected: `test_rope_forward_shape`, `test_rope_causal_property_preserved`, `test_rope_padding_mask_preserved`, `test_rope_backward_produces_finite_gradients` all FAIL — `_encode` still tries to slice `self._causal_mask` which doesn't exist on the rope variant.

- [ ] **Step 3: Modify `_encode` to branch on variant**

Edit `src/analytics/scoutgpt_decoder.py`. Replace the `_encode` body at lines 199–211:

```python
        """Run causal transformer. Returns (batch, seq_len, hidden_dim)."""
        emb = self._embed(action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids)
        seq_len = emb.size(1)

        # Pre-computed causal mask (register_buffer), sliced to seq_len
        causal_mask = self._causal_mask[:seq_len, :seq_len]  # type: ignore[index]

        # Padding mask: TransformerEncoder uses True = ignore
        src_key_padding_mask: torch.Tensor | None = None
        if attention_mask is not None:
            src_key_padding_mask = ~attention_mask

        return self.transformer(emb, mask=causal_mask, src_key_padding_mask=src_key_padding_mask)
```

with:

```python
        """Run causal transformer. Returns (batch, seq_len, hidden_dim)."""
        emb = self._embed(action_ids, start_x, start_y, end_x, end_y, result, time_delta, player_ids)

        # Padding mask: TransformerEncoder uses True = ignore
        src_key_padding_mask: torch.Tensor | None = None
        if attention_mask is not None:
            src_key_padding_mask = ~attention_mask

        if self.config.position_embedding == "rope":
            return self.transformer(emb, src_key_padding_mask=src_key_padding_mask, is_causal=True)

        # Learnable path: explicit triu causal mask sliced to seq_len.
        seq_len = emb.size(1)
        causal_mask = self._causal_mask[:seq_len, :seq_len]  # type: ignore[index]
        return self.transformer(emb, mask=causal_mask, src_key_padding_mask=src_key_padding_mask)
```

- [ ] **Step 4: Run tests, verify all pass**

Run:
```bash
uv run pytest src/tests/test_scoutgpt_rope.py -v
```

Expected: 8 passed.

If `test_rope_causal_property_preserved` fails, the rope branch is leaking future information — STOP, investigate before proceeding. This is the irreplaceable gate on the `is_causal=True` path.

**Do not commit.**

---

### Task 6: Full pre-HF sweep

**Files:** none modified.

- [ ] **Step 1: Ruff lint**

Run:
```bash
uv run ruff check src/analytics/scoutgpt_decoder.py src/tests/test_scoutgpt_rope.py
```

Expected: `All checks passed!`

- [ ] **Step 2: Ruff format check**

Run:
```bash
uv run ruff format --check src/analytics/scoutgpt_decoder.py src/tests/test_scoutgpt_rope.py
```

Expected: `N files already formatted`. If not, run `uv run ruff format <files>` to fix, then re-check.

- [ ] **Step 3: Pyright**

Run:
```bash
uv run pyright src/analytics/scoutgpt_decoder.py src/tests/test_scoutgpt_rope.py
```

Expected: `0 errors`. (Informational warnings about unknown types from `torch` are acceptable — basic mode only flags errors.)

- [ ] **Step 4: Full pytest sweep for the analytics module**

Run:
```bash
uv run pytest src/tests/ -v -k "scoutgpt or rope"
```

Expected: new rope tests pass + any existing scoutgpt tests still pass. No cross-module regressions.

- [ ] **Step 5: Benchmark sanity (optional, no CI gate)**

Run (if a GPU is available):
```bash
uv run python -c "
import time, torch
from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
for variant in ('learnable', 'rope'):
    cfg = ScoutGPTConfig(position_embedding=variant)
    m = ScoutGPTDecoder(cfg).to(device).eval()
    b = {
        'action_ids': torch.randint(0, 23, (8, 128), device=device),
        'start_x': torch.rand(8, 128, device=device),
        'start_y': torch.rand(8, 128, device=device),
        'end_x': torch.rand(8, 128, device=device),
        'end_y': torch.rand(8, 128, device=device),
        'result': torch.randint(0, 2, (8, 128), device=device),
        'time_delta': torch.rand(8, 128, device=device),
        'player_ids': torch.randint(0, 11918, (8, 128), device=device),
        'attention_mask': torch.ones(8, 128, dtype=torch.bool, device=device),
    }
    with torch.no_grad():
        # Warmup
        for _ in range(3):
            m.predict(**b)
        if device.type == 'cuda':
            torch.cuda.synchronize()
        t0 = time.perf_counter()
        for _ in range(20):
            m.predict(**b)
        if device.type == 'cuda':
            torch.cuda.synchronize()
        dt = (time.perf_counter() - t0) / 20
    print(f'{variant}: {dt*1000:.2f} ms/forward (batch=8, seq_len=128)')
"
```

Record the numbers; they will be referenced in `docs/evolve/rope-scoutgpt/SUMMARY.md` §benchmark. Not a blocker.

**Do not commit.**

---

## Phase B — Training plumbing

### Task 7: Add `revision` pin to `load_training_data`

**Files:**
- Modify: `src/analytics/scoutgpt_training.py`

**Rationale:** The A/B shim wants both variants to train against the same pinned dataset SHA. `load_training_data` currently resolves the latest SHA at call time. Adding an optional `revision` param lets the shim pass the pinned SHA to both runs.

- [ ] **Step 1: Update the signature**

Edit `src/analytics/scoutgpt_training.py`. Replace the current `load_training_data` signature at line 61:

```python
def load_training_data(
    hf_token: str,
    dataset_repo: str,
) -> tuple[pd.DataFrame, dict[str, int], str]:
```

with:

```python
def load_training_data(
    hf_token: str,
    dataset_repo: str,
    revision: str | None = None,
) -> tuple[pd.DataFrame, dict[str, int], str]:
```

- [ ] **Step 2: Thread `revision` into the three HF Hub calls**

Edit `src/analytics/scoutgpt_training.py`. In the function body:

- Line 74: `all_items = list(api.list_repo_tree(dataset_repo, repo_type="dataset", recursive=True))` → add `, revision=revision`
- Line 83: `local_path = hf_hub_download(dataset_repo, pf, repo_type="dataset", token=hf_token)` → add `, revision=revision`
- Line 101: `map_path = hf_hub_download(dataset_repo, text_files[0], repo_type="dataset", token=hf_token)` → add `, revision=revision`
- Line 103: `map_path = hf_hub_download(dataset_repo, map_files[0], repo_type="dataset", token=hf_token)` → add `, revision=revision`
- Line 109: `dataset_info = api.repo_info(repo_id=dataset_repo, repo_type="dataset")` → add `, revision=revision`

The final `return data, player_id_map, dataset_info.sha or ""` logic is unchanged — `dataset_info.sha` will equal `revision` when pinned.

- [ ] **Step 3: Verify tests still pass**

Run:
```bash
uv run pytest src/tests/ -v -k "scoutgpt or training"
```

Expected: no regressions. No new test here — `revision` is an optional pass-through; it will be exercised by the shim in Task 9.

**Do not commit.**

---

### Task 8: Add `--variant` + `--output-repo-suffix` to `train_scoutgpt_hf.py`

**Files:**
- Modify: `scripts/train_scoutgpt_hf.py`

- [ ] **Step 1: Remove the hardcoded `MODEL_REPO` module constant**

Edit `scripts/train_scoutgpt_hf.py`. At line 77, delete:

```python
MODEL_REPO = f"{HF_ORG}/scoutgpt"
```

(It is computed per-invocation from variant + suffix below.)

- [ ] **Step 2: Change `_save_checkpoint` to accept `model_repo`**

Edit `scripts/train_scoutgpt_hf.py`. Change the signature at line 89:

```python
def _save_checkpoint(
    model: ScoutGPTDecoder,
    config: ScoutGPTConfig,
    hf_token: str,
    metrics: dict[str, Any],
) -> None:
```

to:

```python
def _save_checkpoint(
    model: ScoutGPTDecoder,
    config: ScoutGPTConfig,
    hf_token: str,
    metrics: dict[str, Any],
    model_repo: str,
) -> None:
```

Inside the body, replace every `MODEL_REPO` reference with `model_repo` (lines 106, 121, 125, 130, 134).

- [ ] **Step 3: Add the new CLI args and compute `model_repo` in `main()`**

Edit `scripts/train_scoutgpt_hf.py`. In `main()` at lines 224–229, the current argparse block is:

```python
    parser = argparse.ArgumentParser(description="Train ScoutGPT on HF Jobs A10G GPU")
    parser.add_argument("--epochs", type=int, default=DEFAULT_EPOCHS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--lr", type=float, default=DEFAULT_LR)
    parser.add_argument("--patience", type=int, default=DEFAULT_PATIENCE)
    args = parser.parse_args()
```

Replace with:

```python
    parser = argparse.ArgumentParser(description="Train ScoutGPT on HF Jobs A10G GPU")
    parser.add_argument("--epochs", type=int, default=DEFAULT_EPOCHS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--lr", type=float, default=DEFAULT_LR)
    parser.add_argument("--patience", type=int, default=DEFAULT_PATIENCE)
    parser.add_argument(
        "--variant",
        type=str,
        required=True,
        choices=("learnable", "rope"),
        help="ScoutGPTConfig.position_embedding value — which A/B variant to train.",
    )
    parser.add_argument(
        "--output-repo-suffix",
        type=str,
        default="",
        help=(
            "Suffix for output HF repo name. Destination = "
            "luxury-lakehouse/scoutgpt{suffix}. Empty string writes to canonical "
            "production repo. Use e.g. '-variant-rope' for sibling-repo A/B runs."
        ),
    )
    args = parser.parse_args()

    model_repo = f"{HF_ORG}/scoutgpt{args.output_repo_suffix}"
    dataset_revision = os.environ.get("DATASET_PINNED_SHA") or None
```

- [ ] **Step 4: Update the `HFJobsCostRecorder` and `ScoutGPTConfig` construction**

Edit `scripts/train_scoutgpt_hf.py`. Replace lines 237–243 (the recorder construction):

```python
    recorder = HFJobsCostRecorder(
        workflow_id="wf-scoutgpt",
        phase="training",
        rate_usd_per_hour=HF_RATE_A10G_LARGE,
        repo_id=MODEL_REPO,
        repo_type="model",
    )
```

with:

```python
    recorder = HFJobsCostRecorder(
        workflow_id="wf-scoutgpt",
        phase="training",
        rate_usd_per_hour=HF_RATE_A10G_LARGE,
        repo_id=model_repo,
        repo_type="model",
    )
```

At line 250, change the `load_training_data` call:

```python
        data, _player_id_map, dataset_commit = load_training_data(hf_token, TRAINING_DATASET)
```

to:

```python
        data, _player_id_map, dataset_commit = load_training_data(
            hf_token, TRAINING_DATASET, revision=dataset_revision
        )
```

At line 262, replace:

```python
        config = ScoutGPTConfig()
```

with:

```python
        config = ScoutGPTConfig(position_embedding=args.variant)
```

At line 334, replace:

```python
        _save_checkpoint(model, config, hf_token, metrics)
```

with:

```python
        _save_checkpoint(model, config, hf_token, metrics, model_repo)
```

- [ ] **Step 5: Update the docstring usage example**

Edit the docstring at lines 26–32 to reflect the new required arg:

```python
Usage (HF Jobs CLI):
    hf jobs uv run scripts/train_scoutgpt_hf.py \\
        --flavor l40sx1 --timeout 180m \\
        --secrets HF_TOKEN=$HF_TOKEN \\
        --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST \\
        --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN \\
        --env DATASET_PINNED_SHA=$DATASET_SHA \\
        -- --variant rope --output-repo-suffix -variant-rope
```

- [ ] **Step 6: Lint + type-check**

Run:
```bash
uv run ruff check scripts/train_scoutgpt_hf.py
uv run ruff format --check scripts/train_scoutgpt_hf.py
uv run pyright scripts/train_scoutgpt_hf.py
```

Expected: all pass.

**Do not commit.**

---

### Task 9: Write `scripts/run_rope_scoutgpt_ab.py` orchestration shim

**Files:**
- Create: `scripts/run_rope_scoutgpt_ab.py`

**Rationale:** The shim (a) captures the dataset SHA once so both variants train on an identical revision, (b) submits two `hf jobs uv run` processes, (c) polls until both finish, (d) downloads `metrics.json` from each variant repo, (e) writes a combined `docs/evolve/rope-scoutgpt/SUMMARY.md`. Runs on the developer machine, not on HF.

- [ ] **Step 1: Create the script**

Create `scripts/run_rope_scoutgpt_ab.py` with content:

```python
"""RoPE-for-ScoutGPT A/B orchestration shim.

Submits two HF Jobs L40S runs (variant=learnable, variant=rope), pinned to the
same dataset SHA, waits for both, downloads metrics.json from each variant
sibling repo, and writes a combined SUMMARY.md.

Usage:
    uv run python scripts/run_rope_scoutgpt_ab.py \\
        [--dataset-sha <sha>] \\
        [--epochs 30] [--batch-size 256] [--lr 1e-4] [--patience 5]

Requires environment variables:
    HF_TOKEN
    MLFLOW_TRACKING_URI
    DATABRICKS_HOST
    DATABRICKS_TOKEN
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from huggingface_hub import HfApi, hf_hub_download

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
TRAINING_DATASET = f"{HF_ORG}/scoutgpt-training-data"
SUMMARY_DIR = Path("docs/evolve/rope-scoutgpt")
SUMMARY_PATH = SUMMARY_DIR / "SUMMARY.md"

VARIANTS = ("learnable", "rope")
# hf jobs ps / logs return these terminal states; poll until both runs land here.
_TERMINAL_STATES = {"COMPLETED", "SUCCEEDED", "FAILED", "CANCELED", "CANCELLED", "ERROR"}


def _require_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"{name} environment variable is required")
    return value


def _resolve_dataset_sha(hf_token: str, explicit_sha: str | None) -> str:
    if explicit_sha:
        logger.info("Using explicit dataset SHA: %s", explicit_sha)
        return explicit_sha
    api = HfApi(token=hf_token)
    info = api.repo_info(repo_id=TRAINING_DATASET, repo_type="dataset")
    sha = info.sha or ""
    if not sha:
        raise RuntimeError(f"could not resolve current SHA for {TRAINING_DATASET}")
    logger.info("Resolved current dataset SHA: %s", sha)
    return sha


def _submit_job(variant: str, dataset_sha: str, args: argparse.Namespace) -> str:
    """Submit one HF Jobs L40S run; return the job id."""
    hf_cli = shutil.which("hf")
    if not hf_cli:
        raise RuntimeError("`hf` CLI not found in PATH — install huggingface_hub[cli]")

    cmd = [
        hf_cli, "jobs", "uv", "run",
        "--flavor", "l40sx1",
        "--timeout", "180m",
        "--secrets", f"HF_TOKEN={_require_env('HF_TOKEN')}",
        "--env", f"MLFLOW_TRACKING_URI={_require_env('MLFLOW_TRACKING_URI')}",
        "--env", f"DATABRICKS_HOST={_require_env('DATABRICKS_HOST')}",
        "--env", f"DATABRICKS_TOKEN={_require_env('DATABRICKS_TOKEN')}",
        "--env", f"DATASET_PINNED_SHA={dataset_sha}",
        "--detach",
        "scripts/train_scoutgpt_hf.py",
        "--",
        "--variant", variant,
        "--output-repo-suffix", f"-variant-{variant}",
        "--epochs", str(args.epochs),
        "--batch-size", str(args.batch_size),
        "--lr", str(args.lr),
        "--patience", str(args.patience),
    ]
    logger.info("Submitting %s variant: %s", variant, " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    # `hf jobs uv run --detach` prints the job id on stdout.
    job_id = proc.stdout.strip().splitlines()[-1].strip()
    if not job_id:
        raise RuntimeError(f"failed to parse job id from: {proc.stdout!r} / {proc.stderr!r}")
    logger.info("Submitted %s variant — job id: %s", variant, job_id)
    return job_id


def _job_status(job_id: str) -> str:
    """Return the current status string for a job id, via `hf jobs ps --format json`."""
    hf_cli = shutil.which("hf")
    if not hf_cli:
        raise RuntimeError("`hf` CLI not found in PATH")
    proc = subprocess.run(
        [hf_cli, "jobs", "ps", "--all", "--format", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    try:
        rows = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"could not parse `hf jobs ps` output: {proc.stdout!r}") from exc
    for row in rows:
        if row.get("id") == job_id or row.get("ID") == job_id:
            return str(row.get("status") or row.get("Status") or "UNKNOWN").upper()
    return "UNKNOWN"


def _wait_for_completion(job_ids: dict[str, str], poll_seconds: int = 60) -> dict[str, str]:
    """Block until every job reaches a terminal state; return final statuses."""
    statuses: dict[str, str] = {v: "PENDING" for v in job_ids}
    while True:
        for variant, job_id in job_ids.items():
            if statuses[variant] not in _TERMINAL_STATES:
                statuses[variant] = _job_status(job_id)
        logger.info("Statuses: %s", statuses)
        if all(s in _TERMINAL_STATES for s in statuses.values()):
            return statuses
        time.sleep(poll_seconds)


def _download_metrics(variant: str, hf_token: str) -> dict[str, Any]:
    repo_id = f"{HF_ORG}/scoutgpt-variant-{variant}"
    with tempfile.TemporaryDirectory() as td:
        local_path = hf_hub_download(
            repo_id=repo_id,
            filename="metrics.json",
            repo_type="model",
            token=hf_token,
            local_dir=td,
        )
        with open(local_path, encoding="utf-8") as f:
            return json.load(f)


def _format_summary(
    dataset_sha: str,
    job_ids: dict[str, str],
    statuses: dict[str, str],
    metrics: dict[str, dict[str, Any]],
) -> str:
    rows = []
    for variant in VARIANTS:
        m = metrics.get(variant, {})
        rows.append(
            f"| {variant} | {m.get('test_top1_accuracy', '?'):.4f} | "
            f"{m.get('test_top5_accuracy', '?'):.4f} | "
            f"{m.get('mean_spearman_rho', '?'):.4f} | "
            f"{m.get('cross_source_gap', '?'):.4f} | "
            f"{m.get('actual_epochs', '?')} | "
            f"${m.get('total_cost_usd', '?'):.2f} |"
        ) if m else rows.append(f"| {variant} | — | — | — | — | — | — |")

    return (
        f"# RoPE-for-ScoutGPT — A/B Summary\n\n"
        f"**Dataset SHA (pinned):** `{dataset_sha}`\n\n"
        f"**HF Jobs:**\n"
        + "".join(f"- {variant}: `{job_ids[variant]}` — {statuses[variant]}\n" for variant in VARIANTS)
        + "\n## Headline metrics\n\n"
        + "| Variant | test_top1 | test_top5 | counterfactual_rho | cross_source_gap | epochs | cost |\n"
        + "|---|---:|---:|---:|---:|---:|---:|\n"
        + "\n".join(rows)
        + "\n\n## Bucket accuracy by episode length\n\n"
        + "| Variant | q1 | q2 | q3 | q4 |\n|---|---:|---:|---:|---:|\n"
        + "\n".join(
            f"| {v} | "
            + " | ".join(
                f"{metrics.get(v, {}).get(f'test_top1_accuracy_{q}', 0.0):.4f}"
                for q in ("q1", "q2", "q3", "q4")
            )
            + " |"
            for v in VARIANTS
        )
        + "\n\n## Baselines (variant-invariant; sanity check)\n\n"
        + "| Variant | most_frequent | bigram |\n|---|---:|---:|\n"
        + "\n".join(
            f"| {v} | {metrics.get(v, {}).get('baseline_most_frequent_accuracy', 0.0):.4f}"
            f" | {metrics.get(v, {}).get('baseline_bigram_accuracy', 0.0):.4f} |"
            for v in VARIANTS
        )
        + "\n\n## Recommendation\n\n"
        + "_Filled in by the human reviewer after reading the metrics above._\n\n"
        + "- Promote rope → separate approval-gated cycle\n"
        + "- Defer (inconclusive) → schedule re-run pair at +$6\n"
        + "- Reject rope (learnable wins or ties with lower complexity) → close cycle\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="RoPE-for-ScoutGPT A/B orchestration")
    parser.add_argument("--dataset-sha", default=None, help="Pin to this dataset SHA (else resolve current HEAD).")
    parser.add_argument("--epochs", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--lr", type=float, default=1e-4)
    parser.add_argument("--patience", type=int, default=5)
    parser.add_argument("--poll-seconds", type=int, default=60)
    args = parser.parse_args()

    hf_token = _require_env("HF_TOKEN")
    _require_env("MLFLOW_TRACKING_URI")
    _require_env("DATABRICKS_HOST")
    _require_env("DATABRICKS_TOKEN")

    dataset_sha = _resolve_dataset_sha(hf_token, args.dataset_sha)

    job_ids = {v: _submit_job(v, dataset_sha, args) for v in VARIANTS}

    statuses = _wait_for_completion(job_ids, poll_seconds=args.poll_seconds)
    logger.info("All jobs reached terminal state: %s", statuses)

    metrics: dict[str, dict[str, Any]] = {}
    for variant in VARIANTS:
        if statuses[variant] in {"COMPLETED", "SUCCEEDED"}:
            try:
                metrics[variant] = _download_metrics(variant, hf_token)
                logger.info("Downloaded metrics for %s", variant)
            except Exception as exc:  # noqa: BLE001 — shim script, failure is surfaced in SUMMARY
                logger.error("Could not download metrics for %s: %s", variant, exc)
                metrics[variant] = {}
        else:
            logger.warning("Variant %s did not succeed (%s) — skipping metrics", variant, statuses[variant])
            metrics[variant] = {}

    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text(
        _format_summary(dataset_sha, job_ids, statuses, metrics),
        encoding="utf-8",
    )
    logger.info("SUMMARY written to %s", SUMMARY_PATH)

    # Exit non-zero if any variant failed, so CI or calling shells can detect.
    if any(s not in {"COMPLETED", "SUCCEEDED"} for s in statuses.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
```

**Note on the broad `except Exception` in `_download_metrics`:** the `# noqa: BLE001 — shim script, failure is surfaced in SUMMARY` is architecturally justified. If a single variant's metrics download fails (network, race, 404 on not-yet-uploaded file), the SUMMARY is still produced with the other variant's data and the gap is explicit. Per CLAUDE.md `## Code Quality` ADR-002, broad catches require a `# noqa` with a written reason; this is that.

- [ ] **Step 2: Lint + type-check**

Run:
```bash
uv run ruff check scripts/run_rope_scoutgpt_ab.py
uv run ruff format --check scripts/run_rope_scoutgpt_ab.py
uv run pyright scripts/run_rope_scoutgpt_ab.py
```

Expected: all pass.

**Do not commit.**

---

### Task 10: Local integration smoke (manual)

**Files:** none modified.

**Rationale:** Confirm that the rope branch trains on a real slice of data without crashing before spending $6 of HF Jobs compute. Exercises the path end-to-end: dataset load → build dataset → model construct (rope) → train loop → eval → metrics dict.

- [ ] **Step 1: Download a tiny slice of the real dataset**

Run (interactive — requires `HF_TOKEN` set):
```bash
uv run python -c "
import os, pandas as pd
from analytics.scoutgpt_training import load_training_data, build_datasets, stratified_split
data, _, sha = load_training_data(os.environ['HF_TOKEN'], 'luxury-lakehouse/scoutgpt-training-data')
data = data.head(500)
parsed = build_datasets(data)
print('SHA:', sha)
print('n_episodes:', len(data))
print('n_actions (first):', len(parsed[0][0]))
"
```

Expected: prints dataset SHA, `n_episodes: 500`, and a small action count for the first episode. If it errors, stop and investigate the dataset access path.

- [ ] **Step 2: Run 1-epoch rope training on local GPU**

Run (interactive, ~3–5 min on RTX 5070 Ti):
```bash
uv run python -c "
import os, torch, logging, sys
logging.basicConfig(level=logging.INFO)
from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder
from analytics.scoutgpt_training import (
    load_training_data, build_datasets, stratified_split, ScoutGPTDataset, train_loop,
)

data, _, _ = load_training_data(os.environ['HF_TOKEN'], 'luxury-lakehouse/scoutgpt-training-data')
data = data.head(500).reset_index(drop=True)
parsed = build_datasets(data)
(all_atypes, all_sxs, all_sys, all_exs, all_eys, all_res, all_vaeps, all_tds, all_pidxs, all_comp_ids) = parsed

train_df, val_df, _ = stratified_split(data)
ti, vi = train_df.index.tolist(), val_df.index.tolist()

def subset(lst, idx):
    return [lst[i] for i in idx]

train_ds = ScoutGPTDataset(
    subset(all_atypes, ti), subset(all_sxs, ti), subset(all_sys, ti),
    subset(all_exs, ti), subset(all_eys, ti), subset(all_res, ti),
    subset(all_vaeps, ti), subset(all_tds, ti), subset(all_pidxs, ti),
    competition_ids=[all_comp_ids[i] for i in ti],
)
val_ds = ScoutGPTDataset(
    subset(all_atypes, vi), subset(all_sxs, vi), subset(all_sys, vi),
    subset(all_exs, vi), subset(all_eys, vi), subset(all_res, vi),
    subset(all_vaeps, vi), subset(all_tds, vi), subset(all_pidxs, vi),
    competition_ids=[all_comp_ids[i] for i in vi],
)

cfg = ScoutGPTConfig(position_embedding='rope')
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print('Device:', device)
model, history = train_loop(train_ds, val_ds, cfg, device, epochs=1, batch_size=32, lr=1e-4, patience=5)
print('train_loss (ep1):', history['train_loss'][-1])
print('val_top1 (ep1):', history['val_top1_accuracy'][-1])
assert history['train_loss'][-1] > 0 and history['train_loss'][-1] < 10
assert 0 <= history['val_top1_accuracy'][-1] <= 1
print('smoke OK')
" 2>&1 | tee /tmp/scoutgpt_rope_smoke.log
```

Expected: finishes without exceptions, prints `smoke OK`, `train_loss` in a sensible range (typically 1–4), `val_top1` in `[0, 1]`.

If this fails, STOP. Investigate before firing HF Jobs.

**Do not commit.**

---

## Phase C — A/B execution (approval-gated)

### Task 11: [APPROVAL #1] — Fire HF Jobs A/B

- [ ] **Step 1: Pause and request user approval**

Pause execution. Present to the user:

```
Ready to fire HF Jobs A/B.
  - Variant: learnable → luxury-lakehouse/scoutgpt-variant-learnable
  - Variant: rope      → luxury-lakehouse/scoutgpt-variant-rope
  - Flavor: l40sx1
  - Epochs: 30, batch: 256, lr: 1e-4, patience: 5, seed: 42
  - Dataset SHA: <pin value resolved just-in-time>
  - Est. cost: 2 × ~2h × $1.50/h = ~$6 total
  - Cannot cancel mid-run without wasting partial cost

Approve?
```

Wait for explicit approval. **Do not proceed without it.**

- [ ] **Step 2: Fire the shim**

Upon approval, run (foreground, captures output):
```bash
uv run python scripts/run_rope_scoutgpt_ab.py 2>&1 | tee /tmp/rope_scoutgpt_ab.log
```

Expected: job submission messages for both variants, then status polling every 60 s. Total wall clock ~2–4 h (the two jobs run concurrently in HF's queue).

---

### Task 12: Monitor, collect metrics, write SUMMARY.md

**Files:**
- Auto-generated by shim: `docs/evolve/rope-scoutgpt/SUMMARY.md`

- [ ] **Step 1: Wait for shim to finish**

The shim blocks until both jobs reach a terminal state, then writes `SUMMARY.md`. Monitor the tail of `/tmp/rope_scoutgpt_ab.log` for progress; do not send separate `hf jobs logs` polls in parallel (feedback_hf_jobs_monitoring).

- [ ] **Step 2: Inspect both variants' `metrics.json` (sanity)**

Run:
```bash
uv run python -c "
import json
from huggingface_hub import hf_hub_download
import os
token = os.environ['HF_TOKEN']
for v in ('learnable', 'rope'):
    path = hf_hub_download(f'luxury-lakehouse/scoutgpt-variant-{v}', 'metrics.json', repo_type='model', token=token)
    with open(path, encoding='utf-8') as f:
        m = json.load(f)
    print(f'--- {v} ---')
    for k in ('test_top1_accuracy', 'test_top5_accuracy', 'mean_spearman_rho', 'cross_source_gap', 'actual_epochs', 'total_cost_usd'):
        print(f'  {k}: {m.get(k)}')
"
```

Expected: both variants have populated metrics. If any variant's metrics are missing or zero across the board, the training run failed silently — STOP and investigate before writing the final SUMMARY.

- [ ] **Step 3: Fill in the human Recommendation section**

Open `docs/evolve/rope-scoutgpt/SUMMARY.md`. The shim writes a placeholder Recommendation section; replace the `_Filled in by the human reviewer..._` line with a concrete recommendation drawn from the metrics, citing:

- Δ `mean_spearman_rho` (rope − learnable) — the downstream-quality signal
- Δ `test_top1_accuracy`
- Bucket deltas (does rope specifically help q3/q4 long episodes?)
- `cross_source_gap` (did rope trade source-uniformity for top-line accuracy?)
- Any anomalous epoch trajectory (the EV1 epoch-7 jump signature)

Close with one of:

- **Promote** — justified if `Δrho ≥ +0.02` and `Δtest_top1 ≥ +0.01` and `cross_source_gap_rope ≤ cross_source_gap_learnable + 0.02`. Rope becomes the next cycle's default PR.
- **Defer** — deltas below noise (≤1 pp). Propose a re-run pair at +$6, or close the cycle with capability shipped but no promotion.
- **Reject** — rope loses on top-line accuracy or counterfactual rho. Close the cycle; capability remains available for explicit opt-in.

- [ ] **Step 4: Verify `SUMMARY.md` is well-formed markdown**

Run:
```bash
uv run python -c "
from pathlib import Path
content = Path('docs/evolve/rope-scoutgpt/SUMMARY.md').read_text(encoding='utf-8')
assert '# RoPE-for-ScoutGPT' in content
assert 'Dataset SHA' in content
assert 'Recommendation' in content
print('SUMMARY.md looks well-formed')
"
```

Expected: `SUMMARY.md looks well-formed`.

**Do not commit.**

---

### Task 13: Update `wf-scoutgpt.yaml` with sibling-repo entries

**Files:**
- Modify: `workflow-cards/wf-scoutgpt.yaml`

- [ ] **Step 1: Add sibling-repo entries**

Edit `workflow-cards/wf-scoutgpt.yaml`. Replace the `outputs.models` block:

```yaml
outputs:
  models:
    - id: "luxury-lakehouse/scoutgpt"
      destination: huggingface
```

with:

```yaml
outputs:
  models:
    - id: "luxury-lakehouse/scoutgpt"
      destination: huggingface
      description: "Canonical production model. Written by explicit promotion runs only."
    - id: "luxury-lakehouse/scoutgpt-variant-learnable"
      destination: huggingface
      description: "A/B variant artefact (position_embedding=learnable). Written by scripts/run_rope_scoutgpt_ab.py."
    - id: "luxury-lakehouse/scoutgpt-variant-rope"
      destination: huggingface
      description: "A/B variant artefact (position_embedding=rope). Written by scripts/run_rope_scoutgpt_ab.py."
```

- [ ] **Step 2: Validate the workflow card parses**

Run:
```bash
uv run validate-workflow-cards
```

Expected: validation passes (all cards including wf-scoutgpt). If it fails, the yaml is malformed; inspect and fix.

**Do not commit.**

---

## Phase D — Ship (approval-gated)

### Task 14: Final full-suite sweep

**Files:** none modified.

- [ ] **Step 1: Ruff (lint + format) across all touched paths**

Run:
```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
```

Expected: `All checks passed!` and `N files already formatted`.

- [ ] **Step 2: Pyright across all touched paths**

Run:
```bash
uv run pyright src/ scripts/
```

Expected: `0 errors`.

- [ ] **Step 3: Full pytest suite**

Run:
```bash
uv run pytest src/tests/ -v
```

Expected: all tests pass — the new rope tests plus every existing test. A regression on an unrelated test indicates a spill-over; stop and investigate.

- [ ] **Step 4: Verify git status matches intended deliverables**

Run:
```bash
git status -s
```

Expected output (exact file set):

```
M src/analytics/scoutgpt_decoder.py
M src/analytics/scoutgpt_training.py
M scripts/train_scoutgpt_hf.py
?? scripts/run_rope_scoutgpt_ab.py
?? src/tests/test_scoutgpt_rope.py
M workflow-cards/wf-scoutgpt.yaml
?? docs/superpowers/specs/2026-04-19-rope-scoutgpt-decoder-design.md
?? docs/superpowers/plans/2026-04-19-rope-scoutgpt-decoder.md
?? docs/evolve/rope-scoutgpt/SUMMARY.md
```

If any unexpected file appears (e.g., a cached pytest artifact, a stray edit in another module), stop and investigate. The cycle's blast radius must be exactly these files.

---

### Task 15: [APPROVAL #2] — Single commit

- [ ] **Step 1: Build the diff summary**

Run:
```bash
git diff --stat
git status -s
```

Capture both outputs for the approval request.

- [ ] **Step 2: Request user approval**

Present to the user:

```
Ready to commit the RoPE-for-ScoutGPT cycle. Single atomic commit.

Diff summary:
<paste git diff --stat output>

New files:
<paste git status -s output for untracked>

Proposed commit message:
  feat: add RoPE position-embedding option to ScoutGPTDecoder + HF Jobs A/B

Approve to commit?
```

Wait for explicit approval. **Do not commit without it.**

- [ ] **Step 3: Stage exactly the intended files (no `git add .` or `git add -A`)**

Upon approval, run:
```bash
git add \
  src/analytics/scoutgpt_decoder.py \
  src/analytics/scoutgpt_training.py \
  scripts/train_scoutgpt_hf.py \
  scripts/run_rope_scoutgpt_ab.py \
  src/tests/test_scoutgpt_rope.py \
  workflow-cards/wf-scoutgpt.yaml \
  docs/superpowers/specs/2026-04-19-rope-scoutgpt-decoder-design.md \
  docs/superpowers/plans/2026-04-19-rope-scoutgpt-decoder.md \
  docs/evolve/rope-scoutgpt/SUMMARY.md
git status -s
```

Expected: every listed file moves to staged (left column `A` or `M`); no `??` remaining. If any `??` remains, abort and review.

- [ ] **Step 4: Create the single commit**

Run:
```bash
git commit -m "$(cat <<'EOF'
feat: add RoPE position-embedding option to ScoutGPTDecoder + HF Jobs A/B

Adds `position_embedding: str = "learnable" | "rope"` config field to
ScoutGPTConfig. RoPE variant routes through RotaryTransformerEncoder (the
reusable primitive from EV1 PR #151), with causal attention driven by
is_causal=True in scaled dot-product attention rather than an explicit triu
mask.

Training script grows --variant and --output-repo-suffix so the two A/B runs
publish to sibling HF repos (luxury-lakehouse/scoutgpt-variant-{learnable,rope})
without touching the canonical production model at luxury-lakehouse/scoutgpt.

HF Jobs L40S A/B results summarised in docs/evolve/rope-scoutgpt/SUMMARY.md.
Promotion (flipping the default) is deferred to a separate approval-gated
cycle informed by those metrics — in particular the counterfactual_spearman_rho
signal that EV1 could not close on the MLM task.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git status
```

Expected: clean working tree, commit landed on `feat/rope-scoutgpt-decoder`. If pre-commit hooks fail, investigate and create a **NEW** commit (not `--amend`) per CLAUDE.md.

---

### Task 16: [APPROVAL #3] — Push + open PR

- [ ] **Step 1: Request user approval**

Present to the user:

```
Ready to push feat/rope-scoutgpt-decoder to origin and open a PR.
Base: main.
PR title: feat: add RoPE position-embedding option to ScoutGPTDecoder + HF Jobs A/B
PR body: summary + SUMMARY.md link + test plan.

Approve push + PR create?
```

Wait for explicit approval.

- [ ] **Step 2: Push the branch**

Upon approval, run:
```bash
git push -u origin feat/rope-scoutgpt-decoder
```

- [ ] **Step 3: Create the PR**

Run (heredoc body):
```bash
gh pr create --title "feat: add RoPE position-embedding option to ScoutGPTDecoder + HF Jobs A/B" --body "$(cat <<'EOF'
## Summary

- Adds `position_embedding: str = "learnable" | "rope"` to `ScoutGPTConfig`. RoPE variant swaps `nn.TransformerEncoder` for `RotaryTransformerEncoder` (the EV1 PR #151 primitive) and drops the learned absolute pos-embedding in favour of Q/K rotation inside scaled dot-product attention with `is_causal=True`.
- Training script grows `--variant` + `--output-repo-suffix` so the two A/B runs publish to sibling HF repos; canonical `luxury-lakehouse/scoutgpt` is untouched.
- HF Jobs L40S A/B results in `docs/evolve/rope-scoutgpt/SUMMARY.md`. Promotion deferred to a separate cycle informed by those metrics.

See `docs/superpowers/specs/2026-04-19-rope-scoutgpt-decoder-design.md` for the full design and `docs/superpowers/plans/2026-04-19-rope-scoutgpt-decoder.md` for the implementation plan.

## Test plan

- [x] Unit tests — `src/tests/test_scoutgpt_rope.py` (8 tests incl. causal-property preservation + padding-mask preservation)
- [x] Backward-compat: `position_embedding="learnable"` default path has identical state-dict keys (regression guard)
- [x] Local 1-epoch smoke on real dataset slice (RTX 5070 Ti)
- [x] HF Jobs L40S A/B — both variants trained, metrics in `SUMMARY.md`
- [x] `ruff check` + `ruff format --check` + `pyright` + full `pytest src/tests/` green
- [x] Workflow card `wf-scoutgpt.yaml` validates

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed. Capture it for the handoff.

- [ ] **Step 4: Report PR URL to the user**

Respond with the PR URL and a 2-line summary of what shipped. End of cycle.

---

## Post-merge (separate cycle, not part of this plan)

After the PR merges, a separate approval-gated step will update memory:

- Create `memory/project_rope_scoutgpt_cycle.md` with PR number, final numbers from SUMMARY, cost, any post-merge decisions.
- Update `memory/MEMORY.md` with one index line under Cycle Completion. Collapse or remove 2–3 stale entries at the same time so the index returns below its 200-line soft cap (currently 255).

That step is **not** in this cycle's scope and must not be done in this PR.

---

## Self-review

**Spec coverage:** Every numbered bullet in the spec maps to a task.

- Spec §"Architecture change" (3 conditional branches) → Tasks 2, 3, 4, 5
- Spec §"Training infrastructure" (`--variant`, `--output-repo-suffix`, revision pin) → Tasks 7, 8, 9
- Spec §"Workflow card update" → Task 13
- Spec §"Test strategy" (8 unit tests) → Tasks 1, 2, 3, 5 (all 8 tests landed); benchmark in Task 6 Step 5; local smoke in Task 10; HF Jobs A/B in Tasks 11–12
- Spec §"A/B protocol" (pinned SHA, identical HP, SUMMARY format) → Tasks 7, 9, 11, 12
- Spec §"Deliverables" (exact file list) → Task 14 Step 4 validates it
- Spec §"Execution order" ([APPROVAL #1/#2/#3]) → Tasks 11, 15, 16
- Spec §"Risks" → Task 5 Step 4 guards causal-path debut; Task 1 Step 3 + Task 6 Step 4 guard backward-compat; Task 12 Step 3 handles the noise-floor-tie recommendation path

**Placeholder scan:** None. All code blocks are complete and copy-pasteable; every expected output is named; every `[APPROVAL #N]` step explicitly requests approval and waits.

**Type / name consistency:**

- `--variant` values: `"learnable"` / `"rope"` used consistently (argparse `choices`, `ScoutGPTConfig.position_embedding`, shim `VARIANTS`, SUMMARY table)
- Repo suffix: empty default in `train_scoutgpt_hf.py`, shim passes `-variant-{variant}` → destinations `scoutgpt-variant-learnable` / `scoutgpt-variant-rope` (matches workflow-card sibling entries)
- `EXPECTED_KEYS_LEARNABLE_DEFAULT` → `frozenset[str]`, referenced consistently as a set operation source in `test_state_dict_keys_stable_for_learnable`
- `DATASET_PINNED_SHA` env var: set by shim (Task 9 Step 1 `_submit_job`), read by training script (Task 8 Step 3) — same name on both sides

No drift detected.
