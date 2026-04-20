# RoPE for ScoutGPTDecoder

**Date:** 2026-04-19
**Branch:** `feat/rope-scoutgpt-decoder`
**Status:** Design approved
**Follows from:** EV1 Football2Vec L1 sweep (PR #151, 2026-04-19) — listed as "RoPE for `ScoutGPTDecoder` — highest-value RoPE reuse target" in `docs/evolve/ev1-football2vec/SUMMARY.md` follow-ups.

## Problem

`ScoutGPTDecoder` (`src/analytics/scoutgpt_decoder.py`) uses learned absolute positional embeddings — a `nn.Embedding(max_seq_len=128, hidden_dim=256)` added to the token stream inside `_embed` (`scoutgpt_decoder.py:90,165`). Learned absolute positions do not generalise to unseen position ranges and encode no explicit relative-position structure.

The EV1 Football2Vec cycle produced a verified RoPE primitive:

- `src/analytics/rope.py` — byte-equivalent to HuggingFace LLaMA (`apply_rotary_pos_emb`, cos/sin tables)
- `src/analytics/rotary_attention.py` — drop-in `RotaryTransformerEncoder` with signature `forward(src, src_key_padding_mask, is_causal)` matching `nn.TransformerEncoder`
- 19 unit tests covering rotation math, HF reference parity, causal masking, and backward pass
- Reused by `Football2VecEncoder` via a `position_embedding: str = "learnable" | "sinusoidal" | "rope"` config field

This cycle ports that capability to `ScoutGPTDecoder` and produces the downstream-quality evidence that EV1 could not (EV1 ended with a +20 pp MLM win for RoPE but an uncharacterised mechanism — possibly a spatial-context shortcut in the MLM objective rather than a genuine embedding-quality improvement). ScoutGPT is a causal decoder with the `mean_spearman_rho` counterfactual-ranking metric already wired into `evaluate_and_report` — that metric is the downstream-quality probe EV1 never closed.

## Non-goals

- **Sinusoidal positional embedding:** not added. Binary config space (`learnable | rope`). Sinusoidal was only added to Football2Vec because EV1 explicitly swept it; no corresponding sweep is scheduled for ScoutGPT in this cycle.
- **Hyperparameter tuning:** not done. Current ScoutGPT defaults (`lr=1e-4`, `epochs=30`, `batch=256`, `patience=5`, `seed=42`) are held fixed across both A/B variants. Conflating RoPE with HP tuning was the explicit EV1 mistake we do not repeat.
- **Promotion to default:** deferred to a separate cycle. This cycle ships capability + A/B evidence; flipping `position_embedding="rope"` as the new default is a human decision informed by the metrics produced here.
- **Canonical HF repo overwrite:** `luxury-lakehouse/scoutgpt` is not touched. Both variants publish to sibling repos.

## Architecture change

Add one field to `ScoutGPTConfig`:

```python
position_embedding: str = "learnable"  # "learnable" | "rope"
```

Three conditional touchpoints in `scoutgpt_decoder.py`, all gated on `cfg.position_embedding == "rope"`:

1. **`__init__`.** For rope: skip allocating `self.position_embedding = nn.Embedding(...)` and skip registering the `_causal_mask` buffer (not used when `is_causal=True` flag drives SDPA's causal mask). Instantiate `self.transformer = RotaryTransformerEncoder(d_model=hidden_dim, nhead=num_heads, dim_feedforward=hidden_dim*4, dropout=dropout, activation="gelu", num_layers=num_layers, max_seq_len=max_seq_len)`. For learnable: instantiate the current stdlib `nn.TransformerEncoder` stack and the two buffers. Unknown values for `position_embedding` raise `ValueError` from `__init__`.

2. **`_embed`.** For rope: omit the `+ self.position_embedding(self._pos_ids[:, :seq_len])` term from the additive embedding sum. Token, spatial×4, result, time_delta embeddings and conditioning (additive/cross_attention/film/gated) are unchanged.

3. **`_encode`.** For rope: call `self.transformer(emb, src_key_padding_mask=src_key_padding_mask, is_causal=True)` instead of `self.transformer(emb, mask=causal_mask, src_key_padding_mask=src_key_padding_mask)`. `is_causal=True` in `RotaryMultiheadAttention.forward` routes to `F.scaled_dot_product_attention(..., is_causal=True)`, which is semantically identical to the explicit upper-triangular mask under SDPA.

### State-dict compatibility

With `position_embedding="learnable"` (the default), every parameter key is bit-identical to the pre-change model. Production checkpoints on `luxury-lakehouse/scoutgpt` continue to load under the new code without surgery. The rope branch has a different state-dict shape (no `position_embedding.weight`; `q_proj/k_proj/v_proj/out_proj/linear1/linear2` per layer in place of the stdlib `in_proj_weight`/`out_proj.weight` keys; no rope parameters because the cos/sin tables are `persistent=False`). Learnable and rope checkpoints are never mixed — variants live in distinct HF repos.

### Why `max_seq_len=128` is correct for the rope tables

ScoutGPT's `ScoutGPTConfig.max_seq_len=128` counts position 0 (BOS + focal player conditioning token) through position 127. There is no CLS prepend — the BOS token at position 0 is part of the sequence and is rotated by RoPE along with the rest. `RotaryEmbedding` is instantiated with `max_seq_len=cfg.max_seq_len=128`, no `+1`. head_dim = 256/8 = 32 is even, so RoPE is applicable without padding the head.

## Training infrastructure

`scripts/train_scoutgpt_hf.py` gains two CLI args:

- `--variant {learnable,rope}` (required, no default) — sets `ScoutGPTConfig.position_embedding`
- `--output-repo-suffix <str>` (optional, default `-variant-{variant}`) — destination repo = `luxury-lakehouse/scoutgpt{suffix}`

The PEP 723 wheel reference, training loop, evaluation pipeline, MLflow logging, and cost recording are unchanged. The script-level delta is limited to: (a) two new `argparse` entries, (b) routing `--variant` into `ScoutGPTConfig.position_embedding=args.variant` at model construction, and (c) computing the upload repo from `args.output_repo_suffix`.

`scripts/run_rope_scoutgpt_ab.py` (new) orchestrates the A/B:

1. Captures the current SHA of dataset `luxury-lakehouse/scoutgpt-training-data`
2. Submits two HF Jobs L40S runs with identical hyperparameters and pinned dataset SHA, distinct `--variant` values
3. Polls job status until both complete (or one fails)
4. Downloads `metrics.json` from each variant repo
5. Writes `docs/evolve/rope-scoutgpt/SUMMARY.md` with the combined results in the EV1 SUMMARY format

Canonical `luxury-lakehouse/scoutgpt` is not touched by this cycle. Promotion is a separate approval-gated step.

## Workflow card update

`workflow-cards/wf-scoutgpt.yaml`:

- `outputs.models` list gains `scoutgpt-variant-learnable` and `scoutgpt-variant-rope` entries alongside canonical `scoutgpt`
- No governance change. `wf-scoutgpt.yaml` is `status: development`, `eu_ai_act: not-high-risk-under-current-posture`. Adding a config option to an existing per-player evaluative system is not the add/modify/rename/remove trigger from `CLAUDE.md § AI Governance`; no `AI_GOVERNANCE.md` or HuggingFace model-card update is required for this cycle.
- No academic-references drift. RoPE (Su et al. 2021) is already cited where the primitive is used; ScoutGPT's paper stack (Hong et al. 2025, Decroos et al. 2019) is unchanged.

## Test strategy

All tests pre-commit except the HF Jobs A/B.

### Unit tests — `src/tests/test_scoutgpt_rope.py` (new file)

| Test | Assertion |
|---|---|
| `test_rope_config_constructs` | `ScoutGPTDecoder(ScoutGPTConfig(position_embedding="rope"))` builds; `self.transformer` is `RotaryTransformerEncoder`; no `position_embedding` attribute; no `_causal_mask` buffer |
| `test_learnable_default_unchanged` | `ScoutGPTDecoder(ScoutGPTConfig())` has `nn.TransformerEncoder`, `nn.Embedding` positional table, `_causal_mask` buffer present |
| `test_state_dict_keys_stable_for_learnable` | Parameter keys under default config match a hardcoded golden set (regression guard against accidental renaming) |
| `test_unknown_position_embedding_raises` | `position_embedding="sinusoidal"` and `position_embedding="garbage"` both raise `ValueError` |
| `test_rope_forward_shape` | `forward(...)` returns `(batch, hidden_dim)`; `predict(...)` returns action `(batch, seq_len, vocab_size)` and vaep `(batch, seq_len, 1)` |
| `test_rope_causal_property_preserved` | Perturbing token at position `t` does not change outputs at positions `< t` — the single most important correctness guard for swapping the causal mechanism |
| `test_rope_padding_mask_preserved` | With `attention_mask = [T,T,T,F,F,...]`, scrambling padded positions does not change outputs at valid positions |
| `test_rope_backward_produces_finite_gradients` | `loss.backward()` completes; all trainable parameters have finite gradients |

### Benchmark (no CI gate)

`pytest-benchmark` forward-pass timing for learnable vs rope at `(batch=8, seq_len=128)`. Expected: rope comparable or faster (SDPA-based). Reported in SUMMARY, not a blocker.

### Local integration smoke (manual, pre-commit)

One epoch of rope training on RTX 5070 Ti with a tiny slice (~500 episodes) of the real HF dataset. Confirms: loss decreases, no NaN, eval path succeeds, `metrics.json` written. Runs in <5 min.

### E2E — HF Jobs L40S A/B

Two full 30-epoch runs on real production-scale data. This is the artifact that ships in the SUMMARY and authorises the commit. See A/B protocol below.

### Pre-existing primitive tests

`rope.py` and `rotary_attention.py` have 19 unit tests from EV1 PR #151 covering rotation math, HuggingFace reference parity, causal masking, and backward. Not duplicated here — this cycle tests only the ScoutGPT wiring.

## A/B protocol

**Run configuration (both variants identical except `--variant`):**

- HF Jobs L40S, same flavor as production training
- Dataset `luxury-lakehouse/scoutgpt-training-data` SHA **pinned** at A/B start via `HfApi.repo_info().sha`; both runs use that SHA
- `epochs=30`, `batch_size=256`, `lr=1e-4`, `patience=5`, `seed=42`
- Per-variant `metrics.json` + best-state checkpoint uploaded to sibling repo

**Metrics recorded (all produced by the existing `evaluate_and_report`):**

| Signal | Source | Role |
|---|---|---|
| `test_top1_accuracy`, `test_top5_accuracy` | `eval_loop` | Primary fitness; matches `wf-scoutgpt.yaml` monitoring threshold 0.20 |
| `test_top1_accuracy_q1..q4` | `accuracy_by_bucket` | Does RoPE help specifically on longer episodes (q3/q4)? |
| `counterfactual_spearman_rho` | `evaluate_counterfactual_ranking` | **Downstream embedding quality** — the EV1 gap |
| `cross_source_gap` | `_cross_source_accuracy` | Sanity: gain real across StatsBomb + Wyscout, or single-source artifact? |
| `baseline_most_frequent_accuracy`, `baseline_bigram_accuracy` | `compute_baselines` | Trivial floor — variant-invariant; presence is a sanity check |
| epoch trajectory `history["val_top1_accuracy"]` | `train_loop` | Do we see the anomalous epoch-7 jump that flagged broken-rope on EV1? |

**Decision criteria (recorded, not pre-registered):** no up-front promotion rule. The SUMMARY lands as empirical evidence; the human decides the promotion in a separate approval-gated cycle. The SUMMARY recommendation section leans on: (a) `counterfactual_spearman_rho` improvement (downstream win), and (b) `test_top1_accuracy` improvement without `cross_source_gap` regression (fitness win without source overfit).

**Budget:** 2 × L40S × ~2h × $1.50/h ≈ **$6.00 total**.

## New-path caveat

Football2Vec's rope exercise was encoder-only — no causal mask. ScoutGPT is the first production consumer of `RotaryMultiheadAttention`'s `is_causal=True` code path. `test_rope_causal_property_preserved` is the gate that validates this path before committing.

## Deliverables (single atomic commit)

### Code

- `src/analytics/scoutgpt_decoder.py` — `ScoutGPTConfig.position_embedding` field + three conditional branches
- `scripts/train_scoutgpt_hf.py` — `--variant` + `--output-repo-suffix` CLI
- `scripts/run_rope_scoutgpt_ab.py` — new — A/B orchestration shim

### Tests

- `src/tests/test_scoutgpt_rope.py` — new — eight unit tests

### Workflow card

- `workflow-cards/wf-scoutgpt.yaml` — sibling-repo entries added to `outputs.models`

### Docs

- `docs/superpowers/specs/2026-04-19-rope-scoutgpt-decoder-design.md` — this file
- `docs/superpowers/plans/2026-04-19-rope-scoutgpt-decoder.md` — implementation plan (written by `writing-plans` after spec approval)
- `docs/evolve/rope-scoutgpt/SUMMARY.md` — A/B results, written only after HF Jobs finish

### Memory (post-merge, separate approval)

- `memory/project_rope_scoutgpt_cycle.md` — session record with final numbers, branch, PR, cost
- `memory/MEMORY.md` — one-line index entry under Cycle Completion; also collapses or removes 2–3 stale entries to return below the 200-line soft cap (currently 255)

## Execution order

Operator-side, gated by explicit approvals at `[APPROVAL]` markers:

1. Write code + unit tests; run `ruff` + `pyright` + `pytest src/tests/test_scoutgpt_rope.py`
2. Run local integration smoke (1 epoch, trimmed real data, RTX 5070 Ti)
3. `[APPROVAL #1]` Fire HF Jobs A/B (~$6) — ask with the planned config summary
4. Wait ~2–4h; download metrics; write `SUMMARY.md`; update workflow card
5. Final sweep: `ruff` + `pyright` + `pytest src/tests/` (full suite)
6. `[APPROVAL #2]` Single commit — ask with diff summary
7. `[APPROVAL #3]` Push + open PR — ask; PR body links SUMMARY
8. Memory update after PR merges (separate approval)

## Risks

- **Backward-compat regression:** existing production checkpoint must load under `position_embedding="learnable"`. Guard = `test_state_dict_keys_stable_for_learnable`.
- **Causal path debut:** `RotaryMultiheadAttention(is_causal=True)` was not exercised by EV1. Guard = `test_rope_causal_property_preserved`.
- **A/B noise floor:** historical HF Jobs reproducibility on Football2Vec was ±0.15 pp; RoPE delta must be ≥1 pp to matter. If observed delta ≤ noise floor, flag in SUMMARY and propose a re-run pair (+$6) rather than declaring a tie.
- **MEMORY.md bloat:** index is 255 lines, over the 200 soft cap. Post-merge memory update must also collapse or remove 2–3 stale entries.

## Success criteria

1. `src/tests/test_scoutgpt_rope.py` passes all eight tests
2. Full `pytest src/tests/` suite passes (no cross-file regressions)
3. Local integration smoke converges on a tiny real-data slice
4. Both HF Jobs A/B variants reach epoch 30 or early-stop cleanly
5. `docs/evolve/rope-scoutgpt/SUMMARY.md` contains both variants' metrics with a clear recommendation (promote / defer / re-run-with-wider-search)
6. `position_embedding="learnable"` default path byte-compatible with pre-change production checkpoint
