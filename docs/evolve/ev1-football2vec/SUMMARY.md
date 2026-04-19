# EV1 — Football2Vec v2 L1 Sweep — Run Summary

**Run timestamp:** 2026-04-18T23:26:28Z -> 2026-04-19T03:10Z (sweep, stopped manually)
**Top-10 15-epoch validation:** 2026-04-19T04:23Z -> 08:30Z (local, overnight)
**Rope-only re-validation (proper RoPE):** 2026-04-19T09:25Z -> 10:09Z (local, 44 min)
**Branch:** `evolve/football2vec-l1-sweep`
**Spec:** `docs/superpowers/specs/2026-04-18-ev1-football2vec-l1-sweep-design.md`
**Plan:** `docs/superpowers/plans/2026-04-18-ev1-football2vec-l1-sweep.md`

## Headline numbers (15-epoch fidelity)

Two distinct ceilings emerged in this search:

| Group | Representative | val_accuracy (15 ep) | Δ vs PR #124 baseline 0.569 |
|-------|---------------|---------------------:|----------------------------:|
| Non-rope (learnable / sinusoidal pos_emb) | **iter 15** (learnable, CLS pool) | **0.5865** | +1.75 pp |
| Rope (`position_embedding="rope"`) | **iter 1** (proper RoPE, attention pool) | **0.7969** | **+22.79 pp** |

The ~18-20 pp rope vs non-rope gap is a genuine empirical finding, verified
below. Which ceiling is preferred as the new production default is a downstream
decision — see Recommendation.

## True non-rope winner — iter 15 config

LLM-mutated at iteration 15 (generation 4). Rank 1 at 15-epoch fidelity across
the top-10 non-rope candidates; the 5-epoch signal ranked this candidate at
position 8 of 10 (example of 5-epoch non-monotonicity).

```python
config = {
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
}
```

Epoch trajectory (smooth, monotonic):

| Epoch | val_accuracy |
|-------|--------------|
| 1-6   | 0.561 -> 0.566 |
| 7     | 0.571 |
| 8-13  | 0.575 -> 0.587 |
| 14-15 | 0.585 -> 0.586 |

## Rope variants — proper RoPE implementation + the fix narrative

### What was broken in the original sweep

The rope branch in `Football2VecEncoder.__init__` / `_embed` (lines 157-170 and
257-264 of `src/analytics/football2vec_transformer.py` before this cycle) was
labelled "rope" but did not implement rotary position embedding. It tiled a
`sin` pattern across `hidden_dim` via `repeat_interleave` + `repeat` and added
it to the input token embeddings — closer to a degenerate sinusoidal embedding
with duplicated channels than to RoPE.

Behavioural signature: iter-1 and iter-16 with the broken rope reached
`val_acc=0.72-0.75` at 15 epochs, but the trajectory had an anomalous jump:
epochs 1-6 normal (~0.55-0.57), epoch 7 suddenly 0.72, then plateau at
0.72-0.75. That "wait, then jump" pattern suggested the model was learning
to factor out the tiled-sine noise before the underlying rope ceiling became
accessible — rather than learning rope the way it is supposed to be learnt.

### Proper RoPE implementation (this cycle)

Shipped as a reusable primitive because RoPE is not Football2Vec-specific
(ScoutGPTDecoder and Football2Vec360Encoder are obvious reusers):

- `src/analytics/rope.py` — `RotaryEmbedding` (precomputed cos/sin tables,
  `persistent=False`), `rotate_half`, `apply_rotary_pos_emb`. LLaMA-style
  first-half / second-half pairing convention.
- `src/analytics/rotary_attention.py` — `RotaryMultiheadAttention` (Q and K
  projected, rotated, then `F.scaled_dot_product_attention`); `RotaryTransformerEncoderLayer`
  (drop-in replacement for `nn.TransformerEncoderLayer`, both pre-norm and
  post-norm modes); `RotaryTransformerEncoder` (stack that owns the RoPE
  tables and shares `(cos, sin)` across layers per forward pass).
- `src/analytics/football2vec_transformer.py` — when `position_embedding=="rope"`
  the encoder is now `RotaryTransformerEncoder` (not `nn.TransformerEncoder`),
  and `_embed` skips the additive position signal (rope is applied inside
  attention, not on the input).

10 unit tests for `rope.py` covering:

- Rotation math parity against an explicit per-pair rotation matrix (`atol=1e-6`).
- Per-pair norm preservation.
- The defining RoPE property: `<q_m · k_n>` depends only on `(m - n)`, not on
  absolute `m` or `n`.
- Bit-identical match to a verbatim transcription of HuggingFace LLaMA's
  `apply_rotary_pos_emb` — a permanent regression guard against anyone
  "simplifying" the implementation and silently breaking it.
- Autograd passthrough (finite gradients, correct shapes).
- Shape / bounds validation.

9 unit tests for `rotary_attention.py` covering attention shape, key-padding
mask propagation, causal masking, encoder-layer shape (both norm schedules),
multi-layer stack determinism in eval mode, and backward pass.

Regression test on `Football2VecEncoder` confirms the rope variant now wires
up `RotaryTransformerEncoder` (not the broken tiled-sine buffers).

### Verification against published reference

Before trusting the rope numbers this cycle produces, the primitive was
byte-compared against HuggingFace Transformers' `LlamaRotaryEmbedding` +
`apply_rotary_pos_emb` (verbatim transcription). Result:

| Comparison | Max abs diff |
|---|---:|
| cos table | 0.000e+00 |
| sin table | 0.000e+00 |
| Q rotation | 0.000e+00 |
| K rotation | 0.000e+00 |
| End-to-end self-attention output (same weights, same input) | 0.000e+00 |

My RoPE implementation is byte-equivalent to the published LLaMA reference.
The +20 pp rope-vs-learnable delta observed on this task is **not** a bug in
the primitive.

### The +20 pp delta is real — what it is

Held-out discriminating experiments (2 training epochs each, RTX 5070 Ti,
same dataset split, same seed):

| Experiment | val_acc ep1 | val_acc ep2 | val_acc ep15 |
|------------|------------:|------------:|------------:|
| iter-1 architecture + `learnable` pos_emb | 0.5642 | 0.5658 | ~0.58 (plateau) |
| iter-1 architecture + proper RoPE (full run) | 0.7523 | 0.7637 | **0.7969** |
| iter-16 architecture (rope, mean pool) (full run) | 0.5592 | 0.7495 | **0.7775** |
| iter-1 architecture + proper RoPE, x/y zeroed at masked positions | 0.7577 | — | — |
| Untrained (random weights) + learnable pos_emb | 0.0854 | — | — |
| Untrained (random weights) + proper RoPE | 0.0009 | — | — |

Architecture alone does not produce the 0.75 — same architecture with
learnable pos_emb plateaus at the normal non-rope ceiling (0.57). Zeroing
spatial coordinates at the masked positions does not reduce the rope number
(rules out "model memorises `x,y -> action` at the masked position" as the
mechanism). Untrained rope is near chance (0.0009 is the model's
consistently-wrong-in-the-same-way random init — no structural leak).

### What remains open

The mechanism by which rope reaches ~0.75 while learnable plateaus at ~0.57
on this MLM task is not fully characterised at the time of writing. The
primitive itself is verified correct; the gap is an empirical property of
rope + this data + this training setup.

Coherent working hypothesis (consistent with all observations so far):

- The MLM task has a latent "spatial-contextual" shortcut. The masked action
  can be predicted from the `(x, y)` coordinates of unmasked neighbouring
  positions with high accuracy, because action types correlate strongly with
  spatial context in soccer.
- Learnable pos_emb is an additive signal on the input that the model must
  simultaneously learn AND factor out to expose the shortcut. In practice
  the pos_emb keeps adapting during training, never stabilising — the
  shortcut stays masked. Ceiling: 0.57.
- Sinusoidal pos_emb is fixed additive. In principle factorable, but harder
  in practice (varied per-dim frequencies). Non-rope variants with
  sinusoidal land at ~0.58 (iter-11), not 0.75 — suggesting the additive
  signal remains hard to isolate.
- Broken tiled-sine "rope" was additive + highly factorable (duplicated
  channels). Model learned to factor it out around epoch 7, after which the
  shortcut was accessible (jump from 0.57 to 0.72).
- Proper RoPE adds zero signal to the input (rotation happens inside Q/K
  attention only). The shortcut is exposed from step 1. Ceiling: 0.75.

This hypothesis predicts that rope's 0.75 is a rope-specific ceiling
reachable on this training-data setup, not a representation-quality signal.
Validating it conclusively requires a downstream study (e.g., does the rope
encoder produce embeddings as useful for player-style clustering as the
learnable one? does masking the spatial coords at all unmasked positions
too also reduce the gap?). Filed as a follow-up.

## 15-epoch leaderboard (sorted by val_acc_15ep)

Source: `scripts/validate_ev1_top_n.py` run on 2026-04-19 for the 8 non-rope
candidates in the top-10. iter-1 and iter-16 (rope) were stopped at 15 epochs
during that initial run because their numbers were known to be artifacts
from the broken rope branch; they are being re-validated against the proper
RoPE primitive via `scripts/validate_ev1_rope_only.py`.

| rank | iter | pos_emb | val_acc_5ep | val_acc_15ep | Δ baseline 0.569 | note |
|-----:|-----:|---------|------------:|-------------:|-----------------:|------|
| 1    | 1    | rope    | 0.5674      | **0.7969**   | **+22.79 pp**    | proper RoPE, attention pool — rope ceiling |
| 2    | 16   | rope    | 0.5671      | **0.7775**   | **+20.85 pp**    | proper RoPE, mean pool — rope ceiling |
| 3    | 15   | learnable | 0.5677    | 0.5865       | +1.75 pp         | true non-rope winner — wide-shallow + CLS |
| 4    | 19   | learnable | 0.5679    | 0.5841       | +1.51 pp         | |
| 4    | 2    | learnable | 0.5679    | 0.5841       | +1.51 pp         | same config as iter 19 |
| 6    | 11   | sinusoidal | 0.5693   | 0.5824       | +1.34 pp         | narrow-deep + FiLM |
| 6    | 14   | sinusoidal | 0.5693   | 0.5824       | +1.34 pp         | same config as iter 11 |
| 8    | 8    | learnable | 0.5682    | 0.5724       | +0.34 pp         | |
| 9    | 6    | learnable | 0.5682    | 0.5700       | +0.10 pp         | |
| 10   | 0    | learnable | 0.5688    | 0.5698       | +0.10 pp         | wider seed |

**5-epoch / 15-epoch non-monotonicity finding**: iter-15 was rank 8 of 10 at
5-epoch fidelity but rank 1 (among non-rope candidates) at 15-epoch. iter-11
was the 5-epoch leader but dropped to rank 4 at 15 epochs. A short
evolve-horizon with a cheap fitness proxy can mis-rank the top-of-pile
candidates — consider running the sweep at higher fidelity next time if
budget allows.

## Run economics

| | Value |
|---|---|
| Iterations budgeted | 50 |
| Iterations completed | 29 (stopped manually; early-stop `patience=40` not triggered) |
| Sweep wall-clock | 3h 36m |
| Pace (2-backend local pool) | 8-10 iter/hr |
| Local GPU compute cost | $0 |
| DGX Spark compute cost | $0 |
| OpenRouter LLM cost (estimate) | ~$1.50 |
| HF Jobs cost | $0 (no HF backend used) |
| Top-10 15-epoch validation wall-clock | ~4h (overnight local, 8 non-rope candidates) |
| Rope re-validation wall-clock | ~1h (iter-1 + iter-16 at 15 epochs) |

## Recommendation

**Before promoting anything, run a downstream study on the rope variant.**

The non-rope winner (iter-15, 0.5865) is a safe +1.75 pp over baseline. It
should reproduce on the production training path at small cost — one HF
Jobs L40S run, ~13 min, $0.32 — and if it reproduces it is a drop-in
improvement to the defaults.

The rope variant (iter-1, ~0.75) is numerically dramatic but the nature of
its gain is not yet characterised. Before swapping Football2Vec v2 to use
`position_embedding="rope"` by default, a downstream probe is needed: are
the embeddings produced by the rope model useful for the things Football2Vec
is used for (player-similarity clustering, style embeddings, VAEP-adjacent
use)? If the rope model gets 0.75 MLM accuracy by exploiting a spatial
shortcut, the learned embeddings may NOT be richer than the 0.58 learnable
ones — just better at the narrow MLM task. The primitive is correct and
ready to use; whether it is the right default is a separate question.

The proper RoPE primitive itself is a durable deliverable regardless:
ScoutGPTDecoder (longer sequences, richer relative-position structure) is
likely to benefit from rope without the MLM-specific concerns seen here,
and that is the obvious next target.

## Follow-ups filed

- **Downstream probe of rope embedding quality** — before promoting the
  rope config: does the iter-1 rope model's 144-dim embedding cluster
  players by style as well as the iter-15 learnable model's? Same infra
  as the existing Football2Vec similarity demo.
- **Full-fidelity production validation of iter-15** — `train_football2vec_v2.py --stage 1`
  at 15 epochs with the iter-15 config on HF Jobs L40S; confirm the +1.75 pp
  gain reproduces.
- **Investigate the mechanism of the rope vs learnable gap** — is it the
  spatial-context shortcut described above, or something else? Possible
  tests: zero x/y at all positions (not just masked); replace action IDs at
  unmasked positions with MASK (force model to rely only on position); port
  the trained rope model's attention weights to a visualisation and inspect
  which positions it attends to. Not urgent for EV1 but useful context if
  rope becomes the default.
- **RoPE for `ScoutGPTDecoder`** — highest-value RoPE reuse target. Long
  sequences, rich relative-position structure. Separate mini-cycle.
- **RoPE for `Football2Vec360Encoder`** — straightforward reuse; composes
  `Football2VecEncoder` directly. Separate cycle.
- **EV2 (stage-2 L2 code evolution)** — separately scoped as Wicked in
  TODO.md; this POC narrows its relative value.
- **Document the OpenEvolve Windows cp1252 stdout quirk** — `PYTHONIOENCODING=utf-8`
  in `runner.py` via `os.environ.setdefault(...)` is too late; must be set
  before Python starts. Cosmetic (logging emoji fails, evolution proceeds)
  but worth a `docs/superpowers/adrs/` or inline README note for future
  contributors.
- **State-space model (SSM) architecture class** — filed as a ROADMAP.md
  future direction; not motivated by EV1 results alone.

## Pre-existing bugs fixed during the cycle

Two latent bugs in the evolve engine were surfaced and fixed as part of this
cycle (bundled into the EV1 commit):

1. **Missing `datasets>=3.0` dependency.** The training-data loader
   (`load_training_data` in `src/ingestion/football2vec_v2_training.py`)
   requires the HuggingFace `datasets` library (PR #124 introduction). The
   HF Jobs PEP 723 header listed it but the wheel's `training` extra did not.
   Now added to `pyproject.toml:training`.
2. **Windows cp1252 encoding bug (latent since PR #88).** `Path.read_text()`
   and `.write_text()` without explicit encoding default to cp1252 on
   Windows, which cannot decode UTF-8 LLM output (em-dashes, curly quotes).
   Fixed 5 read + 1 write site across `src/evolve/evaluator.py`,
   `src/evolve/targets/scoutgpt/evaluator.py`, `src/evolve/backends/hf_jobs.py`,
   `src/evolve/runner.py`. Would have crashed any Windows evolve run
   producing non-ASCII LLM output; the existing ScoutGPT runs happened to
   dodge it.
