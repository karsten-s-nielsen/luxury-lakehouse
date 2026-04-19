# EV1 — Football2Vec v2 L1 Sweep — Run Summary

**Run timestamp:** 2026-04-18T23:26:28Z → 2026-04-19T03:10Z (stopped manually)
**Branch:** `evolve/football2vec-l1-sweep`
**Spec:** `docs/superpowers/specs/2026-04-18-ev1-football2vec-l1-sweep-design.md`
**Plan:** `docs/superpowers/plans/2026-04-18-ev1-football2vec-l1-sweep.md`

## Headline numbers

| Metric | Baseline (defaults, 5 epochs) | Best seed (wider, 5 epochs) | Best evolved (iter 11, 5 epochs) |
|--------|-------------------------------|-----------------------------|---------------------------------|
| val_accuracy | 0.5650 | 0.5688 | **0.5693** |
| val_loss | 1.0920 | 1.0873 | 1.0889 |
| param_count | 1,295,640 | 1,949,655 | **1,295,255** |
| training_time | 504 s | 514 s | 687 s |

The full-data, 15-epoch Football2Vec v2 baseline (the production retrain target) reaches
**val_accuracy ≈ 0.569** — within ±0.3 pp of every configuration explored.

## Best config (from `best_program.py`)

LLM-mutated from the `wider.py` seed at iteration 11. The LLM's reasoning (captured in
the prompt/response artifact) was to trade width for depth, add regularisation for the
deeper stack, try FiLM spatial injection and sinusoidal position embedding, and raise
the learning rate to compensate for depth in a 5-epoch budget.

```python
config = {
    "hidden_dim": 128,
    "num_layers": 6,
    "num_heads": 8,
    "dropout": 0.2,
    "mask_prob": 0.15,
    "spatial_mlp_dim": 64,
    "pooling_type": "mean",
    "spatial_injection": "film",
    "position_embedding": "sinusoidal",
    "learning_rate": 5e-4,
    "batch_size": 256,
}
```

## Run economics

| | Value |
|---|---|
| Iterations budgeted | 50 |
| Iterations completed | 29 (stopped manually; early-stop `patience=40` had not yet triggered) |
| Wall-clock | 3h 36m |
| Pace (2-backend local pool) | 8-10 iter/hr |
| Local GPU compute cost | $0 |
| DGX Spark compute cost | $0 |
| OpenRouter LLM cost (estimate) | ~$1.50 (per `project_evolve_llm_costs.md`: ~$2/day continuous) |
| HF Jobs cost | $0 (no HF backend used) |

## Why stopped at 29/50

- Best (iter 11, val_acc 0.5693) was followed by **18 consecutive non-improvements**
  (iters 12-29). Remaining 21 iterations at the same pace would have required ~2 more
  hours of GPU time with Bayesian-updated probability of improvement trending very low.
- The landscape is definitively flat in this search space: 7 seeds + 29 LLM mutations
  all fell in the 0.564-0.569 val_accuracy band (6 pp total range, 0.5 pp 1-sigma).
- Checkpoint 25 already preserved the best artifact; promoting it to the run-root
  `best_program.py` + `metrics.json` is a mechanical step the normal "Evolution complete"
  path would have done on its own.
- Decision made with full data visibility; user-approved the early stop.

## What this tells us about the Football2Vec v2 stage-1 search space

1. **The defaults are near-optimal on accuracy.** No configuration — scalar or
   architectural-enum — meaningfully improved val_accuracy above 0.569 at 5 epochs.
   The LLM explored 29 genuinely different points (wider, deeper, concat, FiLM,
   sinusoidal, rope, attention pooling, cls pooling) and none broke through.

2. **There is a small parsimony win.** The iter-11 winner achieves the same accuracy
   (within 0.5 pp noise) at 1.30 M parameters vs the wider seed's 1.95 M (-34%). If the
   +0.05 pp holds at the full 15-epoch retrain, this is a drop-in default improvement.

3. **The evolve engine plumbing works.** 28 successful LLM mutations ran across the
   2-backend local pool (RTX 5070 Ti + DGX Spark GB10) with 0 $ compute cost and no
   training failures after the Windows encoding fix was applied. Zero regressions from
   the per-target dispatcher refactor (55 existing ScoutGPT tests stayed green).

4. **Architectural-enum variants didn't dominate.** `film` spatial injection and
   `sinusoidal` position embedding appear in the winner, but so did `additive` +
   `learnable` in several near-ties. No enum corner emerged as a clear winner across
   the 29-candidate search.

## Recommendation

**No immediate promotion.** The +0.05 pp accuracy gain is within statistical noise at
5-epoch fidelity. Before promoting the iter-11 config to `Football2VecConfig` defaults,
run a full 15-epoch retrain via `scripts/train_football2vec_v2.py --stage 1` with
these settings and verify the gain (or non-gain) at production fidelity. That is ~13 min
on HF Jobs L40S / $0.32 cost — a cheap validation step.

Directional read of this POC:
- If the +0.05 pp holds at 15 epochs **and** the parameter reduction is desired, promote.
- If not, defaults stand as-is.
- Either way, further hyperparameter sweeps of stage-1 are unlikely to yield returns.
  Future effort should pivot to **EV2** (stage-2 adversarial L2 code evolution — the
  GRL lambda schedule + adversary head has real architectural variance), bigger data,
  or a different architecture class (e.g. state-space models — see session notes).

## Pre-existing bugs fixed during the cycle

Two latent bugs in the evolve engine were surfaced and fixed as part of this cycle
(bundled into the EV1 commit):

1. **Missing `datasets>=3.0` dependency.** The training-data loader (`load_training_data`
   in `src/ingestion/football2vec_v2_training.py`) requires the HuggingFace `datasets`
   library (PR #124 introduction). The HF Jobs PEP 723 header listed it but the wheel's
   `training` extra did not. Now added to `pyproject.toml:training`.

2. **Windows cp1252 encoding bug (latent since PR #88).** `Path.read_text()` and
   `.write_text()` without explicit encoding default to cp1252 on Windows, which cannot
   decode UTF-8 LLM output (em-dashes, curly quotes). Fixed 5 read + 1 write site
   across `src/evolve/evaluator.py`, `src/evolve/targets/scoutgpt/evaluator.py`,
   `src/evolve/backends/hf_jobs.py`, `src/evolve/runner.py`. Would have crashed any
   Windows evolve run producing non-ASCII LLM output; the existing ScoutGPT runs
   happened to dodge it.

## Follow-ups filed

- **Full-fidelity validation of the iter-11 winner** — run `train_football2vec_v2.py
  --stage 1` at 15 epochs with the iter-11 config and compare to current defaults.
  One-shot HF Job, ~13 min, $0.32.
- **EV2 (stage-2 L2 code evolution)** — separately scoped as Wicked in TODO.md; this
  POC narrows its relative value: EV2 is where real architectural headroom lives.
- **Document the OpenEvolve Windows cp1252 stdout quirk** — `PYTHONIOENCODING=utf-8`
  in `runner.py` via `os.environ.setdefault(...)` is too late; must be set before
  Python starts. Cosmetic (logging emoji fails, evolution proceeds) but worth a
  `docs/superpowers/adrs/` or inline README note for future contributors.
- **State-space model (SSM) architecture class** — filed as a ROADMAP.md future
  direction; not motivated by EV1 results alone.
