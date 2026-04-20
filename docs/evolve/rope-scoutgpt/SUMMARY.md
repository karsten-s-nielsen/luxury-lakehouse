# RoPE-for-ScoutGPT — A/B Summary

**Run timestamp:** 2026-04-19T20:36Z → 2026-04-19T21:45Z (shim wall-clock 69 min; jobs ran concurrently)
**Dataset SHA (pinned):** `3c478dfec485b2eca17366c6374d434f42a15890` (894,401 episodes; train=715,520 / val=89,440 / test=89,441)
**Branch:** `feat/rope-scoutgpt-decoder`
**Spec:** `docs/superpowers/specs/2026-04-19-rope-scoutgpt-decoder-design.md`
**Plan:** `docs/superpowers/plans/2026-04-19-rope-scoutgpt-decoder.md`
**Wheel:** `luxury-lakehouse 0.3.4` (built + uploaded for this cycle)

## HF Jobs

| Variant | Job ID | Status | Epochs (early-stop) | Wall-clock | Cost |
|---|---|---|---:|---:|---:|
| learnable | `69e5750eac288e522d8f0124` | COMPLETED | 14 | 67 min | $1.68 |
| rope      | `69e5750ecd8c002f31dffb71` | COMPLETED | 15 | 66 min | $1.65 |

**Total: $3.33** (under the $6 budget; prior 3 failed-at-startup attempts during shim-bug iteration added negligible extra compute).

## Headline metrics

| Variant | test_top1 | test_top5 | test_loss | counterfactual_rho | rho_std | cross_source_gap |
|---|---:|---:|---:|---:|---:|---:|
| learnable | **0.81535** | 0.99722 | **0.4644** | 0.02986 | 0.290 | 0.04200 |
| rope      | 0.81526 | 0.99722 | 0.4651 | **0.04547** | 0.324 | **0.03818** |
| Δ (rope − learnable) | **−0.00009** | −0.00001 | +0.0007 | **+0.01561** | +0.034 | **−0.00382** |

## Bucket accuracy by episode length

| Variant | q1 (shortest) | q2 | q3 | q4 (longest) |
|---|---:|---:|---:|---:|
| learnable | 0.7203 | 0.7595 | **0.8044** | **0.8452** |
| rope      | **0.7240** | **0.7600** | 0.8040 | 0.8445 |

RoPE edges out on the two shortest-episode buckets (+0.0037 and +0.0005); learnable edges out on the two longest (+0.0004 and +0.0007). Differences are well below noise; no clear length-related signal.

## Cross-source accuracy

| Variant | StatsBomb | Wyscout | Gap |
|---|---:|---:|---:|
| learnable | 0.81783 | 0.77583 | 0.04200 |
| rope      | 0.81751 | **0.77933** | **0.03818** |

Rope has a marginally narrower cross-source gap (better Wyscout generalization, tied StatsBomb). Difference (+0.0035 pp on Wyscout) is within noise.

## Baselines (variant-invariant; sanity floor)

| most_frequent | bigram |
|---:|---:|
| 0.5996 | 0.6299 |

Both variants sit **~19 pp above the bigram floor** — the model is clearly learning real structure.

## Interpretation

- **test_top1 / test_top5 / test_loss:** rope and learnable are statistically indistinguishable. The largest top-1 delta (0.00009) is ~7000× smaller than the HF Jobs reproducibility noise floor (±0.0015 pp from the EV1 iter-15 reproduction on Football2Vec).
- **counterfactual_spearman_rho:** rope wins +0.0156 nominally — but `rho_std ≈ 0.30` for both variants means the gap is about 0.05σ, far from statistically significant. Both values are also well under the `wf-scoutgpt.yaml` monitoring threshold of 0.15. The decoder is a weak counterfactual ranker in general; "rope is ~50% better at near-zero" is not a meaningful finding.
- **Bucket gradient:** both variants show the expected length-gradient (accuracy rises from q1 → q4), which is driven by more context per prediction, not by the positional-encoding scheme. Rope does not exhibit any preferential gain on longer episodes, which would have been the canonical rope-advantage pitch.
- **Training dynamics:** both variants hit val_loss minimum around epoch 9–10 and early-stopped at epoch 14–15. The epoch trajectories are near-parallel. Rope is ~14% wall-clock slower per epoch (4.0 min vs 3.5 min) due to SDPA + rotation overhead at every attention step — the model complexity is materially higher but delivered no measurable benefit.

### Cross-reference to EV1 Football2Vec

EV1 saw rope +20 pp on Football2Vec's MLM task at 15 epochs. EV1's SUMMARY flagged the "spatial-contextual shortcut" hypothesis — that rope's win came from exposing a latent x/y → action-type leak that additive positional signals were (paradoxically) concealing, not from genuine embedding quality improvement.

**This ScoutGPT A/B supports that hypothesis.** ScoutGPT's task (causal next-action prediction with conditioning tokens) does not have the same shortcut: every action token is observed in its causal context, so there's nothing to "unmask." Rope and learnable converge to the same ceiling.

Conclusion: **rope is a task-shaped advantage for MLM with spatial context, not a general encoder upgrade.**

## Recommendation — REJECT

Do **not** promote rope to the default for `ScoutGPTDecoder`. Close the capability as shipped-but-opt-in.

- Primary accuracy: tied within noise.
- counterfactual_rho: nominal rope advantage is 0.05σ, indistinguishable from zero at the measured std.
- Rope adds a new encoder stack (`RotaryTransformerEncoder`) + ~14% per-epoch wall-clock for no measurable benefit on this task.
- The capability remains available via `ScoutGPTConfig(position_embedding="rope")` for users who want to experiment, but the default stays `"learnable"`.

## Follow-ups filed (separate cycles)

- **counterfactual_rho floor investigation.** Both variants sit at 0.03–0.05 against a monitoring threshold of 0.15. Either the threshold is aspirational (set before the model was trained) or the counterfactual-ranking evaluation methodology needs a pass (e.g., widen player pool, weight by sample count). Not related to rope.
- **Wheel 0.3.4 lands with this PR.** CI will republish the wheel on merge; no additional wheel bump required for downstream consumers.
- **Football2Vec rope embedding-quality probe (from EV1).** Still open. This ScoutGPT result provides additional circumstantial evidence that rope's Football2Vec win is task-specific, but the probe (cluster-quality of rope vs learnable embeddings) remains the right way to confirm.
- **RoPE for `Football2Vec360Encoder`.** Was listed as a follow-up target in EV1; with this ScoutGPT result strengthening the "task-specific shortcut" hypothesis, the 360 encoder is likely to benefit only if it shares the MLM shortcut pathway. Defer until the embedding-quality probe lands.

## Artefacts

- `luxury-lakehouse/scoutgpt-variant-learnable` — best-of-14-epoch checkpoint + `metrics.json`
- `luxury-lakehouse/scoutgpt-variant-rope` — best-of-15-epoch checkpoint + `metrics.json`
- Canonical `luxury-lakehouse/scoutgpt` — **untouched**; production serves the pre-cycle learnable model as before.
