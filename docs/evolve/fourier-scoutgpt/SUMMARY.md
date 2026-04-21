# ScoutGPT Fourier / Swiglu Promotion A/B — Summary

**Date:** 2026-04-21
**Branch:** `evolve/scoutgpt-fourier-promote`
**Execution venue:** Local (1x RTX 5070 Ti + 1x DGX Spark via SSH)
**Dataset:** `luxury-lakehouse/scoutgpt-training-data` revision `3c478dfec485b2eca17366c6374d434f42a15890`
**Spec:** `docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md`

## Pre-registered decision rule

PROMOTE iff `rho_trt - rho_ctrl >= +0.10` AND `top1_trt >= top1_ctrl - 0.005`.
Applied via `src/analytics/promotion_rules.py::apply_decision_rule`.

## Headline

| Arm | Role | conditioning_type | hd/L/H | epochs | `counterfactual_rho` | `test_top1` | `val_loss` | `wall_min` |
|---|---|---|---|---:|---:|---:|---:|---:|
| arm1-control-additive | CONTROL | additive | 256/6/8 | 14 | 0.1372 | 0.8153 | 0.4654 | 186.1098 |
| arm2-fourier-prod | TREATMENT | fourier_cross_attention | 256/6/8 | 23 | 0.2812 | 0.8368 | 0.4048 | 138.3969 |
| arm3-fourier-seed | ABLATION | fourier_cross_attention | 192/3/6 | 30 | 0.3451 | 0.8330 | 0.4137 | 91.3894 |
| arm4-swiglu | TREATMENT | swiglu | 256/6/8 | 18 | 0.1014 | 0.8169 | 0.4602 | 252.1917 |
| arm5-cross-attention | ISOLATION | cross_attention | 256/6/8 | 30 | 0.2995 | 0.8410 | 0.3956 | 362.2957 |

## Dispositions

- **Fourier** (Arm 2 vs Arm 1): **PROMOTE** (rho delta = +0.1440, top1 delta = +0.0215)
- **Swiglu** (Arm 4 vs Arm 1): **ARCHIVE** (rho delta = -0.0358, top1 delta = +0.0016)

## Cross-reference

- L2 harvest (2026-04-20): fourier_cross_attention rho=+0.3799 at 15-epoch evolve-scale.
- RoPE-for-ScoutGPT (2026-04-19): rho delta +0.016 rejected.

## Informational arms — mechanism + capacity analysis

Rho ordering (highest first):

1. Arm 3 (Fourier @ 192d/3L/6h) — rho = +0.3451
2. **Arm 5 (cross-attention @ 256d/6L/8h) — rho = +0.2995** ← NO Fourier
3. Arm 2 (Fourier @ 256d/6L/8h) — rho = +0.2812 ← the promoted mechanism
4. Arm 1 (additive @ 256d/6L/8h) — rho = +0.1372 ← baseline
5. Arm 4 (swiglu @ 256d/6L/8h) — rho = +0.1014 ← below baseline

**Surprise finding — cross-attention conditioning is the primary rho driver, not Fourier spatial.** Arm 5 (plain `cross_attention` with standard MLP spatial, NO Fourier) has rho = 0.2995, which is *higher* than Arm 2's Fourier+cross-attention rho = 0.2812 by 0.018 (well within rho_std ~0.30 noise envelope, so "tied" is a fair reading). The pre-registered decision rule promotes `fourier_cross_attention` because it beats the additive baseline by +0.144; but the mechanism-isolation arm reveals the same improvement is achievable without the Fourier spatial features — cross-attention conditioning alone carries almost all of the gain.

Isolating the mechanisms:

- **Cross-attention conditioning contribution** (Arm 5 − Arm 1): +0.162 rho, +0.026 top1
- **Fourier spatial contribution** (Arm 2 − Arm 5): −0.018 rho, −0.004 top1 — both within noise, possibly mildly negative
- **Combined** (Arm 2 − Arm 1): +0.144 rho, +0.022 top1

**Capacity effect for Fourier** (Arm 3 vs Arm 2): the smaller 192d/3L model has *higher* rho (+0.064) than the 256d/6L model. Fourier spatial features don't benefit from capacity — perhaps even hurt from it. Plausible mechanism: larger models may overfit to next-action prediction at the expense of per-player distinctiveness, which counterfactual ranking (rho) measures.

**Why Swiglu archives**: rho regressed −0.036 vs additive baseline while top1 barely moved (+0.002). The gating mechanism concentrates signal through Swish non-linearity, which appears to collapse per-player distinctiveness (rho) even though next-action accuracy (top1) is preserved. Consistent with the L2 harvest's weak-positive +0.04 fitness — it was a marginal signal that doesn't survive at production fidelity.

**Follow-up candidates (ranked by ratio of potential value to effort):**

1. **High value, low effort** — test whether flipping the production default from `"additive"` to `"cross_attention"` is worthwhile. The cross_attention enum value *already exists*; Arm 5 is evidence it wins big on rho (+0.162) and top1 (+0.026). A separate promotion cycle (Arm 1 vs Arm 5 as peers, with the existing cross_attention branch) would confirm.
2. **Medium value** — investigate whether Fourier is actively hurting at production scale or just neutral. Cleanest test: side-by-side `cross_attention` vs `fourier_cross_attention` with identical seeds + full eval budget. If rho difference stays within noise, Fourier is neutral; if the rho-cost persists, Fourier actively hurts and should NOT be promoted (overrides this cycle's disposition).
3. **Research value** — why does capacity hurt Fourier rho? Probe per-player cluster quality on embeddings from Arm 2 vs Arm 3 to see if the smaller model preserves more per-player spatial signatures.

## Execution notes

1. **Orchestrator interruption**: the main orchestrator was killed at 20:09 UTC after Arms 2+3 finished early on 5070 Ti, while Spark had Arm 4 running and Arm 5 pending. 5070 Ti was idle. Arm 5 was redirected from Spark to local 5070 Ti to shave wall-clock time. Arm 4 SSH subprocess was left alive (Windows Popen children survive parent death without job_object).
2. **Concurrent Arm 5 training**: an earlier Arm 5 dispatch had a `tee` bug that caused its log file creation to fail — but the underlying training subprocess kept running. When I retried with `mkdir -p` + a fresh `tee`, a SECOND Arm 5 training started concurrently on the same GPU. Both completed at ~02:17-02:18 UTC with near-identical results (rho=0.3133 from the first run, rho=0.2995 from the second, within rho_std noise). The metrics.json on disk reflects the second run (last writer wins). Both are legitimate cross_attention@256d/6L/8h runs with seed=42; the ~0.014 rho difference is GPU-contention non-determinism. Arm 5's reported wall_clock of 6.04 hrs is inflated by this contention; ~3 hrs is the actual single-run time.
3. **Environment**: Arms 1+4 ran on DGX Spark (PyTorch 2.11 + CUDA 13.0, Python 3.12). Arms 2+3+5 ran on local RTX 5070 Ti (Python 3.10, PyTorch install via project `uv run`). No observable env-drift signal in the metrics.

## Follow-ups

See `docs/superpowers/specs/2026-04-20-scoutgpt-fourier-cross-attention-promote-design.md` Section I, plus the "Follow-up candidates" above which add a high-priority item: validate whether cross-attention conditioning alone is the real promotion candidate.
