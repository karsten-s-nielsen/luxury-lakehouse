# ScoutGPT cross_attention Promotion + Fourier Retention — Summary

**Date:** 2026-04-22
**Branch:** `evolve/scoutgpt-cross-attn-promote`
**Execution venue:** Local — 1x RTX 5070 Ti (AI-PC) + 1x RTX 5070 Ti (Media-PC, SSH)
**Dataset:** `luxury-lakehouse/scoutgpt-training-data` revision `3c478dfec485b2eca17366c6374d434f42a15890`
**Spec:** `docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md`

## Pre-registered decision rules

- **Default flip rule** (Arm 1 additive → Arm 5 cross_attention): PROMOTE iff `rho_trt - rho_ctrl >= +0.10` AND `top1_trt >= top1_ctrl - 0.005`. Applied via `src/analytics/promotion_rules.py::apply_decision_rule`.
- **Fourier retention rule** (Arm 2 incumbent vs Arm 5 challenger): DEPRECATE iff `rho_challenger - rho_incumbent >= +0.05` AND `top1_challenger - top1_incumbent >= -0.005`. Applied via `src/analytics/promotion_rules.py::apply_retention_rule`.

## Headline

| Arm | Role | conditioning_type | hd/L/H | `counterfactual_rho` | `test_top1` | `val_loss` | `wall_clock_min` |
|---|---|---|---|---:|---:|---:|---:|
| arm1-additive | CONTROL | additive | 256/6/8 | 0.0534 | 0.8156 | 0.4649 | 88.2139 |
| arm5-cross-attention | DEFAULT-FLIP-CANDIDATE | cross_attention | 256/6/8 | 0.3003 | 0.8419 | 0.3937 | 185.3182 |
| arm2-fourier | RETENTION-CANDIDATE | fourier_cross_attention | 256/6/8 | 0.2963 | 0.8382 | 0.4014 | 179.1510 |

## Dispositions

- **Default flip** (Arm 5 vs Arm 1): **PROMOTE**
  - rho delta = +0.2469
  - top1 delta = +0.0263
- **Fourier retention** (Arm 2 vs Arm 5): **KEEP**
  - rho delta (challenger - incumbent) = +0.0040
  - top1 delta (challenger - incumbent) = +0.0037

## Cross-reference

- **PR #166 (Fourier promotion, 2026-04-21)** — Arm 5 under GPU contention reported rho=0.2995. Clean re-run this cycle: 0.3003. Arm 2 (clean, no contention) reported 0.2812; clean re-run this cycle: 0.2963.
- **RoPE-for-ScoutGPT A/B (PR #159, 2026-04-19)** — rho delta +0.016 rejected as noise floor.

## Mechanism narrative

_(To be filled in during review — include whether GPU contention was the Arm 5 rho driver, and whether Fourier's within-noise gap under PR #166 persisted under clean conditions.)_

## Follow-ups

- Canonical checkpoint retraining on HF Hub under the new default (if flip promoted).
- Football2Vec cross-attention port.
- Spatial-encoding x conditioning-type axis decomposition (future refactor).
- Hard removal of deprecated `fourier_cross_attention` (after 3+ months of no push-back).

See `docs/superpowers/specs/2026-04-21-scoutgpt-cross-attention-promote-design.md` Section I for the full follow-up list.
