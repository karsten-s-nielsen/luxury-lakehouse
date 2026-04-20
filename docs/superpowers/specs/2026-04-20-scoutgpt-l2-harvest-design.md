# ScoutGPT L2 Seed Harvest

**Date:** 2026-04-20
**Branch:** `evolve/scoutgpt-l2-harvest`
**Status:** Design approved, execution complete; SUMMARY written.
**Cycle-following:** RoPE-for-ScoutGPT (PR #159, session 50). Chose this Option 2 ("Harvest unshipped L2 seeds") before starting EV2 (Football2Vec v2 L2 adversarial) — close the ScoutGPT L2 open loop first.

## Problem

`src/evolve/targets/scoutgpt/seed_programs/` contains 9 seed files, but only 4 of the 9 conditioning variants shipped as `ScoutGPTConfig.conditioning_type` choices (`additive`, `cross_attention`, `film`, `gated`). The remaining 5 — `fourier_cross_attention`, `hybrid_gated_attention`, `orthogonal_cross_attention`, `swiglu_conditioning`, and the `cross_attention` L2 seed variant — have been in the repo since commits #88 / #94 / `b1ed41d` (April) but no `docs/evolve/scoutgpt-l2/SUMMARY.md` was ever written documenting their evaluation. They are **orphans**: code that presumably ran through the evolve engine at some point but whose fitness was never captured in the durable record.

The RoPE-for-ScoutGPT A/B (session 50) refreshed context on ScoutGPT's counterfactual-ranking signal (`rho ≈ 0.03–0.05` at production scale) and raised the question of whether any of these orphan seeds were meaningful architectural wins that got dropped, or confirmed non-starters. Without a captured evaluation, we can't answer.

## Non-goals

- **Promoting any variant to production.** Promotion means porting `custom_layers()` + `custom_embed()` into `ScoutGPTDecoder` as a new `conditioning_type` branch, adding unit tests, and shipping the code. That's a separate approval-gated cycle per seed — scope explodes with promotion count.
- **Re-running any variant that was already evaluated in prior evolve sweeps.** We have no captured scores from the original ScoutGPT evolve run, so every variant is evaluated fresh in this cycle.
- **Changing the evolve evaluator or fitness formula.** We use `0.7 × spearman_rho + 0.3 × top1_accuracy` per the existing `src/evolve/targets/scoutgpt/config.yaml`.
- **Evaluating at production fidelity (30 epochs, hidden_dim=256).** The evolve-scale config (15 epochs, hidden_dim=192, num_layers=3) is what the seeds were designed around and matches the fitness framework they were bred under. Production-fidelity re-evaluation is a per-variant promotion concern.

## Scope — Option B (Evaluate + prune losers)

Chosen from three options at brainstorming:

- A. Evaluate only (no code changes) — too timid; leaves the directory cluttered.
- **B. Evaluate + prune losers** ← chosen
- C. Evaluate + prune + promote winners (in same PR) — scope explosion.

Option B pruning rule:

- `IF fitness(v) < fitness(additive) AND fitness(v) < 0.63` → PRUNE (delete seed file)
- `ELSE IF fitness(v) < fitness(additive)` → ARCHIVE (keep file, document)
- `ELSE` → FLAG FOR PROMOTION (follow-up cycle)

The 0.63 floor was originally intended to represent the bigram top-1 accuracy baseline (structural-soundness floor). During execution this was recognised as ambiguous — see "Disposition table" in SUMMARY.md — and the user ratified the spirit interpretation (ARCHIVE both below-baseline variants because their top1 > 0.63 floor structurally).

## Evaluation protocol

- **Shared config** across all variants, mirroring the L2 seeds' declared hyperparameters:
  - `hidden_dim=192, num_layers=3, num_heads=6, dropout=0.15, max_seq_len=128`
  - `spatial_mlp_dim=64, vaep_loss_weight=0.32`
  - `learning_rate=2e-4, weight_decay=0.01, batch_size=384`
- **Fidelity:** 15 epochs, patience=7, seed=42. Rationale for 15-epoch (not the evolve engine's default 5-epoch): EV1 empirically showed 5→15-epoch rank reorderings; prune decisions must not rest on noise-dominated rankings.
- **Variants (5 total):** `additive` baseline (`program_path=None`) + 4 L2 seeds (`fourier_cross_attention`, `hybrid_gated_attention`, `orthogonal_cross_attention`, `swiglu_conditioning`).
- **Execution:** single HF Jobs L40S batch via `scripts/evaluate_scoutgpt_l2_seeds.py`. Sequential per-variant runs share the `_load_or_cache` dataset cache (5 runs × 1 dataset load).
- **Counterfactual eval:** `_EVOLVE_COUNTERFACTUAL_EPISODES=200`, `_EVOLVE_COUNTERFACTUAL_PLAYERS=50` (evolve evaluator defaults; reduced vs production's 1000 × 100 to match the fitness framework).
- **Fitness:** `0.7 × spearman_rho + 0.3 × top1_accuracy` per `src/evolve/targets/scoutgpt/config.yaml`.
- **Results:** per-variant `metrics.json` uploaded incrementally to `luxury-lakehouse/scoutgpt-l2-harvest` HF Hub repo (partial-crash survival); combined `results.json` at the end. Local mirror to `docs/evolve/scoutgpt-l2-harvest/results.json` for commit.

### L2 conditioning_type override

All 4 L2 seed files declare `"conditioning_type": "cross_attention"` in their config dict. The comment in each seed says the evolve evaluator overrides this to `"additive"` when `custom_embed` is present. This cycle's orchestration script passes `conditioning_type="additive"` explicitly for L2 variants so unused cross-attention layers aren't allocated alongside the `_apply_program`-registered custom layers.

## Testing

No new production code changes → no new unit tests required. Pre-flight validation is implicit in the orchestration script: each seed's `custom_layers(hidden_dim)` and `custom_embed` signatures are verified by the evolve evaluator at exec time (AST allowlist per ADR-001, restricted globals with `__builtins__={}`).

## Deliverables (single commit)

- `scripts/evaluate_scoutgpt_l2_seeds.py` (new) — PEP 723 orchestration script, wheel 0.3.4 pinned with SHA `e2c1526…`
- `docs/superpowers/specs/2026-04-20-scoutgpt-l2-harvest-design.md` (new) — this design
- `docs/superpowers/plans/2026-04-20-scoutgpt-l2-harvest.md` (new) — execution plan
- `docs/evolve/scoutgpt-l2-harvest/SUMMARY.md` (new) — full metrics, dispositions, cross-reference to session-50 findings, follow-ups
- `docs/evolve/scoutgpt-l2-harvest/results.json` (new) — full per-variant metrics JSON mirrored from HF Hub
- No seed file deletions (ARCHIVE ratified for the below-baseline variants)

## Approval gates (post-execution record)

1. [APPROVAL #1 — ratified] Fire HF Jobs evaluation (~$7 est, ~$4 actual)
2. [APPROVAL #2 — pending at commit time] Single commit
3. [APPROVAL #3 — pending at push time] Push + PR

## Headline outcome

Captured in `docs/evolve/scoutgpt-l2-harvest/SUMMARY.md`. Short version: `fourier_cross_attention` is a genuine outlier on counterfactual ranking (`rho = +0.380` vs baseline `−0.018`, ~20× magnitude). Hypothesis: Random Fourier Features preserve high-frequency per-player spatial signatures that MLP spatial encoders smear (Tancik et al. 2020). Recommended for a dedicated promotion cycle at production fidelity.

`swiglu_conditioning` cleared baseline marginally (+0.04 fitness) — flagged for promotion pending a production-fidelity re-evaluation.

`orthogonal_cross_attention` and `hybrid_gated_attention` scored the highest top1 accuracy (0.842 each, vs baseline 0.815) but negative rho — they're structurally sound and better at next-action prediction but worse at counterfactual ranking. ARCHIVE (user-ratified; literal pruning rule would have deleted them, but they're kept for future research use).

## Risks and mitigations (as addressed during execution)

- **PEP 723 dependency chain:** first fire crashed in 3 min with `ModuleNotFoundError: No module named 'openevolve'`. Root cause: `from evolve.targets.scoutgpt.evaluator` triggers `evolve/__init__.py` → `EvolveEvaluator` → `openevolve`. Fix: added `openevolve>=0.2.0` to the script's PEP 723 dependencies. Cost of the failed attempt: ~$0.05 cold-start only, no training. Captured as a "check the import chain, not just the top-level target module" lesson for future PEP 723 scripts that call evolve internals.
- **5 → 15-epoch fidelity trade-off:** EV1 found 5-epoch rankings can flip at 15-epoch. Mitigated by using 15-epoch here (matches natural convergence horizon seen in RoPE A/B early-stops). Cost delta vs 5-epoch: ~3× wall clock, ~$4 vs $1 — affordable.
- **Reduced-episode counterfactual eval:** 200 episodes × 50 players (evolve defaults) vs production's 1000 × 100. Mitigated by cross-referencing the baseline rho against production: evolve-scale additive produced `rho = −0.018, std = 0.353`; production-scale additive produced `rho = +0.030, std = 0.290`. Same neighbourhood, differences within the `std` envelope.
