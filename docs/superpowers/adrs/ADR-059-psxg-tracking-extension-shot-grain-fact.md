# ADR-059: PSxG for the tracking cohort — unified shot-grain fact + GK shot-stopping aggregate

- **Status:** Accepted (2026-06-21) — implemented; production deployment (retrain + re-score + cutover) is the post-merge sequence in the plan.
- **Design spec (full detail + verified data):** [`docs/superpowers/specs/2026-06-20-psxg-tracking-extension-design.md`](../specs/2026-06-20-psxg-tracking-extension-design.md) (v3.3).
- **Executable plan:** [`docs/superpowers/plans/2026-06-21-psxg-tracking-extension.md`](../plans/2026-06-21-psxg-tracking-extension.md) (v4).
- **Related:** ADR-013 (ML inference → writer→bronze→staging→mart), ADR-012 (training→prod delivery), ADR-049 (restricted HF publishing), ADR-018 (cross-table join contracts).

## Context

PSxG (post-shot expected goals) was StatsBomb-only — a historical artifact: the model predates TF-48, so it was wired to the only ball-height source then available (StatsBomb freeze-frame `end_location_z`). TF-48 now derives goalmouth geometry from tracking (`shot_crossing_y/z`), verified populated for GradientSports + SkillCorner + IDSSE. Two latent problems compounded it: (1) shots are fragmented across grains (event shots in `fct_shots`, tracking shots as a row-subset of `fct_action_context`); (2) the PSxG aggregation lived only as an inlined CTE inside `fct_goalkeeper_stats` — no PSxG fact; (3) the model's training population was contaminated — filtered on `end_location_z IS NOT NULL`, which is ~46% off-target (`Off T`), giving a 15.9% goal rate vs the true on-target 29.9%.

## Decision

1. **A dedicated shot-grain fact `fct_shot_psxg`** (one row per on-target shot, all providers, grain `(match_key, action_id)`) is the single source of truth — modality is a column (`psxg_input_source`), not a code fork. A derived additive aggregate `fct_gk_shot_stopping` (GK×match) and a pooled comparison layer (GK×competition×season) sit on top. This replaces the inlined `psxg_agg` CTE in `fct_goalkeeper_stats`.
2. **Retrain the model on the corrected true-on-target population** (`shot_outcome IN ('Goal','Saved','Post','Saved to Post')`) rather than reuse-as-is — this removes the 46% contamination *and* aligns the model's population with tracking's (both ~30–33% goal rate), so cross-modality transfer beats a calibration offset. `Post`/`Saved to Post` are included because tracking's `shot_on_target_derived` counts post/bar strikes (verified, P-1). This is a governed (per-player evaluative model) breaking change.
3. **Gating is a flag, not a row-drop** (`psxg_gated`) — gate-failed shots stay with `psxg` NULL so coverage (`shots_faced_total` vs `shots_faced`) is computable and goals-prevented can't be flattered by silently dropping hard shots.
4. **Out-of-sample calibration via GroupKFold-by-match** (Platt for tracking; StatsBomb stays raw); the closed-form **Poisson-binomial band** (`Var = Σ psxgᵢ(1−psxgᵢ)`), not bootstrap (degenerate at n<15), is the uncertainty surface.
5. **Percentile leaderboard deferred** — verified max GK faced 14 on-target tracking shots; the pooled surface is raw goals-prevented ± band with a `ranking_enabled` gate (false until ≥20 GKs clear ≥20 shots). `low_sample` is the norm.

## Consequences

- One PSxG source of truth across providers; new providers slot in via a writer, not a mart edit.
- Goals-prevented for the tracking cohort (GS/SC/IDSSE); Metrica excluded (no bronze ball-z); StatsBomb consolidated via the `event_id → fct_shots → original_event_id → action_id` bridge.
- Retraining shifts live `goals_prevented` (intentional; `model_version` bump + governance update + published-artifact refresh). The StatsBomb cutover is guarded by an attribution-then-value parity check at deploy.
- Deferred (explicitly approved): a fully unified `fct_shots` (the new fact is built on the universal `(match_key, action_id)` key so a future merge is clean), a richer (>2-feature) model, and a per-provider calibration covariate.
