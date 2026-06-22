# ADR-061: GK insight-views — distribution-profile redesign + defensive-line own-goal-distance normalization

| Field | Value |
|---|---|
| **Date** | 2026-06-22 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The redesigned Goalkeeper Analytics page (two views — Distribution Value + Shot Review) replaced both legacy GK pages this cycle. Two of its analytical primitives turned out to be unsound on live data and were reworked:

1. **Distribution Value originally used a 6-preset "best-fit game model" ladder** — each keeper's distributions re-valued under Default / Possession / Counter / Direct / High-Press / Low-Block xT-GK, ranked to find his "best-fit" system. Investigation (2026-06-22, FIFA World Cup cohort, n=39 keepers with ≥20 distributions of 62) found the six preset columns are **~0.99 collinear** (Spearman ρ 0.985–0.995 across preset pairs; best-fit = Counter for 55/62 = 89%; between-keeper level SD 0.0152 vs within-keeper preset spread 0.0045 — the preset axis is ⅓ the size of the level it drowns out). They are scalar reparameterisations (δ, η via `XtGkParams.for_philosophy`, `enrich.py`) of ONE xT-GK formula, so the ladder ranks the *model gradient*, identical for ~every keeper — not the keeper. A de-levelled vs-cohort-median delta ladder also fails: residual "shape" SD ≈ 0.0018 ≈ noise.

2. **Shot Review's defensive-line dimension used silly-kicks `defensive_line_x`**, which `compute_defensive_line` emits in **absolute home-LTR coordinates** (home team defends x=0 → low-x line; away team defends x=105 → high-x line — verified in source). Raw per-keeper averages are therefore bimodal home-vs-away and team-inconsistent: Matt Turner was mislabelled "Deep block" at raw 64.87 when, normalised to distance from his own goal, his line is ~40 m (a *high* line, above the cohort).

xT-GK is also ~97% negative across keepers (mean −0.023), so a mean-based headline carries almost no keeper signal.

## Decision

1. **Replace the game-model fit ladder with a two-axis cohort distribution profile**: x = average forward progression (directness, short-safe ↔ long-direct), y = **share of distributions that add threat** (`xt_gk > 0`; CV ≈ 0.46 — the signal that actually varies). Computed live per competition from `fct_gk_tracking_actions` (floor n ≥ 20 distributions), rendered as a cohort scatter with a median crosshair (proactive / secure-recycler / risk-without-reward quadrants). The six preset columns are dropped from the read path; `fit_ladder`/`offensive_verdict` are deleted.

2. **Normalise defensive-line height to own-goal distance in `fct_gk_defensive_line`**: `avg_line_height_m = defensive_line_x` when the team defends x=0 else `105 − defensive_line_x`, with orientation recovered per action from `back_line_high_x` (`back_line_high_x > defensive_line_x` ⟺ defends x=0). Floor the cohort at `n_actions ≥ 30`, and **demote** the deep/mid/high bucket to a descriptive metres-from-own-goal number — per-keeper averages span only ~10 m (cohort terciles ~2 m apart), so a hard tercile/"right defensive system?" verdict would assert a mismatch on noise.

All cohort comparisons (distribution scatter, sweeper profile, line height) render as **individual-keeper strips**, not IQR boxes, so the real spread/skew/bimodality is visible.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep the 6-preset best-fit ladder | matches the v4 mockup; "which system fits him" hook | ρ≈0.99 collinear → best-fit = model artifact (Counter for 89%), not keeper-specific | statistically degenerate |
| B. De-levelled vs-cohort delta ladder | removes the common model effect | residual SD 0.0018 ≈ noise | no real per-keeper signal survives |
| C. Raw absolute `defensive_line_x` for the line | zero extra work | absolute home-LTR coord → bimodal home/away, inverted meaning | mislabels keepers (Turner "deep" at a high line) |
| D. (chosen) 2-axis % adds-threat × directness profile + own-goal-distance line normalisation, individual-keeper strips | keeper-differentiating, orientation-consistent, honest about thin signal | WC-only in practice (per-keeper volume); line stays descriptive (no system verdict) | — |

## Consequences

### Positive

- The distribution view differentiates keepers on signals that genuinely vary (share-adds-threat CV 0.46; directness), with the honest headline being the *share* that adds threat, not the ~97%-negative mean.
- Defensive-line height is now orientation-consistent and correctly oriented (higher = higher line); the strip surfaces the real distribution shape that an IQR box would hide (it would have revealed the original bimodality on sight).

### Negative / trade-offs

- **WC-only in practice**: only the World Cup cohort (GradientSports) has both breadth and per-keeper distribution volume. A-League (~9 distributions/keeper) and the Bundesligas (≤12 keepers) fall below the floors → the views degrade to "cohort too small / too few distributions" rather than a profile.
- The defensive-line dimension is **descriptive only** — no deep/high bucket, no "right system" verdict — because the per-keeper tournament average barely varies.

### Downstream / migration

- Read path: `queries.gk_analytics.build_distribution_profile_sql` (reads `fct_gk_tracking_actions_synced`), `services.gk_insight.distribution_quadrant`. Deleted: `fit_ladder`, `offensive_verdict`, `median_fit_spread`, `_MODEL_LABEL`, `measured_style_chip`, the per-preset DIRECTION table.
- `fct_gk_defensive_line` column rename `avg_line_x → avg_line_height_m` (own-goal distance); the `fct_gk_defensive_line_synced` Lakebase table was deleted + recreated + re-granted for the rename.
- The IDSSE-preserving NULL-safe season join (`<=>`) in `fct_gk_shot_stopping_pooled` (keeps NULL-season IDSSE rows in the pooled rollup) is part of the same cycle; guarded by `assert_psxg_pooled_keeps_idsse` (dbt) + `test_gk_pooled_join_null_safe` (CI).

ADR-060 (PSxG projected-goalmouth model) is unchanged and remains the source of the `goals_prevented` metric shown in Shot Review's honest-secondary strip.
