---
title: ExT v2 Reproduction Design Spike
date: 2026-04-25 (revised 2026-04-26 post-PR #205)
status: Accepted — Phase 0 in flight on feat/ext-v2-phase-0-singh-baseline
todo_ref: D66 (TODO.md / On Deck)
roadmap_ref: "ExT-style Conditional xT (xT v2 Candidate)" (ROADMAP.md)
tracker_ref: T1 (docs/research/external-research-tracking.md)
adr_ref: ADR-015 — XTGrid wrapper + differential validation (PR #205, merged aeca30c 2026-04-26)
---

# ExT v2 Reproduction Design Spike

## 0. Decision asked of this document

**Original (2026-04-25):** proceed with a phased reproduction of ExT-style conditional fine-grid xT (per Salimi/Salmankhah methodology, channel-attributed in T1) on lakehouse data, or hold for the published preprint? **Outcome:** approved, proceed.

**Status as of 2026-04-26:** the v1 architectural prerequisites surfaced by this spike (XTGrid typed wrapper, differential validation, workflow card SSOT) have shipped in PR #205 / ADR-015. **Phase 0 is now in flight on `feat/ext-v2-phase-0-singh-baseline`** — this document is the working spec for the reproduction's build phases.

The document fixes the parameter space, fitness function, phasing plan, sizing estimate, and open questions for the build. §10 carries the original recommendation rationale; §10.1 carries the post-PR-205 status update.

---

## 1. Methodology summary

Per T1 tracker (`docs/research/external-research-tracking.md`), ExT extends Singh-2018 xT in three ways:

1. **Per-source-cell conditional formulation.** xT becomes a function of (source position, context features), not just source position.
2. **Higher spatial granularity.** ~24×16 grid (vs Singh's 12×8 / 16×12).
3. **KNN replaces the transition tensor.** Adding a contextual feature is *linear* on KNN dimensionality rather than *multiplicative* on tensor storage. KDE smoothing handles the residual sparsity that finer grids exacerbate.

Methodology is non-neural (numpy/sklearn-shaped), CPU-only, no GPU required.

---

## 2. Codebase context (current Singh-2018 stack)

### 2.1 Where xT lives today (post-PR #205)

| Item | Location | Notes |
|------|----------|-------|
| Singh-2018 implementation | `src/analytics/expected_threat.py` | `ExpectedThreatParams` dataclass, `compute_expected_threat_grid()` entry, `_value_iteration_numpy` / `_value_iteration_jax`. **Returns `XTGrid` typed wrapper** (PR #205); not raw `np.ndarray` |
| Typed wrapper | `analytics.expected_threat.XTGrid` | Frozen dataclass owning values + pitch dims + coord_system + competition_id; methods: `lookup(x, y, input_coord_system)`, `validate_structural(max_value=None)`, `validate_differential(previous, max_relative_change=0.30)`, `to_dataframe()`. Eliminates primitive obsession; supports any resolution (24×16 ready) |
| Grid resolution | 12×8 (96 cells) default | `ExpectedThreatParams` defaults; `XTGrid.shape` derives from `values.shape` so any consumer adapts to whatever resolution is loaded |
| Coordinate system | SPADL 105×68m for the grid | `XTGrid.lookup` handles cross-coord conversion when consumer queries in StatsBomb 120×80; binning is mathematically invariant under coord-system choice when `n_zones` matches |
| Transition matrix | 96×96 (default) | Built in-memory, discarded after value iteration; *not* materialized to Delta |
| Value iteration | Undiscounted fixed-point, max 100 iterations, tol 1e-5 | Converges in ~10 iterations per workflow card |
| Workflow card | `workflow-cards/wf-xt-grids.yaml` | Entry: `compute_expected_threat` → `src/ingestion/expected_threat.py`, daily 06:00 UTC, driver-bound. Now calls `XTGrid.validate_differential` against the previous bronze grid (per-competition + global) |
| Output table | `bronze.expected_threat_grids` | Schema unchanged: `zone_x BIGINT, zone_y BIGINT, xt_value DOUBLE, competition_id STRING, _ingested_at TIMESTAMP`. Per-competition + `competition_id='global'` aggregate. Logical PK: `(zone_x, zone_y, competition_id)` |
| Bronze→staging→mart layering | None (bronze only) | No `stg_expected_threat_grids` view, no `fct_xt_values` mart |
| JAX fallback | Triggered if `n_zones > 200` | At ExT v2's 24×16 = 384 cells, JAX path *would* activate — relevant for sizing |
| SSOT parity test | `src/tests/test_workflow_card_xt_grid_ssot.py` (PR #205) | Asserts any `NxM grid` / `NxM cells` claim in `wf-xt-grids.yaml` matches `ExpectedThreatParams` defaults; opt-out via U+00D7 substitution |

### 2.2 Consumers (small footprint, post-PR #205 wrapper-aware)

xT has exactly *one* downstream consumer in the codebase today:

| Consumer | Path | What it does |
|----------|------|--------------|
| Off-ball xT | `src/analytics/off_ball_xt.py` + `src/ingestion/off_ball_xt.py` | Loads `bronze.expected_threat_grids` as `XTGrid` via `_load_xt_grid_from_spark` (shape derived from query results — supports v2's 24×16); calls `XTGrid.lookup(x, y, "statsbomb")` for per-player xT |
| Off-ball xT mart | `dbt_project/models/marts/fct_off_ball_xt.sql` | Incremental merge on `off_ball_xt_id`, liquid-clustered by `match_id` |
| Movement Analysis Taipy page | `hf_taipy_app/src/state/movement_analysis.py:313` | Reads `total_off_ball_xt`, `avg_off_ball_xt` from the mart |

**Notably absent:** `src/analytics/line_breaking.py`, `src/analytics/obso.py`, the VAEP stack, and `fct_action_values` do **not** consume xT. There is no `fct_pass_epv` mart yet — that is D60 territory and consumes xT downstream of this work.

This is good news for the v2 migration: the blast radius is narrow. An xT v2 rollout primarily impacts off-ball xT computation and the (planned) D60 EPV mart. Both will receive `XTGrid` instances from the v2 producer the same way they receive v1 grids today — the wrapper interface absorbs the resolution change.

### 2.3 Latent bugs surfaced by spike research — **all three SHIPPED in PR #205 / ADR-015**

Three findings surfaced during the spike. All three closed in the v1 hygiene cycle that preceded Phase 0. ADR-015 documents the design rationale. Listed here for archival traceability:

1. **Workflow card prose drift.** `wf-xt-grids.yaml` claimed "16x12 grid" while the code default was 12×8. Pure documentation drift. **Fixed in PR #205**: prose scrubbed to defer to `ExpectedThreatParams` SSOT with explicit pointer to the parity test; new `src/tests/test_workflow_card_xt_grid_ssot.py` enforces match (regex on ASCII 'x' only — opt-out via U+00D7 substitution).
2. **Hardcoded grid resolution coupling** in `_lookup_xt` and `_load_xt_grid_from_spark` (both formerly in `off_ball_xt`). The original §2.3 framing of this bug as a "coordinate system mismatch" causing silent off-ball xT miscalibration was a **false positive** — discovered during the architectural review. Both binning functions are internally consistent with their respective input coord systems, and the binning result is mathematically invariant under coord-system choice when `n_zones` matches: for any physical position, `int(x_sb × 0.875 / 8.75) = int(x_sb / 10)` so SPADL-trained and StatsBomb-queried cells coincide. The real hazard was maintenance fragility — three call sites hardcoded `(12, 8)` with no enforcement they remained coupled, which would silently miscalibrate during ExT v2 rollout if any site lagged. **Fixed in PR #205**: `XTGrid` typed wrapper now owns the lookup; consumers receive `XTGrid` and call `.lookup(x, y, input_coord_system)`. Shape derives from `values.shape`. ExT v2's planned 24×16 conditional grid works with no consumer changes.
3. **Magic-number value-range cap** — `validate_xt_grid` rejected grids with `max > 0.50`, brittle for ExT v2's near-box conditional values that can approach 1.0. **Fixed in PR #205**: replaced with `XTGrid.validate_structural(max_value=None)` (opt-in upper bound, defaults to no upper) plus new `XTGrid.validate_differential(previous, max_relative_change=0.30)` against historical baseline. v1 callers preserve old behavior by passing `max_value=0.50` explicitly (the global-grid validator in `src/ingestion/expected_threat.py` does this).

**Phase 0 implication:** the producer (`compute_expected_threat_grid`) now returns `XTGrid` not `np.ndarray`. The Phase 0 stop condition ("matches existing implementation outputs to within numerical tolerance") compares `XTGrid.values`. The new harness consumes `XTGrid` directly via the same wrapper interface that production code uses — no impedance mismatch.

---

## 3. Tracking-data feature availability for contextual features

The two contextual features named by the authors (T1 methodology field) are *position of the last defender* and *count of opposition between ball and goal*, both at pass-release timestamp. Coverage by source:

| Source | Last-defender position? | Opponents between ball + goal? | Join pattern from `fct_passes` |
|--------|-------------------------|---------------------------------|--------------------------------|
| **StatsBomb 360** | ✅ Yes (positions only, no `player_id` — anonymous freeze frames) | ✅ Yes — filter `is_teammate=false`, count `location_x > pass_start_x` | Direct UUID match: `fct_passes.event_id` → `stg_statsbomb__360.event_uuid` |
| **IDSSE** (Bundesliga) | ✅ Yes (real `player_id`, `is_goalkeeper` flag) | ✅ Yes | `fct_passes.event_id` → `bronze.elastic_sync_results.event_id` → `frame_id` → tracking frame |
| **SkillCorner** (A-League) | ⚠️ Conditional — no event data, no ELASTIC sync output | ⚠️ Same | None established — would require manual timestamp-based nearest-frame implementation |
| **Metrica** | ⚠️ Conditional — anonymized `player_id` in sample data, heuristic GK flag | ⚠️ Same | Possible via `(period, timestamp_seconds)` nearest-frame; not currently implemented |

**Net coverage:** StatsBomb 360 + IDSSE provide both features without new ingestion work. SkillCorner + Metrica require a new pass↔tracking join implementation; out of scope for v1 of this reproduction (file as a follow-up extension).

### 3.1 Reusable precedents

Three existing functions cover all required building blocks — *no new infrastructure code needs to be written* for the StatsBomb 360 + IDSSE feature derivations:

| Function | Path | Reuse for |
|----------|------|-----------|
| `align_events_to_frames` | `src/analytics/elastic_sync.py:158` | The IDSSE event↔frame join (already implemented; result table is `bronze.elastic_sync_results`) |
| `detect_line_breaking_batch` | `src/analytics/line_breaking.py:323` | The `opponents_by_event: dict[event_id, DataFrame]` pattern — exactly the shape needed for "opponents between ball and goal" |
| `compute_defcon_match` | `src/analytics/defcon_lite.py:485` (opponent groupby at line 403) | The `freeze_frames_df.groupby('event_id')` opponent extraction pattern — directly reusable for last-defender position |

### 3.2 Coordinate-flip caveat

All sources are normalized to **StatsBomb 120×80** by the dbt staging layer (`dbt_project/macros/normalize_coordinates.sql:39-90`; Python mirror at `src/analytics/coordinates.py:50-137`). However, **attacking direction is not normalized** — the codebase does not flip coordinates by possession direction. For ExT v2, "last defender" and "opponents between ball and goal" both depend on which goal is being attacked.

The reproduction must implement a per-pass attacking-direction flip *before* computing the contextual features. This is a small helper (~20 LOC) but it does not exist today and is an implementation prerequisite.

---

## 4. Optuna parameter space

Eight axes. TPE sampler (Optuna default). Estimated 200–500 trials per phase to saturate.

```python
import optuna

def objective(trial: optuna.Trial, train_data, holdout_data) -> float:
    # KDE smoothing
    kde_kernel = trial.suggest_categorical(
        "kde_kernel", ["gaussian", "epanechnikov", "tophat"]
    )
    kde_bandwidth = trial.suggest_float(
        "kde_bandwidth", 0.01, 2.0, log=True
    )
    kde_adaptive = trial.suggest_categorical(
        "kde_adaptive", [True, False]
    )

    # KNN lookup
    knn_k = trial.suggest_int("knn_k", 5, 200, log=True)
    knn_distance = trial.suggest_categorical(
        "knn_distance", ["euclidean", "mahalanobis", "weighted_euclidean"]
    )
    feature_norm = trial.suggest_categorical(
        "feature_norm", ["standardize", "minmax", "raw"]
    )

    # Per-feature inclusion (Phase 3+; Phase 2 omits these)
    use_last_defender = trial.suggest_categorical(
        "use_last_defender", [True, False]
    )
    use_opponents_between = trial.suggest_categorical(
        "use_opponents_between", [True, False]
    )
    # Per-feature weights only if knn_distance == "weighted_euclidean"
    if knn_distance == "weighted_euclidean":
        if use_last_defender:
            trial.suggest_float("w_last_defender", 0.0, 5.0)
        if use_opponents_between:
            trial.suggest_float("w_opponents_between", 0.0, 5.0)

    # Build the model
    model = build_ext_model(
        train_data=train_data,
        kde_kernel=kde_kernel,
        kde_bandwidth=kde_bandwidth,
        kde_adaptive=kde_adaptive,
        knn_k=knn_k,
        knn_distance=knn_distance,
        feature_norm=feature_norm,
        contextual_features=_collect_features_from_trial(trial),
    )

    return compute_holdout_nll(model, holdout_data)
```

**Phase-by-phase activation:**

- Phase 0 (Singh baseline): no Optuna, fixed hyperparameters.
- Phase 1 (KDE-smoothed Singh): activate only `kde_kernel`, `kde_bandwidth`, `kde_adaptive`. ~3-axis search.
- Phase 2 (KNN, no context): activate `knn_k`, `knn_distance`, `feature_norm` (in addition to KDE). ~6-axis search.
- Phase 3 (contextual features): activate per-feature inclusion flags. ~8-axis search.
- Phase 4 (feature exploration): expand inclusion flag list with new features (game state, attacker count in attacking third, score differential, time pressure). 8–12 axes.

---

## 5. Fitness function

### 5.1 Inner loop (per Optuna trial)

**Held-out negative log-likelihood of action sequences.** Cheap, dense signal, classical statistical fit metric.

```
NLL = -mean(log P(actual_destination | source, context))
```

Computed against a held-out set of pass events. Lower is better; Optuna minimizes.

### 5.2 Outer selection (after Optuna run)

**Calibration to actual goal-probability outcomes.** Take the top-K (K≈10) trials by held-out NLL, evaluate each by computing implied goal probability for action sequences in a *separate* outer holdout, and select the configuration with best calibration (Brier score against actual outcomes). This guards against NLL-overfitting that doesn't translate to downstream usefulness.

### 5.3 Holdout protocol

**Match-based holdout, stratified by competition.** 15% of matches per competition held out, sampled to preserve the league mix.

Justification:
- Time-based holdout risks confounding with rule changes / strategic evolution across seasons (avoided).
- Pure random match holdout could over-represent leagues with more matches in either split (avoided).
- Per-competition stratification preserves league diversity in both train and holdout (chosen).

The outer-selection set is a separate 5% match-based stratified holdout, never seen during Optuna search. Reduces selection bias on the inner NLL metric.

---

## 6. Phasing plan

| Phase | Goal | Optuna axes active | Validates | Stop condition (continue / archive) |
|-------|------|---------------------|-----------|--------------------------------------|
| **0** | Singh-2018 baseline on our data, NLL on held-out | None | Floor metric; reference point | Continue if implementation matches `src/analytics/expected_threat.py` outputs to within numerical tolerance |
| **1** | KDE-smoothed Singh | KDE only (3 axes) | KDE half of ExT works on our data | Continue if Phase 1 NLL < Phase 0 NLL by ≥1% relative; otherwise KDE smoothing isn't doing useful work and we file the finding |
| **2** | KNN replaces transition matrix, no context | KDE + KNN-no-context (6 axes) | KNN substitution does not regress against Phase 1 (sanity check) | Continue if Phase 2 NLL ≈ Phase 1 NLL within 1% relative; large regression means the KNN implementation has a bug |
| **3** | Add contextual features incrementally — last-defender first, count-between-ball-and-goal second | KDE + KNN + per-feature flags (8 axes) | Each contextual feature improves NLL incrementally | Drop any feature that does not improve held-out NLL by ≥0.5% relative when added |
| **4** | Feature exploration | KDE + KNN + extended feature menu (8-12 axes) | Discover features beyond the published set | Add features one at a time; same 0.5% relative-improvement threshold |

### 6.1 Key implementation question — KNN compatibility with value iteration

This is the trickiest design decision in the reproduction. Singh's value iteration converges because the transition matrix `T(s→d)` is fixed across iterations. With KNN, the "transition" is a query-time lookup that returns a distribution over destination cells; this is fine for transitions, but the iterative term `xT(s', c')` requires evaluating xT at the *next* (cell, context) state, which depends on what action was taken — and `c'` (next-frame context) is itself a function of the action chosen.

Two approaches:

**Approach A — Discretize the joint (cell, context) space; iterate on tabulated values.**
- Bin each contextual feature to a small number of bins (e.g., 10 bins per feature).
- For each (cell, context_bin) joint cell, compute the conditional transition distribution via KNN to training events whose source position falls in that cell *and* whose context features fall in that bin.
- Aggregate the K neighbors' destinations to get a probability distribution over next (cell', context_bin') joint cells.
- Iterate the value function on this finite tabulated joint state until fixed point.
- For Phase 3 with 2 features × 10 bins = 100 context combos, the joint state has 384 × 100 = 38,400 cells. Iteration cost: 38,400² = 1.5B ops per iteration → ~10-100 iterations to converge → seconds-to-minutes per Optuna trial. Tractable.

**Approach B — Continuous KNN at every iteration.**
- Don't discretize; query the full training data via KNN at every iteration step.
- Theoretically cleaner but ~3 orders of magnitude more expensive per trial.
- Probably how the authors do it given the "we use KNN" framing, but reproducible from Approach A's results once it works.

**Recommendation: Approach A for the reproduction.** Documents the discretization explicitly as a design choice; can revisit Approach B if Phase 3+ NLL plateaus suspiciously below the unconditional baseline.

**Open question for the authors (§8):** confirm Approach A vs B vs a hybrid.

### 6.2 Expected per-phase artifact

Each phase produces:
- An `mlflow.run` with all Optuna trial parameters logged (via Optuna's `MLflowCallback`).
- The best-config model serialized to disk (joblib for the KDE+KNN objects, JSON for hyperparameters).
- A held-out NLL summary table.
- A per-phase markdown summary at `docs/evolve/ext-v2-phase-N/SUMMARY.md` mirroring the existing `docs/evolve/<cycle>/` convention.

Phase 4 additionally produces a feature-importance ranking (which contextual features are selected in the top-K trials).

---

## 7. Sizing

### 7.1 Data volume

- Training events: rough estimate ~1.5M+ pass events across all 5 sources (StatsBomb dominant; IDSSE ~10K events × 7 matches; Metrica + SkillCorner small contributions). Verify exact count at Phase 0 from `select count(*) from fct_passes`.
- For Phase 0–2: all sources usable (event-only is sufficient).
- For Phase 3–4: only StatsBomb 360 + IDSSE usable (need pass↔tracking-frame join). This *halves-or-more* the effective training set for contextual phases.

### 7.2 KNN cost per trial

| Cost component | Phase 2 (no context, K=50) | Phase 3 (2 features, K=50) |
|----------------|------------------------------|------------------------------|
| Index build (BallTree on 1.5M events) | ~5–15 s | ~5–15 s |
| Per-query cost (K=50, log N) | ~50–100 µs | ~50–100 µs |
| Number of queries (per Optuna trial, joint state evaluation) | 384 cells × 1 = 384 | 384 cells × 100 context bins = 38,400 |
| Total query time | <1 s | ~3–10 s |
| Value iteration (~10–100 sweeps × joint-state size) | ~1–5 s | ~10–60 s |
| **Total per trial** | **~10–25 s** | **~25–100 s** |
| Optuna trials (TPE saturation) | 200–500 | 200–500 |
| **Wall-clock per phase** | **~30 min – 3 hr** | **~1–14 hr** |

CPU-only, runnable on a laptop. No GPU needed. Phase 4 with 4-6 contextual features at 10 bins each pushes the joint state to 384 × 10⁴ = 3.84M cells — at that point switch to coarser binning (5 bins per feature → 384 × 625 = 240K) or move to Approach B with sampled KNN queries.

### 7.3 ANN backend choice

| Option | When | Rationale |
|--------|------|-----------|
| `sklearn.neighbors.BallTree` | Phase 0–2, low-dim | Built-in, no extra dep, optimal for ≤10 dims, exact KNN |
| `sklearn.neighbors.BallTree` with Mahalanobis | Phase 2–3 if mahalanobis distance is selected | sklearn supports custom metric; small slowdown vs Euclidean |
| `hnswlib` (Python wheel) | Phase 4 if dim > 10 | Sub-linear approximate KNN; we already understand HNSW from pgvector usage |
| `pgvector` HNSW indexes on Lakebase | Production scale (post-spike) | Already infra-resident; fits naturally if ExT v2 inference moves to query-time |

**Spike recommendation:** `BallTree` for Phases 0–3 (Optuna search), evaluate `hnswlib` only if Phase 4 explores >10 features. No new infrastructure needed.

### 7.4 Adds Optuna as new dep

`uv add optuna` — ~3 MB wheel, MIT licensed, well-maintained by Preferred Networks. No transitive dep concerns. Optional `optuna-dashboard` for the web UI (`uv add --optional dev optuna-dashboard`). Plays well with MLflow via `MLflowCallback`.

---

## 8. Open questions for the authors (post-publication)

To be raised after the preprint or code lands. These are the questions whose answers would change *our* design choices and where reproduction-from-LinkedIn will diverge predictably until they're answered:

1. **KDE bandwidth selection.** Single global vs per-axis vs adaptive (per-cell) vs cross-validated bandwidth? Our spike uses per-trial Optuna search; theirs may use a fixed analytic choice (e.g., Scott's rule, Silverman's rule).
2. **KNN distance metric specifics.** Euclidean on raw coords, Mahalanobis with what covariance, or a learned/weighted metric? Affects whether we even select the right metric in the Optuna categorical.
3. **Exact feature definitions.** "Position of the last defender" — deepest defender's Y-coordinate, perpendicular distance to the ball-to-goal line, projection onto the attack-direction axis? "Count of opposition between ball and goal" — count by perpendicular projection onto attack axis, count within visible angular sector from ball, count within Voronoi region between ball and goal-line midpoint? These choices materially affect NLL outcomes.
4. **Holdout protocol.** Time-based, match-based, k-fold over competitions? Affects directly whether our numbers are comparable to theirs.
5. **Value iteration settings.** Discount factor (we use undiscounted; they may use ɣ < 1), convergence tolerance, treatment of absorbing states (goal, out-of-bounds).
6. **Failed-pass treatment.** Singh-2018 treats failed actions as xT=0 (not a transition). With contextual conditioning, this becomes more nuanced — does the conditional transition model failed-pass destinations explicitly, or filter them out?
7. **Approach A vs B for KNN-iteration compatibility (§6.1).** Confirm whether the authors discretize the joint state or run continuous KNN at each iteration step.
8. **What features did they ultimately settle on?** "Still doing some experiments" per their LinkedIn response — once they settle, our Phase 3 list updates.

---

## 9. Kimball-completion dependency

The spike itself is **fully schema-agnostic**. The reproduction phases have differential dependencies:

| Phase | Schema dependency | Kimball PR gate |
|-------|--------------------|------------------|
| Phase 0 (Singh baseline) | `fct_passes` (event-only) | None — works on either smart-keyed `match_id` or BIGINT `match_key` |
| Phase 1 (KDE-smoothed) | Same as Phase 0 | None |
| Phase 2 (KNN, no context) | Same as Phase 0 | None |
| Phase 3 (contextual features) | `fct_passes` + tracking frames at pass-release | **Gated on PR 7** (tracking + formations + pausa + tail facts migration to `match_key`) |
| Phase 4 (feature exploration) | Same as Phase 3 | **Gated on PR 7** |

Per `project_ext_v2_reproduction_posture.md`: PR 6 (defensive + GK + pitch control + IDSSE `is_progressive`) is adjacent but not directly load-bearing for this reproduction — pitch control and defensive marts are not consumed by xT, and the IDSSE `is_progressive` classifier is orthogonal. PR 8 (cleanup + doc sweep) is post-Kimball and not on the critical path.

**Net build-phase gate:** Kimball PR 7. Phases 0–2 can begin immediately upon spike approval; Phases 3–4 should not begin until PR 7 lands.

**Status as of 2026-04-26:** Phase 0 in flight on `feat/ext-v2-phase-0-singh-baseline`. Phases 1–2 will follow on dedicated branches (`feat/ext-v2-phase-1-kde-smoothed`, `feat/ext-v2-phase-2-knn-no-context`) once Phase 0 lands. The §2.3 architectural prerequisites all shipped pre-Phase-0 in PR #205, independent of the Kimball gate.

---

## 10. Recommendation

### 10.0 Original recommendation (2026-04-25 — accepted)

Promote D66 to a full reproduction TODO with phased authorization:

- **Phase 0–2 (event-data only, schema-agnostic):** authorize immediately upon spike approval. Estimated cumulative effort: 1.5–3 weeks part-time wall-clock (mostly Optuna runs; the implementation itself is ~500–800 LOC including tests).
- **Phase 3–4 (contextual features, tracking-frame join):** do not authorize start until Kimball PR 7 lands. Estimated cumulative effort once unblocked: another 2–3 weeks part-time wall-clock.

**Why proceed (not hold for preprint):**
1. Methodology is implementable from current information — no blocker-class unknowns surfaced in this spike.
2. All required tracking-data features are derivable from existing sources (StatsBomb 360 + IDSSE) without new ingestion work; reusable building blocks already exist (`align_events_to_frames`, `defcon_lite` opponent groupby, `line_breaking` opponents-by-event pattern).
3. Spike output positions us to run the build the *moment* Kimball PR 7 lands, vs queuing behind a fresh design after the preprint drops.
4. When the paper does land, the comparison becomes "their bandwidth=X, our Optuna found Y" — a higher-signal validation than running their published config blind.
5. CPU-only, no GPU, no new infrastructure — incremental cost is essentially zero.

**Why this is *not* an evolve cycle:** ExT's KDE+KNN is sklearn-shaped with declared parameter ranges and fast evaluations. Optuna's TPE saturates the space efficiently; evolve's LLM-driven architecture mutation has nothing to mutate. (Full rationale in `project_ext_v2_reproduction_posture.md`.)

**Phase 0–2 success criterion for promotion to Phase 3:** Phase 2 reproduces Phase 1's NLL within 1% relative — confirms the KNN substitution is bug-free against the unconditional KDE-smoothed reference. If Phase 2 regresses materially, halt and debug before adding contextual features.

### 10.1 Post-PR-205 status update (2026-04-26)

The original "tactical now / architectural with Phase 0" trade-off (§2.3 latent bugs handled either as small patches now or as part of Phase 0 work) was rejected in conversation in favor of a dedicated v1 hygiene cycle ahead of Phase 0. That cycle has shipped:

| §2.3 prerequisite | Disposition | Where |
|--------------------|-------------|-------|
| Workflow card prose drift (Bug 1) | Fixed; SSOT parity test added | PR #205 |
| Hardcoded grid resolution coupling (Bug 2 — re-characterized from false-positive) | Fixed via `XTGrid` typed wrapper; eliminates the maintenance-fragility hazard | PR #205, ADR-015 §1 |
| Magic-number value-range cap (Bug 3) | Fixed via `XTGrid.validate_structural(max_value=None)` opt-in plus differential validator | PR #205, ADR-015 §2 |
| Workflow card SSOT discipline (Bug 1 corollary) | Established via parity test pattern | PR #205, ADR-015 §3 |
| Wheel 0.3.15 deployed to UC Volume + HF Hub | Required for HF Jobs scripts that will eventually consume v2 | PR #205 |

**Phase 0 is in flight on `feat/ext-v2-phase-0-singh-baseline`** (cut from `main` at `aeca30c` 2026-04-26). Phase 0 deliverable: Singh-2018 reimplementation under the new Optuna harness with held-out NLL inner loop, validating that the harness produces `XTGrid` outputs matching `compute_expected_threat_grid` to within numerical tolerance on `fct_passes` event data.

**Phase 1–2 will follow on separate branches** (`feat/ext-v2-phase-1-kde-smoothed`, `feat/ext-v2-phase-2-knn-no-context`) once Phase 0 lands. Each phase is its own PR with its own ADR-or-spec follow-up; the per-phase markdown summary at `docs/evolve/ext-v2-phase-N/SUMMARY.md` serves the same role for build phases that this doc serves for the design.

**Phase 3–4 still gated on Kimball PR 7** per §9 — unchanged.

### 10.2 Phase 0 build outcomes (2026-04-26)

Phase 0 implementation shipped on `feat/ext-v2-phase-0-singh-baseline`. Stop condition met: PASS — see `docs/evolve/ext-v2-phase-0/SUMMARY.md` for headline numbers and the per-competition NLL table.

**Locked design decisions made during Phase 0 build (revising spec assumptions):**

| Decision | Lock | Rationale |
|---|---|---|
| Single-table source for both train + NLL eval | `fct_action_values` (filtered to xT-relevant types for training; `action_type='pass'` for NLL eval). The spec's Phase 0 "NLL on `fct_passes`" framing in §3 is corrected to a single-source pattern. | Train↔eval distributional consistency; version stability across phases; numerical-tolerance match purity (v1 trains exclusively on `fct_action_values`). |
| Hash key for holdout split | `match_key` (BIGINT, present on both `fct_action_values` and `fct_passes`) | Pre-positions Phase 3-4 BIGINT requirement; `fct_passes` doesn't have the legacy string `match_id`. |
| Small-comp handling | Hash-split as-is; per-comp NLL skips empty-holdout comps gracefully; global NLL uses contributions from all 22 comps. | 5 comps ({44, 81, 87, 116, 1470}) have 1-6 matches; binomial 15% threshold means most fall entirely to train. Their training contribution is preserved without distorting per-comp reports. |
| MLflow integration | Deferred to Phase 1 (YAGNI — no axes in Phase 0, no trials worth tracking). | Phase 1 wires `optuna.integration.MLflowCallback` when KDE axes activate. |
| JAX path in v2 producer | Skipped — numpy-only. | Phase 0 production grid (12x8, n_zones=96) doesn't trigger v1's JAX path either. v2's tolerance-match test uses strict equality on small grids and `allclose(rtol=1e-5)` on n_zones>200 to absorb cross-backend float-arithmetic noise. |
| Workflow card | None for Phase 0. | Local benchmark, not recurring. |

**Phase 0 baseline numbers** (full breakdown in SUMMARY.md):

- Global held-out NLL: **3.78924** vs `log(96) = 4.564` uniform-baseline → **−17.0%** improvement (Singh-2018 captures non-trivial transition structure)
- Holdout: 677,436 passes across 16 of 22 competitions (6 comps with ≤6 matches each fell entirely into train under 15% hash threshold — preserved decision iii)
- Wall-clock: 6.9s fit + NLL on 8.8M actions (well within budget; vectorization smoke test: 50K actions in 0.5s)
- 127 tests in `src/tests/test_ext_v2/` enforce the parity + numerical-match contracts

**Phase 1 stop condition (pre-registered):** Phase 1 NLL < 3.7513 (i.e., ≥ 1% relative improvement over Phase 0). Below that threshold, KDE smoothing isn't doing useful work on this data — file finding and skip to Phase 2.

### 10.3 Phase 1 design — locked decisions (2026-04-26)

Phase 1 design locked in conversation 2026-04-26 ahead of implementation. This subsection captures the seven decisions resolved before code is written; build outcomes will be appended after the harness run completes.

**Phase 1 brief:** KDE-smoothed Singh transition matrix under the existing Phase 0 Optuna harness skeleton, with `kde_kernel`, `kde_bandwidth`, `kde_adaptive` axes activated. Stop condition pre-registered in §10.2: `nll_primary < 3.7513` (≥ 1% relative improvement over Phase 0's 3.78924). Run venue: local Win11 96 GB. Estimated wall-clock at `n_trials=500`: ~83 minutes.

**Locked design decisions made during Phase 1 brainstorm:**

| # | Decision | Lock | Rationale |
|---|---|---|---|
| 1 | KDE library | `sklearn.neighbors.KernelDensity` | Only library supporting all three named kernels {gaussian, epanechnikov, tophat} (§4); BallTree-backed scales to 8.8M-row training; already top-level dep at `pyproject.toml:28` (zero new-dep cost); same family as Phase 2's KNN. Rejected: `scipy.stats.gaussian_kde` (gaussian-only collapses §4 axis), hand-rolled numpy (oracle-test surface against sklearn anyway). |
| 2 | What gets KDE-smoothed | Per-source-zone destination KDE — one 2D KDE per source row over `(end_x, end_y)`; evaluate at zone centers (point evaluation), row-normalize | Matches Singh's row-stochastic interpretation `T[s,:] = P(d|s)`; aligns with published Salimi/Salmankhah per-source-cell conditional surfaces (LISS poster); 2-3× faster than 4D joint KDE at our scale; trivially parallelizable per source. Rejected: 4D joint (rough bandwidth selection, post-hoc row normalization, GK-area→midfielder probability leakage), zone-integration via Monte Carlo (10-100× per-trial cost for negligible gain when row-normalization absorbs discretization bias). |
| 3 | `kde_adaptive` semantics | Per-row Silverman with global multiplier — `h_s = bandwidth × silverman_2d(n_s)` when `adaptive=True`, `h_s = bandwidth` when `adaptive=False`; `silverman_2d(n) = n^(−1/6) × sigma_s` with isotropic σ proxy `sigma_s = sqrt((var_x + var_y) / 2)` from per-row destination positions; row-mean fallback for zero-event source zones | Both Optuna axes meaningful in both modes (no dead-axis trial waste); statistically legitimate (Silverman 1986 §4.3); aligns with methodology's per-source-cell smoothing variation (LISS poster); enables sparse-row auto-widening; post-hoc trial-table analysis reveals adaptive selection rate as a free ablation. Rejected: Silverman-fallback (kde_bandwidth dead when adaptive=True, TPE waste), drop axis (loses literature-validated per-row variance-aware behavior). |
| 4 | NLL eps clipping | Primary `nll_primary` retains Phase 0 `eps=1e-10`; per-trial diagnostic `nll_floorless` logged at `eps=1e-300` (IEEE numerical safety only) | Stop condition compares to Phase 0's NLL — same machinery required for apples-to-apples comparison; diagnostic catches finite-support kernel impact post-hoc (gaussian: `nll_primary ≈ nll_floorless`; epanechnikov/tophat: diagnostic may differ) without contaminating the primary metric. Rejected: remove floor (∞ on epanechnikov/tophat zero cells), relax to 1e-6 (moves Phase 0 → 1 goalposts), per-kernel branching (breaks cross-kernel comparison within a single Optuna study). |
| 5 | Trial count sizing | `n_trials=500` (~83 min wall-clock at ~10s/trial); plateau-check escape hatch — if `study.best_trial.number` ∈ last 50 trials, manually extend by 200 | Generous saturation margin for 3-axis space with six categorical regimes; `kde_bandwidth` log-uniform over two orders of magnitude needs density; marginal cost over `n_trials=300` is ~30 minutes on local hardware; consistent methodology with Phase 4's higher axis count. Rejected: `n_trials=300` (insufficient margin given 1% stop-condition slack), adaptive extension logic (overkill for 3 axes), time-budgeted via Optuna `timeout=` (results depend on machine speed). |
| 6 | Outer selection (top-K Brier calibration) | Defer entirely to Phase 2; Phase 1 keeps `holdout_split(holdout_fraction=0.15)` unchanged from Phase 0 | Phase 1 stop condition is NLL-only — outer selection adds no decision value here; a 3-way 80/15/5 split would shrink train by 7%, risking ~0.5% relative NLL contamination of the 1% stop-condition margin; Phase 1's KDE config is discarded by Phase 2's KNN substitution anyway; Brier-score machinery (~150 LOC + actual goal-outcome lookup) is genuinely useful in Phase 2 where it drives stop-condition design. Rejected: full inclusion in Phase 1 (train-shrinkage + bloat), hybrid 3-way-now-Brier-later (still pays train-shrinkage cost). |
| 7 | Real-data run venue | Local Win11 96 GB | Phase 0 venue — same hardware, proven 8.8M-row Databricks pull pattern; ~83 min wall-clock acceptable; zero infrastructure cost. Rejected: DGX Spark (Grace ARM ~0.5× speed, ~166 min), Databricks job (16 GB driver constraint complicates 8.8M-row Arrow load, $1-2 cost, slower iteration cycle for an 83-min job). |

**Architecture additions to `src/analytics/ext_v2/`:**

- `kde.py` (new): `KDESmoothedTransition(TransitionModel)` — per-row KDE wrapping 96 `KernelDensity` instances with per-row adaptive bandwidth.
- `producer.py` (extended): `KDESmoothedProducer(Producer)` — composition mirror of `SinghProducer` swapping the transition step; value-iteration and `XTGrid` wrap unchanged.
- `harness.py` (extended): Phase 1 axes activated in `objective`; new `Phase1Result` dataclass; `run_phase1_harness(...)` wires `optuna.integration.MLflowCallback`.

**Optuna study persistence:** SQLite at `docs/evolve/ext-v2-phase-1/optuna.db` (resumable, durable, checked in). **MLflow tracking URI:** `file:./mlruns` (repo-root convention; `mlruns/` added to `.gitignore`).

**Test surface (`src/tests/test_ext_v2/test_kde.py` + extensions to peer test files):**

- `TestKDESmoothedTransitionContract` — row-stochasticity per row; correct dtype/shape; `matrix` raises before `fit`; required-columns validation.
- `TestKernelCorrectness` — for each of {gaussian, epanechnikov, tophat}, fitted matrix matches a hand-built sklearn `KernelDensity` reference on synthetic data.
- `TestSilvermanAdaptive` — `adaptive=True` produces wider bandwidth on sparse rows; multiplier semantics match `bandwidth × silverman_2d(n)`.
- `TestZeroEventSourceFallback` — source zone with no train events gets row equal to mean of all other rows.
- `TestEpsClipBehaviour` — gaussian: `nll_primary ≈ nll_floorless` (within float noise); epanechnikov/tophat: diagnostic captures the gap.
- `TestSmoothingConvergesToSingh` — as `bandwidth → 0` (Dirac limit), `KDESmoothedTransition.matrix ≈ SinghTransitionMatrix.matrix` (within tolerance) — sanity check that smoothing → no smoothing degenerates correctly.
- `test_producer.py::TestKDESmoothedProducerComposition` — `KDESmoothedProducer.transition_matrix == KDESmoothedTransition.fit(...).matrix` (composition is delegation, not duplication).
- `test_harness.py::TestPhase1ObjectiveAxes` — running `objective` with synthetic mock `trial` exercises all 3 axes with at least 1 value each.

**Per-phase artifacts** (mirror Phase 0 convention at `docs/evolve/ext-v2-phase-1/`):

- `SUMMARY.md` — stop-condition disposition, headline numbers, per-competition NLL table, plateau-check note, design-decision back-references.
- `phase1_baseline.json` — best trial's params, `nll_primary` + `nll_floorless`, `n_train_actions`, `n_holdout_passes`, study metadata.
- `best_producer.joblib` — fitted `KDESmoothedProducer` of the best config (per §6.2).
- `optuna.db` — SQLite study (resumable; checked in).
- `mlruns/` — MLflow run artifacts (gitignored).

**Phase 1 build outcomes (2026-04-26):**

Phase 1 implementation shipped on `feat/ext-v2-phase-1-kde-smoothed`. Stop condition met: PASS — see [`docs/evolve/ext-v2-phase-1/SUMMARY.md`](../../evolve/ext-v2-phase-1/SUMMARY.md) for headline numbers, full per-competition NLL table, and lessons-learned narrative.

| Metric | Value |
|---|---:|
| `nll_primary` (eps=1e-10, stop-condition metric) | **3.74823** |
| `nll_floorless` (eps=1e-300, diagnostic) | 3.74823 |
| Phase 0 baseline | 3.78924 |
| Relative improvement | **+1.082%** |
| Stop threshold | 3.7513 |
| Stop disposition | **PASS** |
| Best trial | #276 of 500 |
| Best params | `kernel=gaussian, bandwidth=1.99998, adaptive=True` |
| Plateau warning | False |
| Wall-clock | 8,130.2s (~135 min) |
| Per-trial wall-clock | ~16s (smoke-extrapolated estimate was 10s; revise Phase 4 sizing) |

Holdout fold matches Phase 0 byte-for-byte (677,436 passes across 16 of 22 competitions); the Phase 0→1 NLL comparison is strictly apples-to-apples per Q4/Q6 design locks.

**Diagnostic equality `nll_primary == nll_floorless`** confirms the gaussian kernel never triggered the 1e-10 eps floor — every transition probability is strictly positive within float64 representable range. Validates the Q4 design assumption that gaussian + reasonable bandwidth makes the eps clip dormant.

**Lessons / locked-decision amendments captured during Phase 1 build:**

1. **Bandwidth optimum saturated the upper edge of `[0.01, 2.0]`** — TPE found best at `bandwidth = 1.99998`. Phase 2 should widen the prior (e.g., `[0.01, 5.0]`) to allow further exploration. NOT a Phase 1 blocker (already PASS); recorded as a Phase 2 follow-up.
2. **`(m > 0).all()` for gaussian KDE is float64-wrong** — gaussian density is mathematically unbounded but underflows to 0.0 at destinations many bandwidths from training events. Test rewritten to assert positive density at *observed* destinations only. Pattern worth carrying forward to Phase 2's KNN tests.
3. **Adaptive widening must be tested on the helper, not the matrix** — original entropy-of-row comparison conflated bandwidth-widening with data-spread effects (a row with 500 events naturally spans more zones than a row with 5, even at the same bandwidth). Replaced with a direct property test of `silverman_2d`. Same lesson applies to Phase 2's KNN-K-vs-distribution-spread tests.
4. **`cosine` is a valid sklearn kernel** — `KDESmoothedTransition.__init__` does NOT validate kernel name; defers to sklearn at fit() time. The Optuna axis is the constraint, not the class. Phase 2's `knn_distance` axis should follow the same pattern (constrain at the Optuna layer, not the class layer).
5. **Optuna 4.x removed `MLflowCallback` from core** — needed `optuna-integration[mlflow]>=4.0` added to the `[mlflow]` extra. Phase 2/3/4 inherit this dependency for free.
6. **Per-trial wall-clock 16s, not 10s** — smoke used 5K-row uniform synthetic data; real 8.8M-row clustered data is heavier per BallTree query. Update spec §7 sizing estimates: Phase 4 with 8 axes at 500 trials projects to ~135 min × ~3-5x for the wider search → ~7-12 hr wall-clock. Still feasible on local hardware.
7. **Per-comp NLL improvement was modest and uniform** — no outlier comp where smoothing failed; no outlier comp where smoothing helped enormously. Confirms KDE smoothing is a flat-improvement methodology, not the place to look for big gains. Phase 2's KNN substitution is where structural variance should appear.

**Forward to Phase 2:** branch `feat/ext-v2-phase-2-knn-no-context` (cut after Phase 1 merges to main). Phase 2 stop condition pre-registered per spec §6 row 2: `nll_primary` within 1% relative of Phase 1's 3.74823 — i.e., `3.71 ≤ nll_primary ≤ 3.79`. Below 3.71 → KNN beats KDE (proceed to Phase 3 confidently); above 3.79 → halt + debug. Phase 2 is schema-agnostic; does NOT need Kimball PR 7.

---

## 11. References

- T1 tracker: `docs/research/external-research-tracking.md` — methodology source (KDE+KNN approach, contextual features, channel attribution to author response on LinkedIn 2026-04-25).
- ROADMAP: `ROADMAP.md` § "ExT-style Conditional xT (xT v2 Candidate)" — strategic frame, neural successor as longer-term peer bet.
- TODO: `TODO.md` D66 — original spike authorization.
- Strategic posture: `memory/project_ext_v2_reproduction_posture.md` — Optuna-vs-evolve decision, Kimball-completion dependency analysis, downstream impact.
- **ADR-015**: `docs/superpowers/adrs/ADR-015-xt-pipeline-hardening.md` — XTGrid wrapper + differential validation + workflow card SSOT, the v1 architectural prerequisites for this build phase. Multi-section ADR following ADR-002 pattern.
- **PR #205**: shipped the §2.3 architectural prerequisites; squash-merged to main as `aeca30c` (2026-04-26). Wheel bumped 0.3.14 → 0.3.15.
- **Cycle log**: `memory/project_session57_xt_pipeline_hardening.md` — session-57 cycle history for the v1 hygiene work.
- Codebase research: spike sub-agent reports (original 2026-04-25 conversation; not separately persisted).
- Singh-2018: Singh, "Introducing Expected Threat (xT)," karun.in, 2018-12-15.
- Singh-2018 implementation (post-PR #205): `src/analytics/expected_threat.py` (now exports `XTGrid` + `compute_expected_threat_grid` returning `XTGrid`), `workflow-cards/wf-xt-grids.yaml`.
- Salimi & Salmankhah, "ExT: Improving the Computational Efficiency and Spatial Granularity of the Expected Threat Model," LISS Football Analytics Symposium 2026-04-23 (poster, paper pre-publication).
- Tancik et al., 2020, "Fourier Features Let Networks Learn High Frequency Functions in Low Dimensional Domains" — *not directly relevant to ExT but cited as the precedent for "context as KNN dimensions" thinking from the ScoutGPT cross-attention promotion cycle (PR #166).*
