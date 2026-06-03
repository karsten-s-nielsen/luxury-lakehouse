# ADR-039: action-context GK metrics (xShotOccurrence + gk_influence zones) + SB360 freeze-frame coverage + pitch_control_method provenance

| Field | Value |
|---|---|
| **Date** | 2026-06-03 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

silly-kicks 4.9.x completed the goalkeeper-defensive-value (GKDV) feature line: 4.9.0 shipped the
**trained xShotOccurrence (xS, TF-16)** model — the xS sub-model of **Pipping-Gamón, Feng & Sabin
(2026), arXiv:2512.00203, "Beyond Expected Goals: A Probabilistic Framework for Shot Occurrences in
Soccer"** (weights bundled as `silly-kicks/xshot-occurrence-v1`) — plus the `gk_influence` per-zone
API; 4.9.1 added a DAS empty-frame-batch fix. The action-context pipeline (AC-1, ADR-028) did not
consume xS, ran `add_gk_influence` with its default single zone (`six_yard_box`), and ran only 6 of its
~20 enrichment steps on the StatsBomb-360 (SB360) freeze-frame path.

An empirical probe (real hexagon `run_work_unit` on a committed SB360 fixture) measured exactly which
tracking-only enrichments the freeze-frame data supports. The contract is one synthetic frame per
action, positions only — **no velocity, no temporal sequence, only the visible players**. Result:
single-frame positional metrics work; velocity-dependent (DAS) and temporal (actor-pre-window,
off-ball, space-creation, elastic-sync) metrics do not; and the pitch-control-dependent metrics
(`gk_influence`, OBSO, PAUSA) work **only** with `method='voronoi'` (position-only) — `spearman`
(velocity-aware) returns all-NaN on freeze-frames.

## Decision

1. **GK metrics (tracking path):** persist `xshot_occurrence` (`add_xshot_occurrence`, bundled default,
   `model=None`) and the full `gk_influence` zone set (`zone_names=["six_yard_box","near_post",
   "far_post"]` → `gk_closing_time_{mean,min}_s__{near,far}_post`) on `fct_action_context`. xS follows
   the ghost-GK precedent (an enrichment column on AC-1, computed inline; not a separate ADR-013 model
   mart). `xgboost-cpu==3.2.0` (already in the analytics env) satisfies xS's runtime `import xgboost`.
2. **SB360 coverage expansion:** wire every empirically-supported step into `_enrich_sb360_match` —
   `add_pressure_on_actor`, `add_shape_graph`, `add_ghost_gk`, `add_gk_influence` (**voronoi**, zones),
   `add_obso`/`add_pausa` (**voronoi**), `add_xshot_occurrence`. All partial/sparse (honest NULL where
   the freeze-frame lacks the needed players). Excluded (measured all-NaN / structurally impossible):
   `add_das`, `add_cover_shadows`, `add_pre_shot_gk_position/angle`, and the temporal metrics.
3. **`pitch_control_method` provenance column (the cross-provider value-format contract):** SB360 writes
   the pitch-control-derived metrics (`obso_*`, `pausa_*`, `gk_influence`, `gk_closing_time_*`,
   `gk_pitch_control_share_weighted`, `gk_reachable_area_m2`) computed with **voronoi** into the *same*
   named columns the tracking path fills with **spearman**. To avoid a silent cross-provider estimator
   divergence (ADR-018 spirit; the "never silently substitute" UX rule), a per-row
   `pitch_control_method STRING` records the method: `'spearman'` (tracking) / `'voronoi'` (SB360) /
   NULL (event-only). The divergence is queryable, not buried in prose.
4. **silly-kicks 4.9.1:** floor advanced `>=4.9.0,<5` → `>=4.9.1,<5` across all consumers (pyproject
   `[spadl]`, Terraform analytics env, 6 trainer `_REQUIRED_SK_MIN=(4,9,1)`, the orchestrator-invariants
   sentinel, `submit_ac1_oneshot`, `sk3_mig_b_retrain`) to adopt the DAS empty-frame-batch fix (guards
   accessible-space's `None` `simulation_result` on a zero-frame subset — the GS-10502 crash class).
5. **Migration operator-applied:** the bronze `ALTER TABLE ... ADD COLUMNS (6)` is applied operator-side
   post-merge (the documented fallback) — the `dbt-live-ci.yml` migration-runner step was removed when
   that workflow moved to a daily schedule (the diff-based step is a silent no-op on scheduled runs).
   Re-wiring the runner to a push/PR-triggered, warehouse-capable workflow is a **separate PR**.

Wheel 0.5.14 → 0.5.15.

## Alternatives considered

| Option | Why rejected |
|---|---|
| Don't persist SB360's velocity/pitch-control metrics (only the method-invariant ones) | Forgoes real, supported coverage (gk_influence/obso/pausa via voronoi populate 36–95% of the GK-present subset); the user's "take advantage of any available metric" steer |
| Rely on `data_source='statsbomb'` to imply voronoi (no provenance column) | `data_source` perfectly discriminates today, but the coupling is implicit (Hyrum's Law) — a consumer must *know* statsbomb⇒voronoi. The explicit column removes the tribal knowledge |
| Separate `*__voronoi` column names for SB360 | Explodes the schema and breaks the "same metric, best estimator for the data" intent; the provenance column is the lighter contract |
| Re-add the diff-based migration step to `dbt-live-ci.yml` verbatim | It is a silent no-op on a scheduled run (`origin/main...HEAD` empty) — worse than the honest gap; defer to a properly-designed CI re-wiring PR |

## Consequences

### Positive
- AC-1 gains xShotOccurrence + complete `gk_influence` zone granularity on all tracking providers.
- SB360 freeze-frame matches gain pressure, shape_graph (defending), ghost_gk, gk_influence (voronoi),
  OBSO, PAUSA, and xS — previously all-NULL.
- `pitch_control_method` makes the SB360/tracking estimator divergence queryable; HF publishing
  auto-includes all new columns (`SELECT *`, Hive-partitioned by `data_source`).

### Negative
- SB360 metrics are **partial/sparse** (each populates only the freeze-frame subset with the needed
  players; `xshot_occurrence` ~4% on SB360) and computed with a **different pitch-control estimator** than
  tracking — documented in the dataset card + this ADR; consumers must segment on `pitch_control_method`
  and not compute naive provider-level averages.
- One extra STRING column (`pitch_control_method`) — the first non-DOUBLE feature column; the DDL is the
  single source (`_parse_ddl_to_struct_type` handles STRING) so no separate StructType change.

### Neutral
- Extends the AC-1 silly-kicks lineage (ADR-035 ghost-GK, ADR-036 DAS golden) and the cross-table
  value-format-contract discipline (ADR-018). xS is an enrichment column, not a standalone per-player
  evaluative system — `wf-action-context` stays out of `PER_PLAYER_EVALUATIVE_CARDS` (consistent with
  ghost_gk/gk_influence/obso/pausa already on the table); no new HF model card (the xS model is
  silly-kicks-bundled).

## Related

- **Spec:** `docs/superpowers/specs/2026-06-03-action-context-gk-metrics-and-sb360-coverage-design.md`
- **Plan:** `docs/superpowers/plans/2026-06-03-action-context-gk-metrics-and-sb360-coverage.md`
- **ADRs:** extends ADR-028 (hexagon), ADR-035 / ADR-036 (AC-1 silly-kicks lineage); applies ADR-018
  (cross-table value-format contracts) for `pitch_control_method`; references ADR-013 (ML inference
  outputs — xS follows the ghost-GK enrichment-column path, not a standalone mart) and ADR-023 (the
  retired xG-v1 orphan, unrelated to xS).
- **External:** Pipping-Gamón, Feng & Sabin (2026), arXiv:2512.00203; silly-kicks 4.9.1 release notes
  (DAS empty-frame-batch fix — the GS-10502 crash class investigated upstream).

## Notes

The GS-10502 DAS crash (accessible-space dereferencing a `None` `simulation_result` on an empty frame
subset) was diagnosed locally via the hexagon and handed to the silly-kicks session; the fix shipped in
silly-kicks 4.9.1, adopted here. The SB360 supportability tiers were measured empirically
(`tmp/sb360_tierb_probe.py`), and the voronoi lever — `gk_influence` flips from 0 to 610/1760 non-null
when `method='voronoi'` is passed — was the decisive finding that made SB360 gk_influence viable.
