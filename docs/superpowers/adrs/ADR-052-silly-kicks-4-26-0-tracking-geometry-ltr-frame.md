# ADR-052: silly-kicks 4.26.0 adoption — tracking-geometry action-LTR frame unification

| Field | Value |
|---|---|
| **Date** | 2026-06-13 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

silly-kicks emits SPADL actions LTR-normalized (the team in possession always attacks toward x=105), but the tracking-geometry enrichments (`add_pre_shot_gk_position`, `add_pre_shot_gk_angle`, `add_ghost_gk`, `add_defensive_line`, the team-shape centroids) emitted player/line/centroid positions in the **absolute per-period frame** (`convert_to_frames` is per-period-absolute, ADR-029). Within a single action row, LTR action coordinates were mixed with absolute-frame tracking geometry, so for ~50% of rows (the half where the attacking team physically attacks toward the absolute-left goal) every position feature landed at the wrong end of the pitch.

The lakehouse diagnosed this empirically (2026-06-12): `pre_shot_gk_x` was a near-perfect 50/50 bimodal (912 at x≈10 vs 931 at x≈100, empty middle); `pre_shot_gk_distance_to_goal` reached a physically-impossible 93–107 m; the GK/defending-centroid/defensive-line x's moved together (~97% concordance), confirming one shared absolute frame decoupled from the (correct) LTR action coordinates. The defect was reported upstream (`tmp/silly_kicks_tracking_geometry_ltr_frame_20260612.md`). PR #376 (ADR-051) shipped a **page-side reconciliation macro** (`dbt_project/macros/gk_tracking_geometry.sql`) as a temporary workaround, explicitly flagged: *"REVISIT when the upstream AC coordinate convention is unified — this macro is the single change site."*

silly-kicks **4.26.0** unifies every per-action geometry output into the action LTR frame. This is the upstream fix the workaround was waiting for. The change is **value-only / schema-stable** (no columns added/removed — verified via `result.columns == golden.columns` in `test_mini_golden`).

## Decision

Adopt silly-kicks **4.26.0** as the floor everywhere (wheel **0.5.38**); regenerate both AC-1 goldens AND the legacy `fct_tracking_context` oracle so the frame-fixed values become the validated baseline.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep only the PR #376 page-side workaround | No upstream bump; UI already compensates | Live AC/tracking-context data stays wrong-framed; every new consumer must re-mirror; residual-matching heuristic is fragile | Band-aid; doesn't fix the data |
| B. Adopt 4.26.0 but mark the ~12 diverging columns `known_divergence` in the differential oracle | Fast; pattern-consistent with DAS/xT | Hollows out the differential test's geometry coverage; the legacy oracle stays buggy (nearest_defender 93 m) | Reduces a safety net to hide a fixable staleness |
| C. **Adopt 4.26.0 + regenerate the legacy oracle (chosen)** | Geometry physically correct (nearest_defender 93 m → 28 m); differential keeps full assertion coverage; #376 macro becomes removable | Requires recomputing the live `fct_tracking_context` mart under 4.26.0 + re-extracting the oracle (deploy-gated) | — |

## Consequences

### Positive

- GK / defensive-line / team-centroid / ghost-GK geometry is now expressed in the action LTR frame and physically correct: on the IDSSE J03WMX anchor `nearest_defender_distance` drops from the legacy oracle's impossible max **93.0 m** to **28.2 m** (median 3.9).
- The PR #376 `gk_tracking_geometry.sql` workaround macro becomes **removable** — its single-change-site contract is now satisfiable.
- Position-dependent features corrupted by the frame mismatch (pressure_on_actor, receiver_zone_density, defenders_in_triangle) are corrected.

### Negative

- **Value-changing release**: a full action-context recompute AND a full `fct_tracking_context` recompute under 4.26.0 are required to propagate the fix to live marts. As of this ADR only J03WMX p1 has been recomputed (for the oracle); all other matches still hold old-frame geometry.
- **Lockstep removal required**: the PR #376 `gk_tracking_geometry.sql` macro must be removed/neutralized **in the same change as the full live recompute**, or it will double-correct the now-fixed live geometry and break the GK Analytics page.
- The committed IDSSE `J03WMX/actions.parquet` fixture is stale relative to current bronze (pre-IDSSE-re-conversion); a full fixture refresh is deferred (would require updating `test_xt_gk`'s mini-window assumption).

### Neutral

- Schema-stable (value-only) — no bronze migration, no dbt contract change (contrast ADR-050's column-lean rename).
- 4.26.0 was deployed to **dev** ahead of commit (UC Volume wheel 0.5.38 + targeted `terraform apply module.workflows`) to recompute `fct_tracking_context` for the oracle re-extract; CI redeploys idempotently on merge.

## Related

- **Issues / PRs:** PR #376 / ADR-051 (the page-side workaround this fix retires)
- **ADRs:** follows `ADR-050` (4.25.0 adoption); references `ADR-046` (serverless exact pins), `ADR-029` (per-period-absolute converters), `ADR-013` (ML-mart Kimball resolution)
- **External references:** silly-kicks 4.26.0; lakehouse handoff `tmp/silly_kicks_tracking_geometry_ltr_frame_20260612.md`
- **Wheel:** 0.5.37 → 0.5.38 (`bump_wheel.py`, 28 files)

## Notes

Mini-golden delta confirming the fix (the exact columns predicted in the handoff): `ghost_gk_x` maxd 68.1, `team_shape_centroid_x_attacking` 29.9, `defensive_line_x` 9.6, plus `ghost_gk_y`/centroid_y and the position-dependent features — all geometry, schema unchanged. The differential's legacy oracle was regenerated (not marked divergent) by recomputing `fct_tracking_context` for J03WMX p1 under 4.26.0 and re-extracting via `scripts/extract_action_context_fixture.py`.
