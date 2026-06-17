# ADR-056: pitch_control at_target rename, DEFCON live pitch-control, AC Kimball slim, single-source gk_xt_delta

| Field | Value |
|---|---|
| **Date** | 2026-06-16 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

silly-kicks 4.31.0 renamed the pitch-control feature from `pitch_control_at_ball__<method>` to
`pitch_control_at_target__<method>` (silly-kicks ADR-032): the value is now sampled at the action
DESTINATION `(end_x, end_y)` with the ADR-028 away-team query re-projection, retiring the dead ~0.5
near-ball fallback for a live at-destination feature. Adopting 4.31.0 is the window to also resolve
several foreseeable action-context modeling decisions in ONE tracking-recompute window (the user's
governing principle: never re-recompute action-context for columns we could have predicted).

Three other issues surfaced in the same window: (1) `fct_defcon_actions.pitch_control_at_action` was a
hardcoded constant `0.5` — `assign_defensive_credits` was called with no `pitch_control_fn`; (2)
`fct_action_context` had accreted action-derived columns (`game_state` + the GK action-sequence flags)
that are frame-INDEPENDENT and duplicate what the actions fact serves; (3) the new GVM
`add_gk_distribution_metrics` emits a `gk_xt_delta` binned on silly-kicks' OWN xT grid, which would be a
second, competing xT source of truth alongside the lakehouse's canonical `bronze.expected_threat_grids`.

## Decision

Adopt the `pitch_control_at_ball → pitch_control_at_target` rename atomically with the silly-kicks pin
bump; wire a LIVE event-frame pitch-control function into DEFCON; slim `fct_action_context` to a pure
tracking-derived fact (remove the action-derived columns, served instead by `fct_action_values`); and
derive `gk_xt_delta` in dbt from the lakehouse's canonical xT grid — never a second silly-kicks grid.
`fct_action_values` and `fct_action_context` are two facts at the SAME action grain `(match_id,
action_id)`; consumers JOIN, never duplicate.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep `pitch_control_at_ball` (dead ~0.5) | No migration | Dead constant masquerading as a feature; name never matched semantics | The 4.31.0 window makes the rename + live value free of a second migration |
| B. Leave DEFCON's `pitch_control_at_action` constant 0.5 | No code | A credit-assignment input is a dead constant | Wire the real per-frame surface (`_event_pitch_control_fn`) — validated non-0.5 (0.56/0.27) |
| C. Keep action-derived columns in `fct_action_context` | No mart change | Duplicates `fct_action_values`; couples AC recompute to actions-level logic; misleads consumers | Slim to a pure tracking fact (Kimball "join don't duplicate") |
| D. Persist silly-kicks' `gk_xt_delta` grid | Drop-in | A second xT source of truth that drifts from `expected_threat_grids` | Derive in dbt from the canonical grid (single source) |
| E. (chosen) rename + DEFCON-live + slim + dbt-derived gk_xt_delta | One window; one source of truth; lean leaf mart | RENAME/DROP bronze migration is run-once | — |

## Consequences

### Positive

- `pitch_control_at_target__<method>` is a live at-destination feature (no dead constant); DEFCON's
  `pitch_control_at_action` is a real per-frame value.
- `fct_action_context` is a pure tracking-derived fact (RESULT_COLUMNS 139 → 135): `game_state` +
  `gk_was_distributing` / `gk_was_engaged` / `gk_actions_in_possession` removed (served by
  `fct_action_values`). `defending_gk_player_id_native` is KEPT (load-bearing — resolves
  `defending_gk_player_key`). This sets up the frames-required pipeline (ADR-057).
- `add_shot_goalmouth` (TF-48, Anzer & Bauer 2021) adds 11 tracking columns to `fct_action_context`
  ONLY (post-contact leakage → NOT a VAEP feature).
- `add_gk_distribution_metrics` (Lamberts 2025 GVM) adds 3 grid-free columns
  (`gk_pass_length_m`, `gk_pass_length_class`, `is_launch`) to the `fct_action_values` lineage
  (actions-level, via `spadl_enrichments`); `gk_xt_delta` is derived in `fct_action_values` via a JOIN
  to the canonical `bronze.expected_threat_grids` (single xT source of truth).

### Negative

- Bronze migration is run-once (RENAME pitch_control ×3 + DROP the 4 slimmed columns) — operator-applied
  (`scripts/migrations/2026-06-17-action-context-pitch-control-target-shot-goalmouth-and-slim.sql`).
- A full tracking-provider AC recompute is required to repopulate the renamed + new columns.

### Neutral

- `gk_pass_length_class` is a pandas `category`; coerced to `object`/Spark `StringType` before write.
- `pitch_control_at_target` adoption must be A/B-checked against augmented-VAEP Brier (dead-constant →
  live can regress as easily as help).

## Related

- **ADRs:** extended by `ADR-057` (frames-required pipeline); references `ADR-028` (hexagonal compute),
  `ADR-013` (ML-inference mart pattern), `ADR-039` (SB360 coverage).
- **External references:** silly-kicks ADR-032 (`pitch_control_at_target`); Anzer & Bauer (2021) shot
  goalmouth (TF-48); Lamberts (2025) Goalkeeper Value Model.
