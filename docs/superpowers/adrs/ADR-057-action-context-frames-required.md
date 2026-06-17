# ADR-057: Action-context is a frames-required pipeline (event-only matches out of scope)

| Field | Value |
|---|---|
| **Date** | 2026-06-17 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

After the Kimball slimming (ADR-056), `fct_action_context` is a **pure tracking-derived fact** at the
`(match_id, action_id)` grain: every value column it carries needs a frame (continuous tracking or a 360
freeze-frame). The `event_only` tier — for matches with no frames (wyscout; statsbomb without 360) — existed
only to emit the action-derived columns `game_state`, `gk_role`, `gk_pass_length_m`, `gk_pass_length_class`,
`is_launch`, `gk_xt_delta`, `defending_gk_player_id`. All seven now live in `fct_action_values` (verified in
`fct_action_values.sql`), so the tier's reason to exist is gone.

Concretely, the `event_only` tier produced rows whose only non-NULL content was already served by the actions
fact, leaving ~80 tracking columns 100%-NULL. Such a row is information-negative: a consumer LEFT-joining
`fct_action_values → fct_action_context` can no longer distinguish "tracking exists, value genuinely null here"
from "no tracking for this match at all". It is also pure cost on the worst slice — StatsBomb + Wyscout are
3,400+ matches, exactly the volume that dominates the AC drain `_delta_log` commit-contention stall
(`reference_ac_drain_commit_contention_stall`).

`fct_action_context` is a leaf mart (no other dbt model joins it; the app does not reference it in code; the
remaining consumers — synced-table refresh, index creation, HF publish — are coverage-agnostic), so removing
event-only rows breaks no downstream LEFT-join contract.

## Decision

Action-context is a **frames-required** pipeline: event-only matches do not exist for action-context. The tier
model collapses from `{tracking, sb360, event_only}` to `{tracking, sb360}`; discovery enqueues only
frame-bearing units (tracking per-period + statsbomb matches that have ≥1 `bronze.statsbomb_360` freeze-frame);
the `event_only` concept is deleted end-to-end. "No tracking for this match" is expressed by **row absence**
(the LEFT join from `fct_action_values`), never an empty row.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Make `event_only` write zero rows (edge patches) | Small diff | Leaves the tier enum, `_enrich_event_only_match`, `_process_event_only_match`, `_EVENT_ONLY_PROVIDERS`, the cost tier all present but dead/inconsistent | Debt, not design — neuters the class at three edges instead of removing it |
| B. Keep emitting all-NULL event-only rows | No code change | Destroys the row-absence signal; pure compute/storage cost; feeds the drain-contention path; risks all-NULL void-column inference | Information-negative; violates "never silently substitute" |
| C. Delete the `event_only` tier; frames-required pipeline (chosen) | Single source of truth; absence = no-context; SB/WS exit the drain | Statsbomb-without-360 and wyscout produce no AC rows (by design) | — |

## Consequences

### Positive

- `fct_action_context` coverage is exactly `{idsse, metrica, skillcorner, gradientsports}` + statsbomb matches
  with freeze-frames. Row absence is the unambiguous "no tracking context" signal.
- Wyscout (and statsbomb-without-360) leave the AC drain entirely — relieving the event-only commit-contention
  path.
- `ProviderTier` (`{tracking, statsbomb}`) and `FrameTier` (`{tracking, sb360}`) are distinct `Literal` types
  bridged by one `resolve_frame_tier` function, so a crossed static/runtime tier value is a pyright error at the
  call site rather than a runtime surprise.
- The `add_gk_distribution_metrics` 0-frame / conversion-failure anomaly ("had 360 data but produced 0 frames")
  is now WARN-visible at the production edge (`_process_statsbomb_match`, per ADR-002), distinct from the silent
  out-of-scope "no 360" case.

### Negative

- A statsbomb match without freeze-frames, and every wyscout match, has no `fct_action_context` row. Consumers
  that want `game_state`/GK columns for those matches must read `fct_action_values` (they already can).
- Existing event-only rows in `bronze.spadl_action_context` must be deleted on the next recompute (operational,
  no schema change).

### Neutral

- Production `_process_*_match` (drain) and the hexagonal `enrich_batch`/`run_work_unit` mirror are changed
  together; their lockstep is enforced by existing tests.
- The statsbomb discovery semi-join canonicalizes the id key as `cast(long→string)` on all three sides to
  normalize the `"366.0"` vs `"366"` float-format class (ADR-019). **Bronze id dtypes (recorded per review
  L-new-1):** StatsBomb match ids are integers; a real-dtype set-equality probe
  (`test_sb360_discovery_id_join_is_dtype_safe`) backstops the cast. A live `DESCRIBE bronze.statsbomb_360` +
  `DESCRIBE bronze.spadl_actions` to confirm both columns are numeric is on the operational checklist before the
  recompute; if either is a non-numeric/hash id, the cast switches to the `src/shared/identifiers.py`
  canonicalizer.

## Related

- **Specs / plans:** `docs/superpowers/plans/2026-06-17-action-context-frames-required-pipeline.md`
- **ADRs:** extends `ADR-039` (SB360 coverage) and `ADR-056` (AC Kimball slim); references `ADR-037`
  (worker-drain fan-out), `ADR-028` (hexagonal compute), `ADR-019` (id-dtype contract), `ADR-002` (no silent
  swallow), `ADR-033` (explicit createDataFrame schema).
- **External references:** none.

## Notes

Chesterton's-fence sweep (plan Phase 0) confirmed the deletion surface had no unlisted code consumers beyond the
one it caught — `action_context_queue.py::DrainProcessor.process` (the live drain dispatch) — which was folded
into the surface. The fence proof (all 7 carried columns present in `fct_action_values`) and the
leaf-mart/LEFT-join consumer check both passed before any deletion.
