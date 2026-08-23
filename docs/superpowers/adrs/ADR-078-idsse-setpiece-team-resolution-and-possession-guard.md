# ADR-078: IDSSE set-piece team resolution (silly-kicks) + action-context possession-fill guard

| Field | Value |
|---|---|
| **Date** | 2026-08-22 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The silly-kicks 4.89.0 live AC recompute (Part B of the 4.89.0 adoption, [ADR-077](ADR-077-silly-kicks-4-89-0-full-adoption.md)) crashed 8 `compute_action_context` work-units — all IDSSE period-2, across 4 matches. Root cause chain (evidenced against live bronze, 7 IDSSE matches):

- DFL `FreeKick` events carry `team = 'unknown'` in the main field (all 172 of them); the acting team lives only in the qualifier column `freekick_team` (recoverable to home/away in 172/172). Same for `GoalKick` (`goalkick_team`, 119/119).
- The IDSSE adapter — `silly_kicks.providers.sportec.shape_events_to_native` — resolves set-piece team from a `_TEAM_QUALIFIER_PRIORITY` list that includes `play_team`/`throwin_team`/`foul_team_fouler` but **omits `freekick_team` and `goalkick_team`** (both are in the parser's `_RECOGNIZED_QUALIFIER_COLUMNS`, and the resolver docstring already promises FreeKick/GoalKick). Only the ~4 **direct** freekicks (no nested `play_team`) surface the gap; the rest are masked by `play_team`.
- An unresolved-team SPADL action gets the lakehouse `__UNKNOWN_TEAM__` sentinel (`spadl_adapter.py`). The lakehouse's `analytics.action_context.enrich._fill_possession_from_set_piece_actions` (an [ADR-029](ADR-029-silly-kicks-4-et-direction-adoption.md)/PR-S67 modeling decision) writes that action's team into the frames' `team_in_possession`. silly-kicks' `add_das` then feeds it to accessible_space 2.0.15 `infer_playing_direction`, which unions the frame `team_id` column with `team_in_possession` and hard-raises `ValueError` on a third team — killing the entire applyInPandas unit. accessible_space was bumped this cycle (gkdv DAS), so this strict check is a new raise path landing on a pre-existing sentinel.

## Decision

Fix the root cause **upstream in silly-kicks** — add `freekick_team` + `goalkick_team` to `_TEAM_QUALIFIER_PRIORITY` in `providers/sportec/parse.py` — because the lakehouse deliberately delegates DFL event→SPADL adaptation (including qualifier resolution) to silly-kicks, and the team is fully recoverable from the DFL source. Independently, add a lakehouse **defense-in-depth guard** in `_fill_possession_from_set_piece_actions`: only synthesize `team_in_possession` from a set-piece action whose team is one of the frame's on-pitch player teams, so any *future* genuinely-unresolvable action degrades DAS to NaN instead of crashing a whole work-unit.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Lakehouse possession-fill guard only | Unblocks the drain with a lakehouse-only wheel; hardens the crash class | Leaves the 4 freekick actions with a bogus `__UNKNOWN_TEAM__` team flowing into VAEP/marts; DAS NaN | Insufficient alone — the team is recoverable and must be correct, not merely absent |
| B. silly-kicks qualifier fix only | Correct team at source; matches the adapter's documented contract; recovers 172/172 freekicks | No protection against a future unknown-team action crashing a unit again | Insufficient alone — the sentinel is a *designed* accepted state for genuinely-unresolvable cases |
| C (chosen). B (silly-kicks root fix) **+** A (lakehouse guard) | Correct team at source AND crash-proof against the class; one wheel | Requires a silly-kicks release + pin bump in addition to the lakehouse guard | — |

A lakehouse-side qualifier fix was also rejected: the production IDSSE UDF imports `shape_events_to_native` from silly-kicks; a lakehouse copy of the resolvers existed but was **dead code** (0 references) left from before the logic was upstreamed, and it had already drifted (same missing qualifiers). It is deleted by this change.

## Consequences

### Positive

- IDSSE direct-freekick (and any future no-play goalkick) actions get correct home/away team attribution at the source; `bronze.spadl_actions` produces zero `__UNKNOWN_TEAM__` rows for IDSSE.
- The AC drain can no longer be killed by a single unresolvable-team set-piece action anywhere — the guard degrades that one action's DAS to NaN and the unit completes.
- Removes a dead, drifted lakehouse duplicate of the sportec qualifier resolvers (and its 16 orphan tests) that actively misled diagnosis.

### Negative

- The fix spans two repos: a silly-kicks release (version bump + tag) plus a lakehouse pin bump ([silly-kicks bump sentinel playbook](ADR-046-serverless-env-exact-pins.md)) and wheel bump, then a re-convert of IDSSE SPADL + re-drain of the 4 failed units.
- A genuinely-unresolvable-team set-piece (should one ever exist) now silently yields NaN DAS for that action rather than failing loudly — acceptable because possession is genuinely undefined there, and the sentinel still surfaces in `team_id_native`.

### Neutral

- The guard reads the frames' player `team_id` set; the ball row (team_id NaN) is naturally excluded via `dropna`.

## Related

- **ADRs:** builds on [ADR-029](ADR-029-silly-kicks-4-et-direction-adoption.md) (`_fill_possession_from_set_piece_actions` ownership), [ADR-077](ADR-077-silly-kicks-4-89-0-full-adoption.md) (the recompute that surfaced this), [ADR-046](ADR-046-serverless-env-exact-pins.md) (the sk pin-bump lockstep)
- **External references:** `silly_kicks/providers/sportec/parse.py::_TEAM_QUALIFIER_PRIORITY`; `accessible_space.interface.infer_playing_direction` (2.0.15)
- **Tests:** `src/tests/action_context/test_enrich_helpers.py::test_fill_skips_action_whose_team_is_not_on_the_pitch` (+ `_surgical_`)

## Notes

Evidence (live, warehouse `soccer-analytics-warehouse-dev`): failed run `791329082135550`, 507/515 units succeeded; the 8 failures were idsse p2 of `J03WN1`/`J03WOY`/`J03WQQ`/`J03WR9`, each carrying exactly one `type_id=4` (freekick_short) action with `team_id_native='__UNKNOWN_TEAM__'`. Raw event `18226900000878`: `team='unknown'`, `freekick_team='DFL-CLU-00000B'`, `freekick_execution_mode='direct'`.
