# ADR-010: Match-absolute minute convention for `fct_action_values`

| Field | Value |
|---|---|
| **Date** | 2026-04-19 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

During the Match Summary redesign (2026-04-19, `docs/superpowers/specs/2026-04-19-match-summary-redesign-design.md`) a user-visible discrepancy surfaced: the same goal event — Anthony Martial's strike in Manchester United 1-0 Everton (2016-04-03) — displayed as minute **53'** on the xG race chart (Row 2, driven by `fct_shots`) and as minute **8'** on the Big Story decisive-moment card (Row 1, driven by `fct_action_values`). Bronze `statsbomb_events` records the shot at period 2, minute 53.

Root cause: two gold marts derived from the same underlying event use different minute conventions.

- `fct_shots.minute` is **match-absolute**, inherited from `stg_statsbomb__shots.minute` which inherits from bronze `statsbomb_events.minute` (StatsBomb's own convention — a period-2 event at clock 8 reads as 53).
- `fct_action_values.minute` is **period-local**, computed in `stg_spadl__action_values` as `floor(time_seconds / 60)` where `time_seconds` is the SPADL academic convention (seconds since kickoff of the *current* period). A period-2 event at SPADL time 480s becomes `minute=8`.

Because the SPADL conversion pipeline strips match-absolute minute information down to period-local seconds, the two marts disagreed by the period offset (45 / 90 / 105 depending on the period).

Empirical verification (2026-04-19) across bronze, `fct_shots`, and `fct_action_values` confirmed the split: bronze and `fct_shots` agree on minute 53; `fct_action_values` has minute 8 for the same event-id. The same pattern repeats for every period-2+ action across the 9.5M-row `fct_action_values` table.

Additional pre-existing defect: `fct_action_values.sql`'s running-score JOIN compares `rs.minute*60 + rs.second` to `av.minute*60 + av.second` within the same period. `int_running_score` derives from `int_unified_shots`, which keeps match-absolute minutes for both StatsBomb and Wyscout inputs. The `av` side being period-local made the same-period clause numerically false for every period-2+ action, silently losing period-2 score updates when deriving `game_state`.

## Decision

`fct_action_values.minute` (and by extension every mart downstream of it — `fct_gk_actions_detail`, `fct_goalkeeper_stats`, `fct_vaep_breakdown_agg`, `fct_funnel_stages_agg`, `fct_player_stats`) stores **match-absolute minute**. The conversion from SPADL period-local `time_seconds` to match-absolute minute lives in `stg_spadl__action_values.sql` via a `case period_id` offset (0 / 2700 / 5400 / 6300 / 7200 seconds for periods 1–5). All `minute` columns in the gold layer now share a single semantic.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Do nothing, document as known quirk | Zero risk, zero work | Two gold marts disagree on a user-facing field; forces every new page that shows a minute to pick which mart to trust; makes future joins and downstream models fragile | Cost compounds with every new page and every new mart |
| B. Fix in `fct_action_values.sql` rather than `stg_spadl__action_values.sql` | Change the mart, leave the staging model alone | Hides the conversion one layer deeper; `stg` has an SPADL-convention time field and a non-SPADL minute field that disagree; confusing for anyone who `select *` the staging view | Staging should match the convention it claims to standardise on |
| C. Fix in `stg_spadl__action_values.sql` (chosen) | Single, source-of-truth conversion at the staging boundary; downstream marts pass through; `time_seconds` remains period-local (SPADL standard) while `minute` becomes match-absolute (platform standard) | Requires `--full-refresh` of an incremental 9.5M-row gold table and a Lakebase resync of ~6 downstream synced tables | — |
| D. Add a new `match_minute` column, leave `minute` as-is | Non-breaking for any consumer that reads `minute` | Creates two columns that mean different things; future consumers must remember which is which; doubles the mental model | Pollutes schema for compatibility we don't need (confirmed zero consumers filter on period-local minute) |

## Consequences

### Positive

- `fct_action_values.minute` and `fct_shots.minute` are now interchangeable for any match event — Match Summary Row 1 and Row 2 show identical times; future pages joining the two marts don't need a per-period correction.
- Running-score JOIN in `fct_action_values.sql` silently becomes correct for period-2+ actions: both sides of the `(rs.minute * 60 + rs.second) <= (av.minute * 60 + av.second)` comparison are now match-absolute, so `game_state` derivation no longer loses second-half goals. This fixes a pre-existing defect without a separate change.
- Schema contract description on `minute` now names the convention explicitly, making it testable.
- New dbt singular test (`tests/assert_fct_action_values_minute_match_absolute.sql`) catches any regression to period-local in CI.
- New integration test (`src/tests/test_dbt_fct_action_values_minute.py`) pins the Martial goal as the canonical anchor — an obvious failure mode if a future SPADL version changes `time_seconds` semantics.

### Negative

- One-off `dbt build --full-refresh --select fct_action_values+` needed to backfill the 9.5M rows. Incremental strategy picks up the new SQL but does not rewrite existing rows, so a full rebuild is required.
- Full refresh of every downstream mart (`fct_gk_actions_detail`, `fct_goalkeeper_stats`, `fct_vaep_breakdown_agg`, `fct_funnel_stages_agg`, `fct_player_stats`) and resync of the corresponding Lakebase synced tables.
- User-visible minute values change in any page that showed period-local values. Audited the `hf_taipy_app/src/` tree (2026-04-19): no page filters on `minute` or formats it expecting period-local; the only consumers display it via format strings that are agnostic to the range.

### Neutral

- `time_seconds` remains period-local (SPADL academic convention). Keeping both is useful: `time_seconds` for SPADL-native computations that need per-period timing; `minute`+`second` for display and cross-mart joins.

## CLAUDE.md Amendment

None.

## Related

- **Commits:** (this commit) on `refactor/fct-action-values-absolute-minute`
- **Specs:** `docs/superpowers/specs/2026-04-19-match-summary-redesign-design.md` (the design that surfaced the discrepancy)
- **Issues / PRs:** follow-up to the Match Summary redesign on `ui/match-summary-redesign`
- **ADRs:** n/a
- **External references:** SPADL paper — Decroos et al. 2019, "Actions Speak Louder than Goals" (ACM KDD) — defines `time_seconds` as period-local.

## Notes

Empirical evidence used to make the decision (2026-04-19, match 3754299):

- Bronze `statsbomb_events` for Martial's goal: `(minute=53, second=2, period=2, shot_outcome='Goal', statsbomb_xg=0.47)`.
- `fct_shots` for the same event: `(minute=53, second=2, period=2, is_goal=1, statsbomb_xg=0.47)`.
- `fct_action_values` pre-fix for the same event: `(minute=8, second=2, period=2, action_type='shot', action_result='success', vaep_value=+0.99, time_seconds=482)`.

The period offset of 45 minutes is exact: 53 − 45 = 8. `time_seconds=482` is 8 min 2 s period-local. Post-fix: `minute = floor((2700 + 482) / 60) = floor(3182/60) = 53`. `second = floor(482 % 60) = 2` (unchanged).
