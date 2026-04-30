# PR-LL3 — Deferred Scope Tracker

This document is the durable inventory of items deferred from PR-LL1 + PR-LL2
+ PR-LL2 Path B close-out. PR-LL3 will work from this list. Items are added
when deferred and removed when shipped.

Last updated: 2026-04-29 (PR-LL2 Path B close-out spec).

## Why a scope tracker

Three PR-LL waves shipped between 2026-04-28 and 2026-04-29. Each carried
deferred items into the next. Without an explicit tracker, deferred items
get forgotten between sessions (verified — the 4-source SPADL coverage
deferred from PR-LL1 was a near-miss). This file is the single index
PR-LL3 reads from.

## Scope

### S1 — Per-player Kimball mapping (from ADR-016)

`bronze.spadl_actions.player_id_native` STRING column not yet emitted.
`dim_players.native_player_id` exists for SB/WS but resolves NULL for
IDSSE/Metrica. Mart-level `not_null` tests on `player_id`, `team_id`,
`vaep_value`, `offensive_value`, `defensive_value` carry a
`where: data_source IN ('statsbomb', 'wyscout')` scope filter as a
PR-LL3 placeholder.

**Exit criteria:** IDSSE + Metrica `player_id_native` populated end-to-end;
mart-level filter dropped; full-coverage `not_null` tests pass.

### S2 — IDSSE/Metrica `player_id_native` on `bronze.spadl_actions` (from ADR-016)

Companion to S1. silly-kicks 1.7.0 sportec/metrica converters carry
caller's `player_id` verbatim (per ADR-001), so propagation is just a
matter of populating bronze.idsse_events + bronze.metrica_events with
`player_id_native` and surfacing through the SPADL UDF.

### S3 — silly-kicks sportec event coverage gap (from ADR-016)

silly-kicks 1.7.0 sportec converter still drops `CornerKick` and
`OtherBallAction` event types. ~16% of bronze.idsse_events fall into
these buckets and don't reach the SPADL action stream. Either close at
silly-kicks (preferred) or document as known data-fidelity concern in
the IDSSE UDF code.

### S4 — IDSSE/Metrica `game_state` derivation switch to `team_id_native` (from ADR-016)

`fct_action_values.game_state` currently derives via legacy `team_id`
BIGINT comparison against `int_running_score.home_team_id`. NULL for
IDSSE/Metrica because legacy team_id is NULL there. Switch to
`team_id_native` comparison once `int_running_score.home_team_id_native`
exists.

### S5 — DRY the SPADL UDFs (from PR-LL2 Path B close-out, F8)

Currently 4 SPADL UDFs (StatsBomb / Wyscout / IDSSE / Metrica) duplicate
~80% of post-conversion logic: NULL-fill statsbomb_*, dtype casts on
enrichment columns, populate the 5 `_native` columns, etc. Each UDF is
~120 lines; ~80 lines per UDF are shared.

**Refactor target:** extract to `src/ingestion/spadl_udf_shared.py`:
- `apply_path_b_native_columns(actions, *, source, native_meta) -> pd.DataFrame`
- `apply_enrichments_and_cast(actions, *, source) -> pd.DataFrame`
- `null_fill_statsbomb_columns(actions, *, n) -> pd.DataFrame`

Per-source UDFs become ~30 lines each. Single point of update for new
columns. New ADR captures the pattern.

### S6 — Source-onboarding contract test class (from PR-LL2 Path B close-out, F9)

A parametrized `@pytest.mark.parametrize("source", [...])` test class
covering required invariants per source (native ID format, dim join
coverage, enrichment column population, etc.). Adding a new SPADL source
becomes "parametrize the source name in this test class" rather than
"hope you remembered all the invariants."

### S7 — Type-safe identifier dataclasses (from PR-LL2 Path B close-out, F10)

`src/shared/identifiers.py` (introduced in PR-LL2 Path B close-out) ships
as functions returning strings. Long-term Pydantic NamedTuple wrappers
would fail at construction time on format errors and provide Python-side
type safety throughout the pipeline. YAGNI for PR-LL2 close-out (the
functions catch format errors at the boundary already); revisit if
identifier-related bugs persist.

### S8 — bronze.idsse_events period coverage extension (from PR-LL2 Path B close-out, Bug #6)

The 2-pass parser refactor in PR-LL2 close-out fixes the period 2
misclassification class. `_SECTION_TO_PERIOD = {"firstHalf": 1,
"secondHalf": 2}` only covers 90 minutes. ET periods (`extraTimeFirstHalf`,
`extraTimeSecondHalf`) and penalties (`penaltyShootout`) are not
recognized. No 7-match IDSSE collection rows touch these periods today,
but adding a future Bundesliga match with ET would silently truncate
events. Extend `_SECTION_TO_PERIOD` and update period inference logic.

## Promotion log

When an item ships, move its bullet here with the PR / commit reference.

## Cross-references

- ADR-016 — SPADL post-conversion enrichment stage and canonical/native naming
- ADR-018 — Cross-table format-contract testing (introduced PR-LL2 close-out)
- `docs/superpowers/specs/2026-04-29-pr-ll2-path-b-close-out-design.md`
