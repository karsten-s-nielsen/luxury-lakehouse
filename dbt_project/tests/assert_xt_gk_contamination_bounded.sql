-- Regression guard for the mart-level GK-contamination exclusion on fct_action_context
-- (2026-07-01 handoff). The mart NULLs the xt_gk value family for any match where a
-- (match, team) carries more distinct xt_gk-scored players than
-- var('xt_gk_max_scored_players_per_team') — the whole-squad contamination class that is
-- only visible cross-batch at the mart. AFTER that exclusion, NO SERVED (non-NULL xt_gk)
-- (match, team) may exceed the bound; it holds by construction, so any returned row means
-- the guard logic regressed (or the threshold drifted from the mart var). Fail-safe: better
-- to catch a served-contamination regression loudly than to ship silently-wrong keeper metrics.
--
-- Daily-live guard only: dbt PR CI is parse-only (Thrift unreachable from GH runners), so the
-- mart SQL carrying the guard is additionally asserted at merge time by
-- src/tests/test_action_context_gk_guard.py. See reference_dbt_ci_parse_only_tests_daily.
{{ config(severity='error') }}

with served_scored as (

    select
        match_key,
        team_key,
        count(distinct player_key) as n_scored_players
    from {{ ref('fct_action_context') }}
    where xt_gk is not null
    group by match_key, team_key

)

select
    match_key,
    team_key,
    n_scored_players
from served_scored
where n_scored_players > {{ var('xt_gk_max_scored_players_per_team', 4) }}
