-- fct_off_ball_runs.sql
-- Gold-layer off-ball runs (silly-kicks 4.87.0 TF-4/TF-35; spec §7.5, Task 17e).
-- Grain: one row per (action, runner). The Python writer
-- (ingestion.off_ball_runs_writer) emits only native identifiers + the run
-- geometry/value columns to bronze.off_ball_runs (ADR-013); this mart resolves
-- the Kimball surrogates:
--   * match_key / competition_key / team_key  <- INNER JOIN the AC identity fact
--     fct_action_values on the native shot-action key (data_source, match_id,
--     action_id). team_key is the ACTING team, which is the runner's team
--     (detect_off_ball_runs candidacy = same team as the actor).
--   * player_key (the RUNNER, not the actor) <- LEFT JOIN dim_players on
--     (provider, native_player_id). LEFT because a runner without a dim_players
--     row (a tracking-only id) still ships as a row with NULL player_key.
--
-- value_off_ball_runs values ONLY completed passes/crosses with a resolved
-- receiver, so run_value / role are legitimately NULL on most rows (ADR-033) —
-- the null-rate is by design, not a data gap.

{{ config(
    materialized='table',
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true},
    tags=['marts', 'output_mart']
) }}

with runs as (

    select * from {{ ref('stg_off_ball_runs') }}

),

identity as (

    select
        data_source,
        match_id_native,
        action_id,
        match_key,
        competition_key,
        team_key
    from {{ ref('fct_action_values') }}

)

select
    i.match_key,
    r.action_id,
    dp.player_key,
    i.team_key,
    i.competition_key,
    r.data_source,
    r.period_id,
    r.player_id_native,
    r.run_start_x,
    r.run_start_y,
    r.run_end_x,
    r.run_end_y,
    r.displacement_m,
    r.duration_s,
    r.mean_speed_ms,
    r.peak_speed_ms,
    r.peak_speed_source,
    r.toward_goal,
    r.role,
    r.is_receiver,
    r.run_value,
    r.enabled_pass_credit

from runs r
inner join identity i
    on  i.data_source = r.data_source
   and i.match_id_native = r.native_match_id
   and i.action_id = r.action_id
left join {{ ref('dim_players') }} dp
    on  dp.provider = r.data_source
   and dp.native_player_id = r.player_id_native
