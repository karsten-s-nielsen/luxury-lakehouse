{{ config(
    materialized='table',
    liquid_clustered_by=['player_id'],
    tags=['marts', 'output_mart']
) }}
-- fct_goalkeeper_stats.sql
-- Per-match goalkeeper statistics combining saves, claims, distribution xT,
-- and PSxG metrics.
--
-- Grain: one row per goalkeeper per match per data_source.
-- Feature-gated: requires goalkeeper_enabled=true.
--
-- xT distribution uses the global expected_threat_grids from bronze.
-- SPADL pitch is 105x68m, grid is 12x8 zones.
--
-- D39 columns: psxg_faced, goals_conceded, goals_prevented from
-- stg_psxg__predictions; avg_defensive_action_distance, actions_outside_box_per_90
-- computed from defensive actions in gk_actions.
--
-- PR 6 (ADR-011): Kimball surrogate FKs added.
--   - data_source PROMOTED to permanent column (was dropped from gk_actions
--     pre-PR-6). Closes a latent multi-provider correctness gap: pre-PR-6
--     SB and WS BIGINT match_ids could collide on (player_id, match_id).
--   - match_key inherited from fct_action_values (PR 4b migration) via
--     gk_matches; PR-3 dim_matches bridges in shot_save_stats / psxg_shots
--     RETIRED — now JOIN fct_shots on match_key directly.
--   - team_key + player_key LEFT JOIN-resolved via dim_teams / dim_players
--     using data_source as provider (fct_action_values emits 'statsbomb' /
--     'wyscout' which map 1:1 to dim_matches.provider — no CASE needed).
--   - gk_stat_id surrogate hash now includes data_source — existing IDs
--     CHANGE on first --full-refresh rebuild.
--   - minutes CTE replaced with provider-agnostic int_minutes_played_per_match
--     (OPT-2 sub-item b.3). JOINs on (match_key, player_key, data_source).
--   - player_key propagated through gk_players -> gk_actions -> gk_matches
--     chain; redundant dim_players LEFT JOIN in final CTE removed.

{% if var('goalkeeper_enabled', false) %}

with gk_players as (

    select
        player_id,
        player_key
    from {{ ref('dim_players') }}
    where position_group = 'Goalkeeper'

),

gk_actions as (

    -- PR 6: PROPAGATE av.match_key + av.data_source (was dropped pre-PR-6).
    select
        av.match_id,
        av.match_key,
        av.player_id,
        gk.player_key,
        av.team_id,
        av.competition_id,
        av.season_id,
        av.action_type,
        av.action_result,
        av.start_x,
        av.start_y,
        av.end_x,
        av.end_y,
        av.data_source

    from {{ ref('fct_action_values') }} av
    inner join gk_players gk
        on av.player_id = gk.player_id

),

xt_grid as (

    select
        zone_x,
        zone_y,
        xt_value
    from {{ source('spadl', 'expected_threat_grids') }}
    where competition_id = 'global'

),

-- GK distribution passes with xT delta via zone lookup
gk_passes as (

    select
        a.player_id,
        a.match_id,
        a.data_source,
        a.action_type,
        a.action_result,
        a.start_x,
        a.start_y,
        a.end_x,
        a.end_y,
        coalesce(xt_end.xt_value, 0) - coalesce(xt_start.xt_value, 0)  as xt_delta,
        sqrt(pow(a.end_x - a.start_x, 2) + pow(a.end_y - a.start_y, 2)) as pass_distance

    from gk_actions a
    left join xt_grid xt_start
        on greatest(least(cast(a.start_x / (105.0 / 12) as int), 11), 0) = xt_start.zone_x
        and greatest(least(cast(a.start_y / (68.0 / 8) as int), 7), 0) = xt_start.zone_y
    left join xt_grid xt_end
        on greatest(least(cast(a.end_x / (105.0 / 12) as int), 11), 0) = xt_end.zone_x
        and greatest(least(cast(a.end_y / (68.0 / 8) as int), 7), 0) = xt_end.zone_y
    where a.action_type in ('pass', 'cross', 'freekick_short', 'freekick_crossed', 'goalkick')

),

pass_stats as (

    select
        player_id,
        match_id,
        data_source,
        count(*)                                                        as distribution_passes,
        sum(xt_delta)                                                   as gk_xt_delta_total,
        case
            when count(*) > 0 then sum(xt_delta) / count(*)
            else 0
        end                                                             as gk_xt_per_pass,
        sum(case when pass_distance > 60.0 then 1 else 0 end)          as long_passes

    from gk_passes
    group by player_id, match_id, data_source

),

-- Provider-agnostic minutes from int_minutes_played_per_match.
-- Surrogate-only: JOINs on (match_key, player_key).
minutes as (

    select
        imp.match_key,
        imp.player_key,
        imp.data_source,
        imp.minutes_played
    from {{ ref('int_minutes_played_per_match') }} imp

),

-- Collection stats: keeper_claim + keeper_punch
collection_stats as (

    select
        player_id,
        match_id,
        data_source,
        sum(case when action_type = 'keeper_claim' then 1 else 0 end)  as claims,
        case
            when sum(case when action_type = 'keeper_claim' then 1 else 0 end) > 0
            then cast(
                sum(case when action_type = 'keeper_claim' and action_result = 'success' then 1 else 0 end)
                as double
            ) / sum(case when action_type = 'keeper_claim' then 1 else 0 end)
            else cast(null as double)
        end                                                             as claim_success_rate,
        sum(case when action_type = 'keeper_punch' then 1 else 0 end)  as punches

    from gk_actions
    where action_type in ('keeper_claim', 'keeper_punch')
    group by player_id, match_id, data_source

),

-- Base grain: one row per (player_id, match_id, data_source).
-- min(match_key) safe because match_key is functionally determined by
-- (data_source, match_id). MIN() on competition_id/season_id avoids fan-out.
gk_matches as (

    select
        player_id,
        match_id,
        data_source,
        min(match_key)      as match_key,
        min(player_key)     as player_key,
        min(team_id)        as team_id,
        min(competition_id) as competition_id,
        min(season_id)      as season_id
    from gk_actions
    group by player_id, match_id, data_source

),

-- SPADL save stats: keeper_save (Wyscout source) + keeper_pick_up
spadl_save_stats as (

    select
        player_id,
        match_id,
        data_source,
        sum(case when action_type = 'keeper_save' then 1 else 0 end)   as saves,
        sum(case when action_type = 'keeper_pick_up' then 1 else 0 end) as keeper_pick_ups

    from gk_actions
    where action_type in ('keeper_save', 'keeper_pick_up')
    group by player_id, match_id, data_source

),

-- Shot-based save stats: shots with outcome 'Saved' / 'Saved Off Target' /
-- 'Saved to Post' counted against the GK's team. StatsBomb provides granular
-- shot outcomes; Wyscout only has 'Goal' / 'No Goal' so this CTE only
-- contributes saves for StatsBomb matches.
-- PR 6: dim_matches bridge retired — gk_matches now carries match_key
-- directly via fct_action_values (PR 4b). JOIN fct_shots on match_key.
shot_save_stats as (

    select
        gm.player_id,
        gm.match_id,
        gm.data_source,
        cast(count(*) as bigint)                                        as saves

    from {{ ref('fct_shots') }} s
    inner join gk_matches gm
        on s.match_key = gm.match_key
       and s.team_id != gm.team_id
       and s.data_source = gm.data_source
    where s.shot_outcome in ('Saved', 'Saved Off Target', 'Saved to Post')
    group by gm.player_id, gm.match_id, gm.data_source

),

-- Sweeper-keeper stats: defensive actions outside the penalty area.
-- SPADL coordinates have mixed orientation: some matches place the GK's
-- own goal at x=0, others at x=105. LEAST(x, 105-x) computes distance
-- from the nearest goal line, which is always the GK's own goal.
-- Penalty area extends 16.5m from the goal line.
sweeper_stats as (

    select
        ga.player_id,
        ga.match_id,
        ga.data_source,
        avg(least(ga.start_x, 105.0 - ga.start_x))                      as avg_defensive_action_distance,
        case
            when max(m.minutes_played) > 0
            then cast(
                     sum(case when least(ga.start_x, 105.0 - ga.start_x) > 16.5 then 1 else 0 end)
                     * (90.0 / max(m.minutes_played))
                 as double)
            else cast(0 as double)
        end                                                               as actions_outside_box_per_90

    from gk_actions ga
    inner join minutes m
        on  ga.match_key = m.match_key
        and ga.player_key = m.player_key
        and ga.data_source = m.data_source
    where ga.action_type in (
        'tackle', 'interception', 'clearance', 'block',
        'keeper_save', 'keeper_claim', 'keeper_punch', 'keeper_pick_up'
    )
    group by ga.player_id, ga.match_id, ga.data_source

),

-- PSxG aggregation: shots faced by the GK's team, joined with PSxG predictions.
-- stg_psxg__predictions.event_id is fct_shots.shot_id (MD5 surrogate key).
-- PR 6: shot_id is the unique join key — match_id cross-check dropped.
-- match_key + data_source flow through fct_shots so we can JOIN gk_matches
-- on (match_key, data_source).
psxg_shots as (

    select
        psxg.event_id,
        psxg.match_id,
        psxg.psxg,
        shots.team_id      as shooter_team_id,
        shots.match_key,
        shots.data_source,
        shots.shot_outcome
    from {{ ref('stg_psxg__predictions') }} psxg
    inner join {{ ref('fct_shots') }} shots
        on shots.shot_id = psxg.event_id

),

psxg_agg as (

    select
        gm.player_id,
        gm.match_id,
        gm.data_source,
        sum(ps.psxg)                                                      as psxg_faced,
        cast(sum(case when ps.shot_outcome = 'Goal' then 1 else 0 end)
            as int)                                                       as goals_conceded
    from gk_matches gm
    inner join psxg_shots ps
        on gm.match_key = ps.match_key
       and gm.team_id != ps.shooter_team_id
       and gm.data_source = ps.data_source
    group by gm.player_id, gm.match_id, gm.data_source

),

-- Combined saves: prefer shot-based (ground truth) over SPADL (derived).
save_stats as (

    select
        gm.player_id,
        gm.match_id,
        gm.data_source,
        coalesce(shs.saves, ss.saves, cast(0 as bigint))               as saves,
        case
            when coalesce(shs.saves, ss.saves, 0) + coalesce(pa.goals_conceded, 0) > 0
            then cast(coalesce(shs.saves, ss.saves, 0) as double)
                 / (coalesce(shs.saves, ss.saves, 0) + coalesce(pa.goals_conceded, 0))
            else cast(null as double)
        end                                                             as save_pct,
        coalesce(ss.keeper_pick_ups, cast(0 as bigint))                 as keeper_pick_ups

    from gk_matches gm
    left join shot_save_stats shs
        on gm.player_id = shs.player_id
        and gm.match_id = shs.match_id
        and gm.data_source = shs.data_source
    left join spadl_save_stats ss
        on gm.player_id = ss.player_id
        and gm.match_id = ss.match_id
        and gm.data_source = ss.data_source
    left join psxg_agg pa
        on gm.player_id = pa.player_id
        and gm.match_id = pa.match_id
        and gm.data_source = pa.data_source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'gm.player_id',
            'gm.match_id',
            'gm.data_source'
        ]) }}                                                           as gk_stat_id,

        gm.player_id,
        gm.match_id,
        gm.team_id,
        gm.competition_id,
        gm.season_id,
        gm.data_source,

        -- PR 6 (ADR-011) Kimball surrogate FKs.
        gm.match_key,
        dt.team_key,
        gm.player_key,

        coalesce(m.minutes_played, cast(null as double))                as minutes_played,

        -- Save stats
        coalesce(ss.saves, 0)                                           as saves,
        ss.save_pct,

        -- Collection stats
        coalesce(cs.claims, 0)                                          as claims,
        cs.claim_success_rate,
        coalesce(cs.punches, 0)                                         as punches,

        -- Distribution stats
        coalesce(ps.distribution_passes, 0)                             as distribution_passes,
        coalesce(ps.gk_xt_delta_total, 0)                               as gk_xt_delta_total,
        coalesce(ps.gk_xt_per_pass, 0)                                  as gk_xt_per_pass,
        case
            when coalesce(ps.distribution_passes, 0) > 0
            then cast(coalesce(ps.long_passes, 0) as double) / ps.distribution_passes
            else cast(null as double)
        end                                                             as launch_rate,
        coalesce(ss.keeper_pick_ups, 0)                                 as keeper_pick_ups,

        -- D39: PSxG shot-stopping metrics
        pa.psxg_faced,
        pa.goals_conceded,
        pa.psxg_faced - pa.goals_conceded                              as goals_prevented,

        -- D39: Sweeper-keeper positioning metrics
        sw.avg_defensive_action_distance,
        sw.actions_outside_box_per_90

    from gk_matches gm
    left join minutes m
        on  gm.match_key = m.match_key
        and gm.player_key = m.player_key
        and gm.data_source = m.data_source
    left join save_stats ss
        on gm.player_id = ss.player_id
        and gm.match_id = ss.match_id
        and gm.data_source = ss.data_source
    left join collection_stats cs
        on gm.player_id = cs.player_id
        and gm.match_id = cs.match_id
        and gm.data_source = cs.data_source
    left join pass_stats ps
        on gm.player_id = ps.player_id
        and gm.match_id = ps.match_id
        and gm.data_source = ps.data_source
    left join sweeper_stats sw
        on gm.player_id = sw.player_id
        and gm.match_id = sw.match_id
        and gm.data_source = sw.data_source
    left join psxg_agg pa
        on gm.player_id = pa.player_id
        and gm.match_id = pa.match_id
        and gm.data_source = pa.data_source
    -- PR 6 Kimball FK resolution. fct_action_values emits data_source =
    -- 'statsbomb' / 'wyscout' which maps 1:1 to dim_matches.provider —
    -- no CASE translation needed (unlike the defcon marts).
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = gm.data_source
       and dt.native_team_id = cast(gm.team_id as string)
)

select * from final

{% else %}

-- Goalkeeper stats not enabled — produce empty table with correct schema
select
    cast(null as string)    as gk_stat_id,
    cast(null as int)       as player_id,
    cast(null as bigint)    as match_id,
    cast(null as int)       as team_id,
    cast(null as int)       as competition_id,
    cast(null as int)       as season_id,
    cast(null as string)    as data_source,
    cast(null as bigint)    as match_key,
    cast(null as bigint)    as team_key,
    cast(null as bigint)    as player_key,
    cast(null as double)    as minutes_played,
    cast(null as bigint)    as saves,
    cast(null as double)    as save_pct,
    cast(null as bigint)    as claims,
    cast(null as double)    as claim_success_rate,
    cast(null as bigint)    as punches,
    cast(null as bigint)    as distribution_passes,
    cast(null as double)    as gk_xt_delta_total,
    cast(null as double)    as gk_xt_per_pass,
    cast(null as double)    as launch_rate,
    cast(null as bigint)    as keeper_pick_ups,
    cast(null as double)    as psxg_faced,
    cast(null as int)       as goals_conceded,
    cast(null as double)    as goals_prevented,
    cast(null as double)    as avg_defensive_action_distance,
    cast(null as double)    as actions_outside_box_per_90
where 1 = 0

{% endif %}
