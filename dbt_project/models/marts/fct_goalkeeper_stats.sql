{{ config(
    materialized='table',
    liquid_clustered_by=['player_id']
) }}
-- fct_goalkeeper_stats.sql
-- Per-match goalkeeper statistics combining saves, claims, distribution xT,
-- and future PSxG metrics.
--
-- Grain: one row per goalkeeper per match.
-- Feature-gated: requires goalkeeper_enabled=true.
--
-- xT distribution uses the global expected_threat_grids from bronze.
-- SPADL pitch is 105x68m, grid is 12x8 zones.
-- Zone index: x = least(cast(x / (105.0 / 12) as int), 11)
--             y = least(cast(y / (68.0 / 8) as int), 7)
--
-- D39 columns (psxg_faced, goals_conceded, goals_prevented,
-- avg_defensive_action_distance, actions_outside_box_per_90)
-- are initially NULL — populated after PSxG prediction import.

{% if var('goalkeeper_enabled', false) %}

with gk_players as (

    select
        player_id
    from {{ ref('dim_players') }}
    where position_group = 'Goalkeeper'

),

gk_actions as (

    select
        av.match_id,
        av.player_id,
        av.competition_id,
        av.season_id,
        av.action_type,
        av.action_result,
        av.start_x,
        av.start_y,
        av.end_x,
        av.end_y

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
        count(*)                                                        as distribution_passes,
        sum(xt_delta)                                                   as gk_xt_delta_total,
        case
            when count(*) > 0 then sum(xt_delta) / count(*)
            else 0
        end                                                             as gk_xt_per_pass,
        sum(case when pass_distance > 60.0 then 1 else 0 end)          as long_passes

    from gk_passes
    group by player_id, match_id

),

-- Per-match minutes from event-derived lineup/substitution logic
events as (

    select * from {{ ref('stg_statsbomb__events') }}

),

lineups as (

    select
        match_id,
        player_id
    from {{ ref('stg_statsbomb__lineups') }}
    where position_name is not null

),

match_duration as (

    select
        match_id,
        max(minute) + 1                                                 as match_end_minute
    from events
    group by match_id

),

substitution_off as (

    select
        match_id,
        player_id,
        minute                                                          as off_minute
    from events
    where event_type = 'Substitution'

),

substitution_on as (

    select
        match_id,
        cast(substitution_replacement_id as int)                        as player_id,
        minute                                                          as on_minute
    from events
    where event_type = 'Substitution'
      and substitution_replacement_id is not null

),

player_match_minutes as (

    -- Starting XI
    select
        l.match_id,
        l.player_id,
        coalesce(so.off_minute, md.match_end_minute)                    as minutes_played
    from lineups l
    inner join match_duration md
        on l.match_id = md.match_id
    left join substitution_off so
        on l.match_id = so.match_id
        and l.player_id = so.player_id

    union all

    -- Substitutes coming on
    select
        son.match_id,
        son.player_id,
        coalesce(soff.off_minute, md.match_end_minute) - son.on_minute  as minutes_played
    from substitution_on son
    inner join match_duration md
        on son.match_id = md.match_id
    left join substitution_off soff
        on son.match_id = soff.match_id
        and son.player_id = soff.player_id

),

minutes as (

    select
        pmm.player_id,
        pmm.match_id,
        pmm.minutes_played
    from player_match_minutes pmm
    inner join gk_players gk
        on pmm.player_id = gk.player_id

),

-- Collection stats: keeper_claim + keeper_punch
collection_stats as (

    select
        player_id,
        match_id,
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
    group by player_id, match_id

),

-- Save stats: keeper_save + keeper_pick_up
save_stats as (

    select
        player_id,
        match_id,
        sum(case when action_type = 'keeper_save' then 1 else 0 end)   as saves,
        case
            when sum(case when action_type = 'keeper_save' then 1 else 0 end) > 0
            then cast(
                sum(case when action_type = 'keeper_save' and action_result = 'success' then 1 else 0 end)
                as double
            ) / sum(case when action_type = 'keeper_save' then 1 else 0 end)
            else cast(null as double)
        end                                                             as save_pct,
        sum(case when action_type = 'keeper_pick_up' then 1 else 0 end) as keeper_pick_ups

    from gk_actions
    where action_type in ('keeper_save', 'keeper_pick_up')
    group by player_id, match_id

),

-- Base grain: distinct (player_id, match_id, competition_id, season_id)
gk_matches as (

    select distinct
        player_id,
        match_id,
        competition_id,
        season_id
    from gk_actions

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'gm.player_id',
            'gm.match_id'
        ]) }}                                                           as gk_stat_id,

        gm.player_id,
        gm.match_id,
        gm.competition_id,
        gm.season_id,

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

        -- D39 stubs (populated after PSxG prediction import)
        cast(null as double)                                            as psxg_faced,
        cast(null as int)                                               as goals_conceded,
        cast(null as double)                                            as goals_prevented,
        cast(null as double)                                            as avg_defensive_action_distance,
        cast(null as double)                                            as actions_outside_box_per_90

    from gk_matches gm
    left join minutes m
        on gm.player_id = m.player_id
        and gm.match_id = m.match_id
    left join save_stats ss
        on gm.player_id = ss.player_id
        and gm.match_id = ss.match_id
    left join collection_stats cs
        on gm.player_id = cs.player_id
        and gm.match_id = cs.match_id
    left join pass_stats ps
        on gm.player_id = ps.player_id
        and gm.match_id = ps.match_id

)

select * from final

{% else %}

-- Goalkeeper stats not enabled — produce empty table with correct schema
select
    cast(null as string)    as gk_stat_id,
    cast(null as int)       as player_id,
    cast(null as bigint)    as match_id,
    cast(null as int)       as competition_id,
    cast(null as int)       as season_id,
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
    cast(null as bigint)    as goals_conceded,
    cast(null as double)    as goals_prevented,
    cast(null as double)    as avg_defensive_action_distance,
    cast(null as double)    as actions_outside_box_per_90
where 1 = 0

{% endif %}
