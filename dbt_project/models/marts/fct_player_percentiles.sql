{{ config(
    materialized='table',
    liquid_clustered_by=['competition_id']
) }}
-- fct_player_percentiles.sql
-- Per-competition percentile ranks for all player metrics.
--
-- Provides calibration context for raw metric values (CHI-AUDIT-180).
-- Grain: one row per (player_id, competition_id, season_id).
-- Percentiles computed via PERCENT_RANK() within each competition/season.

with player_names as (

    -- Deduplicate dim_players (multiple rows per player_id across sources)
    select
        cast(player_id as string) as player_id,
        player_display_name,
        row_number() over (partition by cast(player_id as string) order by player_display_name) as _rn
    from {{ ref('dim_players') }}

),

player_stats as (

    select
        cast(ps.player_id as string) as player_id,
        ps.competition_id,
        ps.season_id,
        pn.player_display_name,
        ps.minutes_played,
        ps.xg_per_90,
        ps.goals_per_90,
        ps.passes_per_90,
        ps.progressive_passes_per_90,
        ps.pass_completion_pct,
        ps.vaep_per_90,
        ps.offensive_vaep_per_90,
        ps.defensive_vaep_per_90,
        ps.line_breaking_per_90
        {% if var('defcon_enabled', false) %}
        , ps.defcon_per_90
        {% else %}
        , cast(null as double) as defcon_per_90
        {% endif %}
    from {{ ref('fct_player_stats') }} ps
    left join player_names pn
        on cast(ps.player_id as string) = pn.player_id
        and pn._rn = 1
    where ps.competition_id is not null
      and ps.season_id is not null

),

physical_by_comp as (

    select
        cast(ps.player_id as string) as player_id,
        ms.competition_id,
        ms.season_id,
        avg(ps.distance_per_minute_m)  as avg_distance_per_minute,
        avg(ps.max_speed_ms)           as avg_max_speed
    from {{ ref('fct_physical_stats') }} ps
    inner join {{ ref('fct_match_summary') }} ms
        on cast(ps.match_id as string) = cast(ms.match_id as string)
    group by cast(ps.player_id as string), ms.competition_id, ms.season_id

),

{% if var('pausa_enabled', false) %}
pausa_agg as (

    select * from {{ ref('fct_pausa_rankings') }}

),
{% endif %}

enriched as (

    select
        s.player_id,
        s.competition_id,
        s.season_id,
        s.player_display_name,
        s.minutes_played,

        -- Core per-90 metrics
        s.xg_per_90,
        s.goals_per_90,
        s.passes_per_90,
        s.progressive_passes_per_90,
        s.pass_completion_pct,
        s.vaep_per_90,
        s.offensive_vaep_per_90,
        s.defensive_vaep_per_90,
        s.line_breaking_per_90,
        -- Always emit conditional columns (NULL when disabled) for static contract
        s.defcon_per_90,

        -- Physical stats (NULL for non-tracking)
        ph.avg_distance_per_minute,
        ph.avg_max_speed,

        {% if var('pausa_enabled', false) %}
        pa.avg_pausa
        {% else %}
        cast(null as double) as avg_pausa
        {% endif %}

    from player_stats s
    left join physical_by_comp ph
        on s.player_id = ph.player_id
        and s.competition_id = ph.competition_id
        and s.season_id = ph.season_id
    {% if var('pausa_enabled', false) %}
    left join pausa_agg pa
        on s.player_id = pa.player_id
    {% endif %}

),

percentiled as (

    select
        player_id,
        competition_id,
        season_id,
        player_display_name,
        minutes_played,

        percent_rank() over (partition by competition_id, season_id order by xg_per_90)                as xg_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by goals_per_90)             as goals_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by passes_per_90)            as passes_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by progressive_passes_per_90) as progressive_passes_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by pass_completion_pct)      as pass_completion_pct_pctile,
        percent_rank() over (partition by competition_id, season_id order by vaep_per_90)              as vaep_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by offensive_vaep_per_90)    as offensive_vaep_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by defensive_vaep_per_90)    as defensive_vaep_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by line_breaking_per_90)     as line_breaking_per_90_pctile,
        -- Always emit conditional pctile columns (NULL when source is NULL)
        percent_rank() over (partition by competition_id, season_id order by defcon_per_90)            as defcon_per_90_pctile,
        percent_rank() over (partition by competition_id, season_id order by avg_pausa)                as avg_pausa_pctile,

        percent_rank() over (partition by competition_id, season_id order by avg_distance_per_minute)  as distance_per_minute_pctile,
        percent_rank() over (partition by competition_id, season_id order by avg_max_speed)            as max_speed_pctile

    from enriched

)

select
    cast(player_id as string)          as player_id,
    cast(competition_id as int)        as competition_id,
    cast(season_id as int)             as season_id,
    cast(player_display_name as string) as player_display_name,
    cast(minutes_played as double)     as minutes_played,

    cast(xg_per_90_pctile as double)                as xg_per_90_pctile,
    cast(goals_per_90_pctile as double)             as goals_per_90_pctile,
    cast(passes_per_90_pctile as double)            as passes_per_90_pctile,
    cast(progressive_passes_per_90_pctile as double) as progressive_passes_per_90_pctile,
    cast(pass_completion_pct_pctile as double)      as pass_completion_pct_pctile,
    cast(vaep_per_90_pctile as double)              as vaep_per_90_pctile,
    cast(offensive_vaep_per_90_pctile as double)    as offensive_vaep_per_90_pctile,
    cast(defensive_vaep_per_90_pctile as double)    as defensive_vaep_per_90_pctile,
    cast(line_breaking_per_90_pctile as double)     as line_breaking_per_90_pctile,
    cast(defcon_per_90_pctile as double)            as defcon_per_90_pctile,
    cast(avg_pausa_pctile as double)                as avg_pausa_pctile,

    cast(distance_per_minute_pctile as double)      as distance_per_minute_pctile,
    cast(max_speed_pctile as double)                as max_speed_pctile,

    current_timestamp()                             as _loaded_at

from percentiled
