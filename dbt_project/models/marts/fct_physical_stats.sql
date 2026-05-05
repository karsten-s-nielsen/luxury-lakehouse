{{ config(
    materialized='incremental',
    unique_key=['player_key', 'match_key', 'data_source'],
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['marts', 'output_mart']
) }}
-- fct_physical_stats.sql
-- Per-player per-match physical performance aggregation from tracking data.
--
-- Grain: one row per (player_key, match_key, data_source).
-- Source: fct_tracking_frames with speed_ms and acceleration_ms2 columns.
--
-- Distance computation uses anisotropic scaling per frame to convert
-- StatsBomb 120x80 coordinate deltas to real-world meters:
--   displacement_m = sqrt((dx * 105/120)^2 + (dy * 68/80)^2)
--
-- Physical thresholds (from dbt_project.yml vars):
--   - High-speed running: >= 5.5 m/s
--   - Sprint: >= 7.0 m/s
--   - High acceleration: >= 3.0 m/s^2
--
-- Off-Ball xT columns are populated via LEFT JOIN from stg_off_ball_xt__results.

with

{% if is_incremental() %}
existing_matches as (

    select distinct match_id from {{ this }}

),
{% endif %}

frames as (

    select
        player_id,
        player_key,
        match_id,
        match_key,
        source_provider,
        data_source,
        frame_rate,
        period,
        frame,
        x,
        y,
        speed_ms,
        acceleration_ms2,
        -- Per-frame displacement: speed_ms / frame_rate avoids redundant LAG on x/y.
        -- Derivation: displacement_m = speed_ms / frame_rate, since
        -- speed_ms = sqrt((dx * x_scale * frame_rate)^2 + (dy * y_scale * frame_rate)^2).
        -- First frame per player/period has null speed_ms (no prior position) → 0.
        coalesce(speed_ms / frame_rate, 0) as displacement_m
    from {{ ref('fct_tracking_frames') }}
    where player_id is not null
    {% if is_incremental() %}
    and match_id not in (select match_id from existing_matches)
    {% endif %}

),

player_match_stats as (

    select
        player_id,
        any_value(player_key)                               as player_key,
        match_id,
        any_value(match_key)                                as match_key,
        min(source_provider)                                as source_provider,
        data_source,
        min(frame_rate)                                     as frame_rate,

        -- Minutes played (estimated from frame range)
        (max(frame) - min(frame)) / (min(frame_rate) * 60.0) as minutes_played,

        -- Distance metrics
        sum(displacement_m)                                 as total_distance_m,
        sum(displacement_m) / 1000.0                        as total_distance_km,
        sum(case
            when speed_ms >= {{ var('high_speed_threshold') }} then displacement_m
            else 0
        end)                                                as hsr_distance_m,
        sum(case
            when speed_ms >= {{ var('sprint_speed_threshold') }} then displacement_m
            else 0
        end)                                                as sprint_distance_m,

        -- Count metrics
        sum(case
            when speed_ms >= {{ var('sprint_speed_threshold') }} then 1
            else 0
        end)                                                as sprint_frame_count,
        sum(case
            when acceleration_ms2 >= {{ var('high_acceleration_threshold') }} then 1
            else 0
        end)                                                as high_accel_count,
        sum(case
            when acceleration_ms2 <= -{{ var('high_acceleration_threshold') }} then 1
            else 0
        end)                                                as high_decel_count,

        -- Speed metrics
        avg(speed_ms)                                       as avg_speed_ms,
        max(speed_ms)                                       as max_speed_ms,

        -- Average position
        avg(x)                                              as avg_x,
        avg(y)                                              as avg_y

    from frames
    group by player_id, match_id, data_source

),

{% if var('off_ball_xt_enabled', false) %}
off_ball_xt as (

    select
        player_id,
        match_id,
        total_off_ball_xt,
        avg_off_ball_xt,
        frames_sampled as off_ball_xt_frames_sampled
    from {{ ref('stg_off_ball_xt__results') }}

),
{% endif %}

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['s.player_id', 's.match_id', 's.data_source']) }} as physical_stats_id,
        s.player_id,
        s.player_key,
        s.match_id,
        s.match_key,
        s.source_provider,
        s.data_source,
        s.frame_rate,
        s.minutes_played,
        s.total_distance_m,
        s.total_distance_km,
        s.hsr_distance_m,
        s.sprint_distance_m,
        s.sprint_frame_count,
        s.high_accel_count,
        s.high_decel_count,
        case
            when s.minutes_played > 0 then s.total_distance_m / s.minutes_played
            else 0
        end                                                 as distance_per_minute_m,
        s.avg_speed_ms,
        s.max_speed_ms,
        s.avg_x,
        s.avg_y,

        {% if var('off_ball_xt_enabled', false) %}
        o.total_off_ball_xt,
        o.avg_off_ball_xt,
        o.off_ball_xt_frames_sampled
        {% else %}
        cast(null as double)                                as total_off_ball_xt,
        cast(null as double)                                as avg_off_ball_xt,
        cast(null as int)                                   as off_ball_xt_frames_sampled
        {% endif %}

    from player_match_stats s
    {% if var('off_ball_xt_enabled', false) %}
    left join off_ball_xt o
        on s.player_id = o.player_id and s.match_id = o.match_id
    {% endif %}

)

select * from final
