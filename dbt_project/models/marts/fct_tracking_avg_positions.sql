{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='avg_position_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key'],
    tags=['marts', 'output_mart']
) }}
-- fct_tracking_avg_positions.sql
-- Pre-computed average positions per player per match-period.
--
-- Replaces expensive runtime AVG() queries that scan 1-3M rows of
-- fct_tracking_frames per match. Used by the Taipy app's pass network,
-- team shape, and movement pages.
--
-- Grain: one row per (match_id, period, player_id, team).
-- Source: fct_tracking_frames (tracking data with x/y coordinates and speed).
--
-- Coordinate system: StatsBomb 120x80 pitch coordinates.
-- Speed: meters per second (speed_ms column).

with

{% if is_incremental() %}
existing_matches as (
    select distinct match_id from {{ this }}
),
{% endif %}

tracking as (

    select
        match_id,
        match_key,
        period,
        player_id,
        player_key,
        team,
        team_id,
        team_key,
        data_source,
        x,
        y,
        speed_ms,
        frame,
        frame_rate
    from {{ ref('fct_tracking_frames') }}
    where player_id is not null
    {% if is_incremental() %}
    and match_id not in (select match_id from existing_matches)
    {% endif %}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['match_id', 'period', 'player_id', 'data_source']) }} as avg_position_id,
        match_id,
        match_key,
        period,
        player_id,
        player_key,
        team,
        team_id,
        team_key,
        data_source,
        avg(x)                                              as avg_x,
        avg(y)                                              as avg_y,
        avg(speed_ms)                                       as avg_speed,
        count(*)                                            as frame_count,
        min(frame)                                          as min_frame,
        max(frame)                                          as max_frame,
        max(frame_rate)                                     as frame_rate
    from tracking
    group by match_id, match_key, period, player_id, player_key, team, team_id, team_key, data_source

)

select * from final
