{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='shape_timeline_id',
    on_schema_change='fail',
    liquid_clustered_by=['match_id']
) }}
-- fct_tracking_shape_timeline.sql
-- Pre-computed time-bucketed positions at 5-second intervals.
--
-- Replaces expensive runtime sampled-position queries that scan millions of
-- tracking rows per match. Used by the Taipy app's team shape timeline
-- visualization.
--
-- Grain: one row per (match_id, period, time_bucket, player_id, team).
-- Source: fct_tracking_frames (tracking data with x/y coordinates and speed).
--
-- Time bucketing: FLOOR(timestamp_seconds / 5) * 5 produces 5-second windows.
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
        period,
        floor(timestamp_seconds / 5) * 5                   as time_bucket,
        player_id,
        team,
        x,
        y,
        speed_ms
    from {{ ref('fct_tracking_frames') }}
    where player_id is not null
    {% if is_incremental() %}
    and match_id not in (select match_id from existing_matches)
    {% endif %}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['match_id', 'period', 'time_bucket', 'player_id']) }} as shape_timeline_id,
        match_id,
        period,
        time_bucket,
        player_id,
        team,
        avg(x)                                              as avg_x,
        avg(y)                                              as avg_y,
        avg(speed_ms)                                       as avg_speed
    from tracking
    group by match_id, period, time_bucket, player_id, team

)

select * from final
