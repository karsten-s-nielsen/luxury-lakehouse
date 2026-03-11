{{ config(
    materialized='incremental',
    unique_key='tracking_id',
    cluster_by=['match_id'],
    incremental_strategy='merge'
) }}
-- fct_tracking_frames.sql
-- Enriched tracking data from all providers (Metrica, IDSSE, SkillCorner).
--
-- This table unions tracking frames from all sources and enriches them with
-- spatial analysis metrics: player speed, distance to ball, and velocity.
--
-- Velocity calculation uses per-row frame_rate instead of a hardcoded var,
-- supporting mixed-fps sources (25fps Metrica/IDSSE, 10fps SkillCorner).
-- Formula: velocity = delta_position * frame_rate (since dt = 1/frame_rate).
--
-- Physical columns (speed_ms, velocity_x_ms, velocity_y_ms, acceleration_ms2)
-- use anisotropic scaling to convert from StatsBomb 120x80 coordinate units
-- to real-world meters: x_scale = 105/120, y_scale = 68/80.
--
-- Performance notes:
--   - Very large table (~3M rows per 25fps match, ~680K per 10fps match)
--   - Downstream queries should always filter by match_id and period

with

{% if is_incremental() %}
-- Compute existing match_ids once to avoid 3 separate full scans of {{ this }}.
existing_matches as (

    select distinct match_id from {{ this }}

),
{% endif %}

tracking as (

    select * from {{ ref('stg_metrica__tracking') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}
    union all
    select * from {{ ref('stg_idsse__tracking') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}
    union all
    select * from {{ ref('stg_skillcorner__tracking') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}

),

-- Extract LAG values into a CTE so speed can reference pre-computed deltas
-- instead of recomputing the window functions a second time.
with_lag as (

    select
        *,
        lag(x) over (partition by match_id, player_id, period order by frame) as prev_x,
        lag(y) over (partition by match_id, player_id, period order by frame) as prev_y
    from tracking

),

-- Derive velocity (SB units + m/s) and speed from pre-computed lag values.
-- Anisotropic conversion: x * (105/120), y * (68/80) for meters.
with_derived as (

    select
        *,

        -- Velocity in SB coordinate units/second
        (x - prev_x) * frame_rate                      as velocity_x,
        (y - prev_y) * frame_rate                      as velocity_y,

        -- Speed in SB coordinate units/second
        sqrt(
            power((x - prev_x) * frame_rate, 2)
            + power((y - prev_y) * frame_rate, 2)
        )                                               as speed,

        -- Velocity in m/s (anisotropic scaling per component)
        (x - prev_x) * frame_rate * (105.0 / {{ var('pitch_length') }})  as velocity_x_ms,
        (y - prev_y) * frame_rate * (68.0 / {{ var('pitch_width') }})    as velocity_y_ms,

        -- Speed in m/s (magnitude of velocity in meters)
        sqrt(
            power((x - prev_x) * frame_rate * (105.0 / {{ var('pitch_length') }}), 2)
            + power((y - prev_y) * frame_rate * (68.0 / {{ var('pitch_width') }}), 2)
        )                                               as speed_ms

    from with_lag

),

-- Second LAG pass to get previous speed_ms for acceleration computation.
with_lag_2 as (

    select
        *,
        lag(speed_ms) over (partition by match_id, player_id, period order by frame) as prev_speed_ms
    from with_derived

),

-- Final output with acceleration derived from consecutive speed_ms values.
final as (

    select
        tracking_id,
        match_id,
        period,
        frame,
        timestamp_seconds,
        player_id,
        team,
        source_provider,
        frame_rate,
        x,
        y,
        ball_x,
        ball_y,

        -- Distance to ball (Euclidean, SB coordinate units)
        sqrt(
            power(x - ball_x, 2) + power(y - ball_y, 2)
        )                                               as distance_to_ball,

        -- Velocity in SB coordinate units/second (backward compat)
        velocity_x,
        velocity_y,
        speed,

        -- Velocity in m/s (anisotropic-scaled)
        velocity_x_ms,
        velocity_y_ms,
        speed_ms,

        -- Acceleration in m/s^2: (speed_ms - prev_speed_ms) * frame_rate
        (speed_ms - prev_speed_ms) * frame_rate         as acceleration_ms2,

        -- Pitch control value at this player's location
        -- Populated by external Python model via MLflow (Phase 11+)
        cast(null as double)                            as pitch_control_value,

        -- Voronoi cell area (space controlled)
        -- Computed by external spatial analysis pipeline (Phase 11+)
        cast(null as double)                            as voronoi_area

    from with_lag_2

)

select * from final
