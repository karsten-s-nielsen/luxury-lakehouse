{{ config(
    materialized='incremental',
    unique_key='tracking_id',
    liquid_clustered_by=['match_key'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['marts', 'input_mart'],
    tblproperties={
        'delta.enableChangeDataFeed': 'true',
    }
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

    -- PR 7 (ADR-011): tracking staging now surfaces team_id per Q1 (IDSSE
    -- real DFL TeamId from PR 5a; Metrica synthesized via dim_teams pattern;
    -- SkillCorner via home_team_id/away_team_id CASE). The 14-column shared
    -- schema excludes is_goalkeeper — TC-2 derives it from
    -- int_tracking_goalkeepers (silly-kicks derive_goalkeepers() via TC-1).
    select
        tracking_id, match_id, period, frame, timestamp_seconds,
        frame_rate, player_id, team, team_id, source_provider,
        x, y, ball_x, ball_y
    from {{ ref('stg_metrica__tracking') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}
    union all
    select
        tracking_id, match_id, period, frame, timestamp_seconds,
        frame_rate, player_id, team, team_id, source_provider,
        x, y, ball_x, ball_y
    from {{ ref('stg_idsse__tracking') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}
    union all
    select
        tracking_id, match_id, period, frame, timestamp_seconds,
        frame_rate, player_id, team, team_id, source_provider,
        x, y, ball_x, ball_y
    from {{ ref('stg_skillcorner__tracking') }}
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
        (x - prev_x) * frame_rate * ({{ var('pitch_length_m') }} / {{ var('pitch_length') }})  as velocity_x_ms,
        (y - prev_y) * frame_rate * ({{ var('pitch_width_m') }} / {{ var('pitch_width') }})    as velocity_y_ms,

        -- Speed in m/s (magnitude of velocity in meters)
        sqrt(
            power((x - prev_x) * frame_rate * ({{ var('pitch_length_m') }} / {{ var('pitch_length') }}), 2)
            + power((y - prev_y) * frame_rate * ({{ var('pitch_width_m') }} / {{ var('pitch_width') }}), 2)
        )                                               as speed_ms

    from with_lag

),

-- Spike detection: NULL all derived kinematics when speed_ms exceeds the
-- physical maximum for a human player (15 m/s ≈ 54 km/h). SkillCorner data
-- exhibits coordinate teleportation where all 22 players jump simultaneously
-- on camera-switch / model-reset frames, producing speeds up to 747 m/s.
-- Raw x/y coordinates are preserved (source data); only computed derivatives
-- are NULLed since they are meaningless on spike frames.
with_spike_flag as (

    select
        *,
        speed_ms <= {{ var('speed_spike_threshold_ms', 15) }}
            or speed_ms is null                                     as _coords_valid
    from with_derived

),

with_spike_cleaned as (

    select
        tracking_id, match_id, period, frame, timestamp_seconds,
        frame_rate, player_id, team, team_id, source_provider,
        x, y, ball_x, ball_y,
        prev_x, prev_y,

        _coords_valid,

        case when _coords_valid then velocity_x    else null end as velocity_x,
        case when _coords_valid then velocity_y    else null end as velocity_y,
        case when _coords_valid then speed         else null end as speed,
        case when _coords_valid then velocity_x_ms else null end as velocity_x_ms,
        case when _coords_valid then velocity_y_ms else null end as velocity_y_ms,
        case when _coords_valid then speed_ms      else null end as speed_ms

    from with_spike_flag

),

-- Second LAG pass to get previous speed_ms for acceleration computation.
with_lag_2 as (

    select
        *,
        lag(speed_ms) over (partition by match_id, player_id, period order by frame) as prev_speed_ms
    from with_spike_cleaned

),

-- Final output with acceleration derived from consecutive speed_ms values.
-- PR 7 (ADR-011): adds Kimball surrogate FKs match_key + team_key + player_key
-- via LEFT JOINs to dim_matches / dim_teams / dim_players using
-- (provider, native_id) pairs. Coexists with legacy match_id / team_id /
-- player_id during the 2026-07-22 dual-column window.
final as (

    select
        wl.tracking_id,
        wl.match_id,
        dm.match_key,
        wl.period,
        wl.frame,
        wl.timestamp_seconds,
        wl.player_id,
        dp.player_key,
        wl.team,
        wl.team_id,
        dt.team_key,
        wl.source_provider,
        wl.source_provider                              as data_source,
        gk.player_key is not null                          as is_goalkeeper,
        wl.frame_rate,
        wl.x,
        wl.y,
        wl.ball_x,
        wl.ball_y,

        -- Distance to ball (Euclidean, SB coordinate units).
        -- NULLed on spike frames where player coordinates are unreliable.
        case when wl._coords_valid then sqrt(
            power(wl.x - wl.ball_x, 2) + power(wl.y - wl.ball_y, 2)
        ) else null end                                 as distance_to_ball,

        -- Velocity in SB coordinate units/second (backward compat)
        wl.velocity_x,
        wl.velocity_y,
        wl.speed,

        -- Velocity in m/s (anisotropic-scaled)
        wl.velocity_x_ms,
        wl.velocity_y_ms,
        wl.speed_ms,

        -- Acceleration in m/s^2: (speed_ms - prev_speed_ms) * frame_rate
        (wl.speed_ms - wl.prev_speed_ms) * wl.frame_rate as acceleration_ms2,

        -- Pitch control value at this player's location
        -- Populated by external Python model via MLflow (Phase 11+)
        cast(null as double)                            as pitch_control_value,

        -- Voronoi cell area (space controlled)
        -- Computed by external spatial analysis pipeline (Phase 11+)
        cast(null as double)                            as voronoi_area

    from with_lag_2 wl
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = wl.source_provider
       and dm.native_match_id = cast(wl.match_id as string)
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = wl.source_provider
       and dt.native_team_id = cast(wl.team_id as string)
    left join {{ ref('dim_players') }} dp
        on  dp.provider = wl.source_provider
       and dp.native_player_id = cast(wl.player_id as string)
    left join {{ ref('int_tracking_goalkeepers') }} gk
        on  gk.match_key = dm.match_key
       and gk.player_key = dp.player_key

)

select * from final
