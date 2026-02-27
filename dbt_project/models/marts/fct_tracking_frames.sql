-- fct_tracking_frames.sql
-- Pitch control metrics from Metrica tracking data.
--
-- This table enriches the raw tracking frames with spatial analysis
-- metrics: player speed, distance to ball, and velocity components.
--
-- Performance notes:
--   - This is a very large table (~3M rows per match at 25fps x 22 players)
--   - Downstream queries should always filter by match_id and period

with tracking as (

    select * from {{ ref('stg_metrica__tracking') }}

),

-- Calculate frame-over-frame velocity using window functions
final as (

    select
        tracking_id,
        match_id,
        period,
        frame,
        timestamp_seconds,
        player_id,
        team,
        x,
        y,
        ball_x,
        ball_y,

        -- Distance to ball (Euclidean)
        sqrt(
            power(x - ball_x, 2) + power(y - ball_y, 2)
        )                                               as distance_to_ball,

        -- Velocity components (position delta / time delta between frames)
        -- At 25fps, time_delta = frame_duration_seconds
        (x - lag(x) over (partition by match_id, player_id order by frame)) / {{ var('frame_duration_seconds') }} as velocity_x,
        (y - lag(y) over (partition by match_id, player_id order by frame)) / {{ var('frame_duration_seconds') }} as velocity_y,

        -- Speed (magnitude of velocity vector)
        sqrt(
            power(
                (x - lag(x) over (partition by match_id, player_id order by frame)) / {{ var('frame_duration_seconds') }},
                2
            )
            + power(
                (y - lag(y) over (partition by match_id, player_id order by frame)) / {{ var('frame_duration_seconds') }},
                2
            )
        )                                               as speed,

        -- Pitch control value at this player's location
        -- Populated by external Python model via MLflow (Phase 5+)
        cast(null as double)                            as pitch_control_value,

        -- Voronoi cell area (space controlled)
        -- Computed by external spatial analysis pipeline (Phase 5+)
        cast(null as double)                            as voronoi_area

    from tracking

)

select * from final
