-- fct_tracking_frames.sql
-- Pitch control metrics from Metrica tracking data.
--
-- This table enriches the raw tracking frames with spatial analysis
-- metrics. It serves as the bridge between raw positional data and
-- advanced analytics models.
--
-- Key metrics:
--   - Player speed (from frame-to-frame position deltas)
--   - Distance to ball (Euclidean distance from player to ball)
--   - Pitch control value at player location (Fernandez & Bornn, 2018)
--   - Voronoi cell area (space controlled by player)
--
-- Pitch control model (Fernandez & Bornn, 2018):
--   Calculates the probability that a team controls any point on the pitch,
--   based on player positions, velocities, and distances. The full model
--   is implemented in Python (src/models/) and results loaded back here.
--
-- Performance notes:
--   - This is a very large table (~3M rows per match at 25fps x 22 players)
--   - Consider incremental materialization with partitioning by match_id
--   - Downstream queries should always filter by match_id and period

with tracking as (

    select * from {{ ref('stg_metrica__tracking') }}

),

-- Calculate frame-over-frame velocity using window functions
with_velocity as (

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
        -- At 25fps, time_delta = 0.04 seconds
        -- TODO: Implement using LAG window function:
        -- (x - lag(x) over (partition by match_id, player_id order by frame)) / 0.04
        cast(null as double)                            as velocity_x,
        cast(null as double)                            as velocity_y,

        -- Speed (magnitude of velocity vector)
        -- TODO: sqrt(velocity_x^2 + velocity_y^2)
        cast(null as double)                            as speed,

        -- Pitch control value at this player's location
        -- TODO: Populated by external Python model via MLflow
        -- The Python pitch control model writes results to a separate table
        -- which is joined here. For now, placeholder.
        cast(null as double)                            as pitch_control_value,

        -- Voronoi cell area (space controlled)
        -- TODO: Computed by external spatial analysis pipeline
        cast(null as double)                            as voronoi_area

    from tracking

)

select * from with_velocity
