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
-- Performance notes:
--   - Very large table (~3M rows per 25fps match, ~1.3M per 10fps match)
--   - Downstream queries should always filter by match_id and period

with tracking as (

    select * from {{ ref('stg_metrica__tracking') }}
    union all
    select * from {{ ref('stg_idsse__tracking') }}
    union all
    select * from {{ ref('stg_skillcorner__tracking') }}

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
        source_provider,
        frame_rate,
        x,
        y,
        ball_x,
        ball_y,

        -- Distance to ball (Euclidean)
        sqrt(
            power(x - ball_x, 2) + power(y - ball_y, 2)
        )                                               as distance_to_ball,

        -- Velocity components: delta_position * frame_rate = units/second
        -- Per-row frame_rate replaces hardcoded {{ var('frame_duration_seconds') }}
        (x - lag(x) over (partition by match_id, player_id, period order by frame)) * frame_rate as velocity_x,
        (y - lag(y) over (partition by match_id, player_id, period order by frame)) * frame_rate as velocity_y,

        -- Speed (magnitude of velocity vector)
        sqrt(
            power(
                (x - lag(x) over (partition by match_id, player_id, period order by frame)) * frame_rate,
                2
            )
            + power(
                (y - lag(y) over (partition by match_id, player_id, period order by frame)) * frame_rate,
                2
            )
        )                                               as speed,

        -- Pitch control value at this player's location
        -- Populated by external Python model via MLflow (Phase 11+)
        cast(null as double)                            as pitch_control_value,

        -- Voronoi cell area (space controlled)
        -- Computed by external spatial analysis pipeline (Phase 11+)
        cast(null as double)                            as voronoi_area

    from tracking

)

select * from final
