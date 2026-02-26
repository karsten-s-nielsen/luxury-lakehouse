-- stg_metrica__tracking.sql
-- Parse and normalize Metrica Sports 25fps tracking data.
--
-- Key transformations needed:
--   1. Explode per-frame JSON into one row per player per frame
--      - Raw data has one row per frame with all players nested in JSON
--      - Need to LATERAL VIEW EXPLODE or similar to get individual player rows
--   2. Scale coordinates from [0, 1] normalized to 120x80 pitch system:
--      - x_scaled = raw_x * 120 (pitch length)
--      - y_scaled = raw_y * 80  (pitch width)
--   3. Separate home and away player data into a uniform schema
--   4. Include ball coordinates on every player row for distance calculations
--   5. Add match_id from batch/file metadata (Metrica sample data has 3 matches)
--
-- Performance considerations:
--   - At 25 fps with ~22 players, a 90-minute match generates ~3M rows
--   - Consider incremental materialization for production
--   - Partition by match_id and period for efficient querying
--
-- Coordinate system alignment:
--   Metrica: (0,0) = top-left, (1,1) = bottom-right, normalized [0,1]
--   StatsBomb: (0,0) = bottom-left, (120,80) = top-right, in yards
--   Scaling: x * 120, (1 - y) * 80 to flip vertical axis

with source as (

    select * from {{ source('metrica', 'metrica_tracking') }}

),

-- TODO: Implement the JSON explosion for player tracking data
-- Example approach for Databricks:
--
-- home_players_exploded as (
--     select
--         period,
--         frame,
--         timestamp as timestamp_seconds,
--         'home' as team,
--         player_key as player_id,
--         player_value.x as raw_x,
--         player_value.y as raw_y,
--         ball_x as raw_ball_x,
--         ball_y as raw_ball_y
--     from source
--     lateral view explode(from_json(home_players, 'map<string, struct<x:double, y:double>>'))
--         as player_key, player_value
-- ),
-- ... union all with away_players_exploded ...

normalized as (

    select
        -- Surrogate key
        -- TODO: Generate once columns are populated
        -- {{ dbt_utils.generate_surrogate_key(['match_id', 'period', 'frame', 'player_id']) }} as tracking_id,
        cast(null as string)                            as tracking_id,

        -- Match context
        -- TODO: Derive match_id from batch metadata or filename
        cast(null as string)                            as match_id,

        -- Frame identifiers
        cast(null as int)                               as period,
        cast(null as int)                               as frame,
        cast(null as double)                            as timestamp_seconds,

        -- Player identity
        cast(null as string)                            as player_id,
        cast(null as string)                            as team,

        -- Scaled player coordinates (120x80)
        -- TODO: raw_x * 120 and (1 - raw_y) * 80
        cast(null as double)                            as x,
        cast(null as double)                            as y,

        -- Scaled ball coordinates (120x80)
        -- TODO: raw_ball_x * 120 and (1 - raw_ball_y) * 80
        cast(null as double)                            as ball_x,
        cast(null as double)                            as ball_y,

        -- Player velocity (computed from frame-over-frame position changes)
        -- TODO: Implement in a separate downstream model or window function
        cast(null as double)                            as velocity_x,
        cast(null as double)                            as velocity_y,
        cast(null as double)                            as speed

    from source

)

select * from normalized
