-- stg_wyscout__events.sql
-- Clean and normalize Wyscout event data from the bronze layer.
--
-- Key transformations needed:
--   1. Extract start/end positions from the `positions` JSON array:
--      - positions[0].x, positions[0].y → start coordinates
--      - positions[1].x, positions[1].y → end coordinates
--   2. Scale coordinates from percentage (0-100) to 120x80:
--      - x_scaled = (raw_x / 100) * 120
--      - y_scaled = (raw_y / 100) * 80
--   3. Decode tag IDs into boolean flags:
--      - Tag 101 = Goal
--      - Tag 102 = Own goal
--      - Tag 301 = Assist
--      - Tag 401 = Key pass
--      - Tag 1801 = Accurate
--      - Tag 1802 = Not accurate
--   4. Standardize matchPeriod values:
--      - '1H' → 1, '2H' → 2, 'E1' → 3, 'E2' → 4, 'P' → 5
--   5. Rename columns from camelCase to snake_case (dbt convention)
--
-- Wyscout coordinate system:
--   - x: 0 (own goal line) to 100 (opponent goal line), as percentage
--   - y: 0 (left touchline) to 100 (right touchline), as percentage

with source as (

    select * from {{ source('wyscout', 'wyscout_events') }}

),

cleaned as (

    select
        -- Primary key (rename from camelCase)
        eventId                                         as event_id,
        matchId                                         as match_id,

        -- Event classification
        eventName                                       as event_type,
        subEventName                                    as sub_event_type,

        -- Team and player
        playerId                                        as player_id,
        teamId                                          as team_id,

        -- Temporal fields
        -- TODO: Map matchPeriod string to integer:
        --   CASE matchPeriod WHEN '1H' THEN 1 WHEN '2H' THEN 2 ... END
        cast(null as int)                               as period,
        eventSec                                        as event_sec,

        -- Start location (scaled to 120x80)
        -- TODO: Extract from positions JSON array:
        --   (positions[0].x / 100.0) * 120
        --   (positions[0].y / 100.0) * 80
        cast(null as double)                            as start_x,
        cast(null as double)                            as start_y,

        -- End location (scaled to 120x80)
        -- TODO: Extract from positions JSON array:
        --   (positions[1].x / 100.0) * 120
        --   (positions[1].y / 100.0) * 80
        cast(null as double)                            as end_x,
        cast(null as double)                            as end_y,

        -- Tag-derived boolean flags
        -- TODO: Check if tag ID exists in the tags JSON array
        -- Example Databricks: array_contains(transform(tags, x -> x.id), 101)
        cast(null as boolean)                           as is_goal,
        cast(null as boolean)                           as is_own_goal,
        cast(null as boolean)                           as is_assist,
        cast(null as boolean)                           as is_key_pass,
        cast(null as boolean)                           as is_accurate

    from source

)

select * from cleaned
