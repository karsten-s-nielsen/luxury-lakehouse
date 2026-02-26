-- stg_statsbomb__events.sql
-- Flatten and clean raw StatsBomb event data from the bronze layer.
--
-- Key transformations needed:
--   1. Extract event type name from nested JSON: type.name
--   2. Extract team/player IDs from nested objects: team.id, player.id
--   3. Split location array [x, y] into separate location_x, location_y columns
--   4. Parse timestamp string into proper time components (minute, second)
--   5. Extract possession information: possession, possession_team.id
--   6. Handle NULL locations (off-ball events like substitutions have no location)
--   7. Flatten related_events array into a comma-separated string or lateral view
--
-- StatsBomb coordinate system:
--   - Pitch is 120 x 80 yards
--   - Origin (0,0) is bottom-left when team attacks left to right
--   - x: 0 (own goal line) to 120 (opponent goal line)
--   - y: 0 (right touchline) to 80 (left touchline)

with source as (

    select * from {{ source('statsbomb', 'statsbomb_events') }}

),

flattened as (

    select
        -- Primary key
        id                                              as event_id,
        match_id,

        -- Event classification
        -- TODO: Extract from nested JSON — e.g. type:name for Databricks:
        --   type.name   or   type:name   depending on column format
        cast(null as string)                            as event_type,
        cast(null as int)                               as event_type_id,

        -- Temporal fields
        -- TODO: Parse from period, minute, second, and timestamp fields
        cast(null as int)                               as period,
        cast(null as int)                               as minute,
        cast(null as int)                               as second,
        cast(null as string)                            as timestamp,

        -- Team and player
        -- TODO: Extract from nested JSON objects (team.id, player.id)
        cast(null as int)                               as team_id,
        cast(null as string)                            as team_name,
        cast(null as int)                               as player_id,
        cast(null as string)                            as player_name,

        -- Location (split from [x, y] array)
        -- TODO: Extract array elements — e.g. location[0], location[1]
        cast(null as double)                            as location_x,
        cast(null as double)                            as location_y,

        -- Possession context
        -- TODO: Extract possession number and possession_team from nested JSON
        cast(null as int)                               as possession,
        cast(null as int)                               as possession_team_id,

        -- Play pattern
        -- TODO: Extract play_pattern.name (Regular Play, From Corner, etc.)
        cast(null as string)                            as play_pattern,

        -- Duration (seconds the event lasted, e.g. carry duration)
        cast(null as double)                            as duration,

        -- Index for ordering events within a possession sequence
        cast(null as int)                               as index

    from source

)

select * from flattened
