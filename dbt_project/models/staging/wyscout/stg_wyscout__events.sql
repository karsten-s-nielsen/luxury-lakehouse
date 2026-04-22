-- stg_wyscout__events.sql
-- Clean and normalize Wyscout event data from the bronze layer.
--
-- Key transformations:
--   1. Extract start/end positions from the `positions` JSON array
--   2. Scale coordinates from percentage (0-100) to 120x80
--   3. Decode tag IDs into boolean flags
--   4. Standardize matchPeriod values to integers
--   5. Rename columns from camelCase to snake_case
--
-- Wyscout coordinate system:
--   - x: 0 (own goal line) to 100 (opponent goal line), as percentage
--   - y: 0 (left touchline) to 100 (right touchline), as percentage

with source as (

    select * from {{ source('wyscout', 'wyscout_events') }}

),

-- Parse JSON columns once for reuse
parsed as (

    select
        *,
        from_json(positions, 'ARRAY<STRUCT<x:DOUBLE, y:DOUBLE>>') as parsed_positions,
        from_json(tags, 'ARRAY<STRUCT<id:INT>>')                  as parsed_tags
    from source

),

cleaned as (

    select
        -- Primary key: `id` is the unique Wyscout event identifier
        -- (eventId is an event TYPE code, not unique)
        cast(id as string)                                  as event_sk,
        eventId                                             as event_id,
        cast(subEventId as int)                             as sub_event_id,
        matchId                                             as match_id,

        -- Event classification
        eventName                                           as event_type,
        subEventName                                        as sub_event_type,

        -- Team and player
        playerId                                            as player_id,
        teamId                                              as team_id,

        -- Temporal fields
        case matchPeriod
            when '1H' then 1
            when '2H' then 2
            when 'E1' then 3
            when 'E2' then 4
            when 'P'  then 5
        end                                                 as period,
        eventSec                                            as event_sec,

        -- Start location (scaled to 120x80, use get() for safe access)
        {{ normalize_x('get(parsed_positions, 0).x', 'pct') }} as start_x,
        {{ normalize_y('get(parsed_positions, 0).y', 'pct') }} as start_y,

        -- End location (scaled to 120x80, may be NULL if positions has only 1 element)
        {{ normalize_x('get(parsed_positions, 1).x', 'pct') }} as end_x,
        {{ normalize_y('get(parsed_positions, 1).y', 'pct') }} as end_y,

        -- Tag-derived boolean flags. Tag IDs come from the Wyscout v3 spec;
        -- PR 1.5 expanded from 5 to 21 decoders per bronze-completeness.
        exists(parsed_tags, t -> t.id = 101)                as is_goal,
        exists(parsed_tags, t -> t.id = 102)                as is_own_goal,
        exists(parsed_tags, t -> t.id = 301)                as is_assist,
        exists(parsed_tags, t -> t.id = 401)                as is_key_pass,
        exists(parsed_tags, t -> t.id = 1801)               as is_accurate,
        exists(parsed_tags, t -> t.id = 1802)               as is_not_accurate,
        exists(parsed_tags, t -> t.id = 702)                as is_head,
        exists(parsed_tags, t -> t.id = 703)                as is_right_foot,
        exists(parsed_tags, t -> t.id = 704)                as is_left_foot,
        exists(parsed_tags, t -> t.id = 1601)               as is_counter_attack,
        exists(parsed_tags, t -> t.id = 503)                as under_pressure,
        exists(parsed_tags, t -> t.id = 1702)               as is_blocked,
        exists(parsed_tags, t -> t.id = 1401)               as is_red_card,
        exists(parsed_tags, t -> t.id = 1402)               as is_yellow_card,
        exists(parsed_tags, t -> t.id = 601)                as is_sliding_tackle,
        exists(parsed_tags, t -> t.id = 201)                as is_opportunity,
        exists(parsed_tags, t -> t.id = 1701)               as is_dangerous_ball_lost,
        exists(parsed_tags, t -> t.id = 501)                as is_free_space_right,
        exists(parsed_tags, t -> t.id = 502)                as is_free_space_left,
        exists(parsed_tags, t -> t.id = 801)                as is_high,
        exists(parsed_tags, t -> t.id = 802)                as is_low,

        -- Full parsed tag array pass-through for downstream consumers that
        -- need raw tag IDs not covered by the boolean decoders above.
        parsed_tags                                         as tags_parsed,

        -- Raw JSON pass-through for lineage / unparsed consumers. Bronze stores
        -- the original Wyscout v3 payload as JSON strings; these cols preserve
        -- them verbatim so downstream can re-parse if the struct evolves.
        positions                                           as positions_raw,
        tags                                                as tags_raw,

        -- Bronze passthroughs surfaced for Kimball completeness. The source
        -- bronze table is authoritative for competition context + audit time;
        -- surfacing them in staging keeps the lineage explicit.
        competition_name                                    as competition_name,
        _ingested_at                                        as _ingested_at,

        -- Data provenance
        'wyscout'                                           as data_source

    from parsed

)

select * from cleaned
