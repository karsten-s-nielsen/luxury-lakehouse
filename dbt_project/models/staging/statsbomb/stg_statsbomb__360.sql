-- stg_statsbomb__360.sql
-- Clean StatsBomb 360 freeze-frame data from the bronze layer.
--
-- statsbombpy returns already-exploded data: each bronze row represents
-- one player's position at one event. This model parses the JSON
-- location column, deduplicates (statsbombpy can return identical rows),
-- and renames fields to project conventions.
--
-- StatsBomb coordinate system:
--   - Pitch is 120 x 80 yards
--   - Origin (0,0) is bottom-left when team attacks left to right
--   - x: 0 (own goal line) to 120 (opponent goal line)
--   - y: 0 (right touchline) to 80 (left touchline)

with source as (

    select * from {{ source('statsbomb', 'statsbomb_360') }}

),

-- Parse JSON columns once for reuse
parsed as (

    select
        *,
        from_json(location, 'ARRAY<DOUBLE>')    as parsed_location,
        from_json(visible_area, 'ARRAY<DOUBLE>') as parsed_visible_area
    from source

),

-- Deduplicate: statsbombpy can return identical rows for the same player
-- at the same event. No player ID exists, so all columns define identity.
deduped as (

    select
        *,
        row_number() over (
            partition by id, location, teammate, actor, keeper
            order by _ingested_at desc
        ) as _row_num
    from parsed

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['id', 'location', 'teammate', 'actor', 'keeper']) }}
                                                             as freeze_frame_id,
        id                                                   as event_uuid,
        match_id,
        competition_id,
        season_id,

        -- Player snapshot fields (already flat in bronze)
        teammate                                             as is_teammate,
        actor                                                as is_actor,
        keeper                                               as is_keeper,
        get(parsed_location, 0)                              as location_x,
        get(parsed_location, 1)                              as location_y,

        -- Visible area vertex count (flat array of alternating x,y — divide by 2)
        cast(size(parsed_visible_area) / 2 as int)           as visible_area_vertices,

        -- Full visible-area polygon as a flat array of alternating x,y coords
        -- (StatsBomb's on-wire format). Downstream spatial analysis (observed-
        -- player mask, off-camera handling) reads the full polygon rather than
        -- just the vertex count. PR 1.5 expansion.
        parsed_visible_area                                  as visible_area_polygon

    from deduped
    where _row_num = 1

)

select * from final
