-- stg_metrica__events.sql
-- Clean and normalize Metrica Sports event data.
--
-- Key transformations:
--   1. Scale coordinates from [0, 1] to 120x80 pitch system
--      (x * 120, (1 - y) * 80 to flip vertical axis)
--   2. Standardize event type names for downstream unification
--   3. Link start_frame and end_frame to tracking data for spatial context
--
-- Coordinate system alignment:
--   Metrica: (0,0) = top-left, (1,1) = bottom-right, normalized [0,1]
--   StatsBomb: (0,0) = bottom-left, (120,80) = top-right, in yards

with source as (

    select * from {{ source('metrica', 'metrica_events') }}

),

cleaned as (

    select
        -- Primary key (event_id is only unique within a match)
        {{ dbt_utils.generate_surrogate_key(['match_id', 'event_id']) }} as event_sk,
        event_id,

        -- Match context (added during ingestion)
        match_id,

        -- Event classification
        type                                            as event_type,
        subtype                                         as event_subtype,

        -- Bronze passthrough — raw event classification columns
        type,
        subtype,
        subtypes_all_json,

        -- Temporal context
        period,
        start_frame,
        end_frame,

        -- Bronze passthrough — event timing in seconds from period start
        start_time_s,
        end_time_s,

        -- Scaled start location (120x80)
        {{ normalize_x('start_x', 'metrica') }} as start_x,
        {{ normalize_y('start_y', 'metrica') }} as start_y,

        -- Scaled end location (120x80)
        {{ normalize_x('end_x', 'metrica') }} as end_x,
        {{ normalize_y('end_y', 'metrica') }} as end_y,

        -- Team and player
        team,
        player                                          as player_id,

        -- Bronze passthrough — raw actor and recipient identifiers
        player,
        `to`,

        -- Bronze passthrough — pitch dimensions denormalized onto every row
        pitch_length_m,
        pitch_width_m

    from source

)

select * from cleaned
