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

        -- Temporal context
        period,
        start_frame,
        end_frame,

        -- Scaled start location (120x80)
        start_x * 120.0                                 as start_x,
        (1.0 - start_y) * 80.0                          as start_y,

        -- Scaled end location (120x80)
        end_x * 120.0                                   as end_x,
        (1.0 - end_y) * 80.0                            as end_y,

        -- Team and player
        team,
        player                                          as player_id

    from source

)

select * from cleaned
