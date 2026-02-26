-- stg_metrica__events.sql
-- Clean and normalize Metrica Sports event data.
--
-- Key transformations needed:
--   1. Scale coordinates from [0, 1] to 120x80 pitch system
--      (same scaling as tracking: x * 120, (1 - y) * 80)
--   2. Standardize event type names for downstream unification
--      - Metrica uses: PASS, SHOT, BALL LOST, BALL OUT, CHALLENGE, etc.
--      - May need mapping to a common taxonomy with StatsBomb
--   3. Link start_frame and end_frame to tracking data for spatial context
--   4. Add match_id from batch metadata
--   5. Parse composite subtype field (e.g. "HEAD-LOSS" → technique + outcome)

with source as (

    select * from {{ source('metrica', 'metrica_events') }}

),

cleaned as (

    select
        -- Primary key
        event_id,

        -- Match context
        -- TODO: Derive match_id from batch metadata or filename
        cast(null as string)                            as match_id,

        -- Event classification
        type                                            as event_type,
        subtype                                         as event_subtype,

        -- Temporal context
        period,
        start_frame,
        end_frame,

        -- Scaled start location (120x80)
        -- TODO: start_x * 120
        cast(null as double)                            as start_x,
        -- TODO: (1 - start_y) * 80
        cast(null as double)                            as start_y,

        -- Scaled end location (120x80)
        -- TODO: end_x * 120
        cast(null as double)                            as end_x,
        -- TODO: (1 - end_y) * 80
        cast(null as double)                            as end_y,

        -- Team and player
        team,
        player                                          as player_id

    from source

)

select * from cleaned
