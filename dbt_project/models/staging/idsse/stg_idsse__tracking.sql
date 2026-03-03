-- stg_idsse__tracking.sql
-- Normalize IDSSE Bundesliga tracking data to the shared 120×80 coordinate system.
--
-- Coordinate system alignment:
--   IDSSE (DFL): center-origin meters, x ∈ (-52.5, 52.5), y ∈ (-34, 34) on 105×68m
--   Target: (0,0) = bottom-left, (120,80) = top-right (StatsBomb system)
--   Transform: x_out = (x + 52.5) / 105.0 * 120.0
--              y_out = (y + 34.0) / 68.0 * 80.0

with source as (

    select * from {{ source('idsse', 'idsse_tracking') }}

),

normalized as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['match_id', 'period', 'frame', 'player_id']) }} as tracking_id,

        -- Match context
        match_id,

        -- Frame identifiers
        cast(period as int)                             as period,
        cast(frame as int)                              as frame,
        timestamp                                       as timestamp_seconds,
        frame_rate,

        -- Player identity
        player_id,
        team,

        -- Source provider
        'idsse'                                         as source_provider,

        -- Scaled player coordinates (120×80)
        (x + 52.5) / 105.0 * 120.0                     as x,
        (y + 34.0) / 68.0 * 80.0                       as y,

        -- Ball coordinates scaled to 120×80
        (ball_x + 52.5) / 105.0 * 120.0                as ball_x,
        (ball_y + 34.0) / 68.0 * 80.0                  as ball_y

    from source
    where x is not null
      and y is not null

)

select * from normalized
