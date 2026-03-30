-- stg_idsse__events.sql
-- Normalize IDSSE Bundesliga event data to the shared 120×80 coordinate system.
--
-- Coordinate system alignment:
--   IDSSE events (DFL): pitch-origin meters, x ∈ (0, 105), y ∈ (0, 68)
--   (Note: tracking uses center-origin; events use pitch-origin per DFL XML spec)
--   Target: (0,0) = bottom-left, (120,80) = top-right (StatsBomb system)
--   Transform: x_out = x / 105.0 * 120.0
--              y_out = y / 68.0 * 80.0
--
-- Reference: Bassek et al. (2025), Scientific Data, Nature. CC-BY 4.0.
-- Enabled by pausa_enabled toggle (events consumed by ELASTIC sync → PAUSA pipeline).

{{ config(enabled=var('pausa_enabled', false)) }}

with source as (

    select * from {{ source('idsse', 'idsse_events') }}

),

normalized as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['match_id', 'event_id']) }} as event_sk,

        -- Match context
        match_id,

        -- Event identity
        event_id,
        event_type,

        -- Timing
        timestamp_seconds,
        cast(period as int)                             as period,

        -- Player identity
        player_id,
        team,

        -- Source provider
        'idsse'                                         as source_provider,

        -- Scaled event coordinates (120×80) — events use pitch-origin (0-105, 0-68)
        {{ normalize_x('x', 'pitch_m') }} as x,
        {{ normalize_y('y', 'pitch_m') }} as y

    from source
    where x is not null
      and y is not null

)

select * from normalized
