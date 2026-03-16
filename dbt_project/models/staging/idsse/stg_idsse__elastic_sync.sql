-- stg_idsse__elastic_sync.sql
-- Staging view joining ELASTIC sync results with event and tracking metadata.
--
-- Provides a unified view of event-to-frame alignments produced by the ELASTIC
-- algorithm (Kim et al. 2025, arXiv:2508.09238). Each row maps one event to
-- its best-matching tracking frame, with alignment confidence and error.
--
-- Enabled by pausa_enabled toggle (consumed by downstream PAUSA pipeline).

{{ config(enabled=var('pausa_enabled', false)) }}

with sync_results as (

    select * from {{ source('idsse', 'elastic_sync_results') }}

),

events as (

    select * from {{ ref('stg_idsse__events') }}

),

joined as (

    select
        -- Surrogate key for the alignment
        {{ dbt_utils.generate_surrogate_key(['s.match_id', 's.event_id']) }} as sync_id,

        -- Identifiers
        s.match_id,
        s.event_id,
        s.frame_id,

        -- Alignment quality
        cast(s.alignment_confidence as double)          as alignment_confidence,
        cast(s.alignment_error_seconds as double)       as alignment_error_seconds,

        -- Event context (from staging)
        e.event_type,
        e.period,
        e.player_id,
        e.team,
        e.timestamp_seconds                             as event_timestamp_seconds,

        -- Frame timestamp derived from frame_id and known 25fps rate
        cast(s.frame_id as double) / 25.0               as frame_timestamp_seconds,

        -- Source provider
        'idsse'                                         as source_provider

    from sync_results s
    inner join events e
        on s.match_id = e.match_id
       and s.event_id = e.event_id

)

select * from joined
