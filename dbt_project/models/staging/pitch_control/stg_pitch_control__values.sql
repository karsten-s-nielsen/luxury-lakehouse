-- stg_pitch_control__values.sql
-- Clean and deduplicate pitch control values from the bronze layer +
-- derive Kimball-conformed FKs (PR 6, ADR-011).
--
-- Dedup: ROW_NUMBER partitioned by tracking_id, latest _ingested_at wins.
--
-- Provider derivation: data_source is derived from the match_id prefix
-- (idsse_*, Sample_Game_*, skillcorner_*). PR 7 will collapse this
-- to a passthrough once pitch_control_batch.py emits data_source
-- natively into bronze.
--
-- match_key resolved via LEFT JOIN dim_matches on (provider, native_match_id).
-- LEFT JOIN with severity:warn — preserves row counts during the
-- 2026-07-22 dual-column window.
--
-- Consumer: notebooks/publish_datasets.py:248 INNER JOINs on tracking_id
-- to publish luxury-lakehouse/pitch-control-tracking. Additive columns
-- don't break the JOIN.

with source as (

    select * from {{ source('pitch_control', 'pitch_control_values') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by tracking_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

with_provider as (

    select
        cast(tracking_id as string)              as tracking_id,
        cast(match_id as string)                 as match_id,
        cast(pitch_control_value as double)      as pitch_control_value,
        _ingested_at,
        case
            when match_id like 'idsse_%'       then 'idsse'
            when match_id like 'Sample_Game_%' then 'metrica'
            when match_id like 'skillcorner_%' then 'skillcorner'
            else cast(null as string)
        end                                      as data_source

    from deduplicated
    where _row_num = 1

),

cleaned as (

    select
        wp.tracking_id,
        wp.match_id,
        wp.pitch_control_value,
        wp._ingested_at,
        wp.data_source,
        dm.match_key

    from with_provider wp
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = wp.data_source
       and dm.native_match_id = regexp_replace(
               wp.match_id,
               '^(idsse_|Sample_Game_|skillcorner_)',
               ''
           )

)

select * from cleaned
