-- stg_pitch_control__values.sql
-- Clean and deduplicate pitch control values from the bronze layer.
--
-- Dedup: ROW_NUMBER partitioned by tracking_id, latest _ingested_at wins.
--
-- PR 7 (ADR-011 close-out): pitch_control_batch.py now emits data_source +
-- match_key natively into bronze, so the prefix-CASE bridge introduced in
-- PR 6 (§4.7) collapses to a passthrough. The schema-widening migration
-- runs once on deploy: drop bronze.pitch_control_values + re-trigger
-- wf-pitch-control to repopulate with the wider schema. Existing rows
-- carry NULL data_source / match_key until then — covered by the live-CI
-- coverage test thresholds.
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

cleaned as (

    -- PR 7 transitional: existing bronze rows pre-date the writer schema
    -- widening — they have NULL data_source / match_key. Backfill data_source
    -- via the PR 6 prefix-CASE on match_id so the downstream not_null test
    -- holds across both schemas. match_key stays NULL on legacy rows
    -- (relationships test gates on `where match_key IS NOT NULL`); Phase 2
    -- redeploy of wf-pitch-control with the widened writer populates both
    -- columns natively. After full re-population, the COALESCE branch
    -- becomes dead code and can be removed.
    select
        cast(tracking_id as string)              as tracking_id,
        cast(match_id as string)                 as match_id,
        coalesce(
            cast(data_source as string),
            case
                when match_id like 'idsse_%'        then 'idsse'
                when match_id like 'Sample_Game_%'  then 'metrica'
                when match_id like 'skillcorner_%'  then 'skillcorner'
            end
        )                                        as data_source,
        cast(match_key as bigint)                as match_key,
        cast(pitch_control_value as double)      as pitch_control_value,
        _ingested_at

    from deduplicated
    where _row_num = 1

)

select * from cleaned
