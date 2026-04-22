{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='line_breaking_id',
    on_schema_change='fail',
    liquid_clustered_by=['match_key']
) }}
-- fct_line_breaking_results.sql
-- Gold-layer line-breaking detection results per pass event.
--
-- PR 2 (ADR-011) migrates the native `match_id` column to the Kimball
-- surrogate `match_key` (BIGINT FK to dim_matches). The upstream
-- `stg_line_breaking__results.match_id` has also been normalised as
-- part of this PR — IDSSE rows no longer carry the legacy `idsse_`
-- namespace prefix — so the join against dim_matches on
-- (provider, native_match_id) is direct.
--
-- data_source → dim_matches.provider mapping:
--   statsbomb_360    → statsbomb
--   metrica_tracking → metrica
--   idsse_tracking   → idsse

with line_breaking_raw as (

    select
        event_id,
        cast(match_id as string)                        as native_match_id,
        case data_source
            when 'statsbomb_360'    then 'statsbomb'
            when 'metrica_tracking' then 'metrica'
            when 'idsse_tracking'   then 'idsse'
            else data_source
        end                                             as provider,
        is_line_breaking,
        lines_broken,
        line_breaking_type,
        data_source
    from {{ ref('stg_line_breaking__results') }}

),

keyed as (

    select
        lb.event_id,
        dm.match_key,
        lb.is_line_breaking,
        lb.lines_broken,
        lb.line_breaking_type,
        lb.data_source
    from line_breaking_raw lb
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = lb.provider
       and dm.native_match_id = lb.native_match_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['event_id']) }}    as line_breaking_id,
        event_id,
        match_key,
        is_line_breaking,
        lines_broken,
        line_breaking_type,
        data_source

    from keyed

)

select * from final
