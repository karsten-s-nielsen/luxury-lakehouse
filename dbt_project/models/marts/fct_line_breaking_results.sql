{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='line_breaking_id',
    on_schema_change='fail',
    liquid_clustered_by=['match_id']
) }}
-- fct_line_breaking_results.sql
-- Gold-layer line-breaking detection results per pass event.
--
-- Each row records whether a pass was line-breaking, how many defensive
-- lines were broken, and the type of line break. One row per event.
--
-- Incremental: only processes match_ids not yet present in this table.
-- The surrogate key is (event_id).

with

{% if is_incremental() %}
existing_matches as (
    select distinct match_id from {{ this }}
),
{% endif %}

line_breaking as (

    select * from {{ ref('stg_line_breaking__results') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'line_breaking.event_id'
        ]) }}                                       as line_breaking_id,

        line_breaking.event_id,
        line_breaking.match_id,
        line_breaking.is_line_breaking,
        line_breaking.lines_broken,
        line_breaking.line_breaking_type,
        line_breaking.data_source

    from line_breaking

)

select * from final
