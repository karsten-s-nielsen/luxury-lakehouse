{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='formation_label_id',
    on_schema_change='fail',
    liquid_clustered_by=['match_id']
) }}
-- fct_formation_labels.sql
-- Gold-layer formation detection results per match, period, and team.
--
-- Contains one row per detected formation window: the time range during which
-- a given team held a specific shape, as identified by the EFPI algorithm
-- (elastic template matching via the Hungarian method).
--
-- Incremental: only processes match_ids not yet present in this table.
-- The surrogate key is (match_id, period, team, window_start_s).
--
-- Coordinate system: timestamps in seconds from period start.
-- Reference: Shaw, L. & Glickman, M. (2019). "Dynamic analysis of team
-- strategy in professional football."

with

{% if is_incremental() %}
existing_matches as (
    select distinct match_id from {{ this }}
),
{% endif %}

formation_labels as (

    select * from {{ ref('stg_formations__labels') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'formation_labels.match_id',
            'formation_labels.period',
            'formation_labels.team',
            'formation_labels.window_start_s'
        ]) }}                                       as formation_label_id,

        formation_labels.match_id,
        formation_labels.period,
        formation_labels.team,
        formation_labels.window_start_s,
        formation_labels.window_end_s,
        formation_labels.formation_label,
        formation_labels.cost,
        formation_labels._ingested_at

    from formation_labels

)

select * from final
