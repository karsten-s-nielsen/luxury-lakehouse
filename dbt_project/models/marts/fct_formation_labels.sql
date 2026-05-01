{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='formation_label_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key'],
    tags=['marts', 'output_mart']
) }}
-- fct_formation_labels.sql
-- Gold-layer formation detection results per match, period, and team.
--
-- Contains one row per detected formation window: the time range during which
-- a given team held a specific shape, as identified by either the EFPI algorithm
-- (elastic template matching via the Hungarian method) or the shape graph
-- detector (Sotudeh 2026).
--
-- Incremental: only processes match_ids not yet present in this table.
-- The surrogate key is (match_id, period, team, window_start_s, detector,
-- source_provider).
--
-- Coordinate system: timestamps in seconds from period start.
-- References:
--   Shaw, L. & Glickman, M. (2019). "Dynamic analysis of team strategy in
--   professional football."
--   Sotudeh, S. (2026). Shape graph formation detection.
--
-- PR 7 (ADR-011 close-out): adds Kimball surrogate FKs match_key + team_key.
-- The bronze `team` column is a 'home'/'away' role string, so team_key is
-- resolved via fct_match_summary JOIN on match_key + CASE on team. match_key
-- resolves via dim_matches JOIN on the staging-derived source_provider.
-- Surrogate-key inputs gain source_provider for provider-stable formation_label_id.

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
            'formation_labels.window_start_s',
            'formation_labels.detector',
            'formation_labels.source_provider'
        ]) }}                                       as formation_label_id,

        formation_labels.match_id,
        dm.match_key,
        formation_labels.period,
        formation_labels.team,
        case
            when formation_labels.team = 'home' then ms.home_team_key
            when formation_labels.team = 'away' then ms.away_team_key
        end                                         as team_key,
        formation_labels.window_start_s,
        formation_labels.window_end_s,
        formation_labels.formation_label,
        formation_labels.cost,
        formation_labels.detector,
        formation_labels.source_provider            as data_source,
        formation_labels._ingested_at

    from formation_labels
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = formation_labels.source_provider
       and dm.native_match_id = formation_labels.match_id
    left join {{ ref('fct_match_summary') }} ms
        on  ms.match_key = dm.match_key

)

select * from final
