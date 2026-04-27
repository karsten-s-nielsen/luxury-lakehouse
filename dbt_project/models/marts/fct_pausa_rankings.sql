{{ config(
    materialized='table',
    liquid_clustered_by=['player_id']
) }}
-- fct_pausa_rankings.sql
-- Player-level PAUSA aggregate with activity quality filters.
--
-- Aggregates pass-level PAUSA scores to one row per player across all matches.
-- Uses actual_obso > 0 as a quality proxy for "successful" passes, since
-- IDSSE event data lacks a pass outcome attribute.
--
-- Reference: Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa."
--
-- PR 7 (ADR-011 close-out): adds Kimball surrogate FK player_key (career
-- grain — no match/team) and a new pausa_ranking_id surrogate.

{% if var('pausa_enabled', false) %}

with pass_quality as (

    select * from {{ ref('int_pausa__pass_quality') }}

),

physical_minutes as (

    select
        cast(player_id as string) as player_id,
        sum(minutes_played)       as total_minutes
    from {{ ref('fct_physical_stats') }}
    group by cast(player_id as string)

),

aggregated as (

    select
        pq.player_id,
        max(pq.player_key)                                           as player_key,
        pq.player_display_name,
        count(distinct pq.match_id)                                  as total_matches,
        count(*)                                                     as total_passes,
        sum(case when pq.actual_obso > 0 then 1 else 0 end)         as passes_with_value,
        avg(pq.pausa_score)                                          as avg_pausa,
        avg(pq.temporal_judgment)                                    as avg_temporal_judgment,
        avg(pq.spatial_selection)                                    as avg_spatial_selection,
        percentile_approx(pq.pausa_score, 0.5)                      as median_pausa

    from pass_quality pq
    group by pq.player_id, pq.player_display_name

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['a.player_id']) }}      as pausa_ranking_id,
        cast(a.player_id as string)                                  as player_id,
        a.player_key,
        cast(a.player_display_name as string)                        as player_display_name,
        cast(a.total_matches as int)                                 as total_matches,
        cast(a.total_passes as int)                                  as total_passes,
        cast(a.passes_with_value as int)                             as passes_with_value,
        cast(a.avg_pausa as double)                                  as avg_pausa,
        cast(a.avg_temporal_judgment as double)                      as avg_temporal_judgment,
        cast(a.avg_spatial_selection as double)                      as avg_spatial_selection,
        cast(a.median_pausa as double)                               as median_pausa,
        cast(pm.total_minutes as double)                             as total_minutes,
        current_timestamp()                                          as _loaded_at

    from aggregated a
    left join physical_minutes pm
        on a.player_id = pm.player_id

)

select * from final

{% else %}

-- PAUSA not enabled — produce empty table with correct schema
select
    cast(null as string)    as pausa_ranking_id,
    cast(null as string)    as player_id,
    cast(null as bigint)    as player_key,
    cast(null as string)    as player_display_name,
    cast(null as int)       as total_matches,
    cast(null as int)       as total_passes,
    cast(null as int)       as passes_with_value,
    cast(null as double)    as avg_pausa,
    cast(null as double)    as avg_temporal_judgment,
    cast(null as double)    as avg_spatial_selection,
    cast(null as double)    as median_pausa,
    cast(null as double)    as total_minutes,
    current_timestamp()     as _loaded_at
where 1 = 0

{% endif %}
