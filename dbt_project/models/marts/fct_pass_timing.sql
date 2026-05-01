{{ config(
    materialized='table',
    liquid_clustered_by=['match_key'],
    tags=['marts', 'output_mart']
) }}
-- fct_pass_timing.sql
-- Per-player per-match PAUSA pass timing aggregation.
--
-- Aggregates PAUSA temporal judgment, spatial selection, and composite score
-- at the player-match grain. Used by the Pass Timing Streamlit page for
-- player rankings and comparative analysis.
--
-- One row per player per match.
--
-- PR 7 (ADR-011 close-out): adds Kimball surrogate FKs match_key + player_key
-- and a new pass_timing_id surrogate (over player_id, match_id) for stable
-- downstream FK references.

{% if var('pausa_enabled', false) %}

with pass_quality as (

    select * from {{ ref('int_pausa__pass_quality') }}

),

-- Compute median PAUSA per player-match using percentile_approx
aggregated as (

    select
        player_id,
        match_id,
        player_display_name,
        max(player_key)                                             as player_key,
        max(match_key)                                              as match_key,

        count(*)                                                    as pass_count,
        avg(temporal_judgment)                                      as avg_temporal_judgment,
        avg(spatial_selection)                                      as avg_spatial_selection,
        avg(pausa_score)                                            as avg_pausa,
        percentile_approx(pausa_score, 0.5)                        as median_pausa,
        sum(case when pausa_score > 0.5 then 1 else 0 end)         as passes_above_median_pausa

    from pass_quality
    group by player_id, match_id, player_display_name

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'player_id',
            'match_id'
        ]) }}                                                       as pass_timing_id,
        cast(player_id as string)                                   as player_id,
        player_key,
        cast(match_id as string)                                    as match_id,
        match_key,
        cast(player_display_name as string)                         as player_display_name,
        cast(pass_count as int)                                     as pass_count,
        cast(avg_temporal_judgment as double)                        as avg_temporal_judgment,
        cast(avg_spatial_selection as double)                        as avg_spatial_selection,
        cast(avg_pausa as double)                                   as avg_pausa,
        cast(median_pausa as double)                                as median_pausa,
        cast(passes_above_median_pausa as int)                      as passes_above_median_pausa,
        current_timestamp()                                         as _loaded_at

    from aggregated

)

select * from final

{% else %}

-- PAUSA not enabled — produce empty table with correct schema
select
    cast(null as string)    as pass_timing_id,
    cast(null as string)    as player_id,
    cast(null as bigint)    as player_key,
    cast(null as string)    as match_id,
    cast(null as bigint)    as match_key,
    cast(null as string)    as player_display_name,
    cast(null as int)       as pass_count,
    cast(null as double)    as avg_temporal_judgment,
    cast(null as double)    as avg_spatial_selection,
    cast(null as double)    as avg_pausa,
    cast(null as double)    as median_pausa,
    cast(null as int)       as passes_above_median_pausa,
    current_timestamp()     as _loaded_at
where 1 = 0

{% endif %}
