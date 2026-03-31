-- fct_player_embeddings_season.sql
-- Per player-competition-season aggregation of embedding vectors.
--
-- Joins match-level embeddings with fct_match_summary to derive
-- competition and season context. Both behavioral and stat vectors
-- are aggregated via element-wise mean. NULL stat_vectors are excluded
-- from the stat mean.
--
-- Grain: one row per player per competition per season.

{{ config(
    materialized='table',
    enabled=var('embeddings_enabled', false)
) }}

with embeddings_with_context as (

    select
        e.canonical_player_id,
        e.match_id,
        e.data_source,
        e.behavioral_vector,
        e.stat_vector,
        m.competition_id,
        m.season_id
    from {{ ref('fct_player_embeddings') }} e
    inner join {{ ref('fct_match_summary') }} m
        on e.match_id = m.match_id

),

grouped as (

    select
        canonical_player_id,
        competition_id,
        season_id,
        collect_list(behavioral_vector)                       as behavioral_vectors,
        filter(collect_list(stat_vector), v -> v is not null) as non_null_stat_vectors,
        count(*)                                              as matches_in_sample,
        collect_set(data_source)                              as data_sources
    from embeddings_with_context
    group by canonical_player_id, competition_id, season_id

)

select
    {{ dbt_utils.generate_surrogate_key(['canonical_player_id', 'competition_id', 'season_id']) }}
        as embedding_season_id,
    canonical_player_id,
    competition_id,
    season_id,
    -- Element-wise mean of behavioral vectors
    transform(
        sequence(0, 127),
        i -> aggregate(
            behavioral_vectors,
            cast(0.0 as double),
            (acc, vec) -> acc + vec[i],
            acc -> acc / size(behavioral_vectors)
        )
    ) as behavioral_vector,
    -- Element-wise mean of stat vectors (pre-filtered NULLs in CTE)
    case
        when size(non_null_stat_vectors) > 0
        then transform(
            sequence(0, 12),
            i -> aggregate(
                non_null_stat_vectors,
                cast(0.0 as double),
                (acc, vec) -> acc + coalesce(vec[i], 0.0),
                acc -> acc / size(non_null_stat_vectors)
            )
        )
        else null
    end as stat_vector,
    matches_in_sample,
    data_sources
from grouped
