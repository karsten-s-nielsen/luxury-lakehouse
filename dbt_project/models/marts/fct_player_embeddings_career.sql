-- fct_player_embeddings_career.sql
-- Career-level aggregation of player embedding vectors.
--
-- Aggregates all match-level embeddings across competitions and seasons
-- into a single career embedding per player. Both behavioral and stat
-- vectors are averaged element-wise. NULL stat_vectors are excluded
-- from the stat mean.
--
-- Grain: one row per player (canonical_player_id).

{{ config(
    materialized='table',
    enabled=var('embeddings_enabled', false)
) }}

with grouped as (

    select
        canonical_player_id,
        collect_list(behavioral_vector)                    as behavioral_vectors,
        filter(collect_list(stat_vector), v -> v is not null) as non_null_stat_vectors,
        count(*)                                           as total_matches,
        collect_set(data_source)                           as data_sources
    from {{ ref('fct_player_embeddings') }}
    group by canonical_player_id

)

select
    canonical_player_id,
    -- Element-wise mean of behavioral vectors
    transform(
        sequence(0, 31),
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
    total_matches,
    data_sources
from grouped
