{{ config(
    materialized='table',
    enabled=var('embeddings_enabled', false),
    tags=['marts', 'output_mart']
) }}

-- PR 5b (ADR-011): added player_key passthrough.

with grouped as (
    select
        canonical_player_id,
        any_value(player_key) as player_key,
        collect_list(behavioral_vector) as behavioral_vectors,
        count(*) as total_matches,
        collect_set(data_source) as data_sources
    from {{ ref('fct_player_embeddings') }}
    where data_source = 'football2vec_360'
    group by canonical_player_id
)

select
    {{ dbt_utils.generate_surrogate_key(['canonical_player_id']) }} as embedding_career_360_id,
    canonical_player_id,
    player_key,
    transform(
        sequence(0, 143),
        i -> aggregate(
            behavioral_vectors,
            cast(0.0 as double),
            (acc, arr) -> acc + arr[i]
        ) / size(behavioral_vectors)
    ) as behavioral_vector,
    total_matches,
    data_sources
from grouped
