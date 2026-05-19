{{ config(
    materialized='table',
    enabled=var('embeddings_enabled', false),
    liquid_clustered_by=['canonical_player_id'],
    tags=['marts', 'output_mart']
) }}

-- PR 5b (ADR-011): retired dim_matches bridge (fct_player_embeddings now
-- carries match_key directly) and added player_key passthrough.

with match_context as (
    select
        pe.canonical_player_id,
        pe.player_key,
        pe.match_id,
        pe.match_key,
        pe.behavioral_vector,
        pe.data_source,
        ms.competition_id,
        ms.season_id
    from {{ ref('fct_player_embeddings') }} pe
    inner join {{ ref('fct_match_summary') }} ms
        on ms.match_key = pe.match_key
    where pe.data_source = 'football2vec_360'
),

grouped as (
    select
        canonical_player_id,
        any_value(player_key) as player_key,
        competition_id,
        season_id,
        collect_list(behavioral_vector) as behavioral_vectors,
        count(*) as total_matches,
        collect_set(data_source) as data_sources
    from match_context
    group by canonical_player_id, competition_id, season_id
)

select
    {{ dbt_utils.generate_surrogate_key(['canonical_player_id', 'competition_id', 'season_id']) }} as embedding_season_360_id,
    canonical_player_id,
    player_key,
    cast(competition_id as bigint) as competition_id,
    cast(season_id as bigint) as season_id,
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
