{{ config(
    materialized='table',
    enabled=var('embeddings_enabled', false)
) }}

with match_context as (
    select
        pe.canonical_player_id,
        pe.match_id,
        pe.behavioral_vector,
        pe.data_source,
        ms.competition_id,
        ms.season_id
    from {{ ref('fct_player_embeddings') }} pe
    -- PR 2 (ADR-011) migrated fct_match_summary from match_id to match_key;
    -- fct_player_embeddings still keyed on native StatsBomb match_id (PR 5b
    -- migrates the embedding marts to player_key + match_key). Bridge via
    -- dim_matches until PR 5b: cast SB native match_id string to bigint to
    -- join the embedding's int match_id.
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = 'statsbomb'
       and try_cast(dm.native_match_id as bigint) = pe.match_id
    inner join {{ ref('fct_match_summary') }} ms
        on ms.match_key = dm.match_key
    where pe.data_source = 'football2vec_360'
),

grouped as (
    select
        canonical_player_id,
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
