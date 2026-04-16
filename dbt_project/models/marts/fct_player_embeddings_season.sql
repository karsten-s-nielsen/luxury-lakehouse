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

with player_best_dim as (
    -- For players with mixed-dimension vectors (32d v1 + 128d v2),
    -- keep only the highest-dimension embeddings per player.
    -- D62 2026-04-15: explicitly exclude 360-enriched rows (144d) so they
    -- do not promote over v2's 128d embeddings. The 360 aggregates live
    -- in fct_player_embeddings_season_360 / _career_360 with their own
    -- dimensionally-homogeneous aggregation.
    select canonical_player_id, max(size(behavioral_vector)) as best_dim
    from {{ ref('fct_player_embeddings') }}
    where data_source != 'football2vec_360'
    group by canonical_player_id
),

embeddings_with_context as (

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
    inner join player_best_dim p
        on e.canonical_player_id = p.canonical_player_id
        and size(e.behavioral_vector) = p.best_dim
    -- D62 2026-04-15: 360-enriched embeddings live in their own mart; exclude here.
    where e.data_source != 'football2vec_360'

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
    -- Element-wise mean of behavioral vectors (dimension derived from data)
    transform(
        sequence(0, size(behavioral_vectors[0]) - 1),
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
