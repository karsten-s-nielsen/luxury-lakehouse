-- fct_player_embeddings_career.sql
-- Career-level aggregation of player embedding vectors.
--
-- Aggregates all match-level embeddings across competitions and seasons
-- into a single career embedding per player. Both behavioral and stat
-- vectors are averaged element-wise. NULL stat_vectors are excluded
-- from the stat mean.
--
-- Grain: one row per player (canonical_player_id).
--
-- PR 5b (ADR-011): added player_key passthrough.

{{ config(
    materialized='table',
    enabled=var('embeddings_enabled', false),
    tags=['marts', 'output_mart']
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
      and data_source != 'football2vec_v1'   -- PR-Cycle-C 2026-05-01: exclude 32d v1 Doc2Vec.
                                             -- v1 is "Retained for comparison; superseded by v2"
                                             -- per terraform/modules/workflows/main.tf:22-24.
                                             -- Mixed-dim career rows broke HNSW build at vector(192).
    group by canonical_player_id
),

grouped as (

    select
        e.canonical_player_id,
        any_value(e.player_key)                                 as player_key,
        collect_list(e.behavioral_vector)                       as behavioral_vectors,
        filter(collect_list(e.stat_vector), v -> v is not null) as non_null_stat_vectors,
        count(*)                                                as total_matches,
        collect_set(e.data_source)                              as data_sources
    from {{ ref('fct_player_embeddings') }} e
    inner join player_best_dim p
        on e.canonical_player_id = p.canonical_player_id
        and size(e.behavioral_vector) = p.best_dim
    -- D62 2026-04-15: 360-enriched embeddings live in their own mart; exclude here.
    where e.data_source != 'football2vec_360'
    group by e.canonical_player_id

)

select
    canonical_player_id,
    player_key,
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
    total_matches,
    data_sources
from grouped
