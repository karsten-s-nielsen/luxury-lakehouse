{{ config(enabled=var('embeddings_enabled', false), severity='error') }}

-- Per-match HF redistribution (spec 2026-06-29 §6.8 / Task 17 M2).
--
-- A football2vec career/season vector is a PRE-MIX of per-match embeddings that
-- cannot be split apart at publish time, so restricted matches must be excluded
-- UPSTREAM in fct_player_embeddings_{career,season}. This data test re-aggregates
-- the SAME population from PUBLIC rows only and asserts the published aggregate's
-- match count equals the public-only recount for every player (career) and every
-- (player, competition, season) (season).
--
-- For a player with both public and private matches: if the public filter were
-- ever dropped from the mart, its match count would include the private rows and
-- diverge from this public-only recompute → the offending key is returned → FAIL.
-- Integer-exact (no float comparison); mirrors the mart's best-dim + 360 filters.
-- Runs only in the daily dbt build (parse-only PR CI cannot execute it).

with career_public_best_dim as (

    select canonical_player_id, max(size(behavioral_vector)) as best_dim
    from {{ ref('fct_player_embeddings') }}
    where access_tier = 'public'
      and data_source != 'football2vec_360'
      and size(behavioral_vector) != 32
    group by canonical_player_id

),

career_public_recount as (

    select e.canonical_player_id, count(*) as n_public
    from {{ ref('fct_player_embeddings') }} e
    inner join career_public_best_dim p
        on e.canonical_player_id = p.canonical_player_id
        and size(e.behavioral_vector) = p.best_dim
    where e.access_tier = 'public'
      and e.data_source != 'football2vec_360'
    group by e.canonical_player_id

),

career_violations as (

    select
        'career'                                as grain,
        cast(m.canonical_player_id as string)   as key_1,
        cast(null as string)                    as key_2,
        cast(null as string)                    as key_3
    from {{ ref('fct_player_embeddings_career') }} m
    inner join career_public_recount r
        on r.canonical_player_id = m.canonical_player_id
    where m.total_matches != r.n_public

),

season_public_best_dim as (

    select canonical_player_id, max(size(behavioral_vector)) as best_dim
    from {{ ref('fct_player_embeddings') }}
    where access_tier = 'public'
      and data_source != 'football2vec_360'
      and size(behavioral_vector) != 32
    group by canonical_player_id

),

season_public_recount as (

    select
        e.canonical_player_id,
        ms.competition_id,
        ms.season_id,
        count(*) as n_public
    from {{ ref('fct_player_embeddings') }} e
    inner join {{ ref('fct_match_summary') }} ms
        on ms.match_key = e.match_key
    inner join season_public_best_dim p
        on e.canonical_player_id = p.canonical_player_id
        and size(e.behavioral_vector) = p.best_dim
    where e.access_tier = 'public'
      and e.data_source != 'football2vec_360'
    group by e.canonical_player_id, ms.competition_id, ms.season_id

),

season_violations as (

    select
        'season'                                as grain,
        cast(m.canonical_player_id as string)   as key_1,
        cast(m.competition_id as string)        as key_2,
        cast(m.season_id as string)             as key_3
    from {{ ref('fct_player_embeddings_season') }} m
    inner join season_public_recount r
        on  r.canonical_player_id = m.canonical_player_id
        and r.competition_id <=> m.competition_id
        and r.season_id <=> m.season_id
    where m.matches_in_sample != r.n_public

)

select * from career_violations
union all
select * from season_violations
