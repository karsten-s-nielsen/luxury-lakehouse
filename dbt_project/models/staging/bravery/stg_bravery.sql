-- stg_bravery.sql
-- Staging view for per-(match, defending team) bravery (spec §7.5, Task 17g; ADR-013 writer-fed).
-- Source: bronze.bravery, written by ingestion.bravery_writer (silly-kicks 4.87.0 compute_bravery).
-- Deduplicates by (data_source, match_id, team_id), latest _ingested_at wins, then resolves the
-- native identifiers to Kimball surrogates (review-4 B2 — a hashed BIGINT team_id would land all-NULL):
--   * match_key <- INNER JOIN dim_matches on (provider, native_match_id).
--   * team_key (the DEFENDING team) <- LEFT JOIN dim_teams on (provider, native_team_id). LEFT so a
--     defending team with no dim_teams row still ships (NULL team_key just won't join the mart).
-- fct_match_summary is one-row-per-match with home_/away_ pivots, so it LEFT-JOINs this view TWICE
-- (team_key = home_team_key, team_key = away_team_key) -> home_bravery_* / away_bravery_*.

with source as (

    select * from {{ source('bravery', 'bravery') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by data_source, match_id, team_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(data_source as string)                    as data_source,
        cast(match_id as string)                       as native_match_id,
        cast(team_id as string)                        as team_id_native,
        cast(bravery_shots as double)                  as bravery_shots,
        cast(bravery_open_play_crosses as double)      as bravery_open_play_crosses,
        cast(bravery_set_piece_crosses as double)      as bravery_set_piece_crosses,
        cast(bravery_pct_known_domain as double)       as bravery_pct_known_domain,
        cast(n_shots_faced as bigint)                  as n_shots_faced,
        cast(n_open_play_crosses_faced as bigint)      as n_open_play_crosses_faced,
        cast(n_set_piece_crosses_faced as bigint)      as n_set_piece_crosses_faced,
        cast(n_blocks_known as bigint)                 as n_blocks_known

    from deduplicated
    where _row_num = 1

),

resolved as (

    select
        dm.match_key,
        dt.team_key,
        c.data_source,
        c.bravery_shots,
        c.bravery_open_play_crosses,
        c.bravery_set_piece_crosses,
        c.bravery_pct_known_domain,
        c.n_shots_faced,
        c.n_open_play_crosses_faced,
        c.n_set_piece_crosses_faced,
        c.n_blocks_known
    from cleaned c
    inner join {{ ref('dim_matches') }} dm
        on  dm.provider = c.data_source
       and dm.native_match_id = c.native_match_id
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = c.data_source
       and dt.native_team_id = c.team_id_native

)

select * from resolved
