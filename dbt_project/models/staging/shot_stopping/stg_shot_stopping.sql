-- stg_shot_stopping.sql
-- Staging view for per-(match, defending keeper) shot-stopping (silly-kicks 4.120.0 TF-59; sk4118 P1
-- Task B1; ADR-013). Source: bronze.shot_stopping, written by ingestion.shot_stopping_writer
-- (compute_shot_stopping — Goals Prevented / GSAA, 8 metric cols incl. the 4 _excl_penalties companions).
-- Deduplicates by (data_source, match_id, player_id), latest _ingested_at wins, and casts the native
-- identifiers + access_tier to canonical types. Kimball surrogates (player_key / match_key) resolve in
-- the GK marts (fct_gk_shot_stopping via dim_players / dim_matches on the native ids). player_id is the
-- NATIVE defending keeper id; team_id the native keeper team. access_tier (ADR-064) rides through per-row.

with source as (

    select * from {{ source('shot_stopping', 'shot_stopping') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by data_source, match_id, player_id
            order by _ingested_at desc
        ) as _row_num
    from source

)

select
    cast(data_source as string)                             as data_source,
    cast(match_id as string)                                as native_match_id,
    cast(player_id as string)                               as native_player_id,
    cast(team_id as string)                                 as team_id_native,
    cast(access_tier as string)                             as access_tier,
    cast(shots_faced as bigint)                             as shots_faced,
    cast(goals_conceded as bigint)                          as goals_conceded,
    cast(psxg_faced as double)                              as psxg_faced,
    cast(goals_prevented as double)                         as goals_prevented,
    cast(shots_faced_excl_penalties as bigint)             as shots_faced_excl_penalties,
    cast(goals_conceded_excl_penalties as bigint)          as goals_conceded_excl_penalties,
    cast(psxg_faced_excl_penalties as double)              as psxg_faced_excl_penalties,
    cast(goals_prevented_excl_penalties as double)         as goals_prevented_excl_penalties

from deduplicated
where _row_num = 1
