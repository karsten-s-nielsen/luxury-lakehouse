-- stg_duels.sql
-- Staging view for per-(match, player) ground-duel Glicko-2 ratings (silly-kicks 4.120.0 TF-55; sk4118 P1
-- Task C2; ADR-013). Source: bronze.duels, written by ingestion.duels_writer (compute_duel_ratings, a
-- STATEFUL single-driver ordered pass — ratings carry forward across matches in ascending game_id).
-- Deduplicates by (data_source, match_id, player_id), latest _ingested_at wins, and casts the native
-- identifiers + access_tier + the 6 metric cols + duel_winner_source provenance to canonical types.
-- Kimball surrogates (match_key / player_key) resolve in the mart fct_player_match_metrics (INNER JOIN
-- dim_matches / dim_players on the native ids). access_tier (ADR-064) rides through per-row.

with source as (

    select * from {{ source('duels', 'duels') }}

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
    cast(data_source as string)                 as data_source,
    cast(match_id as string)                    as native_match_id,
    cast(player_id as string)                   as player_id_native,
    cast(team_id as string)                     as team_id_native,
    cast(access_tier as string)                 as access_tier,
    cast(duel_rating as double)                 as duel_rating,
    cast(duel_rating_deviation as double)       as duel_rating_deviation,
    cast(duel_volatility as double)             as duel_volatility,
    cast(duels_contested as bigint)             as duels_contested,
    cast(duels_won as bigint)                   as duels_won,
    cast(duels_lost as bigint)                  as duels_lost,
    cast(duel_winner_source as string)          as duel_winner_source

from deduplicated
where _row_num = 1
