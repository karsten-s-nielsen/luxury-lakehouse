-- stg_territory.sql
-- Staging view for per-(match, player) territorial dominance (silly-kicks 4.120.0 TF-54/TF-54b; sk4118 P1
-- Task C1; ADR-013). Source: bronze.territory, written by ingestion.territory_writer
-- (compute_territorial_dominance BOTH methods). Deduplicates by (data_source, match_id, player_id),
-- latest _ingested_at wins, and casts the native identifiers + access_tier + the 20-col counterfactual
-- union (12 v1 metric + territory_hull_source + 4 cf metric + territory_target_source) to canonical types.
-- Kimball surrogates (match_key / player_key) resolve in the mart fct_player_match_metrics (INNER JOIN
-- dim_matches / dim_players on the native ids) — the native string ids are carried through. access_tier
-- (ADR-064) rides through per-row from the SPADL bronze stamp.

with source as (

    select * from {{ source('territory', 'territory') }}

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
    cast(data_source as string)                                 as data_source,
    cast(match_id as string)                                    as native_match_id,
    cast(player_id as string)                                   as player_id_native,
    cast(team_id as string)                                     as team_id_native,
    cast(access_tier as string)                                 as access_tier,
    -- territory v1 metric cols (12)
    cast(territory_xt_conceded as double)                       as territory_xt_conceded,
    cast(territory_xt_prevented as double)                      as territory_xt_prevented,
    cast(territory_xt_net as double)                            as territory_xt_net,
    cast(territory_xt_conceded_forward as double)               as territory_xt_conceded_forward,
    cast(territory_xt_prevented_forward as double)              as territory_xt_prevented_forward,
    cast(territory_passes_into_hull as bigint)                  as territory_passes_into_hull,
    cast(territory_xt_conceded_rate as double)                  as territory_xt_conceded_rate,
    cast(territory_xt_prevented_rate as double)                 as territory_xt_prevented_rate,
    cast(territory_hull_area_m2 as double)                      as territory_hull_area_m2,
    cast(territory_hull_centroid_x as double)                   as territory_hull_centroid_x,
    cast(territory_hull_centroid_y as double)                   as territory_hull_centroid_y,
    cast(territory_defensive_actions_in_hull as bigint)         as territory_defensive_actions_in_hull,
    -- v1 provenance
    cast(territory_hull_source as string)                       as territory_hull_source,
    -- counterfactual-only cols (5): 4 metric + target_source provenance [SPEC-R5-01]
    cast(territory_expected_threat_faced as double)             as territory_expected_threat_faced,
    cast(territory_xt_prevented_above_expectation as double)    as territory_xt_prevented_above_expectation,
    cast(territory_passes_aimed_into_hull as bigint)            as territory_passes_aimed_into_hull,
    cast(territory_mean_completion_faced as double)             as territory_mean_completion_faced,
    cast(territory_target_source as string)                     as territory_target_source

from deduplicated
where _row_num = 1
