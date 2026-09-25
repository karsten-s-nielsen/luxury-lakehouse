-- stg_rest_defense.sql
-- Staging view for rest-defense L1 + L2 samples (silly-kicks 4.102/4.103; sk4118 P1 Phase E; ADR-013).
-- Source: bronze.rest_defense, written by ingestion.restdefense_writer (per tracking work unit,
-- silly-kicks compute_rest_defense). Grain: one row per (data_source, match_id, period_id, team_id,
-- action_id) = the DEFENDING team's rest-defence geometry at that in-possession action. Deduplicates by
-- that key, latest _ingested_at wins, and casts to canonical types + renames match_id -> native_match_id
-- for the Kimball-side resolution in fct_rest_defense. team_id_native is the mapped native defending-team
-- id (dim_teams join). Layer-2 cols are NaN without a fitted xt (ADR-033).

with source as (

    select * from {{ source('restdefense', 'rest_defense') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by data_source, match_id, period_id, team_id, action_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(data_source as string)                     as data_source,
        cast(match_id as string)                        as native_match_id,
        cast(team_id_native as string)                  as team_id_native,
        cast(period_id as bigint)                       as period_id,
        cast(action_id as bigint)                       as action_id,
        cast(possession_id as bigint)                   as possession_id,
        cast(is_possession_loss as boolean)             as is_possession_loss,

        -- Layer 1 (11 descriptive geometry cols)
        cast(rd_num_superiority as bigint)              as rd_num_superiority,
        cast(rd_num_superiority_gk as bigint)           as rd_num_superiority_gk,
        cast(rd_zone_occupancy as bigint)               as rd_zone_occupancy,
        cast(rd_line_height as double)                  as rd_line_height,
        cast(rd_line_height_relative as double)         as rd_line_height_relative,
        cast(rd_compactness_x as double)                as rd_compactness_x,
        cast(rd_width as double)                        as rd_width,
        cast(rd_depth as double)                        as rd_depth,
        cast(rd_shape_2_3_vs_3_2 as string)             as rd_shape_2_3_vs_3_2,
        cast(rd_gk_line_height as double)               as rd_gk_line_height,
        cast(rd_gk_to_line_distance as double)          as rd_gk_to_line_distance,

        -- Layer 2 (5 xT / space-control cols — NaN without a fitted xt)
        cast(rd_attacker_space_control as double)       as rd_attacker_space_control,
        cast(rd_danger_behind_line as double)           as rd_danger_behind_line,
        cast(rd_danger_behind_line_gk as double)        as rd_danger_behind_line_gk,
        cast(rd_gk_coverage_behind_line as double)      as rd_gk_coverage_behind_line,
        cast(rd_gk_reachable_coverage_m2 as double)     as rd_gk_reachable_coverage_m2,

        -- Provenance
        cast(rd_geometry_source as string)              as rd_geometry_source,
        cast(access_tier as string)                     as access_tier

    from deduplicated
    where _row_num = 1

)

select * from cleaned
