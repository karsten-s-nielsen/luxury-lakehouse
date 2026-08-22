-- stg_gkdv.sql
-- Staging view for per-keeper-pooled GKDV (GK Deterrent Value) (spec §7.5, Task 17h; ADR-013 writer-fed).
-- Source: bronze.gkdv_keeper_pooled, written by ingestion.gkdv_writer (silly-kicks 4.87.0 gkdv:
-- build_ghost_frames -> delta_das/delta_threat_suppression per scored+defending frame ->
-- aggregate_by_keeper, partitioned by (competition, season)). Deduplicates by
-- (data_source, player_id, competition_id, season_id), latest _ingested_at wins, then resolves the
-- native identifiers to the SAME Kimball surrogates the mart is keyed on (review-4 B2 — a mismatch
-- lands the fct_gk_shot_stopping_pooled join all-NULL):
--   * player_key <- LEFT JOIN dim_players on (provider, native_player_id).
--   * competition_key <- generate_competition_key(data_source, competition_id) — the SAME macro
--     dim_matches uses, on the SAME native competition_id, so the surrogate is identical.
--   * season_id <- cast(season_id as int) — the SAME cast fct_shot_psxg applies to dim_matches.season_id
--     (idsse/metrica are NULL by dim_matches fiat, which the writer already emits).
-- gkdv API is evolving upstream; pinned to the silly-kicks 4.87.0 surface.

with source as (

    select * from {{ source('gkdv', 'gkdv_keeper_pooled') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by data_source, player_id, competition_id, season_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(data_source as string)                         as data_source,
        cast(player_id as string)                           as player_id_native,
        cast(competition_id as string)                      as competition_id_native,
        cast(season_id as string)                           as season_id_native,
        cast(gkdv_delta_das_mean as double)                 as gkdv_delta_das_mean,
        cast(gkdv_delta_das_median as double)               as gkdv_delta_das_median,
        cast(gkdv_delta_das_n as bigint)                    as gkdv_delta_das_n,
        cast(gkdv_delta_das_n_nonzero as bigint)            as gkdv_delta_das_n_nonzero,
        cast(gkdv_delta_das_n_games as bigint)              as gkdv_delta_das_n_games,
        cast(gkdv_delta_das_gate_eligible as boolean)       as gkdv_delta_das_gate_eligible,
        cast(gkdv_delta_threat_mean as double)              as gkdv_delta_threat_mean,
        cast(gkdv_delta_threat_median as double)            as gkdv_delta_threat_median,
        cast(gkdv_delta_threat_n as bigint)                 as gkdv_delta_threat_n,
        cast(gkdv_delta_threat_n_nonzero as bigint)         as gkdv_delta_threat_n_nonzero,
        cast(gkdv_delta_threat_n_games as bigint)           as gkdv_delta_threat_n_games,
        cast(gkdv_delta_threat_gate_eligible as boolean)    as gkdv_delta_threat_gate_eligible

    from deduplicated
    where _row_num = 1

),

resolved as (

    select
        dp.player_key,
        {{ generate_competition_key('c.data_source', 'c.competition_id_native') }} as competition_key,
        cast(c.season_id_native as int)                     as season_id,
        c.data_source,
        c.gkdv_delta_das_mean,
        c.gkdv_delta_das_median,
        c.gkdv_delta_das_n,
        c.gkdv_delta_das_n_nonzero,
        c.gkdv_delta_das_n_games,
        c.gkdv_delta_das_gate_eligible,
        c.gkdv_delta_threat_mean,
        c.gkdv_delta_threat_median,
        c.gkdv_delta_threat_n,
        c.gkdv_delta_threat_n_nonzero,
        c.gkdv_delta_threat_n_games,
        c.gkdv_delta_threat_gate_eligible
    from cleaned c
    left join {{ ref('dim_players') }} dp
        on  dp.provider = c.data_source
       and dp.native_player_id = c.player_id_native

)

select * from resolved
