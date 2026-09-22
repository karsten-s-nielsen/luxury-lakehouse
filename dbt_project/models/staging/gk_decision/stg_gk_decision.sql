-- stg_gk_decision.sql
-- Staging view for per-DECISION GK decision value (silly-kicks 4.120.0 TF-62; sk4118 P1 Task B2 /
-- Phase E; ADR-013). Source: bronze.gk_decision, written by ingestion.gk_decision_writer
-- (compute_gk_decision_value, RECONSTRUCTION TIER — SB360 freeze-frames + full-tracking, bundled
-- PassCompletionModel). sk returns one row per GK-distribution DECISION (decision_id == the SPADL
-- action_id), so the grain is per-(match, keeper, decision). Deduplicates by
-- (data_source, match_id, decision_id), latest _ingested_at wins, and casts the native identifiers +
-- access_tier to canonical types. Kimball surrogates (player_key / match_key) resolve in the mart
-- fct_gk_decision (dim_players / dim_matches on the native ids). keeper is the NATIVE goalkeeper id.
-- access_tier (ADR-064) rides through per-row. option_set_source distinguishes the reconstruction tier
-- from the not-yet-sourced native SkillCorner GI tier.

with source as (

    select * from {{ source('gk_decision', 'gk_decision') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by data_source, match_id, decision_id
            order by _ingested_at desc
        ) as _row_num
    from source

)

select
    cast(data_source as string)                             as data_source,
    cast(match_id as string)                                as native_match_id,
    cast(period_id as bigint)                               as period_id,
    cast(decision_id as bigint)                             as decision_id,
    cast(keeper as string)                                  as native_player_id,
    cast(team_id as string)                                 as team_id_native,
    cast(access_tier as string)                             as access_tier,
    cast(decision_value as double)                          as decision_value,
    cast(chosen_ev as double)                               as chosen_ev,
    cast(best_ev as double)                                 as best_ev,
    cast(sel_efficiency as double)                          as sel_efficiency,
    cast(decision_pct as double)                            as decision_pct,
    cast(n_options as bigint)                               as n_options,
    cast(option_set_source as string)                       as option_set_source

from deduplicated
where _row_num = 1
