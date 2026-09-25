-- stg_match_outcome.sql
-- Staging view for per-(match, team) match-outcome win probabilities (silly-kicks 4.120.0 TF-53;
-- sk4118 P1 Task A2; ADR-013). Source: bronze.match_outcome, written by ingestion.match_outcome_writer
-- (compute_match_outcome: p_win/p_draw/p_loss/xpoints/expected_goals, integrated from per-shot xG).
-- Deduplicates by (data_source, match_id, team_id), latest _ingested_at wins, and casts the native
-- identifiers + access_tier to canonical types. Kimball surrogates resolve in the shared team-match
-- mart fct_team_metrics (INNER JOIN dim_matches / dim_teams on the native ids).

with source as (

    select * from {{ source('match_outcome', 'match_outcome') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by data_source, match_id, team_id
            order by _ingested_at desc
        ) as _row_num
    from source

)

select
    cast(data_source as string)         as data_source,
    cast(match_id as string)            as native_match_id,
    cast(team_id as string)             as team_id_native,
    cast(access_tier as string)         as access_tier,
    cast(p_win as double)               as p_win,
    cast(p_draw as double)              as p_draw,
    cast(p_loss as double)              as p_loss,
    cast(xpoints as double)             as xpoints,
    cast(expected_goals as double)      as expected_goals

from deduplicated
where _row_num = 1
