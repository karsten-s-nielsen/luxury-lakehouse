{{ config(
    materialized='table',
    liquid_clustered_by=['match_key'],
    tags=['marts', 'input_mart']
) }}
-- fct_discipline_events.sql
-- Per-event discipline mart. One row per card issued (Yellow, Red, or Second
-- Yellow) on either a 'Bad Behaviour' or 'Foul Committed' event. Used by the
-- Match Summary redesign (Row 1 auto-include of red cards + Row 2 red-card
-- markers on the xG race chart).
--
-- Motivation: StatsBomb card data is NOT flattened by statsbombpy — it lives
-- in the `bad_behaviour.card.name` / `foul_committed.card.name` paths inside
-- the `_raw_extra_json` column on bronze. The staging model
-- `stg_statsbomb__events` now exposes a `card_name` column that coalesces
-- across both sources. This mart projects the relevant columns to gold with
-- a narrow schema suitable for Lakebase sync + point-lookup queries.
--
-- Grain: one row per discipline event (event_type IN ('Bad Behaviour',
--        'Foul Committed') AND card_name IS NOT NULL).
-- Row estimate: ~14,100 across all StatsBomb data (~13,400 yellow,
--               ~700 red/second-yellow). Liquid clustering on match_key
--               for sub-10ms point lookups from Taipy.
-- Data source: 'statsbomb' only (Wyscout events lack card metadata in
--              the current bronze schema; add UNION ALL if that changes).
--
-- PR 7 (ADR-011 close-out): adds Kimball surrogate FKs match_key + team_key
-- + player_key via dim_matches/dim_teams/dim_players LEFT JOINs (provider
-- hardcoded to 'statsbomb'). Liquid clustering moves to match_key.

with source as (

    select
        event_id,
        match_id,
        competition_id,
        season_id,
        period,
        minute,
        second,
        player_id,
        team_id,
        event_type,
        card_name
    from {{ ref('stg_statsbomb__events') }}
    where card_name is not null

),

final as (

    select
        cast(s.event_id as string)                      as event_id,
        cast(s.match_id as bigint)                      as match_id,
        dm.match_key,
        cast(s.competition_id as int)                   as competition_id,
        cast(s.season_id as int)                        as season_id,
        cast(s.period as int)                           as period,
        cast(s.minute as int)                           as minute,
        cast(s.second as int)                           as second,
        cast(s.player_id as int)                        as player_id,
        dp.player_key,
        cast(s.team_id as int)                          as team_id,
        dt.team_key,
        cast(s.event_type as string)                    as event_type,
        cast(s.card_name as string)                     as card_name,
        cast('statsbomb' as string)                     as data_source,
        current_timestamp()                             as _loaded_at

    from source s
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = 'statsbomb'
       and dm.native_match_id = cast(s.match_id as string)
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = 'statsbomb'
       and dt.native_team_id = cast(s.team_id as string)
    left join {{ ref('dim_players') }} dp
        on  dp.provider = 'statsbomb'
       and dp.native_player_id = cast(s.player_id as string)

)

select * from final
