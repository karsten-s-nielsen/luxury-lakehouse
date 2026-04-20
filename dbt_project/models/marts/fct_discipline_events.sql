{{ config(
    materialized='table',
    liquid_clustered_by=['match_id']
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
--               ~700 red/second-yellow). Liquid clustering on match_id
--               for sub-10ms point lookups from Taipy.
-- Data source: 'statsbomb' only (Wyscout events lack card metadata in
--              the current bronze schema; add UNION ALL if that changes).

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
        cast(event_id as string)                        as event_id,
        cast(match_id as bigint)                        as match_id,
        cast(competition_id as int)                     as competition_id,
        cast(season_id as int)                          as season_id,
        cast(period as int)                             as period,
        cast(minute as int)                             as minute,
        cast(second as int)                             as second,
        cast(player_id as int)                          as player_id,
        cast(team_id as int)                            as team_id,
        cast(event_type as string)                      as event_type,
        cast(card_name as string)                       as card_name,
        cast('statsbomb' as string)                     as data_source,
        current_timestamp()                             as _loaded_at

    from source

)

select * from final
