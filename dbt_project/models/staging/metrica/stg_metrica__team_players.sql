-- stg_metrica__team_players.sql
-- Per-match team + player identity for Metrica.
--
-- Metrica sample data is anonymised: home_players + away_players are
-- MAP<STRING, STRUCT<...>> JSON columns where keys are "Player11"-"Player25"
-- style strings with no real identity. Per ADR-011 + PR 5a design: synthesise
-- per-match team + player IDs rather than fabricating cross-match identity
-- we can't verify.
--
-- Forward-compat: bronze `is_anonymized` flag drives synthesis branch.
-- Future subscription data (is_anonymized=false) flows through a parallel
-- real-identity branch (zero rows on current data).
--
-- Grain:
--   Teams: one row per (match_id, side).
--   Players: one row per (match_id, side, player_key_in_map).

with tracking as (

    select
        match_id,
        home_players,
        away_players,
        is_anonymized
    from {{ source('metrica', 'metrica_tracking') }}

),

home_exploded as (

    select distinct
        match_id,
        'home'                                          as side,
        is_anonymized,
        k                                               as player_key_in_map
    from tracking
    lateral view explode(
        from_json(home_players, 'MAP<STRING, STRUCT<x:DOUBLE, y:DOUBLE>>')
    ) AS k, v

),

away_exploded as (

    select distinct
        match_id,
        'away'                                          as side,
        is_anonymized,
        k                                               as player_key_in_map
    from tracking
    lateral view explode(
        from_json(away_players, 'MAP<STRING, STRUCT<x:DOUBLE, y:DOUBLE>>')
    ) AS k, v

),

all_team_players as (

    select * from home_exploded
    union all
    select * from away_exploded

),

final as (

    select
        match_id,
        side,
        is_anonymized,
        player_key_in_map,

        case
            when is_anonymized then concat('metrica_', match_id, '_', side)
            else cast(null as string)
        end                                             as native_team_id,

        case
            when is_anonymized then concat('metrica_', match_id, '_', side, '_', player_key_in_map)
            else cast(null as string)
        end                                             as native_player_id,

        case when is_anonymized then true else false end as is_synthesized,

        case
            when is_anonymized then 'metrica_anonymized'
            else cast(null as string)
        end                                             as synthesis_reason

    from all_team_players

)

select * from final
