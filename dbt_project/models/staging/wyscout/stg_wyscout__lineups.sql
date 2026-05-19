-- stg_wyscout__lineups.sql
-- Extract per-player lineup participation from Wyscout match metadata.
--
-- Source: stg_wyscout__matches.teams_data_parsed MAP.
-- Grain: one row per (match_id, team_id, player_id).
-- Each row carries pre-resolved minute_on/minute_off for direct minutes calculation.
--
-- Starters: minute_on = 0, minute_off = substitution minute (or NULL if played to end).
-- Subs entering: minute_on = substitution minute, minute_off = NULL (or next sub minute).
-- Bench players who never enter are excluded (they have no minutes).

with matches as (

    select
        match_id,
        teams_data_parsed
    from {{ ref('stg_wyscout__matches') }}
    where teams_data_parsed is not null

),

-- Explode the MAP to get one row per team per match.
teams as (

    select
        m.match_id,
        cast(t.key as string)             as team_id,
        t.value.formation                 as formation
    from matches m
    lateral view explode(m.teams_data_parsed) t as key, value
    where t.value.formation is not null

),

-- Starters from lineup array.
-- Type: playerId is BIGINT in the from_json MAP schema (stg_wyscout__matches:61).
-- Keep as BIGINT for consistency with stg_wyscout__events.player_id.
starters as (

    select
        t.match_id,
        t.team_id,
        cast(p.playerId as bigint)        as player_id,
        true                              as is_starter,
        0                                 as minute_on
    from teams t
    lateral view explode(t.formation.lineup) l as p

),

-- Substitutes entering from substitutions array.
subs_in as (

    select
        t.match_id,
        t.team_id,
        cast(s.playerIn as bigint)        as player_id,
        false                             as is_starter,
        cast(s.minute as int)             as minute_on
    from teams t
    lateral view explode(t.formation.substitutions) sub as s

),

-- Substitution-off events (starters leaving).
-- Assumption: each player appears at most once in subs_off per (match, team).
-- The unique_combination_of_columns test on (match_id, team_id, player_id)
-- catches any Wyscout data quality issue with duplicate substitution entries.
-- Standard single-chain subs (A->B at 60', B->C at 75') resolve correctly:
-- A gets minute_off=60, B gets minute_on=60 + minute_off=75.
subs_off as (

    select
        t.match_id,
        t.team_id,
        cast(s.playerOut as bigint)       as player_id,
        cast(s.minute as int)             as minute_off
    from teams t
    lateral view explode(t.formation.substitutions) sub as s

),

-- Combine starters + subs entering, then LEFT JOIN sub-off minute.
combined as (

    select * from starters
    union all
    select * from subs_in

),

final as (

    select
        c.match_id,
        c.team_id,
        c.player_id,
        c.is_starter,
        c.minute_on,
        so.minute_off
    from combined c
    left join subs_off so
        on  c.match_id = so.match_id
        and c.team_id = so.team_id
        and c.player_id = so.player_id
    -- Wyscout uses playerId=0 as a placeholder in lineup/substitution arrays.
    -- Filter at the staging boundary (same pattern as stg_spadl__action_values).
    where c.player_id != 0

)

select * from final
