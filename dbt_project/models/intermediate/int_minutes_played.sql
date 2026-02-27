-- int_minutes_played.sql
-- Derive approximate minutes played per player per match from event data.
--
-- Materialized as ephemeral (CTE).
--
-- Logic:
--   - Players who appear in the starting lineup start at minute 0
--   - Substitution On events mark when a substitute enters
--   - Substitution Off events mark when a player leaves
--   - Match duration defaults to 90 minutes if no explicit end found
--   - Aggregate by player/competition/season for downstream per-90 calculations

with events as (

    select * from {{ ref('stg_statsbomb__events') }}

),

matches as (

    select
        match_id,
        competition_id,
        season_id
    from {{ ref('stg_statsbomb__matches') }}

),

lineups as (

    select
        match_id,
        player_id,
        position_name
    from {{ ref('stg_statsbomb__lineups') }}
    where position_name is not null

),

-- Find the last event minute per match as a proxy for match duration
match_duration as (

    select
        match_id,
        max(minute) + 1                                 as match_end_minute
    from events
    group by match_id

),

-- Substitution events: the player_id on the Substitution event is the player
-- going OFF. The replacement (player coming ON) is in the nested
-- substitution:replacement:id field, extracted from the raw bronze source.
substitution_off as (

    select
        match_id,
        player_id,
        minute                                          as off_minute
    from events
    where event_type = 'Substitution'

),

substitution_on as (

    select
        match_id,
        cast(substitution_replacement_id as int)            as player_id,
        minute                                              as on_minute
    from {{ ref('stg_statsbomb__events') }}
    where event_type = 'Substitution'
      and substitution_replacement_id is not null

),

-- Build minutes for each player
-- Starting players: on_minute = 0, off_minute = sub_off or match_end
-- Substitutes: on_minute = sub_on, off_minute = match_end
player_minutes as (

    -- Starting XI
    select
        l.match_id,
        l.player_id,
        0                                               as on_minute,
        coalesce(so.off_minute, md.match_end_minute)    as off_minute,
        coalesce(so.off_minute, md.match_end_minute)    as minutes_played
    from lineups l
    inner join match_duration md
        on l.match_id = md.match_id
    left join substitution_off so
        on l.match_id = so.match_id
        and l.player_id = so.player_id

    union all

    -- Substitutes coming on
    select
        son.match_id,
        son.player_id,
        son.on_minute,
        coalesce(soff.off_minute, md.match_end_minute)  as off_minute,
        coalesce(soff.off_minute, md.match_end_minute) - son.on_minute as minutes_played
    from substitution_on son
    inner join match_duration md
        on son.match_id = md.match_id
    left join substitution_off soff
        on son.match_id = soff.match_id
        and son.player_id = soff.player_id

),

-- Aggregate by player, competition, season
aggregated as (

    select
        pm.player_id,
        m.competition_id,
        m.season_id,
        sum(pm.minutes_played)                          as total_minutes_played
    from player_minutes pm
    inner join matches m
        on pm.match_id = m.match_id
    group by pm.player_id, m.competition_id, m.season_id

)

select * from aggregated
