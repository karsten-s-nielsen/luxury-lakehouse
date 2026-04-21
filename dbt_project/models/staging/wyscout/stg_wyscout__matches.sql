-- stg_wyscout__matches.sql
-- Wyscout match metadata with competition ID mapped to StatsBomb ID space.
--
-- The competition_id_mapping seed translates Wyscout-native competition IDs
-- (e.g., 795 for La Liga) to StatsBomb competition IDs (e.g., 11 for La Liga)
-- so all downstream models use a single competition ID space.

with source as (

    select * from {{ source('wyscout', 'wyscout_matches') }}

),

mapping as (

    select
        wyscout_competition_id,
        statsbomb_competition_id
    from {{ ref('competition_id_mapping') }}

),

final as (

    select
        cast(s.wyId as bigint)                           as match_id,
        cast(coalesce(m.statsbomb_competition_id,
                 cast(s.competitionId as int)) as int)    as competition_id,
        cast(s.seasonId as int)                          as season_id,
        s.competition_name,
        s.dateutc                                        as match_date,

        -- PR 1.5 expansion — surface previously-dropped match metadata.
        s.status                                         as match_status,
        cast(s.roundId as bigint)                        as round_id,
        cast(s.gameweek as int)                          as gameweek,
        cast(s.winner as bigint)                         as winner_team_id,
        s.venue,
        s.label                                          as match_label,
        s.date                                           as match_date_local,
        s.duration                                       as match_duration,
        s.groupName                                      as group_name,
        s.referees                                       as referees_json,

        -- teamsData: Wyscout's nested per-team struct (keyed by team_id).
        -- Parsed as MAP<STRING, STRUCT<...>>. Downstream consumers can
        -- `LATERAL VIEW explode` this or access specific team_ids.
        -- Structure: {team_id: {side, score, formation: {lineup, bench}, ...}}
        from_json(
            s.teamsData,
            'MAP<STRING, STRUCT<side: STRING, teamId: BIGINT, coachId: BIGINT, score: BIGINT, scoreET: BIGINT, scoreP: BIGINT, hasFormation: STRING, formation: STRUCT<lineup: ARRAY<STRUCT<playerId: BIGINT, assists: STRING, goals: STRING, ownGoals: STRING, redCards: STRING, yellowCards: STRING>>, bench: ARRAY<STRUCT<playerId: BIGINT, assists: STRING, goals: STRING, ownGoals: STRING, redCards: STRING, yellowCards: STRING>>, substitutions: ARRAY<STRUCT<playerIn: BIGINT, playerOut: BIGINT, minute: BIGINT>>>>>'
        )                                                as teams_data_parsed,
        s.teamsData                                      as teams_data_json,

        'wyscout'                                        as data_source

    from source s
    left join mapping m
        on cast(s.competitionId as int) = m.wyscout_competition_id

)

select * from final
