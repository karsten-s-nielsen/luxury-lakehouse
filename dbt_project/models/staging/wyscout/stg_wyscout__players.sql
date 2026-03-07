-- stg_wyscout__players.sql
-- Wyscout player metadata from Figshare (Pappalardo et al. 2019).
--
-- Source: https://figshare.com/collections/Soccer_match_event_dataset/4415000
-- 3,603 players across 7 competitions (2017/18 season).
--
-- JSON columns (role, passportArea, birthArea) are pre-serialized to strings
-- by the ingestion layer.

with source as (

    select * from {{ source('wyscout', 'wyscout_players') }}

),

final as (

    select
        cast(wyId as int)                                    as player_id,
        firstName                                            as first_name,
        lastName                                             as last_name,
        shortName                                            as short_name,
        concat_ws(' ', firstName, lastName)                  as player_name,
        birthDate                                            as birth_date,
        role:name::string                                    as position_name,
        role:code2::string                                   as position_code,
        cast(currentTeamId as int)                           as current_team_id,
        foot,
        cast(height as int)                                  as height_cm,
        cast(weight as int)                                  as weight_kg,
        passportArea:name::string                            as nationality,
        passportArea:alpha3code::string                      as nationality_code,
        'wyscout'                                            as data_source

    from source
    where wyId is not null

)

select * from final
