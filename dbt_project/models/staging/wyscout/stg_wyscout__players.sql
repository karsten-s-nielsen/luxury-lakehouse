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

        -- Bronze passthroughs surfaced for Kimball completeness. Wyscout's
        -- raw Figshare payload carries identity + biography fields verbatim;
        -- surfacing them preserves full lineage from bronze to staging. Names
        -- intentionally retain the bronze camelCase so consumers can cross-
        -- reference the Figshare CSV schema without additional mapping.
        cast(wyId as bigint)                                 as wyId,
        firstName                                            as firstName,
        lastName                                             as lastName,
        middleName                                           as middleName,
        shortName                                            as shortName,
        birthDate                                            as birthDate,
        cast(height as bigint)                               as height,
        cast(weight as bigint)                               as weight,
        cast(currentTeamId as double)                        as currentTeamId,
        cast(currentNationalTeamId as double)                as currentNationalTeamId,
        role                                                 as role,
        passportArea                                         as passportArea,
        birthArea                                            as birthArea,
        _ingested_at                                         as _ingested_at,

        'wyscout'                                            as data_source

    from source
    where wyId is not null

)

select * from final
