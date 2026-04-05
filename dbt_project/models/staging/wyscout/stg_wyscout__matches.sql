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
        'wyscout'                                        as data_source

    from source s
    left join mapping m
        on cast(s.competitionId as int) = m.wyscout_competition_id

)

select * from final
