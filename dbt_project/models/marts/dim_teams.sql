-- dim_teams.sql
-- Team dimension table combining data from all sources.
--
-- StatsBomb team_id + team_name come from events (matches only has names).
-- Wyscout team_id comes from events (team names not available in open data).
-- Cross-source deduplication: prefer StatsBomb record (has team_name).
--
-- Grain: one row per unique team_id.

with all_teams as (

    select
        team_id,
        team_name,
        'statsbomb'                                     as data_source
    from {{ ref('stg_statsbomb__events') }}
    where team_id is not null

    union all

    select
        team_id,
        cast(null as string)                            as team_name,
        'wyscout'                                       as data_source
    from {{ ref('stg_wyscout__events') }}
    where team_id is not null

),

-- Deduplicate: prefer row with team_name (StatsBomb) over NULL (Wyscout)
ranked as (

    select
        team_id,
        team_name,
        data_source,
        row_number() over (
            partition by team_id
            order by case when team_name is not null then 0 else 1 end,
                     data_source
        )                                               as rn

    from all_teams

),

final as (

    select
        team_id,
        team_name,
        data_source

    from ranked
    where rn = 1

)

select * from final
