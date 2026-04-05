-- dim_competitions.sql
-- Competition dimension table enriched with seed data.
--
-- Combines competition information from StatsBomb match metadata and
-- Wyscout match metadata with the competition_metadata seed for
-- additional attributes (country, gender).
--
-- StatsBomb and Wyscout use separate competition ID spaces for the same
-- leagues (e.g., La Liga is 11 in StatsBomb, 795 in Wyscout). Both ID
-- spaces are included as independent rows so downstream fact tables from
-- either source can JOIN without NULL competition names.
--
-- Grain: one row per unique competition_id (across all sources).

with statsbomb_competitions as (

    select distinct
        competition_id,
        competition_name

    from {{ ref('stg_statsbomb__matches') }}
    where competition_id is not null

),

wyscout_competitions as (

    select distinct
        competition_id,
        competition_name

    from {{ ref('stg_wyscout__matches') }}
    where competition_id is not null

),

-- Union with StatsBomb priority: when both sources map to the same
-- competition_id (e.g., La Liga = 11), keep the StatsBomb row which
-- has the richer competition_name (e.g., "Spain - La Liga" vs "Spain").
all_competitions as (

    select
        competition_id,
        competition_name,
        row_number() over (
            partition by competition_id
            order by case when source = 'statsbomb' then 0 else 1 end
        ) as _rn
    from (
        select competition_id, competition_name, 'statsbomb' as source
        from statsbomb_competitions
        union all
        select competition_id, competition_name, 'wyscout' as source
        from wyscout_competitions
    )

),

deduped_competitions as (

    select competition_id, competition_name
    from all_competitions
    where _rn = 1

),

-- Seed data with enrichment attributes
seed_metadata as (

    select
        competition_id,
        competition_name                                as seed_competition_name,
        country,
        gender

    from {{ ref('competition_metadata') }}

),

final as (

    select
        c.competition_id,
        -- Prefer curated seed name, fall back to source data
        coalesce(s.seed_competition_name, c.competition_name) as competition_name,
        s.country,
        s.gender

    from deduped_competitions c
    left join seed_metadata s
        on c.competition_id = s.competition_id

)

select * from final
