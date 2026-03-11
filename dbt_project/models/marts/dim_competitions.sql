-- dim_competitions.sql
-- Competition dimension table enriched with seed data.
--
-- Combines competition information extracted from match metadata
-- with the competition_metadata seed for additional attributes
-- (country, gender) that may not be present in the raw data.
--
-- Grain: one row per unique competition.

with statsbomb_competitions as (

    select distinct
        competition_id,
        competition_name

    from {{ ref('stg_statsbomb__matches') }}
    where competition_id is not null

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
        -- Prefer the name from actual data, fall back to seed
        coalesce(c.competition_name, s.seed_competition_name) as competition_name,
        s.country,
        s.gender

    from statsbomb_competitions c
    left join seed_metadata s
        on c.competition_id = s.competition_id

)

-- Wyscout competitions (La Liga, PL, Serie A, Bundesliga, Ligue 1) are a
-- strict subset of StatsBomb competitions already captured above. No union
-- needed — Wyscout uses its own competition IDs with no staging model.

select * from final
