-- dim_tracking_matches.sql
-- Dimension table for tracking-only matches (IDSSE, SkillCorner, Metrica).
--
-- One row per (match_id, team) with human-readable team names from
-- the tracking player metadata pipeline. Used by the Taipy app for
-- the tracking match selector dropdown labels.
--
-- Grain: one row per match_id (pivoted home/away team names).

with team_names as (

    select distinct
        match_id,
        team_side,
        team_display_name,
        provider

    from {{ ref('stg_tracking__player_metadata') }}

),

pivoted as (

    select
        match_id,
        max(case when team_side = 'home' then team_display_name end) as home_team_name,
        max(case when team_side = 'away' then team_display_name end) as away_team_name,
        max(provider)                                                 as provider

    from team_names
    group by match_id

)

select * from pivoted
