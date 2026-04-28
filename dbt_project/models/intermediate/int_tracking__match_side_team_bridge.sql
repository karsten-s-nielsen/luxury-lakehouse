{{ config(materialized='table', schema='silver') }}

-- One row per (source_provider, match_id, side='home'/'away', team_id) — the
-- per-match home/away team mapping needed by:
--   * fct_match_summary IDSSE / Metrica / SkillCorner home/away resolution
--     (replacing the prior "cannot be pivoted" branches with a bridge JOIN).
--   * fct_formation_labels per-(match, side) team_key resolution.
--   * stg_idsse__passes.ball_at_end_frame (formerly read stg_idsse__home_away_teams,
--     now reads this filtered to source_provider='idsse').
--
-- Generalises the deleted stg_idsse__home_away_teams across all 3 tracking providers.
-- Sourced from the three stg_*__tracking views (post-PR-7-hotfix-#3 staging
-- canonicalization).
--
-- Materialized as table for the same reason as int_tracking__player_match_team_bridge:
-- 40 rows total; tabling avoids repeat 38M-row DISTINCT scans across consumers.

with idsse_mst as (
    select distinct
        'idsse'                          as source_provider,
        cast(match_id as string)         as match_id,
        cast(team as string)             as side,
        cast(team_id as string)          as team_id
    from {{ ref('stg_idsse__tracking') }}
    where team in ('home', 'away')
      and team_id is not null
),
metrica_mst as (
    select distinct
        'metrica'                        as source_provider,
        cast(match_id as string)         as match_id,
        cast(team as string)             as side,
        cast(team_id as string)          as team_id
    from {{ ref('stg_metrica__tracking') }}
    where team in ('home', 'away')
      and team_id is not null
),
skillcorner_mst as (
    select distinct
        'skillcorner'                    as source_provider,
        cast(match_id as string)         as match_id,
        cast(team as string)             as side,
        cast(team_id as string)          as team_id
    from {{ ref('stg_skillcorner__tracking') }}
    where team in ('home', 'away')
      and team_id is not null
)

select * from idsse_mst
union all select * from metrica_mst
union all select * from skillcorner_mst
