{{ config(materialized='table', schema='silver') }}

-- One row per (source_provider, match_id, player_id, team_id) — the per-match
-- player→team mapping needed by formations marts where the formations
-- algorithm output strips team labels and dim_players doesn't carry team_key.
--
-- Sourced from the three stg_*__tracking views (NOT fct_tracking_frames) to
-- avoid the circular dependency where a formations mart depending on a
-- fct_tracking_frames-derived bridge would force fct_tracking_frames to
-- be built before formations marts. Tracking staging is provider-canonical
-- post-PR-7-hotfix-#3 (idsse_ prefix stripped, metrica player_id synth form),
-- so the union here works directly.
--
-- Materialized as table (not view): bridge cardinality is ~616 rows total.
-- The DISTINCT collapse over 38M underlying tracking rows is the expensive
-- operation; tabling pays it once at build, then 4 downstream consumer JOINs
-- hit a tiny lookup table. View materialization would force a fresh 38M-row
-- distinct on every consumer JOIN.

with idsse_pmt as (
    select distinct
        'idsse'                          as source_provider,
        cast(match_id as string)         as match_id,
        cast(player_id as string)        as player_id,
        cast(team_id as string)          as team_id
    from {{ ref('stg_idsse__tracking') }}
    where team_id is not null
      and player_id is not null
),
metrica_pmt as (
    select distinct
        'metrica'                        as source_provider,
        cast(match_id as string)         as match_id,
        cast(player_id as string)        as player_id,
        cast(team_id as string)          as team_id
    from {{ ref('stg_metrica__tracking') }}
    where team_id is not null
      and player_id is not null
),
skillcorner_pmt as (
    select distinct
        'skillcorner'                    as source_provider,
        cast(match_id as string)         as match_id,
        cast(player_id as string)        as player_id,
        cast(team_id as string)          as team_id
    from {{ ref('stg_skillcorner__tracking') }}
    where team_id is not null
      and player_id is not null
)

select * from idsse_pmt
union all select * from metrica_pmt
union all select * from skillcorner_pmt
