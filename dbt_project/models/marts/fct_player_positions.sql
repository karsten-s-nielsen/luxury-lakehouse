{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='position_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key']
) }}
-- fct_player_positions.sql
-- Gold-layer per-frame position labels from the shape graph algorithm.
--
-- Each row assigns a player to a tactical position (5-level vertical x
-- 5-level horizontal grid) for one tracking frame. Used as the base for
-- fct_position_maps (aggregated time-in-position percentages).
--
-- Incremental: only processes match_ids not yet present in this table.
-- The surrogate key is (match_id, frame_id, player_id, source_provider).
--
-- Reference: Sotudeh, S. (2026). Shape graph formation detection.
--
-- PR 7 (ADR-011 close-out): adds Kimball surrogate FKs match_key + team_key
-- + player_key. The bronze `team` column is a 'home'/'away' role string
-- (no real team_id — formations algorithm doesn't carry tracking-team
-- identity), so team_key is resolved via fct_match_summary JOIN on
-- match_key + CASE on team='home'/'away' → home_team_key/away_team_key.
-- match_key + player_key resolve via dim_matches/dim_players JOIN on the
-- staging-derived source_provider. Surrogate-key inputs gain source_provider
-- to keep position_id stable when SkillCorner+Metrica match_ids that would
-- otherwise collide on a hypothetical future provider get re-derived.

with

{% if is_incremental() %}
existing_matches as (
    select distinct match_id from {{ this }}
),
{% endif %}

player_positions as (

    select * from {{ ref('stg_shape_graphs__positions') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}

),

-- Tracking player metadata: maps provider player IDs to display names.
-- Small table (~200 rows) — safe to LEFT JOIN at frame granularity.
tracking_meta as (

    select
        match_id,
        player_id,
        player_display_name,
        team_display_name
    from {{ ref('stg_tracking__player_metadata') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'pp.match_id',
            'pp.frame_id',
            'pp.player_id',
            'pp.source_provider'
        ]) }}                                       as position_id,

        pp.match_id,
        dm.match_key,
        pp.frame_id,
        pp.player_id,
        dp.player_key,
        coalesce(tm.player_display_name, pp.player_id)
                                                    as player_display_name,
        pp.team,
        case
            when pp.team = 'home' then ms.home_team_key
            when pp.team = 'away' then ms.away_team_key
        end                                         as team_key,
        coalesce(tm.team_display_name, initcap(pp.team))
                                                    as team_display_name,
        pp.position_label,
        pp.vertical_level,
        pp.horizontal_level,
        pp.detector,
        pp.source_provider                          as data_source,
        pp._ingested_at

    from player_positions as pp
    left join tracking_meta as tm
        on  pp.match_id = tm.match_id
       and pp.player_id = tm.player_id
    left join {{ ref('dim_matches') }} dm
        on  dm.provider = pp.source_provider
       and dm.native_match_id = pp.match_id
    left join {{ ref('dim_players') }} dp
        on  dp.provider = pp.source_provider
       and dp.native_player_id = pp.player_id
    left join {{ ref('fct_match_summary') }} ms
        on  ms.match_key = dm.match_key

)

select * from final
