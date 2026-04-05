{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='position_id',
    on_schema_change='fail',
    liquid_clustered_by=['match_id']
) }}
-- fct_player_positions.sql
-- Gold-layer per-frame position labels from the shape graph algorithm.
--
-- Each row assigns a player to a tactical position (5-level vertical x
-- 5-level horizontal grid) for one tracking frame. Used as the base for
-- fct_position_maps (aggregated time-in-position percentages).
--
-- Incremental: only processes match_ids not yet present in this table.
-- The surrogate key is (match_id, frame_id, player_id).
--
-- Reference: Sotudeh, S. (2026). Shape graph formation detection.

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
            'pp.player_id'
        ]) }}                                       as position_id,

        pp.match_id,
        pp.frame_id,
        pp.player_id,
        coalesce(tm.player_display_name, pp.player_id)
                                                    as player_display_name,
        pp.team,
        coalesce(tm.team_display_name, initcap(pp.team))
                                                    as team_display_name,
        pp.position_label,
        pp.vertical_level,
        pp.horizontal_level,
        pp.detector,
        pp._ingested_at

    from player_positions as pp
    left join tracking_meta as tm
        on pp.match_id = tm.match_id
        and pp.player_id = tm.player_id

)

select * from final
