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

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'player_positions.match_id',
            'player_positions.frame_id',
            'player_positions.player_id'
        ]) }}                                       as position_id,

        player_positions.match_id,
        player_positions.frame_id,
        player_positions.player_id,
        player_positions.team,
        player_positions.position_label,
        player_positions.vertical_level,
        player_positions.horizontal_level,
        player_positions.detector,
        player_positions._ingested_at

    from player_positions

)

select * from final
