{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='position_map_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key']
) }}
-- fct_position_maps.sql
-- Gold-layer aggregated position maps per player per match.
--
-- Computes the proportion of time each player spent in each tactical position
-- (5x5 vertical/horizontal grid) by counting frames per position and
-- normalising to a percentage of total frames for that player in the match.
--
-- The phase column supports future possession-phase variants (in_possession,
-- out_of_possession); currently only 'all' is populated.
--
-- Incremental: only processes match_ids not yet present in this table.
--
-- Reference: Sotudeh, S. (2026). Shape graph formation detection.
--
-- PR 7 (ADR-011 close-out): adds Kimball surrogate FKs match_key + team_key
-- + player_key (passthrough from upstream fct_player_positions). Surrogate-key
-- inputs gain data_source so position_map_id stays stable when a hypothetical
-- future provider re-uses an existing match_id+player_id+position_label tuple.

with

{% if is_incremental() %}
existing_matches as (
    select distinct match_id from {{ this }}
),
{% endif %}

player_positions as (

    select
        player_id,
        player_key,
        player_display_name,
        match_id,
        match_key,
        team,
        team_key,
        team_display_name,
        position_label,
        vertical_level,
        horizontal_level,
        data_source
    from {{ ref('fct_player_positions') }}
    {% if is_incremental() %}
    where match_id not in (select match_id from existing_matches)
    {% endif %}

),

frame_counts as (

    select
        player_id,
        player_key,
        player_display_name,
        match_id,
        match_key,
        team,
        team_key,
        team_display_name,
        position_label,
        vertical_level,
        horizontal_level,
        data_source,
        count(*) as frame_count
    from player_positions
    group by
        player_id,
        player_key,
        player_display_name,
        match_id,
        match_key,
        team,
        team_key,
        team_display_name,
        position_label,
        vertical_level,
        horizontal_level,
        data_source

),

total_frames as (

    select
        player_id,
        match_id,
        data_source,
        count(*) as total_frame_count
    from player_positions
    group by
        player_id,
        match_id,
        data_source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'fc.player_id',
            'fc.match_id',
            'fc.position_label',
            'fc.data_source',
            "'all'"
        ]) }}                                       as position_map_id,

        fc.player_id,
        fc.player_key,
        fc.player_display_name,
        fc.match_id,
        fc.match_key,
        fc.team,
        fc.team_key,
        fc.team_display_name,
        fc.position_label,
        fc.vertical_level,
        fc.horizontal_level,
        cast(round(
            100.0 * fc.frame_count / tf.total_frame_count,
            2
        ) as double)                                as pct_time,
        'all'                                       as phase,
        fc.data_source

    from frame_counts as fc
    inner join total_frames as tf
        on  fc.player_id = tf.player_id
       and fc.match_id = tf.match_id
       and fc.data_source = tf.data_source

)

select * from final
