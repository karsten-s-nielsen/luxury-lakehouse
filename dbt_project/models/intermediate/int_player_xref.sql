-- int_player_xref.sql
-- Cross-source player identity mapping.
--
-- Combines automated resolution results (player_xref_raw bronze table)
-- with manual overrides (player_xref_overrides seed).
--
-- Grain: one row per cross-source match (statsbomb_player_id <-> wyscout_player_id).

{{ config(
    materialized='ephemeral',
    enabled=var('entity_resolution_enabled', false)
) }}

with automated_matches as (

    select
        cast(player_id_a as int)    as statsbomb_player_id,
        cast(player_id_b as int)    as wyscout_player_id,
        confidence,
        match_layer

    from {{ source('entity_resolution', 'player_xref_raw') }}
    where confidence >= 70.0

),

overrides as (

    select
        cast(statsbomb_player_id as int)    as statsbomb_player_id,
        cast(wyscout_player_id as int)      as wyscout_player_id,
        action

    from {{ ref('player_xref_overrides') }}

),

-- Remove automated matches that have a force_reject or force_match override
-- (force_match pairs are re-added in the forced CTE with 100% confidence)
filtered as (

    select
        a.statsbomb_player_id,
        a.wyscout_player_id,
        a.confidence,
        a.match_layer,
        'automated' as resolution_type

    from automated_matches a
    left join overrides o
        on  a.statsbomb_player_id = o.statsbomb_player_id
        and a.wyscout_player_id = o.wyscout_player_id
    where o.statsbomb_player_id is null

),

-- Add force_match overrides (may or may not have been in automated results)
forced as (

    select
        o.statsbomb_player_id,
        o.wyscout_player_id,
        100.0 as confidence,
        0 as match_layer,
        'manual_override' as resolution_type

    from overrides o
    where o.action = 'force_match'

),

combined as (

    select * from filtered
    union all
    select * from forced

)

select * from combined
