-- int_team_xref.sql
-- Cross-provider team identity mapping — mirror of int_player_xref.
--
-- Populated by scripts/generate_entity_xref.py + seeds/team_xref_overrides.csv.
-- Grain: one row per (source_a, team_id_a, source_b, team_id_b).
-- Convention: source_a < source_b lexicographically.

{{ config(
    materialized='view',
    enabled=var('entity_resolution_enabled', false)
) }}

with automated_matches as (

    select
        cast(source_a as string)            as source_a,
        cast(team_id_a as string)           as team_id_a,
        cast(source_b as string)            as source_b,
        cast(team_id_b as string)           as team_id_b,
        confidence,
        match_layer
    from {{ source('entity_resolution', 'team_xref_raw') }}
    where confidence >= 70.0
      and source_a is not null
      and source_b is not null
      and source_a < source_b

),

overrides as (

    select
        cast(source_a as string)            as source_a,
        cast(team_id_a as string)           as team_id_a,
        cast(source_b as string)            as source_b,
        cast(team_id_b as string)           as team_id_b,
        action
    from {{ ref('team_xref_overrides') }}

),

filtered as (

    select
        a.source_a,
        a.team_id_a,
        a.source_b,
        a.team_id_b,
        a.confidence,
        a.match_layer,
        'automated' as resolution_type
    from automated_matches a
    left join overrides o
        on  a.source_a = o.source_a
       and a.team_id_a = o.team_id_a
       and a.source_b = o.source_b
       and a.team_id_b = o.team_id_b
    where o.source_a is null

),

forced as (

    select
        source_a, team_id_a, source_b, team_id_b,
        100.0 as confidence, 0 as match_layer, 'manual_override' as resolution_type
    from overrides
    where action = 'force_match'

),

combined as (

    select * from filtered
    union all
    select * from forced

)

select * from combined
