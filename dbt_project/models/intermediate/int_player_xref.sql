-- int_player_xref.sql
-- Cross-provider player identity mapping.
--
-- PR 5a (ADR-011): extended from SB↔WS-only to multi-provider pairs —
-- (source_a, player_id_a) ↔ (source_b, player_id_b) at confidence ≥ 70.
-- Populated by scripts/generate_entity_xref.py + manual overrides via
-- seeds/player_xref_overrides.csv.
--
-- Materialisation: view (was ephemeral). Flipped so
-- test_int_player_xref_invariants.py can query it directly.
--
-- Convention: source_a < source_b lexicographically — each unordered pair
-- appears exactly once. Enforced here via WHERE clause.

{{ config(
    materialized='view',
    enabled=var('entity_resolution_enabled', false)
) }}

with automated_matches as (

    select
        cast(source_a as string)            as source_a,
        cast(player_id_a as string)         as player_id_a,
        cast(source_b as string)            as source_b,
        cast(player_id_b as string)         as player_id_b,
        confidence,
        match_layer
    from {{ source('entity_resolution', 'player_xref_raw') }}
    where confidence >= 70.0
      and source_a is not null
      and source_b is not null
      and source_a < source_b

),

overrides as (

    select
        cast(source_a as string)            as source_a,
        cast(player_id_a as string)         as player_id_a,
        cast(source_b as string)            as source_b,
        cast(player_id_b as string)         as player_id_b,
        action
    from {{ ref('player_xref_overrides') }}

),

-- Remove automated matches vetoed by a manual override (force_reject or force_match).
-- force_match pairs are re-added below with 100% confidence + layer=0.
filtered as (

    select
        a.source_a,
        a.player_id_a,
        a.source_b,
        a.player_id_b,
        a.confidence,
        a.match_layer,
        'automated' as resolution_type
    from automated_matches a
    left join overrides o
        on  a.source_a = o.source_a
       and a.player_id_a = o.player_id_a
       and a.source_b = o.source_b
       and a.player_id_b = o.player_id_b
    where o.source_a is null

),

forced as (

    select
        o.source_a,
        o.player_id_a,
        o.source_b,
        o.player_id_b,
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
