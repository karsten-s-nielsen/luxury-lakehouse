{{ config(
    materialized='table',
    liquid_clustered_by=['match_key'],
    contract=({'enforced': true} if var('space_creation_enabled', false) else {'enforced': false})
) }}

-- fct_space_creation.sql
-- Gold-layer space-creation values per player-frame.
--
-- PR 7 (ADR-011 close-out): adds Kimball surrogate FKs match_key + player_key
-- via dim_matches/dim_players LEFT JOINs on the staging-derived source_provider.
-- Per-row team is a 'home'/'away' role string only — team_key is not added at
-- this grain (resolution would require fct_match_summary JOIN identical to
-- the formations-mart pattern; deferred until a use case demands it).
-- Surrogate-key inputs gain data_source for provider-stable IDs.

{% if var('space_creation_enabled', false) %}

with values as (
    select * from {{ ref('stg_space_creation__values') }}
),

players as (
    select cast(player_id as string) as player_id, canonical_player_id
    from {{ ref('dim_players') }}
),

dim_players_keys as (
    select provider, native_player_id, player_key
    from {{ ref('dim_players') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'v.match_id',
        'v.frame_id',
        'v.player_id',
        'v.source_provider'
    ]) }} as space_creation_id,
    v.match_id,
    dm.match_key,
    v.frame_id,
    coalesce(p.canonical_player_id, v.player_id) as player_id,
    dpk.player_key,
    v.team,
    v.period,
    v.space_created_m2,
    v.space_destroyed_m2,
    v.net_space_m2,
    v.source_provider as data_source
from values v
left join players p on v.player_id = p.player_id
left join {{ ref('dim_matches') }} dm
    on  dm.provider = v.source_provider
   and dm.native_match_id = v.match_id
left join dim_players_keys dpk
    on  dpk.provider = v.source_provider
   and dpk.native_player_id = v.player_id

{% else %}

select
    cast(null as string) as space_creation_id,
    cast(null as string) as match_id,
    cast(null as bigint) as match_key,
    cast(null as int) as frame_id,
    cast(null as string) as player_id,
    cast(null as bigint) as player_key,
    cast(null as string) as team,
    cast(null as int) as period,
    cast(null as double) as space_created_m2,
    cast(null as double) as space_destroyed_m2,
    cast(null as double) as net_space_m2,
    cast(null as string) as data_source
where 1 = 0

{% endif %}
