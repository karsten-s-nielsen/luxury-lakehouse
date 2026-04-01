{{ config(
    materialized='table',
    liquid_clustered_by=['match_id'],
    contract=({'enforced': true} if var('space_creation_enabled', false) else {'enforced': false})
) }}

{% if var('space_creation_enabled', false) %}

with values as (
    select * from {{ ref('stg_space_creation__values') }}
),

players as (
    select cast(player_id as string) as player_id, canonical_player_id
    from {{ ref('dim_players') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['v.match_id', 'v.frame_id', 'v.player_id']) }} as space_creation_id,
    v.match_id, v.frame_id,
    coalesce(p.canonical_player_id, v.player_id) as player_id,
    v.team, v.period,
    v.space_created_m2, v.space_destroyed_m2, v.net_space_m2
from values v
left join players p on v.player_id = p.player_id

{% else %}

select
    cast(null as string) as space_creation_id,
    cast(null as string) as match_id,
    cast(null as int) as frame_id,
    cast(null as string) as player_id,
    cast(null as string) as team,
    cast(null as int) as period,
    cast(null as double) as space_created_m2,
    cast(null as double) as space_destroyed_m2,
    cast(null as double) as net_space_m2
where 1 = 0

{% endif %}
