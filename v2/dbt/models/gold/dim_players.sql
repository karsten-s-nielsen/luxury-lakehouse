{{ config(materialized='table') }}
-- One row per provider's player record (spec §11.6, §14.3): the natural key, the provider key and the canonical
-- pointer of the resolved cluster. Ported from v1 dim_players without its legacy canonical_player_id (guess 50).
select
    r.record_key as player_key,
    r.provider,
    r.native_id as native_player_id,
    coalesce(p.canonical_key, r.record_key) as canonical_player_key,
    r.name as player_name,
    r.short_name,
    r.birth_date,
    r.position,
    r.team as team_name,
    r.jersey as jersey_number,
    r.matches as match_count
from {{ source('ops', 'ops_entity_records') }} as r
left join {{ source('ops', 'ops_canonical_pointers') }} as p
    on p.entity = r.entity and p.provider = r.provider and p.native_id = r.native_id
where r.entity = 'player'
