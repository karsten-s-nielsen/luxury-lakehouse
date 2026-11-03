{{ config(materialized='table') }}
-- One row per provider's competition record, with the canonical pointer (spec §11.6, §14.3).
select
    r.record_key as competition_key,
    r.provider,
    r.native_id as native_competition_id,
    coalesce(p.canonical_key, r.record_key) as canonical_competition_key,
    r.name as competition_name,
    r.area as area_name
from {{ source('ops', 'ops_entity_records') }} as r
left join {{ source('ops', 'ops_canonical_pointers') }} as p
    on p.entity = r.entity and p.provider = r.provider and p.native_id = r.native_id
where r.entity = 'competition'
