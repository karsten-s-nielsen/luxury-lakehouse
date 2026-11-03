{{ config(materialized='table') }}
-- One row per provider's match record, with the canonical pointer (spec §11.6, §14.3). `has_360_frames` is true
-- exactly for a StatsBomb match with 360 bronze rows (the flag of ops_matches, the same source) and null for other
-- providers (spec §11.3, §29.13).
select
    r.record_key as match_key,
    r.provider,
    r.native_id as native_match_id,
    coalesce(p.canonical_key, r.record_key) as canonical_match_key,
    r.match_date,
    r.home_team_key,
    r.away_team_key,
    r.competition_key,
    r.season_key,
{%- if lake_table_present('ops_matches') %}
    case when r.provider = 'statsbomb' then coalesce(m.has_360_frames, false) end as has_360_frames
from {{ source('ops', 'ops_entity_records') }} as r
left join {{ source('ops', 'ops_matches') }} as m on m.match_key = r.record_key
{%- else %}
    case when r.provider = 'statsbomb' then false end as has_360_frames
from {{ source('ops', 'ops_entity_records') }} as r
{%- endif %}
left join {{ source('ops', 'ops_canonical_pointers') }} as p
    on p.entity = r.entity and p.provider = r.provider and p.native_id = r.native_id
where r.entity = 'match'
