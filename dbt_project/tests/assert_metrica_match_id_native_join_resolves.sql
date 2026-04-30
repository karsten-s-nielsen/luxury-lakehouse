-- assert_metrica_match_id_native_join_resolves.sql
-- ADR-018 cross-table JOIN-coverage gate.

{{ config(tags=['slim_ci']) }}

select distinct b.match_id_native
from {{ ref('stg_spadl__action_values') }} b
left join {{ ref('dim_matches') }} d
    on b.match_id_native = d.native_match_id
   and b.data_source = d.provider
where b.data_source = 'metrica'
  and b.match_id_native is not null
  and d.match_key is null
