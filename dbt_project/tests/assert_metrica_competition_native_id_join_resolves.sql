-- assert_metrica_competition_native_id_join_resolves.sql
-- ADR-018 cross-table JOIN-coverage gate.
-- This test catches Bug #4 — dim_matches.metrica_matches CTE hardcoded NULL
-- competition_id even though stg_metrica__matches emitted 'metrica-sample'.
-- Goes GREEN after the dim_matches CTE passthrough fix.

{{ config(tags=['slim_ci']) }}

select distinct b.competition_native_id
from {{ ref('stg_spadl__action_values') }} b
left join {{ ref('dim_competitions') }} d
    on b.competition_native_id = d.native_competition_id
   and b.data_source = d.provider
where b.data_source = 'metrica'
  and b.competition_native_id is not null
  and d.competition_key is null
