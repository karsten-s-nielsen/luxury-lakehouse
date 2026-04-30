-- assert_statsbomb_match_id_native_join_resolves.sql
-- ADR-018 cross-table JOIN-coverage gate (slim_ci-runnable).
-- Returns rows ⇒ test failure. Asserts that every distinct value of
-- bronze.spadl_actions.match_id_native for StatsBomb rows is resolvable in
-- dim_matches.native_match_id.

{{ config(tags=['slim_ci']) }}

select distinct b.match_id_native
from {{ ref('stg_spadl__action_values') }} b
left join {{ ref('dim_matches') }} d
    on b.match_id_native = d.native_match_id
   and b.data_source = d.provider
where b.data_source = 'statsbomb'
  and b.match_id_native is not null
  and d.match_key is null
