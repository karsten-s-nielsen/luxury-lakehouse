-- assert_gradientsports_match_id_native_join_resolves.sql
-- ADR-018 cross-table JOIN-coverage gate.
-- Asserts bronze.spadl_actions.match_id_native for Gradient Sports rows resolves
-- in dim_matches.native_match_id.

{{ config(
    tags=['post_deploy_only'],
    enabled=var('include_post_deploy_tests', false),
) }}

select distinct b.match_id_native
from {{ ref('stg_spadl__action_values') }} b
left join {{ ref('dim_matches') }} d
    on b.match_id_native = d.native_match_id
   and b.data_source = d.provider
where b.data_source = 'gradientsports'
  and b.match_id_native is not null
  and d.match_key is null
