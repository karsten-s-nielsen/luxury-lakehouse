-- assert_gradientsports_team_id_native_join_resolves.sql
-- ADR-018 cross-table JOIN-coverage gate.

{{ config(
    tags=['post_deploy_only'],
    enabled=var('include_post_deploy_tests', false),
) }}

select distinct b.team_id_native
from {{ ref('stg_spadl__action_values') }} b
left join {{ ref('dim_teams') }} d
    on b.team_id_native = d.native_team_id
   and b.data_source = d.provider
where b.data_source = 'gradientsports'
  and b.team_id_native is not null
  and d.team_key is null
