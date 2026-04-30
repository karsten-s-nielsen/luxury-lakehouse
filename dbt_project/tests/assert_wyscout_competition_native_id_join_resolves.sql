-- assert_wyscout_competition_native_id_join_resolves.sql
-- ADR-018 cross-table JOIN-coverage gate.

{{ config(
    tags=['post_deploy_only'],
    enabled=var('include_post_deploy_tests', false),
) }}

select distinct b.competition_native_id
from {{ ref('stg_spadl__action_values') }} b
left join {{ ref('dim_competitions') }} d
    on b.competition_native_id = d.native_competition_id
   and b.data_source = d.provider
where b.data_source = 'wyscout'
  and b.competition_native_id is not null
  and d.competition_key is null
