-- assert_metrica_team_id_native_join_resolves.sql
-- ADR-018 cross-table JOIN-coverage gate.
-- THIS TEST WAS RED ON main BEFORE PR-LL2 PATH B CLOSE-OUT —
-- bronze.spadl_actions.team_id_native for Metrica was 'Sample_Game_1-Home'
-- (capital, hyphen) but dim_teams.native_team_id was 'metrica_Sample_Game_1_home'
-- (lowercase, prefixed). Bug #2 aligns bronze writer to dim format; this test
-- goes GREEN after re-ingestion.

{{ config(
    tags=['post_deploy_only'],
    enabled=var('include_post_deploy_tests', false),
) }}

select distinct b.team_id_native
from {{ ref('stg_spadl__action_values') }} b
left join {{ ref('dim_teams') }} d
    on b.team_id_native = d.native_team_id
   and b.data_source = d.provider
where b.data_source = 'metrica'
  and b.team_id_native is not null
  and d.team_key is null
