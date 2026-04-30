-- assert_idsse_match_id_native_join_resolves.sql
-- ADR-018 cross-table JOIN-coverage gate.
-- THIS TEST WAS RED ON main BEFORE PR-LL2 PATH B CLOSE-OUT —
-- bronze.spadl_actions.match_id_native for IDSSE was 'idsse_J03WMX' (prefixed)
-- but dim_matches.native_match_id was bare 'J03WMX'. Bug #1 strips the prefix
-- in bronze writer; this test goes GREEN after re-ingestion.

{{ config(tags=['slim_ci']) }}

select distinct b.match_id_native
from {{ ref('stg_spadl__action_values') }} b
left join {{ ref('dim_matches') }} d
    on b.match_id_native = d.native_match_id
   and b.data_source = d.provider
where b.data_source = 'idsse'
  and b.match_id_native is not null
  and d.match_key is null
