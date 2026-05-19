-- assert_unresolved_gk_player_ids.sql
-- TC-2: Guards against silent drops from the INNER JOIN in
-- int_tracking_goalkeepers. Any defending_gk_player_id_native
-- that cannot resolve to a dim_players entry is flagged.
{{ config(severity='warn') }}

select distinct
    tc.data_source,
    tc.defending_gk_player_id_native
from {{ ref('stg_spadl__tracking_context') }} tc
left join {{ ref('dim_players') }} dp
    on  dp.provider = tc.data_source
   and dp.native_player_id = tc.defending_gk_player_id_native
where tc.defending_gk_player_id_native is not null
  and dp.player_key is null
