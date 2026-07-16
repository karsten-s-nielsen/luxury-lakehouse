-- assert_unresolved_gk_player_ids.sql
-- TC-2: Guards against silent drops from the INNER JOIN in int_tracking_goalkeepers.
-- Any GK identity the model would include (AC-1 source, tracking providers only, n_actions>=2)
-- that cannot resolve to a dim_players entry is flagged. Re-homed off the retired TC-1 pipeline
-- in PR-1; mirrors int_tracking_goalkeepers' source + provider scope + threshold so it flags
-- exactly the identities the model attempts to resolve.
{{ config(severity='warn') }}

with gk_identities as (

    select
        data_source,
        defending_gk_player_id_native
    from {{ ref('stg_action_context__values') }}
    where defending_gk_player_id_native is not null
      and data_source in ('idsse', 'metrica', 'skillcorner')
    group by data_source, native_match_id, defending_gk_player_id_native
    having count(*) >= 2

)

select distinct
    g.data_source,
    g.defending_gk_player_id_native
from gk_identities g
left join {{ ref('dim_players') }} dp
    on  dp.provider = g.data_source
   and dp.native_player_id = g.defending_gk_player_id_native
where dp.player_key is null
