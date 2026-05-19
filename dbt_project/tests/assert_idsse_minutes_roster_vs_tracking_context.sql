-- assert_idsse_minutes_roster_vs_tracking_context.sql
-- IDSSE players in int_minutes_played_per_match must be a subset of
-- tracking_context roster. Uses surrogates (match_key, player_key) since
-- the intermediate outputs surrogates only — IDSSE native IDs are DFL
-- strings that cannot be cast to BIGINT.

with minutes_players as (

    select distinct
        match_key,
        player_key
    from {{ ref('int_minutes_played_per_match') }}
    where data_source = 'idsse'

),

tc_players as (

    -- Resolve tracking_context native IDs to surrogates via dim JOINs.
    select distinct
        dm.match_key,
        dp.player_key
    from {{ ref('stg_spadl__tracking_context') }} tc
    inner join {{ ref('dim_matches') }} dm
        on  dm.provider = 'idsse'
        and dm.native_match_id = tc.native_match_id
    inner join {{ ref('dim_players') }} dp
        on  dp.provider = 'idsse'
        and dp.native_player_id = tc.player_id_native
    where tc.data_source = 'idsse'

)

select
    mp.match_key,
    mp.player_key
from minutes_players mp
left join tc_players tc
    on  mp.match_key = tc.match_key
    and mp.player_key = tc.player_key
where tc.player_key is null
