-- assert_idsse_gk_parity.sql
-- TC-2: The set of GK (match_key, player_key) pairs identified by
-- int_tracking_goalkeepers must match the set identified by
-- stg_idsse__tracking's per-frame is_goalkeeper flag.
-- A non-empty result indicates the GK substitution temporal
-- regression (spec §4 H1) is material for this match set.

with from_intermediate as (

    select distinct gk.match_key, gk.player_key
    from {{ ref('int_tracking_goalkeepers') }} gk
    inner join {{ ref('dim_matches') }} dm
        on dm.match_key = gk.match_key
    where dm.provider = 'idsse'

),

from_staging as (

    select distinct dm.match_key, dp.player_key
    from {{ ref('stg_idsse__tracking') }} st
    inner join {{ ref('dim_matches') }} dm
        on  dm.provider = 'idsse'
       and dm.native_match_id = cast(st.match_id as string)
    inner join {{ ref('dim_players') }} dp
        on  dp.provider = 'idsse'
       and dp.native_player_id = cast(st.player_id as string)
    where st.is_goalkeeper = true

)

-- Symmetric difference: rows in one set but not the other.
select match_key, player_key, 'intermediate_only' as source
from from_intermediate
except
select match_key, player_key, 'intermediate_only'
from from_staging

union all

select match_key, player_key, 'staging_only' as source
from from_staging
except
select match_key, player_key, 'staging_only'
from from_intermediate
