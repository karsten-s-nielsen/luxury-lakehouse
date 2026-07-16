-- assert_idsse_minutes_roster_vs_bronze_tracking.sql
-- Containment guard (re-based in PR-1). Every idsse player credited with minutes in
-- int_minutes_played_per_match must appear in the raw idsse tracking (stg_idsse__tracking) for
-- that match: minutes-roster ⊆ tracking-players. The previous vs-tracking_context version became
-- circular once BOTH the minutes roster and the tracking-context roster re-homed onto AC-1 (PR-1);
-- stg_idsse__tracking is genuine ground truth, independent of the SPADL/AC pipeline.
--
-- Direction matters: tracking-players is a SUPERSET (a tracked player who generated no SPADL
-- action legitimately has no minutes row), so this is CONTAINMENT, not equality. A minutes-roster
-- player absent from tracking bronze is a genuine identity bug. Surrogates only (IDSSE native IDs
-- are DFL strings not castable to BIGINT).
{{ config(severity='error') }}

with minutes_players as (

    select distinct
        match_key,
        player_key
    from {{ ref('int_minutes_played_per_match') }}
    where data_source = 'idsse'

),

tracking_players as (

    select distinct
        dm.match_key,
        dp.player_key
    from {{ ref('stg_idsse__tracking') }} st
    inner join {{ ref('dim_matches') }} dm
        on  dm.provider = 'idsse'
       and dm.native_match_id = cast(st.match_id as string)
    inner join {{ ref('dim_players') }} dp
        on  dp.provider = 'idsse'
       and dp.native_player_id = cast(st.player_id as string)

)

select
    mp.match_key,
    mp.player_key
from minutes_players mp
left join tracking_players tp
    on  mp.match_key = tp.match_key
   and mp.player_key = tp.player_key
where tp.player_key is null
