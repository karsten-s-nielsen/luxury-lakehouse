-- int_tracking_goalkeepers.sql
-- Extracts distinct GK player identities per match from tracking_context staging.
-- Single source of truth for GK identification across all tracking providers,
-- powered by silly-kicks derive_goalkeepers() 3-tier identification.
--
-- Grain: one row per (match_key, player_key) for each GK in a match.
-- Expected: ~2 rows/match (IDSSE/SkillCorner), 1 row/match (Metrica — home only).
-- GK substitution matches may have 3 rows.
--
-- Uses INNER JOINs for dimension resolution — a valid GK native ID with no
-- dim_players entry is silently dropped. warn_unresolved_gk_player_ids.sql
-- guards against this.

with gks as (

    select distinct
        data_source,
        native_match_id,
        defending_gk_player_id_native as player_id_native
    from {{ ref('stg_spadl__tracking_context') }}
    where defending_gk_player_id_native is not null

)

select
    dm.match_key,
    dp.player_key
from gks
inner join {{ ref('dim_matches') }} dm
    on  dm.provider = gks.data_source
   and dm.native_match_id = gks.native_match_id
inner join {{ ref('dim_players') }} dp
    on  dp.provider = gks.data_source
   and dp.native_player_id = gks.player_id_native
