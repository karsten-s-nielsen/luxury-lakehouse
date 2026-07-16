-- int_tracking_goalkeepers.sql
-- Distinct GK player identities per match, powered by silly-kicks derive_goalkeepers()
-- 3-tier identification. Single source of truth for GK identification across tracking providers.
--
-- Grain: one row per (match_key, player_key) for each GK in a match.
-- Expected: ~2 rows/match (IDSSE/SkillCorner), 1 row/match (Metrica — home only).
-- GK substitution matches may have 3 rows.
--
-- SOURCE (PR-1): re-homed from the retired TC-1 pipeline (stg_spadl__tracking_context) onto
-- AC-1 (stg_action_context__values). AC is a proven-canonical coverage superset (TC_only=0 live),
-- computed on geometrically-oriented frames, and is 0-dup by M13 work-unit ownership.
--
-- data_source filter (THE TRAP): AC carries all 6 providers; this model must cover ONLY the
-- tracking providers TC-1 covered (idsse/metrica/skillcorner). gradientsports and statsbomb-360
-- must NOT enter. Pinned by assert_tracking_gk_provider_scope.sql.
--
-- n_actions >= 2 threshold: silly-kicks derive_goalkeepers occasionally tags an outfield player
-- as the defending GK for a SINGLE action (a one-off mis-tag). Requiring a player to be the
-- defending GK in more than one action strips those (validated live PR-1 Task 1: every mis-tag
-- had n_actions=1; confirmed GKs had >=53; 0 confirmed goalkeepers dropped) while keeping true
-- substitute keepers (e.g. a sub GK with ~96 actions). Pinned by
-- test_int_tracking_goalkeepers_min_actions.py.
--
-- Uses INNER JOINs for dimension resolution — a valid GK native ID with no dim_players entry is
-- silently dropped. The unresolved-GK guard (assert_unresolved_gk_player_ids.sql) covers that.

with gks as (

    select
        data_source,
        native_match_id,
        defending_gk_player_id_native as player_id_native
    from {{ ref('stg_action_context__values') }}
    where defending_gk_player_id_native is not null
      and data_source in ('idsse', 'metrica', 'skillcorner')
    group by data_source, native_match_id, defending_gk_player_id_native
    having count(*) >= 2

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
