-- int_minutes_played.sql
-- Aggregate per-match minutes to per-player per-competition per-season totals.
--
-- Materialized as ephemeral (CTE).
-- Consumes int_minutes_played_per_match (surrogate-only) and resolves native
-- player_id via dim_players for downstream consumers that still JOIN on
-- native IDs (fct_player_stats).
--
-- Grain: one row per (player_id, data_source, competition_id, season_id).
-- Note: IDSSE rows get player_id = NULL because IDSSE native player IDs are
-- DFL strings (e.g. "DFL-OBJ-0028GH") that cannot be cast to BIGINT.
-- try_cast returns NULL for non-numeric strings (safe); BIGINT avoids
-- overflow for SkillCorner IDs that may exceed INT range (~2.1B).
-- The WHERE filter drops NULL rows -- no regression since int_minutes_played
-- was previously StatsBomb-only and had no IDSSE rows.

with per_match as (

    select
        try_cast(dp.native_player_id as bigint)            as player_id,
        imp.data_source,
        dm.competition_id,
        dm.season_id,
        imp.minutes_played
    from {{ ref('int_minutes_played_per_match') }} imp
    inner join {{ ref('dim_matches') }} dm
        on imp.match_key = dm.match_key
    inner join {{ ref('dim_players') }} dp
        on imp.player_key = dp.player_key

)

select
    player_id,
    data_source,
    competition_id,
    season_id,
    sum(minutes_played) as total_minutes_played
from per_match
where player_id is not null
group by player_id, data_source, competition_id, season_id
