-- stg_idsse__home_away_teams.sql
-- Bridge: one row per (match_id, side) → real DFL TeamId.
--
-- IDSSE tracking carries team_id + team (home/away) per row. Collapsing
-- to grain (match_id, side) gives a bridge joinable from event-layer
-- staging (stg_idsse__passes, etc.) where `*_team` columns carry
-- 'home'/'away' strings only (no raw DFL TeamId on events).
--
-- `match_id` is normalized to strip the 'idsse_' bronze prefix so the
-- bridge keys align with dim_matches.native_match_id (e.g., 'J03WMX'
-- rather than 'idsse_J03WMX').
--
-- Grain: one row per (match_id, side). Uniqueness enforced in
-- _idsse__models.yml.

with tracking as (

    select distinct
        match_id,
        team          as side,
        team_id
    from {{ ref('stg_idsse__tracking') }}
    where team in ('home', 'away')
      and team_id is not null

),

final as (

    select
        regexp_replace(match_id, '^idsse_', '') as match_id,
        side,
        team_id                                  as team_id
    from tracking

)

select * from final
