-- stg_idsse__matches.sql
-- One row per IDSSE Bundesliga match. Match count is data-driven —
-- whatever distinct match_ids exist in bronze.idsse_tracking ship through.
-- The legacy hardcoded `idsse_competitions` CTE was eliminated in
-- session 69 once the bronze writer was wired to surface
-- competition_native_id directly from the DFL matchinformation XML
-- (parity with bronze.idsse_events; see ADR-018 + the meta-test in
-- src/tests/test_idsse_match_metadata_parity.py).
--
-- Sources:
--   - bronze.idsse_tracking (one distinct row per match_id; carries
--     competition_native_id + season_native_id + home/away_team_id_native)
--   - stg_tracking__player_metadata (home/away team display names)
--
-- Format invariant: bronze.idsse_*.match_id is the BARE DFL MatchId
-- (e.g. 'J03WMX') per shared/identifiers.py:idsse_native_match_id.
-- Live boundary test: src/tests/test_idsse_bronze_match_id_format.py.

with tracking_metadata as (

    -- Pull one row per match carrying the per-match metadata that the
    -- bronze writer broadcasts to every tracking row. Picking any single
    -- row is fine since these columns are constant per match — `select
    -- distinct match_id, competition_native_id, ...` collapses the
    -- tracking-scale row count down to one row per match.
    select distinct
        match_id                  as native_match_id,
        competition_native_id     as competition_id,
        season_native_id          as season_id,
        home_team_id_native       as home_team_id,
        away_team_id_native       as away_team_id
    from {{ source('idsse', 'idsse_tracking') }}

),

team_names as (

    select
        match_id,
        max(case when team_side = 'home' then team_display_name end) as home_team_name,
        max(case when team_side = 'away' then team_display_name end) as away_team_name
    from {{ ref('stg_tracking__player_metadata') }}
    where provider = 'idsse'
    group by match_id

),

final as (

    select
        tm.native_match_id,
        'idsse'                                              as provider,
        tm.competition_id,
        tm.season_id,
        tm.home_team_id,
        tm.away_team_id,
        tn.home_team_name,
        tn.away_team_name,
        tm.native_match_id                                   as bronze_match_id

    from tracking_metadata tm
    left join team_names tn
        on tm.native_match_id = tn.match_id

)

select * from final
